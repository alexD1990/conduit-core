# src/conduit_core/engine.py
import time
from pathlib import Path
from typing import Optional
from rich import print
from .config import IngestConfig, Resource
from .state import load_state, save_state
from .errors import ErrorLog
from .batch import read_in_batches
from .logging_utils import ConduitLogger
from .connectors.registry import get_source_connector_map, get_destination_connector_map
from .manifest import PipelineManifest, ManifestTracker
from .checkpoint import CheckpointManager


def run_resource(
    resource: Resource,
    config: IngestConfig,
    batch_size: int = 1000,
    manifest_path: Optional[Path] = None,
):
    """Kjører en enkelt dataflyt-ressurs med batch processing, error handling, state management og manifest-sporing."""

    logger = ConduitLogger(resource.name)
    logger.start_resource()

    manifest = PipelineManifest(manifest_path)
    checkpoint_mgr = CheckpointManager() # Initialize checkpoint manager

    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(
        d for d in config.destinations if d.name == resource.destination
    )

    with ManifestTracker(
        manifest=manifest,
        pipeline_name=resource.name,
        source_type=source_config.type,
        destination_type=destination_config.type,
    ) as tracker:
        total_processed = 0
        total_successful = 0
        error_log = ErrorLog(resource.name)
        
        try:
            # --- CHECKPOINT/RESUME LOGIC START ---
            last_checkpoint_value = None
            if source_config.resume and source_config.checkpoint_column:
                checkpoint = checkpoint_mgr.load_checkpoint(resource.name)
                if checkpoint:
                    last_checkpoint_value = checkpoint['last_value']
                    total_processed = checkpoint.get('records_processed', 0) # Resume count
                    logger.info(f"Resuming from checkpoint: {source_config.checkpoint_column} > {last_checkpoint_value}")
                else:
                    logger.info(f"Resume enabled, but no checkpoint found. Starting from beginning.")
            # --- CHECKPOINT/RESUME LOGIC END ---

            # Load legacy state (for incremental loads without resume)
            current_state = load_state()
            last_value = current_state.get(resource.name, 0)
            logger.info(f"Last known state value: {last_value}", prefix="→")

            # Prepare query
            final_query = resource.query.replace(":last_value", str(last_value))
            
            # --- MODIFY QUERY FOR CHECKPOINT START ---
            if last_checkpoint_value is not None:
                # Naive implementation as per instructions. Assumes numeric/string comparison.
                # A more robust solution would handle data types.
                wrapped_value = f"'{last_checkpoint_value}'" if isinstance(last_checkpoint_value, str) else last_checkpoint_value
                if "WHERE" in final_query.upper():
                    final_query += f" AND {source_config.checkpoint_column} > {wrapped_value}"
                else:
                    final_query += f" WHERE {source_config.checkpoint_column} > {wrapped_value}"
                logger.info(f"Modified query for resume: ...{final_query.split('WHERE')[-1]}")
            # --- MODIFY QUERY FOR CHECKPOINT END ---


            logger.info(f"Source: {source_config.type} ({source_config.name})", prefix="→")
            logger.info(
                f"Destination: {destination_config.type} ({destination_config.name})", prefix="→"
            )

            source_map = get_source_connector_map()
            destination_map = get_destination_connector_map()
            SourceConnector = source_map.get(source_config.type)
            DestinationConnector = destination_map.get(destination_config.type)

            if not SourceConnector:
                raise ValueError(f"Kilde-konnektor av typen '{source_config.type}' ble ikke funnet.")
            if not DestinationConnector:
                raise ValueError(f"Destinasjons-konnektor av typen '{destination_config.type}' ble ikke funnet.")

            source = SourceConnector(source_config)
            destination = DestinationConnector(destination_config)
            destination.mode = resource.mode or destination_config.mode or "append"
            tracker.metadata = {"mode": destination.mode}

            supports_write_one = hasattr(destination, "write_one") and callable(
                getattr(destination, "write_one")
            )

            batch_number = 0
            max_value_seen = last_value
            current_checkpoint_max = last_checkpoint_value

            logger.info(f"Processing in batches (batch_size={batch_size})...", prefix="→")
            logger.separator()

            for batch in read_in_batches(source.read(final_query), batch_size=batch_size):
                batch_number += 1
                successful_batch = []
                
                # --- TRACK CHECKPOINT VALUE IN BATCH ---
                batch_checkpoint_values = []
                if source_config.resume and source_config.checkpoint_column:
                    for record in batch:
                        if source_config.checkpoint_column in record:
                            batch_checkpoint_values.append(record[source_config.checkpoint_column])
                # --- END TRACKING ---
                
                # Process each record in the batch
                for record in batch:
                    try:
                        if not isinstance(record, dict):
                            raise ValueError(f"Record is not a dictionary: {type(record)}")

                        if resource.incremental_column and resource.incremental_column in record:
                            try:
                                current_value = int(record[resource.incremental_column])
                                max_value_seen = max(max_value_seen, current_value)
                            except (ValueError, TypeError):
                                pass

                        if supports_write_one:
                            destination.write_one(record)
                        else:
                            successful_batch.append(record)

                    except Exception as e:
                        error_log.add_error(record, e, row_number=(total_processed + len(successful_batch)))
                        continue
                
                # Write batch if destination doesn't support write_one
                if not supports_write_one and successful_batch:
                    try:
                        destination.write(successful_batch)
                    except Exception as e:
                        logger.error(f"Batch {batch_number} write failed: {e}")
                        for record in successful_batch:
                            error_log.add_error(record, e, row_number=(total_processed + len(successful_batch)))
                        # If a batch fails, we should not proceed.
                        raise
                
                # Update counters after successful batch processing
                batch_size_processed = len(batch)
                batch_successful = len(successful_batch) if not supports_write_one else batch_size_processed - error_log.error_count() # Simplified logic
                total_processed += batch_size_processed
                total_successful += batch_successful

                # --- SAVE CHECKPOINT AFTER BATCH ---
                if source_config.resume and source_config.checkpoint_column and batch_checkpoint_values:
                    batch_max = max(batch_checkpoint_values)
                    current_checkpoint_max = max(current_checkpoint_max, batch_max) if current_checkpoint_max is not None else batch_max
                    
                    checkpoint_mgr.save_checkpoint(
                        pipeline_name=resource.name,
                        checkpoint_column=source_config.checkpoint_column,
                        last_value=current_checkpoint_max,
                        records_processed=total_processed
                    )
                # --- END SAVE CHECKPOINT ---

            logger.separator()

            if hasattr(destination, "finalize") and callable(destination.finalize):
                logger.info("🔧 ENGINE: Calling destination.finalize()")
                destination.finalize()

            if error_log.has_errors():
                error_file = error_log.save()
                logger.warning(f"{error_log.error_count()} rows failed - see {error_file}")
            
            # Update legacy state if successful records exist
            if total_successful > 0 and resource.incremental_column:
                if max_value_seen > last_value:
                    current_state[resource.name] = max_value_seen
                    save_state(current_state)
                    logger.info(
                        f"State updated: {resource.incremental_column}={max_value_seen}",
                        prefix="→",
                    )
            
            # --- CLEAR CHECKPOINT ON SUCCESS ---
            if source_config.resume:
                checkpoint_mgr.clear_checkpoint(resource.name)
                logger.info("Pipeline completed successfully, checkpoint cleared.")
            # --- END CLEAR CHECKPOINT ---

            total_failed = error_log.error_count()
            tracker.records_read = total_processed
            tracker.records_written = total_successful
            tracker.records_failed = total_failed

            logger.complete_resource(total_processed, total_successful, total_failed)

        except Exception as e:
            # --- HANDLE FAILURE WITH CHECKPOINT ---
            if source_config.resume:
                logger.warning(f"Pipeline failed, checkpoint preserved for future resume.")
            # --- END HANDLE FAILURE ---
            
            logger.error(f"Resource '{resource.name}' failed with an unhandled exception: {e}")
            
            total_failed = error_log.error_count()
            tracker.records_read = total_processed
            tracker.records_written = total_successful
            tracker.records_failed = total_failed
            raise