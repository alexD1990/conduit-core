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
    dry_run: bool = False,
):
    """Kjører en enkelt dataflyt-ressurs med batch processing, error handling, state management og manifest-sporing."""

    logger = ConduitLogger(resource.name)
    logger.start_resource()

    if dry_run:
        logger.info("DRY RUN MODE - Data will be read but not written", prefix="⚠")

    manifest = PipelineManifest(manifest_path)
    checkpoint_mgr = CheckpointManager()

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
            last_checkpoint_value = None
            if source_config.resume and source_config.checkpoint_column:
                checkpoint = checkpoint_mgr.load_checkpoint(resource.name)
                if checkpoint:
                    last_checkpoint_value = checkpoint['last_value']
                    total_processed = checkpoint.get('records_processed', 0)
                    logger.info(f"Resuming from checkpoint: {source_config.checkpoint_column} > {last_checkpoint_value}")
                else:
                    logger.info(f"Resume enabled, but no checkpoint found. Starting from beginning.")

            current_state = load_state()
            last_value = current_state.get(resource.name, 0)
            final_query = resource.query.replace(":last_value", str(last_value))
            
            if last_checkpoint_value is not None:
                wrapped_value = f"'{last_checkpoint_value}'" if isinstance(last_checkpoint_value, str) else last_checkpoint_value
                if "WHERE" in final_query.upper():
                    final_query += f" AND {source_config.checkpoint_column} > {wrapped_value}"
                else:
                    final_query += f" WHERE {source_config.checkpoint_column} > {wrapped_value}"

            logger.info(f"Source: {source_config.type} ({source_config.name})", prefix="→")
            logger.info(f"Destination: {destination_config.type} ({destination_config.name})", prefix="→")

            source_map = get_source_connector_map()
            destination_map = get_destination_connector_map()
            SourceConnector = source_map.get(source_config.type)
            DestinationConnector = destination_map.get(destination_config.type)

            if not SourceConnector or not DestinationConnector:
                raise ValueError("Connector type not found.")

            source = SourceConnector(source_config)
            destination = DestinationConnector(destination_config)
            destination.mode = resource.mode or destination_config.mode or "append"
            tracker.metadata = {"mode": destination.mode}

            supports_write_one = hasattr(destination, "write_one") and callable(getattr(destination, "write_one"))

            batch_number = 0
            max_value_seen = last_value
            current_checkpoint_max = last_checkpoint_value

            logger.info(f"Processing in batches (batch_size={batch_size})...", prefix="→")
            logger.separator()

            for batch in read_in_batches(source.read(final_query), batch_size=batch_size):
                batch_number += 1
                successful_batch = []
                
                batch_checkpoint_values = []
                if source_config.resume and source_config.checkpoint_column:
                    for record in batch:
                        if source_config.checkpoint_column in record:
                            batch_checkpoint_values.append(record[source_config.checkpoint_column])
                
                # --- FIX IS HERE: Re-introduced record-level vs batch-level logic ---
                records_in_this_batch = 0
                for record in batch:
                    records_in_this_batch += 1
                    try:
                        if not isinstance(record, dict):
                            raise ValueError(f"Record is not a dictionary: {type(record)}")
                        
                        if resource.incremental_column and resource.incremental_column in record:
                            max_value_seen = max(max_value_seen, int(record[resource.incremental_column]))

                        if dry_run:
                            successful_batch.append(record)
                        elif supports_write_one:
                            destination.write_one(record)
                            total_successful += 1
                        else:
                            successful_batch.append(record)
                    except Exception as e:
                        error_log.add_error(record, e, row_number=(total_processed + records_in_this_batch))
                        continue
                
                total_processed += records_in_this_batch

                if not dry_run and not supports_write_one and successful_batch:
                    try:
                        destination.write(successful_batch)
                        total_successful += len(successful_batch)
                    except Exception as e:
                        logger.error(f"Batch {batch_number} write failed: {e}")
                        for record in successful_batch:
                            error_log.add_error(record, e, row_number=total_processed)
                        raise # Failing the whole batch is correct for batch-only connectors

                if dry_run and successful_batch:
                    logger.info(f"[DRY RUN] Would write {len(successful_batch)} records (batch {batch_number})")
                    total_successful += len(successful_batch)

                if source_config.resume and source_config.checkpoint_column and batch_checkpoint_values and not dry_run:
                    batch_max = max(batch_checkpoint_values)
                    current_checkpoint_max = max(current_checkpoint_max, batch_max) if current_checkpoint_max is not None else batch_max
                    checkpoint_mgr.save_checkpoint(
                        pipeline_name=resource.name,
                        checkpoint_column=source_config.checkpoint_column,
                        last_value=current_checkpoint_max,
                        records_processed=total_processed
                    )
            # --- End of batch loop ---

            logger.separator()
            
            if hasattr(destination, "finalize") and callable(destination.finalize):
                if dry_run:
                    logger.info("🔧 [DRY RUN] Skipping destination.finalize()")
                else:
                    logger.info("🔧 ENGINE: Calling destination.finalize()")
                    destination.finalize()

            if error_log.has_errors() and not dry_run:
                error_log.save()
            
            if not dry_run:
                if total_successful > 0 and resource.incremental_column and max_value_seen > last_value:
                    current_state[resource.name] = max_value_seen
                    save_state(current_state)
                if source_config.resume:
                    checkpoint_mgr.clear_checkpoint(resource.name)
            else: # Dry run logging
                if total_successful > 0 and resource.incremental_column: logger.info(f"[DRY RUN] Would update state to {max_value_seen}")
                if source_config.resume: logger.info("[DRY RUN] Would clear checkpoint.")
                
            total_failed = error_log.error_count()
            tracker.records_read = total_processed
            tracker.records_written = total_successful if not dry_run else 0
            tracker.records_failed = total_failed

            logger.complete_resource(total_processed, total_successful, total_failed, dry_run=dry_run)

        except Exception as e:
            if source_config.resume and not dry_run:
                logger.warning(f"Pipeline failed, checkpoint preserved for future resume.")
            logger.error(f"Resource '{resource.name}' failed: {e}")
            tracker.records_read = total_processed
            tracker.records_written = total_successful if not dry_run else 0
            tracker.records_failed = total_processed - total_successful
            raise