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

    # Get configs early to populate the tracker
    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(
        d for d in config.destinations if d.name == resource.destination
    )

    # The ManifestTracker context manager handles start/end times, status, and saving the manifest
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
            # Load state
            current_state = load_state()
            last_value = current_state.get(resource.name, 0)
            logger.info(f"Last known value: {last_value}", prefix="→")

            # Prepare query
            final_query = resource.query.replace(":last_value", str(last_value))

            logger.info(f"Source: {source_config.type} ({source_config.name})", prefix="→")
            logger.info(
                f"Destination: {destination_config.type} ({destination_config.name})", prefix="→"
            )

            # Get connector classes from registry
            source_map = get_source_connector_map()
            destination_map = get_destination_connector_map()

            SourceConnector = source_map.get(source_config.type)
            DestinationConnector = destination_map.get(destination_config.type)

            if not SourceConnector:
                raise ValueError(f"Kilde-konnektor av typen '{source_config.type}' ble ikke funnet.")
            if not DestinationConnector:
                raise ValueError(
                    f"Destinasjons-konnektor av typen '{destination_config.type}' ble ikke funnet."
                )

            # Initialize connectors
            source = SourceConnector(source_config)
            destination = DestinationConnector(destination_config)

            # Set mode AFTER creation, directly on the connector instance
            destination.mode = (
                resource.mode 
                or destination_config.mode 
                or ("incremental" if resource.incremental_column else "append")
            )
            # Add mode to tracker metadata
            tracker.metadata = {"mode": destination.mode}

            # Check if destination supports write_one (row-by-row) or only batch
            supports_write_one = hasattr(destination, "write_one") and callable(
                getattr(destination, "write_one")
            )

            # Counters
            batch_number = 0
            max_value_seen = last_value

            logger.info(f"Processing in batches (batch_size={batch_size})...", prefix="→")
            logger.separator()

            # Process in batches
            for batch in read_in_batches(source.read(final_query), batch_size=batch_size):
                batch_number += 1
                batch_start_time = time.time()
                successful_batch = []

                logger.info(f"🔍 DEBUG: Processing batch {batch_number} with {len(batch)} records")

                # Process each record in the batch
                for record in batch:
                    total_processed += 1

                    try:
                        if not isinstance(record, dict):
                            raise ValueError(f"Record is not a dictionary: {type(record)}")

                        if resource.incremental_column and resource.incremental_column in record:
                            try:
                                current_value = int(record[resource.incremental_column])
                                max_value_seen = max(max_value_seen, current_value)
                            except (ValueError, TypeError):
                                pass  # Skip if value is not convertible to int

                        if supports_write_one:
                            destination.write_one(record)
                            total_successful += 1
                        else:
                            successful_batch.append(record)

                    except Exception as e:
                        error_log.add_error(record, e, row_number=total_processed)
                        continue

                # Write batch if destination doesn't support write_one
                if not supports_write_one and successful_batch:
                    try:
                        logger.info(
                            f"🔍 DEBUG: Writing batch {batch_number} with {len(successful_batch)} records to destination"
                        )
                        destination.write(successful_batch)
                        total_successful += len(successful_batch)

                        batch_elapsed = time.time() - batch_start_time
                        logger.success(
                            f"Batch {batch_number}: wrote {len(successful_batch)} records",
                            timing=batch_elapsed,
                        )
                    except Exception as e:
                        logger.error(f"Batch {batch_number} write failed: {e}")
                        for record in successful_batch:
                            error_log.add_error(record, e, row_number=total_processed)
                elif supports_write_one:
                    logger.batch_progress(batch_number, len(batch), total_successful)

            logger.separator()

            # Finalize destination
            if hasattr(destination, "finalize") and callable(destination.finalize):
                logger.info("🔧 ENGINE: Calling destination.finalize()")
                destination.finalize()

            # Handle errors
            if error_log.has_errors():
                error_file = error_log.save()
                logger.warning(f"{error_log.error_count()} rows failed - see {error_file}")

            # Update state if successful records exist
            if total_successful > 0 and resource.incremental_column:
                if max_value_seen > last_value:
                    current_state[resource.name] = max_value_seen
                    save_state(current_state)
                    logger.info(
                        f"State updated: {resource.incremental_column}={max_value_seen}",
                        prefix="→",
                    )

            # Update tracker with final counts before exiting
            total_failed = error_log.error_count()
            tracker.records_read = total_processed
            tracker.records_written = total_successful
            tracker.records_failed = total_failed

            logger.complete_resource(total_processed, total_successful, total_failed)

        except Exception as e:
            logger.error(f"Resource '{resource.name}' failed with an unhandled exception: {e}")
            # The tracker will catch this exception and mark the run as 'failed'
            # Update final counts before re-raising
            total_failed = error_log.error_count()
            tracker.records_read = total_processed
            tracker.records_written = total_successful
            # Add records that might have failed in a batch write
            tracker.records_failed = total_failed
            raise