# src/conduit_core/engine.py
import time
from rich import print
from .config import IngestConfig, Resource
from .state import load_state, save_state
from .errors import ErrorLog
from .batch import read_in_batches
from .logging_utils import ConduitLogger
from .connectors.registry import get_source_connector_map, get_destination_connector_map


def run_resource(resource: Resource, config: IngestConfig, batch_size: int = 1000):
    """Kjører en enkelt dataflyt-ressurs med batch processing, error handling og state management."""
    
    # Initialize logger
    logger = ConduitLogger(resource.name)
    logger.start_resource()
    
    resource_start_time = time.time()
    
    # Load state
    current_state = load_state()
    last_value = current_state.get(resource.name, 0)
    logger.info(f"Last known value: {last_value}", prefix="→")
    
    # Prepare query
    final_query = resource.query.replace(":last_value", str(last_value))
    
    # Get configs
    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(d for d in config.destinations if d.name == resource.destination)
    
    logger.info(f"Source: {source_config.type} ({source_config.name})", prefix="→")
    logger.info(f"Destination: {destination_config.type} ({destination_config.name})", prefix="→")
    
    # Get connector classes from registry
    source_map = get_source_connector_map()
    destination_map = get_destination_connector_map()
    
    SourceConnector = source_map.get(source_config.type)
    DestinationConnector = destination_map.get(destination_config.type)
    
    if not SourceConnector:
        logger.error(f"Source connector type '{source_config.type}' not found")
        raise ValueError(f"Kilde-konnektor av typen '{source_config.type}' ble ikke funnet.")
    if not DestinationConnector:
        logger.error(f"Destination connector type '{destination_config.type}' not found")
        raise ValueError(f"Destinasjons-konnektor av typen '{destination_config.type}' ble ikke funnet.")
    
    # Initialize connectors
    source = SourceConnector(source_config)
    destination = DestinationConnector(destination_config)
    
    # Initialize error log
    error_log = ErrorLog(resource.name)
    
    # Check if destination supports write_one (row-by-row) or only batch
    supports_write_one = hasattr(destination, 'write_one') and callable(getattr(destination, 'write_one'))
    
    # Counters
    total_processed = 0
    total_successful = 0
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
                # Validate record (basic check)
                if not isinstance(record, dict):
                    raise ValueError(f"Record is not a dictionary: {type(record)}")
                
                # Track max value for incremental column
                if resource.incremental_column and resource.incremental_column in record:
                    try:
                        current_value = int(record[resource.incremental_column])
                        max_value_seen = max(max_value_seen, current_value)
                    except (ValueError, TypeError):
                        pass  # Skip if value is not convertible to int
                
                # If destination supports write_one, write immediately
                if supports_write_one:
                    destination.write_one(record)
                    total_successful += 1
                else:
                    # Otherwise, collect for batch write
                    successful_batch.append(record)
                
            except Exception as e:
                # Log the error but continue processing
                error_log.add_error(record, e, row_number=total_processed)
                continue
        
        # Write batch if destination doesn't support write_one
        if not supports_write_one and successful_batch:
            try:
                logger.info(f"🔍 DEBUG: Writing batch {batch_number} with {len(successful_batch)} records to destination")
                destination.write(successful_batch)
                total_successful += len(successful_batch)
                
                batch_elapsed = time.time() - batch_start_time
                logger.success(
                    f"Batch {batch_number}: wrote {len(successful_batch)} records",
                    timing=batch_elapsed
                )
            except Exception as e:
                logger.error(f"Batch {batch_number} write failed: {e}")
                # Log all records in failed batch as errors
                for record in successful_batch:
                    error_log.add_error(record, e, row_number=total_processed)
        elif supports_write_one:
            batch_elapsed = time.time() - batch_start_time
            logger.batch_progress(batch_number, len(batch), total_successful)
    
    logger.separator()
    
    # NEW: Finalize destination (for akkumulerende destinations som S3)
    if hasattr(destination, 'finalize') and callable(destination.finalize):
        logger.info("Finalizing destination...", prefix="→")
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
            logger.info(f"State updated: {resource.incremental_column}={max_value_seen}", prefix="→")
    
    # Complete
    total_failed = error_log.error_count()
    logger.complete_resource(total_processed, total_successful, total_failed)