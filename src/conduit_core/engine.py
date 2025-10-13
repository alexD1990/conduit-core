# src/conduit_core/engine.py
import logging
from rich import print
from .config import IngestConfig, Resource
from .state import load_state, save_state
from .errors import ErrorLog
from .connectors.registry import get_source_connector_map, get_destination_connector_map

logger = logging.getLogger(__name__)


def run_resource(resource: Resource, config: IngestConfig):
    """Kjører en enkelt dataflyt-ressurs med error handling og state management."""
    logger.info(f"--- 🚀 Kjører ressurs: [bold blue]{resource.name}[/bold blue] ---")
    
    # Load state
    current_state = load_state()
    last_value = current_state.get(resource.name, 0)
    logger.info(f"Siste kjente verdi for '{resource.name}': {last_value}")
    
    # Prepare query
    final_query = resource.query.replace(":last_value", str(last_value))
    
    # Get configs
    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(d for d in config.destinations if d.name == resource.destination)
    
    # Get connector classes from registry
    source_map = get_source_connector_map()
    destination_map = get_destination_connector_map()
    
    SourceConnector = source_map.get(source_config.type)
    DestinationConnector = destination_map.get(destination_config.type)
    
    if not SourceConnector:
        raise ValueError(f"Kilde-konnektor av typen '{source_config.type}' ble ikke funnet.")
    if not DestinationConnector:
        raise ValueError(f"Destinasjons-konnektor av typen '{destination_config.type}' ble ikke funnet.")
    
    # Initialize connectors
    source = SourceConnector(source_config)
    destination = DestinationConnector(destination_config)
    
    # Initialize error log
    error_log = ErrorLog(resource.name)
    
    # Process records with error handling
    successful_records = []
    row_number = 0
    
    logger.info(f"Starter prosessering av records...")
    
    for record in source.read(final_query):
        row_number += 1
        try:
            # Validate record (basic check)
            if not isinstance(record, dict):
                raise ValueError(f"Record is not a dictionary: {type(record)}")
            
            # Check if destination has write_one method (new pattern)
            if hasattr(destination, 'write_one'):
                destination.write_one(record)
            else:
                # Fallback: collect for batch write
                successful_records.append(record)
            
        except Exception as e:
            # Log the error but continue processing
            error_log.add_error(record, e, row_number)
            continue
    
    # If destination doesn't have write_one, write batch now
    if successful_records and not hasattr(destination, 'write_one'):
        try:
            destination.write(successful_records)
        except Exception as e:
            logger.error(f"Batch write failed: {e}")
            # Log all records as failed
            for i, record in enumerate(successful_records, start=1):
                error_log.add_error(record, e, row_number=i)
            successful_records = []
    
    # Summary
    total_processed = row_number
    total_successful = total_processed - error_log.error_count()
    
    logger.info(f"✅ Prosessert {total_processed} rader totalt")
    logger.info(f"✅ {total_successful} rader vellykket")
    
    if error_log.has_errors():
        error_file = error_log.save()
        logger.warning(f"⚠️  {error_log.error_count()} rader feilet")
        print(f"⚠️  [yellow]{error_log.error_count()} rows failed. See {error_file}[/yellow]")
    
    # Update state if successful records exist
    if successful_records or (hasattr(destination, 'write_one') and total_successful > 0):
        if resource.incremental_column:
            # Determine what records to check for max value
            records_to_check = successful_records if successful_records else list(source.read(final_query))
            
            if records_to_check and resource.incremental_column in records_to_check[0]:
                try:
                    new_max_value = max(int(r[resource.incremental_column]) for r in records_to_check)
                    current_state[resource.name] = new_max_value
                    save_state(current_state)
                    logger.info(f"Ny state lagret for '{resource.name}': {new_max_value}")
                except (ValueError, KeyError) as e:
                    logger.warning(f"Kunne ikke oppdatere state: {e}")
    
    logger.info(f"--- ✅ Ferdig med ressurs: [bold blue]{resource.name}[/bold blue] ---\n")