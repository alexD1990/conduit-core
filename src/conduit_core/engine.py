# src/conduit_core/engine.py
import logging
from rich import print
from .config import IngestConfig, Resource
from .state import load_state, save_state

# NEW: Import the registry instead of individual connectors
from .connectors.registry import get_source_connector_map, get_destination_connector_map


def run_resource(resource: Resource, config: IngestConfig):
    """Kjører en enkelt dataflyt-ressurs med state management."""
    logging.info((f"--- 🚀 Kjører ressurs: [bold blue]{resource.name}[/bold blue] ---"))
    
    current_state = load_state()
    last_value = current_state.get(resource.name, 0)
    logging.info(f"Siste kjente verdi for '{resource.name}': {last_value}")
    
    final_query = resource.query.replace(":last_value", str(last_value))
    
    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(d for d in config.destinations if d.name == resource.destination)
    
    # NEW: Get connector maps from registry
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
    
    records = list(source.read(final_query))
    
    if not records:
        logging.info("Ingen nye rader funnet.")
        destination.write([])
    else:
        destination.write(records)
        
        if resource.incremental_column and resource.incremental_column in records[0]:
            # This needs to handle non-integer types for timestamps in the future
            new_max_value = max(int(r[resource.incremental_column]) for r in records)
            current_state[resource.name] = new_max_value
            save_state(current_state)
            logging.info(f"Ny state lagret for '{resource.name}': {new_max_value}")
    
    logging.info(f"--- ✅ Ferdig med ressurs: [bold blue]{resource.name}[/bold blue] ---\n")