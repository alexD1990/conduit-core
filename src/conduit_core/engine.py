# src/conduit_core/engine.py

import logging
import pkgutil
import inspect
from pathlib import Path
from . import connectors # Importerer hele connectors-pakken
from .config import IngestConfig, Resource
from .state import load_state, save_state
from .connectors.base import BaseSource, BaseDestination

# --- Automatisk "Plugin Discovery" ---

def discover_connectors():
    """Finner og laster alle konnektor-klasser fra connectors-mappen."""
    source_map = {}
    destination_map = {}
    
    # Går gjennom alle filene i 'connectors'-mappen
    for _, module_name, _ in pkgutil.iter_modules(connectors.__path__, f"{connectors.__name__}."):
        module = __import__(module_name, fromlist=["*"])
        
        # Går gjennom alle klasser i hver fil
        for name, obj in inspect.getmembers(module, inspect.isclass):
            # Sjekker om klassen er en Source, men ikke BaseSource selv
            if issubclass(obj, BaseSource) and obj is not BaseSource:
                if obj.connector_type: # Sjekker at den har et "navneskilt"
                    source_map[obj.connector_type] = obj
            
            # Sjekker om klassen er en Destination, men ikke BaseDestination selv
            if issubclass(obj, BaseDestination) and obj is not BaseDestination:
                if obj.connector_type: # Sjekker at den har et "navneskilt"
                    destination_map[obj.connector_type] = obj
                    
    return source_map, destination_map

# Kjører discovery-funksjonen én gang og logger resultatet
SOURCE_CONNECTOR_MAP, DESTINATION_CONNECTOR_MAP = discover_connectors()
logging.info(f"Lastet inn {len(SOURCE_CONNECTOR_MAP)} kilder og {len(DESTINATION_CONNECTOR_MAP)} destinasjoner.")

# --- Kjøremotoren (resten av filen er uendret) ---

def run_resource(resource: Resource, config: IngestConfig):
    """Kjører en enkelt dataflyt-ressurs."""
    logging.info(f"--- 🚀 Kjører ressurs: {resource.name} ---")

    current_state = load_state()
    last_value = current_state.get(resource.name, 0)
    logging.info(f"Siste kjente verdi for '{resource.name}': {last_value}")

    final_query = resource.query.replace(":last_value", str(last_value))
    
    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(d for d in config.destinations if d.name == resource.destination)

    SourceConnector = SOURCE_CONNECTOR_MAP.get(source_config.type)
    DestinationConnector = DESTINATION_CONNECTOR_MAP.get(destination_config.type)

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
            new_max_value = max(int(r[resource.incremental_column]) for r in records)
            current_state[resource.name] = new_max_value
            save_state(current_state)
            logging.info(f"Ny state lagret for '{resource.name}': {new_max_value}")

    logging.info(f"--- ✅ Ferdig med ressurs: {resource.name} ---\n")