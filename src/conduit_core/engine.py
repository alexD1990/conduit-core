# src/conduit_core/engine.py

from rich import print
from .config import IngestConfig, Resource
from .state import load_state, save_state

# Import all the connectors
from .connectors.dummy import DummySource, DummyDestination
from .connectors.azuresql import AzureSqlSource, AzureSqlDestination
# from .connectors.databricks import DatabricksDestination # Paused
from .connectors.csv import CsvSource, CsvDestination

# --- Connector Maps ---
SOURCE_CONNECTOR_MAP = {
    "dummy_source": DummySource,
    "azuresql": AzureSqlSource,
    "csv": CsvSource,
}

DESTINATION_CONNECTOR_MAP = {
    "dummy_destination": DummyDestination,
    "csv": CsvDestination,
    "azuresql": AzureSqlDestination, # Ny destinasjon lagt til
    # "databricks": DatabricksDestination, # Paused
}

def run_resource(resource: Resource, config: IngestConfig):
    """Kjører en enkelt dataflyt-ressurs med state management."""
    print(f"--- 🚀 Kjører ressurs: [bold blue]{resource.name}[/bold blue] ---")

    current_state = load_state()
    last_value = current_state.get(resource.name, 0)
    print(f"Siste kjente verdi for '{resource.name}': {last_value}")

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
        print("Ingen nye rader funnet.")
        destination.write([])
    else:
        destination.write(records)
        
        if resource.incremental_column and resource.incremental_column in records[0]:
            # This needs to handle non-integer types for timestamps in the future
            new_max_value = max(int(r[resource.incremental_column]) for r in records)
            current_state[resource.name] = new_max_value
            save_state(current_state)
            print(f"Ny state lagret for '{resource.name}': {new_max_value}")

    print(f"--- ✅ Ferdig med ressurs: [bold blue]{resource.name}[/bold blue] ---\n")