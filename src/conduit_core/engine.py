# src/conduit_core/engine.py

from rich import print
from .config import IngestConfig, Resource
from .state import load_state, save_state

# Import all the connectors
from .connectors.dummy import DummySource, DummyDestination
from .connectors.azuresql import AzureSqlSource
# from .connectors.databricks import DatabricksDestination # Paused
from .connectors.csv import CsvSource, CsvDestination

# --- Forbedret Konnektor-register ---
# Ett register for kilder
SOURCE_CONNECTOR_MAP = {
    "dummy_source": DummySource,
    "azuresql": AzureSqlSource,
    "csv": CsvSource,  # Ny kilde lagt til
}

# Ett register for destinasjoner
DESTINATION_CONNECTOR_MAP = {
    "dummy_destination": DummyDestination,
    "csv": CsvDestination,
    # "databricks": DatabricksDestination, # Paused
}

def run_resource(resource: Resource, config: IngestConfig):
    """Kjører en enkelt dataflyt-ressurs med state management."""
    print(f"--- 🚀 Kjører ressurs: [bold blue]{resource.name}[/bold blue] ---")

    # --- State Management Logikk ---
    current_state = load_state()
    # Bruker 0 som default for numerisk inkrementering, men None kan være bedre for datoer
    last_value = current_state.get(resource.name, 0)
    print(f"Siste kjente verdi for '{resource.name}': {last_value}")

    final_query = resource.query.replace(":last_value", str(last_value))
    
    # --- Konnektor-logikk (oppdatert) ---
    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(d for d in config.destinations if d.name == resource.destination)

    # Bruker de nye, spesifikke registrene
    SourceConnector = SOURCE_CONNECTOR_MAP.get(source_config.type)
    DestinationConnector = DESTINATION_CONNECTOR_MAP.get(destination_config.type)

    if not SourceConnector:
        raise ValueError(f"Kilde-konnektor av typen '{source_config.type}' ble ikke funnet.")
    if not DestinationConnector:
        raise ValueError(f"Destinasjons-konnektor av typen '{destination_config.type}' ble ikke funnet.")

    # --- Kjøring av dataflyt ---
    source = SourceConnector(source_config)
    destination = DestinationConnector(destination_config)

    records = list(source.read(final_query))
    
    if not records:
        print("Ingen nye rader funnet.")
        destination.write([])  # Sender en tom liste for å evt. lage en tom fil med overskrifter
    else:
        destination.write(records)
        
        if resource.incremental_column and resource.incremental_column in records[0]:
            new_max_value = max(r[resource.incremental_column] for r in records)
            current_state[resource.name] = new_max_value
            save_state(current_state)
            print(f"Ny state lagret for '{resource.name}': {new_max_value}")

    print(f"--- ✅ Ferdig med ressurs: [bold blue]{resource.name}[/bold blue] ---\n")