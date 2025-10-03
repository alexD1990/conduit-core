# src/conduit_core/engine.py

from rich import print
from .config import IngestConfig, Resource
from .state import load_state, save_state  # <-- Ny import
from .connectors.dummy import DummySource, DummyDestination
from .connectors.azuresql import AzureSqlSource
# from .connectors.databricks import DatabricksDestination # Kommentert ut
from .connectors.csv import CsvDestination

CONNECTOR_MAP = {
    "dummy_source": DummySource,
    "dummy_destination": DummyDestination,
    "azuresql": AzureSqlSource,
    # "databricks": DatabricksDestination,
    "csv": CsvDestination,
}

def run_resource(resource: Resource, config: IngestConfig):
    """Kjører en enkelt dataflyt-ressurs med state management."""
    print(f"--- 🚀 Kjører ressurs: [bold blue]{resource.name}[/bold blue] ---")

    # --- START NY LOGIKK ---
    # 1. Last inn state
    current_state = load_state()
    last_value = current_state.get(resource.name, 0) # Hent siste verdi, default til 0
    print(f"Siste kjente verdi for '{resource.name}': {last_value}")

    # 2. Forbered spørringen
    final_query = resource.query.replace(":last_value", str(last_value))
    # --- SLUTT NY LOGIKK ---

    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(d for d in config.destinations if d.name == resource.destination)

    SourceConnector = CONNECTOR_MAP.get(source_config.type)
    DestinationConnector = CONNECTOR_MAP.get(destination_config.type)

    source = SourceConnector(source_config)
    destination = DestinationConnector(destination_config)

    records = list(source.read(final_query)) # Bruker den nye spørringen

    if not records:
        print("Ingen nye rader funnet.")
        destination.write(records) # Skriver en tom fil
    else:
        destination.write(records)

        # --- START NY LOGIKK ---
        # 3. Finn ny maksverdi og lagre state
        if resource.incremental_column:
            new_max_value = max(r[resource.incremental_column] for r in records)
            current_state[resource.name] = new_max_value
            save_state(current_state)
            print(f"Ny state lagret for '{resource.name}': {new_max_value}")
        # --- SLUTT NY LOGIKK ---

    print(f"--- ✅ Ferdig med ressurs: [bold blue]{resource.name}[/bold blue] ---\n")