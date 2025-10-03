# src/conduit_core/engine.py

from rich import print
from .config import IngestConfig, Resource
from .connectors.dummy import DummySource, DummyDestination
from .connectors.azuresql import AzureSqlSource
from .connectors.databricks import DatabricksDestination

# Register for å finne riktig konnektor-klasse
CONNECTOR_MAP = {
    "dummy_source": DummySource,
    "dummy_destination": DummyDestination,
    "azuresql": AzureSqlSource,
    "databricks": DatabricksDestination, # Ny destinasjon lagt til
}

def run_resource(resource: Resource, config: IngestConfig):
    """Kjører en enkelt dataflyt-ressurs."""
    print(f"--- 🚀 Kjører ressurs: [bold blue]{resource.name}[/bold blue] ---")

    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(d for d in config.destinations if d.name == resource.destination)

    SourceConnector = CONNECTOR_MAP.get(source_config.type)
    DestinationConnector = CONNECTOR_MAP.get(destination_config.type)

    if not SourceConnector or not DestinationConnector:
        raise ValueError("Kunne ikke finne konnektor. Sjekk 'type' i ingest.yml.")

    # Oppdatering: Sender nå med config til begge konnektorene
    source = SourceConnector(source_config)
    destination = DestinationConnector(destination_config)

    records = list(source.read(resource.query))
    destination.write(records)

    print(f"--- ✅ Ferdig med ressurs: [bold blue]{resource.name}[/bold blue] ---\n")