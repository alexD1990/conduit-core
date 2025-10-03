# src/conduit_core/engine.py

from rich import print
from .config import IngestConfig, Resource
from .connectors.dummy import DummySource, DummyDestination
# Importer den nye klassen
from .connectors.azuresql import AzureSqlSource

CONNECTOR_MAP = {
    "dummy_source": DummySource,
    "dummy_destination": DummyDestination,
    # Legg til den nye konnektoren i registeret
    "azuresql": AzureSqlSource,
}

def run_resource(resource: Resource, config: IngestConfig):
    print(f"--- 🚀 Kjører ressurs: [bold blue]{resource.name}[/bold blue] ---")
    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(d for d in config.destinations if d.name == resource.destination)

    SourceConnector = CONNECTOR_MAP.get(source_config.type)
    DestinationConnector = CONNECTOR_MAP.get(destination_config.type)

    # Send med konfigurasjonen til konnektoren
    source = SourceConnector(source_config)
    destination = DestinationConnector() # Destination trenger ikke config ennå

    # Send med spørringen til read-metoden
    records = list(source.read(resource.query))
    destination.write(records)
    print(f"--- ✅ Ferdig med ressurs: [bold blue]{resource.name}[/bold blue] ---\n")