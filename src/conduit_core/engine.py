# src/conduit_core/engine.py

from .config import IngestConfig, Resource
from .connectors.dummy import DummySource, DummyDestination

# Et register for å finne riktig konnektor-klasse basert på navnet i YAML-filen.
# Denne vil vi utvide etter hvert som vi bygger flere konnektorer.
CONNECTOR_MAP = {
    "dummy_source": DummySource,
    "dummy_destination": DummyDestination,
}

def run_resource(resource: Resource, config: IngestConfig):
    """Kjører en enkelt dataflyt-ressurs."""
    print(f"--- 🚀 Kjører ressurs: [bold blue]{resource.name}[/bold blue] ---")

    # 1. Finn detaljene for kilde og destinasjon
    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(d for d in config.destinations if d.name == resource.destination)

    # 2. Finn riktig konnektor-klasse fra vårt register
    SourceConnector = CONNECTOR_MAP.get(source_config.type)
    DestinationConnector = CONNECTOR_MAP.get(destination_config.type)

    if not SourceConnector or not DestinationConnector:
        raise ValueError("Kunne ikke finne konnektor. Sjekk 'type' i ingest.yml.")

    # 3. Opprett instanser av konnektorene
    source = SourceConnector()
    destination = DestinationConnector()

    # 4. Kjør dataflyten: les fra kilden og skriv til destinasjonen
    records = list(source.read())
    destination.write(records)

    print(f"--- ✅ Ferdig med ressurs: [bold blue]{resource.name}[/bold blue] ---\n")