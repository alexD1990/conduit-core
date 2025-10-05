# src/conduit_core/connectors/dummy.py

from typing import Iterable, Dict, Any
from rich import print
from .base import BaseSource, BaseDestination

class DummySource(BaseSource):
    """En test-kilde som bare genererer tre rader med fiktiv data."""

    def __init__(self, *args, **kwargs):
        # Vi ignorerer all konfigurasjon for denne enkle konnektoren
        pass

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        print("[DummySource]: Genererer data...")
        yield {"id": 1, "name": "Alice", "email": "alice@example.com"}
        yield {"id": 2, "name": "Bob", "email": "bob@example.com"}
        yield {"id": 3, "name": "Charlie", "email": "charlie@example.com"}
        print("[DummySource]: Ferdig med å generere data.")

class DummyDestination(BaseDestination):
    """En test-destinasjon som bare skriver mottatt data til terminalen."""

    def __init__(self, *args, **kwargs):
        # Vi ignorerer all konfigurasjon for denne enkle konnektoren
        pass

    def write(self, records: Iterable[Dict[str, Any]]):
        print("[DummyDestination]: Mottok data for skriving:")
        for record in records:
            print(record)
        print("[DummyDestination]: Ferdig med å skrive.")