from abc import ABC, abstractmethod
from typing import Iterable, Dict, Any

class BaseSource(ABC):
    """En 'kontrakt' for alle datakilde-konnektorer."""
    connector_type: str = ""

    @abstractmethod
    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        pass

class BaseDestination(ABC):
    """En 'kontrakt' for alle destinasjons-konnektorer."""
    connector_type: str = ""

    @abstractmethod
    def write(self, records: Iterable[Dict[str, Any]]):
        pass