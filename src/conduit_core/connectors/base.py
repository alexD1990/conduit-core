# src/conduit_core/connectors/base.py

from abc import ABC, abstractmethod
from typing import Iterable, Dict, Any

class BaseSource(ABC):
    """En 'kontrakt' for alle datakilde-konnektorer."""

    @abstractmethod
    def read(self) -> Iterable[Dict[str, Any]]:
        """
        Leser data fra kilden og returnerer en strøm av rader.
        Hver rad er en dictionary.
        """
        pass

class BaseDestination(ABC):
    """En 'kontrakt' for alle destinasjons-konnektorer."""

    @abstractmethod
    def write(self, records: Iterable[Dict[str, Any]]):
        """
        Mottar en strøm av rader og skriver dem til destinasjonen.
        """
        pass