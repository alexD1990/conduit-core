# src/conduit_core/connectors/base.py

from abc import ABC, abstractmethod
from typing import Iterable, Dict, Any, Optional

class BaseSource(ABC):
    """En 'kontrakt' for alle datakilde-konnektorer."""

    @abstractmethod
    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        """
        Leser data fra kilden og returnerer en strøm av rader.
        Hver rad er en dictionary.
        """
        pass

    def test_connection(self) -> bool:
        """
        Test connection to source.
        
        Returns:
            bool: True if connection successful
        
        Raises:
            ConnectionError: If connection fails, with helpful message
        """
        # Default implementation - subclasses should override
        return True

    def estimate_total_records(self) -> Optional[int]:
        """
        Estimate total number of records (for progress bar).
        
        Returns:
            int: Estimated record count, or None if unknown
        """
        return None  # Default: unknown

class BaseDestination(ABC):
    """En 'kontrakt' for alle destinasjons-konnektorer."""

    @abstractmethod
    def write(self, records: Iterable[Dict[str, Any]]):
        """
        Mottar en strøm av rader og skriver dem til destinasjonen.
        Brukes for batch-skriving.
        """
        pass
    
    def write_one(self, record: Dict[str, Any]):
        """
        Skriver én enkelt record til destinasjonen.
        Dette er en optional metode for connectors som støtter single-record writes.
        
        Default implementasjon: kaller write() med en liste av én record.
        Connectors kan override denne for mer effektiv single-record skriving.
        """
        self.write([record])

    def finalize(self):
        """
        Optional cleanup/finalization method.
        Kalles når alle batches er prosessert.
        """
        pass
        
    def test_connection(self) -> bool:
        """
        Test connection to destination.
        
        Returns:
            bool: True if connection successful
        
        Raises:
            ConnectionError: If connection fails, with helpful message
        """
        # Default implementation - subclasses should override
        return True