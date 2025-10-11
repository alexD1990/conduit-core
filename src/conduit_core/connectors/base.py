# src/conduit_core/connectors/base.py

from abc import ABC, abstractmethod
from typing import Iterable, Dict, Any
import logging

logger = logging.getLogger(__name__)


class BaseSource(ABC):
    """Base class for all data source connectors."""

    @abstractmethod
    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        """
        Read data from the source and return a stream of records.
        Each record is a dictionary.
        """
        pass
    
    def test_connection(self) -> bool:
        """
        Test if connection to source is working.
        
        Returns:
            True if connection successful, False otherwise
        
        Raises:
            ConnectionError with helpful message if connection fails
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement test_connection()"
        )


class BaseDestination(ABC):
    """Base class for all destination connectors."""

    @abstractmethod
    def write(self, records: Iterable[Dict[str, Any]]):
        """
        Receive a stream of records and write them to the destination.
        """
        pass
    
    def test_connection(self) -> bool:
        """
        Test if connection to destination is working.
        
        Returns:
            True if connection successful, False otherwise
        
        Raises:
            ConnectionError with helpful message if connection fails
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement test_connection()"
        )