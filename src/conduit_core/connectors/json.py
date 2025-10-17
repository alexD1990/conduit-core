# src/conduit_core/connectors/json.py

import json
import logging
from pathlib import Path
from typing import Iterable, Dict, Any

from .base import BaseSource, BaseDestination
from ..config import Source as SourceConfig
from ..config import Destination as DestinationConfig

logger = logging.getLogger(__name__)


class JsonSource(BaseSource):
    """Reads data from a JSON file."""

    def __init__(self, config: SourceConfig):
        if not config.path:
            raise ValueError("A 'path' (file path) must be defined for JsonSource.")
        self.filepath = Path(config.path)

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        """Reads all records from JSON file and yields them as dictionaries."""
        logger.info(f"Reading from JSON file: {self.filepath}")

        if not self.filepath.exists():
            raise FileNotFoundError(f"JSON file not found: {self.filepath}")

        with self.filepath.open(mode='r', encoding='utf-8') as infile:
            data = json.load(infile)

            # Handle both array of objects and single object
            if isinstance(data, list):
                for record in data:
                    yield record
            elif isinstance(data, dict):
                yield data
            else:
                logger.warning(f"JSON content in {self.filepath} is not a list or a dictionary.")

        logger.info(f"✅ Finished reading from {self.filepath}")


class JsonDestination(BaseDestination):
    """Writes data to a JSON file using an atomic write pattern."""

    def __init__(self, config: DestinationConfig):
        if not config.path:
            raise ValueError("A 'path' (file path) must be defined for JsonDestination.")
        self.filepath = Path(config.path)
        self.temp_filepath = self.filepath.with_suffix(f"{self.filepath.suffix}.tmp")
        self.accumulated_records = []

    def write(self, records: Iterable[Dict[str, Any]]):
        """Accumulates records in memory. The actual write happens in finalize()."""
        self.accumulated_records.extend(list(records))
        logger.info(f"Accumulated {len(list(records))} records for JSON destination.")

    def finalize(self):
        """Writes all accumulated records to a JSON file atomically."""
        if not self.accumulated_records:
            logger.info("No records to write to JSON.")
            return

        # Ensure the output directory exists
        self.filepath.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Writing {len(self.accumulated_records)} records to JSON file: {self.filepath}")

        try:
            # 1. Write to a temporary file
            with self.temp_filepath.open('w', encoding='utf-8') as f:
                json.dump(self.accumulated_records, f, indent=2, ensure_ascii=False, default=str)

            # 2. Atomically replace the final file with the temporary one
            self.temp_filepath.replace(self.filepath)
            logger.info(f"✅ Successfully wrote to {self.filepath}")

        except Exception as e:
            # Clean up the temporary file on error
            if self.temp_filepath.exists():
                self.temp_filepath.unlink()
            logger.error(f"❌ Failed to write to JSON file: {e}")
            raise
        finally:
            # Clear the buffer to release memory
            self.accumulated_records.clear()