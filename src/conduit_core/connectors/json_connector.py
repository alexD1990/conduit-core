# src/conduit_core/connectors/json_connector.py

import json
import logging
import os
from typing import Iterable, Dict, Any
from .base import BaseSource, BaseDestination
from ..config import Source, Destination
from ..validators import RecordValidator

logger = logging.getLogger(__name__)


class JsonSource(BaseSource):
    """Reads data from a JSON file."""

    def __init__(self, config: Source):
        if not config.path:
            raise ValueError("A 'path' (file path) must be defined for JsonSource.")
        self.filepath = config.path

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        """Reads all records from JSON file and yields them as dictionaries."""
        logger.info(f"Reading from JSON file: {self.filepath}")

        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"JSON file not found: {self.filepath}")

        with open(self.filepath, mode='r', encoding='utf-8') as infile:
            data = json.load(infile)
            
            # Handle both array of objects and single object
            if isinstance(data, list):
                for record in data:
                    yield record
            else:
                yield data

        logger.info(f"✅ Finished reading from {self.filepath}")
    
    def test_connection(self) -> bool:
        """Test that JSON file exists and can be read"""
        if not os.path.exists(self.filepath):
            from ..errors import ConnectionError as ConduitConnectionError
            raise ConduitConnectionError(
                f"JSON file not found: {self.filepath}",
                suggestions=[
                    "Check that the file path is correct",
                    "Check that the file exists",
                    f"Searched for: {os.path.abspath(self.filepath)}"
                ]
            )
        logger.info(f"✅ JSON source connection test successful: {self.filepath}")
        return True


class JsonDestination(BaseDestination):
    """Writes data to a JSON file."""

    def __init__(self, config: Destination):
        if not config.path:
            raise ValueError("A 'path' (file path) must be defined for JsonDestination.")
        self.filepath = config.path

    def write(self, records: Iterable[Dict[str, Any]]):
        records = list(records)
        if not records:
            logger.info("No rows to write to JSON.")
            return

        output_dir = os.path.dirname(self.filepath)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        logger.info(f"Writing {len(records)} rows to JSON file: {self.filepath}")

        # Validate and sanitize records
        validator = RecordValidator(skip_invalid=True)
        cleaned_records = []
        for i, record in enumerate(records, start=1):
            validated = validator.validate_record(record, i)
            if validated:
                cleaned_records.append(validated)

        with open(self.filepath, 'w', encoding='utf-8') as output_file:
            json.dump(cleaned_records, output_file, indent=2, ensure_ascii=False, default=str)

        logger.info(f"✅ Successfully wrote to {self.filepath}")
    
    def test_connection(self) -> bool:
        """Test that output directory exists or can be created"""
        output_dir = os.path.dirname(self.filepath)
        if output_dir and not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir, exist_ok=True)
            except Exception as e:
                from ..errors import ConnectionError as ConduitConnectionError
                raise ConduitConnectionError(
                    f"Cannot create output directory: {output_dir}",
                    suggestions=[
                        "Check that you have write permissions to the directory",
                        f"Error: {e}"
                    ]
                )
        logger.info(f"✅ JSON destination connection test successful: {self.filepath}")
        return True