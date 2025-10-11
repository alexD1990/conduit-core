# src/conduit_core/connectors/parquet_connector.py

import logging
import os
import pandas as pd
from typing import Iterable, Dict, Any
from .base import BaseSource, BaseDestination
from ..config import Source, Destination
from ..validators import RecordValidator

logger = logging.getLogger(__name__)


class ParquetSource(BaseSource):
    """Reads data from a Parquet file."""

    def __init__(self, config: Source):
        if not config.path:
            raise ValueError("A 'path' (file path) must be defined for ParquetSource.")
        self.filepath = config.path

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        """Reads all records from Parquet file and yields them as dictionaries."""
        logger.info(f"Reading from Parquet file: {self.filepath}")

        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"Parquet file not found: {self.filepath}")

        df = pd.read_parquet(self.filepath)
        
        for _, row in df.iterrows():
            yield row.to_dict()

        logger.info(f"✅ Finished reading {len(df)} rows from {self.filepath}")
    
    def test_connection(self) -> bool:
        """Test that Parquet file exists and can be read"""
        if not os.path.exists(self.filepath):
            from ..errors import ConnectionError as ConduitConnectionError
            raise ConduitConnectionError(
                f"Parquet file not found: {self.filepath}",
                suggestions=[
                    "Check that the file path is correct",
                    "Check that the file exists",
                    f"Searched for: {os.path.abspath(self.filepath)}"
                ]
            )
        logger.info(f"✅ Parquet source connection test successful: {self.filepath}")
        return True


class ParquetDestination(BaseDestination):
    """Writes data to a Parquet file."""

    def __init__(self, config: Destination):
        if not config.path:
            raise ValueError("A 'path' (file path) must be defined for ParquetDestination.")
        self.filepath = config.path

    def write(self, records: Iterable[Dict[str, Any]]):
        records = list(records)
        if not records:
            logger.info("No rows to write to Parquet.")
            return

        output_dir = os.path.dirname(self.filepath)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        logger.info(f"Writing {len(records)} rows to Parquet file: {self.filepath}")

        # Validate and sanitize records
        validator = RecordValidator(skip_invalid=True)
        cleaned_records = []
        for i, record in enumerate(records, start=1):
            validated = validator.validate_record(record, i)
            if validated:
                cleaned_records.append(validated)

        df = pd.DataFrame(cleaned_records)
        df.to_parquet(self.filepath, index=False)

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
        logger.info(f"✅ Parquet destination connection test successful: {self.filepath}")
        return True