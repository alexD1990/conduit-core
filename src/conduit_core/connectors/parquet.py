"""Parquet connector for reading and writing Parquet files."""

from pathlib import Path
from typing import Any, Dict, Iterable

import pyarrow.parquet as pq
import pyarrow as pa

from .base import BaseSource, BaseDestination
from ..config import Source as SourceConfig
from ..config import Destination as DestinationConfig
from ..errors import ConnectionError


class ParquetSource(BaseSource):
    """Read data from Parquet files."""

    def __init__(self, config: Any):
        # --- FIX IS HERE: Handle both Pydantic object and dict ---
        path = None
        if hasattr(config, 'path'):  # Pydantic object from application
            path = config.path
            self.batch_size = getattr(config, "batch_size", 10000)
        elif isinstance(config, dict):  # Dictionary from tests
            path = config.get("file_path") or config.get("path")
            self.batch_size = config.get("batch_size", 10000)
        
        if not path:
            raise ValueError("ParquetSource requires a 'path' or 'file_path'.")
        
        self.file_path = Path(path)

    def test_connection(self) -> bool:
        """Test that Parquet file exists and is readable."""
        if not self.file_path.exists():
            raise ConnectionError(
                f"Parquet file not found: {self.file_path}\n\n"
                f"Suggestions:\n"
                f"  • Check the file path is correct.\n"
                f"  • Verify the file exists and has read permissions."
            )
        try:
            pq.ParquetFile(self.file_path)
        except Exception as e:
            raise ConnectionError(
                f"Cannot read Parquet file: {e}\n\n"
                f"Suggestions:\n"
                f"  • The file may be corrupted or not a valid Parquet file."
            )
        return True

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        """Read records from Parquet file in batches."""
        parquet_file = pq.ParquetFile(self.file_path)
        for batch in parquet_file.iter_batches(batch_size=self.batch_size):
            yield from batch.to_pylist()


class ParquetDestination(BaseDestination):
    """Write data to Parquet files."""

    def __init__(self, config: Any):
        # --- FIX IS HERE: Handle both Pydantic object and dict ---
        path = None
        if hasattr(config, 'path'): # Pydantic object from application
            path = config.path
            self.compression = getattr(config, "compression", "snappy")
        elif isinstance(config, dict): # Dictionary from tests
            path = config.get("file_path") or config.get("path")
            self.compression = config.get("compression", "snappy")

        if not path:
            raise ValueError("ParquetDestination requires a 'path' or 'file_path'.")
        
        self.file_path = Path(path)
        self.accumulated_records = []

    def test_connection(self) -> bool:
        """Test that output directory is writable."""
        output_dir = self.file_path.parent
        if not output_dir.exists():
            try:
                output_dir.mkdir(parents=True, exist_ok=True)
            except PermissionError:
                raise ConnectionError(
                    f"Cannot create output directory: {output_dir}\n\n"
                    f"Suggestions:\n"
                    f"  • Check you have write permissions for the parent directory."
                ) from None
        return True

    def write(self, records: Iterable[Dict[str, Any]]) -> None:
        """Buffer records in memory."""
        self.accumulated_records.extend(list(records))

    def finalize(self) -> None:
        """Write all buffered records to Parquet file."""
        if not self.accumulated_records:
            return

        table = pa.Table.from_pylist(self.accumulated_records)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            pq.write_table(
                table,
                self.file_path,
                compression=self.compression,
            )
        finally:
            self.accumulated_records.clear()