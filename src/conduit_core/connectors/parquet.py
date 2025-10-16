"""Parquet connector for reading and writing Parquet files."""

from pathlib import Path
from typing import Any, Dict, Iterable

import pyarrow.parquet as pq
import pyarrow as pa

from conduit_core.connectors.base import BaseSource, BaseDestination

class ParquetSource(BaseSource):
    """Read data from Parquet files."""

    def __init__(self, config: Dict[str, Any]):
        self.file_path = Path(config["file_path"])
        self.batch_size = config.get("batch_size", 10000)
        
        if not self.file_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {self.file_path}")

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        """Read records from Parquet file in batches."""
        parquet_file = pq.ParquetFile(self.file_path)
        
        for batch in parquet_file.iter_batches(batch_size=self.batch_size):
            records = batch.to_pylist()
            for record in records:
                yield record


class ParquetDestination(BaseDestination):
    """Write data to Parquet files."""

    def __init__(self, config: Dict[str, Any]):
        self.file_path = Path(config["file_path"])
        self.compression = config.get("compression", "snappy")
        self._buffer = []

    def write(self, records: Iterable[Dict[str, Any]]) -> None:
        """Write records to Parquet file."""
        self._buffer.extend(list(records))

    def finalize(self) -> None:
        """Write all buffered records to Parquet file."""
        if not self._buffer:
            return

        table = pa.Table.from_pylist(self._buffer)
        
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        
        pq.write_table(
            table,
            self.file_path,
            compression=self.compression,
        )
        
        self._buffer.clear()