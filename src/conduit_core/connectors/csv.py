# src/conduit_core/connectors/csv.py

import csv
import logging
import os
from pathlib import Path
from typing import Iterable, Dict, Any, Optional

from .base import BaseSource, BaseDestination
from ..config import Destination as DestinationConfig, Source as SourceConfig
from ..errors import ConnectionError

# A set of common string representations for NULL/NA values.
NA_VALUES = {'', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan',
             '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null'}

class CsvDestination(BaseDestination):
    """Skriver data til en lokal CSV-fil med atomic writes."""
    def __init__(self, config: DestinationConfig):
        if not config.path: raise ValueError("CsvDestination requires a 'path'.")
        self.filepath = Path(config.path)
        self.temp_filepath = self.filepath.with_suffix(f"{self.filepath.suffix}.tmp")
        self.accumulated_records = []

    def write(self, records: Iterable[Dict[str, Any]]):
        self.accumulated_records.extend(list(records))

    def finalize(self):
        if not self.accumulated_records: return
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        headers = self.accumulated_records[0].keys()
        try:
            with self.temp_filepath.open('w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(self.accumulated_records)
            self.temp_filepath.replace(self.filepath)
        except Exception as e:
            if self.temp_filepath.exists(): self.temp_filepath.unlink()
            raise IOError(f"Failed to write CSV file: {e}") from e
        finally:
            self.accumulated_records.clear()

    def test_connection(self) -> bool:
        output_dir = self.filepath.parent
        if not output_dir.exists():
            try: output_dir.mkdir(parents=True, exist_ok=True)
            except PermissionError: raise ConnectionError(f"Cannot create output directory: {output_dir}")
        
        test_file = output_dir / ".conduit_test_write"
        try:
            test_file.touch()
            test_file.unlink()
        except PermissionError:
            raise ConnectionError(f"Cannot write to directory: {output_dir}")
        return True

class CsvSource(BaseSource):
    """Leser data fra en lokal CSV-fil."""
    def __init__(self, config: SourceConfig):
        if not config.path: raise ValueError("CsvSource requires a 'path'.")
        self.filepath = Path(config.path)

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        if not self.filepath.exists():
            raise FileNotFoundError(f"CSV file not found: {self.filepath}")
        
        # --- FIX: Try common encodings, but don't use the unreliable Sniffer ---
        encodings_to_try = ['utf-8', 'latin-1', 'utf-8-sig']
        for encoding in encodings_to_try:
            try:
                with self.filepath.open(mode='r', encoding=encoding, newline='') as infile:
                    # Create a reader and process all rows in a memory-safe way
                    reader = csv.DictReader(infile)
                    for row in reader:
                        # Convert common NULL values to Python's None
                        yield {key: None if value in NA_VALUES else value for key, value in row.items()}
                return # Stop after successful read
            except UnicodeDecodeError:
                continue # Try next encoding
        raise ConnectionError(f"Could not decode file {self.filepath} with tested encodings.")

    def test_connection(self) -> bool:
        if not self.filepath.exists():
            raise ConnectionError(
                f"CSV file not found: {self.filepath}\n\n"
                f"Suggestions:\n  • Check file path is correct."
            )
        if not os.access(self.filepath, os.R_OK):
            raise ConnectionError(f"Cannot read CSV file (check permissions): {self.filepath}")
        return True

    def estimate_total_records(self) -> Optional[int]:
        try:
            # Use a robust way to open the file for counting, ignoring decoding errors
            with self.filepath.open('r', encoding='utf-8', errors='ignore') as f:
                return max(0, sum(1 for _ in f) - 1)
        except Exception: return None