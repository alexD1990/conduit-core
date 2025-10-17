# src/conduit_core/connectors/csv.py

import csv
import logging
import os
from pathlib import Path
from typing import Iterable, Dict, Any

from .base import BaseSource, BaseDestination
from ..config import Destination as DestinationConfig
from ..config import Source as SourceConfig
from ..errors import ConnectionError


class CsvDestination(BaseDestination):
    """Skriver data til en lokal CSV-fil med atomic writes."""

    def __init__(self, config: DestinationConfig):
        if not config.path:
            raise ValueError("En 'path' (filsti) må være definert for CsvDestination.")
        self.filepath = Path(config.path)
        self.temp_filepath = self.filepath.with_suffix(f"{self.filepath.suffix}.tmp")
        self.accumulated_records = []

    def write(self, records: Iterable[Dict[str, Any]]):
        """Akkumulerer records. Actual write skjer i finalize()."""
        self.accumulated_records.extend(list(records))

    def finalize(self):
        """Skriver alle akkumulerte records til CSV med atomic write pattern."""
        if not self.accumulated_records:
            logging.info("Ingen rader å skrive til CSV.")
            return

        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        headers = self.accumulated_records[0].keys()
        logging.info(f"Skriver {len(self.accumulated_records)} rader til CSV-fil: {self.filepath}")

        try:
            with self.temp_filepath.open('w', newline='', encoding='utf-8') as output_file:
                writer = csv.DictWriter(output_file, fieldnames=headers)
                writer.writeheader()
                writer.writerows(self.accumulated_records)
            self.temp_filepath.replace(self.filepath)
            logging.info(f"✅ Vellykket skriving til {self.filepath}")
        except Exception as e:
            if self.temp_filepath.exists():
                self.temp_filepath.unlink()
            logging.error(f"❌ Feil ved skriving til CSV: {e}")
            raise
        finally:
            self.accumulated_records.clear()

    def test_connection(self) -> bool:
        """Test that output directory is writable."""
        output_dir = self.filepath.parent
        if output_dir and not output_dir.exists():
            try:
                output_dir.mkdir(parents=True, exist_ok=True)
            except PermissionError:
                raise ConnectionError(
                    f"Cannot create output directory: {output_dir}\n\n"
                    f"Suggestions:\n"
                    f"  • Check parent directory permissions\n"
                    f"  • Create directory manually: mkdir -p {output_dir}"
                ) from None
        
        test_file = output_dir / ".conduit_test_write"
        try:
            test_file.touch()
            test_file.unlink()
        except PermissionError:
            raise ConnectionError(
                f"Cannot write to directory: {output_dir}\n\n"
                f"Suggestions:\n"
                f"  • Check directory permissions\n"
                f"  • Run: chmod +w {output_dir}"
            ) from None
        return True


class CsvSource(BaseSource):
    """Leser data fra en lokal CSV-fil."""

    def __init__(self, config: SourceConfig):
        if not config.path:
            raise ValueError("En 'path' (filsti) må være definert for CsvSource.")
        self.filepath = Path(config.path)

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        """Leser alle rader fra CSV-filen og yielder dem som dictionaries."""
        logging.info(f"Leser fra CSV-fil: {self.filepath}")
        if not self.filepath.exists():
            raise FileNotFoundError(f"Finner ikke CSV-filen: {self.filepath}")

        with self.filepath.open(mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                yield row
        logging.info(f"✅ Ferdig med å lese fra {self.filepath}")

    def test_connection(self) -> bool:
        """Test that CSV file exists and is readable."""
        if not self.filepath.exists():
            raise ConnectionError(
                f"CSV file not found: {self.filepath}\n\n"
                f"Suggestions:\n"
                f"  • Check file path is correct\n"
                f"  • Verify file exists: ls {self.filepath}\n"
                f"  • Check permissions: ls -la {self.filepath.parent}"
            )
        if not os.access(self.filepath, os.R_OK):
            raise ConnectionError(
                f"Cannot read CSV file: {self.filepath}\n\n"
                f"Suggestions:\n"
                f"  • Check file permissions\n"
                f"  • Run: chmod +r {self.filepath}"
            )
        return True