# src/conduit_core/connectors/csv.py

from ..schema import CsvDelimiterDetector
import csv
import logging
import os
from typing import Iterable, Dict, Any

# Manglende importer lagt til her
from .base import BaseSource, BaseDestination
from ..config import Destination as DestinationConfig
from ..config import Source as SourceConfig
from ..types import EncodingDetector, TypeConverter


class CsvDestination(BaseDestination):
    """Skriver data til en lokal CSV-fil."""

    def __init__(self, config: DestinationConfig):
        if not config.path:
            raise ValueError("En 'path' (filsti) må være definert for CsvDestination.")
        self.filepath = config.path

    def test_connection(self) -> bool:
        """Test at output-mappen eksisterer eller kan opprettes"""
        output_dir = os.path.dirname(self.filepath)
        if output_dir and not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir, exist_ok=True)
            except Exception as e:
                from ..errors import ConnectionError as ConduitConnectionError
                raise ConduitConnectionError(
                    f"Kan ikke opprette output-mappe: {output_dir}",
                    suggestions=[
                        "Sjekk at du har skrivetilgang til mappen",
                        f"Feil: {e}"
                    ]
                )
        logging.info(f"✅ CSV destination tilkoblingstest vellykket: {self.filepath}")
        return True

    def write(self, records: Iterable[Dict[str, Any]]):
        records = list(records)
        if not records:
            print("Ingen rader å skrive til CSV.")
            return

        output_dir = os.path.dirname(self.filepath)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        headers = records[0].keys()

        logging.info(f"Skriver {len(records)} rader til CSV-fil: {self.filepath}")

        with open(self.filepath, 'w', newline='', encoding='utf-8') as output_file:
            writer = csv.DictWriter(output_file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(records)

        logging.info(f"✅ Vellykket skriving til {self.filepath}")


class CsvSource(BaseSource):
    """Leser data fra en lokal CSV-fil med auto-detection."""

    def __init__(self, config: SourceConfig):
        if not config.path:
            raise ValueError("En 'path' (filsti) må være definert for CsvSource.")
        self.filepath = config.path
        # Auto-detect encoding
        self.encoding = EncodingDetector.detect_encoding(self.filepath)
        # Auto-detect delimiter
        self.delimiter = CsvDelimiterDetector.detect_delimiter(self.filepath)

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        """Leser alle rader fra CSV-filen og yielder dem som dictionaries."""
        logging.info(f"Leser fra CSV-fil: {self.filepath} (encoding: {self.encoding}, delimiter: '{self.delimiter}')")

        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"Finner ikke CSV-filen: {self.filepath}")

        with open(self.filepath, mode='r', encoding=self.encoding, errors='replace') as infile:
            reader = csv.DictReader(infile, delimiter=self.delimiter)
            for row in reader:
                yield row

        logging.info(f"✅ Ferdig med å lese fra {self.filepath}")