# src/conduit_core/connectors/csv.py

import csv
import logging
import os
from pathlib import Path
from typing import Iterable, Dict, Any

from .base import BaseSource, BaseDestination
from ..config import Destination as DestinationConfig
from ..config import Source as SourceConfig


class CsvDestination(BaseDestination):
    """Skriver data til en lokal CSV-fil med atomic writes."""

    def __init__(self, config: DestinationConfig):
        if not config.path:
            raise ValueError("En 'path' (filsti) må være definert for CsvDestination.")
        self.filepath = Path(config.path)
        self.temp_filepath = self.filepath.with_suffix('.tmp')

    def write(self, records: Iterable[Dict[str, Any]]):
        """Skriver records til CSV med atomic write pattern."""
        records = list(records)
        if not records:
            logging.info("Ingen rader å skrive til CSV.")
            return

        # Opprett output directory hvis den ikke finnes
        output_dir = self.filepath.parent
        if output_dir and not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)

        headers = records[0].keys()

        logging.info(f"Skriver {len(records)} rader til CSV-fil: {self.filepath}")

        try:
            # STEP 1: Skriv til temp fil
            with open(self.temp_filepath, 'w', newline='', encoding='utf-8') as output_file:
                writer = csv.DictWriter(output_file, fieldnames=headers)
                writer.writeheader()
                writer.writerows(records)
            
            # STEP 2: Atomic rename (erstatter eksisterende fil trygt)
            self.temp_filepath.replace(self.filepath)
            
            logging.info(f"✅ Vellykket skriving til {self.filepath}")
        
        except Exception as e:
            # Rydd opp temp fil ved feil
            if self.temp_filepath.exists():
                self.temp_filepath.unlink()
            logging.error(f"❌ Feil ved skriving til CSV: {e}")
            raise


class CsvSource(BaseSource):
    """Leser data fra en lokal CSV-fil."""

    def __init__(self, config: SourceConfig):
        if not config.path:
            raise ValueError("En 'path' (filsti) må være definert for CsvSource.")
        self.filepath = config.path

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        """Leser alle rader fra CSV-filen og yielder dem som dictionaries."""
        logging.info(f"Leser fra CSV-fil: {self.filepath}")

        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"Finner ikke CSV-filen: {self.filepath}")

        with open(self.filepath, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                yield row

        logging.info(f"✅ Ferdig med å lese fra {self.filepath}")