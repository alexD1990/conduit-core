import csv
import os
import logging
from typing import Iterable, Dict, Any
from .base import BaseSource, BaseDestination
from ..config import Destination as DestinationConfig
from ..config import Source as SourceConfig

class CsvDestination(BaseDestination):
    """Skriver data til en lokal CSV-fil."""
    connector_type = "csv"

    def __init__(self, config: DestinationConfig):
        if not config.path:
            raise ValueError("En 'path' (filsti) må være definert for CsvDestination.")
        self.filepath = config.path

    def write(self, records: Iterable[Dict[str, Any]]):
        records = list(records)
        if not records:
            logging.info("Ingen rader å skrive til CSV.")
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
    """Leser data fra en lokal CSV-fil."""
    connector_type = "csv"

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