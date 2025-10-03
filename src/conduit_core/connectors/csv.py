# src/conduit_core/connectors/csv.py

import csv
import os
from typing import Iterable, Dict, Any
from .base import BaseDestination
from ..config import Destination as DestinationConfig

class CsvDestination(BaseDestination):
    """Skriver data til en lokal CSV-fil."""

    def __init__(self, config: DestinationConfig):
        if not config.path:
            raise ValueError("En 'path' (filsti) må være definert for CsvDestination.")
        self.filepath = config.path

    def write(self, records: Iterable[Dict[str, Any]]):
        records = list(records)
        if not records:
            print("Ingen rader å skrive til CSV.")
            return

        # Sikrer at mappen vi skal skrive til eksisterer
        output_dir = os.path.dirname(self.filepath)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        # Henter kolonnenavn fra den første raden
        headers = records[0].keys()

        print(f"Skriver {len(records)} rader til CSV-fil: {self.filepath}")

        with open(self.filepath, 'w', newline='', encoding='utf-8') as output_file:
            writer = csv.DictWriter(output_file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(records)

        print(f"✅ Vellykket skriving til {self.filepath}")