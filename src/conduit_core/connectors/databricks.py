# src/conduit_core/connectors/databricks.py

import os
import io
import csv
from typing import Iterable, Dict, Any
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from .base import BaseDestination
from ..config import Destination as DestinationConfig

class DatabricksDestination(BaseDestination):
    """Skriver data til en Delta-tabell i Databricks."""

    def __init__(self, config: DestinationConfig):
        load_dotenv()
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")

        # --- START FEILSØKINGSKODE ---
        print("\n--- 🕵️ FEILSØKER KOBBING TIL DATABRICKS 🕵️ ---")
        print(f"Lest HOST fra .env: {host}")
        if token:
            # Viser kun starten og slutten av tokenet for sikkerhets skyld
            print(f"Lest TOKEN fra .env: {token[:5]}...{token[-4:]}")
        else:
            print("Lest TOKEN fra .env: None")
        print("--------------------------------------------------\n")
        # --- SLUTT FEILSØKINGSKODE ---

        if not all([host, token]):
            raise ValueError("Databricks host/token er ikke satt i .env-filen.")

        self.ws = WorkspaceClient(host=host, token=token)
        self.table_path = config.path

    def write(self, records: Iterable[Dict[str, Any]]):
        """Laster opp data som en CSV og bruker COPY INTO."""
        if not self.table_path:
            raise ValueError("'path' (tabellnavn) må være definert for Databricks-destinasjonen.")
            
        # Konverterer dicts til en CSV-fil i minnet
        records = list(records)
        if not records:
            print("Ingen rader å skrive. Avslutter.")
            return

        # Oppretter en CSV-fil i minnet
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
        csv_data = output.getvalue().encode('utf-8')

        staging_path = f"/tmp/conduit_core/{self.table_path}.csv"
        
        # 1. Laster opp CSV-filen
        print(f"Laster opp data til midlertidig fil: {staging_path}")
        self.ws.dbfs.upload(staging_path, io.BytesIO(csv_data), overwrite=True)

        # 2. Kjører COPY INTO for å laste data inn i tabellen
        copy_sql = f"""
        COPY INTO {self.table_path}
        FROM '{staging_path}'
        FILEFORMAT = CSV
        FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
        COPY_OPTIONS ('mergeSchema' = 'true')
        """
        
        print(f"Kjører COPY INTO for tabell: {self.table_path}")
        self.ws.statement_execution.execute_statement(statement=copy_sql)
        
        # 3. RYDDE OPP (valgfritt, men god praksis)
        print(f"Sletter midlertidig fil: {staging_path}")
        self.ws.dbfs.delete(staging_path)

        print(f"✅ Data ble skrevet til Databricks-tabell: {self.table_path}")