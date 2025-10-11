# src/conduit_core/connectors/azuresql.py

import os
import logging
import pyodbc
from typing import Iterable, Dict, Any
from dotenv import load_dotenv
from ..config import Source
from .base import BaseSource, BaseDestination
from ..utils.retry import retry_on_db_error
from ..errors import ConnectionError as ConduitConnectionError

class AzureSqlSource(BaseSource):
    """Henter data fra en Azure SQL Database."""

    def __init__(self, config: Source):
        load_dotenv()
        server = os.getenv("DB_SERVER")
        database = os.getenv("DB_DATABASE")
        username = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")

        if not all([server, database, username, password]):
            raise ValueError("Database-hemmeligheter er ikke satt i .env-filen.")

        # Bygger tilkoblingsstrengen for pyodbc
        self.connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )
        # Vi lagrer configen for senere bruk (f.eks. for query)
        self.config = config

    @retry_on_db_error
    def read(self, query: str) -> Iterable[Dict[str, Any]]:
        """Kjører en spørring mot databasen og yielder rader."""
        try:
            logging.info(f"Kobler til Azure SQL Database...")
            cnxn = pyodbc.connect(self.connection_string, timeout=60)
            cursor = cnxn.cursor()

            logging.info(f"Kjører spørring: {query}")
            cursor.execute(query)

            columns = [column[0] for column in cursor.description]

            row_count = 0
            for row in cursor.fetchall():
                row_count += 1
                yield dict(zip(columns, row))

            cnxn.close()
            logging.info(f"✅ Vellykket lesing av {row_count} rader fra Azure SQL")

        except pyodbc.Error as e:
            raise ConduitConnectionError(
                f"Klarte ikke koble til eller hente data fra Azure SQL.",
                suggestions=[
                    "Sjekk at DB_SERVER, DB_DATABASE, DB_USER, DB_PASSWORD er satt i .env",
                    "Verifiser nettverkstilkobling til Azure SQL",
                    "Sjekk om brannmur tillater din IP-adresse",
                    f"Original feil: {e}"
                ]
            )
    
    def test_connection(self) -> bool:
        """Test tilkobling til Azure SQL Database"""
        try:
            logging.info("Tester Azure SQL tilkobling...")
            cnxn = pyodbc.connect(self.connection_string, timeout=10)
            cursor = cnxn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cnxn.close()
            logging.info("✅ Azure SQL tilkoblingstest vellykket")
            return True
        except pyodbc.Error as e:
            raise ConduitConnectionError(
                f"Azure SQL tilkoblingstest feilet",
                suggestions=[
                    "Sjekk at .env-filen har riktige credentials",
                    "Verifiser nettverkstilkobling",
                    f"Feildetaljer: {e}"
                ]
            )
        

from ..config import Destination as DestinationConfig

class AzureSqlDestination(BaseDestination):
    """Skriver data til en tabell i Azure SQL Database."""

    def __init__(self, config: DestinationConfig):
        load_dotenv()
        server = os.getenv("DB_SERVER")
        database = os.getenv("DB_DATABASE")
        username = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")

        if not all([server, database, username, password]):
            raise ValueError("Database-hemmeligheter er ikke satt i .env-filen.")

        if not config.path:
            raise ValueError("En 'path' (tabellnavn) må være definert for AzureSqlDestination.")

        self.table_name = config.path
        self.connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )

    def test_connection(self) -> bool:
        """Test tilkobling til Azure SQL Database"""
        try:
            logging.info("Tester Azure SQL tilkobling (destination)...")
            cnxn = pyodbc.connect(self.connection_string, timeout=10)
            cursor = cnxn.cursor()
            
            # Check if table exists
            cursor.execute(f"SELECT TOP 1 1 FROM {self.table_name}")
            
            cnxn.close()
            logging.info("✅ Azure SQL destination tilkoblingstest vellykket")
            return True
        except pyodbc.Error as e:
            raise ConduitConnectionError(
                f"Azure SQL destination tilkoblingstest feilet",
                suggestions=[
                    "Sjekk at .env-filen har riktige credentials",
                    f"Sjekk at tabellen '{self.table_name}' eksisterer",
                    "Verifiser nettverkstilkobling",
                    f"Feildetaljer: {e}"
                ]
            )

    def write(self, records: Iterable[Dict[str, Any]]):
        records = list(records)
        if not records:
            print("Ingen rader å skrive til Azure SQL.")
            return

        cnxn = pyodbc.connect(self.connection_string, timeout=60)
        cursor = cnxn.cursor()

        # Forbered INSERT-setningen dynamisk
        headers = records[0].keys()
        columns = ', '.join(f'[{h}]' for h in headers)
        placeholders = ', '.join(['?'] * len(headers))

        sql = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"

        # Konverter dataene til en liste av tupler
        data_to_insert = [tuple(r.values()) for r in records]

        logging.info(f"Skriver {len(data_to_insert)} rader til tabell: {self.table_name}")

        # Bruk executemany for effektiv bulkskriving
        cursor.executemany(sql, data_to_insert)

        cnxn.commit()
        cursor.close()
        cnxn.close()

        logging.info(f"✅ Vellykket skriving til {self.table_name}")