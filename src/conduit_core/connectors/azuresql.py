import os
import pyodbc
import logging
from typing import Iterable, Dict, Any
from dotenv import load_dotenv
from ..config import Source as SourceConfig
from ..config import Destination as DestinationConfig
from .base import BaseSource, BaseDestination

class AzureSqlSource(BaseSource):
    """Henter data fra en Azure SQL Database."""
    connector_type = "azuresql"

    def __init__(self, config: SourceConfig):
        load_dotenv()
        server = os.getenv("DB_SERVER")
        database = os.getenv("DB_DATABASE")
        username = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        
        if not all([server, database, username, password]):
            raise ValueError("Database-hemmeligheter er ikke satt i .env-filen.")

        self.connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )
        self.config = config

    def read(self, query: str) -> Iterable[Dict[str, Any]]:
        """Kjører en spørring mot databasen og yielder rader."""
        try:
            logging.info("Kobler til Azure SQL Database...")
            cnxn = pyodbc.connect(self.connection_string, timeout=60)
            cursor = cnxn.cursor()
            
            logging.info(f"Kjører spørring: {query}")
            cursor.execute(query)
            
            columns = [column[0] for column in cursor.description]
            
            for row in cursor.fetchall():
                yield dict(zip(columns, row))
                
            cnxn.close()
            logging.info("Tilkobling til Azure SQL lukket.")

        except pyodbc.Error as e:
            raise ConnectionError(f"Klarte ikke koble til eller hente data fra Azure SQL. Sjekk tilkoblingsdetaljer og nettverk. Original feil: {e}")


class AzureSqlDestination(BaseDestination):
    """Skriver data til en tabell i Azure SQL Database."""
    connector_type = "azuresql"

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

    def write(self, records: Iterable[Dict[str, Any]]):
        records = list(records)
        if not records:
            logging.info("Ingen rader å skrive til Azure SQL.")
            return

        cnxn = pyodbc.connect(self.connection_string, timeout=60)
        cursor = cnxn.cursor()

        headers = records[0].keys()
        columns = ', '.join(f'[{h}]' for h in headers)
        placeholders = ', '.join(['?'] * len(headers))
        
        sql = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
        
        data_to_insert = [tuple(r.values()) for r in records]

        logging.info(f"Skriver {len(data_to_insert)} rader til tabell: {self.table_name}")

        cursor.executemany(sql, data_to_insert)
        
        cnxn.commit()
        cursor.close()
        cnxn.close()
        
        logging.info(f"✅ Vellykket skriving til {self.table_name}")