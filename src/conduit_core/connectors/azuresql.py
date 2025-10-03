# src/conduit_core/connectors/azuresql.py

import os
import pyodbc
from typing import Iterable, Dict, Any
from dotenv import load_dotenv
from ..config import Source
from .base import BaseSource

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

    def read(self, query: str) -> Iterable[Dict[str, Any]]:
        """Kjører en spørring mot databasen og yielder rader."""
        print(f"Kobler til Azure SQL Database...")
        cnxn = pyodbc.connect(self.connection_string)
        cursor = cnxn.cursor()

        print(f"Kjører spørring: {query}")
        cursor.execute(query)

        columns = [column[0] for column in cursor.description]

        for row in cursor.fetchall():
            yield dict(zip(columns, row))

        cnxn.close()
        print("Tilkobling til Azure SQL lukket.")