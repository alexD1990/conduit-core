# src/conduit_core/connectors/postgresql.py

import logging
import os
from typing import Iterable, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from dotenv import load_dotenv

from .base import BaseSource, BaseDestination
from ..config import Source as SourceConfig
from ..config import Destination as DestinationConfig

logger = logging.getLogger(__name__)


class PostgresSource(BaseSource):
    """Leser data fra PostgreSQL database."""

    def __init__(self, config: SourceConfig):
        load_dotenv()
        
        # Build connection string
        if config.connection_string:
            self.connection_string = config.connection_string
        else:
            # Build from individual parameters
            host = config.host or os.getenv("POSTGRES_HOST", "localhost")
            port = config.port or int(os.getenv("POSTGRES_PORT", "5432"))
            database = config.database or os.getenv("POSTGRES_DATABASE")
            user = config.user or os.getenv("POSTGRES_USER")
            password = config.password or os.getenv("POSTGRES_PASSWORD")
            
            if not all([database, user, password]):
                raise ValueError("PostgresSource requires database, user, and password")
            
            self.connection_string = (
                f"host={host} port={port} dbname={database} "
                f"user={user} password={password}"
            )
        
        self.schema = config.schema or "public"
        
        # Test connection
        try:
            conn = psycopg2.connect(self.connection_string)
            conn.close()
            logger.info(f"PostgresSource initialized successfully")
        except psycopg2.Error as e:
            raise ValueError(f"Failed to connect to PostgreSQL: {e}")

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        """
        Leser data fra PostgreSQL med en SQL query.
        
        Args:
            query: SQL query (kan inneholde :last_value placeholder)
        
        Yields:
            Dictionaries representing rows
        """
        if not query or query == "n/a":
            raise ValueError("PostgresSource requires a SQL query")
        
        logger.info(f"Executing query: {query[:100]}...")
        
        conn = None
        cursor = None
        
        try:
            # Connect to database
            conn = psycopg2.connect(self.connection_string)
            
            # Use RealDictCursor to get results as dictionaries
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Execute query
            cursor.execute(query)
            
            # Fetch and yield rows one by one (memory efficient)
            row_count = 0
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                row_count += 1
                yield dict(row)
            
            logger.info(f"Successfully read {row_count} rows from PostgreSQL")
        
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL query error: {e}")
            raise ValueError(f"Failed to execute query: {e}")
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


class PostgresDestination(BaseDestination):
    """Skriver data til PostgreSQL database."""

    def __init__(self, config: DestinationConfig):
        load_dotenv()
        
        # Build connection string
        if config.connection_string:
            self.connection_string = config.connection_string
        else:
            # Build from individual parameters
            host = config.host or os.getenv("POSTGRES_HOST", "localhost")
            port = config.port or int(os.getenv("POSTGRES_PORT", "5432"))
            database = config.database or os.getenv("POSTGRES_DATABASE")
            user = config.user or os.getenv("POSTGRES_USER")
            password = config.password or os.getenv("POSTGRES_PASSWORD")
            
            if not all([database, user, password]):
                raise ValueError("PostgresDestination requires database, user, and password")
            
            self.connection_string = (
                f"host={host} port={port} dbname={database} "
                f"user={user} password={password}"
            )
        
        self.schema = config.schema or "public"
        self.table = config.table
        
        if not self.table:
            raise ValueError("PostgresDestination requires 'table' parameter")
        
        # Accumulate records
        self.accumulated_records = []
        
        # Test connection
        try:
            conn = psycopg2.connect(self.connection_string)
            conn.close()
            logger.info(f"PostgresDestination initialized: {self.schema}.{self.table}")
        except psycopg2.Error as e:
            raise ValueError(f"Failed to connect to PostgreSQL: {e}")

    def write(self, records: Iterable[Dict[str, Any]]):
        """Akkumulerer records. Actual write skjer i finalize()."""
        records_list = list(records)
        logger.info(f"PostgresDestination.write() accumulating {len(records_list)} records")
        self.accumulated_records.extend(records_list)

    def finalize(self):
        """
        Skriver alle akkumulerte records til PostgreSQL.
        Bruker batch INSERT for performance.
        """
        if not self.accumulated_records:
            logger.info("No records to write to PostgreSQL")
            return
        
        conn = None
        cursor = None
        
        try:
            conn = psycopg2.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Get columns from first record
            columns = list(self.accumulated_records[0].keys())
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            
            # Build INSERT query
            insert_query = (
                f"INSERT INTO {self.schema}.{self.table} ({columns_str}) "
                f"VALUES ({placeholders})"
            )
            
            # Prepare data for batch insert
            data = [
                tuple(record.get(col) for col in columns)
                for record in self.accumulated_records
            ]
            
            logger.info(f"Writing {len(data)} records to {self.schema}.{self.table}")
            
            # Execute batch insert
            execute_batch(cursor, insert_query, data, page_size=1000)
            
            # Commit transaction
            conn.commit()
            
            logger.info(f"✅ Successfully wrote {len(data)} records to PostgreSQL")
        
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            logger.error(f"Failed to write to PostgreSQL: {e}")
            raise ValueError(f"PostgreSQL write error: {e}")
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()