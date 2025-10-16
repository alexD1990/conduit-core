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
from ..utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


class PostgresSource(BaseSource):
    """Leser data fra PostgreSQL database."""

    def __init__(self, config: SourceConfig):
        load_dotenv()
        
        if config.connection_string:
            self.connection_string = config.connection_string
        else:
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
        
        # Test connection with retry
        self._test_connection()
        logger.info(f"PostgresSource initialized successfully")

    @retry_with_backoff(
        max_attempts=3,
        initial_delay=1.0,
        backoff_factor=2.0,
        exceptions=(psycopg2.OperationalError, psycopg2.DatabaseError)
    )
    def _test_connection(self):
        """Test connection with retry logic."""
        conn = psycopg2.connect(self.connection_string)
        conn.close()

    @retry_with_backoff(
        max_attempts=3,
        initial_delay=1.0,
        backoff_factor=2.0,
        exceptions=(psycopg2.OperationalError,)
    )
    def _execute_query(self, query: str):
        """Execute query with retry logic."""
        conn = psycopg2.connect(self.connection_string)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        return conn, cursor

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        """Leser data fra PostgreSQL."""
        if not query or query == "n/a":
            raise ValueError("PostgresSource requires a SQL query")
        
        logger.info(f"Executing query: {query[:100]}...")
        
        conn = None
        cursor = None
        
        try:
            conn, cursor = self._execute_query(query)
            
            row_count = 0
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                row_count += 1
                yield dict(row)
            
            logger.info(f"Successfully read {row_count} rows")
        
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
        
        if config.connection_string:
            self.connection_string = config.connection_string
        else:
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
        
        self.accumulated_records = []
        
        # Test connection with retry
        self._test_connection()
        logger.info(f"PostgresDestination initialized: {self.schema}.{self.table}")

    @retry_with_backoff(
        max_attempts=3,
        initial_delay=1.0,
        exceptions=(psycopg2.OperationalError,)
    )
    def _test_connection(self):
        """Test connection with retry."""
        conn = psycopg2.connect(self.connection_string)
        conn.close()

    def write(self, records: Iterable[Dict[str, Any]]):
        """Akkumulerer records."""
        records_list = list(records)
        logger.info(f"PostgresDestination.write() accumulating {len(records_list)} records")
        self.accumulated_records.extend(records_list)

    @retry_with_backoff(
        max_attempts=3,
        initial_delay=1.0,
        backoff_factor=2.0,
        exceptions=(psycopg2.OperationalError, psycopg2.DatabaseError)
    )
    def _execute_batch_insert(self, cursor, insert_query, data):
        """Execute batch insert with retry."""
        execute_batch(cursor, insert_query, data, page_size=1000)

    def finalize(self):
        """Skriver alle akkumulerte records til PostgreSQL."""
        if not self.accumulated_records:
            logger.info("No records to write to PostgreSQL")
            return
        
        conn = None
        cursor = None
        
        try:
            conn = psycopg2.connect(self.connection_string)
            cursor = conn.cursor()
            
            columns = list(self.accumulated_records[0].keys())
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            
            insert_query = (
                f"INSERT INTO {self.schema}.{self.table} ({columns_str}) "
                f"VALUES ({placeholders})"
            )
            
            data = [
                tuple(record.get(col) for col in columns)
                for record in self.accumulated_records
            ]
            
            logger.info(f"Writing {len(data)} records to {self.schema}.{self.table}")
            
            # Execute with retry
            self._execute_batch_insert(cursor, insert_query, data)
            
            conn.commit()
            logger.info(f"✅ Successfully wrote {len(data)} records")
        
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