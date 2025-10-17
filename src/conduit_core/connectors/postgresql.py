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
from ..errors import ConnectionError

logger = logging.getLogger(__name__)


def _test_postgres_connection(connection_string: str, host: str, port: int, database: str):
    """Shared connection test logic for PostgreSQL connectors."""
    try:
        conn = psycopg2.connect(connection_string)
        conn.close()
        return True
    except psycopg2.OperationalError as e:
        error_msg = str(e).strip()
        suggestions = []
        if "password authentication failed" in error_msg:
            suggestions.append("Check username and password in your config or .env file.")
        elif "could not connect to server" in error_msg:
            suggestions.append("Check that the host and port are correct.")
            suggestions.append(f"Verify the server is running and accessible: pg_isready -h {host} -p {port}")
            suggestions.append("Check firewall rules.")
        elif "database" in error_msg and "does not exist" in error_msg:
            suggestions.append(f"Ensure the database '{database}' exists.")
        else:
            suggestions.append("Check the full connection string format.")
        
        suggestion_str = "\n".join(f"  • {s}" for s in suggestions)
        raise ConnectionError(
            f"PostgreSQL connection failed: {error_msg}\n\nSuggestions:\n{suggestion_str}"
        ) from e


class PostgresSource(BaseSource):
    """Leser data fra PostgreSQL database."""

    def __init__(self, config: Any):
        load_dotenv()
        is_pydantic_config = not isinstance(config, dict)

        self.host = (config.host if is_pydantic_config else config.get('host')) or os.getenv("POSTGRES_HOST", "localhost")
        self.port = (config.port if is_pydantic_config else config.get('port')) or int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = (config.database if is_pydantic_config else config.get('database')) or os.getenv("POSTGRES_DATABASE")
        self.user = (config.user if is_pydantic_config else config.get('user')) or os.getenv("POSTGRES_USER")
        self.password = (config.password if is_pydantic_config else config.get('password')) or os.getenv("POSTGRES_PASSWORD")
        self.schema = (config.schema if is_pydantic_config else config.get('schema')) or "public"
        self.connection_string = (config.connection_string if is_pydantic_config else config.get('connection_string'))

        if not self.connection_string:
            if not all([self.database, self.user, self.password]):
                raise ValueError("PostgresSource requires database, user, and password.")
            self.connection_string = f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"
        
        logger.info(f"PostgresSource initialized successfully.")
    
    def test_connection(self) -> bool:
        """Test PostgreSQL connection."""
        return _test_postgres_connection(self.connection_string, self.host, self.port, self.database)

    @retry_with_backoff(exceptions=(psycopg2.OperationalError,))
    def _execute_query(self, query: str):
        conn = psycopg2.connect(self.connection_string)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        return conn, cursor

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        if not query or query == "n/a":
            raise ValueError("PostgresSource requires a SQL query.")
        
        conn, cursor = None, None
        try:
            conn, cursor = self._execute_query(query)
            for row in cursor:
                yield dict(row)
        finally:
            if cursor: cursor.close()
            if conn: conn.close()


class PostgresDestination(BaseDestination):
    """Skriver data til PostgreSQL database."""

    def __init__(self, config: Any):
        load_dotenv()
        is_pydantic_config = not isinstance(config, dict)

        self.host = (config.host if is_pydantic_config else config.get('host')) or os.getenv("POSTGRES_HOST", "localhost")
        self.port = (config.port if is_pydantic_config else config.get('port')) or int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = (config.database if is_pydantic_config else config.get('database')) or os.getenv("POSTGRES_DATABASE")
        self.user = (config.user if is_pydantic_config else config.get('user')) or os.getenv("POSTGRES_USER")
        self.password = (config.password if is_pydantic_config else config.get('password')) or os.getenv("POSTGRES_PASSWORD")
        self.schema = (config.schema if is_pydantic_config else config.get('schema')) or "public"
        self.table = config.table if is_pydantic_config else config.get('table')
        self.connection_string = (config.connection_string if is_pydantic_config else config.get('connection_string'))

        if not self.connection_string:
            if not all([self.database, self.user, self.password]):
                raise ValueError("PostgresDestination requires database, user, and password.")
            self.connection_string = f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"

        if not self.table:
            raise ValueError("PostgresDestination requires a 'table' parameter")

        self.accumulated_records = []
        self.mode = (config.mode if is_pydantic_config else config.get('mode')) or 'append'
        logger.info(f"PostgresDestination initialized: {self.schema}.{self.table} (mode: {self.mode})")

    def test_connection(self) -> bool:
        """Test PostgreSQL connection."""
        return _test_postgres_connection(self.connection_string, self.host, self.port, self.database)

    def write(self, records: Iterable[Dict[str, Any]]):
        self.accumulated_records.extend(list(records))

    @retry_with_backoff(exceptions=(psycopg2.OperationalError, psycopg2.DatabaseError))
    def _execute_batch_insert(self, cursor, insert_query, data):
        execute_batch(cursor, insert_query, data, page_size=1000)

    def finalize(self):
        if not self.accumulated_records:
            return
        
        conn, cursor = None, None
        try:
            conn = psycopg2.connect(self.connection_string)
            cursor = conn.cursor()
            
            if self.mode == 'full_refresh':
                cursor.execute(f"TRUNCATE TABLE {self.schema}.{self.table}")
            
            columns = list(self.accumulated_records[0].keys())
            columns_str = ", ".join(f'"{c}"' for c in columns)
            placeholders = ", ".join(["%s"] * len(columns))
            insert_query = f"INSERT INTO {self.schema}.{self.table} ({columns_str}) VALUES ({placeholders})"
            data = [tuple(record.get(col) for col in columns) for record in self.accumulated_records]
            
            self._execute_batch_insert(cursor, insert_query, data)
            conn.commit()
            logger.info(f"✅ Successfully wrote {len(data)} records to {self.schema}.{self.table}")
        except psycopg2.Error as e:
            if conn: conn.rollback()
            raise ValueError(f"PostgreSQL write error: {e}") from e
        finally:
            self.accumulated_records.clear()
            if cursor: cursor.close()
            if conn: conn.close()