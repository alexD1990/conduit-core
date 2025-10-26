# src/conduit_core/connectors/postgresql.py
import logging
import os
from typing import Iterable, Dict, Any, Optional, List
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
    """Reads data from PostgreSQL database."""

    def __init__(self, config: Any):
        super().__init__(config)
        load_dotenv()
        is_pydantic_config = not isinstance(config, dict)

        self.host = (config.host if is_pydantic_config else config.get('host')) or os.getenv("POSTGRES_HOST", "localhost")
        self.port = (config.port if is_pydantic_config else config.get('port')) or int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = (config.database if is_pydantic_config else config.get('database')) or os.getenv("POSTGRES_DATABASE")
        self.user = (config.user if is_pydantic_config else config.get('user')) or os.getenv("POSTGRES_USER")
        self.password = (config.password if is_pydantic_config else config.get('password')) or os.getenv("POSTGRES_PASSWORD")
        self.db_schema = (config.db_schema if is_pydantic_config else config.get('schema')) or "public"
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
            if cursor:
                cursor.close()
            if conn:
                conn.close()


class PostgresDestination(BaseDestination):
    """Writes data to PostgreSQL database."""

    def __init__(self, config: Any):
        super().__init__(config)
        load_dotenv()
        is_pydantic_config = not isinstance(config, dict)

        self.host = (config.host if is_pydantic_config else config.get('host')) or os.getenv("POSTGRES_HOST", "localhost")
        self.port = (config.port if is_pydantic_config else config.get('port')) or int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = (config.database if is_pydantic_config else config.get('database')) or os.getenv("POSTGRES_DATABASE")
        self.user = (config.user if is_pydantic_config else config.get('user')) or os.getenv("POSTGRES_USER")
        self.password = (config.password if is_pydantic_config else config.get('password')) or os.getenv("POSTGRES_PASSWORD")
        self.db_schema = (config.db_schema if is_pydantic_config else config.get('schema')) or "public"
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
        logger.info(f"PostgresDestination initialized: {self.db_schema}.{self.table} (mode: {self.mode})")

    def test_connection(self) -> bool:
        """Test PostgreSQL connection."""
        return _test_postgres_connection(self.connection_string, self.host, self.port, self.database)

    def execute_ddl(self, sql: str) -> None:
        conn = psycopg2.connect(self.connection_string)
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
            conn.commit()
            logger.info("DDL executed successfully")
        finally:
            conn.close()

    def alter_table(self, alter_sql: str) -> None:
        """Execute ALTER TABLE statement."""
        self.execute_ddl(alter_sql)

    # --- Phase 3 additions ---
    def get_table_schema(self) -> Optional[Dict[str, Any]]:
        """
        Query PostgreSQL information_schema to get current table structure.

        Returns:
            Schema dict in same format as SchemaInferrer:
            {column_name: {type: str, nullable: bool}}
            Or None if table doesn't exist
        """
        import psycopg2

        conn = psycopg2.connect(self.connection_string)
        try:
            with conn.cursor() as cursor:
                # Check if table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s 
                        AND table_name = %s
                    )
                """, (self.db_schema, self.table))

                if not cursor.fetchone()[0]:
                    return None  # Table doesn't exist

                # Get column info
                cursor.execute("""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s 
                    AND table_name = %s
                    ORDER BY ordinal_position
                """, (self.db_schema, self.table))

                schema = {}
                for row in cursor.fetchall():
                    col_name, data_type, is_nullable = row
                    internal_type = self._map_pg_type_to_internal(data_type)
                    schema[col_name] = {
                        'type': internal_type,
                        'nullable': (is_nullable == 'YES')
                    }

                return schema
        finally:
            conn.close()

    def _map_pg_type_to_internal(self, pg_type: str) -> str:
        """Map PostgreSQL type to internal schema type"""
        type_mapping = {
            'integer': 'integer',
            'bigint': 'integer',
            'smallint': 'integer',
            'double precision': 'float',
            'real': 'float',
            'numeric': 'decimal',
            'decimal': 'decimal',
            'boolean': 'boolean',
            'date': 'date',
            'timestamp': 'datetime',
            'timestamp without time zone': 'datetime',
            'timestamp with time zone': 'datetime',
            'text': 'string',
            'character varying': 'string',
            'varchar': 'string',
            'char': 'string',
            'character': 'string',
        }
        return type_mapping.get(pg_type.lower(), 'string')
    # --- End Phase 3 additions ---

    def _generate_merge_sql(self, table_name: str, columns: List[str], primary_keys: List[str]) -> str:
        """
        Generate PostgreSQL MERGE (INSERT ... ON CONFLICT) statement.
        
        Args:
            table_name: Target table name
            columns: List of all column names
            primary_keys: List of primary key column names
        
        Returns:
            SQL MERGE statement
        """
        if not primary_keys:
            raise ValueError("primary_keys required for MERGE operation")
        
        # Build column lists
        all_cols = ', '.join([f'"{col}"' for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Build UPDATE SET clause (exclude primary keys)
        update_cols = [col for col in columns if col not in primary_keys]
        update_set = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])
        
        # Build conflict target (primary keys)
        conflict_target = ', '.join([f'"{pk}"' for pk in primary_keys])
        
        sql = f"""
            INSERT INTO {self.db_schema}.{table_name} ({all_cols})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_target})
            DO UPDATE SET {update_set}
        """
        
        return sql.strip()



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
            
            # Handle full_refresh mode (legacy)
            if self.mode == 'full_refresh':
                cursor.execute(f"TRUNCATE TABLE {self.db_schema}.{self.table}")
            
            # Handle write_mode
            if self.config.write_mode == 'truncate' or self.config.write_mode == 'replace':
                cursor.execute(f"TRUNCATE TABLE {self.db_schema}.{self.table}")
            
            columns = list(self.accumulated_records[0].keys())
            data = [tuple(record.get(col) for col in columns) for record in self.accumulated_records]
            
            # Choose INSERT or MERGE based on write_mode
            if self.config.write_mode == 'merge':
                if not self.config.primary_keys:
                    raise ValueError("write_mode='merge' requires primary_keys configuration")
                
                merge_query = self._generate_merge_sql(self.table, columns, self.config.primary_keys)
                self._execute_batch_insert(cursor, merge_query, data)
                logger.info(f"[OK] Merged {len(data)} records into {self.db_schema}.{self.table}")
            else:
                # Default: append mode (INSERT)
                columns_str = ", ".join(f'"{c}"' for c in columns)
                placeholders = ", ".join(["%s"] * len(columns))
                insert_query = f"INSERT INTO {self.db_schema}.{self.table} ({columns_str}) VALUES ({placeholders})"
                self._execute_batch_insert(cursor, insert_query, data)
                logger.info(f"[OK] Successfully wrote {len(data)} records to {self.db_schema}.{self.table}")
            
            conn.commit()
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            raise ValueError(f"PostgreSQL write error: {e}") from e
        finally:
            self.accumulated_records.clear()
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def table_exists(self) -> bool:
        """Check if the destination table exists."""
        try:
            # Use existing connection if available
            if hasattr(self, 'conn') and self.conn:
                conn = self.conn
                cursor = conn.cursor()
                should_close = False
            else:
                conn = psycopg2.connect(self.connection_string)
                cursor = conn.cursor()
                should_close = True

            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_name = %s
                )
            """, (self.db_schema, self.table))
            exists = cursor.fetchone()[0]
            cursor.close()
            if should_close:
                conn.close()
            return exists
        except Exception as e:
            raise ValueError(f"Failed to check table existence: {e}")

    def get_table_schema(self) -> dict:
        """Get schema of existing table."""
        try:
            conn = psycopg2.connect(self.connection_string)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (self.db_schema, self.table))
            
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == 'YES'
                })
            cursor.close()
            conn.close()
            
            return {"columns": columns}
        except Exception as e:
            raise ValueError(f"Failed to get table schema: {e}")
