# src/conduit_core/connectors/mysql.py

import logging
from typing import Iterable, Dict, Any
import mysql.connector
from mysql.connector import Error as MySQLError

from .base import BaseSource, BaseDestination
from ..config import Source as SourceConfig
from ..config import Destination as DestinationConfig
from ..utils.retry import retry_with_backoff
from ..errors import ConnectionError

logger = logging.getLogger(__name__)


class MySQLSource(BaseSource):
    """Read data from MySQL database."""

    def __init__(self, config: SourceConfig):
        self.host = config.host
        self.port = config.port or 3306
        self.database = config.database
        self.user = config.user
        self.password = config.password
        self.connection = None
        logger.info(f"MySQLSource initialized: {self.user}@{self.host}:{self.port}/{self.database}")

    def test_connection(self) -> bool:
        """Test MySQL connection."""
        try:
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connection_timeout=10
            )
            conn.close()
            return True
        except MySQLError as e:
            if e.errno == 1045:  # Access denied
                raise ConnectionError(
                    f"MySQL authentication failed for user '{self.user}'.\n\n"
                    f"Suggestions:\n"
                    f"  • Check username and password are correct.\n"
                    f"  • Verify the user has access to database '{self.database}'."
                ) from e
            elif e.errno == 2003:  # Can't connect
                raise ConnectionError(
                    f"Cannot connect to MySQL server at {self.host}:{self.port}.\n\n"
                    f"Suggestions:\n"
                    f"  • Check the host and port are correct.\n"
                    f"  • Verify MySQL server is running.\n"
                    f"  • Check firewall rules allow connections."
                ) from e
            elif e.errno == 1049:  # Unknown database
                raise ConnectionError(
                    f"MySQL database not found: {self.database}\n\n"
                    f"Suggestions:\n"
                    f"  • Check the database name for typos.\n"
                    f"  • Verify the database exists: SHOW DATABASES;"
                ) from e
            else:
                raise ConnectionError(f"MySQL connection failed: {e}") from e

    @retry_with_backoff(exceptions=(MySQLError,))
    def _execute_query(self, query: str):
        """Execute query with retry logic."""
        if not self.connection or not self.connection.is_connected():
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
        
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query)
        return cursor

    def read(self, query: str) -> Iterable[Dict[str, Any]]:
        """Read data from MySQL and yield records."""
        try:
            cursor = self._execute_query(query)
            
            for row in cursor:
                # Convert datetime objects to strings for JSON serialization
                cleaned_row = {}
                for key, value in row.items():
                    if hasattr(value, 'isoformat'):
                        cleaned_row[key] = value.isoformat()
                    elif isinstance(value, bytes):
                        cleaned_row[key] = value.decode('utf-8', errors='replace')
                    else:
                        cleaned_row[key] = value
                yield cleaned_row
            
            cursor.close()
        finally:
            if self.connection and self.connection.is_connected():
                self.connection.close()


class MySQLDestination(BaseDestination):
    """Write data to MySQL database."""

    def __init__(self, config: DestinationConfig):
        self.host = config.host
        self.port = config.port or 3306
        self.database = config.database
        self.user = config.user
        self.password = config.password
        self.table = config.table
        self.mode = getattr(config, 'mode', 'append')
        self.accumulated_records = []
        self.connection = None
        logger.info(f"MySQLDestination initialized: {self.table} (mode: {self.mode})")

    def test_connection(self) -> bool:
        """Test MySQL connection."""
        try:
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connection_timeout=10
            )
            conn.close()
            return True
        except MySQLError as e:
            raise ConnectionError(f"MySQL connection failed: {e}") from e

    def write(self, records: Iterable[Dict[str, Any]]):
        """Accumulate records for batch insert."""
        self.accumulated_records.extend(list(records))

    @retry_with_backoff(exceptions=(MySQLError,))
    def finalize(self):
        """Write all accumulated records to MySQL."""
        if not self.accumulated_records:
            logger.info("No records to write to MySQL")
            return

        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = self.connection.cursor()

            # Create table if not exists
            self._create_table_if_not_exists(cursor, self.accumulated_records[0])

            # Handle mode
            if self.mode == 'full_refresh':
                logger.info(f"🗑️  TRUNCATE `{self.table}` (full_refresh mode)")
                cursor.execute(f"TRUNCATE TABLE `{self.database}`.`{self.table}`")

            # Batch insert
            columns = list(self.accumulated_records[0].keys())
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f"INSERT INTO `{self.database}`.`{self.table}` ({', '.join([f'`{col}`' for col in columns])}) VALUES ({placeholders})"

            batch_size = 1000
            for i in range(0, len(self.accumulated_records), batch_size):
                batch = self.accumulated_records[i:i + batch_size]
                values = [tuple(record[col] for col in columns) for record in batch]
                cursor.executemany(insert_sql, values)
                self.connection.commit()

            logger.info(f"✓ Wrote {len(self.accumulated_records)} records to MySQL")

        finally:
            self.accumulated_records.clear()
            if self.connection and self.connection.is_connected():
                cursor.close()
                self.connection.close()

    def _create_table_if_not_exists(self, cursor, sample_record: Dict[str, Any]):
        """Create table with schema inferred from sample record."""
        columns = []
        for key, value in sample_record.items():
            if isinstance(value, bool):
                col_type = "BOOLEAN"
            elif isinstance(value, int):
                col_type = "BIGINT"
            elif isinstance(value, float):
                col_type = "DOUBLE"
            elif isinstance(value, str):
                col_type = "TEXT"
            else:
                col_type = "TEXT"
            columns.append(f"`{key}` {col_type}")

        create_sql = f"CREATE TABLE IF NOT EXISTS `{self.database}`.`{self.table}` ({', '.join(columns)})"
        cursor.execute(create_sql)
        logger.debug(f"Ensured table exists: {self.table}")
