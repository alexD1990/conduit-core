# src/conduit_core/connectors/snowflake.py
import logging
import os
import tempfile
import csv
from typing import Iterable, Dict, Any, Optional
from pathlib import Path
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError
from dotenv import load_dotenv

from .base import BaseDestination
from ..config import Destination as DestinationConfig
from ..utils.retry import retry_with_backoff
from ..errors import ConnectionError

logger = logging.getLogger(__name__)


class SnowflakeDestination(BaseDestination):
    """Writes data to Snowflake data warehouse."""

    def __init__(self, config: DestinationConfig):
        super().__init__(config)
        load_dotenv()
        
        self.account = config.account or os.getenv('SNOWFLAKE_ACCOUNT')
        self.user = config.user or os.getenv('SNOWFLAKE_USER')
        self.password = config.password or os.getenv('SNOWFLAKE_PASSWORD')
        self.warehouse = config.warehouse or os.getenv('SNOWFLAKE_WAREHOUSE')
        self.database = config.database or os.getenv('SNOWFLAKE_DATABASE')
        self.db_schema = config.db_schema or os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
        self.table = config.table
        
        if not all([self.account, self.user, self.password, self.warehouse, self.database, self.table]):
            raise ValueError("SnowflakeDestination requires account, user, password, warehouse, database, and table.")
        
        self.accumulated_records = []
        self.mode = getattr(config, 'mode', 'append')
        
        logger.info(f"SnowflakeDestination initialized: {self.database}.{self.db_schema}.{self.table} (mode: {self.mode})")

    def _get_connection(self):
        """Helper to create a new Snowflake connection."""
        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.db_schema
        )

    def test_connection(self) -> bool:
        """Test Snowflake connection."""
        try:
            conn = self._get_connection()
            conn.close()
            return True
        except snowflake.connector.errors.DatabaseError as e:
            error_msg = str(e)
            suggestions = []
            if "Incorrect username or password" in error_msg:
                suggestions.append("Check your username and password in the config or .env file.")
            elif "account" in error_msg.lower():
                suggestions.append(f"Verify your account identifier is correct: {self.account}")
                suggestions.append("The format should be <account_locator>.<region> (e.g., xy12345.us-east-1).")
            elif "warehouse" in error_msg.lower():
                suggestions.append(f"Check that the warehouse '{self.warehouse}' exists and is running.")
            elif "database" in error_msg.lower():
                suggestions.append(f"Check that the database '{self.database}' exists.")

            suggestion_str = "\n".join(f"  • {s}" for s in suggestions)
            raise ConnectionError(
                f"Snowflake connection failed: {error_msg}\n\nSuggestions:\n{suggestion_str}"
            ) from e

    def execute_ddl(self, sql: str) -> None:
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            logger.info("DDL executed successfully")
        except Exception as e:
            logger.error(f"Snowflake DDL execution failed: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def alter_table(self, alter_sql: str) -> None:
        """Execute ALTER TABLE statement."""
        self.execute_ddl(alter_sql)

    # --- Phase 3 additions ---
    def get_table_schema(self) -> Optional[Dict[str, Any]]:
        """Get table schema from Snowflake INFORMATION_SCHEMA"""
        import snowflake.connector

        conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.db_schema
        )

        try:
            cursor = conn.cursor()

            # Check if table exists
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{self.db_schema}' 
                AND TABLE_NAME = '{self.table}'
            """)

            if cursor.fetchone()[0] == 0:
                return None

            # Get columns
            cursor.execute(f"""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{self.db_schema}'
                AND TABLE_NAME = '{self.table}'
                ORDER BY ORDINAL_POSITION
            """)

            schema = {}
            for row in cursor.fetchall():
                col_name, data_type, is_nullable = row
                internal_type = self._map_snowflake_type_to_internal(data_type)
                schema[col_name] = {
                    'type': internal_type,
                    'nullable': (is_nullable == 'YES')
                }

            return schema
        finally:
            conn.close()

    def _map_snowflake_type_to_internal(self, sf_type: str) -> str:
        """Map Snowflake type to internal schema type"""
        type_mapping = {
            'NUMBER': 'integer',
            'DECIMAL': 'decimal',
            'NUMERIC': 'decimal',
            'INT': 'integer',
            'INTEGER': 'integer',
            'BIGINT': 'integer',
            'SMALLINT': 'integer',
            'FLOAT': 'float',
            'DOUBLE': 'float',
            'BOOLEAN': 'boolean',
            'DATE': 'date',
            'TIMESTAMP_NTZ': 'datetime',
            'TIMESTAMP_LTZ': 'datetime',
            'TIMESTAMP_TZ': 'datetime',
            'VARCHAR': 'string',
            'TEXT': 'string',
            'STRING': 'string',
        }
        return type_mapping.get(sf_type.upper(), 'string')
    # --- End Phase 3 additions ---

    def write(self, records: Iterable[Dict[str, Any]]):
        """Accumulate records before write."""
        self.accumulated_records.extend(list(records))

    @retry_with_backoff(exceptions=(DatabaseError, ProgrammingError))
    def _execute_snowflake_commands(self, cursor, stage_name, temp_csv_path):
        """Execute Snowflake commands with retry."""
        csv_filename = Path(temp_csv_path).name
        cursor.execute(f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}")
        cursor.execute(f"PUT file://{temp_csv_path} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        
        copy_command = f"""
            COPY INTO {self.database}.{self.db_schema}.{self.table}
            FROM @{stage_name}/{csv_filename}.gz
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = ABORT_STATEMENT
        """
        cursor.execute(copy_command)
        return cursor.fetchone()

    def finalize(self):
        """Writes accumulated records to Snowflake."""
        if not self.accumulated_records:
            logger.info("No records to write to Snowflake")
            return
        
        conn = None
        temp_csv_path = None
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            columns = list(self.accumulated_records[0].keys())
            self._create_table_if_not_exists(cursor, columns)
            
            if self.mode == 'full_refresh':
                logger.info(f"🗑️  TRUNCATE {self.table} (full_refresh mode)")
                cursor.execute(f"TRUNCATE TABLE IF EXISTS {self.database}.{self.db_schema}.{self.table}")
            
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='', encoding='utf-8') as temp_csv:
                temp_csv_path = temp_csv.name
                writer = csv.DictWriter(temp_csv, fieldnames=columns)
                writer.writeheader()
                writer.writerows(self.accumulated_records)
            
            logger.info(f"Writing {len(self.accumulated_records)} records to Snowflake via staged file.")
            
            stage_name = f"conduit_stage_{self.table}"
            result = self._execute_snowflake_commands(cursor, stage_name, temp_csv_path)
            
            if result and len(result) >= 4 and result[3] == 'LOADED':  # Status in column 4
                logger.info(f"✅ Successfully loaded rows into Snowflake")
            else:
                logger.warning(f"Snowflake COPY command did not return 'LOADED' status. Result: {result}")

        except Exception as e:
            logger.error(f"Failed to write to Snowflake: {e}")
            raise ValueError(f"Snowflake write error: {e}") from e
        
        finally:
            self.accumulated_records.clear()
            if conn:
                conn.close()
            if temp_csv_path and os.path.exists(temp_csv_path):
                os.unlink(temp_csv_path)
    
    def _create_table_if_not_exists(self, cursor, columns):
        """Create table if it doesn't exist."""
        column_defs = ", ".join([f'"{col.upper()}" VARCHAR' for col in columns])
        create_table_sql = f'CREATE TABLE IF NOT EXISTS "{self.database}"."{self.db_schema}"."{self.table}" ({column_defs})'
        cursor.execute(create_table_sql)
        logger.info(f"Ensured table {self.table} exists")
