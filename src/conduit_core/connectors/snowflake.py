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
    """Skriver data til Snowflake data warehouse."""

    def __init__(self, config: DestinationConfig):
        super().__init__(config)
        load_dotenv()
        
        self.account = config.account or os.getenv('SNOWFLAKE_ACCOUNT')
        self.user = config.user or os.getenv('SNOWFLAKE_USER')
        self.password = config.password or os.getenv('SNOWFLAKE_PASSWORD')
        self.warehouse = config.warehouse or os.getenv('SNOWFLAKE_WAREHOUSE')
        self.database = config.database or os.getenv('SNOWFLAKE_DATABASE')
        self.schema = config.schema or os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
        self.table = config.table
        
        if not all([self.account, self.user, self.password, self.warehouse, self.database, self.table]):
            raise ValueError("SnowflakeDestination requires account, user, password, warehouse, database, and table.")
        
        self.accumulated_records = []
        self.mode = getattr(config, 'mode', 'append')
        
        logger.info(f"SnowflakeDestination initialized: {self.database}.{self.schema}.{self.table} (mode: {self.mode})")

    def _get_connection(self):
        """Helper to create a new Snowflake connection."""
        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
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

    def _map_snowflake_type_to_conduit(self, sf_type: str) -> str:
        """Maps Snowflake data types to internal Conduit types."""
        sf_type = sf_type.upper()
        if sf_type.startswith('NUMBER'):
            # NUMBER(38,0) is integer, NUMBER(10,2) is float
            if ',' in sf_type:
                scale = sf_type.split(',')[-1].replace(')', '')
                if int(scale) > 0:
                    return 'float'
            return 'integer'
        if sf_type in ('FLOAT', 'DOUBLE'):
            return 'float'
        if sf_type == 'BOOLEAN':
            return 'boolean'
        if sf_type == 'DATE':
            return 'date'
        if 'TIMESTAMP' in sf_type:
            return 'timestamp'
        if sf_type in ('VARIANT', 'OBJECT', 'ARRAY'):
            return 'json'
        # Default for text types (TEXT, STRING, VARCHAR, etc.)
        return 'string'

    def get_table_schema(self) -> Optional[Dict[str, Any]]:
        """Query information_schema for current table structure."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor(snowflake.connector.cursor.DictCursor)
            
            # Must use quoted identifiers for DESCRIBE
            query = f'DESCRIBE TABLE "{self.database}"."{self.schema}"."{self.table}"'
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            if not rows:
                logger.warning(f"Table '{self.table}' not found or has no columns.")
                return None

            columns = []
            for row in rows:
                columns.append({
                    "name": row['name'],
                    "type": self._map_snowflake_type_to_conduit(row['type']),
                    "nullable": True if row['null?'] == 'Y' else False
                })
            
            return {"columns": columns}

        except ProgrammingError as e:
            if "does not exist" in str(e):
                logger.info(f"Table '{self.table}' does not exist, returning no schema.")
                return None # Table doesn't exist, which is fine
            logger.error(f"Failed to get table schema for '{self.table}': {e}")
            raise ConnectionError(f"Failed to get table schema: {e}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred getting schema for '{self.table}': {e}")
            raise
        finally:
            if conn:
                conn.close()

    def write(self, records: Iterable[Dict[str, Any]]):
        """Akkumulerer records."""
        self.accumulated_records.extend(list(records))

    @retry_with_backoff(exceptions=(DatabaseError, ProgrammingError))
    def _execute_snowflake_commands(self, cursor, stage_name, temp_csv_path):
        """Execute Snowflake commands with retry."""
        csv_filename = Path(temp_csv_path).name
        cursor.execute(f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}")
        cursor.execute(f"PUT file://{temp_csv_path} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        
        copy_command = f"""
            COPY INTO {self.database}.{self.schema}.{self.table}
            FROM @{stage_name}/{csv_filename}.gz
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = ABORT_STATEMENT
        """
        cursor.execute(copy_command)
        return cursor.fetchone()

    def finalize(self):
        """Skriver alle akkumulerte records til Snowflake."""
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
                cursor.execute(f"TRUNCATE TABLE IF EXISTS {self.database}.{self.schema}.{self.table}")
            
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='', encoding='utf-8') as temp_csv:
                temp_csv_path = temp_csv.name
                writer = csv.DictWriter(temp_csv, fieldnames=columns)
                writer.writeheader()
                writer.writerows(self.accumulated_records)
            
            logger.info(f"Writing {len(self.accumulated_records)} records to Snowflake via staged file.")
            
            stage_name = f"conduit_stage_{self.table}"
            result = self._execute_snowflake_commands(cursor, stage_name, temp_csv_path)
            
            if result and result[3] == 'LOADED': # Status is in the 4th column
                logger.info(f"✅ Successfully loaded {result[5]} rows into Snowflake")
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
        # Using quoted identifiers to handle case-sensitivity
        column_defs = ", ".join([f'"{col.upper()}" VARCHAR' for col in columns])
        create_table_sql = f'CREATE TABLE IF NOT EXISTS "{self.database}"."{self.schema}"."{self.table}" ({column_defs})'
        cursor.execute(create_table_sql)
        logger.info(f"Ensured table {self.table} exists")