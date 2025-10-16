# src/conduit_core/connectors/snowflake.py

import logging
import os
import tempfile
import csv
from typing import Iterable, Dict, Any
from pathlib import Path
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError

from .base import BaseDestination
from ..config import Destination as DestinationConfig
from ..utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


class SnowflakeDestination(BaseDestination):
    """Skriver data til Snowflake data warehouse."""

    def __init__(self, config: DestinationConfig):
        env_vars = {}
        try:
            with open('.env', 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        env_vars[key] = value
        except:
            pass
        
        self.account = getattr(config, 'account', None) or env_vars.get('SNOWFLAKE_ACCOUNT')
        self.user = getattr(config, 'user', None) or env_vars.get('SNOWFLAKE_USER')
        self.password = getattr(config, 'password', None) or env_vars.get('SNOWFLAKE_PASSWORD')
        self.warehouse = getattr(config, 'warehouse', None) or env_vars.get('SNOWFLAKE_WAREHOUSE')
        self.database = getattr(config, 'database', None) or env_vars.get('SNOWFLAKE_DATABASE')
        self.schema = getattr(config, 'schema', None) or env_vars.get('SNOWFLAKE_SCHEMA', 'PUBLIC')
        self.table = getattr(config, 'table', None)
        
        if not all([self.account, self.user, self.password, self.warehouse, self.database, self.table]):
            raise ValueError("SnowflakeDestination requires account, user, password, warehouse, database, and table")
        
        self.accumulated_records = []
        
        # Test connection with retry
        self._test_connection()
        logger.info(f"SnowflakeDestination initialized: {self.database}.{self.schema}.{self.table}")

    @retry_with_backoff(
        max_attempts=3,
        initial_delay=2.0,
        backoff_factor=2.0,
        exceptions=(DatabaseError, ConnectionError)
    )
    def _test_connection(self):
        """Test connection with retry."""
        conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )
        conn.close()

    def write(self, records: Iterable[Dict[str, Any]]):
        """Akkumulerer records."""
        records_list = list(records)
        logger.info(f"SnowflakeDestination.write() accumulating {len(records_list)} records")
        self.accumulated_records.extend(records_list)

    @retry_with_backoff(
        max_attempts=3,
        initial_delay=2.0,
        backoff_factor=2.0,
        exceptions=(DatabaseError, ProgrammingError)
    )
    def _execute_snowflake_commands(self, cursor, stage_name, temp_csv_path, csv_filename):
        """Execute Snowflake commands with retry."""
        # Create stage
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")
        
        # PUT file
        put_command = f"PUT file://{temp_csv_path} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        cursor.execute(put_command)
        
        # COPY INTO
        copy_command = f"""
            COPY INTO {self.table}
            FROM @{stage_name}/{csv_filename}.gz
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = ABORT_STATEMENT
        """
        cursor.execute(copy_command)
        result = cursor.fetchone()
        
        # Cleanup
        cursor.execute(f"REMOVE @{stage_name}")
        
        return result

    def finalize(self):
        """Skriver alle akkumulerte records til Snowflake."""
        if not self.accumulated_records:
            logger.info("No records to write to Snowflake")
            return
        
        conn = None
        cursor = None
        temp_csv = None
        
        try:
            conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            cursor = conn.cursor()
            
            # Create table if not exists
            columns = list(self.accumulated_records[0].keys())
            self._create_table_if_not_exists(cursor, columns)
            
            # Write to temp CSV
            temp_csv = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='')
            writer = csv.DictWriter(temp_csv, fieldnames=columns)
            writer.writeheader()
            writer.writerows(self.accumulated_records)
            temp_csv.close()
            
            logger.info(f"Writing {len(self.accumulated_records)} records to Snowflake")
            
            # Execute with retry
            stage_name = f"conduit_stage_{self.table}"
            csv_filename = Path(temp_csv.name).name
            result = self._execute_snowflake_commands(cursor, stage_name, temp_csv.name, csv_filename)
            
            logger.info(f"✅ Successfully loaded {result[0]} rows into Snowflake")
            
        except Exception as e:
            logger.error(f"Failed to write to Snowflake: {e}")
            raise ValueError(f"Snowflake write error: {e}")
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            if temp_csv and os.path.exists(temp_csv.name):
                os.unlink(temp_csv.name)
    
    def _create_table_if_not_exists(self, cursor, columns):
        """Create table if it doesn't exist."""
        column_defs = ", ".join([f"{col} VARCHAR(16777216)" for col in columns])
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                {column_defs}
            )
        """
        cursor.execute(create_table_sql)
        logger.info(f"Ensured table {self.table} exists")