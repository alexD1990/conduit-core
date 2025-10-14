# src/conduit_core/connectors/snowflake.py

import logging
import os
import tempfile
import csv
from typing import Iterable, Dict, Any
from pathlib import Path
import snowflake.connector
from snowflake.connector import DictCursor

from .base import BaseDestination
from ..config import Destination as DestinationConfig

logger = logging.getLogger(__name__)


class SnowflakeDestination(BaseDestination):
    """Skriver data til Snowflake data warehouse."""

    def __init__(self, config: DestinationConfig):
        # Load env vars
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
            raise ValueError(f"Missing: account={self.account}, user={self.user}, warehouse={self.warehouse}, database={self.database}, table={self.table}")
        
        # Accumulate records
        self.accumulated_records = []
        
        # Test connection
        try:
            conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            conn.close()
            logger.info(f"SnowflakeDestination initialized: {self.database}.{self.schema}.{self.table}")
        except Exception as e:
            raise ValueError(f"Failed to connect to Snowflake: {e}")
    
    def _load_env_vars(self):
        """Load environment variables from .env file manually."""
        env_vars = {}
        env_file = Path('.env')
        if env_file.exists():
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        env_vars[key] = value
        return env_vars

    def write(self, records: Iterable[Dict[str, Any]]):
        """Akkumulerer records. Actual write skjer i finalize()."""
        records_list = list(records)
        logger.info(f"SnowflakeDestination.write() accumulating {len(records_list)} records")
        self.accumulated_records.extend(records_list)

    def finalize(self):
        """
        Skriver alle akkumulerte records til Snowflake.
        
        Strategi:
        1. Skriv records til lokal CSV fil
        2. PUT fil til Snowflake internal stage
        3. COPY INTO table fra stage
        4. Cleanup
        """
        if not self.accumulated_records:
            logger.info("No records to write to Snowflake")
            return
        
        conn = None
        cursor = None
        temp_csv = None
        
        try:
            # Connect to Snowflake
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
            
            # Write data to temporary CSV
            temp_csv = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', newline='')
            writer = csv.DictWriter(temp_csv, fieldnames=columns)
            writer.writeheader()
            writer.writerows(self.accumulated_records)
            temp_csv.close()
            
            logger.info(f"Writing {len(self.accumulated_records)} records to Snowflake via staging")
            
            # Create internal stage (if not exists)
            stage_name = f"conduit_stage_{self.table}"
            cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")
            
            # PUT file to stage
            put_command = f"PUT file://{temp_csv.name} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            cursor.execute(put_command)
            
            # COPY INTO table from stage
            csv_filename = Path(temp_csv.name).name
            copy_command = f"""
                COPY INTO {self.table}
                FROM @{stage_name}/{csv_filename}.gz
                FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
                ON_ERROR = ABORT_STATEMENT
            """
            cursor.execute(copy_command)
            
            # Get load results
            result = cursor.fetchone()
            logger.info(f"✅ Successfully loaded {result[0]} rows into Snowflake")
            
            # Cleanup stage
            cursor.execute(f"REMOVE @{stage_name}")
            
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
        """Create table if it doesn't exist with basic VARCHAR columns."""
        # Simple approach: all VARCHAR(16777216) for now
        column_defs = ", ".join([f"{col} VARCHAR(16777216)" for col in columns])
        
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                {column_defs}
            )
        """
        
        cursor.execute(create_table_sql)
        logger.info(f"Ensured table {self.table} exists")