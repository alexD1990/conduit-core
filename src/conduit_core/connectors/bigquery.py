# src/conduit_core/connectors/bigquery.py
import logging
from typing import Iterable, Dict, Any, Optional
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError, NotFound
from decimal import Decimal
from datetime import date, datetime
from .base import BaseDestination
from ..config import Destination as DestinationConfig
from ..errors import ConnectionError

logger = logging.getLogger(__name__)


def _convert_for_bigquery(value):
    """Convert Python types to BigQuery-compatible JSON types."""
    if isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, (date, datetime)):
        return value.isoformat()
    elif isinstance(value, bytes):
        return value.decode('utf-8', errors='replace')
    return value


class BigQueryDestination(BaseDestination):
    """Writes data to a Google BigQuery table using Load Jobs."""

    def __init__(self, config: DestinationConfig):
        super().__init__(config)
        self.config = config
        self.project_id = config.project
        self.dataset_id = config.dataset
        self.table_name = config.table
        self.credentials_path = getattr(config, 'credentials_path', None)
        self.location = getattr(config, 'location', 'US')
        
        if not all([self.project_id, self.dataset_id, self.table_name]):
            raise ValueError("BigQueryDestination requires 'project', 'dataset', and 'table'.")

        self.client = self._get_client()
        self.table_id = f"{self.project_id}.{self.dataset_id}.{self.table_name}"
        self.accumulated_records = []
        self.mode = getattr(config, 'mode', 'append')
        
        logger.info(f"BigQueryDestination initialized for table: {self.table_id}")

    def _get_client(self) -> bigquery.Client:
        """Initializes the BigQuery client with appropriate credentials."""
        try:
            if self.credentials_path:
                credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
                return bigquery.Client(project=self.project_id, credentials=credentials, location=self.location)
            else:
                return bigquery.Client(project=self.project_id, location=self.location)
        except Exception as e:
            raise ConnectionError(f"BigQuery authentication failed: {e}") from e

    def test_connection(self) -> bool:
        """Test BigQuery connection and dataset access."""
        try:
            dataset_ref = f"{self.project_id}.{self.dataset_id}"
            self.client.get_dataset(dataset_ref)
            return True
        except Exception as e:
            error_msg = str(e)
            suggestions = []
            
            if "404" in error_msg or "not found" in error_msg.lower():
                suggestions.append(f"Ensure the dataset exists: bq mk {self.project_id}:{self.dataset_id}")
                suggestions.append("Check the dataset name for typos.")
            elif "403" in error_msg or "permission" in error_msg.lower():
                suggestions.append("Check IAM permissions: 'BigQuery Data Editor' and 'BigQuery Job User' roles are recommended.")
                suggestions.append("Verify the service account or your user account has access to the project.")
            elif "credentials" in error_msg.lower():
                suggestions.append("Check that your credentials_path is correct (if using a service account).")
                suggestions.append("Verify the GOOGLE_APPLICATION_CREDENTIALS environment variable or run `gcloud auth application-default login`.")
            
            suggestion_str = "\n".join(f"  • {s}" for s in suggestions)
            raise ConnectionError(
                f"BigQuery connection failed: {error_msg}\n\nSuggestions:\n{suggestion_str}"
            ) from e

    def execute_ddl(self, sql: str) -> None:
        """Execute DDL statement."""
        try:
            query_job = self.client.query(sql)
            query_job.result()
            logger.info("DDL executed successfully")
        except Exception as e:
            logger.error(f"BigQuery DDL execution failed: {e}")
            raise

    def write(self, records: Iterable[Dict[str, Any]]):
        """Accumulates records in memory."""
        # Convert Decimal and other types to JSON-serializable types
        converted_records = []
        for record in records:
            converted = {k: _convert_for_bigquery(v) for k, v in record.items()}
            converted_records.append(converted)
        self.accumulated_records.extend(converted_records)

    def finalize(self):
        """Loads all accumulated records into BigQuery using a Load Job."""
        if not self.accumulated_records:
            logger.info("No records to write to BigQuery.")
            return
        
        # Check if table exists
        table_exists = False
        try:
            self.client.get_table(self.table_id)
            table_exists = True
        except:
            pass

         # Handle schema evolution (removed columns)
        if hasattr(self, 'config') and self.config:
            from ..schema_evolution import SchemaEvolutionManager
            self.accumulated_records = SchemaEvolutionManager.inject_nulls_for_removed_columns(
                self.accumulated_records,
                getattr(self.config, '_removed_columns', [])
            )
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )
        
        if self.mode == 'full_refresh':
            if table_exists:
                # Delete table for full refresh
                self.client.delete_table(self.table_id, not_found_ok=True)
                logger.info(f"🗑️  Deleted table {self.table_id} (full_refresh mode)")
            # Use WRITE_EMPTY to create fresh table
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        
        try:
            load_job = self.client.load_table_from_json(
                self.accumulated_records,
                self.table_id,
                job_config=job_config,
            )
            load_job.result()
            
            if load_job.errors:
                raise ValueError(f"BigQuery load job failed: {load_job.errors}")
            
            logger.info(f"[OK] Successfully loaded {load_job.output_rows} rows to {self.table_id}")
            
        except Exception as e:
            logger.error(f"[FAIL] Unexpected error during BigQuery load job: {e}")
            raise
        finally:
            self.accumulated_records.clear()

    def execute_ddl(self, sql: str) -> None:
        """Execute DDL statement."""
        query_job = self.client.query(sql)
        query_job.result()
        logger.info("DDL executed successfully")

    def alter_table(self, alter_sql: str) -> None:
        """Execute ALTER TABLE statement."""
        self.execute_ddl(alter_sql)

    def table_exists(self) -> bool:
        """Check if the destination table exists."""
        try:
            self.client.get_table(self.table_id)
            return True
        except NotFound:
            return False
        except Exception as e:
            raise ValueError(f"Failed to check table existence: {e}")