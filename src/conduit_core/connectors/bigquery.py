# src/conduit_core/connectors/bigquery.py
import logging
from typing import Iterable, Dict, Any, Optional

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError, NotFound

from .base import BaseDestination
from ..config import Destination as DestinationConfig
from ..errors import ConnectionError

logger = logging.getLogger(__name__)


class BigQueryDestination(BaseDestination):
    """Writes data to a Google BigQuery table using Load Jobs."""

    def __init__(self, config: DestinationConfig):
        super().__init__(config)
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
        try:
            query_job = self.client.query(sql)
            query_job.result()
            logger.info("DDL executed successfully")
        except Exception as e:
            logger.error(f"BigQuery DDL execution failed: {e}")
            raise

    def alter_table(self, alter_sql: str) -> None:
        """Execute ALTER TABLE statement."""
        self.execute_ddl(alter_sql)

    # --- Phase 3 additions ---
    def get_table_schema(self) -> Optional[Dict[str, Any]]:
        """Get table schema from BigQuery"""
        from google.api_core.exceptions import NotFound

        try:
            table = self.client.get_table(self.table_id)

            schema = {}
            for field in table.db_schema:
                internal_type = self._map_bq_type_to_internal(field.field_type)
                schema[field.name] = {
                    'type': internal_type,
                    'nullable': (field.mode != 'REQUIRED')
                }

            return schema
        except NotFound:
            logger.info(f"Table '{self.table_id}' not found, returning None.")
            return None
        except Exception as e:
            logger.error(f"Failed to get table schema for '{self.table_id}': {e}")
            raise ConnectionError(f"Failed to get table schema: {e}") from e

    def _map_bq_type_to_internal(self, bq_type: str) -> str:
        """Map BigQuery type to internal schema type"""
        type_mapping = {
            'INTEGER': 'integer',
            'INT64': 'integer',
            'FLOAT': 'float',
            'FLOAT64': 'float',
            'NUMERIC': 'decimal',
            'BIGNUMERIC': 'decimal',
            'BOOLEAN': 'boolean',
            'BOOL': 'boolean',
            'DATE': 'date',
            'DATETIME': 'datetime',
            'TIMESTAMP': 'datetime',
            'STRING': 'string',
        }
        return type_mapping.get(bq_type.upper(), 'string')
    # --- End Phase 3 additions ---

    def write(self, records: Iterable[Dict[str, Any]]):
        """Accumulates records in memory."""
        self.accumulated_records.extend(list(records))

    def finalize(self):
        """Loads all accumulated records into BigQuery using a Load Job."""
        if not self.accumulated_records:
            logger.info("No records to write to BigQuery.")
            return

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
        )

        if self.mode == 'full_refresh':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
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
            
            logger.info(f"✅ Successfully loaded {load_job.output_rows} rows to {self.table_id}")

        except NotFound:
            if self.config.auto_create_table:
                logger.error(f"Table '{self.table_id}' not found. Auto-create failed or skipped.")
                raise ValueError(f"Table '{self.table_id}' not found. Auto-create failed or skipped.")
            else:
                raise ValueError(f"The BigQuery table '{self.table_id}' does not exist. Enable 'auto_create_table' in your destination config to create it.") from None
        except Exception as e:
            logger.error(f"❌ Unexpected error during BigQuery load job: {e}")
            raise
        finally:
            self.accumulated_records.clear()
