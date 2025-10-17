# src/conduit_core/connectors/bigquery.py

import logging
from typing import Iterable, Dict, Any

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError, NotFound

from .base import BaseDestination
from ..config import Destination as DestinationConfig

logger = logging.getLogger(__name__)


class BigQueryDestination(BaseDestination):
    """Writes data to a Google BigQuery table using Load Jobs."""

    def __init__(self, config: DestinationConfig):
        self.project_id = config.project
        self.dataset_id = config.dataset
        self.table_name = config.table
        self.credentials_path = getattr(config, 'credentials_path', None)
        self.location = getattr(config, 'location', 'US')
        self.mode = config.mode or 'append'
        
        if not all([self.project_id, self.dataset_id, self.table_name]):
            raise ValueError(
                "BigQueryDestination requires 'project', 'dataset', and 'table'."
            )

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

        # Check destination config mode, not self.mode
        if self.mode == 'full_refresh':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:  # Default to append
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
            raise ValueError(f"The BigQuery table '{self.table_id}' does not exist.") from None
        except Exception as e:
            logger.error(f"❌ An unexpected error occurred during the BigQuery load job: {e}")
            raise
        finally:
            self.accumulated_records.clear()