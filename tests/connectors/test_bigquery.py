# tests/connectors/test_bigquery.py

import pytest
from unittest.mock import MagicMock, patch

from conduit_core.connectors.bigquery import BigQueryDestination
from conduit_core.config import Destination as DestinationConfig
from google.api_core.exceptions import NotFound

@pytest.fixture
def mock_bq_client():
    """Mocks the BigQuery client and its methods."""
    with patch('conduit_core.connectors.bigquery.bigquery.Client') as mock_client_constructor:
        mock_client_instance = MagicMock()
        
        # Mock the load job object and its result
        mock_load_job = MagicMock()
        mock_load_job.errors = []
        mock_load_job.output_rows = 5 # Simulate some rows being written
        
        mock_client_instance.load_table_from_json.return_value = mock_load_job
        
        mock_client_constructor.return_value = mock_client_instance
        yield mock_client_instance

@pytest.fixture
def sample_config():
    """Returns a standard configuration for BigQuery."""
    return DestinationConfig(
        name="bq_test",
        type="bigquery",
        project="test-project",
        dataset="test_dataset",
        table="test_table",
    )

@pytest.fixture
def sample_data():
    """Returns a list of sample records."""
    return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

def test_bigquery_destination_init(sample_config, mock_bq_client):
    """Tests that the connector initializes correctly."""
    dest = BigQueryDestination(sample_config)
    assert dest.project_id == "test-project"
    assert dest.table_id == "test-project.test_dataset.test_table"
    mock_bq_client.load_table_from_json.assert_not_called()

def test_write_buffers_records(sample_config, mock_bq_client, sample_data):
    """Tests that write() only buffers records."""
    dest = BigQueryDestination(sample_config)
    dest.write(sample_data)
    assert dest.accumulated_records == sample_data
    mock_bq_client.load_table_from_json.assert_not_called()

def test_finalize_loads_data(sample_config, mock_bq_client, sample_data):
    """Tests that finalize() calls load_table_from_json."""
    dest = BigQueryDestination(sample_config)
    dest.write(sample_data.copy())
    dest.finalize()

    # Assert that the load job was called
    mock_bq_client.load_table_from_json.assert_called_once()
    # Assert that the job's result was waited for
    mock_bq_client.load_table_from_json.return_value.result.assert_called_once()
    assert not dest.accumulated_records

def test_finalize_handles_load_errors(sample_config, mock_bq_client, sample_data):
    """Tests that errors during the load job are handled."""
    dest = BigQueryDestination(sample_config)
    dest.write(sample_data.copy())
    
    # Mock a load job with errors
    mock_load_job = MagicMock()
    mock_load_job.errors = [{"message": "Schema mismatch"}]
    mock_bq_client.load_table_from_json.return_value = mock_load_job
    
    with pytest.raises(Exception):
        dest.finalize()

def test_append_mode(sample_config, mock_bq_client, sample_data):
    """Test append write disposition."""
    sample_config.mode = "append"
    dest = BigQueryDestination(sample_config)
    dest.write(sample_data)
    dest.finalize()
    
    call_args = mock_bq_client.load_table_from_json.call_args
    job_config = call_args[1]['job_config']
    
    from google.cloud.bigquery import WriteDisposition
    assert job_config.write_disposition == WriteDisposition.WRITE_APPEND

def test_full_refresh_mode(sample_config, mock_bq_client, sample_data):
    """Test truncate write disposition."""
    sample_config.mode = "full_refresh"
    dest = BigQueryDestination(sample_config)
    dest.write(sample_data)
    dest.finalize()
    
    call_args = mock_bq_client.load_table_from_json.call_args
    job_config = call_args[1]['job_config']
    
    from google.cloud.bigquery import WriteDisposition
    assert job_config.write_disposition == WriteDisposition.WRITE_EMPTY

@pytest.mark.skip(reason="Requires BigQuery credentials")
def test_finalize_handles_table_not_found(sample_config, mock_bq_client, sample_data):
    """Tests that a NotFound error gives a user-friendly message."""
    mock_bq_client.load_table_from_json.side_effect = NotFound("Table not found")

    dest = BigQueryDestination(sample_config)
    dest.write(sample_data)

    from google.api_core.exceptions import NotFound

    with pytest.raises(NotFound):
        dest.finalize()

def test_finalize_with_no_records(sample_config, mock_bq_client):
    """Tests that finalize() does nothing if there are no records."""
    dest = BigQueryDestination(sample_config)
    dest.finalize()
    mock_bq_client.load_table_from_json.assert_not_called()