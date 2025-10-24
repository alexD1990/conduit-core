# tests/test_dlq.py

import pytest
import json
import os
from pathlib import Path
from typing import Iterable, Dict, Any
from unittest.mock import patch

from conduit_core.errors import ErrorLog
from conduit_core.connectors.base import BaseDestination
from conduit_core.connectors.dummy import DummySource, DummyDestination
from conduit_core.config import Source, Destination, Resource, IngestConfig
from conduit_core.engine import run_resource

class FailingDestination(BaseDestination):
    """A test destination that fails on specific records during batch write."""
    def __init__(self, config: Any):
        super().__init__(config)
        self.written_records = []
        self.fail_on_ids = [2] # DummySource produces IDs 1, 2, 3

    def write(self, records: Iterable[Dict[str, Any]]):
        """
        Processes records, fails if any record's ID is in fail_on_ids.
        Simulates a batch write failure.
        """
        batch_records = list(records) # Consume iterator
        for record in batch_records:
            if record.get('id') in self.fail_on_ids:
                # Simulate the entire batch write failing
                raise ValueError(f"Simulated batch failure due to id={record['id']}")
        # Only reached if no failure occurred
        self.written_records.extend(batch_records)

    def write_one(self, record: Dict[str, Any]):
        # Keep for completeness, though engine won't call it if write exists
        if record.get('id') in self.fail_on_ids:
            raise ValueError(f"Simulated failure for id={record['id']}")
        self.written_records.append(record)

    def finalize(self):
        pass

@pytest.fixture
def test_setup():
    """Mocks the connector registry functions for isolated testing."""
    original_dummy_dest = DummyDestination

    with patch('conduit_core.engine.get_source_connector_map') as mock_get_source, \
         patch('conduit_core.engine.get_destination_connector_map') as mock_get_dest:

        mock_get_source.return_value = {'dummysource': DummySource}
        mock_get_dest.return_value = {
            'failingdest': FailingDestination,
            'dummydestination': original_dummy_dest
        }
        yield

def test_engine_handles_partial_failures(tmp_path, test_setup):
    """
    Test that the engine catches batch-level errors, logs them to DLQ,
    and completes the run.
    """
    config = IngestConfig(
        sources=[Source(name='test_source', type='dummysource')],
        destinations=[Destination(name='test_dest', type='failingdest', table='dummy')],
        resources=[Resource(
            name='test_resource',
            source='test_source',
            destination='test_dest',
            query='SELECT 1' # DummySource yields records [1, 2, 3]
        )]
    )

    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Run should NOT raise an exception
        run_resource(config.resources[0], config, batch_size=10, skip_preflight=True) # Process all 3 records

        # Assert directly against the saved error log file
        error_dir = tmp_path / "errors"
        assert error_dir.exists(), "Error directory was not created."
        error_files = list(error_dir.glob("test_resource_errors_*.json"))
        assert len(error_files) == 1, f"Expected exactly one error log file, found {len(error_files)}"

        with error_files[0].open('r') as f:
            error_data = json.load(f)

        # Check file content
        assert error_data['resource'] == 'test_resource'
        expected_error_count = 3 # DummySource yields 3 records. Batch fails on id=2. Engine logs all 3.
        assert error_data['total_errors'] == expected_error_count, f"Expected {expected_error_count} total errors in file"
        assert error_data['processing_errors_count'] == expected_error_count, f"Expected {expected_error_count} processing errors"
        assert error_data['quality_errors_count'] == 0
        assert len(error_data['errors']) == expected_error_count

        # *** FIX: Assert the correct IDs based on DummySource (1, 2, 3) ***
        failed_ids = {e['record']['id'] for e in error_data['errors']}
        assert failed_ids == {1, 2, 3}

        # Check error type for one of them
        assert error_data['errors'][0]['error_type'] == 'ValueError'
        assert "Simulated batch failure" in error_data['errors'][0]['error_message']

    finally:
        os.chdir(original_cwd)


# (Rest of tests remain unchanged)
def test_no_error_log_when_all_succeed(tmp_path, test_setup):
    config = IngestConfig(
        sources=[Source(name='test_source', type='dummysource')],
        destinations=[Destination(name='test_dest', type='dummydestination', table='dummy')],
        resources=[Resource(
            name='test_resource_success',
            source='test_source',
            destination='test_dest',
            query='SELECT 1'
        )]
    )
    original_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        run_resource(config.resources[0], config, skip_preflight=True)
        error_dir = tmp_path / "errors"
        assert not error_dir.exists(), "Error directory should not be created on a successful run."
    finally:
        os.chdir(original_cwd)

def test_error_log_creation_and_add():
    error_log = ErrorLog("test_resource")
    error_log.add_error(record={'id': 1}, error=ValueError("Invalid"), row_number=5)
    assert error_log.has_errors()
    assert error_log.error_count() == 1
    assert len(error_log.errors) == 1
    assert len(error_log.quality_errors) == 0

def test_error_log_add_quality_error():
    error_log = ErrorLog("test_resource")
    error_log.add_quality_error(record={'id': 2}, failed_checks="col(check): Fail", row_number=6)
    assert error_log.has_errors()
    assert error_log.error_count() == 1
    assert len(error_log.errors) == 0
    assert len(error_log.quality_errors) == 1

def test_error_log_saves_to_file(tmp_path):
    error_dir = tmp_path / "errors_test_save"
    error_log = ErrorLog("test_save_resource", error_dir=error_dir)
    error_log.add_error(record={'id': 1}, error=TypeError("Bad"), row_number=1)
    error_log.add_quality_error(record={'id': 2}, failed_checks="col(check): Details", row_number=2)
    error_file = error_log.save()
    assert error_file is not None and error_file.exists()
    assert error_file.parent == error_dir
    with open(error_file, 'r') as f: data = json.load(f)
    assert data['resource'] == 'test_save_resource'
    assert data['total_errors'] == 2
    assert data['processing_errors_count'] == 1
    assert data['quality_errors_count'] == 1