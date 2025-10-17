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
    """A test destination that fails on specific records."""
    
    def __init__(self, *args, **kwargs):
        self.written_records = []
        self.fail_on_ids = [2]  # Fail when id == 2
    
    def write(self, records: Iterable[Dict[str, Any]]):
        """Not used by this test, as it relies on write_one."""
        pass
    
    def write_one(self, record: Dict[str, Any]):
        """Fails if the record's ID is in the fail_on_ids list."""
        if record.get('id') in self.fail_on_ids:
            raise ValueError(f"Simulated failure for id={record['id']}")
        self.written_records.append(record)

    def finalize(self):
        pass

@pytest.fixture
def test_setup():
    """Mocks the connector registry functions for isolated testing."""
    with patch('conduit_core.engine.get_source_connector_map') as mock_get_source, \
         patch('conduit_core.engine.get_destination_connector_map') as mock_get_dest:
        
        # Configure the mocks to return our test connectors
        mock_get_source.return_value = {'dummysource': DummySource}
        mock_get_dest.return_value = {
            'failingdest': FailingDestination,
            'dummydestination': DummyDestination
        }
        
        yield # Run the test while the mocks are active

def test_engine_handles_partial_failures(tmp_path, test_setup):
    """Test that the engine continues on record-level errors and logs to DLQ."""
    config = IngestConfig(
        sources=[Source(name='test_source', type='dummysource')],
        destinations=[Destination(name='test_dest', type='failingdest')],
        resources=[Resource(
            name='test_resource',
            source='test_source',
            destination='test_dest',
            query='SELECT 1'
        )]
    )
    
    original_cwd = os.getcwd()
    os.chdir(tmp_path)
    
    try:
        # This should NOT raise an exception.
        run_resource(config.resources[0], config)
        
        error_dir = Path("./errors")
        assert error_dir.exists(), "Error directory was not created."
        error_files = list(error_dir.glob("test_resource_errors_*.json"))
        assert len(error_files) == 1, "Expected exactly one error log file."
        
        with error_files[0].open('r') as f:
            error_data = json.load(f)
        
        assert error_data['total_errors'] == 1
        assert error_data['errors'][0]['record']['id'] == 2
    finally:
        os.chdir(original_cwd)

def test_no_error_log_when_all_succeed(tmp_path, test_setup):
    """Test that no error log is created on a successful run."""
    config = IngestConfig(
        sources=[Source(name='test_source', type='dummysource')],
        destinations=[Destination(name='test_dest', type='dummydestination')],
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
        run_resource(config.resources[0], config)
        
        error_dir = Path("./errors")
        assert not error_dir.exists(), "Error directory should not be created on a successful run."
    finally:
        os.chdir(original_cwd)

# The simple tests below don't need the complex setup fixture
def test_error_log_creation():
    """Test that ErrorLog can be created and can store errors."""
    error_log = ErrorLog("test_resource")
    error_log.add_error(
        record={'id': 1, 'name': 'Bad Data'},
        error=ValueError("Invalid value"),
        row_number=5
    )
    assert error_log.has_errors()
    assert error_log.error_count() == 1

def test_error_log_saves_to_file(tmp_path):
    """Test that the error log is saved to a file correctly."""
    error_dir = tmp_path / "errors"
    error_log = ErrorLog("test_resource", error_dir=error_dir)
    error_log.add_error(
        record={'id': 1, 'name': 'Bad'},
        error=ValueError("Test error"),
        row_number=1
    )
    error_file = error_log.save()
    
    assert error_file is not None and error_file.exists()
    
    with open(error_file, 'r') as f:
        data = json.load(f)
    assert data['resource'] == 'test_resource'
    assert data['total_errors'] == 1