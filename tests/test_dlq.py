# tests/test_dlq.py

import pytest
import json
from pathlib import Path
from conduit_core.errors import ErrorLog
from conduit_core.connectors.base import BaseSource, BaseDestination
from conduit_core.connectors.dummy import DummySource, DummyDestination
from conduit_core.config import Source, Destination, Resource, IngestConfig
from conduit_core.engine import run_resource


class FailingDestination(BaseDestination):
    """En test-destination som feiler på spesifikke records."""
    
    def __init__(self, *args, **kwargs):
        self.written_records = []
        self.fail_on_ids = [2]  # Fail when id == 2
    
    def write(self, records):
        """Batch write - not used in new pattern."""
        for record in records:
            self.write_one(record)
    
    def write_one(self, record):
        """Fail if record id is in fail_on_ids."""
        if record.get('id') in self.fail_on_ids:
            raise ValueError(f"Simulated failure for id={record['id']}")
        self.written_records.append(record)


def test_error_log_creation():
    """Test at ErrorLog kan lages og lagre errors."""
    error_log = ErrorLog("test_resource")
    
    # Legg til noen errors
    error_log.add_error(
        record={'id': 1, 'name': 'Bad Data'},
        error=ValueError("Invalid value"),
        row_number=5
    )
    
    assert error_log.has_errors()
    assert error_log.error_count() == 1


def test_error_log_saves_to_file(tmp_path):
    """Test at error log blir lagret til fil."""
    error_dir = tmp_path / "errors"
    error_log = ErrorLog("test_resource", error_dir=error_dir)
    
    error_log.add_error(
        record={'id': 1, 'name': 'Bad'},
        error=ValueError("Test error"),
        row_number=1
    )
    
    error_file = error_log.save()
    
    assert error_file is not None
    assert error_file.exists()
    
    # Sjekk innholdet
    with open(error_file, 'r') as f:
        data = json.load(f)
    
    assert data['resource'] == 'test_resource'
    assert data['total_errors'] == 1
    assert data['errors'][0]['record']['id'] == 1


def test_engine_handles_partial_failures(tmp_path, monkeypatch):
    """Test at engine fortsetter ved feil og logger til DLQ."""
    
    # Mock connector registry - CORRECT way
    import conduit_core.connectors.registry as registry
    
    original_source_map = registry._SOURCE_CONNECTOR_MAP
    original_dest_map = registry._DESTINATION_CONNECTOR_MAP
    
    # Set mocked maps
    registry._SOURCE_CONNECTOR_MAP = {'dummysource': DummySource}
    registry._DESTINATION_CONNECTOR_MAP = {'failingdest': FailingDestination}
    
    try:
        # Lag config
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
        
        # Kjør resource
        resource = config.resources[0]
        
        # Change working directory temporarily so errors go to tmp
        import os
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            # This should NOT crash even though record id=2 fails
            run_resource(resource, config)
            
            # Sjekk at error log ble laget
            error_dir = Path("./errors")
            error_files = list(error_dir.glob("test_resource_errors_*.json"))
            assert len(error_files) > 0, "No error log file was created"
            
            # Sjekk innholdet
            with open(error_files[0], 'r') as f:
                error_data = json.load(f)
            
            assert error_data['total_errors'] == 1
            assert error_data['errors'][0]['record']['id'] == 2
            
        finally:
            os.chdir(original_cwd)
    
    finally:
        # Restore original registry
        registry._SOURCE_CONNECTOR_MAP = original_source_map
        registry._DESTINATION_CONNECTOR_MAP = original_dest_map


def test_no_error_log_when_all_succeed(tmp_path, monkeypatch):
    """Test at ingen error log lages når alt går bra."""
    
    import conduit_core.connectors.registry as registry
    
    original_source_map = registry._SOURCE_CONNECTOR_MAP
    original_dest_map = registry._DESTINATION_CONNECTOR_MAP
    
    # Set mocked maps
    registry._SOURCE_CONNECTOR_MAP = {'dummysource': DummySource}
    registry._DESTINATION_CONNECTOR_MAP = {'dummydestination': DummyDestination}
    
    try:
        config = IngestConfig(
            sources=[Source(name='test_source', type='dummysource')],
            destinations=[Destination(name='test_dest', type='dummydestination')],
            resources=[Resource(
                name='test_resource',
                source='test_source',
                destination='test_dest',
                query='SELECT 1'
            )]
        )
        
        import os
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            run_resource(config.resources[0], config)
            
            # Ingen error files skal eksistere
            error_dir = Path("./errors")
            if error_dir.exists():
                error_files = list(error_dir.glob("*.json"))
                assert len(error_files) == 0, f"Unexpected error files: {error_files}"
        
        finally:
            os.chdir(original_cwd)
    
    finally:
        registry._SOURCE_CONNECTOR_MAP = original_source_map
        registry._DESTINATION_CONNECTOR_MAP = original_dest_map