# tests/connectors/test_json_connector.py

import pytest
import json
from pathlib import Path
from conduit_core.connectors.json import JsonSource, JsonDestination
from conduit_core.config import Source as SourceConfig
from conduit_core.config import Destination as DestinationConfig

@pytest.fixture
def sample_data():
    """A reusable list of test records."""
    return [
        {"id": 1, "name": "Alice", "active": True},
        {"id": 2, "name": "Bob", "active": False},
        {"id": 3, "name": "Charlie", "active": True},
    ]

def test_json_destination(tmp_path, sample_data):
    """Test that JsonDestination writes data correctly using the finalize pattern."""
    output_file = tmp_path / "output.json"
    config = DestinationConfig(name="test_dest", type="json", path=str(output_file))
    destination = JsonDestination(config)

    # Action
    destination.write(sample_data)
    destination.finalize()

    # Verification
    assert output_file.exists()
    with output_file.open('r') as f:
        data_from_file = json.load(f)
    
    assert data_from_file == sample_data

def test_json_source(tmp_path, sample_data):
    """Test that JsonSource reads data correctly."""
    input_file = tmp_path / "input.json"
    with input_file.open('w') as f:
        json.dump(sample_data, f)
    
    config = SourceConfig(name="test_src", type="json", path=str(input_file))
    source = JsonSource(config)

    # Action
    records = list(source.read())

    # Verification
    assert records == sample_data

def test_json_roundtrip(tmp_path, sample_data):
    """Test writing with JsonDestination and reading back with JsonSource."""
    file_path = tmp_path / "roundtrip.json"
    
    # Write data
    dest_config = DestinationConfig(name="dest", type="json", path=str(file_path))
    destination = JsonDestination(dest_config)
    destination.write(sample_data)
    destination.finalize()

    # Read data back
    source_config = SourceConfig(name="src", type="json", path=str(file_path))
    source = JsonSource(source_config)
    records_from_source = list(source.read())

    # Verification
    assert records_from_source == sample_data

def test_destination_handles_empty_records(tmp_path):
    """Test that writing an empty list does not create a file."""
    output_file = tmp_path / "output.json"
    config = DestinationConfig(name="test_dest", type="json", path=str(output_file))
    destination = JsonDestination(config)

    destination.write([])
    destination.finalize()

    assert not output_file.exists()