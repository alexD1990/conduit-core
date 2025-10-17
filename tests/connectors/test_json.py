# tests/connectors/test_json.py

import pytest
import json
from pathlib import Path

from conduit_core.connectors.json import JsonSource, JsonDestination
from conduit_core.config import Source, Destination
from conduit_core.errors import ConnectionError # Added import

# ... (dine fixtures er uendret) ...
@pytest.fixture
def sample_data():
    return [{"id": 1, "name": "Alice", "age": 30}, {"id": 2, "name": "Bob", "age": 25}, {"id": 3, "name": "Charlie", "age": 35}]

@pytest.fixture
def json_array_file(sample_data, tmp_path):
    file_path = tmp_path / "test.json"
    with open(file_path, 'w') as f: json.dump(sample_data, f)
    return file_path

@pytest.fixture
def ndjson_file(sample_data, tmp_path):
    file_path = tmp_path / "test.ndjson"
    with open(file_path, 'w') as f:
        for record in sample_data: f.write(json.dumps(record) + '\n')
    return file_path

# ... (de fleste testene er uendret) ...
def test_json_source_read_array(json_array_file, sample_data):
    config = Source(name="test", type="json", path=str(json_array_file))
    source = JsonSource(config)
    records = list(source.read())
    assert records == sample_data

def test_json_source_read_ndjson(ndjson_file, sample_data):
    config = Source(name="test", type="json", path=str(ndjson_file), format="ndjson")
    source = JsonSource(config)
    records = list(source.read())
    assert records == sample_data

def test_json_source_file_not_found():
    """Test error when file doesn't exist."""
    config = Source(name="test", type="json", path="/nonexistent/file.json")
    source = JsonSource(config)
    # --- FIX: Error is raised on read(), not __init__() ---
    with pytest.raises(FileNotFoundError):
        list(source.read())

def test_json_source_test_connection_invalid(tmp_path):
    """Test connection validation with invalid JSON."""
    file_path = tmp_path / "invalid.json"
    file_path.write_text("{'invalid': json}")
    config = Source(name="test", type="json", path=str(file_path))
    source = JsonSource(config)
    # --- FIX: Match the actual error message ---
    with pytest.raises(ConnectionError, match="Invalid JSON format"):
        source.test_connection()

def test_json_destination_write_ndjson(sample_data, tmp_path):
    """Test writing NDJSON format."""
    file_path = tmp_path / "output.ndjson"
    # --- FIX: Use the Pydantic model correctly ---
    config = Destination(name="test", type="json", path=str(file_path), format="ndjson")
    destination = JsonDestination(config)
    destination.write(sample_data)
    destination.finalize()
    
    assert file_path.exists()
    with open(file_path, 'r') as f:
        result = [json.loads(line) for line in f if line.strip()]
    assert result == sample_data

# ... (resten av testene dine er uendret) ...
def test_json_source_read_single_object(tmp_path):
    file_path = tmp_path / "single.json"
    data = {"id": 1, "name": "Alice"}
    with open(file_path, 'w') as f: json.dump(data, f)
    config = Source(name="test", type="json", path=str(file_path))
    source = JsonSource(config)
    records = list(source.read())
    assert len(records) == 1 and records[0] == data

def test_json_source_empty_file(tmp_path):
    file_path = tmp_path / "empty.json"
    file_path.write_text("")
    config = Source(name="test", type="json", path=str(file_path))
    source = JsonSource(config)
    records = list(source.read())
    assert len(records) == 0

def test_json_source_test_connection_valid(json_array_file):
    config = Source(name="test", type="json", path=str(json_array_file))
    source = JsonSource(config)
    assert source.test_connection()

def test_json_destination_write_array(sample_data, tmp_path):
    file_path = tmp_path / "output.json"
    config = Destination(name="test", type="json", path=str(file_path))
    destination = JsonDestination(config)
    destination.write(sample_data)
    destination.finalize()
    assert file_path.exists()
    with open(file_path, 'r') as f:
        result = json.load(f)
    assert result == sample_data

def test_json_destination_creates_directory(sample_data, tmp_path):
    file_path = tmp_path / "nested" / "dir" / "output.json"
    config = Destination(name="test", type="json", path=str(file_path))
    destination = JsonDestination(config)
    destination.write(sample_data)
    destination.finalize()
    assert file_path.exists()

def test_json_roundtrip(sample_data, tmp_path):
    file_path = tmp_path / "roundtrip.json"
    dest_config = Destination(name="dest", type="json", path=str(file_path))
    destination = JsonDestination(dest_config)
    destination.write(sample_data)
    destination.finalize()
    source_config = Source(name="src", type="json", path=str(file_path))
    source = JsonSource(source_config)
    records = list(source.read())
    assert records == sample_data

def test_json_destination_empty_write(tmp_path):
    file_path = tmp_path / "empty.json"
    config = Destination(name="test", type="json", path=str(file_path))
    destination = JsonDestination(config)
    destination.finalize()
    assert not file_path.exists()

def test_json_handles_unicode(tmp_path):
    data = [{"id": 1, "name": "Åse"}, {"id": 2, "name": "José"}]
    file_path = tmp_path / "unicode.json"
    dest_config = Destination(name="dest", type="json", path=str(file_path))
    destination = JsonDestination(dest_config)
    destination.write(data)
    destination.finalize()
    source_config = Source(name="src", type="json", path=str(file_path))
    source = JsonSource(source_config)
    records = list(source.read())
    assert records == data