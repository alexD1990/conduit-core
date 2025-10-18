# tests/integration/test_auto_detection.py

import pytest
from pathlib import Path

from conduit_core.config import Source, Destination, Resource, IngestConfig
from conduit_core.schema import SchemaInferrer, CsvDelimiterDetector
from conduit_core.engine import run_resource

# Define fixtures for test setup (e.g., config, sample files)
@pytest.fixture
def fixtures_dir():
    return Path(__file__).parent.parent / "fixtures/data"

@pytest.fixture
def base_config(tmp_path, fixtures_dir):
    source_path = tmp_path / "source.csv"
    dest_path = tmp_path / "dest.jsonl"
    config = IngestConfig(
        sources=[Source(name="test_source", type="csv", path=str(source_path))],
        destinations=[Destination(name="test_dest", type="json", path=str(dest_path), format="jsonl")],
        resources=[Resource(name="test_resource", source="test_source", destination="test_dest", query="n/a")]
    )
    return config, source_path, dest_path

# Tests for automatic delimiter detection
def test_detect_comma_delimiter(fixtures_dir):
    delimiter = CsvDelimiterDetector.detect_delimiter(fixtures_dir / "comma_delim.csv")
    assert delimiter == ','

def test_detect_semicolon_delimiter(fixtures_dir):
    delimiter = CsvDelimiterDetector.detect_delimiter(fixtures_dir / "semicolon_delim.csv")
    assert delimiter == ';'

def test_detect_tab_delimiter(fixtures_dir):
    delimiter = CsvDelimiterDetector.detect_delimiter(fixtures_dir / "tab_delim.tsv")
    assert delimiter == '\t'

# Tests for automatic schema inference
def test_schema_inference_integration(fixtures_dir):
    from conduit_core.connectors.csv import CsvSource
    source_config = Source(name="test", type="csv", path=str(fixtures_dir / "comma_delim.csv"))
    source = CsvSource(source_config)
    records = list(source.read())
    schema = SchemaInferrer.infer_schema(records)
    
    # *** FIX: Assert based on new schema format ***
    assert "columns" in schema
    columns_dict = {col['name']: col for col in schema['columns']}
    assert "id" in columns_dict
    assert "name" in columns_dict
    assert "age" in columns_dict
    assert columns_dict["id"]["type"] == "integer"
    assert columns_dict["name"]["type"] == "string"
    assert columns_dict["age"]["type"] == "integer"
    assert not columns_dict["id"]["nullable"] # Assuming ID is never null

@pytest.mark.skip(reason="Full integration test needs more setup")
def test_pipeline_with_auto_detection(base_config, tmp_path, caplog):
    config, source_path, dest_path = base_config
    
    # Prepare a semicolon-delimited CSV
    csv_content = "id;name;value\n1;test;10.5\n2;another;20.0"
    source_path.write_text(csv_content)

    # Modify config to NOT specify delimiter
    # config.sources[0].delimiter = None # Assuming delimiter is optional
    
    # Enable schema inference
    config.sources[0].infer_schema = True

    # Run the pipeline
    run_resource(config.resources[0], config)

    # Assertions
    # 1. Delimiter was detected (check logs or connector state if possible)
    assert "Detected delimiter: ';'" in caplog.text 
    
    # 2. Schema was inferred (check logs)
    assert "Schema inferred: 3 columns" in caplog.text
    
    # 3. Data was written correctly
    assert dest_path.exists()
    lines = dest_path.read_text().strip().split('\n')
    assert len(lines) == 2
    import json
    assert json.loads(lines[0]) == {"id": "1", "name": "test", "value": "10.5"} # CSV reads as string by default