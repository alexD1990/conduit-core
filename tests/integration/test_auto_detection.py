# tests/integration/test_auto_detection.py

import pytest
from pathlib import Path
from conduit_core.config import IngestConfig, Source, Destination, Resource
from conduit_core.engine import run_resource
from conduit_core.schema import SchemaInferrer
import json


@pytest.fixture
def fixtures_dir():
    return Path(__file__).parent.parent / "fixtures" / "data"


@pytest.fixture
def output_dir(tmp_path):
    return tmp_path / "output"


@pytest.mark.skip(reason="JSON connector not in v1.0")
def test_auto_detect_comma_delimiter(fixtures_dir, output_dir):
    """Test that comma delimiter is auto-detected"""
    output_dir.mkdir(exist_ok=True)

    config = IngestConfig(
        sources=[Source(name="src", type="csv", path=str(fixtures_dir / "comma_delim.csv"))],
        destinations=[Destination(name="dest", type="json", path=str(output_dir / "comma_out.json"))],
        resources=[Resource(name="transfer", source="src", destination="dest", query="n/a")],
    )

    run_resource(config.resources[0], config)

    with open(output_dir / "comma_out.json", "r") as f:
        data = json.load(f)

    assert len(data) == 3
    assert data[0]["name"] == "Alice"


@pytest.mark.skip(reason="JSON connector not in v1.0")
def test_auto_detect_semicolon_delimiter(fixtures_dir, output_dir):
    """Test that semicolon delimiter is auto-detected"""
    output_dir.mkdir(exist_ok=True)

    config = IngestConfig(
        sources=[Source(name="src", type="csv", path=str(fixtures_dir / "semicolon_delim.csv"))],
        destinations=[Destination(name="dest", type="json", path=str(output_dir / "semicolon_out.json"))],
        resources=[Resource(name="transfer", source="src", destination="dest", query="n/a")],
    )

    run_resource(config.resources[0], config)

    with open(output_dir / "semicolon_out.json", "r") as f:
        data = json.load(f)

    assert len(data) == 3
    assert data[1]["name"] == "Bob"


@pytest.mark.skip(reason="JSON connector not in v1.0")
def test_auto_detect_tab_delimiter(fixtures_dir, output_dir):
    """Test that tab delimiter is auto-detected"""
    output_dir.mkdir(exist_ok=True)

    config = IngestConfig(
        sources=[Source(name="src", type="csv", path=str(fixtures_dir / "tab_delim.csv"))],
        destinations=[Destination(name="dest", type="json", path=str(output_dir / "tab_out.json"))],
        resources=[Resource(name="transfer", source="src", destination="dest", query="n/a")],
    )

    run_resource(config.resources[0], config)

    with open(output_dir / "tab_out.json", "r") as f:
        data = json.load(f)

    assert len(data) == 3
    assert data[2]["name"] == "Charlie"


@pytest.mark.skip(reason="JSON connector not in v1.0")
def test_auto_detect_pipe_delimiter(fixtures_dir, output_dir):
    """Test that pipe delimiter is auto-detected"""
    output_dir.mkdir(exist_ok=True)

    config = IngestConfig(
        sources=[Source(name="src", type="csv", path=str(fixtures_dir / "pipe_delim.csv"))],
        destinations=[Destination(name="dest", type="json", path=str(output_dir / "pipe_out.json"))],
        resources=[Resource(name="transfer", source="src", destination="dest", query="n/a")],
    )

    run_resource(config.resources[0], config)

    with open(output_dir / "pipe_out.json", "r") as f:
        data = json.load(f)

    assert len(data) == 3


def test_schema_inference_integration(fixtures_dir):
    """Test that schema can be inferred from actual CSV file"""
    from conduit_core.connectors.csv import CsvSource

    # Use comma_delim.csv which has cleaner data
    source_config = Source(
        name="test", type="csv", path=str(fixtures_dir / "comma_delim.csv")
    )
    source = CsvSource(source_config)

    # Read records
    records = list(source.read())

    # Infer schema
    schema = SchemaInferrer.infer_schema(records)

    # Check that we got all columns
    assert "id" in schema
    assert "name" in schema
    assert "age" in schema

    # Check that types were inferred correctly
    assert schema["id"]["type"] == "integer"
    assert schema["name"]["type"] == "string"
    assert schema["age"]["type"] == "integer"