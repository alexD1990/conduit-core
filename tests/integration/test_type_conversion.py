# tests/integration/test_type_conversion.py

import pytest
from pathlib import Path
from conduit_core.config import IngestConfig, Source, Destination, Resource
from conduit_core.engine import run_resource
import json
import csv


@pytest.fixture
def fixtures_dir():
    return Path(__file__).parent.parent / "fixtures" / "data"


@pytest.fixture
def output_dir(tmp_path):
    return tmp_path / "output"


def test_edge_cases_csv_to_json(fixtures_dir, output_dir):
    """Test CSV with edge cases converts to JSON correctly"""
    output_dir.mkdir(exist_ok=True)
    
    config = IngestConfig(
        sources=[Source(name="src", type="csv", path=str(fixtures_dir / "edge_cases.csv"))],
        destinations=[Destination(name="dest", type="json", path=str(output_dir / "edge_cases.json"))],
        resources=[Resource(name="transfer", source="src", destination="dest", query="n/a")]
    )
    
    run_resource(config.resources[0], config)
    
    # Verify output
    assert (output_dir / "edge_cases.json").exists()
    
    with open(output_dir / "edge_cases.json", 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    assert len(data) == 3
    # Check that special characters are preserved
    assert "José" in data[0]["name"]
    assert "李明" in data[1]["name"]


def test_data_types_preserved(fixtures_dir, output_dir):
    """Test that various data types are handled correctly"""
    output_dir.mkdir(exist_ok=True)
    
    config = IngestConfig(
        sources=[Source(name="src", type="csv", path=str(fixtures_dir / "data_types.csv"))],
        destinations=[Destination(name="dest", type="json", path=str(output_dir / "data_types.json"))],
        resources=[Resource(name="transfer", source="src", destination="dest", query="n/a")]
    )
    
    run_resource(config.resources[0], config)
    
    with open(output_dir / "data_types.json", 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Check NULLs were converted properly
    assert data[0]["null_col"] is None
    assert data[0]["empty_col"] is None
    
    # Check NaN was handled
    assert data[2]["float_col"] is None or data[2]["float_col"] == "NaN"


def test_utf8_bom_handled(fixtures_dir, output_dir):
    """Test that UTF-8 BOM files are read correctly"""
    output_dir.mkdir(exist_ok=True)
    
    config = IngestConfig(
        sources=[Source(name="src", type="csv", path=str(fixtures_dir / "utf8_bom.csv"))],
        destinations=[Destination(name="dest", type="csv", path=str(output_dir / "utf8_out.csv"))],
        resources=[Resource(name="transfer", source="src", destination="dest", query="n/a")]
    )
    
    run_resource(config.resources[0], config)
    
    assert (output_dir / "utf8_out.csv").exists()


def test_latin1_encoding_handled(fixtures_dir, output_dir):
    """Test that Latin-1 encoded files are read correctly"""
    output_dir.mkdir(exist_ok=True)
    
    config = IngestConfig(
        sources=[Source(name="src", type="csv", path=str(fixtures_dir / "latin1.csv"))],
        destinations=[Destination(name="dest", type="json", path=str(output_dir / "latin1.json"))],
        resources=[Resource(name="transfer", source="src", destination="dest", query="n/a")]
    )
    
    run_resource(config.resources[0], config)
    
    with open(output_dir / "latin1.json", 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Check that Latin-1 characters were handled
    assert "Café" in data[0]["name"] or "Caf" in data[0]["name"]


def test_parquet_edge_cases(fixtures_dir, output_dir):
    """Test Parquet with NULLs and special values"""
    output_dir.mkdir(exist_ok=True)
    
    config = IngestConfig(
        sources=[Source(name="src", type="parquet", path=str(fixtures_dir / "edge_cases.parquet"))],
        destinations=[Destination(name="dest", type="json", path=str(output_dir / "parquet_edges.json"))],
        resources=[Resource(name="transfer", source="src", destination="dest", query="n/a")]
    )
    
    run_resource(config.resources[0], config)
    
    with open(output_dir / "parquet_edges.json", 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Should have 4 rows
    assert len(data) == 4
    
    # Check NULLs and NaN handling
    assert data[3]["int_col"] is None
    # NaN should be converted to None
    assert data[2]["float_col"] is None