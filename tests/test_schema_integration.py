# tests/test_schema_integration.py

import pytest
import json
import yaml
from pathlib import Path
from typer.testing import CliRunner
from unittest.mock import patch, MagicMock
import logging  # Import logging


from conduit_core.cli import app as cli_app
from conduit_core.config import IngestConfig, Source, Destination, Resource, SchemaEvolutionConfig
from conduit_core.engine import run_resource
from conduit_core.schema_evolution import SchemaEvolutionError
from conduit_core.schema_store import SchemaStore


# Fixture to create a dummy config
@pytest.fixture
def base_config(tmp_path):
    """Provides a base config object and paths."""
    source_path = tmp_path / "source.csv"
    dest_path = tmp_path / "dest.json"
    
    config = IngestConfig(
        sources=[
            Source(name="csv_source", type="csv", path=str(source_path)),
            Source(name="pg_source", type="postgresql", connection_string="dummy_conn"),
        ],
        destinations=[
            Destination(name="json_dest", type="json", path=str(dest_path)),
            Destination(name="pg_dest", type="postgresql", connection_string="dummy_conn", table="test_table"),
            Destination(name="sf_dest", type="snowflake", account="dummy", user="dummy", password="dummy", warehouse="dummy", database="dummy", table="test_table"),
            Destination(name="bq_dest", type="bigquery", project="dummy", dataset="dummy", table="test_table"),
        ],
        resources=[
            Resource(name="csv_to_json", source="csv_source", destination="json_dest", query="n/a"),
            Resource(name="pg_to_pg", source="pg_source", destination="pg_dest", query="SELECT * FROM users"),
            Resource(name="pg_to_sf", source="pg_source", destination="sf_dest", query="SELECT * FROM users"),
            Resource(name="pg_to_bq", source="pg_source", destination="bq_dest", query="SELECT * FROM users"),
        ]
    )
    return config, source_path, dest_path

# --- 1. Schema Inference in Pipeline ---

@pytest.mark.skip(reason="TODO: Implement pipeline run with mock CSV")
def test_infer_schema_from_csv_source(base_config, tmp_path, caplog):
    """Test schema inference in full pipeline from a CSV with mixed types."""
    config, source_path, _ = base_config
    
    # Setup: Create CSV with mixed types
    csv_content = (
        "id,name,rate,active,joined_date\n"
        "1,Alice,10.5,true,2023-01-01\n"
        "2,Bob,20.0,false,2023-01-02\n"
        "3,Charlie,,true,\n"
    )
    source_path.write_text(csv_content)

    # Get resource and enable schema inference
    resource = next(r for r in config.resources if r.name == "csv_to_json")
    source_config = next(s for s in config.sources if s.name == resource.source)
    source_config.infer_schema = True

    # Run the resource
    # run_resource(resource, config, dry_run=True) # Needs mock source/dest
    
    # Assert schema was logged (placeholder until run implemented)
    # assert "Schema inferred: 5 columns" in caplog.text
    pass


@pytest.mark.skip(reason="TODO: Implement pipeline run with mock CSV")
def test_infer_schema_with_nulls(base_config, caplog):
    """Verify nullable detection during inference."""
    pass

@pytest.mark.skip(reason="TODO: Implement pipeline run with mock CSV")
def test_infer_schema_respects_sample_size(base_config, caplog):
    """Test that the sample_size parameter is correctly used."""
    pass

@pytest.mark.skip(reason="TODO: Implement pipeline run with mock CSV")
def test_infer_schema_disabled_by_default(base_config, caplog):
    """Ensure schema inference doesn't run unless infer_schema=True."""
    pass

# --- 2. Schema Export ---

@pytest.mark.skip(reason="TODO: Implement pipeline run with mock source")
def test_export_schema_json(base_config, tmp_path):
    """Test exporting the inferred schema to a .json file."""
    config, _, _ = base_config
    export_path = tmp_path / "exports" / "schema.json"

    # Get resource, enable inference, and set export path
    resource = next(r for r in config.resources if r.name == "csv_to_json")
    resource.export_schema_path = str(export_path)
    source_config = next(s for s in config.sources if s.name == resource.source)
    source_config.infer_schema = True

    # TODO: Mock source.read() to return sample data
    
    # Run the resource
    # run_resource(resource, config, dry_run=True)
    
    # Assert file was created and contains valid JSON
    # assert export_path.exists()
    # with open(export_path, 'r') as f:
    #     schema_data = json.load(f)
    # assert "columns" in schema_data # Check new format
    pass

@pytest.mark.skip(reason="TODO: Implement pipeline run with mock source")
def test_export_schema_yaml(base_config, tmp_path):
    """Test exporting the inferred schema to a .yml file."""
    pass

@pytest.mark.skip(reason="TODO: Implement pipeline run with mock source")
def test_export_schema_creates_directories(base_config, tmp_path):
    """Test that nested parent directories for the export path are created."""
    pass

@pytest.mark.skip(reason="TODO: Implement pipeline run with mock source")
def test_export_schema_not_triggered_without_path(base_config, tmp_path):
    """Ensure no schema file is exported if export_schema_path is not set."""
    pass

# --- 3. Auto Create Table ---

def test_auto_create_table_postgresql(base_config):
    """Verify DDL execution is called for PostgreSQL."""
    config, _, _ = base_config
    
    # Get PG resource and enable auto-create
    resource = next(r for r in config.resources if r.name == "pg_to_pg")
    source_config = next(s for s in config.sources if s.name == resource.source)
    dest_config = next(d for d in config.destinations if d.name == resource.destination)
    source_config.infer_schema = True
    dest_config.auto_create_table = True

    # 1. Mock the Source
    mock_source_class = MagicMock()
    mock_source_instance = mock_source_class.return_value
    mock_source_instance.read.return_value = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'}
    ]
    
    # 2. Mock the Destination
    mock_ddl_method = MagicMock()  # This is the mock for the execute_ddl method
    mock_dest_class = MagicMock()
    mock_dest_instance = mock_dest_class.return_value
    # *** IMPORTANT: Make sure the mock instance has the 'config' attribute ***
    mock_dest_instance.config = dest_config # Use the actual dest config
    mock_dest_instance.execute_ddl = mock_ddl_method  # Attach our method mock


    # 3. Patch *both* maps where they are used (in the engine)
    with patch('conduit_core.engine.get_source_connector_map', return_value={'postgresql': mock_source_class}), \
         patch('conduit_core.engine.get_destination_connector_map', return_value={'postgresql': mock_dest_class}):
        
        run_resource(resource, config, dry_run=False)

    # 4. Assert our new mock was called
    mock_ddl_method.assert_called_once()
    called_sql = mock_ddl_method.call_args[0][0]
    # *** FIX: Assert based on quoted table name ***
    assert 'CREATE TABLE IF NOT EXISTS "test_table"' in called_sql
    assert '"id" INTEGER NOT NULL' in called_sql
    assert '"name" TEXT NOT NULL' in called_sql

@pytest.mark.skip(reason="TODO: Implement mock and run")
def test_auto_create_table_snowflake(base_config):
    """Verify DDL execution is called for Snowflake."""
    pass

@pytest.mark.skip(reason="TODO: Implement mock and run")
def test_auto_create_table_bigquery(base_config):
    """Verify DDL execution is called for BigQuery."""
    pass

@pytest.mark.skip(reason="TODO: Implement mock and run")
def test_auto_create_disabled_by_default(base_config, caplog):
    """Ensures DDL is not run if auto_create_table=False."""
    pass

@pytest.mark.skip(reason="TODO: Implement mock and run")
def test_auto_create_only_for_db_destinations(base_config, caplog):
    """DDL should be skipped for CSV/JSON/S3 destinations."""
    pass

# --- 4. Schema Validation (optional for v1) ---

@pytest.mark.skip(reason="TODO: Schema validation not yet implemented")
def test_validate_schema_compatible():
    pass

@pytest.mark.skip(reason="TODO: Schema validation not yet implemented")
def test_validate_schema_incompatible():
    pass

# --- 5. CLI Schema Command ---

@patch('conduit_core.connectors.csv.CsvSource.read')
def test_cli_schema_command(mock_read, tmp_path):
    """Test the 'conduit schema' CLI command."""
    runner = CliRunner()
    
    # Setup mock data and config file
    mock_read.return_value = [
        {'id': 1, 'user': 'cli_user', 'value': 1.23},
        {'id': 2, 'user': 'test_user', 'value': 4.56}
    ]
    
    config_content = """
sources:
  - name: test_source
    type: csv
    path: "fake.csv"
destinations: []
resources:
  - name: test_resource
    source: test_source
    destination: ""
    query: "n/a"
"""
    config_file = tmp_path / "ingest.yml"
    config_file.write_text(config_content)
    
    output_file = tmp_path / "cli_schema.json"

    # Run the CLI command
    result = runner.invoke(cli_app, [
        "schema",
        "--file", str(config_file),
        "--output", str(output_file),
        "test_resource"
    ])

    # Check assertions
    assert result.exit_code == 0
    assert "Schema exported to" in result.stdout
    assert output_file.exists()
    
    with open(output_file, 'r') as f:
        schema_data = json.load(f)

    # *** FIX: Check new schema format ***
    assert "columns" in schema_data
    cols = {c['name']: c for c in schema_data['columns']}
    
    assert "id" in cols
    assert cols["id"]["type"] == "integer"
    assert "user" in cols
    assert cols["user"]["type"] == "string"
    assert "value" in cols
    assert cols["value"]["type"] == "float"


def test_cli_schema_invalid_resource(tmp_path):
    """Test error handling for the CLI schema command."""
    runner = CliRunner()
    
    config_content = """
sources:
  - name: test_source
    type: csv
    path: "fake.csv"
destinations: []
resources:
  - name: test_resource
    source: test_source
    destination: ""
    query: "n/a"
"""
    config_file = tmp_path / "ingest.yml"
    config_file.write_text(config_content)

    # Run the CLI command with a non-existent resource
    result = runner.invoke(cli_app, [
        "schema",
        "--file", str(config_file),
        "non_existent_resource"
    ])
    
    assert result.exit_code == 1
    assert "Resource 'non_existent_resource' not found" in result.stdout

# --- 6. Edge Cases ---

@pytest.mark.skip(reason="TODO: Implement pipeline run with empty source")
def test_schema_inference_empty_source(base_config, caplog):
    """Handle empty data gracefully during inference."""
    pass

@pytest.mark.skip(reason="TODO: Implement pipeline run with single record")
def test_schema_inference_single_record(base_config, caplog):
    """Test inference works with minimal data (1 record)."""
    pass

@pytest.mark.skip(reason="TODO: Implement pipeline run with checkpoint logic")
def test_infer_then_resume(base_config, caplog):
    """Test compatibility of schema inference + checkpoint/resume."""
    pass