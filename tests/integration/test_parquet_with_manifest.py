"""Integration test: Parquet connector with manifest tracking."""

import pytest
from pathlib import Path

from conduit_core.connectors.parquet import ParquetSource, ParquetDestination
from conduit_core.manifest import PipelineManifest, ManifestTracker


def test_parquet_pipeline_with_manifest(tmp_path):
    """Test full pipeline: CSV -> Parquet with manifest."""
    # Setup
    source_file = tmp_path / "source.parquet"
    dest_file = tmp_path / "dest.parquet"
    manifest_file = tmp_path / "manifest.json"
    
    # Create source data
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    data = [
        {"id": 1, "name": "Alice", "amount": 100.50},
        {"id": 2, "name": "Bob", "amount": 200.75},
        {"id": 3, "name": "Charlie", "amount": 150.25},
    ]
    table = pa.Table.from_pylist(data)
    pq.write_table(table, source_file)
    
    # Run pipeline with manifest
    manifest = PipelineManifest(manifest_file)
    
    with ManifestTracker(
        manifest=manifest,
        pipeline_name="parquet_test",
        source_type="parquet",
        destination_type="parquet"
    ) as tracker:
        source = ParquetSource({"file_path": str(source_file)})
        destination = ParquetDestination({"file_path": str(dest_file)})
        
        records = list(source.read())
        destination.write(records)
        destination.finalize()
        
        tracker.records_read = len(records)
        tracker.records_written = len(records)
    
    # Verify data
    result_table = pq.read_table(dest_file)
    result_data = result_table.to_pylist()
    
    assert len(result_data) == 3
    assert result_data[0]["name"] == "Alice"
    
    # Verify manifest
    assert manifest_file.exists()
    latest = manifest.get_latest("parquet_test")
    
    assert latest.status == "success"
    assert latest.records_read == 3
    assert latest.records_written == 3
    assert latest.records_failed == 0


def test_manifest_cli_output(tmp_path, capsys):
    """Test manifest CLI command."""
    from conduit_core.cli import app
    from typer.testing import CliRunner
    
    manifest_file = tmp_path / "manifest.json"
    manifest = PipelineManifest(manifest_file)
    
    # Add test entries
    from conduit_core.manifest import ManifestEntry
    
    entry = ManifestEntry(
        run_id="test_run_id",
        pipeline_name="test_pipeline",
        source_type="parquet",
        destination_type="postgresql",
        started_at="2025-10-16T10:00:00",
        completed_at="2025-10-16T10:01:00",
        status="success",
        records_read=1000,
        records_written=1000,
        records_failed=0,
        duration_seconds=60.0
    )
    manifest.add_entry(entry)
    
    # Test CLI
    runner = CliRunner()
    result = runner.invoke(app, ["manifest", "--manifest-path", str(manifest_file)])
    
    assert result.exit_code == 0
    assert "test_pipeline" in result.stdout
    assert "success" in result.stdout