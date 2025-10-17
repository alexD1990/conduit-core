# tests/test_dry_run.py

import pytest
from pathlib import Path
import os

from conduit_core.config import IngestConfig, Source, Destination, Resource
from conduit_core.engine import run_resource
from conduit_core.state import save_state, load_state

@pytest.fixture
def test_config(tmp_path: Path) -> IngestConfig:
    """Creates a basic IngestConfig for testing dry-run."""
    source_csv = tmp_path / "source.csv"
    source_csv.write_text("id,name\n1,Alice\n2,Bob\n")
    
    output_csv = tmp_path / "output.csv"
    
    return IngestConfig(
        sources=[Source(
            name="src",
            type="csv",
            path=str(source_csv),
            # Add incremental column for state test
            incremental_column="id"
        )],
        destinations=[Destination(name="dest", type="csv", path=str(output_csv))],
        resources=[Resource(
            name="test_pipeline",
            source="src",
            destination="dest",
            query="n/a",
            # Add incremental column for state test
            incremental_column="id"
        )]
    )

def test_dry_run_does_not_write_to_csv(tmp_path: Path, test_config: IngestConfig):
    """Test that dry-run mode doesn't create the output file."""
    output_csv = tmp_path / "output.csv"

    # Run in dry-run mode
    run_resource(test_config.resources[0], test_config, dry_run=True)

    # Verify the output file was NOT created
    assert not output_csv.exists(), "Dry-run should not create an output file."

def test_dry_run_does_not_update_state(tmp_path: Path, test_config: IngestConfig):
    """Test that dry-run doesn't create or modify the state file."""
    state_file = tmp_path / "conduit_state.json"
    
    # Change to temp directory to ensure state file is written there
    original_cwd = os.getcwd()
    os.chdir(tmp_path)
    
    try:
        # Run in dry-run mode
        run_resource(test_config.resources[0], test_config, dry_run=True)

        # Verify the state file was NOT created
        assert not state_file.exists(), "Dry-run should not create a state file."
    finally:
        # Change back to the original directory
        os.chdir(original_cwd)

def test_normal_mode_writes_data(tmp_path: Path, test_config: IngestConfig):
    """Test that normal mode (dry_run=False) writes data correctly."""
    output_csv = tmp_path / "output.csv"
    
    # Run in NORMAL mode
    run_resource(test_config.resources[0], test_config, dry_run=False)

    # Verify the output file WAS created
    assert output_csv.exists(), "Normal mode should create an output file."
    
    content = output_csv.read_text()
    assert "Alice" in content
    assert "Bob" in content