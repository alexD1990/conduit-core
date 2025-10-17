# tests/test_progress_bars.py

import pytest
import os
from pathlib import Path

from conduit_core.config import IngestConfig, Source, Destination, Resource
from conduit_core.engine import run_resource

@pytest.fixture
def test_config(tmp_path: Path) -> IngestConfig:
    """Creates a basic IngestConfig for testing."""
    source_csv = tmp_path / "source.csv"
    source_csv.write_text("id,name\n1,Alice\n2,Bob\n")
    
    output_csv = tmp_path / "output.csv"
    
    return IngestConfig(
        sources=[Source(name="src", type="csv", path=str(source_csv))],
        destinations=[Destination(name="dest", type="csv", path=str(output_csv))],
        resources=[Resource(name="test", source="src", destination="dest", query="n/a")]
    )

def test_progress_disabled_in_non_tty(test_config: IngestConfig):
    """
    Tests that progress bars are automatically disabled when not in an interactive terminal (like pytest).
    The main verification is that the pipeline runs to completion without errors.
    """
    output_path = Path(test_config.destinations[0].path)
    
    # Run normally; engine.py should detect it's not a TTY and use text logging.
    run_resource(test_config.resources[0], test_config)
    
    # If the file was created, the pipeline ran successfully without the progress bar crashing it.
    assert output_path.exists()

def test_progress_disabled_with_flag(test_config: IngestConfig):
    """Tests that the --no-progress flag (via environment variable) disables progress bars."""
    os.environ['CONDUIT_NO_PROGRESS'] = '1'
    output_path = Path(test_config.destinations[0].path)
    
    try:
        run_resource(test_config.resources[0], test_config)
        # Verify the pipeline still runs and creates the file.
        assert output_path.exists()
    finally:
        # Crucial for test isolation: clean up the environment variable.
        del os.environ['CONDUIT_NO_PROGRESS']

def test_large_dataset_runs_successfully(tmp_path: Path):
    """
    A simple integration test to ensure the progress bar logic doesn't crash on a larger dataset.
    """
    # Create a larger CSV file with 1000 records
    source_csv = tmp_path / "large_source.csv"
    with source_csv.open('w', newline='') as f:
        f.write("id,name\n")
        for i in range(1, 1001):
            f.write(f"{i},User_{i}\n")
            
    output_csv = tmp_path / "large_output.csv"
    
    config = IngestConfig(
        sources=[Source(name="src", type="csv", path=str(source_csv))],
        destinations=[Destination(name="dest", type="csv", path=str(output_csv))],
        resources=[Resource(name="test", source="src", destination="dest", query="n/a")]
    )
    
    # Run the resource. This will use the text-based fallback logger.
    run_resource(config.resources[0], config)
    
    assert output_csv.exists()
    
    # Verify all records were processed by checking the line count
    with output_csv.open('r') as f:
        lines = f.readlines()
        assert len(lines) == 1001  # 1000 records + 1 header line