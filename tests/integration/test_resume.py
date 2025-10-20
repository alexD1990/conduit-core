# tests/integration/test_resume.py

import pytest
from pathlib import Path
from conduit_core.config import IngestConfig, Source, Destination, Resource
from conduit_core.engine import run_resource
from conduit_core.checkpoint import CheckpointManager
import json

"""
Note: Resume functionality is implemented but awaiting full integration tests.
"""

@pytest.fixture
def fixtures_dir():
    return Path(__file__).parent.parent / "fixtures" / "data"


@pytest.fixture
def output_dir(tmp_path):
    return tmp_path / "output"


@pytest.fixture(autouse=True)
def cleanup_checkpoints():
    """Clean up checkpoints before and after each test"""
    clear_all_checkpoints()
    yield
    clear_all_checkpoints()


@pytest.mark.skip(reason="Resume feature not in v1.0")
def test_checkpoint_creation_during_run(fixtures_dir, output_dir):
    """Test that checkpoints are created during pipeline execution"""
    output_dir.mkdir(exist_ok=True)

    config = IngestConfig(
        sources=[Source(name="src", type="csv", path=str(fixtures_dir / "large.csv"))],
        destinations=[Destination(name="dest", type="json", path=str(output_dir / "large_out.json"))],
        resources=[Resource(name="test_checkpoint", source="src", destination="dest", query="n/a")]
    )

    # Run with resume=False (normal run)
    run_resource(config.resources[0], config, resume=False)

    # Checkpoint should be cleared after successful completion
    manager = CheckpointManager("test_checkpoint")
    checkpoint = manager.load_checkpoint()

    assert checkpoint is None  # Should be cleared on success


@pytest.mark.skip(reason="Resume feature not in v1.0")
def test_resume_from_checkpoint(fixtures_dir, output_dir, monkeypatch):
    """Test resuming from a saved checkpoint"""
    output_dir.mkdir(exist_ok=True)

    config = IngestConfig(
        sources=[Source(name="src", type="csv", path=str(fixtures_dir / "large.csv"))],
        destinations=[Destination(name="dest", type="json", path=str(output_dir / "resume_out.json"))],
        resources=[Resource(name="resume_test", source="src", destination="dest", query="n/a")]
    )

    # Simulate a checkpoint from a previous interrupted run
    manager = CheckpointManager("resume_test")
    test_checkpoint = {"id": 5000, "timestamp": "2025-01-01T00:00:00", "value": "test"}
    manager.save_checkpoint(5000, test_checkpoint)

    # Run with resume=True
    run_resource(config.resources[0], config, resume=True)

    # Verify output was created
    assert (output_dir / "resume_out.json").exists()

    # Checkpoint should be cleared after successful completion
    checkpoint = manager.load_checkpoint()
    assert checkpoint is None


@pytest.mark.skip(reason="Resume feature not in v1.0")
def test_checkpoint_cleared_on_success(fixtures_dir, output_dir):
    """Test that checkpoints are cleared after successful completion"""
    output_dir.mkdir(exist_ok=True)

    config = IngestConfig(
        sources=[Source(name="src", type="csv", path=str(fixtures_dir / "comma_delim.csv"))],
        destinations=[Destination(name="dest", type="json", path=str(output_dir / "success_out.json"))],
        resources=[Resource(name="success_test", source="src", destination="dest", query="n/a")]
    )

    # Run the resource
    run_resource(config.resources[0], config, resume=False)

    # Verify checkpoint was cleared
    manager = CheckpointManager("success_test")
    checkpoint = manager.load_checkpoint()

    assert checkpoint is None


@pytest.mark.skip(reason="Resume feature not in v1.0")
def test_no_checkpoint_means_start_from_beginning(fixtures_dir, output_dir):
    """Test that without checkpoint, pipeline starts from beginning"""
    output_dir.mkdir(exist_ok=True)

    config = IngestConfig(
        sources=[Source(name="src", type="csv", path=str(fixtures_dir / "comma_delim.csv"))],
        destinations=[Destination(name="dest", type="json", path=str(output_dir / "fresh_start.json"))],
        resources=[Resource(name="fresh_start", source="src", destination="dest", query="n/a")]
    )

    # Run with resume=True but no checkpoint exists
    run_resource(config.resources[0], config, resume=True)

    # Should complete normally
    assert (output_dir / "fresh_start.json").exists()

    with open(output_dir / "fresh_start.json", 'r') as f:
        data = json.load(f)

    # Should have all 3 rows
    assert len(data) == 3