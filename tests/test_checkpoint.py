# tests/test_checkpoint.py

import pytest
import json
from pathlib import Path
from datetime import datetime, timezone

from conduit_core.checkpoint import CheckpointManager

# --- Fixtures for Testing ---

@pytest.fixture
def checkpoint_dir(tmp_path: Path) -> Path:
    """Creates a temporary directory for checkpoint files."""
    return tmp_path / ".checkpoints"

@pytest.fixture
def checkpoint_mgr(checkpoint_dir: Path) -> CheckpointManager:
    """Creates a CheckpointManager instance using the temporary directory."""
    return CheckpointManager(checkpoint_dir)

# --- Test Cases ---

def test_checkpoint_save_and_load(checkpoint_mgr: CheckpointManager):
    """Tests the basic save and load cycle of a checkpoint."""
    pipeline_name = "test_pipeline"
    last_value = 12345
    records_processed = 100

    # Save checkpoint
    checkpoint_mgr.save_checkpoint(pipeline_name, "id", last_value, records_processed)

    # Verify file exists
    assert checkpoint_mgr.checkpoint_exists(pipeline_name)

    # Load and verify content
    checkpoint = checkpoint_mgr.load_checkpoint(pipeline_name)
    assert checkpoint is not None
    assert checkpoint["pipeline_name"] == pipeline_name
    assert checkpoint["last_value"] == last_value
    assert checkpoint["checkpoint_type"] == "integer"
    assert checkpoint["records_processed"] == records_processed

def test_clear_checkpoint(checkpoint_mgr: CheckpointManager):
    """Tests that a checkpoint can be successfully cleared."""
    pipeline_name = "pipeline_to_clear"
    checkpoint_mgr.save_checkpoint(pipeline_name, "id", 100, 10)
    
    assert checkpoint_mgr.checkpoint_exists(pipeline_name)
    
    was_cleared = checkpoint_mgr.clear_checkpoint(pipeline_name)
    
    assert was_cleared
    assert not checkpoint_mgr.checkpoint_exists(pipeline_name)
    assert checkpoint_mgr.load_checkpoint(pipeline_name) is None

def test_load_nonexistent_checkpoint(checkpoint_mgr: CheckpointManager):
    """Tests that loading a non-existent checkpoint returns None."""
    assert checkpoint_mgr.load_checkpoint("nonexistent_pipeline") is None

def test_handle_corrupted_checkpoint(checkpoint_mgr: CheckpointManager, checkpoint_dir: Path):
    """Tests that a corrupted (invalid JSON) checkpoint file is handled gracefully."""
    pipeline_name = "corrupted_pipeline"
    checkpoint_file = checkpoint_dir / f"{pipeline_name}.json"
    
    # Create a corrupted file
    checkpoint_file.write_text("this is not valid json")

    # Loading should return None and not crash
    checkpoint = checkpoint_mgr.load_checkpoint(pipeline_name)
    assert checkpoint is None

def test_list_checkpoints(checkpoint_mgr: CheckpointManager):
    """Tests that list_checkpoints returns all saved checkpoints."""
    checkpoint_mgr.save_checkpoint("pipeline_1", "id", 1, 10)
    checkpoint_mgr.save_checkpoint("pipeline_2", "timestamp", "2025-10-17", 20)
    
    checkpoints = checkpoint_mgr.list_checkpoints()
    
    assert len(checkpoints) == 2
    pipeline_names = {cp["pipeline_name"] for cp in checkpoints}
    assert "pipeline_1" in pipeline_names
    assert "pipeline_2" in pipeline_names

def test_atomic_write_cleans_up_temp_file(checkpoint_mgr: CheckpointManager, checkpoint_dir: Path):
    """Tests that the temporary file is removed after a successful write."""
    pipeline_name = "atomic_test"
    temp_file = checkpoint_dir / f"{pipeline_name}.json.tmp"
    
    checkpoint_mgr.save_checkpoint(pipeline_name, "id", 1, 1)
    
    assert not temp_file.exists()
    assert (checkpoint_dir / f"{pipeline_name}.json").exists()

def test_data_type_detection(checkpoint_mgr: CheckpointManager):
    """Tests that different data types are correctly identified."""
    # Integer
    checkpoint_mgr.save_checkpoint("int_pipe", "id", 123, 1)
    cp = checkpoint_mgr.load_checkpoint("int_pipe")
    assert cp["checkpoint_type"] == "integer"

    # Float
    checkpoint_mgr.save_checkpoint("float_pipe", "score", 99.9, 1)
    cp = checkpoint_mgr.load_checkpoint("float_pipe")
    assert cp["checkpoint_type"] == "float"

    # Datetime
    now = datetime.now(timezone.utc)
    checkpoint_mgr.save_checkpoint("dt_pipe", "updated_at", now, 1)
    cp = checkpoint_mgr.load_checkpoint("dt_pipe")
    assert cp["checkpoint_type"] == "datetime"
    
    # String
    checkpoint_mgr.save_checkpoint("str_pipe", "uuid", "abc-123", 1)
    cp = checkpoint_mgr.load_checkpoint("str_pipe")
    assert cp["checkpoint_type"] == "string"