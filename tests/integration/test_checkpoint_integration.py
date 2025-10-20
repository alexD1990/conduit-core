"""Integration tests for checkpoint/resume functionality."""

import pytest
from pathlib import Path


def test_checkpoint_resume_after_failure(tmp_path):
    """Test that pipeline resumes from checkpoint after failure."""

    checkpoint_dir = tmp_path / ".conduit_checkpoints"
    checkpoint_dir.mkdir()

    # Create test manager
    from conduit_core.checkpoint import CheckpointManager
    mgr = CheckpointManager(checkpoint_dir)

    # Test 1: Save a checkpoint (simulating partial failure)
    mgr.save_checkpoint(
        pipeline_name="test_pipeline",
        checkpoint_column="id",
        last_value=500,
        records_processed=500
    )

    # Verify checkpoint exists
    checkpoint = mgr.load_checkpoint("test_pipeline")
    assert checkpoint is not None
    assert checkpoint['last_value'] == 500
    assert checkpoint['checkpoint_column'] == "id"

    # Test 2: Simulate successful completion - clear checkpoint
    mgr.clear_checkpoint("test_pipeline")

    # Verify checkpoint cleared
    checkpoint_after = mgr.load_checkpoint("test_pipeline")
    assert checkpoint_after is None

    print("[OK] Checkpoint persistence test passed")


def test_checkpoint_with_different_types(tmp_path):
    """Test checkpoint with different data types."""

    checkpoint_dir = tmp_path / ".conduit_checkpoints"
    checkpoint_dir.mkdir()

    from conduit_core.checkpoint import CheckpointManager
    mgr = CheckpointManager(checkpoint_dir)

    # Test integer
    mgr.save_checkpoint("pipeline_int", "id", 12345, 12345)
    cp = mgr.load_checkpoint("pipeline_int")
    assert cp['last_value'] == 12345

    # Test string
    mgr.save_checkpoint("pipeline_str", "batch_id", "batch_2024_10_17", 1000)
    cp = mgr.load_checkpoint("pipeline_str")
    assert cp['last_value'] == "batch_2024_10_17"

    # Test float
    mgr.save_checkpoint("pipeline_float", "version", 3.14, 500)
    cp = mgr.load_checkpoint("pipeline_float")
    assert cp['last_value'] == 3.14

    print("[OK] Checkpoint type handling test passed")