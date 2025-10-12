# tests/test_checkpoint.py

import pytest
from pathlib import Path
from conduit_core.checkpoint import CheckpointManager, list_checkpoints, clear_all_checkpoints


def test_checkpoint_save_and_load(tmp_path, monkeypatch):
    """Test saving and loading checkpoints"""
    # Use temp directory for checkpoints
    test_checkpoint_dir = tmp_path / ".conduit_checkpoints"
    monkeypatch.setattr("conduit_core.checkpoint.CHECKPOINT_DIR", test_checkpoint_dir)
    
    manager = CheckpointManager("test_resource", checkpoint_interval=100)
    
    # Save a checkpoint
    test_record = {"id": 500, "name": "Test"}
    manager.save_checkpoint(500, test_record)
    
    # Load the checkpoint
    checkpoint = manager.load_checkpoint()
    
    assert checkpoint is not None
    assert checkpoint['row_number'] == 500
    assert checkpoint['last_record']['id'] == 500


def test_checkpoint_interval(tmp_path, monkeypatch):
    """Test that checkpoints are saved at correct intervals"""
    test_checkpoint_dir = tmp_path / ".conduit_checkpoints"
    monkeypatch.setattr("conduit_core.checkpoint.CHECKPOINT_DIR", test_checkpoint_dir)
    
    manager = CheckpointManager("test_resource", checkpoint_interval=100)
    
    # Should not checkpoint at row 50
    assert not manager.should_checkpoint(50)
    
    # Should checkpoint at row 100
    manager.last_checkpoint_row = 0
    assert manager.should_checkpoint(100)
    
    # Should checkpoint at row 200
    manager.last_checkpoint_row = 100
    assert manager.should_checkpoint(200)


def test_clear_checkpoint(tmp_path, monkeypatch):
    """Test clearing checkpoints"""
    test_checkpoint_dir = tmp_path / ".conduit_checkpoints"
    monkeypatch.setattr("conduit_core.checkpoint.CHECKPOINT_DIR", test_checkpoint_dir)
    
    manager = CheckpointManager("test_resource")
    
    # Save a checkpoint
    manager.save_checkpoint(100, {"id": 100})
    assert manager.checkpoint_file.exists()
    
    # Clear it
    manager.clear_checkpoint()
    assert not manager.checkpoint_file.exists()


def test_list_checkpoints(tmp_path, monkeypatch):
    """Test listing all checkpoints"""
    test_checkpoint_dir = tmp_path / ".conduit_checkpoints"
    monkeypatch.setattr("conduit_core.checkpoint.CHECKPOINT_DIR", test_checkpoint_dir)
    
    # Create multiple checkpoints
    manager1 = CheckpointManager("resource1")
    manager1.save_checkpoint(100, {"id": 100})
    
    manager2 = CheckpointManager("resource2")
    manager2.save_checkpoint(200, {"id": 200})
    
    # List them
    checkpoints = list_checkpoints()
    
    assert len(checkpoints) == 2
    assert "resource1" in checkpoints
    assert "resource2" in checkpoints


def test_increment_processed():
    """Test incrementing processed count"""
    manager = CheckpointManager("test")
    
    assert manager.rows_processed == 0
    
    manager.increment_processed()
    assert manager.rows_processed == 1
    
    manager.increment_processed()
    manager.increment_processed()
    assert manager.rows_processed == 3