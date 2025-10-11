# tests/test_state_management.py

import pytest
import json
from pathlib import Path
from conduit_core.state import load_state, save_state, validate_state, recover_state, STATE_FILE, BACKUP_FILE


def test_save_and_load_state(tmp_path, monkeypatch):
    """Test that state can be saved and loaded correctly"""
    # Use temporary directory for test
    test_state_file = tmp_path / ".conduit_state.json"
    monkeypatch.setattr("conduit_core.state.STATE_FILE", test_state_file)
    monkeypatch.setattr("conduit_core.state.BACKUP_FILE", tmp_path / ".conduit_state.backup.json")
    
    # Save state
    test_state = {"resource1": 100, "resource2": 200}
    save_state(test_state)
    
    # Load state
    loaded_state = load_state()
    
    assert loaded_state == test_state


def test_atomic_save_creates_backup(tmp_path, monkeypatch):
    """Test that saving state creates a backup of previous state"""
    test_state_file = tmp_path / ".conduit_state.json"
    test_backup_file = tmp_path / ".conduit_state.backup.json"
    monkeypatch.setattr("conduit_core.state.STATE_FILE", test_state_file)
    monkeypatch.setattr("conduit_core.state.BACKUP_FILE", test_backup_file)
    
    # Save initial state
    state1 = {"resource1": 100}
    save_state(state1)
    
    # Save new state (should backup the old one)
    state2 = {"resource1": 200}
    save_state(state2)
    
    # Check that backup exists and contains old state
    assert test_backup_file.exists()
    with open(test_backup_file, 'r') as f:
        backup = json.load(f)
        assert backup == state1


def test_load_state_returns_empty_if_no_file(tmp_path, monkeypatch):
    """Test that load_state returns empty dict when no file exists"""
    test_state_file = tmp_path / ".conduit_state.json"
    monkeypatch.setattr("conduit_core.state.STATE_FILE", test_state_file)
    
    state = load_state()
    
    assert state == {}


def test_load_state_recovers_from_backup_if_corrupted(tmp_path, monkeypatch):
    """Test that corrupted state file loads from backup"""
    test_state_file = tmp_path / ".conduit_state.json"
    test_backup_file = tmp_path / ".conduit_state.backup.json"
    monkeypatch.setattr("conduit_core.state.STATE_FILE", test_state_file)
    monkeypatch.setattr("conduit_core.state.BACKUP_FILE", test_backup_file)
    
    # Create a valid backup
    backup_state = {"resource1": 100}
    with open(test_backup_file, 'w') as f:
        json.dump(backup_state, f)
    
    # Create a corrupted main file
    with open(test_state_file, 'w') as f:
        f.write("{invalid json")
    
    # Load should recover from backup
    loaded_state = load_state()
    
    assert loaded_state == backup_state


def test_validate_state():
    """Test state validation"""
    # Valid states
    assert validate_state({"resource1": 100}) == True
    assert validate_state({"resource1": 100, "resource2": 200}) == True
    assert validate_state({}) == True
    
    # Invalid states
    assert validate_state("not a dict") == False
    assert validate_state({123: "value"}) == False  # Non-string key


def test_state_file_corruption_handling(tmp_path, monkeypatch):
    """Test that the system handles corrupted state files gracefully"""
    test_state_file = tmp_path / ".conduit_state.json"
    test_backup_file = tmp_path / ".conduit_state.backup.json"
    monkeypatch.setattr("conduit_core.state.STATE_FILE", test_state_file)
    monkeypatch.setattr("conduit_core.state.BACKUP_FILE", test_backup_file)
    
    # Write corrupted JSON
    with open(test_state_file, 'w') as f:
        f.write("{ this is not valid json }")
    
    # Should not crash, should return empty state
    state = load_state()
    assert isinstance(state, dict)