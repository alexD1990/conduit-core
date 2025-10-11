# src/conduit_core/state.py

import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Any

# Platform-specific imports for file locking
if sys.platform != 'win32':
    import fcntl
else:
    # Windows doesn't have fcntl, we'll use a simpler approach
    import msvcrt

logger = logging.getLogger(__name__)

STATE_FILE = Path(".conduit_state.json")
BACKUP_FILE = Path(".conduit_state.backup.json")
LOCK_FILE = Path(".conduit_state.lock")


def load_state() -> Dict[str, Any]:
    """
    Loads state from JSON file with validation.
    Returns an empty dict if file doesn't exist or is corrupted.
    """
    # Try to load main state file
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                logger.debug(f"Loaded state from {STATE_FILE}")
                return state
        except json.JSONDecodeError as e:
            logger.warning(f"State file corrupted: {e}. Attempting to load backup...")
            
            # Try backup if main file is corrupted
            if BACKUP_FILE.exists():
                try:
                    with open(BACKUP_FILE, 'r') as f:
                        state = json.load(f)
                        logger.info(f"✅ Loaded state from backup: {BACKUP_FILE}")
                        # Restore the main file from backup
                        save_state(state)
                        return state
                except json.JSONDecodeError:
                    logger.error("Backup file also corrupted. Starting with empty state.")
    
    logger.info("No valid state file found. Starting with empty state.")
    return {}


def save_state(state: Dict[str, Any]):
    """
    Atomically saves state to JSON file with backup.
    Uses temp file + atomic rename pattern for safety.
    """
    # Create a temporary file
    temp_file = STATE_FILE.with_suffix('.tmp')
    
    try:
        # Acquire file lock to prevent concurrent writes
        with _acquire_lock():
            # 1. Backup current state if it exists
            if STATE_FILE.exists():
                try:
                    STATE_FILE.replace(BACKUP_FILE)
                    logger.debug(f"Backed up state to {BACKUP_FILE}")
                except Exception as e:
                    logger.warning(f"Failed to create backup: {e}")
            
            # 2. Write to temporary file
            with open(temp_file, 'w') as f:
                json.dump(state, f, indent=4)
                f.flush()  # Ensure data is written to disk
                os.fsync(f.fileno())  # Force write to disk
            
            # 3. Atomic rename (this is atomic on most filesystems)
            temp_file.replace(STATE_FILE)
            
            logger.debug(f"State saved atomically to {STATE_FILE}")
    
    except Exception as e:
        logger.error(f"Failed to save state: {e}")
        # Clean up temp file if it exists
        if temp_file.exists():
            temp_file.unlink()
        raise
    finally:
        # Ensure temp file is cleaned up
        if temp_file.exists():
            try:
                temp_file.unlink()
            except:
                pass


class _FileLock:
    """Cross-platform file-based lock for preventing concurrent state writes."""
    
    def __init__(self, lock_file: Path):
        self.lock_file = lock_file
        self.lock_fd = None
    
    def __enter__(self):
        self.lock_fd = open(self.lock_file, 'w')
        try:
            if sys.platform != 'win32':
                # Unix/Linux: use fcntl
                fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX)
            else:
                # Windows: use msvcrt
                msvcrt.locking(self.lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
            logger.debug("Acquired state file lock")
        except Exception as e:
            logger.warning(f"Could not acquire lock (continuing anyway): {e}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.lock_fd:
            try:
                if sys.platform != 'win32':
                    fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)
                else:
                    msvcrt.locking(self.lock_fd.fileno(), msvcrt.LK_UNLCK, 1)
                self.lock_fd.close()
                logger.debug("Released state file lock")
            except Exception as e:
                logger.warning(f"Error releasing lock: {e}")
            finally:
                # Clean up lock file
                try:
                    if self.lock_file.exists():
                        self.lock_file.unlink()
                except:
                    pass


def _acquire_lock():
    """Returns a context manager for file locking."""
    return _FileLock(LOCK_FILE)


def validate_state(state: Dict[str, Any]) -> bool:
    """
    Validates that state has correct structure.
    
    Returns:
        True if valid, False otherwise
    """
    if not isinstance(state, dict):
        logger.error("State must be a dictionary")
        return False
    
    # All values should be integers (for incremental column tracking)
    for key, value in state.items():
        if not isinstance(key, str):
            logger.error(f"State key must be string, got {type(key)}")
            return False
        # Allow int, None, or other serializable types
        if value is not None and not isinstance(value, (int, float, str, bool)):
            logger.error(f"State value for '{key}' has invalid type: {type(value)}")
            return False
    
    return True


def recover_state() -> Dict[str, Any]:
    """
    Attempts to recover state from backup if main file is corrupted.
    
    Returns:
        Recovered state or empty dict
    """
    logger.info("Attempting state recovery...")
    
    if BACKUP_FILE.exists():
        try:
            with open(BACKUP_FILE, 'r') as f:
                state = json.load(f)
                if validate_state(state):
                    logger.info("✅ State recovered from backup")
                    save_state(state)  # Restore main file
                    return state
        except Exception as e:
            logger.error(f"Failed to recover from backup: {e}")
    
    logger.warning("Could not recover state. Starting fresh.")
    return {}