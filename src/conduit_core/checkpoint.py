# src/conduit_core/checkpoint.py

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

CHECKPOINT_DIR = Path(".conduit_checkpoints")


class CheckpointManager:
    """
    Manages checkpoints for resumable data transfers.
    Saves progress periodically so pipelines can resume from failure points.
    """
    
    def __init__(self, resource_name: str, checkpoint_interval: int = 1000):
        """
        Args:
            resource_name: Name of the resource being processed
            checkpoint_interval: Save checkpoint every N rows
        """
        self.resource_name = resource_name
        self.checkpoint_interval = checkpoint_interval
        self.checkpoint_file = CHECKPOINT_DIR / f"{resource_name}.json"
        
        # Create checkpoint directory if it doesn't exist
        CHECKPOINT_DIR.mkdir(exist_ok=True)
        
        self.rows_processed = 0
        self.last_checkpoint_row = 0
    
    def save_checkpoint(self, row_number: int, last_record: Dict[str, Any]):
        """
        Save a checkpoint with current progress.
        
        Args:
            row_number: Current row number being processed
            last_record: The last successfully processed record
        """
        checkpoint_data = {
            'resource_name': self.resource_name,
            'row_number': row_number,
            'last_record': last_record,
            'timestamp': datetime.now().isoformat(),
            'rows_processed': self.rows_processed
        }
        
        try:
            # Atomic write using temp file
            temp_file = self.checkpoint_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2, default=str)
            
            temp_file.replace(self.checkpoint_file)
            self.last_checkpoint_row = row_number
            logger.debug(f"Checkpoint saved at row {row_number}")
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")
    
    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """
        Load the last checkpoint if it exists.
        
        Returns:
            Checkpoint data or None if no checkpoint exists
        """
        if not self.checkpoint_file.exists():
            logger.info("No checkpoint found, starting from beginning")
            return None
        
        try:
            with open(self.checkpoint_file, 'r') as f:
                checkpoint = json.load(f)
            
            logger.info(
                f"Found checkpoint at row {checkpoint['row_number']} "
                f"from {checkpoint['timestamp']}"
            )
            return checkpoint
        except Exception as e:
            logger.warning(f"Failed to load checkpoint: {e}")
            return None
    
    def should_checkpoint(self, row_number: int) -> bool:
        """
        Determine if we should save a checkpoint at this row.
        
        Args:
            row_number: Current row number
        
        Returns:
            True if checkpoint should be saved
        """
        return (row_number - self.last_checkpoint_row) >= self.checkpoint_interval
    
    def clear_checkpoint(self):
        """Remove checkpoint file after successful completion"""
        try:
            if self.checkpoint_file.exists():
                self.checkpoint_file.unlink()
                logger.info(f"Checkpoint cleared for {self.resource_name}")
        except Exception as e:
            logger.warning(f"Failed to clear checkpoint: {e}")
    
    def increment_processed(self):
        """Increment the count of processed rows"""
        self.rows_processed += 1


def list_checkpoints() -> list[str]:
    """
    List all available checkpoints.
    
    Returns:
        List of resource names with checkpoints
    """
    if not CHECKPOINT_DIR.exists():
        return []
    
    checkpoints = []
    for checkpoint_file in CHECKPOINT_DIR.glob("*.json"):
        resource_name = checkpoint_file.stem
        checkpoints.append(resource_name)
    
    return checkpoints


def get_checkpoint_info(resource_name: str) -> Optional[Dict[str, Any]]:
    """
    Get information about a specific checkpoint.
    
    Args:
        resource_name: Name of the resource
    
    Returns:
        Checkpoint info or None
    """
    checkpoint_file = CHECKPOINT_DIR / f"{resource_name}.json"
    
    if not checkpoint_file.exists():
        return None
    
    try:
        with open(checkpoint_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to read checkpoint: {e}")
        return None


def clear_all_checkpoints():
    """Clear all checkpoint files"""
    if not CHECKPOINT_DIR.exists():
        return
    
    count = 0
    for checkpoint_file in CHECKPOINT_DIR.glob("*.json"):
        try:
            checkpoint_file.unlink()
            count += 1
        except Exception as e:
            logger.warning(f"Failed to delete {checkpoint_file}: {e}")
    
    logger.info(f"Cleared {count} checkpoints")