# src/conduit_core/checkpoint.py

import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)

class CheckpointManager:
    """Manages persistence of pipeline checkpoints to disk."""

    def __init__(self, checkpoint_dir: Optional[Path] = None):
        """Initializes the CheckpointManager."""
        self.checkpoint_dir = checkpoint_dir or Path(".checkpoints/")
        try:
            self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            logger.error(f"Permission denied to create checkpoint directory: {self.checkpoint_dir}")
            raise

    def _get_checkpoint_path(self, pipeline_name: str) -> Path:
        """Constructs the file path for a given pipeline's checkpoint."""
        return self.checkpoint_dir / f"{pipeline_name}.json"

    def save_checkpoint(
        self,
        pipeline_name: str,
        checkpoint_column: str,
        last_value: Any,
        records_processed: int
    ) -> None:
        """Saves a checkpoint to a JSON file using an atomic write pattern."""
        checkpoint_path = self._get_checkpoint_path(pipeline_name)
        temp_path = checkpoint_path.with_suffix(".json.tmp")

        # Determine the type of the checkpoint value
        value_type = "string"
        if isinstance(last_value, int):
            value_type = "integer"
        elif isinstance(last_value, float):
            value_type = "float"
        elif isinstance(last_value, datetime):
            value_type = "datetime"

        checkpoint_data = {
            "pipeline_name": pipeline_name,
            "checkpoint_column": checkpoint_column,
            "last_value": last_value,
            "checkpoint_type": value_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "records_processed": records_processed,
        }

        try:
            with temp_path.open("w", encoding="utf-8") as f:
                json.dump(checkpoint_data, f, indent=2, default=str)
            temp_path.replace(checkpoint_path)
            logger.info(f"Checkpoint saved for pipeline '{pipeline_name}' with value {last_value}.")
        except Exception as e:
            logger.error(f"Failed to save checkpoint for '{pipeline_name}': {e}")
            if temp_path.exists():
                temp_path.unlink() # Clean up temp file on failure

    def load_checkpoint(self, pipeline_name: str) -> Optional[Dict[str, Any]]:
        """Loads a checkpoint from a JSON file."""
        checkpoint_path = self._get_checkpoint_path(pipeline_name)
        if not checkpoint_path.exists():
            return None

        try:
            with checkpoint_path.open("r", encoding="utf-8") as f:
                checkpoint_data = json.load(f)
            logger.info(f"Loaded checkpoint for pipeline '{pipeline_name}'.")
            return checkpoint_data
        except json.JSONDecodeError:
            logger.warning(f"Corrupted checkpoint file found for '{pipeline_name}'. Ignoring.")
            return None
        except Exception as e:
            logger.error(f"Failed to load checkpoint for '{pipeline_name}': {e}")
            return None

    def clear_checkpoint(self, pipeline_name: str) -> bool:
        """Deletes a checkpoint file."""
        checkpoint_path = self._get_checkpoint_path(pipeline_name)
        if checkpoint_path.exists():
            try:
                checkpoint_path.unlink()
                logger.info(f"Checkpoint cleared for pipeline '{pipeline_name}'.")
                return True
            except Exception as e:
                logger.error(f"Failed to clear checkpoint for '{pipeline_name}': {e}")
                return False
        return False

    def checkpoint_exists(self, pipeline_name: str) -> bool:
        """Checks if a checkpoint file exists for a given pipeline."""
        return self._get_checkpoint_path(pipeline_name).exists()

    def list_checkpoints(self) -> List[Dict[str, Any]]:
        """Returns metadata for all saved checkpoints."""
        checkpoints = []
        for file_path in self.checkpoint_dir.glob("*.json"):
            pipeline_name = file_path.stem
            checkpoint_data = self.load_checkpoint(pipeline_name)
            if checkpoint_data:
                checkpoints.append(checkpoint_data)
        return checkpoints