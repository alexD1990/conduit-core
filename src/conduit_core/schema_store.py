# src/conduit_core/schema_store.py

import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, UTC  # Use UTC consistently

logger = logging.getLogger(__name__)

class SchemaStore:
    """
    Persists and retrieves pipeline schemas to/from the local filesystem
    to track evolution over time.
    """
    
    BASE_DIR = Path(".conduit")
    SCHEMA_DIR = BASE_DIR / "schemas"

    def __init__(self, base_dir: Optional[Path] = None):
        if base_dir:
            self.base_dir = base_dir
            self.schema_dir = base_dir / "schemas"
        else:
            self.base_dir = self.BASE_DIR
            self.schema_dir = self.SCHEMA_DIR
        
        self.schema_dir.mkdir(parents=True, exist_ok=True)

    def _get_latest_path(self, resource_name: str) -> Path:
        """Get the file path for the latest schema of a resource."""
        return self.schema_dir / f"{resource_name}_latest.json"

    def _get_history_dir(self, resource_name: str) -> Path:
        """Get the directory path for the schema history of a resource."""
        return self.schema_dir / resource_name

    def save_schema(self, resource_name: str, schema: Dict[str, Any]) -> None:
        """
        Saves the given schema as the 'latest' schema for the resource
        and archives the previous 'latest' schema to history.
        """
        latest_path = self._get_latest_path(resource_name)
        history_dir = self._get_history_dir(resource_name)
        history_dir.mkdir(parents=True, exist_ok=True)
        
        # 1. Archive the current 'latest' schema if it exists
        if latest_path.exists():
            try:
                with open(latest_path, 'r') as f:
                    old_schema_data = json.load(f)
                
                # Use timestamp from old schema, or current time if missing
                timestamp_str = old_schema_data.get(
                    'timestamp', 
                    datetime.now(UTC).strftime('%Y%m%dT%H%M%S')
                )
                
                # Sanitize timestamp for filename
                filename_ts = timestamp_str.replace(':', '').replace('-', '').replace('T', '_')
                archive_path = history_dir / f"{filename_ts}.json"
                
                latest_path.rename(archive_path)
                logger.debug(f"Archived old schema to {archive_path}")

            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"Could not archive previous schema at {latest_path}: {e}")

        # 2. Save the new schema as 'latest'
        try:
            schema_to_save = {
                'timestamp': datetime.now(UTC).isoformat(),
                'schema': schema
            }
            with open(latest_path, 'w') as f:
                json.dump(schema_to_save, f, indent=2)
            logger.debug(f"Saved new schema for '{resource_name}' to {latest_path}")
        except (IOError, TypeError) as e:
            logger.error(f"Failed to save new schema to {latest_path}: {e}")
            raise

    def load_last_schema(self, resource_name: str) -> Optional[Dict[str, Any]]:
        """
        Loads the most recently saved schema ('_latest.json') for the given resource.
        """
        latest_path = self._get_latest_path(resource_name)
        
        if not latest_path.exists():
            logger.debug(f"No previous schema found for '{resource_name}' at {latest_path}")
            return None
        
        try:
            with open(latest_path, 'r') as f:
                schema_data = json.load(f)
            # Return the actual schema content, not the wrapper
            return schema_data.get('schema')
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to load last schema from {latest_path}: {e}")
            return None

    def get_schema_history(self, resource_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Retrieves a list of historical schemas for a resource, sorted newest first.
        """
        history_dir = self._get_history_dir(resource_name)
        if not history_dir.exists():
            return []

        try:
            # Get all .json files, sort by name (timestamp) descending
            history_files = sorted(
                history_dir.glob("*.json"), 
                key=lambda p: p.name, 
                reverse=True
            )
            
            history = []
            for file_path in history_files[:limit]:
                try:
                    with open(file_path, 'r') as f:
                        history.append(json.load(f))
                except (json.JSONDecodeError, IOError):
                    logger.warning(f"Could not read schema history file: {file_path}")
            
            return history
        except OSError as e:
            logger.error(f"Could not list schema history for {resource_name}: {e}")
            return []
