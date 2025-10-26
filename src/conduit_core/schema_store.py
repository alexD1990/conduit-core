import hashlib
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, UTC

logger = logging.getLogger(__name__)


def compute_schema_hash(schema: Dict[str, Any]) -> str:
    sorted_cols = sorted(schema.get('columns', []), key=lambda x: x['name'])
    schema_str = json.dumps(sorted_cols, sort_keys=True)
    return hashlib.sha256(schema_str.encode()).hexdigest()[:16]


class SchemaStore:
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
        self.audit_dir = self.base_dir / "schema_audit"
        self.audit_dir.mkdir(parents=True, exist_ok=True)

    def _get_latest_path(self, resource_name: str) -> Path:
        return self.schema_dir / f"{resource_name}_latest.json"

    def _get_history_dir(self, resource_name: str) -> Path:
        return self.schema_dir / resource_name

    def save_schema(self, resource_name: str, schema: Dict[str, Any]) -> int:
        version = self._get_next_version(resource_name)
        schema_hash = compute_schema_hash(schema)
        
        latest_path = self._get_latest_path(resource_name)
        history_dir = self._get_history_dir(resource_name)
        history_dir.mkdir(parents=True, exist_ok=True)
        
        if latest_path.exists():
            try:
                with open(latest_path, 'r') as f:
                    old_schema_data = json.load(f)
                
                timestamp_str = old_schema_data.get(
                    'timestamp', 
                    datetime.now(UTC).strftime('%Y%m%dT%H%M%S')
                )
                
                filename_ts = timestamp_str.replace(':', '').replace('-', '').replace('T', '_')
                archive_path = history_dir / f"{filename_ts}.json"
                
                latest_path.rename(archive_path)
                logger.debug(f"Archived old schema to {archive_path}")

            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"Could not archive previous schema at {latest_path}: {e}")

        try:
            schema_to_save = {
                'timestamp': datetime.now(UTC).isoformat(),
                'schema': schema,
                'hash': schema_hash,
                'version': version
            }
            with open(latest_path, 'w') as f:
                json.dump(schema_to_save, f, indent=2)
            logger.debug(f"Saved new schema for '{resource_name}' to {latest_path}")
        except (IOError, TypeError) as e:
            logger.error(f"Failed to save new schema to {latest_path}: {e}")
            raise
        
        return version

    def load_last_schema(self, resource_name: str) -> Optional[Dict[str, Any]]:
        latest_path = self._get_latest_path(resource_name)
        
        if not latest_path.exists():
            logger.debug(f"No previous schema found for '{resource_name}' at {latest_path}")
            return None
        
        try:
            with open(latest_path, 'r') as f:
                schema_data = json.load(f)
            return schema_data
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to load last schema from {latest_path}: {e}")
            return None

    def get_schema_history(self, resource_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        history_dir = self._get_history_dir(resource_name)
        if not history_dir.exists():
            return []

        try:
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

    def log_evolution_event(
        self, 
        resource_name: str, 
        changes: Dict[str, Any], 
        ddl_applied: List[str],
        old_version: int,
        new_version: int
    ) -> Path:
        timestamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
        audit_entry = {
            'timestamp': datetime.now(UTC).isoformat(),
            'resource': resource_name,
            'old_version': old_version,
            'new_version': new_version,
            'changes': changes,
            'ddl_executed': ddl_applied
        }
        
        audit_file = self.audit_dir / f"{resource_name}_{timestamp}.json"
        with open(audit_file, 'w') as f:
            json.dump(audit_entry, f, indent=2)
        
        return audit_file

    def _get_next_version(self, resource_name: str) -> int:
        latest = self.load_last_schema(resource_name)
        return latest.get('version', 0) + 1 if latest else 1