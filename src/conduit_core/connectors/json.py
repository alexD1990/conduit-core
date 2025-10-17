# src/conduit_core/connectors/json.py
import json
import logging
from pathlib import Path
from typing import Iterable, Dict, Any, Optional

from .base import BaseSource, BaseDestination
from ..config import Source as SourceConfig, Destination as DestinationConfig
from ..errors import ConnectionError

logger = logging.getLogger(__name__)

class JsonSource(BaseSource):
    def __init__(self, config: Any):
        path = getattr(config, 'path', None) or config.get('path')
        if not path: raise ValueError("JsonSource requires a 'path'.")
        self.filepath = Path(path)
        self.format = getattr(config, 'format', 'auto')

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        if not self.filepath.exists():
            raise FileNotFoundError(f"JSON file not found: {self.filepath}")
        with self.filepath.open('r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content: return
            try:
                data = json.loads(content)
                if isinstance(data, list): yield from data
                elif isinstance(data, dict): yield data
            except json.JSONDecodeError:
                f.seek(0)
                for line in f:
                    if line.strip(): yield json.loads(line)

    def test_connection(self) -> bool:
        if not self.filepath.exists():
            raise ConnectionError(f"JSON file not found: {self.filepath}")
        try:
            with self.filepath.open('r', encoding='utf-8') as f:
                content = f.read(1024)
                if not content.strip(): return True # Empty file is valid
                if '\n' in content: # Potentially NDJSON
                    first_line = content.split('\n', 1)[0]
                    json.loads(first_line)
                else: # Standard JSON
                    json.loads(content)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ConnectionError(f"Invalid JSON format: {e}")
        return True

class JsonDestination(BaseDestination):
    def __init__(self, config: Any):
        path = getattr(config, 'path', None) or config.get('path')
        if not path: raise ValueError("JsonDestination requires a 'path'.")
        self.filepath = Path(path)
        self.format = getattr(config, 'format', 'array')
        self.indent = getattr(config, 'indent', 2)
        self.temp_filepath = self.filepath.with_suffix(f"{self.filepath.suffix}.tmp")
        self.accumulated_records = []

    def write(self, records: Iterable[Dict[str, Any]]):
        self.accumulated_records.extend(list(records))

    def finalize(self):
        if not self.accumulated_records: return
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self.temp_filepath.open('w', encoding='utf-8') as f:
                if self.format == 'ndjson':
                    for record in self.accumulated_records:
                        f.write(json.dumps(record, ensure_ascii=False, default=str) + '\n')
                else:
                    json.dump(self.accumulated_records, f, indent=self.indent, ensure_ascii=False, default=str)
            self.temp_filepath.replace(self.filepath)
        except Exception as e:
            if self.temp_filepath.exists(): self.temp_filepath.unlink()
            raise IOError(f"Failed to write JSON file: {e}") from e
        finally:
            self.accumulated_records.clear()