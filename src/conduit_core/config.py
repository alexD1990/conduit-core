# src/conduit_core/config.py

import yaml
from pathlib import Path
from pydantic import BaseModel
from typing import List, Optional

# Pydantic-modeller som definerer strukturen til ingest.yml
class Source(BaseModel):
    name: str
    type: str
    # Endret: Dette feltet er nå valgfritt
    connection_string: Optional[str] = None

class Destination(BaseModel):
    name: str
    type: str
    # Endret: Dette feltet er nå valgfritt
    path: Optional[str] = None

class Resource(BaseModel):
    name: str
    source: str
    destination: str
    query: str
    incremental_column: Optional[str] = None

class IngestConfig(BaseModel):
    sources: List[Source]
    destinations: List[Destination]
    resources: List[Resource]

def load_config(config_path: Path) -> IngestConfig:
    """Laster og validerer ingest.yml fra en filsti."""
    with open(config_path, 'r') as f:
        data = yaml.safe_load(f)
    
    config = IngestConfig(**data)
    return config