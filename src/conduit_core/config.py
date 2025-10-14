# src/conduit_core/config.py

import yaml
from pathlib import Path
from pydantic import BaseModel
from typing import List, Optional

class Source(BaseModel):
    name: str
    type: str
    connection_string: Optional[str] = None
    path: Optional[str] = None
    bucket: Optional[str] = None
    
    # PostgreSQL specific
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = "public"

class Destination(BaseModel):
    name: str
    type: str
    path: Optional[str] = None
    bucket: Optional[str] = None
    connection_string: Optional[str] = None
    
    # PostgreSQL specific
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = "public"
    table: Optional[str] = None

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