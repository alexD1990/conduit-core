# src/conduit_core/config.py

from typing import Optional, List
from pydantic import BaseModel


class Source(BaseModel):
    name: str
    type: str
    path: Optional[str] = None
    bucket: Optional[str] = None
    connection_string: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    
    # Checkpoint/Resume fields
    checkpoint_column: Optional[str] = None  # ADDED
    resume: bool = False                     # ADDED


class Destination(BaseModel):
    name: str
    type: str
    path: Optional[str] = None
    bucket: Optional[str] = None
    connection_string: Optional[str] = None
    
    # Database fields
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    table: Optional[str] = None
    
    # Snowflake specific
    account: Optional[str] = None
    warehouse: Optional[str] = None
    
    # BigQuery specific
    project: Optional[str] = None
    dataset: Optional[str] = None
    credentials_path: Optional[str] = None

    # Mode field
    mode: Optional[str] = None


class Resource(BaseModel):
    name: str
    source: str
    destination: str
    query: str
    incremental_column: Optional[str] = None
    mode: Optional[str] = None


class IngestConfig(BaseModel):
    sources: List[Source]
    destinations: List[Destination]
    resources: List[Resource]

def load_config(filepath: str) -> IngestConfig:
    """Load and validate ingest config from YAML file."""
    import yaml
    
    with open(filepath, 'r') as f:
        config_dict = yaml.safe_load(f)
    
    return IngestConfig(**config_dict)