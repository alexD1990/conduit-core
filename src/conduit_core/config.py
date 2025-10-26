# src/conduit_core/config.py

from typing import Optional, List
from pydantic import BaseModel

# Import QualityCheck from the new quality module
from .quality import QualityCheck


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
    db_schema: Optional[str] = None

    # Checkpoint/Resume fields
    checkpoint_column: Optional[str] = None
    resume: bool = False

    # Schema fields
    infer_schema: bool = False
    schema_sample_size: int = 100


class SchemaEvolutionConfig(BaseModel):
    enabled: bool = False
    mode: str = "manual"
    auto_add_columns: bool = True
    on_column_removed: str = "warn"
    on_type_change: str = "warn"
    update_yaml: bool = True
    track_history: bool = True


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
    db_schema: Optional[str] = None
    table: Optional[str] = None

    # Snowflake specific
    account: Optional[str] = None
    warehouse: Optional[str] = None

    # BigQuery specific
    project: Optional[str] = None
    dataset: Optional[str] = None
    credentials_path: Optional[str] = None

    # JSON-specific
    format: Optional[str] = None
    indent: Optional[int] = None

    # Mode field
    mode: Optional[str] = None

    # Schema fields
    auto_create_table: bool = False
    validate_schema: bool = False # Already exists

    # Schema validation additions (Phase 3)
    strict_validation: bool = True  # NEW - fail on warnings?
    required_columns: Optional[List[str]] = None  # NEW

    # Schema evolution
    schema_evolution: Optional[SchemaEvolutionConfig] = None


class Resource(BaseModel):
    name: str
    source: str
    destination: str
    query: str
    incremental_column: Optional[str] = None
    mode: Optional[str] = None
    export_schema_path: Optional[str] = None  # e.g. "./schemas/users.json"

    # Data Quality
    quality_checks: Optional[List[QualityCheck]] = None


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