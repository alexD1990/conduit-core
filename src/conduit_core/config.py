# src/conduit_core/config.py

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator, model_validator

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
    connection_string: Optional[str] = None
    table: Optional[str] = None
    write_mode: str = "append"
    primary_keys: Optional[List[str]] = None
    update_strategy: str = "update_all"
    isolation_level: str = "READ COMMITTED"
    checkpoint_interval: Optional[int] = None
    enable_type_coercion: bool = True  
    strict_type_coercion: bool = False  
    custom_null_values: Optional[List[str]] = None  
    type_mappings: Optional[Dict[str, str]] = None  
    path: Optional[str] = None
    bucket: Optional[str] = None

    @model_validator(mode='after')
    def validate_merge_requirements(self):
        """Validate that merge mode has required config."""
        if self.write_mode == "merge" and not self.primary_keys:
            raise ValueError(f"write_mode='merge' requires primary_keys to be specified")
        return self
    

    # Database fields
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    db_schema: Optional[str] = None

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

class IncrementalConfig(BaseModel):
    """Configuration for incremental data loading."""
    column: str  # The column to track (e.g., updated_at, id)
    strategy: str = "timestamp"  # timestamp, sequential, cursor
    lookback_seconds: Optional[int] = None  # Reprocess last N seconds
    detect_gaps: bool = True  # Warn on missing sequential values
    initial_value: Optional[Any] = None  # Start value for first run
    
    @field_validator('strategy')
    def validate_strategy(cls, v):
        allowed = ['timestamp', 'sequential', 'cursor']
        if v not in allowed:
            raise ValueError(f"strategy must be one of {allowed}")
        return v

class Resource(BaseModel):
    name: str
    source: str
    destination: str
    query: str
    incremental_column: Optional[str] = None  # Legacy (deprecated)
    incremental: Optional[IncrementalConfig] = None  # NEW
    mode: Optional[str] = None
    export_schema_path: Optional[str] = None
    quality_checks: Optional[List[QualityCheck]] = None
    enhanced_quality_checks: Optional[Dict[str, Any]] = None
    
    @model_validator(mode='after')
    def convert_legacy_incremental(self):
        """Convert old incremental_column to new incremental config for backward compatibility."""
        if self.incremental_column and not self.incremental:
            self.incremental = IncrementalConfig(
                column=self.incremental_column,
                strategy='timestamp'
            )
        return self


class IngestConfig(BaseModel):
    sources: List[Source]
    destinations: List[Destination]
    resources: List[Resource]
    parallel_extraction: Optional[Dict[str, Any]] = None

def load_config(filepath: str) -> IngestConfig:
    """Load and validate ingest config from YAML file."""
    import yaml

    with open(filepath, 'r') as f:
        config_dict = yaml.safe_load(f)

    return IngestConfig(**config_dict)