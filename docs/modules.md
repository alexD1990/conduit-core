# Module Reference (v1.0)

This document provides an overview of Conduit Core’s internal modules and their roles in the ingestion engine.
Each module contributes to reliability, schema intelligence, or runtime orchestration.

## batch.py — Batch Processing Engine

Handles memory-efficient streaming and batching of records between sources and destinations.

**Key responsibilities:**
- Reads data in configurable batch sizes (`--batch-size`, default 1000)
- Streams data through validation, transformation, and write stages
- Supports generator-based pipelines (no full dataset in memory)
- Tracks record counts, success/failure rates, and throughput

**Core Classes:**
- `BatchIterator`
- `BatchProcessor`

## logging_utils.py — Rich Logging System

Provides structured, colorized logs using the `rich` library.

**Features:**
- Unified logging interface for CLI and internal components
- Context-aware logging (source/destination/task)
- Log levels: DEBUG, INFO, WARNING, ERROR
- Supports environment variable `LOG_LEVEL`
- Optional plain-text mode for CI/CD environments

**Example:**
```text
from conduit_core.logging_utils import get_logger
logger = get_logger("engine")
logger.info("Starting pipeline run")
```

## types.py — Type Conversion Utilities

Centralizes data type normalization and mapping across connectors.

**Responsibilities:**
- Maps Python types ↔ SQL/Parquet/JSON equivalents
- Handles `None` normalization and type coercion
- Provides consistent schema definitions across connectors
- Used by `schema_inference`, `schema_validator`, and connectors

**Example:**
```text
normalize_type("varchar")  # -> "STRING"
```

## schema.py — Schema Inference Engine

Implements automatic schema discovery and comparison.

**Core Classes:**
- `SchemaInferrer`: Samples source data and builds schema objects
- `TableAutoCreator`: Optionally generates `CREATE TABLE` statements
- `SchemaField`: Represents column name, type, and nullability

**Responsibilities:**
- Sample-based inference from sources
- Type detection with fallback heuristics
- Supports export to JSON/YAML for schema versioning

## schema_store.py — Schema Versioning & History

Tracks schema versions over time to detect drift and store baselines.

**Responsibilities:**
- Maintains schema history per resource
- Persists historical schemas to `.conduit/schema_store/`
- Used by `schema-compare` and schema evolution engine
- Enables rollback to prior schema versions

## schema_validator.py — Schema Validation Layer

Performs pre-flight schema compatibility checks between source and destination.

**Responsibilities:**
- Ensures required columns exist
- Detects incompatible type changes
- Flags nullable-to-non-nullable regressions
- Provides actionable validation errors

**Core Function:**
`validate_schema(source_schema, dest_schema) -> ValidationResult`

## schema_evolution.py — Schema Drift Detection

Implements Conduit Core’s schema evolution logic.

**Responsibilities:**
- Detects added, removed, or changed columns
- Applies configuration from `SchemaEvolutionConfig`
- Supports auto-addition of nullable columns
- Integrates with `schema_store.py` and `manifest.py`

**Modes:**
- `manual`: Logs drift, requires user action
- `auto`: Applies compatible changes automatically
- `warn`: Logs warnings but proceeds with run

## quality.py — Data Quality Framework

Replaces legacy `validators.py`.  
Defines reusable data validation rules configurable via YAML.

**Responsibilities:**
- Executes column-level quality checks
- Supports actions: `fail`, `warn`, `dlq`
- Records failed rows to DLQ if configured
- Integrated into `engine.py` before data write

**Supported Checks:**
- `not_null`
- `unique`
- `regex`
- `range`
- `enum`

## manifest.py — Run Manifest System

Tracks metadata for every pipeline run.

**Responsibilities:**
- Records execution stats (records processed, duration, errors)
- Writes to `.conduit/manifest.json`
- Used by `conduit manifest` CLI command
- Enables lineage and observability

**Data stored:**
```text
{
  "pipeline_name": "users_pg",
  "records_read": 1000,
  "records_written": 1000,
  "status": "success",
  "duration_seconds": 64.2
}
```

---
This document reflects all active modules in **Conduit Core v1.0**.  
Internal structure may evolve in future versions (v1.2+).