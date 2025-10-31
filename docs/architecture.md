# Architecture

## Design Patterns

### Core Patterns
- **Strategy Pattern**: `BaseSource`/`BaseDestination` define contracts; connectors implement strategies
- **Factory Pattern**: Registry auto-discovers and instantiates connectors dynamically
- **Context Manager**: `ManifestTracker` uses `with` for automatic timing/status tracking
- **Atomic Write**: temp → rename for files; transactions for databases
- **Retry with Backoff**: `@retry_with_backoff` decorator for transient failures
- **Batch Processing**: Generator-based streaming for memory efficiency
- **Checkpoint/Resume**: Fault-tolerant execution with automatic state recovery

### Reliability Patterns
- **Dead Letter Queue (DLQ)**: Failed records isolated to `./errors/` with full context
- **Preflight Validation**: Connection tests, schema validation, compatibility checks
- **Incremental State**: Track high-water marks for delta loads (`WHERE column > last_value`)
- **Quality Gates**: Configurable validators with `fail`/`warn`/`dlq` actions

## Tech Stack

**Core Dependencies:**
- Python 3.12+
- Poetry (dependency management)
- Pydantic (config validation)
- Typer + Rich (CLI + progress bars)
- PyTest (testing framework)

**Connector Libraries:**
- boto3 (S3)
- psycopg2 (PostgreSQL)
- snowflake-connector-python (Snowflake)
- google-cloud-bigquery (BigQuery)
- pyarrow (Parquet)

## Project Structure

```
src/conduit_core/
├── connectors/              # All data adapters
│   ├── base.py             # BaseSource + BaseDestination contracts
│   ├── registry.py         # Auto-discovery and factory
│   ├── csv.py              # CSV source/dest with encoding detection
│   ├── json.py             # JSON/NDJSON support
│   ├── parquet.py          # Parquet via PyArrow
│   ├── s3.py               # S3 source/dest
│   ├── postgresql.py       # PostgreSQL via COPY
│   ├── snowflake.py        # Snowflake staged loads
│   ├── bigquery.py         # BigQuery Load Jobs
│   ├── dummy.py            # Test connectors
│   └── utils/
│       └── retry.py        # Exponential backoff decorator
│
├── engine.py               # Pipeline orchestration + batch processing
├── config.py               # Pydantic models (IngestConfig, Resource)
├── cli.py                  # Typer CLI commands
│
├── checkpoint.py           # Resume from failure
├── state.py                # Incremental sync high-water marks
├── manifest.py             # Audit trail + run history
├── errors.py               # DLQ + custom exceptions
│
├── schema.py               # Inference engine
├── schema_store.py         # Version history
├── schema_validator.py     # Pre-flight schema checks
├── schema_evolution.py     # Drift detection + auto-apply
│
├── quality.py              # Data validation framework
├── batch.py                # Streaming batch utilities
├── types.py                # Type coercion + normalization
├── logging_utils.py        # Rich logging + progress bars
│
└── engine_modules/         # Modular engine components
    ├── preflight.py        # Validation orchestration
    ├── schema_operations.py
    ├── type_coercion.py
    ├── incremental_sync.py
    └── quality_checks.py

tests/                      # 141+ unit + integration tests
├── connectors/             # Connector-specific tests
├── integration/            # E2E scenarios
└── test_*.py              # Feature tests

pyproject.toml              # Poetry config
README.md                   # Project overview
LICENSE                     # BSL 1.1
```

## Data Flow

### Execution Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. CONFIG LOADING (config.py)                                   │
│    • Parse ingest.yml via Pydantic                              │
│    • Validate sources, destinations, resources                  │
│    • Load .env variables                                        │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│ 2. PREFLIGHT CHECKS (engine_modules/preflight.py)               │
│    • Test source/destination connections                        │
│    • Validate schema compatibility                              │
│    • Verify incremental state                                   │
│    • Quality check configuration                                │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│ 3. SCHEMA OPERATIONS (schema_operations.py)                     │
│    • Infer schema from source (if enabled)                      │
│    • Detect schema drift vs destination                         │
│    • Auto-create table (if configured)                          │
│    • Apply schema evolution rules                               │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│ 4. SOURCE READING (connectors/base.py)                          │
│    • Initialize source connector via registry                   │
│    • Apply incremental WHERE clause (if configured)             │
│    • Stream records in batches (generator)                      │
│    • Checkpoint progress periodically                           │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│ 5. BATCH PROCESSING (engine.py + batch.py)                      │
│    • Iterate batches (default: 1000 records)                    │
│    • Apply quality validators (quality.py)                      │
│    • Type coercion (types.py)                                   │
│    • Route failures to DLQ (errors.py)                          │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│ 6. DESTINATION WRITING (connectors/base.py)                     │
│    • Write batch atomically                                     │
│    • Retry on transient failures                                │
│    • Update progress bar (Rich)                                 │
│    • Track success/failure counts                               │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│ 7. FINALIZATION (manifest.py + state.py)                        │
│    • Persist incremental state                                  │
│    • Write manifest entry                                       │
│    • Clear checkpoints on success                               │
│    • Generate summary report                                    │
└─────────────────────────────────────────────────────────────────┘
```

### Component Interactions

```
┌──────────────┐
│   CLI        │──┐
│  (cli.py)    │  │
└──────────────┘  │
                  ↓
┌──────────────────────────────────────────────┐
│         Engine (engine.py)                   │
│  • Orchestrates execution                    │
│  • Manages batch loop                        │
│  • Coordinates all subsystems                │
└──────────────────────────────────────────────┘
       │         │          │          │
       ↓         ↓          ↓          ↓
┌─────────┐ ┌─────────┐ ┌──────────┐ ┌──────────┐
│Registry │ │Manifest │ │Checkpoint│ │Quality   │
│(factory)│ │(audit)  │ │(resume)  │ │(validate)│
└─────────┘ └─────────┘ └──────────┘ └──────────┘
       │
       ↓
┌────────────────────────────────────────────┐
│    Connector Ecosystem                     │
│  ┌────────────┐  ┌───────────────┐         │
│  │BaseSource  │  │BaseDestination│         │
│  └────────────┘  └───────────────┘         │
│       ↑                  ↑                 │
│       │                  │                 │
│  ┌────┴─────┐      ┌─────┴─────┐           │
│  │CSV       │      │PostgreSQL │           │
│  │JSON      │      │Snowflake  │           │
│  │Parquet   │      │BigQuery   │           │
│  │S3        │      │S3         │           │
│  │PostgreSQL│      │...        │           │
│  └──────────┘      └───────────┘           │
└────────────────────────────────────────────┘
```

## Connector Architecture

### Base Contracts

**BaseSource** (abstract):
```python
def read(query: str) -> Iterator[Dict[str, Any]]:
    """Stream records from source"""
    
def estimate_total_records() -> Optional[int]:
    """Return count for progress bar (optional)"""
    
def test_connection() -> bool:
    """Validate connectivity"""
```

**BaseDestination** (abstract):
```python
def write(records: List[Dict[str, Any]]) -> None:
    """Write batch atomically"""
    
def finalize() -> None:
    """Cleanup/commit (optional)"""
    
def test_connection() -> bool:
    """Validate connectivity"""
    
def execute_ddl(sql: str) -> None:
    """Run DDL for auto-create (DB connectors)"""
    
def get_table_schema() -> Dict[str, Any]:
    """Query current schema (DB connectors)"""
```

### Registry System

```python
# connectors/registry.py
def get_source_connector_map() -> Dict[str, Type[BaseSource]]:
    """Auto-discover all BaseSource implementations"""
    
def get_destination_connector_map() -> Dict[str, Type[BaseDestination]]:
    """Auto-discover all BaseDestination implementations"""
```

**Discovery Process:**
1. Scan `connectors/` package
2. Find all classes inheriting `BaseSource`/`BaseDestination`
3. Map `type` field → connector class
4. Cache in registry

**Example:**
```yaml
sources:
  - name: my_csv
    type: csv  # ← Registry maps "csv" → CsvSource class
```

## Schema Intelligence

### Four-Phase System

**1. Inference (`schema.py`)**
- Samples N records (configurable)
- Detects types using heuristics
- Handles nullability
- Exports to JSON/YAML

**2. Validation (`schema_validator.py`)**
- Pre-flight compatibility check
- Detects breaking changes
- Type mismatches
- Missing required columns

**3. Evolution (`schema_evolution.py`)**
- Detects drift (added/removed/changed columns)
- Applies rules: `manual`/`auto`/`warn`
- Generates ALTER TABLE statements
- Integrates with `schema_store`

**4. Versioning (`schema_store.py`)**
- Persists schema history to `.conduit/schema_store/`
- Enables `conduit schema-compare`
- Tracks baselines per resource

### Evolution Modes

| Mode     | Behavior                                      |
|----------|-----------------------------------------------|
| `manual` | Log drift, require user action (default)     |
| `auto`   | Auto-apply compatible changes (add columns)  |
| `warn`   | Log warnings, continue execution             |

## Quality Framework

### Validator System

**Built-in Checks:**
- `not_null`: Column must have value
- `unique`: No duplicates in batch
- `regex`: Pattern matching
- `range`: Min/max bounds
- `enum`: Allowed value list

**Actions:**
- `fail`: Halt pipeline immediately
- `warn`: Log warning, continue
- `dlq`: Route to dead letter queue

**Configuration:**
```yaml
resources:
  - name: users
    quality_checks:
      - column: email
        check: regex
        pattern: ".+@.+\\..+"
        action: fail
      - column: age
        check: range
        min_value: 18
        max_value: 100
        action: warn
```

### Custom Validators

```python
from conduit_core.quality import QualityCheckRegistry

def is_valid_sku(value: Any) -> Tuple[bool, Optional[str]]:
    if not re.match(r"^[A-Z]{3}-\d{5}$", str(value)):
        return False, "Invalid SKU format"
    return True, None

QualityCheckRegistry.register_custom("valid_sku", is_valid_sku)
```

## State Management

### Incremental Loading

**High-Water Mark Pattern:**
```yaml
resources:
  - name: orders
    source: postgres_source
    destination: warehouse
    incremental_column: updated_at  # Track this column
```

**Automatic Query Rewrite:**
```sql
-- Original query
SELECT * FROM orders

-- First run (no state)
SELECT * FROM orders

-- Subsequent runs (with state)
SELECT * FROM orders WHERE updated_at > '2025-01-15 10:30:00'
```

**State Persistence:**
- Stored in `.conduit_state.json`
- Format: `{resource_name: {incremental_column: last_value}}`
- Atomic updates on successful runs

### Checkpoint System

**Failure Recovery:**
1. Save checkpoint every N batches
2. On crash/error, state preserved in `.checkpoints/{resource_name}.json`
3. Next run: `conduit run --resume`
4. Engine reads checkpoint, skips processed batches
5. On success, checkpoint deleted

**Checkpoint Data:**
```json
{
  "offset": 5000,
  "last_record": {"id": 5000, "timestamp": "..."},
  "batch_size": 1000,
  "created_at": "2025-01-15T10:30:00Z"
}
```

## Error Handling

### Three-Tier Strategy

**1. Retry Layer (`utils/retry.py`)**
```python
@retry_with_backoff(max_attempts=3, base_delay=1.0)
def connect_to_database():
    # Transient failures (network, timeout) auto-retry
```

**2. Dead Letter Queue (`errors.py`)**
```python
# Failed records isolated with context
{
  "record": {"id": 123, "email": "invalid"},
  "error": "regex validation failed",
  "row_number": 4501,
  "timestamp": "2025-01-15T10:30:00Z"
}
```

**3. Manifest Logging (`manifest.py`)**
```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "partial_success",
  "records_read": 10000,
  "records_written": 9500,
  "records_failed": 500,
  "duration_seconds": 142.3,
  "error_summary": "500 records failed quality checks"
}
```

## Observability

### Manifest System

**Run Metadata:**
- Unique run ID
- Start/end timestamps
- Record counts (read/written/failed)
- Duration
- Error summaries
- Preflight results
- Schema changes applied

**Query Interface:**
```bash
conduit manifest                    # Last 10 runs
conduit manifest --all              # Full history
conduit manifest --status=failed    # Filter by status
```

### Progress Tracking

**Rich Progress Bars:**
- Real-time throughput (rows/sec)
- Estimated time remaining
- Batch progress
- Auto-fallback to text in CI/CD

**Logging Levels:**
- `DEBUG`: Internal state transitions
- `INFO`: Pipeline milestones
- `WARNING`: Non-fatal issues
- `ERROR`: Critical failures

## Extension Points

### Adding Connectors

1. Inherit from `BaseSource` or `BaseDestination`
2. Implement required methods
3. Place in `connectors/` directory
4. Registry auto-discovers on next run

**Minimal Example:**
```python
# connectors/my_source.py
from .base import BaseSource

class MySource(BaseSource):
    def read(self, query: str = None):
        for record in my_data_source:
            yield record
    
    def test_connection(self) -> bool:
        return True  # Implement check
```

### Custom Quality Checks

See "Custom Validators" section above. Register via `QualityCheckRegistry.register_custom()`.

### Schema Evolution Hooks

Override methods in `schema_evolution.py`:
- `_generate_add_column_ddl()`
- `_generate_alter_column_ddl()`

---

*This architecture supports the vision: make data ingestion as reliable and declarative as dbt makes transformation.*
