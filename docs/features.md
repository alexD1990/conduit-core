# Features Guide

Comprehensive guide to Conduit Core's production-grade features for reliable data ingestion.

## Overview

Conduit Core is built on four core pillars:

1. **Reliability** - Atomic operations, retries, and error handling
2. **State Management** - Checkpoints, resume, and incremental loading
3. **Observability** - Manifests, progress tracking, and error logs
4. **Developer Experience** - Declarative config, validation, and testing

---

## Reliability Features

### Atomic Operations

Ensures data integrity by guaranteeing all-or-nothing writes.

**File Destinations:**

Uses temp file → rename pattern:

```python
# Internally, file writes work like:
1. Write to: /tmp/users_abc123.csv.tmp
2. Validate write completed
3. Rename: users_abc123.csv.tmp → users.csv
```

**Benefits**:
- No partial/corrupted files
- Crash-safe writes
- No readers see incomplete data

**Supported Connectors**: CSV, JSON, Parquet

**Database Destinations:**

Uses transactions:

```sql
BEGIN;
  TRUNCATE TABLE users;  -- if full_refresh
  COPY users FROM ...;
COMMIT;
```

**Benefits**:
- Rollback on failure
- Consistent state
- No partial writes visible

**Supported Connectors**: PostgreSQL, MySQL, Snowflake, BigQuery

---

### Automatic Retry Logic

Handles transient network failures with exponential backoff.

**Configuration:**

```python
# Defaults (configured in code)
max_retries = 3
base_delay = 1.0  # seconds
max_delay = 60.0  # seconds
exponential_base = 2
```

**Behavior:**

| Attempt | Delay    | Total Wait |
|---------|----------|------------|
| 1       | 0s       | 0s         |
| 2       | 1s       | 1s         |
| 3       | 2s       | 3s         |
| 4       | 4s       | 7s         |

**Example Log Output:**

```
14:30:01 [WARN] S3 upload failed (attempt 1/3): ConnectionError
14:30:02 [INFO] Retrying in 1.0s...
14:30:03 [WARN] S3 upload failed (attempt 2/3): ConnectionError
14:30:04 [INFO] Retrying in 2.0s...
14:30:06 [INFO] S3 upload successful (attempt 3/3)
```

**Retryable Errors**:
- Network timeouts
- Connection refused
- Temporary service unavailability
- Rate limiting (429 errors)

**Non-Retryable Errors**:
- Authentication failures (401, 403)
- Invalid configuration
- Data validation errors
- Permission denied

**Supported Operations**:
- Database connections
- S3 uploads/downloads
- Snowflake stage uploads
- BigQuery load jobs

---

### Dead Letter Queue (DLQ)

Isolates bad records without stopping the pipeline.

**How It Works:**

```
Source → [Batch Processing] → Validation → Destination
                    ↓
              Failed Records
                    ↓
           ./errors/*.json
```

**Configuration:**

No configuration needed - automatically enabled.

**When Records Fail**:
- Data type mismatches
- Validation rule failures
- Quality check violations (with `action: dlq`)
- Write errors for specific records

**DLQ File Format:**

```json
{
  "resource": "users_pipeline",
  "total_errors": 3,
  "timestamp": "2025-10-31T14:30:45.123Z",
  "errors": [
    {
      "row_number": 142,
      "record": {
        "id": 142,
        "email": "invalid-email",
        "age": "thirty"
      },
      "error_type": "ValueError",
      "error_message": "invalid literal for int() with base 10: 'thirty'",
      "timestamp": "2025-10-31T14:30:12.456Z"
    },
    {
      "row_number": 256,
      "record": {
        "id": 256,
        "email": "user@example.com",
        "age": 150
      },
      "error_type": "QualityCheckFailure",
      "error_message": "age: Value 150 exceeds maximum 120",
      "timestamp": "2025-10-31T14:30:15.789Z"
    }
  ]
}
```

**Location**: `./errors/<pipeline_name>_errors_<timestamp>.json`

**Example Pipeline Behavior:**

```yaml
resources:
  - name: users_pipeline
    source: csv_source
    destination: pg_dest
```

**Execution:**
```
Processing 10,000 records...
[WARN] Record #142 failed: invalid age format
[WARN] Record #256 failed: age validation (>120)
[INFO] Processed 9,998 records successfully
[INFO] 2 records sent to DLQ: ./errors/users_pipeline_errors_20251031_143045.json
```

**Inspecting DLQ:**

```bash
# View failed records
cat ./errors/users_pipeline_errors_*.json | jq '.errors'

# Count failures
cat ./errors/*.json | jq '.total_errors' | awk '{sum+=$1} END {print sum}'

# Extract failed records for reprocessing
cat ./errors/users_pipeline_errors_*.json | jq '.errors[].record' > failed_records.json
```

**Limitations:**
- No automatic replay (v1.0)
- Batch-level failures may lose granular context
- Storage not automatically cleaned up

---

## State Management

### Checkpoint & Resume

Resume long-running pipelines from the last successful point.

**Configuration:**

```yaml
sources:
  - name: large_csv
    type: csv
    path: data/large_file.csv
    resume: true
    checkpoint_column: id  # Must be monotonically increasing
```

**Requirements**:
- `checkpoint_column` must be numeric or timestamp
- Values must increase monotonically (no duplicates, no gaps okay)
- Column must be present in all records

**How It Works:**

```
Run 1: Process records 1-5000 → Crash at record 5000
       Checkpoint saved: {"last_value": 5000}

Run 2: Resume from checkpoint
       Automatic filter: WHERE id > 5000
       Process records 5001-10000 → Success
       Checkpoint cleared
```

**Checkpoint Storage:**

Location: `.checkpoints/<pipeline_name>.json`

```json
{
  "resource_name": "large_pipeline",
  "checkpoint_column": "id",
  "last_value": 5000,
  "timestamp": "2025-10-31T14:30:00Z"
}
```

**Behavior:**

| Scenario                  | Behavior                          |
|---------------------------|-----------------------------------|
| First run                 | No checkpoint, process all        |
| Run fails mid-processing  | Checkpoint saved with last ID     |
| Resume run                | Start from checkpoint + 1         |
| Run completes             | Checkpoint deleted                |
| Force full rerun          | Delete `.checkpoints/` directory  |

**Example:**

```yaml
sources:
  - name: events_csv
    type: csv
    path: data/events.csv
    resume: true
    checkpoint_column: event_id

resources:
  - name: load_events
    source: events_csv
    destination: warehouse
```

**Run 1 (fails):**
```
Processing events...
Processed 500,000 records (event_id: 1-500000)
[ERROR] Connection lost to warehouse
[INFO] Checkpoint saved: event_id=500000
```

**Run 2 (resume):**
```
[INFO] Checkpoint found: event_id=500000
[INFO] Resuming from event_id > 500000
Processing events...
Processed 250,000 records (event_id: 500001-750000)
[SUCCESS] Pipeline completed
[INFO] Checkpoint cleared
```

**Force Full Rerun:**

```bash
rm -rf .checkpoints/
conduit run ingest.yml
```

**Best Practices:**
- Use for files >100MB or >100k records
- Choose stable, immutable checkpoint columns
- Test resume behavior before production
- Monitor checkpoint directory size

---

### Incremental Loading

Process only new or changed records based on a timestamp or ID column.

**Configuration:**

```yaml
resources:
  - name: incremental_users
    source: db_source
    destination: warehouse
    query: "SELECT * FROM users"
    incremental_column: updated_at
    write_mode: append
```

**How It Works:**

```
Run 1: Process all records
       State saved: {"incremental_users": "2025-10-31 14:00:00"}

Run 2: Engine automatically appends:
       WHERE updated_at > '2025-10-31 14:00:00'
       Process only new records
       State updated: {"incremental_users": "2025-10-31 15:00:00"}
```

**State Storage:**

Location: `.conduit_state.json`

```json
{
  "incremental_users": "2025-10-31T15:00:00Z",
  "incremental_orders": 150042,
  "other_pipeline": "2025-10-30T10:00:00Z"
}
```

**Supported Column Types:**
- Timestamps: `updated_at`, `modified_date`, `event_timestamp`
- Integers: `id`, `sequence_number`, `version`
- Dates: `created_date`, `batch_date`

**Example - Timestamp-Based:**

```yaml
sources:
  - name: prod_db
    type: postgresql
    host: ${POSTGRES_HOST}
    database: production
    user: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}

destinations:
  - name: warehouse_db
    type: postgresql
    host: ${WAREHOUSE_HOST}
    database: warehouse
    user: ${WAREHOUSE_USER}
    password: ${WAREHOUSE_PASSWORD}
    table: events

resources:
  - name: sync_events
    source: prod_db
    destination: warehouse_db
    query: "SELECT * FROM events"
    incremental_column: event_timestamp
    write_mode: append
```

**Execution:**

```
Run 1 (full load):
  Query: SELECT * FROM events
  Records: 1,000,000
  Max event_timestamp: 2025-10-31 14:00:00
  State saved: 2025-10-31 14:00:00

Run 2 (incremental):
  Query: SELECT * FROM events WHERE event_timestamp > '2025-10-31 14:00:00'
  Records: 5,000
  Max event_timestamp: 2025-10-31 15:00:00
  State updated: 2025-10-31 15:00:00
```

**Example - ID-Based:**

```yaml
resources:
  - name: sync_orders
    source: orders_db
    destination: warehouse
    query: "SELECT * FROM orders"
    incremental_column: order_id
    write_mode: append
```

**Execution:**

```
Run 1: Process orders 1-10000
Run 2: Process orders 10001-15000
Run 3: Process orders 15001-18000
```

**Force Full Refresh:**

```bash
# Delete state file
rm .conduit_state.json

# Or use full_refresh mode
conduit run ingest.yml --resource sync_events
```

```yaml
resources:
  - name: sync_events
    write_mode: full_refresh  # Ignores incremental state
```

**Best Practices:**
- Always use `append` mode with incremental columns
- Ensure source column is indexed for performance
- Choose columns that are immutable (never updated backwards)
- Monitor state file for unexpected values
- Backfill strategy: delete state + run once

**Limitations:**
- No support for deletes/updates detection (use CDC in v1.2)
- Requires source column to be monotonically increasing
- Single column only (no composite keys in v1.0)

---

## Batch Processing

Streams data in configurable batches for memory efficiency.

**Default Batch Size**: 1000 records

**Configuration:**

```yaml
resources:
  - name: large_pipeline
    source: big_source
    destination: warehouse
    batch_size: 5000  # Optional: increase for performance
```

**Memory Usage:**

| Batch Size | Peak Memory | Throughput |
|------------|-------------|------------|
| 1,000      | ~10MB       | 5,000/s    |
| 5,000      | ~50MB       | 15,000/s   |
| 10,000     | ~100MB      | 25,000/s   |

**How It Works:**

```python
# Simplified internal logic
for batch in read_in_batches(source, batch_size=1000):
    validated = validate(batch)
    write(destination, validated)
    checkpoint_save()
```

**Streaming Benefits:**
- Processes files larger than available RAM
- Consistent memory footprint
- Interruptible (with checkpoints)

**Example - Large File:**

```yaml
sources:
  - name: huge_csv
    type: csv
    path: data/10GB_file.csv
    resume: true
    checkpoint_column: id

resources:
  - name: process_huge_file
    source: huge_csv
    destination: warehouse
    batch_size: 10000  # Larger batches for performance
```

**Execution:**

```
Processing 10,000,000 records in batches of 10,000...
Batch 1: 10,000 records (15,234 rows/sec)
Batch 2: 10,000 records (16,123 rows/sec)
...
Batch 1000: 10,000 records (15,987 rows/sec)
Completed in 650 seconds
```

---

## Observability

### Pipeline Manifest

Comprehensive audit trail of all pipeline executions.

**Location**: `manifest.json`

**Format:**

```json
{
  "entries": [
    {
      "run_id": "run_20251031_143052_a7f3e2",
      "pipeline_name": "users_sync",
      "source_type": "postgresql",
      "destination_type": "snowflake",
      "started_at": "2025-10-31T14:30:52Z",
      "completed_at": "2025-10-31T14:35:20Z",
      "status": "success",
      "records_read": 10000,
      "records_written": 9998,
      "records_failed": 2,
      "duration_seconds": 268.5,
      "preflight_duration_s": 2.3,
      "preflight_warnings": [],
      "error_message": null
    }
  ]
}
```

**Status Values:**
- `success`: All records processed successfully
- `partial`: Some records failed (sent to DLQ)
- `failed`: Pipeline crashed or validation failed

**Viewing Manifest:**

```bash
# All runs
conduit manifest

# Specific pipeline
conduit manifest --pipeline users_sync

# Only failures
conduit manifest --failed
```

**Example Output:**

```
📜 Pipeline Manifest

run_20251031_143052 | users_sync | success | 10,000 records | 268.5s
run_20251031_120034 | orders_load | failed | 0 records | 5.2s
  Error: Connection timeout to Snowflake
run_20251030_093015 | events_sync | partial | 50,000 records | 450.1s
  Warnings: 25 records failed validation
```

**Use Cases:**
- Debugging failures
- Tracking pipeline performance
- Auditing data lineage
- Monitoring record counts
- SLA compliance

---

### Progress Tracking

Real-time feedback during pipeline execution.

**Progress Bar:**

```
Processing users_sync: |████████████████████| 10,000/10,000 • 0:04 • 2,500 rows/sec
```

**Components:**
- Visual progress bar
- Records processed / Total
- Elapsed time
- Throughput (rows/sec)
- Estimated time remaining

**Behavior:**
- Interactive terminals: Rich progress bar
- Non-interactive (CI/CD): Text logging
- Dry-run mode: Shows progress without writes

**Log Output:**

```
14:30:52 [INFO] START resource users_sync
14:30:52 [INFO] Source: postgresql (prod_db)
14:30:52 [INFO] Destination: snowflake (warehouse)
14:30:54 [INFO] Batch 1: 1,000 records written
14:31:02 [INFO] Batch 2: 1,000 records written
...
14:35:20 [INFO] DONE resource users_sync [in 268.5s]
14:35:20 [INFO] Total: 10,000 records read, 9,998 written, 2 failed
```

---

## Write Modes

Control how data is written to destinations.

### Append Mode

Adds new records without removing existing data.

**Configuration:**

```yaml
destinations:
  - name: warehouse
    type: postgresql
    table: users
    write_mode: append  # Default
```

**Behavior:**

```sql
-- No truncation
INSERT INTO users (id, name, email) VALUES (...);
```

**Use Cases:**
- Incremental loading
- Event streams
- Append-only tables
- Time-series data

---

### Full Refresh Mode

Deletes all existing data before writing new data.

**Configuration:**

```yaml
destinations:
  - name: warehouse
    type: postgresql
    table: users
    write_mode: full_refresh
```

**Behavior:**

```sql
BEGIN;
  TRUNCATE TABLE users;  -- Remove all existing records
  INSERT INTO users (id, name, email) VALUES (...);
COMMIT;
```

**Use Cases:**
- Daily snapshots
- Complete rebuilds
- Dimension tables
- Reference data

**Warning**: Deletes all data in destination before writing!

---

## Best Practices

### Reliability

1. **Always use preflight**:
   ```bash
   conduit preflight ingest.yml && conduit run ingest.yml
   ```

2. **Enable checkpoints for large files**:
   ```yaml
   sources:
     - name: large_file
       resume: true
       checkpoint_column: id
   ```

3. **Monitor DLQ regularly**:
   ```bash
   ls -lh ./errors/
   conduit manifest --failed
   ```

### State Management

1. **Use incremental loading for large tables**:
   ```yaml
   resources:
     - name: sync_events
       incremental_column: updated_at
       write_mode: append
   ```

2. **Backup state before major changes**:
   ```bash
   cp .conduit_state.json .conduit_state.json.backup
   ```

3. **Version control checkpoint strategy**:
   ```bash
   # .gitignore
   .conduit_state.json
   .checkpoints/
   ```

### Observability

1. **Review manifest regularly**:
   ```bash
   conduit manifest --failed
   ```

2. **Alert on failures**:
   ```bash
   conduit manifest --failed | grep -q "failed" && alert-team
   ```

3. **Track performance trends**:
   ```bash
   cat manifest.json | jq '.entries[] | {pipeline: .pipeline_name, duration: .duration_seconds}'
   ```

---

## Next Steps

- **Usage Guide**: [docs/usage.md](usage.md) - CLI commands and examples
- **Connectors**: [docs/connectors.md](connectors.md) - Connector configuration
- **Data Quality**: [docs/data-quality.md](data-quality.md) - Validation rules
- **Architecture**: [docs/architecture.md](architecture.md) - Design patterns
