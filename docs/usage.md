# Usage Guide

Complete guide to using Conduit Core CLI commands and configuring pipelines.

## CLI Commands

### `conduit run`

Execute pipelines with automatic preflight validation.

```bash
# Run all resources
conduit run ingest.yml

# Run specific resource
conduit run ingest.yml --resource users_pipeline

# Dry-run (no writes)
conduit run ingest.yml --dry-run

# Skip preflight (not recommended)
conduit run ingest.yml --skip-preflight
```

**Execution Flow**:
1. **Preflight checks** - Validates config, connections, schema
2. **Data processing** - Reads in batches, applies transformations
3. **Writes** - Atomic writes with retry logic
4. **Audit** - Logs execution to `manifest.json`

### `conduit preflight`

Validate configuration and connections without moving data.

```bash
# Check all resources
conduit preflight ingest.yml

# Check specific resource
conduit preflight ingest.yml --resource users_pipeline
```

**Checks Performed**:
- ✅ YAML syntax and schema validation
- ✅ Source connection test
- ✅ Destination connection test
- ✅ Schema compatibility
- ✅ Quality rules validation

**Exit codes**: `0` (success), `1` (failure)

### `conduit schema`

Infer and export schema from a source.

```bash
# Export to JSON
conduit schema users_pipeline --output schema.json

# Export to YAML
conduit schema users_pipeline --output schema.yaml --format yaml

# Preview without saving
conduit schema users_pipeline

# Sample more records
conduit schema users_pipeline --sample-size 1000
```

### `conduit manifest`

View execution history and audit trail.

```bash
# View all runs
conduit manifest

# Filter by pipeline
conduit manifest --pipeline users_pipeline

# Show only failures
conduit manifest --failed
```

## Pipeline Configuration

### Basic Structure

```yaml
sources:
  - name: source_identifier
    type: connector_type
    # connector-specific config

destinations:
  - name: destination_identifier
    type: connector_type
    # connector-specific config

resources:
  - name: pipeline_name
    source: source_identifier
    destination: destination_identifier
    query: "SQL query or n/a"
    # optional settings
```

### Environment Variables

Reference credentials securely:

```yaml
sources:
  - name: prod_db
    type: postgresql
    host: ${POSTGRES_HOST}
    database: ${POSTGRES_DATABASE}
    user: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}
```

### Write Modes

Control how data is written to destinations:

```yaml
resources:
  - name: users_sync
    source: csv_source
    destination: pg_dest
    write_mode: append  # Options: append (default), full_refresh
```

**Modes**:
- `append`: Add new records (default)
- `full_refresh`: Truncate/overwrite before writing

### Incremental Loading

Process only new or changed records:

```yaml
sources:
  - name: events_db
    type: postgresql
    host: ${POSTGRES_HOST}
    database: analytics

resources:
  - name: incremental_events
    source: events_db
    destination: warehouse
    query: "SELECT * FROM events"
    incremental_column: created_at
    write_mode: append
```

**How it works**:
- First run: Processes all records
- Subsequent runs: Only processes records where `created_at > last_value`
- State stored in `.conduit_state.json`
- Engine automatically appends `WHERE` clause - no manual placeholders needed

### Checkpoint/Resume

For long-running pipelines, enable checkpoints:

```yaml
sources:
  - name: large_file
    type: csv
    path: large_dataset.csv
    resume: true
    checkpoint_column: id  # Must be monotonically increasing
```

**Features**:
- Automatically saves progress to `.checkpoints/`
- Resumes from last checkpoint on failure
- Clear checkpoints: `rm -rf .checkpoints/`

## Examples

### Example 1: CSV to JSON

Simple file conversion.

```yaml
sources:
  - name: users_csv
    type: csv
    path: data/users.csv

destinations:
  - name: users_json
    type: json
    path: output/users.json
    indent: 2

resources:
  - name: csv_to_json
    source: users_csv
    destination: users_json
```

**Run**:
```bash
conduit run ingest.yml
```

### Example 2: PostgreSQL to Snowflake

Database replication with incremental sync.

```yaml
sources:
  - name: pg_source
    type: postgresql
    host: ${POSTGRES_HOST}
    database: production
    user: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}

destinations:
  - name: sf_warehouse
    type: snowflake
    account: ${SNOWFLAKE_ACCOUNT}
    user: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
    warehouse: COMPUTE_WH
    database: ANALYTICS
    schema: RAW
    table: users

resources:
  - name: pg_to_sf_incremental
    source: pg_source
    destination: sf_warehouse
    query: "SELECT * FROM users"
    incremental_column: updated_at
    write_mode: append
```

**Run with validation**:
```bash
conduit preflight ingest.yml
conduit run ingest.yml
```

### Example 3: S3 to BigQuery

Cloud data lake to warehouse.

```yaml
sources:
  - name: s3_data
    type: s3
    bucket: my-data-lake
    path: raw/events.csv
    # AWS credentials from env or ~/.aws/credentials

destinations:
  - name: bq_warehouse
    type: bigquery
    project: my-gcp-project
    dataset: analytics
    table: events
    write_mode: full_refresh

resources:
  - name: s3_to_bigquery
    source: s3_data
    destination: bq_warehouse
```

**Run**:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
conduit run ingest.yml
```

### Example 4: Multiple Resources

Run multiple pipelines from one config.

```yaml
sources:
  - name: orders_csv
    type: csv
    path: data/orders.csv
  
  - name: customers_csv
    type: csv
    path: data/customers.csv

destinations:
  - name: warehouse_db
    type: postgresql
    host: ${POSTGRES_HOST}
    database: warehouse
    user: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}

resources:
  - name: load_orders
    source: orders_csv
    destination: warehouse_db
    query: "n/a"
    write_mode: full_refresh
  
  - name: load_customers
    source: customers_csv
    destination: warehouse_db
    query: "n/a"
    write_mode: full_refresh
```

**Run all**:
```bash
conduit run ingest.yml
```

**Run specific**:
```bash
conduit run ingest.yml --resource load_orders
```

### Example 5: Data Quality Checks

Validate data during pipeline execution.

```yaml
sources:
  - name: user_csv
    type: csv
    path: users.csv

destinations:
  - name: clean_json
    type: json
    path: validated_users.json

resources:
  - name: validated_pipeline
    source: user_csv
    destination: clean_json
    quality_checks:
      - column: email
        check: regex
        pattern: "^[^@]+@[^@]+\\.[^@]+$"
        action: fail
      
      - column: age
        check: range
        min: 0
        max: 120
        action: warn
      
      - column: user_id
        check: unique
        action: fail
      
      - column: name
        check: not_null
        action: dlq  # Send failures to dead letter queue
```

**Run**:
```bash
conduit run ingest.yml
```

**Failed records** saved to `./errors/validated_pipeline_*.json`

See [Data Quality Guide](data-quality.md) for more details.

## Advanced Features

### Dry-Run Mode

Preview execution without writes:

```bash
conduit run ingest.yml --dry-run
```

**What happens**:
- ✅ Reads source data
- ✅ Shows progress and record counts
- ✅ Tests connections
- ❌ No writes to destination
- ❌ No state updates
- ❌ No checkpoint changes

### Schema Inference

Automatically detect source schema:

```yaml
sources:
  - name: my_source
    type: csv
    path: data.csv
    infer_schema: true
    schema_sample_size: 100  # Default
```

### Auto-Create Tables

Automatically create destination tables:

```yaml
destinations:
  - name: pg_dest
    type: postgresql
    host: ${POSTGRES_HOST}
    database: warehouse
    table: users
    auto_create_table: true
```

**Supports**: PostgreSQL, Snowflake, BigQuery

### Batch Size Configuration

Control memory usage:

```yaml
resources:
  - name: large_pipeline
    source: big_source
    destination: warehouse
    batch_size: 5000  # Default: 1000
```

## Best Practices

### 1. Always Use Preflight

Catch issues before execution:

```bash
conduit preflight ingest.yml && conduit run ingest.yml
```

### 2. Use Environment Variables

Never commit credentials:

```yaml
# Good
password: ${POSTGRES_PASSWORD}

# Bad
password: "mysecretpassword"
```

### 3. Version Control Schemas

Track schema changes:

```bash
conduit schema users_pipeline --output schemas/users_v1.json
git add schemas/
git commit -m "Add users schema v1"
```

### 4. Monitor with Manifest

Track execution history:

```bash
# Check for failures
conduit manifest --failed

# Pipeline-specific history
conduit manifest --pipeline critical_sync
```

### 5. Use Incremental Loading

Reduce processing time and costs:

```yaml
resources:
  - name: events_sync
    source: db_source
    destination: warehouse
    incremental_column: updated_at
    write_mode: append
```

### 6. Enable Checkpoints for Large Jobs

Resume after failures:

```yaml
sources:
  - name: large_dataset
    type: csv
    path: massive_file.csv
    resume: true
    checkpoint_column: id
```

### 7. Test with Dry-Run

Validate before production:

```bash
# Test locally
conduit run ingest.yml --dry-run

# Deploy with confidence
conduit run ingest.yml
```

## Troubleshooting

### Pipeline Fails During Execution

**Check preflight**:
```bash
conduit preflight ingest.yml
```

**Review manifest**:
```bash
conduit manifest --failed
```

**Check error logs**:
```bash
ls -l errors/
cat errors/pipeline_name_*.json
```

### Connection Errors

**PostgreSQL**:
- Verify credentials in `.env`
- Check host and port
- Test connection: `psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DATABASE`

**Snowflake**:
- Verify account identifier format: `<account>.<region>`
- Ensure warehouse is running
- Check user permissions

**BigQuery**:
- Verify `GOOGLE_APPLICATION_CREDENTIALS` path
- Check service account permissions
- Ensure dataset exists

### Performance Issues

**Optimize batch size**:
```yaml
resources:
  - name: slow_pipeline
    batch_size: 10000  # Increase for faster processing
```

**Enable checkpoints**:
```yaml
sources:
  - name: large_source
    resume: true
    checkpoint_column: id
```

## Next Steps

- **Connectors**: [docs/connectors.md](connectors.md) - Detailed connector configuration
- **Data Quality**: [docs/data-quality.md](data-quality.md) - Quality checks and validation
- **Features**: [docs/features.md](features.md) - All available features
- **Architecture**: [docs/architecture.md](architecture.md) - Design patterns and internals
