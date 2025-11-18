# Connectors Reference

Complete configuration guide for all 8 Conduit Core connectors. Always use environment variables for credentials.

## Overview

| Connector   | Type        | Source | Destination | Notes                          |
|-------------|-------------|--------|-------------|--------------------------------|
| CSV         | File        | ✅     | ✅          | Auto-detect delimiters         |
| JSON        | File        | ✅     | ✅          | Supports NDJSON                |
| Parquet     | File        | ✅     | ✅          | Columnar format, compressed    |
| S3          | Cloud       | ✅     | ✅          | AWS S3 buckets                 |
| PostgreSQL  | Database    | ✅     | ✅          | Full read/write support        |
| MySQL       | Database    | ✅     | ✅          | Full read/write support        |
| Snowflake   | Warehouse   | ❌     | ✅          | Destination only               |
| BigQuery    | Warehouse   | ❌     | ✅          | Destination only               |

**Testing Connectors**: `DummySource` and `DummyDestination` available for local testing.

---

## File Connectors

### CSV

Reads and writes standard CSV files with automatic delimiter and encoding detection.

**Source Configuration:**

```yaml
sources:
  - name: users_csv
    type: csv
    path: data/users.csv
    # Optional: Enable checkpoint/resume for large files
    resume: false
    checkpoint_column: id  # Must be monotonically increasing
```

**Features**:
- Auto-detects delimiters: `,` `;` `\t` `|`
- Auto-detects encoding: UTF-8, Latin-1, UTF-8-BOM
- Handles NULL values: `NULL`, `null`, `None`, empty strings
- Progress bar support with `estimate_total_records`

**Destination Configuration:**

```yaml
destinations:
  - name: users_output
    type: csv
    path: output/users.csv
```

**Features**:
- Atomic writes (temp file → rename pattern)
- UTF-8 encoding by default
- Preserves column order

**Example:**

```yaml
resources:
  - name: csv_pipeline
    source: users_csv
    destination: users_output
```

---

### JSON

Reads and writes JSON data in multiple formats.

**Source Configuration:**

```yaml
sources:
  - name: events_json
    type: json
    path: data/events.json
    # Optional: Explicitly set format
    format: array  # Options: array (default), ndjson
```

**Supported Formats**:
- Standard array: `[{...}, {...}]`
- Single object: `{...}`
- NDJSON: One JSON object per line

**Destination Configuration:**

```yaml
destinations:
  - name: events_output
    type: json
    path: output/events.json
    indent: 2  # Pretty-print (default: 2, use 0 for compact)
    format: array  # Options: array (default), ndjson
```

**Features**:
- Full UTF-8 support
- Atomic writes
- Automatic type conversion (datetime → ISO strings)

**Example:**

```yaml
resources:
  - name: json_pipeline
    source: events_json
    destination: events_output
```

---

### Parquet

Reads and writes Apache Parquet columnar files using PyArrow.

**Source Configuration:**

```yaml
sources:
  - name: data_parquet
    type: parquet
    path: data/analytics.parquet
    batch_size: 50000  # Optional: Control read batch size
```

**Destination Configuration:**

```yaml
destinations:
  - name: data_output
    type: parquet
    path: output/analytics.parquet
    compression: snappy  # Options: snappy (default), gzip, zstd
```

**Features**:
- Efficient columnar storage
- Compression: `snappy`, `gzip`, `zstd`
- Atomic writes
- Type-preserving (no lossy conversions)

**Example:**

```yaml
resources:
  - name: parquet_pipeline
    source: data_parquet
    destination: data_output
```

---

### S3 (AWS)

Reads and writes files (CSV, JSON) from/to Amazon S3 buckets.

**Source Configuration:**

```yaml
sources:
  - name: s3_landing
    type: s3
    bucket: my-data-lake
    path: raw/users.csv  # S3 key (object path)
```

**Destination Configuration:**

```yaml
destinations:
  - name: s3_processed
    type: s3
    bucket: my-data-lake
    path: processed/users.json
```

**Authentication** (in order of precedence):

1. **Environment variables**:
   ```bash
   AWS_ACCESS_KEY_ID=your_key
   AWS_SECRET_ACCESS_KEY=your_secret
   AWS_SESSION_TOKEN=your_token  # Optional, for temporary credentials
   ```

2. **AWS credentials file** (`~/.aws/credentials`):
   ```ini
   [default]
   aws_access_key_id = YOUR_KEY
   aws_secret_access_key = YOUR_SECRET
   region = us-east-1
   ```

3. **IAM roles** (when running on EC2/ECS/Lambda)

**Features**:
- Automatic retry with exponential backoff
- Streaming uploads (no local temp files for large objects)
- Supports CSV and JSON file formats

**Example:**

```yaml
resources:
  - name: s3_pipeline
    source: s3_landing
    destination: s3_processed
```

---

## Database Connectors

### PostgreSQL

Full read/write support for PostgreSQL databases.

**Source Configuration:**

```yaml
sources:
  - name: prod_db
    type: postgresql
    host: ${POSTGRES_HOST}
    port: 5432
    database: production
    user: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}
    schema: public  # Optional, defaults to 'public'
    # Optional: Enable checkpoint/resume
    resume: false
    checkpoint_column: updated_at
```

**Connection String Alternative:**

```yaml
sources:
  - name: prod_db
    type: postgresql
    connection_string: "host=localhost dbname=mydb user=postgres password=secret"
```

**Destination Configuration:**

```yaml
destinations:
  - name: warehouse_db
    type: postgresql
    host: ${POSTGRES_HOST}
    port: 5432
    database: warehouse
    user: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}
    schema: staging
    table: stg_users
    write_mode: append  # Options: append (default), full_refresh
    auto_create_table: false  # Optional: Create table if missing
```

**Environment Variables:**

```bash
# .env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=mydb
POSTGRES_USER=postgres
POSTGRES_PASSWORD=mysecretpassword
```

**Features**:
- Transactional writes with `COPY` for efficiency
- `full_refresh`: `TRUNCATE` + `INSERT`
- `append`: Incremental inserts
- Automatic retry logic
- Connection pooling

**Example - Read with Query:**

```yaml
resources:
  - name: pg_to_warehouse
    source: prod_db
    destination: warehouse_db
    query: |
      SELECT 
        id, 
        email, 
        created_at 
      FROM users 
      WHERE status = 'active'
    write_mode: full_refresh
```

**Example - Incremental Sync:**

```yaml
resources:
  - name: incremental_users
    source: prod_db
    destination: warehouse_db
    query: "SELECT * FROM users"
    incremental_column: updated_at
    write_mode: append
```

---

### MySQL

Full read/write support for MySQL databases.

**Source Configuration:**

```yaml
sources:
  - name: mysql_prod
    type: mysql
    host: ${MYSQL_HOST}
    port: 3306
    database: production
    user: ${MYSQL_USER}
    password: ${MYSQL_PASSWORD}
```

**Destination Configuration:**

```yaml
destinations:
  - name: mysql_warehouse
    type: mysql
    host: ${MYSQL_HOST}
    port: 3306
    database: warehouse
    user: ${MYSQL_USER}
    password: ${MYSQL_PASSWORD}
    table: stg_orders
    write_mode: append  # Options: append (default), full_refresh
```

**Environment Variables:**

```bash
# .env
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=mydb
MYSQL_USER=root
MYSQL_PASSWORD=mysecretpassword
```

**Features**:
- Transactional writes
- Batch inserts for efficiency
- `full_refresh`: `TRUNCATE` + `INSERT`
- `append`: Incremental inserts
- Automatic retry logic

**Example:**

```yaml
resources:
  - name: mysql_sync
    source: mysql_prod
    destination: mysql_warehouse
    query: "SELECT * FROM orders WHERE created_at > '2024-01-01'"
    write_mode: full_refresh
```

**Example - Incremental:**

```yaml
resources:
  - name: incremental_orders
    source: mysql_prod
    destination: mysql_warehouse
    query: "SELECT * FROM orders"
    incremental_column: updated_at
    write_mode: append
```

---

## Data Warehouse Connectors

### Snowflake

Destination-only connector for Snowflake data warehouse.

**Destination Configuration:**

```yaml
destinations:
  - name: snowflake_warehouse
    type: snowflake
    account: ${SNOWFLAKE_ACCOUNT}  # Format: <locator>.<region>
    user: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
    warehouse: COMPUTE_WH
    database: ANALYTICS
    schema: RAW_DATA  # Optional, defaults to 'PUBLIC'
    table: users
    write_mode: append  # Options: append (default), full_refresh
```

**Account Identifier Format:**

```
<account_locator>.<region>
```

**Examples**:
- `xy12345.us-east-1`
- `ab98765.eu-west-1`
- `org123-account456.us-central1.gcp` (for Snowflake on GCP)

**Environment Variables:**

```bash
# .env
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_USER=etl_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ANALYTICS
SNOWFLAKE_SCHEMA=PUBLIC
```

**Features**:
- Staged loads: `PUT` → internal stage → `COPY INTO`
- Auto-creates tables with VARCHAR columns
- `full_refresh`: `TRUNCATE` + `COPY INTO`
- `append`: Incremental `COPY INTO`
- Efficient for large datasets

**Prerequisites**:
1. Warehouse must be running
2. User needs permissions:
   - `USAGE` on warehouse and database
   - `CREATE TABLE` on schema
   - `INSERT` on target table

**Example:**

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
  - name: pg_to_snowflake
    source: pg_source
    destination: sf_warehouse
    query: "SELECT * FROM users"
    write_mode: full_refresh
```

---

### BigQuery

Destination-only connector for Google BigQuery.

**Destination Configuration:**

```yaml
destinations:
  - name: bigquery_warehouse
    type: bigquery
    project: ${BIGQUERY_PROJECT}
    dataset: analytics
    table: users
    write_mode: append  # Options: append (default), full_refresh
    location: US  # Optional: dataset location (default: US)
```

**Authentication:**

Uses Google Cloud Application Default Credentials (ADC):

1. **Service Account JSON** (recommended):
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
   ```

2. **gcloud CLI**:
   ```bash
   gcloud auth application-default login
   ```

**Service Account Setup:**

1. Create service account in Google Cloud Console
2. Grant roles:
   - `BigQuery Data Editor`
   - `BigQuery Job User`
3. Download JSON key
4. Set environment variable

**Environment Variables:**

```bash
# .env
BIGQUERY_PROJECT=my-gcp-project
BIGQUERY_DATASET=analytics
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

**Features**:
- Load Jobs API for scalability
- Auto-detects schema from first batch
- `full_refresh`: `WRITE_TRUNCATE`
- `append`: `WRITE_APPEND`
- Handles nested/repeated fields
- Automatic type conversion

**Required Permissions**:
- `bigquery.tables.create`
- `bigquery.tables.updateData`
- `bigquery.jobs.create`

**Example:**

```yaml
sources:
  - name: s3_data
    type: s3
    bucket: my-data-lake
    path: raw/events.csv

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

**Example - Incremental:**

```yaml
sources:
  - name: pg_events
    type: postgresql
    host: ${POSTGRES_HOST}
    database: production
    user: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}

destinations:
  - name: bq_warehouse
    type: bigquery
    project: my-gcp-project
    dataset: analytics
    table: events

resources:
  - name: incremental_events
    source: pg_events
    destination: bq_warehouse
    query: "SELECT * FROM events"
    incremental_column: event_timestamp
    write_mode: append
```

---

## Testing Connectors

For local testing and CI/CD validation.

### DummySource

Generates fixed small datasets.

```yaml
sources:
  - name: test_source
    type: dummysource
```

**Output**: `[{"id": 1}, {"id": 2}, {"id": 3}]`

### DummyDestination

In-memory sink for inspecting written records.

```yaml
destinations:
  - name: test_dest
    type: dummydestination
```

**Usage**: Access `written_records` attribute in tests.

---

## Common Patterns

### Pattern 1: File Format Conversion

```yaml
sources:
  - name: csv_input
    type: csv
    path: data.csv

destinations:
  - name: parquet_output
    type: parquet
    path: data.parquet
    compression: snappy

resources:
  - name: csv_to_parquet
    source: csv_input
    destination: parquet_output
```

### Pattern 2: Database Replication

```yaml
sources:
  - name: source_db
    type: postgresql
    host: ${SOURCE_HOST}
    database: prod
    user: ${SOURCE_USER}
    password: ${SOURCE_PASSWORD}

destinations:
  - name: replica_db
    type: postgresql
    host: ${REPLICA_HOST}
    database: replica
    user: ${REPLICA_USER}
    password: ${REPLICA_PASSWORD}
    table: users
    write_mode: full_refresh

resources:
  - name: db_replication
    source: source_db
    destination: replica_db
    query: "SELECT * FROM users"
```

### Pattern 3: Cloud Data Lake to Warehouse

```yaml
sources:
  - name: s3_raw
    type: s3
    bucket: data-lake
    path: raw/events.json

destinations:
  - name: snowflake_warehouse
    type: snowflake
    account: ${SNOWFLAKE_ACCOUNT}
    user: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
    warehouse: COMPUTE_WH
    database: ANALYTICS
    schema: RAW
    table: events
    write_mode: append

resources:
  - name: lake_to_warehouse
    source: s3_raw
    destination: snowflake_warehouse
```

### Pattern 4: Multi-Database Sync

```yaml
sources:
  - name: mysql_orders
    type: mysql
    host: ${MYSQL_HOST}
    database: ecommerce
    user: ${MYSQL_USER}
    password: ${MYSQL_PASSWORD}

  - name: pg_customers
    type: postgresql
    host: ${POSTGRES_HOST}
    database: crm
    user: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}

destinations:
  - name: warehouse
    type: bigquery
    project: analytics-project
    dataset: warehouse

resources:
  - name: sync_orders
    source: mysql_orders
    destination: warehouse
    query: "SELECT * FROM orders"
    
  - name: sync_customers
    source: pg_customers
    destination: warehouse
    query: "SELECT * FROM customers"
```

---

## Troubleshooting

### Connection Errors

**PostgreSQL/MySQL**:
```bash
# Test connection manually
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DATABASE
mysql -h $MYSQL_HOST -u $MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DATABASE

# Use preflight
conduit preflight ingest.yml
```

**Snowflake**:
- Verify account format: `<locator>.<region>`
- Ensure warehouse is running
- Check user permissions

**BigQuery**:
- Verify `GOOGLE_APPLICATION_CREDENTIALS` path
- Check service account permissions
- Ensure dataset exists

### Performance Optimization

**Large Files**:
```yaml
sources:
  - name: large_csv
    type: csv
    path: huge_file.csv
    resume: true
    checkpoint_column: id

resources:
  - name: large_pipeline
    batch_size: 10000  # Increase batch size
```

**Database Queries**:
```yaml
resources:
  - name: optimized_query
    source: db_source
    destination: warehouse
    query: |
      SELECT * FROM events 
      WHERE created_at > '2024-01-01'
      AND status = 'completed'
      LIMIT 1000000
```

---

## Next Steps

- **Installation**: [docs/installation.md](installation.md) - Setup and credentials
- **Usage Guide**: [docs/usage.md](usage.md) - CLI commands and examples
- **Data Quality**: [docs/data-quality.md](data-quality.md) - Validation rules
