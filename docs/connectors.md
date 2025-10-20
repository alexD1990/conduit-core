# Conduit Core Connectors (v1.0)

This document provides configuration examples for all supported sources and destinations. Credentials should typically be managed via environment variables (e.g., in a `.env` file) rather than being hardcoded in `ingest.yml`.

## File Connectors

### Testing Connectors

- **DummySource** (`dummysource`): emits small fixed datasets (e.g., `{"id":1}`, `{"id":2}`, `{"id":3}`)
- **DummyDestination** (`dummydestination`): in-memory sink capturing `written_records` for inspection

These are for local testing and CI validation; not intended for production data movement.

### CSV
Supports both reading and writing of standard Comma-Separated Value (CSV) files.

**Source:**
```yaml
sources:
  - name: my_csv_source
    type: csv
    path: data/input_file.csv
    # Optional: Enable resume for large files
    # resume: true
    # checkpoint_column: id # Column must be increasing
```

**Features:** Auto-detects delimiters (,, ;, \t, |) and common encodings (UTF-8, Latin-1, UTF-8-BOM). Handles common NULL values automatically. ```estimate_total_records``` supported for progress bars.

**Destination:**
```yaml
destinations:
  - name: my_csv_destination
    type: csv
    path: output/result_file.csv
```
**Features:** Uses atomic writes (temp file → rename) to ensure data integrity even during process interruptions.

### JSON
Supports both reading and writing JSON data. Handles standard arrays, single objects, and Newline Delimited JSON (NDJSON).

***Source:***
```yaml
sources:
  - name: my_json_source
    type: json
    path: data/records.json # Auto-detects array vs NDJSON
    # format: ndjson # Optional: Explicitly set format
```
**Features:** Reads standard JSON arrays ```[...]```, single root objects ```{...}```, and NDJSON (one valid JSON object per line). Full UTF-8 support and schema inference on sample data.

**Destination:**
```yaml
destinations:
  - name: my_json_destination
    type: json
    path: output/result.json
    indent: 2          # Optional: Pretty-print JSON array (default: 2)
    # format: ndjson   # Optional: Write NDJSON instead of array
```
**Features:** Writes standard JSON arrays (pretty-printed) or NDJSON. Handles complex data types (e.g., datetimes → strings).
Uses atomic writes (temp file → rename) for reliability.

### Parquet
Reads and writes Apache Parquet columnar files using PyArrow.

**Source:**
```yaml
  - name: my_parquet_source
    type: parquet
    path: data/input_data.parquet
    # batch_size: 50000 # Optional: Control read batch size
```

**Destination:**
```yaml
destinations:
  - name: my_parquet_destination
    type: parquet
    path: output/output_data.parquet
    # compression: gzip # Optional: snappy (default), gzip, zstd
```

**Features:** Supports ```snappy``` (default), ```gzip```, and ```zstd``` compression.
Efficient columnar reads using PyArrow.

### S3 (AWS)

Reads and writes files (currently CSV and JSON) from/to Amazon S3 buckets.

**sources:**
```yaml
  - name: s3_csv_source
    type: s3
    bucket: my-landing-zone-bucket
    path: raw_data/users.csv # S3 Key (path within bucket)
```

**Destination:**
```yaml
destinations:
  - name: s3_json_destination
    type: s3
    bucket: my-processed-data-bucket
    path: curated/users.json
```
**Authentication:** Uses the standard AWS credentials chain (in order):
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`)
2. `~/.aws/credentials`
3. EC2/ECS/IAM role credentials if present


## Database & Data Warehouse Connectors
### PostgreSQL

Reads from and writes to PostgreSQL databases.

**Source:**
```yaml
sources:
  - name: postgres_source
    type: postgresql
    database: production_db
    user: readonly_user
    password: ${PG_PASSWORD} # Use env var from .env
    host: prod-db.example.com
    port: 5432
    schema: public
    # Optional: Enable resume
    # resume: true
    # checkpoint_column: updated_at
```

**Destination:**
```yaml
destinations:
  - name: postgres_destination
    type: postgresql
    database: analytics_warehouse
    user: etl_writer
    password: ${PG_WH_PASSWORD}
    host: warehouse-db.example.com
    schema: staging
    table: stg_users
    mode: full_refresh # or append
```

**Authentication:** Reads credentials from config, falls back to environment variables (```POSTGRES_HOST```, ```POSTGRES_USER```, etc.), or uses a ```connection_string```. Features: Supports standard SQL queries in ```resources```. Destination supports ```full_refresh``` (TRUNCATE + INSERT) and ```append``` modes using efficient ```COPY```. Transactional writes. Retry logic included.

### Snowflake
Writes data to Snowflake data warehouse. (Source not yet implemented).

**Destination:**
```yaml
destinations:
  - name: snowflake_destination
    type: snowflake
    account: xy12345.us-east-1 # Your Snowflake account locator + region
    user: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
    warehouse: COMPUTE_WH
    database: ANALYTICS
    schema: RAW_STAGING
    table: raw_orders
    mode: append # or full_refresh
```

**Authentication:** Reads credentials from config or environment variables (```SNOWFLAKE_USER```, etc.). Features: Uses efficient staged loads (```PUT``` local CSV to internal stage + ```COPY INTO``` table). Automatically creates table if it doesn't exist (with VARCHAR columns). Supports ```full_refresh``` and ```append```. Retry logic included

## BigQuery (Google Cloud)
Writes data to Google BigQuery. (Source not yet implemented).

**Destination:**
```yaml
destinations:
  - name: bigquery_destination
    type: bigquery
    project: my-gcp-project-id
    dataset: landing_zone
    table: landing_customers
    mode: full_refresh # or append
    # Optional: Use a specific service account key file
    # credentials_path: /path/to/service-account.json
    # Optional: Specify dataset location if not default
    # location: europe-west1
```

**Authentication:** Uses Application Default Credentials (ADC) by default (run ```gcloud auth application-default login```) or a specific service account key file (```credentials_path```). Features: Uses efficient Load Jobs API (```load_table_from_json```). Auto-detects schema. Supports ```full_refresh``` (```WRITE_TRUNCATE```) and ```append``` (```WRITE_APPEND```).

