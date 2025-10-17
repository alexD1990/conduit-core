# Conduit Core Features (v1.2.0)

This document details the core features available in Conduit Core.

## Data Connectors (7)

Conduit Core supports reading from and writing to a variety of sources and destinations.

### Sources (Read From)
* **CSV:** Local CSV files with auto-delimiter detection (`,`, `;`, `\t`, `|`) and encoding detection (UTF-8, Latin-1, UTF-8-BOM). Handles common NULL representations.
* **JSON:** Local JSON files supporting standard arrays `[...]`, single objects `{...}`, and Newline Delimited JSON (NDJSON). Full UTF-8 support.
* **Parquet:** Local Parquet files using PyArrow. Supports `snappy` (default), `gzip`, and `zstd` compression.
* **S3:** Files (CSV, JSON currently) stored in Amazon S3 buckets. Uses standard AWS credential chain (env vars, `~/.aws/credentials`, IAM roles).
* **PostgreSQL:** Read data from PostgreSQL databases using standard SQL queries.

### Destinations (Write To)
* **CSV:** Writes to local CSV files using atomic operations (temp file → rename) to prevent corrupted output.
* **JSON:** Writes standard JSON arrays (with configurable indentation) or NDJSON format. Full UTF-8 support. Atomic writes.
* **Parquet:** Writes to local Parquet files using PyArrow. Supports `snappy` (default), `gzip`, `zstd` compression. Atomic writes.
* **S3:** Uploads files (CSV, JSON currently) to Amazon S3 buckets.
* **PostgreSQL:** Writes data to PostgreSQL tables using transactional `COPY` for efficiency. Supports `full_refresh` (TRUNCATE + INSERT) and `append` modes.
* **Snowflake:** Loads data efficiently into Snowflake using staged loads (`PUT` + `COPY INTO`). Supports `full_refresh` and `append` modes.
* **BigQuery:** Loads data into Google BigQuery using the Load Jobs API for scalability. Supports `full_refresh` (`WRITE_TRUNCATE`) and `append` (`WRITE_APPEND`) modes. Auto-detects schema.

## Core Reliability Features

Designed to handle failures gracefully and ensure data integrity.

* **Atomic Operations:** File writes use a temp file → rename pattern, ensuring the final file is always complete. Database writes use transactions.
* **Retry Logic:** Automatically retries operations (like database connections or S3 uploads) with exponential backoff on transient network errors. (Default: 3 attempts).
* **Dead Letter Queue (DLQ):** Records that fail during processing (e.g., due to data type issues, validation errors) are automatically saved to `./errors/` as JSON files, including the original record and the error message. The pipeline continues processing valid records.
* **Checkpoint/Resume:** For long-running jobs, Conduit Core saves progress periodically. If a run fails, the next run can automatically resume from the last successful checkpoint, avoiding reprocessing large amounts of data.
* **Connection Validation:** The `conduit test` command verifies connections to all configured sources and destinations before a pipeline starts, providing immediate feedback and helpful suggestions for common errors.

## Pipeline Management & Execution

* **Declarative YAML:** Define all sources, destinations, and data flows (`resources`) in a simple `ingest.yml` file.
* **Two Write Modes:**
    * `append`: Adds new records to the destination (default).
    * `full_refresh`: Deletes all existing data in the destination (e.g., `TRUNCATE` table, overwrite file) before inserting new data.
* **Incremental Loading:** Process only new records from sources using the `incremental_column` setting in `resources` and the `:last_value` placeholder in your source `query`. State is managed automatically in `.conduit_state.json`.
* **Batch Processing:** Data is read, processed, and written in batches (default: 1000 records, configurable via `--batch-size`) for memory efficiency.
* **Streaming Architecture:** Data flows through the pipeline without loading entire datasets into memory.

## Developer & User Experience

* **Rich Progress Bars:** Real-time feedback during `conduit run` shows records processed, throughput (rows/sec), and estimated time remaining. Automatically falls back to text logging in non-interactive environments (like CI/CD).
* **Dry-Run Mode:** Preview a pipeline run using `conduit run --dry-run`. It reads data, shows progress, but performs *no* writes, state updates, or checkpoint changes.
* **Pipeline Manifest:** A detailed JSON audit trail (`manifest.json`) logs every execution, including timestamps, status, record counts, duration, errors, and metadata. View history with `conduit manifest`.
* **Helpful Error Messages:** Connection errors provide specific suggestions for common problems (credentials, permissions, typos, firewall rules).
* **Pydantic Validation:** `ingest.yml` is validated on load, catching configuration errors early with clear messages.
* **Environment Variable Support:** Securely manage credentials (like database passwords, API keys) using a `.env` file or system environment variables.