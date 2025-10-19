# Conduit Core

**The dbt of data ingestion** - Declarative Data Movement Infrastructure for the Modern Data Stack.

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Poetry](https://img.shields.io/badge/dependency-poetry-blue)](https://python-poetry.org/)
[![Tests Passing](https://img.shields.io/badge/tests-141%20passing-brightgreen)](./tests/)
[![License](https://img.shields.io/badge/license-BSL--1.1-orange)](LICENSE)

An **source-available**, **bulletproof CLI** for declarative, reliable, and testable data ingestion. Conduit Core is designed to be the **industry standard for the "Extract & Load** part of the modern data stack - the perfect companion to **dbt**.

Source ──▶ Conduit Core ──▶ Destination (Retry, DLQ, Checkpoint)

##  Why Conduit Core Now?

Data engineers shouldn’t waste time maintaining brittle ingestion scripts.
**dbt** made transformation declarative — **Conduit Core** brings that reliability and structure to ingestion.

* **Before:** Custom Python, inconsistent error handling, manual retries, no observability.
* **After:** Simple YAML configs, checkpoint/resume, schema validation, and complete auditability.

Move data confidently. Focus on modeling — not on plumbing.

## Core features

### Reliability

* Automatic retries and resumable jobs
* Dead-letter queue (DLQ) for failed records
* Atomic writes and streaming batch execution

### Schema Intelligence

* **Schema Validation:** Detect breaking changes before execution
* **Schema Evolution:** Plan and apply compatible schema updates
* **Schema Compare CLI:** Detect added, removed, or changed columns

### Data Quality

* Built-in validators for nulls, duplicates, regex, and ranges
* Define rules per resource directly in YAML
* Strict or warning-only modes
* See docs/data-quality.md

### Declarative YAML Pipelines

Define the “what,” not the “how.” Conduit builds and executes robust ingestion pipelines automatically.

### Developer Experience

* Pre-flight validation via ```conduit validate```
* Schema discovery with ```conduit schema```
* Schema drift detection with ```conduit schema-compare```
* Real-time progress bars and connection testing

### Observability

* Built-in manifest logging every pipeline run
* Queryable via ```conduit manifest```
* Perfect for CI/CD auditing and lineage tracking

##  Quick Start

### 1. Install

Requires Python 3.12+.

```bash
pip install conduit-core
```
Or install from source using Poetry 
```bash
poetry install
```

### 2. Define Your Pipeline (```ingest.yml```)

```yaml
sources:
  - name: local_users_csv
    type: csv
    path: "./input_users.csv"

destinations:
  - name: local_output_json
    type: json
    path: "./output_users.json"
    indent: 2

resources:
  - name: csv_to_json_transfer
    source: local_users_csv
    destination: local_output_json
    query: "n/a"
```

### 3. Run it:
```bash
conduit run --file ingest.yml
```

Output written to ```./output_users.json```

## Validation, Schema, and Quality
### Validate a Resource
```bash
conduit validate csv_to_json_transfer --file ingest.yml
```
**Checks:**

* Config syntax
* Source & destination connectivity
* Schema compatibility
* Data quality rules
* Required columns

**Example Output:**
```sql
 Conduit Pre-Flight Validation

✓ Configuration loaded successfully
✓ Source connection (csv)
✓ Destination connection (json)
✓ Inferred schema from 100 records
✓ All required columns present
✓ Quality checks passed
────────────────────────────
✓ All validations passed
```
### Compare Schemas
```bash
conduit schema-compare csv_to_json_transfer --file ingest.yml
```

**Output:**
```sql
 Schema Comparison

 ADDED: signup_date (DATE)
 CHANGED: amount FLOAT → DOUBLE
────────────────────────────
⚠ Review changes before next deployment
```

**Example: Data Quality Checks**
```yaml
resources:
  - name: customers_to_pg
    source: csv_source
    destination: pg_dest
    quality_checks:
      - column: email
        rule: regex
        pattern: "^[^@]+@[^@]+$"
      - column: id
        rule: unique
```

**Run:**
```bash
conduit validate customers_to_pg
```

**Output:**
```yaml
⚠ 2 records failed quality checks in sample
✗ Validation failed
```
See docs/data-quality.md
 for configuration details.

## CLI Commands Overview
| Command                  | Description                                |
| ------------------------ | ------------------------------------------ |
| `conduit run`            | Execute pipeline                           |
| `conduit validate`       | Validate config, schema, and quality rules |
| `conduit schema`         | Infer and export source schema             |
| `conduit schema-compare` | Compare current schema vs baseline         |
| `conduit manifest`       | View run history and performance metrics   |

Full CLI reference: docs/cli.md

## Supported Connectors 

| Category          | Connectors                        |
| ----------------- | --------------------------------- |
| **Files**         | CSV, JSON (Array/NDJSON), Parquet |
| **Cloud Storage** | AWS S3                            |
| **Databases**     | PostgreSQL                        |
| **Warehouses**    | Snowflake, BigQuery               |

(See docs/connectors.md
 for setup details.)

## Advanced Features
| Feature                 | Description                      |
| ----------------------- | -------------------------------- |
| **Checkpoint & Resume** | Resume jobs from last checkpoint |
| **Dry-Run Mode**        | Preview transformations safely   |
| **Manifest**            | Full run audit trail             |
| **Incremental Loads**   | Process only new data            |
| **Error Handling**      | Automatic retries + DLQ storage  |

## Roadmap
### v1.0 (Current)

* CLI Validation System (```conduit validate```)
* Schema Evolution & Drift Detection
* Data Quality Framework
* Full Documentation Set
* 173 Passing Tests, 0 Warnings

 ### v1.2

* Incremental Sync & Change Data Capture (CDC)
* Redshift & Azure Connectors
* Cloud Telemetry & Conduit Cloud
* Web UI for Orchestration

See ```docs/roadmap.md```
 for progress.

## License

This project is licensed under the **Business Source License 1.1 (BSL-1.1)**.
It automatically converts to **Apache 2.0** on **January 1, 2030**.
See LICENSE
 for details.