# Conduit Core

**The dbt of data ingestion** - Declarative Data Movement Infrastructure for the Modern Data Stack.

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Poetry](https://img.shields.io/badge/dependency-poetry-blue)](https://python-poetry.org/)
[![Tests Passing](https://img.shields.io/badge/tests-141%20passing-brightgreen)](./tests/)
[![License](https://img.shields.io/badge/license-BSL--1.1-orange)](LICENSE)

An source-available, bulletproof command-line tool for declarative, reliable, and testable data ingestion. Conduit Core is designed to be the industry standard for the "Extract & Load" part of the modern data stack - the perfect companion to **dbt**.

Source ──▶ Conduit Core ──▶ Destination (Retry, DLQ, Checkpoint)

---

**Conduit Core** makes moving data between systems reliable, transparent, and easy to manage. Define your entire pipeline in simple YAML, and let Conduit handle the hard parts: error handling, retries, checkpointing, and providing real-time progress.

##  Why Conduit Core Now?

Data teams waste countless hours building and maintaining brittle ingestion scripts. **dbt** solved this for transformations – **Conduit Core** does the same for ingestion. Reliable, testable data movement should be declarative, not handcrafted. Move data confidently without losing sleep over failures.

**Before Conduit:** Custom Python scripts, complex error handling, manual retries, difficult monitoring. 
**After Conduit:** Simple YAML config, automatic reliability, built-in observability, fast development. 



##  Core Features

* **Declarative Pipelines:** Define everything in simple, version-controllable YAML.
* **Reliability First:** Automatic retries, checkpoint/resume, atomic writes, and Dead-Letter Queue (DLQ) for failed records.
* **7 Production Connectors:** CSV, JSON (Array/NDJSON), Parquet, S3, PostgreSQL, Snowflake, BigQuery. (More coming!)
* **Excellent DevEx:** Real-time progress bars, dry-run mode (`--dry-run`), connection testing (`conduit test`), and helpful error messages.
* **Observability Built-In:** Pipeline Manifest provides a complete audit trail of every run.
* **Memory Efficient:** Streams data in batches without loading entire datasets.

*(See the [full feature list](/docs/features.md) for details)*

---

##  Quick Start

### Installation

Requires Python 3.12+.

```bash
pip install conduit-core

# Or install from source using Poetry (see Contributing)
```

## Your First Pipeline

1.  Create ```ingest.yml```:
```yaml
sources:
  - name: local_users_csv
    type: csv
    path: "./input_users.csv" # Create this file

destinations:
  - name: local_output_json
    type: json
    path: "./output_users.json"
    indent: 2 # Make it readable

resources:
  - name: csv_to_json_transfer
    source: local_users_csv
    destination: local_output_json
    query: "n/a"
```
 2. Create input data ( ```./input_users.csv```):
```yaml
 id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
```

3. Run:
```conduit run --file ingest.yml```

Check ```./output_users.json for the result!```

## Supported Connectors (v1.0)

###  Files:
- CSV
- JSON (Array/NDJSON)
- Parquet
### Cloud Storage;
- AWS S3
### Databases:
- PostgreSQL
### Data Warehouse:
- SnowFlake
- Google BigQuery

## Supported Connectors (v1.0) ([Learn More](/docs/connectors.md))
* **Checkpoint & Resume:** Automatically resume large jobs from failure points. ([Details](/docs/checkpoint.md))
* **Dry-Run Mode:** Preview pipeline runs without writing data. ([Details](/docs/dry-run.md))
* **Connection Validation:** Test connections before running (`conduit test`). ([CLI Docs](/docs/cli.md))
* **Pipeline Manifest:** Audit trail of all runs (`conduit manifest`). ([Details](/docs/manifest.md))
* **Incremental Loading & State:** Process only new data. ([Details](/docs/state.md))
* **Error Handling (DLQ):** Manage failed records gracefully. ([Details](/docs/errors.md))

## Roadmap (High Level)
* Reliability, Core Connectors, Developer Experience (Current focus)
* Data Quality, Schema Evolution, Advanced Sync (CDC), More Connectors
* Integrations (dbt, Airflow), Observability, Enterprise Features (UI, Streaming)

(See [Details](/docs/roadmap.md))

## License

This project is licensed under the **Business Source License 1.1 (BSL-1.1)**.  
It will convert to **Apache 2.0** on January 1, 2030.  
See [LICENSE](./LICENSE) for details.


## Acknowledgments & Inspiration
Built with Python 3.12+ and libraries like Typer, Rich, Pydantic, PyArrow, Boto3. Inspired by **dbt**, **Airbyte**, and **Singer**.



