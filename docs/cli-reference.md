# Conduit Core CLI Reference
The Conduit Core CLI provides declarative control for running, validating, and inspecting data ingestion pipelines.
## Overview

The **Conduit Core CLI** is your command-line interface for running, validating, and managing data pipelines.
It’s designed for **data engineers** and **platform teams** who want declarative control with operational transparency.

Each command supports ```--file``` (path to ```ingest.yml```) and ```--help``` for inline assistance.

## Quick Start
```text
# View available commands
conduit --help

# Validate a pipeline
conduit validate my_resource --file ingest.yml

# Run a pipeline
conduit run my_resource --file ingest.yml
```

## Example Help Output
```text
Usage: conduit [OPTIONS] COMMAND [ARGS]...

Conduit Core CLI

Commands:
  run              Execute a data pipeline resource.
  validate         Pre-flight validation without running the pipeline.
  schema           Infer and export schema from a source.
  schema-compare   Compare current source schema against a baseline.
  manifest         Display the pipeline manifest (recent runs summary).
```

## ```conduit run```
### Description

Executes a specific resource defined in ```ingest.yml```.
Validates connections and schema before loading data.
```text
conduit run my_resource --file ingest.yml
```
### Options
| Option         | Description                         | Default      |
| -------------- | ----------------------------------- | ------------ |
| `--file`, `-f` | Path to `ingest.yml`                | `ingest.yml` |
| `--batch-size` | Records per batch                   | `1000`       |
| `--dry-run`    | Show actions without executing      | `False`      |
| `--no-progress`| Disable progress bars               | `False`      |

### Example Output
```text
 Running pipeline: users_to_pg
 Extracted 1000 records from CSV
 Loaded 1000 records into PostgreSQL
 Manifest updated: manifest.json
 Pipeline completed successfully
```

## Exit Codes
| Code | Meaning                    |      
| ---- | ---------------------------|
| 0    | Success                    |
| 1    | Pipeline failure           |
| 2    | Config or connection error |

## ```conduit validate```
### Description

Performs **pre-flight validation** without executing the pipeline.
Checks configuration, connections, schema, and data quality rules.
```text
conduit validate my_resource --file ingest.yml
```
| Option                 | Description                  | Default      |
| ---------------------- | ---------------------------- | ------------ |
| `--file`, `-f`         | Path to config file          | `ingest.yml` |

### Example Output
```text
 Conduit Pre-Flight Validation

✓ Configuration loaded successfully
✓ Source connection (csv)
✓ Destination connection (postgres)
✓ Inferred schema from 100 records
✓ All required columns present
⚠ 2 records failed quality checks in sample
────────────────────────────────────────────
✓ All validations passed
```

### Exit Codes
| Code | Description            |
| ---- | ---------------------- |
| 0    | All validations passed |
| 1    | Validation failed      |
| 2    | Configuration error    |

## ```conduit schema```
### Description

Infers a source schema from sampled records and exports it to a file.
```text
conduit schema RESOURCE_NAME --file ingest.yml --output schema.json
```
### Options
| Option            | Description                      | Default       |
| ----------------- | -------------------------------- | ------------- |
| `--file`, `-f`    | Path to config file              | `ingest.yml`  |
| `--output`, `-o`  | Output path for schema           | `schema.json` |
| `--sample-size`   | Records to sample                | `100`         |
| `--format`        | Output format (`json` or `yaml`) | `json`        |
| `--verbose`, `-v` | Show detailed schema info        | `False`       |

### Example Output
```text
Sampling 100 records from csv_source...
✓ Schema exported to schema.json
  12 columns, 100 sample records
```
### Example Verbose Output
```text
Schema for users_to_pg
┏━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Column   ┃ Type      ┃ Nullable  ┃ Samples      ┃
┡━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━┩
│ id       │ INTEGER   │ No        │ 100          │
│ email    │ STRING    │ No        │ 100          │
│ age      │ INTEGER   │ Yes       │ 95           │
└──────────┴───────────┴───────────┴──────────────┘
```

## ```conduit schema-compare```
### Description

Compares the current inferred schema against a **baseline** (previous run or JSON file).
```text
conduit schema-compare RESOURCE_NAME --file ingest.yml
```
| Option             | Description            | Default      |
| ------------------ | ---------------------- | ------------ |
| `--file`, `-f`     | Path to config file    | `ingest.yml` |
| `--baseline`, `-b` | Optional baseline JSON | Last run     |
| `--sample-size`    | Records to sample      | `100`        |

### Example Output
```text
 Schema Comparison

 ADDED: signup_date (DATE)
 CHANGED: rate FLOAT → DOUBLE

Summary:
 • 1 column added
 • 1 type change
 Review type change before next run
```
### Use Cases

* Detect upstream schema drift
* Preview schema evolution
* Generate change audit trails

## ```conduit manifest```
### Description

Displays the **pipeline manifest**, a historical log of past runs including timing, record counts, and statuses.
```text
conduit manifest --manifest-path manifest.json
```
| Option            | Description           | Default         |
| ----------------- | --------------------- | --------------- |
| `--manifest-path` | Path to manifest file | `.conduit/manifest.json` |
| `--failed`        | Show only failed runs | `False`         |
| `--pipeline-name` | Filter by pipeline    | `None`          |

The manifest file tracks metadata for every pipeline run — including record counts, duration, and status.

### Example Output
```text
 Pipeline Manifest

┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ Pipeline    ┃ Source→Dest   ┃ Records   ┃ Duration ┃ Status     ┃
┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━┩
│ users_pg    │ csv→postgres  │ 1000      │ 00:01:04 │ ✅ success │
│ products_bq │ api→bigquery  │ 5000      │ 00:02:15 │ ⚠ warning │
└─────────────┴───────────────┴───────────┴──────────┴────────────┘
```

### Manifest Storage

By default, manifest data is written to:
```text
.conduit/manifest.json
```
Each entry includes:
```json
{
  "pipeline_name": "users_pg",
  "source_type": "csv",
  "destination_type": "postgresql",
  "records_read": 1000,
  "records_written": 1000,
  "status": "success",
  "duration_seconds": 64.2
}
```

## Developer Notes
Logs are written both to stdout and the manifest for audit consistency.
### Logging

All commands honor the ```LOG_LEVEL``` environment variable (```DEBUG```, ```INFO```, ```WARNING```, ```ERROR```).
```text
export LOG_LEVEL=DEBUG
conduit validate my_resource
```
| Code | Meaning                   |
| ---- | ------------------------- |
| 0    | Success                   |
| 1    | Failure                   |
| 2    | Config / Connection Error |

## Tips

* Run ```conduit validate``` in CI/CD before deploying new pipelines.
* Use ```schema-compare``` nightly to detect upstream drift.
* Pipe CLI output to JSON for programmatic monitoring.
* Keep ```manifest.json``` under version control for traceability.
* Combine ```schema```, ```validate```, and ```run``` for a full pre-flight → execute cycle.

## See Also
This document reflects the Conduit Core v1.0 CLI. Future versions may expand functionality (e.g., parallel execution, advanced schema diffing).

- [Schema Validation](./schema-validation.md)
- [Schema Evolution](./schema-evolution.md)
- [Data Quality](./data-quality.md)
- [README](../README.md)
