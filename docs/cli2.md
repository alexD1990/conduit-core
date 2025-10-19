# Conduit Core CLI — Reference Guide
## Overview

The **Conduit Core CLI** is your command-line interface for running, validating, and managing data pipelines.
It’s designed for **data engineers** and **platform teams** who want declarative control with operational transparency.

Each command supports ```--file``` (path to ```ingest.yml```) and ```--help``` for inline assistance.

## Quick Start
```bash
# View available commands
conduit --help

# Validate a pipeline
conduit validate my_resource --file ingest.yml

# Run a pipeline
conduit run my_resource --file ingest.yml
```

## Example Help Output
```sql
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

Executes a full data pipeline defined in ```ingest.yml``` for a given resource.
```bash
conduit run my_resource --file ingest.yml
```
### Options
| Option         | Description                         | Default      |
| -------------- | ----------------------------------- | ------------ |
| `--file`, `-f` | Path to `ingest.yml`                | `ingest.yml` |
| `--parallel`   | Run multiple resources concurrently | `False`      |
| `--dry-run`    | Show actions without executing      | `False`      |
| `--limit`      | Limit number of records to extract  | `None`       |

### Example Output
```yaml
 Running pipeline: users_to_pg
 Extracted 1000 records from CSV
 Loaded 1000 records into PostgreSQL
 Manifest updated: manifest.json
 Pipeline completed successfully
```

## Exit Codes
| Code | Meaning          |
| ---- | ---------------- |
| 0    | Success          |
| 1    | Pipeline failure |
| 2    | Config error     |

## ```conduit validate```
### Description

Performs **pre-flight validation** without executing the pipeline.
Checks configuration, connections, schema, and data quality rules.
```bash
conduit validate my_resource --file ingest.yml
```
| Option                 | Description                  | Default      |
| ---------------------- | ---------------------------- | ------------ |
| `--file`, `-f`         | Path to config file          | `ingest.yml` |
| `--sample-size`        | Number of records to sample  | `100`        |
| `--strict/--no-strict` | Treat warnings as errors     | `--strict`   |
| `resource_name`        | Name of resource to validate | *(required)* |

### Example Output
```sql
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
```bash
conduit schema my_resource --file ingest.yml --output schema.json
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
```css
Sampling 100 records from csv_source...
✓ Schema exported to schema.json
  12 columns, 100 sample records
```
### Example Verbose Output
```sql
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
```bash
conduit schema-compare my_resource --file ingest.yml
```
| Option             | Description            | Default      |
| ------------------ | ---------------------- | ------------ |
| `--file`, `-f`     | Path to config file    | `ingest.yml` |
| `--baseline`, `-b` | Optional baseline JSON | Last run     |
| `--sample-size`    | Records to sample      | `100`        |

### Example Output
```sql
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
```bash
conduit manifest --manifest-path manifest.json
```
| Option            | Description           | Default         |
| ----------------- | --------------------- | --------------- |
| `--manifest-path` | Path to manifest file | `manifest.json` |
| `--failed`        | Show only failed runs | `False`         |
| `--pipeline-name` | Filter by pipeline    | `None`          |

### Example Output
```yaml
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
```bash
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
### Logging

All commands honor the ```LOG_LEVEL``` environment variable (```DEBUG```, ```INFO```, ```WARNING```, ```ERROR```).
```bash
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

* Schema Validation
* Schema Evolution
* Data Quality
* README.md
