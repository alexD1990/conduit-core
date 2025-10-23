# Conduit Core CLI Reference

The Conduit Core CLI provides declarative control for running, validating, and inspecting data ingestion pipelines.

## Overview

The **Conduit Core CLI** is your command-line interface for running, validating, and managing data pipelines.
It's designed for **data engineers** and **platform teams** who want declarative control with operational transparency.

Each command supports inline `--help` for assistance.

## Quick Start
```bash
# View available commands
conduit --help

# Run preflight health checks
conduit preflight ingest.yml

# Run a pipeline (with automatic preflight)
conduit run ingest.yml --resource my_resource

# View execution history
conduit manifest
```

## Available Commands
```text
Usage: conduit [OPTIONS] COMMAND [ARGS]...

Conduit Core CLI

Commands:
  run        Execute a data pipeline resource (with auto-preflight)
  preflight  Run health checks without executing pipeline
  schema     Infer and export schema from a source
  manifest   Display pipeline execution history
```

---

## `conduit run`

### Description

Executes resources defined in `ingest.yml`. **Automatically runs preflight checks** before execution to catch configuration issues, connection failures, and schema problems early.

### Usage
```bash
# Run all resources
conduit run ingest.yml

# Run specific resource
conduit run ingest.yml --resource my_resource

# Skip preflight checks (not recommended)
conduit run ingest.yml --skip-preflight

# Dry-run mode (simulate without writing)
conduit run ingest.yml --dry-run
```

### Options

| Option            | Description                              | Default      |
|-------------------|------------------------------------------|--------------|
| `config_file`     | Path to `ingest.yml` (positional arg)    | `ingest.yml` |
| `--resource`, `-r`| Specific resource to run                 | All resources|
| `--dry-run`, `-d` | Simulate without executing writes        | `False`      |
| `--skip-preflight`| Skip automatic preflight checks          | `False`      |

### Examples
```bash
# Production run with all safety checks
conduit run production.yml

# Test specific resource
conduit run ingest.yml --resource load_users --dry-run

# Emergency run (skip preflight - use with caution)
conduit run ingest.yml --skip-preflight
```

### What Happens During Run

1. **Preflight Checks** (unless `--skip-preflight`):
   - Config syntax validation
   - Source connection test
   - Destination connection test
   - Schema inference (if enabled)
   - Compatibility checks

2. **Execution**:
   - Batch processing with progress bars
   - Atomic writes (transactions for DBs, temp files for files)
   - Retry logic with exponential backoff
   - Dead Letter Queue (DLQ) for failed records
   - Checkpoint tracking for resume capability

3. **Audit Trail**:
   - Unique run ID generated
   - Manifest logged with timing, record counts, errors
   - Preflight results included in manifest

---

## `conduit preflight`

### Description

**Dry-run health check** that validates your pipeline configuration without moving any data. Perfect for CI/CD pipelines, production readiness checks, and troubleshooting.

### Usage
```bash
# Check all resources
conduit preflight ingest.yml

# Check specific resource
conduit preflight ingest.yml --resource load_users
```

### Options

| Option            | Description                           | Default      |
|-------------------|---------------------------------------|--------------|
| `config_file`     | Path to `ingest.yml` (positional arg) | `ingest.yml` |
| `--resource`, `-r`| Specific resource to check            | All resources|

### What It Checks

✅ **Config Syntax** - Valid YAML and schema  
✅ **Source Connection** - Can connect to data source  
✅ **Destination Connection** - Can connect to destination  
✅ **Schema Inference** - Can read and parse source data  
✅ **Destination Compatibility** - Destination ready for writes  
✅ **Quality Checks** - Validation rules properly configured  

### Example Output
```bash
$ conduit preflight ingest.yml

🔍 Running preflight checks...

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Check                      ┃ Status ┃ Message                       ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ Config Syntax              │   ✓    │ Valid configuration with 3... │
│ [load_users] Source Conn   │   ✓    │ Connected to postgres source  │
│ [load_users] Dest Conn     │   ✓    │ Connected to snowflake dest   │
│ [load_users] Schema        │   ✓    │ Inferred schema with 12 cols  │
│ [load_users] Compatibility │   ✓    │ Auto-create table enabled     │
└────────────────────────────┴────────┴───────────────────────────────┘

⏱ Preflight completed in 2.34s

✓ All checks passed - safe to run
```

### Exit Codes

- `0` - All checks passed
- `1` - One or more checks failed

### Use Cases

**CI/CD Pipeline:**
```bash
# Fail deployment if config is invalid
conduit preflight production.yml || exit 1
```

**Production Readiness:**
```bash
# Verify connections before scheduled run
0 8 * * * conduit preflight /opt/pipelines/daily.yml && conduit run /opt/pipelines/daily.yml
```

**Troubleshooting:**
```bash
# Diagnose connection issues
conduit preflight ingest.yml --resource failing_pipeline
```

---

## `conduit schema`

### Description

Infer schema from a source and export to a file for version control, documentation, or comparison.

### Usage
```bash
# Infer and export schema
conduit schema my_resource --output schema.json

# Export as YAML
conduit schema my_resource --output schema.yaml --format yaml

# Preview without saving
conduit schema my_resource
```

### Options

| Option            | Description                    | Default       |
|-------------------|--------------------------------|---------------|
| `resource_name`   | Resource to infer schema from  | Required      |
| `--file`, `-f`    | Path to `ingest.yml`           | `ingest.yml`  |
| `--output`, `-o`  | Output file path               | `schema.json` |
| `--format`        | Output format (json or yaml)   | `json`        |
| `--sample-size`   | Number of records to sample    | `100`         |
| `--verbose`, `-v` | Show detailed information      | `False`       |

### Example Output
```json
{
  "columns": [
    {
      "name": "id",
      "type": "integer",
      "nullable": false
    },
    {
      "name": "email",
      "type": "string",
      "nullable": false
    }
  ]
}
```

---

## `conduit manifest`

### Description

Display pipeline execution history with run IDs, timing, record counts, and errors.

### Usage
```bash
# View all runs
conduit manifest

# View runs for specific pipeline
conduit manifest --pipeline load_users

# Show only failed runs
conduit manifest --failed
```

### Options

| Option                | Description                  | Default         |
|-----------------------|------------------------------|-----------------|
| `--pipeline`, `-p`    | Filter by pipeline name      | All pipelines   |
| `--failed`            | Show only failed runs        | `False`         |
| `--manifest-path`, `-m`| Path to manifest file       | `manifest.json` |

### Example Output
```text
📜 Pipeline Manifest

run_20251023_143052_a7f3e2 | load_users    | success | 1,000 records | 5.2s
run_20251023_120034_b8e4f1 | load_orders   | failed  | 0 records     | 0.8s
  Error: Connection timeout to warehouse
```

---

## Deprecated Commands

The following commands have been **removed** in favor of `conduit preflight`:

- ~~`conduit validate`~~ → Use `conduit preflight`
- ~~`conduit schema-compare`~~ → Use `conduit preflight` (auto-detects drift)

---

## Global Options

All commands support:

- `--help` - Show command-specific help
- `--version`, `-v` - Show Conduit Core version

---

## Best Practices

1. **Always use preflight in production**:
```bash
   conduit preflight prod.yml && conduit run prod.yml
```

2. **Version control your schemas**:
```bash
   conduit schema my_resource --output schemas/my_resource_v1.json
   git add schemas/
```

3. **Monitor with manifest**:
```bash
   conduit manifest --failed | alert-on-failures
```

4. **Use dry-run for testing**:
```bash
   conduit run new_pipeline.yml --dry-run
```