# Configuration Reference

This document describes the `ingest.yml` configuration for Conduit Core v1.0.
Each section maps to Pydantic models in `config.py`.

## Sources:
  
### Source Configuration

Defines where data is read from.

```text
sources:
  - name: string                    # Required: unique source name
    type: string                    # Required: csv, json, parquet, s3, postgresql
```

### Common Fields
| Field | Type | Description | Default |
|------|------|-------------|---------|
| `path` | string | Path to local file | — |
| `bucket` | string | S3 bucket name (for S3 sources) | — |
| `connection_string` | string | Full database connection string | — |
| `host` | string | Database host | — |
| `port` | int | Database port | — |
| `database` | string | Database name | — |
| `user` | string | Database user | — |
| `password` | string | Database password (supports `${ENV_VAR}`) | — |
| `db_schema` | string | Database schema | `public` |
| `checkpoint_column` | string | Column for resume tracking | — |
| `resume` | bool | Enable checkpoint/resume | `false` |
| `infer_schema` | bool | Enable schema inference | `false` |
| `schema_sample_size` | int | Records to sample for inference | `100` |


## Destinations:

### Destination Configuration

Defines where data is written to.

```text
destinations:
  - name: string                    # Required: unique destination name
    type: string                    # Required: csv, json, parquet, s3, postgresql, snowflake, bigquery
```
### Common Fields
| Field | Type | Description | Default |
|------|------|-------------|---------|
| `path` | string | Output file path | — |
| `format` | string | For JSON: `array` or `jsonl` | — |
| `indent` | int | JSON indentation | `null` |
| `bucket` | string | S3 bucket name | — |
| `connection_string` | string | Database connection string | — |
| `host` | string | Database host | — |
| `port` | int | Database port | — |
| `database` | string | Database name | — |
| `user` | string | Database username | — |
| `password` | string | Database password (supports `${ENV_VAR}`) | — |
| `db_schema` | string | Schema name | `public` |
| `table` | string | Destination table name | — |
| `mode` | string | `append` or `full_refresh` | `append` |

### Schema & Validation
| Field | Description | Default |
|------|-------------|---------|
| `auto_create_table` | Create table if missing | `false` |
| `validate_schema` | Validate compatibility before write | `false` |
| `strict_validation` | Fail on warnings | `true` |
| `required_columns` | Columns that must be present | — |

## Schema Evolution Configuration

Schema evolution is configured under each destination.

```text
schema_evolution:
  enabled: bool                 # REQUIRED to activate
  mode: string                  # "auto", "manual", "warn"
  on_new_column: string         # "add_nullable", "fail", "ignore"
  on_removed_column: string     # "ignore", "fail"
  on_type_change: string        # "fail", "warn", "auto"
```
  ### Example:
```text
  schema_evolution:
  enabled: true
  mode: auto
  on_new_column: add_nullable
  on_removed_column: ignore
  on_type_change: fail
```

## Resource Configuration

Defines the pipeline logic linking sources and destinations.

```text
resources:
  - name: string                    # Required
    source: string                  # Required (must match a source.name)
    destination: string             # Required (must match a destination.name)
    query: string                   # SQL for DB sources, "n/a" for files
```

### Optional Fields
| Field | Description |
|------|-------------|
| `incremental_column` | Enables incremental loading using column value tracking |
| `export_schema_path` | Path to export inferred schema |
| `mode` | Override destination write mode for this resource |
| `quality_checks` | List of data quality rules |

**Example**
```text
resources:
  - name: customers_to_pg
    source: csv_source
    destination: pg_dest
    query: "n/a"
    incremental_column: updated_at
    export_schema_path: ./schemas/users.json
    quality_checks:
      - column: email
        check: regex
        pattern: "^[^@]+@[^@]+$"
        action: fail
      - column: id
        check: unique
        action: warn
```

## Data Quality Checks

Each check uses the `QualityCheck` model.

| Field | Type | Description |
|------|------|-------------|
| `column` | string | Column to validate |
| `check` | string | `not_null`, `unique`, `regex`, `range`, `enum` |
| `action` | string | `fail`, `warn`, `dlq` |
| `pattern` | string | For `regex` |
| `min_value` | number | For `range` |
| `max_value` | number | For `range` |
| `allowed_values` | list | For `enum` |

**Example**
```text
sources:
  - name: csv_source
    type: csv
    path: ./input/users.csv
    infer_schema: true
    resume: true
    checkpoint_column: id

destinations:
  - name: pg_dest
    type: postgresql
    table: users
    auto_create_table: true
    validate_schema: true
    strict_validation: true
    schema_evolution:
      enabled: true
      mode: auto
      on_new_column: add_nullable
      on_removed_column: ignore
      on_type_change: fail

resources:
  - name: csv_to_pg
    source: csv_source
    destination: pg_dest
    query: "n/a"
    incremental_column: updated_at
    quality_checks:
      - column: email
        check: regex
        pattern: "^[^@]+@[^@]+$"
        action: warn
```

---
This document reflects Conduit Core v1.0. Future versions may expand configuration options (e.g., CDC).
