# Schema Evolution — Conduit Core
## Overview

**Schema Evolution** in Conduit Core ensures your destination tables stay synchronized with the evolving shape of your source data.

When a new column appears, a type changes, or a nullable flag shifts — Conduit can detect, log, and optionally apply schema changes automatically.

It’s designed for **safe**, **versioned evolution** rather than destructive schema drift, providing observability and control.

## Modes of Operation

You can configure schema evolution at the **destination level** in your ```ingest.yml```.
```yml
destinations:
  - name: pg_dest
    type: postgresql
    connection_string: ${PG_CONN}
    table: users
    schema_evolution:
      enabled: true
      mode: auto
      backup_previous: true
      log_changes: true
```

## Mode Options
| Mode     | Description                            | Behavior                                          |
| -------- | -------------------------------------- | ------------------------------------------------- |
| `none`   | No evolution applied                   | Pipeline fails if schema mismatch detected        |
| `detect` | Detect and log schema differences      | Reports added/removed/changed columns without DDL |
| `manual` | Generate SQL but do not apply          | Saves ALTER statements for review                 |
| `auto`   | Automatically alter destination schema | Executes ALTER TABLE commands safely before load  |


## Detection Logic

Conduit compares the **inferred source schema** with the **destination schema** fetched from the connector.

This process runs during pipeline execution (and during ``conduit validate`` if schema validation is enabled).

### Detected Changes
| Change Type             | Description                                    | Example                                |
| ----------------------- | ---------------------------------------------- | -------------------------------------- |
| **Added Columns**       | New columns in source not found in destination | `ADD COLUMN address TEXT`              |
| **Removed Columns**     | Columns no longer present in source            | Logged only; not dropped automatically |
| **Type Changes**        | Data type differences                          | `ALTER COLUMN rate TYPE FLOAT`         |
| **Nullability Changes** | Column changed from NOT NULL → NULLABLE        | Logged, may require manual approval    |


## SQL Generation

When schema evolution is set to ```manual``` or ```auto```, Conduit generates vendor-specific SQL via ```TableAutoCreator```.

**Example (PostgreSQL):**
```sql
ALTER TABLE "public"."users" ADD COLUMN "signup_date" DATE;
ALTER TABLE "public"."users" ALTER COLUMN "age" TYPE INTEGER USING age::integer;
```
**Example (Snowflake):**
```sql
ALTER TABLE "DB"."PUBLIC"."USERS" ADD COLUMN "signup_date" DATE;
```
**Example (BigQuery):**
```sql
ALTER TABLE `project.dataset.users`
ADD COLUMN signup_date DATE;
```
All statements are logged in ```.conduit/schema_changes/<table>_<timestamp>.sql```.

## Backup & Logging

Before any modification:

* The **previous schema** is archived in ```.conduit/schemas/<resource>_latest.json```

* If ```backup_previous: true```, Conduit saves a timestamped backup in the ```schemas/<resource>/``` history folder

Example directory structure:
```sql
.conduit/
  └── schemas/
      ├── users_latest.json
      └── users/
          ├── 20251018T192433.json
          └── 20251019T093233.json
  └── schema_changes/
      └── users_20251019T093233.sql
```

## Strategies
### 1. Auto Mode (Recommended for Stable Sources)

Let Conduit apply non-breaking schema updates automatically.
```yml
schema_evolution:
  enabled: true
  mode: auto
```
* Automatically adds new columns
* Logs type changes but does not drop or rename columns
* Never drops data without confirmation

## #2. Manual Mode (Controlled Evolution)

Generate DDL statements but review before applying.
```yml
schema_evolution:
  enabled: true
  mode: manual
  output_dir: "./schema_changes"
```
Use this in production environments where **change approval** is required.

Run the pipeline, review generated SQL, and apply manually using your DB tool.

## 3. Detect Mode (Observation Only)

Detect and log schema drift but take no action.
```yml
schema_evolution:
  enabled: true
  mode: detect
```
Conduit will:

* Log differences to the console and manifest

* Mark pipeline status as “schema drift detected”

* Continue ingestion if compatible

Ideal for early warning systems in data observability.

## Troubleshooting
| Symptom                       | Likely Cause                        | Fix                             |
| ----------------------------- | ----------------------------------- | ------------------------------- |
| Schema drift not detected     | `schema_evolution.enabled` is false | Enable it                       |
| Columns added but not applied | Mode is set to `detect` or `manual` | Switch to `auto`                |
| ALTER TABLE fails             | Database permissions missing        | Grant `ALTER` privileges        |
| Type changes not applied      | Type conversion unsafe              | Apply manually after validation |
| Backup not created            | `backup_previous` is false          | Set `backup_previous: true`     |

## Integration with CLI

You can inspect and manage schema evolution directly from CLI commands.

### Compare Schemas
```bash
conduit schema-compare my_resource --file ingest.yml
```
**Output:**
```sql
 Schema Comparison

 ADDED: signup_date (DATE)
 CHANGED: rate FLOAT → DOUBLE
```
### Validate Schema Compatibility
```bash
Validate Schema Compatibility
```
Checks:

* Source/destination connectivity

* Type compatibility

* Missing/added columns

* Schema drift warnings

## Best Practices

* Use **auto** only for additive schema changes (new columns).

* Use manual or detect for production-grade governance.

* Commit **.conduit/schemas/** to version control for reproducibility.

* Combine with Data Quality
 for full integrity coverage.

* Run nightly **schema-compare** in CI/CD to detect drift before jobs run.

## Advanced: Custom Evolution Policies

You can define a custom handler by subclassing ```SchemaEvolutionManager```:

from conduit_core.schema_evolution import SchemaEvolutionManager
```python
class CustomEvolution(SchemaEvolutionManager):
    def should_apply_change(self, change):
        # Only auto-add columns with 'safe_' prefix
        return change.type == "ADD" and change.column.startswith("safe_")
```
Then use it via environment variable:
```bash
export CONDUIT_SCHEMA_MANAGER="custom_evolution.CustomEvolution"
```

## See Also

* Data Quality

* Schema Validation

* CLI Reference

* README.md