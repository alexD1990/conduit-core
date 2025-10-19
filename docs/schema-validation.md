# Schema Validation — Conduit Core
## Overview

**Schema Validation** ensures that the data structure produced by your source matches the expectations of your destination before any records are written.
It prevents silent mismatches, broken loads, and inconsistent analytics by running pre-flight checks on:

* Column existence
* Type compatibility
* Nullability requirements
* Required-column presence
* Schema drift (added / removed / changed columns)

Validation runs automatically when ```validate_schema: true``` in your destination configuration, or manually via:
```bash
conduit validate my_resource --file ingest.yml
```

## Configuration
Enable schema validation at the destination level in ```ingest.yml```.
```yml
destinations:
  - name: pg_dest
    type: postgresql
    connection_string: ${PG_CONN}
    table: users
    validate_schema: true
    required_columns:
      - id
      - email
```
| Field              | Description                                                                     | Example        |
| ------------------ | ------------------------------------------------------------------------------- | -------------- |
| `validate_schema`  | Enables schema validation before write                                          | `true`         |
| `required_columns` | Columns that must exist in source schema                                        | `[id, email]`  |
| `schema_evolution` | Optional auto-correction config (see [Schema Evolution](./schema-evolution.md)) | `mode: detect` |

## Validation Pipeline

### Schema Validation is performed in six ordered stages:

1. **Config Load** – Confirms valid ```ingest.yml``` syntax and structure
2. **Source Connection Test** – Verifies credentials, file path, or API reachability
3. **Destination Connection Test** – Ensures write access and metadata visibility
4. **Schema Inference** – Samples records to infer the source schema via ```SchemaInferrer```
5. **Compatibility Check** – Compares inferred schema vs destination schema
6. **Quality & Required Column Check** – Validates column presence and quality rules (if defined)

If any stage fails, Conduit exits early with a clear diagnostic and exit code:
| Code | Meaning                           |
| ---- | --------------------------------- |
| `0`  | All checks passed                 |
| `1`  | Validation failed                 |
| `2`  | Configuration or connection error |

## Compatibility Rules

### The validator applies conservative compatibility logic:
| Scenario                                             | Result                          |
| ---------------------------------------------------- | ------------------------------- |
| Column missing in source but required in destination | ❌ Fail                          |
| Extra column in source                               | ⚠ Warning (allowed)             |
| Type widening (INT → FLOAT, STRING → TEXT)           | ✅ Allowed                       |
| Type narrowing (FLOAT → INT)                         | ⚠ Warning in strict mode → Fail |
| Nullability change (NOT NULL → NULL)                 | ⚠ Warning                       |
| Nullability change (NULL → NOT NULL)                 | ❌ Fail                          |
| Unknown type mapping                                 | ❌ Fail (manual review needed)   |

## CLI Usage
### Validate Without Running Pipeline
```bash
conduit validate users_to_pg --file ingest.yml
```
Example output:
```sql
 Conduit Pre-Flight Validation

✓ Configuration loaded successfully
✓ Source connection (csv)
✓ Destination connection (postgres)
✓ Inferred schema from 100 records
✓ Type compatibility verified
✓ All required columns present
✓ Quality checks passed on sample
──────────────────────────────────────────────
Validation Complete
✓ All validations passed
```
If warnings exist and ```--strict``` is set, Conduit treats them as failures:
```bash
conduit validate users_to_pg --no-strict
```

### Compare Schema Drift

Detect upstream schema changes before they break a pipeline:
```bash
conduit schema-compare users_to_pg --file ingest.yml
```

Output example:
```sql
 Schema Comparison
 ADDED: signup_date (DATE)
 CHANGED: rate FLOAT → DOUBLE
──────────────────────────────────────────────
Summary:
 • 1 column added
 • 1 type change
 Review type change before next run
```

### Strict Mode

The ```--strict``` flag enforces full compliance:

* Treats warnings as errors
* Blocks loads if nullable/type drift exists
* Recommended for production pipelines with strong schema guarantees

In CI/CD, run:
```bash
conduit validate my_resource --strict
```
Failing this check prevents promotion to production.

### Integration with Schema Evolution

When both **validation** and **evolution** are enabled:

1. Validation detects the drift
2. Evolution generates SQL to fix it (manual or auto mode)
3. Validation reruns to confirm alignment

This closed loop allows for **self-healing pipelines** under controlled governance.

## Troubleshooting
| Symptom                             | Cause                                       | Solution                                       |
| ----------------------------------- | ------------------------------------------- | ---------------------------------------------- |
| “Destination schema not found”      | Table doesn’t exist                         | Create table or enable `schema_evolution.auto` |
| “Type mismatch”                     | Source → destination type conversion unsafe | Review column types or use manual evolution    |
| “Missing required column”           | Source file/query missing required field    | Update source or drop from `required_columns`  |
| “Validation passed but load failed” | Downstream constraint violation             | Check destination logs (unique, FK, etc.)      |
| “Validation skipped”                | `validate_schema` disabled                  | Enable it or run `conduit validate` manually   |

## Examples
### Example 1: CSV → PostgreSQL with Required Columns
```yml
resources:
  - name: customers_to_pg
    source: csv_source
    destination: pg_dest
    query: "n/a"
destinations:
  - name: pg_dest
    type: postgresql
    validate_schema: true
    required_columns:
      - id
      - email
```
```bash
conduit validate customers_to_pg --file ingest.yml
```
### Example 2: JSON API → BigQuery with Strict Mode
```bash
conduit validate api_to_bq --strict --file ingest.yml
```

## Best Practices

* Run ```validate``` as a pre-deployment CI step for all pipelines.
* Keep a baseline schema under ```.conduit/schemas/``` in Git.
* Use strict mode for production and non-strict for exploratory pipelines.
* Pair with schema-compare for drift detection reports.
* Combine with Data Quality
 for full dataset assurance.

## Developer Notes

The internal engine relies on:

* ```SchemaInferrer``` — infers schema from sample data
* ```SchemaValidator``` — compares schema pairs and reports structured errors
* ```ValidationReport``` — used for CLI output and manifest logging

Example internal API:
```python
from conduit_core.schema_validator import SchemaValidator

validator = SchemaValidator()
report = validator.validate_type_compatibility(src_schema, dest_schema)
if report.has_errors():
    raise ValidationError(report.summary())
```

## See Also

Data Quality

Schema Evolution

CLI Reference

README.md