# Data Quality — Conduit Core

Conduit Core includes a built-in data quality framework that validates records during ingestion.
It lets you define column-level rules directly in your `ingest.yml` and choose how violations are handled.

## Overview

Conduit Core introduces a **first-class Data Quality Framework** — enabling you to define validation rules declaratively in your ```ingest.yml```.
The framework runs automated checks on source data before writing to destinations, ensuring integrity, consistency, and reliability across pipelines.

### Quality rules can:

* Enforce **business constraints** (e.g., no null IDs, valid email formats)
* Prevent **bad data propagation**
* Automatically route invalid rows to a **Dead-Letter Queue (DLQ)**
* Either **fail**, **warn**, or send records to the **DLQ**.

## Configuration

```yaml
# Each resource in your ingest.yml can specify a list of quality checks.
resources:
  - name: users_to_postgres
    source: csv_source
    destination: pg_dest
    query: "SELECT * FROM users"
    quality_checks:
      - name: not_null_id
        column: id
        check: not_null
        action: fail

      - name: positive_age
        column: age
        check: greater_than
        value: 0
        action: dlq

      - name: valid_email
        column: email
        check: regex
        pattern: "^[^@]+@[^@]+\\.[^@]+$"
        action: warn
```

## Configuration Fields
| Field     | Description                                 | Example             |
| --------- | ------------------------------------------- | ------------------- |
| `name`    | Unique identifier for the check             | `valid_email`       |
| `column`  | Column to apply the check to                | `email`             |
| `check`   | Built-in or custom check type               | `regex`, `not_null` |
| `value`   | Check threshold (for numeric checks)        | `0`                 |
| `pattern` | Regex pattern (for string checks)           | `"^[A-Z]+$"`        |
| `action`  | Behavior on failure (`fail`, `warn`, `dlq`) | `fail`              |

## Built-in Checks

Conduit Core ships with a set of **standard checks** for common quality rules.
| Rule           | Description                                      | Parameters   | Example                         |
| -------------- | ------------------------------------------------ | ------------ | ------------------------------- |
| `not_null`     | Fails if the value is null                       | —            | —                               |
| `greater_than` | Value must be strictly greater than given number | `value`      | `greater_than: 0`               |
| `less_than`    | Value must be strictly less than given number    | `value`      | `less_than: 100`                |
| `between`      | Value must be within an inclusive range          | `min`, `max` | `between: [1, 10]`              |
| `regex`        | Matches a regex pattern                          | `pattern`    | `regex: "^[A-Z]+$"`             |
| `in_list`      | Value must be one of allowed values              | `allowed`    | `in_list: ["A","B","C"]`        |
| `unique`       | Column must have unique values in batch          | —            | —                               |
| `not_in_list`  | Value must *not* be in forbidden values          | `forbidden`  | `not_in_list: ["error", "n/a"]` |

## Custom Checks

You can register custom validation logic by subclassing ```QualityValidator``` or by defining inline lambdas.

### Example: Custom Python Validator
```python
# custom_validators.py
from conduit_core.quality import register_validator

@register_validator("is_weekday")
def is_weekday(value):
    from datetime import datetime
    if not value:
        return True
    date = datetime.strptime(value, "%Y-%m-%d")
    return date.weekday() < 5  # Mon–Fri
```

Then in ```ingest.yml:```
```text
quality_checks:
  - name: weekday_check
    column: signup_date
    check: is_weekday
    action: warn
```
This modular design allows teams to share reusable validation logic across projects.

## Action Behaviors

Each check defines an action determining how the pipeline reacts when a record fails validation:
| Action | Description                                  | Behavior                                            |
| ------ | -------------------------------------------- | --------------------------------------------------- |
| `fail` | Stops the pipeline immediately               | Raises `DataQualityError`                           |
| `warn` | Logs a warning and continues                 | Visible in logs and manifest                        |
| `dlq`  | Sends failed record to **Dead-Letter Queue** | Record logged in `/errors/<resource>_errors_*.json` |

> **Note:**  
> The `unique` check validates uniqueness *within each batch*.  
> For global uniqueness across the full dataset, rely on database constraints.

### Example DLQ output:
```json
{
  "row_number": 5,
  "record": {"id": null, "email": "bob@invalid"},
  "error_type": "DataQualityError",
  "error_message": "not_null_id failed",
  "timestamp": "2025-10-19T18:44:12Z",
  "failure_type": "quality_check"
}
```
## Running Quality Checks

### CLI Pre-Flight Validation

Run quality checks on sample data without executing the pipeline:
```bash
conduit validate users_to_postgres --file ingest.yml
```
**Output:**
```text
 Conduit Pre-Flight Validation

✓ Configuration loaded successfully
✓ Source connection (csv)
✓ Destination connection (postgres)
✓ Inferred schema from 100 records
✓ All required columns present
⚠ 2 records failed quality checks in sample
```

## Full Pipeline Run

Quality checks are automatically executed during normal ```conduit run``` operations.

If ```action=fail```, the job will terminate with an error.
If ```action=dlq```, failed records will be logged and excluded from the load.

## Best Practices

 Use ```fail``` for critical integrity rules (e.g., null IDs, broken foreign keys).

 Use ```warn``` for soft business rules (e.g., incomplete optional data).

 Use ```dlq``` for data you might fix later (e.g., partially corrupted CSVs).

 Group reusable rules in a ```quality.py``` module.

 Always run ```conduit validate``` before scheduled jobs to catch schema/quality drift.

 Combine with schema validation for complete data reliability.

| Symptom                                 | Likely Cause                         | Fix                                    |
| --------------------------------------- | ------------------------------------ | -------------------------------------- |
| All rows fail a numeric rule            | Incorrect value type (string vs int) | Cast upstream or adjust rule threshold |
| Regex rule never matches                | Missing `^` and `$` anchors          | Ensure pattern is anchored             |
| DLQ file not created                    | `action` not set to `dlq`            | Add `action: dlq`                      |
| CLI exits early with `DataQualityError` | `action: fail` triggered             | Change to `warn` or `dlq` if non-fatal |
| No output in logs                       | Logging level too high               | Set `LOG_LEVEL=DEBUG`                  |

## Examples
### Example 1: Customer Data CSV → PostgreSQL
```text
resources:
  - name: customers_to_pg
    source: csv_source
    destination: pg_dest
    query: "n/a"
    quality_checks:
      - name: id_not_null
        column: id
        check: not_null
        action: fail
      - name: valid_email
        column: email
        check: regex
        pattern: "^[^@]+@[^@]+\\.[^@]+$"
        action: dlq
```

## Example 2: JSON API → BigQuery with Soft Rules
```text
quality_checks:
  - name: country_code
    column: country
    check: in_list
    allowed: ["NO", "SE", "DK"]
    action: warn
  - name: positive_amount
    column: amount
    check: greater_than
    value: 0
    action: fail
```

## See Also
- [Schema Validation](schema-validation.md)
- [Schema Evolution](schema-evolution.md)
- [CLI Reference](cli-reference.md)
- [README](../README.md)

---
This document describes the data quality framework in Conduit Core v1.0.  
Future versions (v1.2+) will add row-level lineage and DLQ replay capabilities.
