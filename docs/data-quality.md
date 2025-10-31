# Data Quality — Schema Validation & Quality Checks

Comprehensive guide to Conduit Core's dual-layer quality assurance: schema validation (structure) and data quality checks (values).

---

## Overview

Conduit Core provides **two complementary quality systems**:

1. **Schema Validation** — Structural integrity (columns, types, constraints)
2. **Quality Checks** — Value-level validation (format, range, uniqueness)

Together, they ensure both the **structure** and **content** of your data meet requirements before writes occur.

```
┌──────────────────────────────────────────────────────────────┐
│                    QUALITY PIPELINE                          │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  1. Schema Validation (Pre-flight)                           │
│     ↓                                                        │
│     • Compare source vs destination schemas                  │
│     • Detect type mismatches                                 │
│     • Flag missing/extra columns                             │
│     • Validate constraints (NOT NULL)                        │
│                                                              │
│  2. Data Quality Checks (Runtime)                            │
│     ↓                                                        │
│     • Validate record values                                 │
│     • Apply business rules                                   │
│     • Route failures (fail/warn/dlq)                         │
│                                                              │
│  3. Write (Only if passed)                                   │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

## Part 1: Schema Validation

### Purpose

Prevents pipeline failures by detecting **structural incompatibilities** before execution:
- Missing required columns
- Type mismatches (e.g., `string` → `integer`)
- Constraint violations (nullable → NOT NULL)
- Schema drift detection

### When It Runs

**Automatically** during:
- `conduit run` (pre-flight phase)
- `conduit validate` (explicit check)
- `conduit preflight` (comprehensive health check)

### Configuration

**Enable in destination config:**
```yaml
destinations:
  - name: warehouse
    type: postgresql
    table: users
    validate_schema: true        # Enable validation
    strict_validation: false     # Treat warnings as errors if true
    required_columns:            # Optional: enforce specific columns
      - id
      - email
      - created_at
```

### Validation Rules

| Check                  | Description                                      | Severity |
|------------------------|--------------------------------------------------|----------|
| **Type Compatibility** | Source type can convert to destination type      | Error    |
| **Required Columns**   | All `required_columns` present in source         | Error    |
| **Missing Columns**    | Destination columns not in source                | Warning  |
| **Constraint Violation** | Nullable source → NOT NULL destination         | Error    |
| **Extra Columns**      | Source columns not in destination                | Warning  |

### Type Compatibility Matrix

```python
# Safe conversions (source → destination)
integer  → [integer, float, decimal, string]
float    → [float, decimal, string]
decimal  → [decimal, string]
string   → [string]
boolean  → [boolean, integer, string]
date     → [date, datetime, string]
datetime → [datetime, string]
```

**Examples:**
- ✅ `integer` → `float` (safe widening)
- ✅ `date` → `string` (safe conversion)
- ❌ `string` → `integer` (unsafe, may fail at runtime)
- ❌ `float` → `integer` (precision loss)

### CLI Commands

**Pre-flight Validation:**
```bash
conduit validate my_resource --file ingest.yml
```

**Output:**
```
✓ Configuration syntax valid
✓ Source connection (csv)
✓ Destination connection (postgresql)
✓ Schema inferred (5 columns)
✓ Type compatibility passed
⚠ Warning: Column 'deprecated_field' in destination not present in source
✓ All required columns present
```

**Schema Comparison:**
```bash
conduit schema-compare my_resource --file ingest.yml
```

**Output:**
```
Schema Drift Detected:
  [+] ADDED: signup_date (DATE, nullable)
  [-] REMOVED: old_status (VARCHAR)
  [~] CHANGED: amount (INTEGER → DECIMAL)
```

**Schema Inference & Export:**
```bash
conduit schema my_resource --format json --output schema.json
```

### Validation Report Structure

```python
# Internal API (schema_validator.py)
ValidationReport(
    is_valid: bool,
    errors: [
        ValidationError(
            column='age',
            issue='type_mismatch',
            expected='integer',
            actual='string',
            severity='error'
        )
    ],
    warnings: [
        ValidationError(
            column='deprecated_field',
            issue='missing_column',
            severity='warning'
        )
    ]
)
```

### Integration with Schema Evolution

When **both** validation and evolution are enabled:

```yaml
destinations:
  - name: warehouse
    type: postgresql
    validate_schema: true
    schema_evolution:
      enabled: true
      mode: auto                    # auto/manual/warn
      on_new_column: add_nullable   # Action for new columns
      on_removed_column: ignore     # Action for removed columns
      on_type_change: fail          # Action for type changes
```

**Flow:**
1. **Validation** detects drift
2. **Evolution** generates DDL to fix it
3. **Validation** re-runs to confirm alignment
4. Pipeline proceeds

**Example:**
```
[Pre-flight] Validation detected: Column 'signup_date' missing in destination
[Evolution] Generated: ALTER TABLE users ADD COLUMN signup_date DATE NULL
[Evolution] Applied DDL successfully
[Validation] Re-validated: Schema now compatible
[Engine] Starting pipeline execution...
```

### Best Practices

- ✅ Enable `validate_schema: true` for production pipelines
- ✅ Use `strict_validation: true` to treat warnings as errors
- ✅ Commit `.conduit/schemas/` to Git for baseline tracking
- ✅ Run `conduit validate` as CI/CD pre-deployment check
- ✅ Combine with `schema_evolution.mode: manual` for controlled changes
- ⚠️ Avoid disabling validation in production
- ⚠️ Review warnings — they often indicate data quality issues

### Troubleshooting

| Symptom                             | Cause                                       | Solution                                       |
|-------------------------------------|---------------------------------------------|------------------------------------------------|
| "Destination schema not found"      | Table doesn't exist                         | Create table or enable `auto_create_table`     |
| "Type mismatch: string → integer"   | Source has non-numeric strings              | Clean source data or adjust destination type   |
| "Missing required column: email"    | Source missing column                       | Add column to source or remove from `required_columns` |
| "Validation passed but load failed" | Downstream constraint (FK, unique)          | Check database logs for constraint violations  |
| "Validation skipped"                | `validate_schema: false`                    | Enable validation in destination config        |

---

## Part 2: Data Quality Checks

### Purpose

Validates **record values** against business rules during pipeline execution:
- Data format validation (email, phone, regex)
- Range/boundary checks (age > 0, price < 1000)
- Uniqueness constraints (within batch)
- Allowed value lists (enums)
- Custom business logic

### When It Runs

**During execution** (`conduit run`):
- After reading each batch from source
- Before writing to destination
- Between schema validation and writes

### Configuration

**Define rules per resource:**
```yaml
resources:
  - name: users_to_warehouse
    source: csv_source
    destination: pg_warehouse
    query: "SELECT * FROM users"
    quality_checks:
      # Critical validation (stop pipeline)
      - column: id
        check: not_null
        action: fail
      
      # Format validation (send to DLQ)
      - column: email
        check: regex
        pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
        action: dlq
      
      # Range validation (log warning)
      - column: age
        check: range
        min_value: 0
        max_value: 120
        action: warn
      
      # Enum validation
      - column: status
        check: enum
        allowed_values: ["active", "pending", "inactive"]
        action: fail
      
      # Uniqueness (within batch)
      - column: id
        check: unique
        action: dlq
```

### Built-in Checks

| Check       | Description                          | Parameters                  | Example                                    |
|-------------|--------------------------------------|-----------------------------|---------------------------------------------|
| `not_null`  | Value must not be null/empty         | None                        | `check: not_null`                          |
| `regex`     | Value must match pattern             | `pattern` (string)          | `pattern: "^\\d{3}-\\d{4}$"`               |
| `range`     | Value within numeric bounds          | `min_value`, `max_value`    | `min_value: 0, max_value: 100`             |
| `unique`    | No duplicates within batch           | None                        | `check: unique`                            |
| `enum`      | Value in allowed list                | `allowed_values` (list)     | `allowed_values: ["US", "CA", "MX"]`       |

**Implementation (`quality.py`):**
```python
def not_null_validator(value: Any, **kwargs) -> bool:
    """Checks if value is not None or empty string"""
    return value is not None and value != ''

def regex_validator(value: Any, pattern: str, **kwargs) -> bool:
    """Validates string against regex pattern"""
    if not isinstance(value, str):
        return False
    return re.fullmatch(pattern, value) is not None

def range_validator(value: Any, min_value: float = None, max_value: float = None, **kwargs) -> bool:
    """Validates numeric value within bounds (inclusive)"""
    try:
        numeric_value = float(value)
    except (ValueError, TypeError):
        return False
    
    if min_value is not None and numeric_value < min_value:
        return False
    if max_value is not None and numeric_value > max_value:
        return False
    return True

def unique_validator(value: Any, seen_values: Set[Any], **kwargs) -> Tuple[bool, bool]:
    """Checks uniqueness within batch context"""
    if value in seen_values:
        return (False, False)  # Invalid, don't add
    return (True, True)  # Valid, add to set

def enum_validator(value: Any, allowed_values: List[Any], **kwargs) -> bool:
    """Checks if value in allowed list"""
    return value in allowed_values
```

### Actions

Determines pipeline behavior when validation fails:

| Action  | Behavior                                  | Use Case                                    |
|---------|-------------------------------------------|---------------------------------------------|
| `fail`  | Stop pipeline immediately                 | Critical integrity rules (null IDs, FKs)    |
| `warn`  | Log warning, continue processing          | Soft business rules (optional fields)       |
| `dlq`   | Route to Dead Letter Queue, continue      | Recoverable errors (bad formats)            |

**Action Priority:**
If a record fails multiple checks with different actions, the **highest severity** applies:
- `fail` > `warn` > `dlq`

### Quality Validator Architecture

```python
# quality.py
class QualityValidator:
    """Applies validation rules to records"""
    
    def __init__(self, checks: List[QualityCheck]):
        # Group checks by column for efficiency
        self.checks_by_column: Dict[str, List[QualityCheck]] = {}
        self._unique_keys_config: List[Tuple[str, str]] = []
    
    def validate_record(
        self, 
        record: Dict[str, Any],
        batch_unique_sets: Dict[str, Set[Any]]
    ) -> ValidationResult:
        """Validate single record, return result with failures"""
        
    def validate_batch(
        self, 
        records: List[Dict[str, Any]]
    ) -> BatchValidationResult:
        """Validate batch, manage unique check state"""
```

**Flow:**
```python
validator = QualityValidator(quality_checks)

for batch in batches:
    result = validator.validate_batch(batch)
    
    # Process valid records
    destination.write(result.valid_records)
    
    # Handle invalid records
    for invalid in result.invalid_records:
        highest_action = determine_action(invalid.failed_checks)
        
        if highest_action == QualityAction.FAIL:
            raise DataQualityError("Critical validation failed")
        elif highest_action == QualityAction.WARN:
            logger.warning(f"Record failed: {invalid.failed_checks}")
        else:  # DLQ
            error_log.add_quality_error(invalid.record, summary)
```

### Dead Letter Queue (DLQ)

Failed records saved to `./errors/` with full context:

```json
{
  "row_number": 4501,
  "record": {
    "id": 123,
    "email": "invalid-email",
    "age": 15
  },
  "error_type": "DataQualityError",
  "error_message": "email(regex): Value 'invalid-email' does not match pattern; age(range): Value '15' out of range (min=18)",
  "timestamp": "2025-10-31T14:23:45Z",
  "failure_type": "quality_check"
}
```

**DLQ Files:**
- Location: `./errors/{resource_name}_errors_{timestamp}.json`
- Format: NDJSON (one error per line)
- Rotation: New file per run

### Custom Validators

**Register custom business logic:**

```python
# custom_validators.py
from conduit_core.quality import QualityCheckRegistry

def is_valid_sku(value: Any, **kwargs) -> bool:
    """Validate SKU format: ABC-12345"""
    if not isinstance(value, str):
        return False
    return re.match(r"^[A-Z]{3}-\d{5}$", value) is not None

def is_weekday(value: Any, **kwargs) -> bool:
    """Validate date is Monday-Friday"""
    try:
        from datetime import datetime
        date = datetime.strptime(value, "%Y-%m-%d")
        return date.weekday() < 5
    except (ValueError, TypeError):
        return False

# Register validators
QualityCheckRegistry.register_custom("valid_sku", is_valid_sku)
QualityCheckRegistry.register_custom("is_weekday", is_weekday)
```

**Use in YAML:**
```yaml
quality_checks:
  - column: product_code
    check: valid_sku
    action: fail
  - column: order_date
    check: is_weekday
    action: warn
```

### CLI Integration

**Dry-run validation:**
```bash
conduit run my_resource --dry-run
```
- Reads source data
- Applies quality checks
- Shows validation results
- **Does not write** to destination

**Pre-flight with quality sampling:**
```bash
conduit validate my_resource --file ingest.yml
```
- Samples first 100 records
- Runs all quality checks
- Reports failure rate
- **Does not persist** DLQ

**Output:**
```
✓ Configuration valid
✓ Source connection (csv)
✓ Destination connection (postgresql)
✓ Schema inferred (8 columns)
⚠ Quality check results (sample):
  • email(regex): 2/100 records failed
  • age(range): 5/100 records failed
  • status(enum): 0/100 records failed
```

### Best Practices

**Rule Design:**
- ✅ Use `fail` for critical integrity (null IDs, broken FKs)
- ✅ Use `warn` for soft business rules (incomplete optional data)
- ✅ Use `dlq` for recoverable errors (bad formats, typos)
- ✅ Combine checks: `not_null` + `regex` for mandatory formatted fields
- ⚠️ Avoid over-validation (degrades performance)

**Performance:**
- ✅ `unique` check scopes to **batch only** (not global)
- ✅ Use database constraints for global uniqueness
- ✅ Regex patterns: anchor with `^...$` for performance
- ⚠️ Complex custom validators may slow throughput

**Maintenance:**
- ✅ Group reusable validators in `custom_validators.py`
- ✅ Version control quality rules with `ingest.yml`
- ✅ Monitor DLQ volume over time
- ✅ Periodically replay DLQ after fixing source issues

### Troubleshooting

| Symptom                                 | Likely Cause                          | Fix                                            |
|-----------------------------------------|---------------------------------------|------------------------------------------------|
| All records fail numeric rule           | Type mismatch (string vs number)      | Add type coercion or adjust source             |
| Regex never matches                     | Missing `^` and `$` anchors           | Update pattern to `^pattern$`                  |
| DLQ file not created                    | Action not set to `dlq`               | Change `action: dlq` in config                 |
| Pipeline exits early with error         | `action: fail` triggered              | Change to `warn` or `dlq` if non-critical      |
| No validation logs                      | Log level too high                    | Set `LOG_LEVEL=DEBUG`                          |
| Unique check fails unexpectedly         | Duplicate within same batch           | Expected behavior; check source data           |
| Custom validator not found              | Not registered                        | Call `QualityCheckRegistry.register_custom()`  |

---

## Part 3: Combined Workflows

### End-to-End Quality Assurance

**Production Pipeline:**
```yaml
sources:
  - name: api_source
    type: csv
    path: ./data/users.csv
    infer_schema: true

destinations:
  - name: warehouse
    type: postgresql
    table: users
    validate_schema: true          # Schema validation
    strict_validation: true        # Warnings = errors
    required_columns: [id, email]
    schema_evolution:
      enabled: true
      mode: manual
      on_new_column: add_nullable

resources:
  - name: users_pipeline
    source: api_source
    destination: warehouse
    query: "n/a"
    quality_checks:                # Data quality
      - column: id
        check: not_null
        action: fail
      - column: email
        check: regex
        pattern: "^[^@]+@[^@]+$"
        action: fail
      - column: age
        check: range
        min_value: 0
        max_value: 120
        action: dlq
```

**Execution Flow:**
```
1. Config Validation (Pydantic)
   ↓
2. Pre-flight Checks
   ├─ Source connection test
   ├─ Destination connection test
   ├─ Schema inference
   ├─ Schema validation (structure)
   └─ Quality check config validation
   ↓
3. Schema Evolution (if enabled)
   ├─ Detect drift
   ├─ Generate DDL
   └─ Apply changes (auto) or log (manual)
   ↓
4. Pipeline Execution
   ├─ Read batch
   ├─ Apply quality checks (values)
   ├─ Route failures (DLQ/warn/fail)
   └─ Write valid records
   ↓
5. Manifest Logging
   ├─ Schema changes applied
   ├─ Quality failure summary
   └─ Records processed/failed
```

### CI/CD Integration

**GitHub Actions Example:**
```yaml
name: Validate Pipeline

on: [push, pull_request]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Conduit
        run: pip install conduit-core
      
      - name: Schema Validation
        run: |
          conduit validate users_pipeline \
            --file ingest.yml \
            --strict
      
      - name: Schema Drift Check
        run: |
          conduit schema-compare users_pipeline \
            --file ingest.yml
      
      - name: Quality Check Preview
        run: |
          conduit run users_pipeline \
            --dry-run \
            --batch-size 100
```

### Monitoring & Observability

**Manifest Metadata:**
```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "resource": "users_pipeline",
  "status": "partial_success",
  "preflight": {
    "schema_validation": "passed",
    "schema_drift": "detected",
    "evolution_applied": false
  },
  "quality": {
    "checks_applied": 4,
    "records_failed": 47,
    "failure_rate": 0.047,
    "dlq_count": 47
  },
  "records": {
    "read": 1000,
    "written": 953,
    "failed": 47
  },
  "duration_seconds": 23.4
}
```

**Query Manifest:**
```bash
conduit manifest --status=partial_success --last=10
```

---

## Part 4: Examples

### Example 1: E-commerce Orders

**Requirements:**
- Orders must have valid order_id, customer_id
- Email format validation
- Amount must be positive
- Status must be in allowed list
- SKU format validation (custom)

```yaml
resources:
  - name: orders_ingestion
    source: s3_orders
    destination: postgres_warehouse
    quality_checks:
      # Critical fields
      - column: order_id
        check: not_null
        action: fail
      
      - column: customer_id
        check: not_null
        action: fail
      
      # Format validation
      - column: email
        check: regex
        pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
        action: dlq
      
      # Business rules
      - column: amount
        check: range
        min_value: 0
        action: fail
      
      - column: status
        check: enum
        allowed_values: ["pending", "processing", "shipped", "delivered", "cancelled"]
        action: fail
      
      # Custom validation
      - column: sku
        check: valid_sku
        action: warn
```

### Example 2: User Signups with Evolution

**Requirements:**
- Handle schema changes automatically
- Strict email validation
- Age range enforcement
- Duplicate detection

```yaml
destinations:
  - name: user_warehouse
    type: snowflake
    table: users
    validate_schema: true
    strict_validation: false
    schema_evolution:
      enabled: true
      mode: auto
      on_new_column: add_nullable
      on_removed_column: ignore
      on_type_change: fail

resources:
  - name: user_signups
    source: json_api
    destination: user_warehouse
    quality_checks:
      - column: user_id
        check: not_null
        action: fail
      
      - column: user_id
        check: unique
        action: fail
      
      - column: email
        check: regex
        pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
        action: fail
      
      - column: age
        check: range
        min_value: 13
        max_value: 120
        action: warn
```

**Outcome:**
- New columns automatically added to warehouse
- Invalid emails rejected (fail)
- Out-of-range ages logged as warnings
- Duplicate user_ids rejected

### Example 3: Financial Transactions

**Requirements:**
- Zero tolerance for data quality issues
- Strict schema validation
- No automatic evolution

```yaml
destinations:
  - name: transactions_db
    type: postgresql
    table: transactions
    validate_schema: true
    strict_validation: true
    required_columns:
      - transaction_id
      - account_id
      - amount
      - timestamp
    schema_evolution:
      enabled: true
      mode: manual  # No auto-changes

resources:
  - name: transaction_stream
    source: kafka_source
    destination: transactions_db
    quality_checks:
      - column: transaction_id
        check: not_null
        action: fail
      
      - column: transaction_id
        check: unique
        action: fail
      
      - column: amount
        check: not_null
        action: fail
      
      - column: amount
        check: range
        min_value: 0.01
        action: fail
      
      - column: account_id
        check: not_null
        action: fail
```

---

## Summary

### When to Use What

| Concern                          | Use Schema Validation            | Use Quality Checks               |
|----------------------------------|----------------------------------|----------------------------------|
| Column missing                   | ✅ Automatic detection           | ❌                               |
| Type mismatch                    | ✅ Pre-flight check              | ❌                               |
| Invalid email format             | ❌                               | ✅ `regex` check                 |
| Negative price                   | ❌                               | ✅ `range` check                 |
| Duplicate IDs                    | ❌                               | ✅ `unique` check (batch-level)  |
| Null in required field           | ✅ Constraint check              | ✅ `not_null` check              |
| Schema drift over time           | ✅ Evolution detection           | ❌                               |
| Business rule violation          | ❌                               | ✅ Custom validator              |

### Decision Matrix

```
Question: Should I use schema validation or quality checks?

Does it relate to column existence/types/structure?
  YES → Schema Validation
  NO  → ↓

Does it validate record values against rules?
  YES → Quality Checks
  NO  → ↓

Is it a database constraint (FK, unique)?
  YES → Schema Validation + DB constraints
```

### Key Takeaways

1. **Schema Validation** runs **before** execution (pre-flight)
2. **Quality Checks** run **during** execution (per-batch)
3. Both systems integrate seamlessly with schema evolution
4. DLQ enables recovery from data quality issues
5. Manifest tracks both schema and quality metadata
6. Custom validators extend quality framework
7. CI/CD integration prevents bad deploys

---

*This comprehensive guide covers Conduit Core v1.0. Future versions will add row-level lineage, DLQ replay, and advanced anomaly detection.*
