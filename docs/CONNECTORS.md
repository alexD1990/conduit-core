# Connector Reference

Configuration examples for all 8 Conduit Core connectors.

---

## File Connectors

### CSV

**Source Configuration:**

```yaml
sources:
  - type: "csv"
    name: "my_csv"
    config:
      path: "/data/users.csv"
      delimiter: ","
      encoding: "utf-8"
      header: true
```

**Destination Configuration:**

```yaml
destinations:
  - type: "csv"
    name: "output_csv"
    config:
      path: "/output/results.csv"
      delimiter: ","
      encoding: "utf-8"
```

---

### JSON

**Source Configuration:**

```yaml
sources:
  - type: "json"
    name: "my_json"
    config:
      path: "/data/events.json"
      encoding: "utf-8"
```

**Destination Configuration:**

```yaml
destinations:
  - type: "json"
    name: "output_json"
    config:
      path: "/output/results.json"
      encoding: "utf-8"
```

---

### Parquet

**Source Configuration:**

```yaml
sources:
  - type: "parquet"
    name: "my_parquet"
    config:
      path: "/data/dataset.parquet"
```

**Destination Configuration:**

```yaml
destinations:
  - type: "parquet"
    name: "output_parquet"
    config:
      path: "/output/results.parquet"
      compression: "snappy"
```

---

## Cloud Storage

### S3

**Source Configuration:**

```yaml
sources:
  - type: "s3"
    name: "s3_source"
    config:
      bucket: "my-data-bucket"
      key: "path/to/file.csv"
      aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
      aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
      region: "us-east-1"
      file_format: "csv"  # csv, json, parquet
```

**Destination Configuration:**

```yaml
destinations:
  - type: "s3"
    name: "s3_dest"
    config:
      bucket: "my-output-bucket"
      key: "output/results.csv"
      aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
      aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
      region: "us-east-1"
      file_format: "csv"
```

---

## Databases

### PostgreSQL

**Source Configuration:**

```yaml
sources:
  - type: "postgresql"
    name: "pg_source"
    config:
      host: "${PG_HOST}"
      port: 5432
      database: "analytics"
      user: "${PG_USER}"
      password: "${PG_PASSWORD}"
      query: "SELECT * FROM users WHERE created_at > '2024-01-01'"
```

**Destination Configuration:**

```yaml
destinations:
  - type: "postgresql"
    name: "pg_dest"
    config:
      host: "${PG_HOST}"
      port: 5432
      database: "warehouse"
      user: "${PG_USER}"
      password: "${PG_PASSWORD}"
      schema: "public"
      table: "users"
      auto_create_table: true
```

---

### MySQL

**Source Configuration:**

```yaml
sources:
  - type: "mysql"
    name: "mysql_source"
    config:
      host: "${MYSQL_HOST}"
      port: 3306
      database: "production"
      user: "${MYSQL_USER}"
      password: "${MYSQL_PASSWORD}"
      query: "SELECT * FROM orders"
```

**Destination Configuration:**

```yaml
destinations:
  - type: "mysql"
    name: "mysql_dest"
    config:
      host: "${MYSQL_HOST}"
      port: 3306
      database: "analytics"
      user: "${MYSQL_USER}"
      password: "${MYSQL_PASSWORD}"
      table: "orders"
      auto_create_table: true
```

---

## Data Warehouses

### Snowflake

**Destination Configuration:**

```yaml
destinations:
  - type: "snowflake"
    name: "snowflake_dest"
    config:
      account: "${SNOWFLAKE_ACCOUNT}"
      user: "${SNOWFLAKE_USER}"
      password: "${SNOWFLAKE_PASSWORD}"
      warehouse: "COMPUTE_WH"
      database: "ANALYTICS"
      schema: "PUBLIC"
      table: "users"
      role: "ACCOUNTADMIN"  # optional
      auto_create_table: true
```

**Connection String Format:**
- Account: `abc12345.us-east-1` or `orgname-accountname`

---

### BigQuery

**Destination Configuration:**

```yaml
destinations:
  - type: "bigquery"
    name: "bigquery_dest"
    config:
      project_id: "${GCP_PROJECT_ID}"
      dataset: "analytics"
      table: "users"
      credentials_path: "${GOOGLE_APPLICATION_CREDENTIALS}"
      location: "US"  # optional, default: US
      auto_create_table: true
```

**Authentication:**
- Set `GOOGLE_APPLICATION_CREDENTIALS` to service account JSON path
- Or use Application Default Credentials (ADC)

---

## Common Configuration Patterns

### Environment Variables

```yaml
# ✅ Recommended: Use environment variables for secrets
password: "${DB_PASSWORD}"
aws_access_key_id: "${AWS_KEY}"
credentials_path: "${GOOGLE_CREDS}"

# ❌ Avoid: Hardcoded credentials
password: "mypassword123"
```

### Auto Table Creation

```yaml
destinations:
  - type: "postgresql"
    config:
      table: "new_table"
      auto_create_table: true  # Creates table if missing
```

### Incremental Loading

```yaml
pipelines:
  - name: "incremental_sync"
    source: "pg_source"
    destination: "snowflake_dest"
    incremental:
      strategy: "timestamp"
      column: "updated_at"
      state_file: ".conduit/state/sync.json"
```

### Quality Checks

```yaml
pipelines:
  - name: "validated_load"
    source: "csv_source"
    destination: "pg_dest"
    quality_checks:
      - column: "id"
        check: "not_null"
        action: "fail"
      - column: "email"
        check: "regex"
        pattern: ".+@.+\\..+"
        action: "warn"
```

---

## Notes

**File Paths:**
- Absolute: `/data/file.csv`
- Relative: `./data/file.csv`
- Environment: `${DATA_PATH}/file.csv`

**Database Queries:**
- Use full SQL: `SELECT id, name FROM users WHERE active = true`
- Use incremental column in WHERE: `created_at > '2024-01-01'`
- Queries are executed as-is (no modification)

**Cloud Authentication:**
- S3: AWS access key/secret or IAM role
- BigQuery: Service account JSON or ADC
- Snowflake: Username/password or key-pair auth

**Schema Evolution:**
- Enable `auto_create_table: true` for automatic table creation
- Schema inference detects column types automatically
- Schema evolution handles new columns in source data

---

## Quick Reference Table

| Connector   | Type             | Auth Method                    |
|-------------|------------------|--------------------------------|
| CSV         | File             | File system access             |
| JSON        | File             | File system access             |
| Parquet     | File             | File system access             |
| S3          | Cloud Storage    | AWS access key/secret          |
| PostgreSQL  | Database         | Username/password              |
| MySQL       | Database         | Username/password              |
| Snowflake   | Data Warehouse   | Username/password              |
| BigQuery    | Data Warehouse   | Service account JSON           |

---

For complete examples, use `conduit template list` and generate templates for your use case.
