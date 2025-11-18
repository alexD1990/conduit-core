# Quick Start Guide

## Installation

```bash
pip install conduit-core
```

## Using Templates (Recommended)

### 1. List Available Templates

```bash
conduit template list
```

Shows all 12 templates organized by category:
- **File → Warehouse:** csv_to_snowflake, csv_to_bigquery, csv_to_postgresql, parquet_to_bigquery
- **Database → Warehouse:** postgresql_to_snowflake, postgresql_to_bigquery, mysql_to_snowflake, mysql_to_bigquery
- **Cloud Storage:** s3_to_snowflake, s3_to_bigquery, s3_to_postgresql, json_to_s3

### 2. Get Template Details

```bash
conduit template info csv_to_snowflake
```

Shows:
- Description
- Source/destination types
- Required configuration
- Capabilities

### 3. Generate a Template

```bash
conduit template csv_to_snowflake > pipeline.yml
```

Creates a complete YAML with:
- ⚠️ markers showing what to update
- 💡 tips for auto-detected fields
- 🔒 security hints for credentials
- Step-by-step usage instructions

### 4. Configure Your Pipeline

Edit `pipeline.yml`:

```yaml
sources:
  - type: "csv"
    config:
      path: "/data/users.csv"  # ⚠️ UPDATE THIS

destinations:
  - type: "snowflake"
    config:
      account: "abc123"        # ⚠️ UPDATE THIS
      password: "${SNOWFLAKE_PASSWORD}"  # 🔒 Use env vars
      database: "ANALYTICS"    # ⚠️ UPDATE THIS
      table: "users"           # ⚠️ UPDATE THIS
```

### 5. Set Environment Variables

Create `.env` file:

```bash
SNOWFLAKE_PASSWORD=your_password
PG_PASSWORD=your_pg_password
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
```

### 6. Run Your Pipeline

```bash
conduit run pipeline.yml
```

## Common Patterns

### CSV to Data Warehouse

```bash
conduit template csv_to_snowflake > etl.yml
# Edit etl.yml with your file path and Snowflake credentials
conduit run etl.yml
```

### Database Replication

```bash
conduit template postgresql_to_bigquery > replication.yml
# Configure source query and BigQuery destination
conduit run replication.yml
```

### S3 to Warehouse

```bash
conduit template s3_to_snowflake > s3_load.yml
# Set S3 bucket/key and Snowflake table
conduit run s3_load.yml
```

### JSON Files to S3

```bash
conduit template json_to_s3 > backup.yml
# Configure local JSON path and S3 destination
conduit run backup.yml
```

## CLI Reference

### Template Commands

```bash
conduit template list              # Show all templates
conduit template info <name>       # Show template details
conduit template <name>            # Generate template YAML
```

### Pipeline Commands

```bash
conduit run <file.yml>             # Run a pipeline
conduit manifest --last            # Show last run details
```

## Tips

**Use environment variables for secrets:**
```yaml
password: "${DB_PASSWORD}"  # ✅ Good
password: "hardcoded123"    # ❌ Bad
```

**Start with templates:**
- Faster than writing YAML from scratch
- Shows best practices
- Includes validation rules

**Test connections first:**
- Templates include quality checks
- Run with small data samples initially
- Verify output before large loads

## Troubleshooting

**"Connection failed"**
- Check credentials in `.env`
- Verify host/account names
- Test network connectivity

**"YAML validation error"**
- Use templates as reference
- Check indentation (2 spaces)
- Verify all ⚠️ fields are updated

**"Schema mismatch"**
- Enable `auto_create_table: true`
- Check column names match
- Review data types

## Next Steps

- See [CONNECTORS.md](CONNECTORS.md) for detailed connector configuration
- Check [GitHub Issues](https://github.com/alexD1990/conduit-core/issues) for bug reports
- Review generated templates for advanced features (incremental loading, quality checks)
