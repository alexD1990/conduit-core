# Installation & Setup

## Requirements

- **Python**: 3.12 or higher
- **Package Manager**: `pip` or `poetry`

## Installation

### Option 1: Install via pip

```bash
pip install conduit-core
```

### Option 2: Install from Source (Poetry)

For development or to install directly from the repository:

```bash
# Clone the repository
git clone https://github.com/conduit-core/conduit-core.git
cd conduit-core

# Install dependencies with Poetry
poetry install

# Activate the virtual environment
poetry shell
```

### Verify Installation

```bash
conduit --version
```

You should see output like: `Conduit Core version: 1.0.0`

## Credential Management

Conduit Core supports secure credential management through environment variables. **Never hardcode credentials in `ingest.yml` files.**

### Using a `.env` File (Recommended)

Create a `.env` file in your project root:

```bash
# .env
# AWS Credentials
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_SESSION_TOKEN=your_session_token  # Optional

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=mydb
POSTGRES_USER=postgres
POSTGRES_PASSWORD=mysecretpassword

# MySQL
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=mydb
MYSQL_USER=root
MYSQL_PASSWORD=mysecretpassword

# Snowflake
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_USER=etl_user
SNOWFLAKE_PASSWORD=snowflake_pass
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ANALYTICS
SNOWFLAKE_SCHEMA=PUBLIC

# BigQuery (uses service account JSON)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

**Important**: Add `.env` to your `.gitignore`:

```bash
echo ".env" >> .gitignore
```

### Using System Environment Variables

Alternatively, set credentials as system environment variables:

```bash
# Linux/macOS
export AWS_ACCESS_KEY_ID=your_access_key
export POSTGRES_PASSWORD=mysecretpassword

# Windows (Command Prompt)
set AWS_ACCESS_KEY_ID=your_access_key
set POSTGRES_PASSWORD=mysecretpassword

# Windows (PowerShell)
$env:AWS_ACCESS_KEY_ID="your_access_key"
$env:POSTGRES_PASSWORD="mysecretpassword"
```

### Referencing Environment Variables in `ingest.yml`

Use `${VARIABLE_NAME}` syntax to reference environment variables:

```yaml
sources:
  - name: prod_db
    type: postgresql
    host: ${POSTGRES_HOST}
    database: ${POSTGRES_DATABASE}
    user: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}

destinations:
  - name: s3_bucket
    type: s3
    bucket: my-data-lake
    path: raw/users.csv
    # AWS credentials automatically loaded from environment
```

## Connector-Specific Setup

### AWS (S3)

**Authentication Methods** (in order of precedence):

1. Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
2. AWS credentials file: `~/.aws/credentials`
3. IAM role (when running on EC2/ECS/Lambda)

**Example `~/.aws/credentials`:**

```ini
[default]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY
region = us-east-1
```

### PostgreSQL

**Connection String Format** (alternative to individual parameters):

```yaml
sources:
  - name: pg_source
    type: postgresql
    connection_string: "host=localhost dbname=mydb user=postgres password=secret"
```

**Connection Testing:**

```bash
# Test PostgreSQL connection before running pipeline
conduit preflight ingest.yml --resource my_postgres_resource
```

### MySQL

**Setup**:

```yaml
sources:
  - name: mysql_source
    type: mysql
    host: ${MYSQL_HOST}
    port: 3306
    database: ${MYSQL_DATABASE}
    user: ${MYSQL_USER}
    password: ${MYSQL_PASSWORD}
```

### Snowflake

**Account Identifier Format**: `<account_locator>.<region>`

Example: `xy12345.us-east-1`

**Warehouse Setup**: Ensure your Snowflake warehouse is running before executing pipelines.

```yaml
destinations:
  - name: snowflake_dest
    type: snowflake
    account: ${SNOWFLAKE_ACCOUNT}
    user: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
    warehouse: ${SNOWFLAKE_WAREHOUSE}
    database: ${SNOWFLAKE_DATABASE}
    schema: ${SNOWFLAKE_SCHEMA}
    table: target_table
```

### BigQuery

**Authentication**: Uses Google Cloud service account JSON file.

**Setup Steps**:

1. Create a service account in Google Cloud Console
2. Download the JSON key file
3. Set the environment variable:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

**Configuration**:

```yaml
destinations:
  - name: bigquery_dest
    type: bigquery
    project: my-gcp-project
    dataset: analytics
    table: users
    location: US  # Optional, defaults to US
```

**Permissions Required**:
- `bigquery.tables.create`
- `bigquery.tables.updateData`
- `bigquery.jobs.create`

## Project Structure

Recommended directory structure for a Conduit Core project:

```
my-data-project/
├── .env                    # Credentials (gitignored)
├── ingest.yml             # Pipeline configuration
├── .conduit_state.json    # Incremental state (auto-generated)
├── .checkpoints/          # Resume checkpoints (auto-generated)
├── manifest.json          # Execution history (auto-generated)
├── errors/                # Dead letter queue (auto-generated)
│   └── failed_records_*.json
├── data/                  # Local data files
│   ├── input/
│   └── output/
└── .gitignore
```

**Recommended `.gitignore` entries**:

```
.env
.conduit_state.json
.checkpoints/
manifest.json
errors/
data/output/
*.pyc
__pycache__/
```

## Testing Your Installation

Create a simple test pipeline:

**`test.yml`:**

```yaml
sources:
  - name: test_source
    type: csv
    path: test_input.csv

destinations:
  - name: test_output
    type: json
    path: test_output.json

resources:
  - name: test_pipeline
    source: test_source
    destination: test_output
```

**`test_input.csv`:**

```csv
id,name
1,Alice
2,Bob
```

**Run the test:**

```bash
conduit run test.yml
```

If successful, you'll see `test_output.json` containing the CSV data in JSON format.

## Next Steps

- **Usage Guide**: [docs/usage.md](usage.md) - Learn CLI commands and pipeline configuration
- **Connectors**: [docs/connectors.md](connectors.md) - Detailed connector configuration
- **Examples**: Create your first production pipeline

## Troubleshooting

### `ModuleNotFoundError: No module named 'conduit_core'`

**Solution**: Ensure you've activated the Poetry virtual environment:

```bash
poetry shell
```

### Connection Errors

**Use preflight checks to diagnose**:

```bash
conduit preflight ingest.yml
```

Common issues:
- Incorrect credentials in `.env`
- Firewall blocking database connections
- Missing permissions on cloud resources
- Warehouse not running (Snowflake)

### Import Errors for Specific Connectors

**Missing optional dependencies**:

```bash
# For PostgreSQL
pip install psycopg2-binary

# For Snowflake
pip install snowflake-connector-python

# For BigQuery
pip install google-cloud-bigquery
```

All standard dependencies are included in the base installation.
