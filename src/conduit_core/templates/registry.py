from __future__ import annotations
from pathlib import Path
from typing import Dict, Any

BASE_DIR = Path(__file__).resolve().parent
DEFINITIONS_DIR = BASE_DIR / "definitions"

CATEGORIES: Dict[str, str] = {
    "file_to_warehouse": "FILE TO DATA WAREHOUSE",
    "database_to_warehouse": "DATABASE TO DATA WAREHOUSE",
    "cloud_storage": "CLOUD STORAGE",
}

TEMPLATE_REGISTRY: Dict[str, Dict[str, Any]] = {
    "csv_to_snowflake": {
        "name": "csv_to_snowflake",
        "description": "Import CSV files into Snowflake",
        "category": "file_to_warehouse",
        "source_type": "csv",
        "destination_type": "snowflake",
        "yaml_path": "definitions/csv_to_snowflake.yaml",
        "required_config": [
            "sources[0].config.path",
            "destinations[0].config.account",
            "destinations[0].config.table",
        ],
        "capabilities": [
            "Auto schema inference",
            "Incremental loading via file timestamp",
            "Data type mapping CSV → Snowflake",
        ],
    },
    "csv_to_bigquery": {
        "name": "csv_to_bigquery",
        "description": "Load CSV files into BigQuery",
        "category": "file_to_warehouse",
        "source_type": "csv",
        "destination_type": "bigquery",
        "yaml_path": "definitions/csv_to_bigquery.yaml",
        "required_config": [
            "sources[0].config.path",
            "destinations[0].config.project",
            "destinations[0].config.dataset",
            "destinations[0].config.table",
        ],
        "capabilities": [
            "Auto schema inference",
            "Batch load to BigQuery",
            "Support for GCS/BQ credentials via env vars",
        ],
    },
    "csv_to_postgresql": {
        "name": "csv_to_postgresql",
        "description": "Ingest CSV into PostgreSQL",
        "category": "file_to_warehouse",
        "source_type": "csv",
        "destination_type": "postgresql",
        "yaml_path": "definitions/csv_to_postgresql.yaml",
        "required_config": [
            "sources[0].config.path",
            "destinations[0].config.host",
            "destinations[0].config.database",
            "destinations[0].config.table",
        ],
        "capabilities": [
            "Auto schema inference",
            "Append or truncate-load modes",
            "Basic data type normalization",
        ],
    },
    "postgresql_to_snowflake": {
        "name": "postgresql_to_snowflake",
        "description": "Replicate PostgreSQL table into Snowflake",
        "category": "database_to_warehouse",
        "source_type": "postgresql",
        "destination_type": "snowflake",
        "yaml_path": "definitions/postgresql_to_snowflake.yaml",
        "required_config": [
            "sources[0].config.host",
            "sources[0].config.database",
            "sources[0].config.table",
            "destinations[0].config.account",
            "destinations[0].config.table",
        ],
        "capabilities": [
            "Incremental sync via primary key or timestamp",
            "Type-safe mapping Postgres → Snowflake",
            "Supports warehouse/schema configuration",
        ],
    },
    "postgresql_to_bigquery": {
        "name": "postgresql_to_bigquery",
        "description": "Replicate PostgreSQL table into BigQuery",
        "category": "database_to_warehouse",
        "source_type": "postgresql",
        "destination_type": "bigquery",
        "yaml_path": "definitions/postgresql_to_bigquery.yaml",
        "required_config": [
            "sources[0].config.host",
            "sources[0].config.database",
            "sources[0].config.table",
            "destinations[0].config.project",
            "destinations[0].config.dataset",
            "destinations[0].config.table",
        ],
        "capabilities": [
            "Incremental sync support",
            "Automatic schema mapping where compatible",
            "Env-based credentials for security",
        ],
    },
    "mysql_to_snowflake": {
        "name": "mysql_to_snowflake",
        "description": "Replicate MySQL table into Snowflake",
        "category": "database_to_warehouse",
        "source_type": "mysql",
        "destination_type": "snowflake",
        "yaml_path": "definitions/mysql_to_snowflake.yaml",
        "required_config": [
            "sources[0].config.host",
            "sources[0].config.database",
            "sources[0].config.table",
            "destinations[0].config.account",
            "destinations[0].config.table",
        ],
        "capabilities": [
            "Incremental sync via numeric/id columns",
            "Schema normalization MySQL → Snowflake",
        ],
    },
    "mysql_to_bigquery": {
        "name": "mysql_to_bigquery",
        "description": "Replicate MySQL table into BigQuery",
        "category": "database_to_warehouse",
        "source_type": "mysql",
        "destination_type": "bigquery",
        "yaml_path": "definitions/mysql_to_bigquery.yaml",
        "required_config": [
            "sources[0].config.host",
            "sources[0].config.database",
            "sources[0].config.table",
            "destinations[0].config.project",
            "destinations[0].config.dataset",
            "destinations[0].config.table",
        ],
        "capabilities": [
            "Incremental sync support",
            "Automatic schema mapping where compatible",
        ],
    },
    "s3_to_snowflake": {
        "name": "s3_to_snowflake",
        "description": "Load S3 objects into Snowflake",
        "category": "cloud_storage",
        "source_type": "s3",
        "destination_type": "snowflake",
        "yaml_path": "definitions/s3_to_snowflake.yaml",
        "required_config": [
            "sources[0].config.bucket",
            "sources[0].config.prefix",
            "destinations[0].config.account",
            "destinations[0].config.table",
        ],
        "capabilities": [
            "Bulk loading via external stage pattern",
            "Support for CSV/JSON/Parquet on S3",
        ],
    },
    "s3_to_bigquery": {
        "name": "s3_to_bigquery",
        "description": "Load S3 objects into BigQuery",
        "category": "cloud_storage",
        "source_type": "s3",
        "destination_type": "bigquery",
        "yaml_path": "definitions/s3_to_bigquery.yaml",
        "required_config": [
            "sources[0].config.bucket",
            "sources[0].config.prefix",
            "destinations[0].config.project",
            "destinations[0].config.dataset",
            "destinations[0].config.table",
        ],
        "capabilities": [
            "Batch ingestion from S3",
            "Support for federated or staged loads",
        ],
    },
    "s3_to_postgresql": {
        "name": "s3_to_postgresql",
        "description": "Ingest S3 files into PostgreSQL",
        "category": "cloud_storage",
        "source_type": "s3",
        "destination_type": "postgresql",
        "yaml_path": "definitions/s3_to_postgresql.yaml",
        "required_config": [
            "sources[0].config.bucket",
            "sources[0].config.prefix",
            "destinations[0].config.host",
            "destinations[0].config.database",
            "destinations[0].config.table",
        ],
        "capabilities": [
            "File-based ingestion via local staging",
            "Append mode into PostgreSQL",
        ],
    },
    "json_to_s3": {
        "name": "json_to_s3",
        "description": "Write JSON records to S3",
        "category": "cloud_storage",
        "source_type": "json",
        "destination_type": "s3",
        "yaml_path": "definitions/json_to_s3.yaml",
        "required_config": [
            "sources[0].config.path",
            "destinations[0].config.bucket",
            "destinations[0].config.prefix",
        ],
        "capabilities": [
            "Structured JSON ingestion",
            "Env-based AWS credentials",
        ],
    },
    "parquet_to_bigquery": {
        "name": "parquet_to_bigquery",
        "description": "Load Parquet files into BigQuery",
        "category": "file_to_warehouse",
        "source_type": "parquet",
        "destination_type": "bigquery",
        "yaml_path": "definitions/parquet_to_bigquery.yaml",
        "required_config": [
            "sources[0].config.path",
            "destinations[0].config.project",
            "destinations[0].config.dataset",
            "destinations[0].config.table",
        ],
        "capabilities": [
            "Zero-copy style Parquet → BQ schema",
            "Efficient columnar load path",
        ],
    },
}

def get_template(name: str) -> Dict[str, Any]:
    """Fetch template metadata by name."""
    try:
        return TEMPLATE_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(f"Template '{name}' not found") from exc

def load_template_yaml(name: str) -> str:
    """Load YAML content from definitions/ directory."""
    _ = get_template(name)

    # Support both .yaml and .yml
    yaml_file = None
    for ext in (".yaml", ".yml"):
        candidate = DEFINITIONS_DIR / f"{name}{ext}"
        if candidate.is_file():
            yaml_file = candidate
            break

    if yaml_file is None:
        raise FileNotFoundError(
            f"YAML definition for template '{name}' not found "
            f"(checked {name}.yaml and {name}.yml)"
        )

    return yaml_file.read_text(encoding="utf-8")
