# Conduit Core

**The dbt of data ingestion** — declarative, reliable, testable data pipelines in Python.

[![PyPI](https://img.shields.io/pypi/v/conduit-core)](https://pypi.org/project/conduit-core/)
[![Tests](https://img.shields.io/badge/tests-190%2B%20passing-brightgreen)](https://github.com/alexD1990/conduit-core)
[![License](https://img.shields.io/badge/license-BSL%201.1-blue)](LICENSE)

## Install

```bash
pip install conduit-core
```

## Quick Start with Templates

```bash
# List all available templates
conduit template list

# Generate a CSV → Snowflake pipeline
conduit template csv_to_snowflake > pipeline.yml

# Edit pipeline.yml (update marked fields)

# Run your pipeline
conduit run pipeline.yml
```

That's it.

## Features

- **8 Connectors** — CSV, JSON, Parquet, S3, PostgreSQL, MySQL, Snowflake, BigQuery
- **Template System** — Zero-config YAML generation for all connector combinations
- **Schema Management** — Auto-inference, validation, evolution, drift detection
- **Atomic Operations** — All-or-nothing writes with automatic rollback
- **Checkpoint/Resume** — Pick up where you left off after failures
- **Dead Letter Queue** — Quarantine bad records without stopping pipelines
- **Quality Checks** — Validate data with not_null, regex, range, custom rules
- **Incremental Loading** — Sync only new/changed records
- **Environment Variables** — Secure credential management via `${VAR}` in YAML
- **Progress Tracking** — Real-time progress bars and batch statistics
- **Audit Trail** — Complete run history with manifest files
- **Preflight Validation** — Test connections and schemas before running
- **Retry Logic** — Automatic retries with exponential backoff
- **Type Coercion** — Smart type conversion between sources and destinations
- **190+ Tests** — Comprehensive test coverage with real cloud services

## CLI Commands

```bash
conduit template list                    # List all templates
conduit template info <name>             # Show template details
conduit template <name>                  # Generate template YAML
conduit run <pipeline.yml>               # Run a pipeline
conduit manifest --last                  # Show last run details
```

## Example: PostgreSQL → Snowflake

```bash
# Generate template
conduit template postgresql_to_snowflake > pipeline.yml

# Configure credentials in .env
echo "PG_PASSWORD=xxx" >> .env
echo "SNOWFLAKE_PASSWORD=yyy" >> .env

# Run
conduit run pipeline.yml
```

## Documentation

- **[Quick Start Guide](docs/QUICK_START.md)** — Common patterns and workflows
- **[Connector Reference](docs/CONNECTORS.md)** — Configuration for all 8 connectors

## License

Business Source License 1.1 (converts to Apache 2.0 on January 1, 2030)

## Links

- **PyPI:** https://pypi.org/project/conduit-core/
- **GitHub:** https://github.com/alexD1990/conduit-core
- **Issues:** https://github.com/alexD1990/conduit-core/issues
