[![PyPI version](https://img.shields.io/pypi/v/conduit-core.svg)](https://pypi.org/project/conduit-core/)
[![Python versions](https://img.shields.io/pypi/pyversions/conduit-core.svg)](https://pypi.org/project/conduit-core/)
[![Downloads](https://img.shields.io/pypi/dm/conduit-core.svg)](https://pypi.org/project/conduit-core/)

# Conduit Core  
![Python](https://img.shields.io/badge/python-3.9+-blue.svg) ![License](https://img.shields.io/badge/license-BSL%201.1-blue.svg) ![Tests](https://img.shields.io/badge/tests-22%20passing-brightgreen.svg) ![Coverage](https://img.shields.io/badge/coverage-85%25-green.svg) ![Code Style](https://img.shields.io/badge/code%20style-black-black.svg)

**Declarative data ingestion framework for Python — inspired by dbt's approach to analytics engineering.**

Conduit Core is an open-source Python framework for building production-grade data pipelines. Move data between CSV, JSON, Parquet, PostgreSQL, MySQL, S3, Snowflake, and BigQuery with YAML configuration, automatic retries, checkpoints, and comprehensive testing.

## Features

- **Declarative YAML Configuration** — Define sources, destinations, and transformations without writing Python
- **8 Connectors** — CSV, JSON, Parquet, S3, PostgreSQL, MySQL, Snowflake, BigQuery
- **Production-Ready Reliability** — Atomic operations, automatic retries, dead letter queue, checkpoint/resume
- **Schema Management** — Auto-inference, validation, evolution tracking, and quality checks
- **Developer Experience** — Rich progress bars, dry-run mode, audit trail, and comprehensive testing (141+ tests)

## Quick Start

```bash
pip install conduit-core
```

Create `ingest.yml`:

```yaml
sources:
  - name: users_csv
    type: csv
    path: "./users.csv"

destinations:
  - name: users_json
    type: json
    path: "./users.json"

resources:
  - name: csv_to_json
    source: users_csv
    destination: users_json
```

Run:

```bash
conduit run csv_to_json
```

## Documentation

- **[Installation & Setup](docs/installation.md)** — Installation, environment setup, and credentials
- **[Usage Guide](docs/usage.md)** — CLI commands, pipeline configuration, and examples
- **[Connectors](docs/connectors.md)** — Configuration for all 8 connectors
- **[Features](docs/features.md)** — Reliability, checkpoints, DLQ, incremental loads
- **[Architecture](docs/architecture.md)** — Design patterns, tech stack, and structure
- **[Data Quality](docs/data-quality.md)** — Schema validation and quality checks
- **[Contributing](docs/contributing.md)** — Development setup and contribution guidelines
- **[Roadmap](docs/roadmap.md)** — Future features and connector expansion

## Example: Incremental PostgreSQL → Snowflake

```yaml
sources:
  - name: pg_source
    type: postgresql
    host: ${PG_HOST}
    database: analytics
    user: ${PG_USER}
    password: ${PG_PASSWORD}

destinations:
  - name: sf_dest
    type: snowflake
    account: ${SF_ACCOUNT}
    warehouse: COMPUTE_WH
    database: ANALYTICS
    schema: PUBLIC

resources:
  - name: incremental_load
    source: pg_source
    destination: sf_dest
    query: "SELECT * FROM events"
    incremental_column: created_at
    write_mode: append
```

Run with validation:

```bash
conduit preflight incremental_load
conduit run incremental_load
```

## Why Conduit Core?

Built for data engineers who need the simplicity of declarative configuration with the reliability of enterprise-grade systems:

- ✅ **No boilerplate** — YAML instead of Python for 90% of use cases
- ✅ **Crash-safe** — Atomic writes, transactions, and checkpoint/resume
- ✅ **Observable** — Audit trail, progress bars, and detailed error messages
- ✅ **Extensible** — Plugin architecture for custom connectors
- ✅ **Battle-tested** — 141+ tests covering edge cases and failure modes

## Community

- **Issues & Bugs**: [GitHub Issues](https://github.com/conduit-core/conduit-core/issues)
- **Feature Requests**: [GitHub Discussions](https://github.com/conduit-core/conduit-core/discussions)
- **Contributing**: See [CONTRIBUTING.md](docs/contributing.md)

## License

BSL 1.1 (Business Source License) — See [LICENSE](LICENSE) for details
