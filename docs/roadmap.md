# Conduit Core Roadmap
This document outlines completed and planned features for Conduit Core.

## v1.0 — Core Stability & Data Integrity (Current)
Status: Released (Current)
Focus: Reliability, schema intelligence, and pre-flight validation

### Key Features Delivered

* **Schema Validation:**
Pre-flight compatibility checks between source and destination schemas.
Detects missing, extra, or type-mismatched columns before execution.

* **Schema Evolution:**
Detects schema drift (added/removed/changed columns).
Supports configurable strategies:

* `auto` → apply compatible changes (e.g., add nullable columns)
* `manual` → log diffs and generate SQL, do not apply automatically
* `warn` → log warnings, proceed without applying DDL

* **Data Quality Framework:**
Define column-level rules directly in YAML:
```yaml
quality_checks:
  - column: email
    check: regex
    pattern: "^[^@]+@[^@]+$"
    action: warn
  - column: id
    check: unique
    action: fail
```

Built-in checks: `not_null`, `unique`, `regex`, `range`, `in_list` (and related variants).
Configurable actions: `fail`, `warn`, `dlq`.

* **Enhanced CLI Suite:**
    * ```conduit validate``` → Pre-flight validation (config, schema, quality).
    * ```conduit schema-compare``` → Detect schema drift between runs.
    * ```conduit schema``` → Infer and export schema in JSON or YAML format.

* **Improved Observability:**
Expanded pipeline manifest with structured metadata.
Consistent exit codes for automation and CI/CD use.

## v1.2 — Incremental & Cloud Readiness

Focus: Incremental loading, CDC, deeper monitoring, and integration with orchestration tools.

### Planned Features

**Incremental & Upsert Operations:**
Support for ```mode: upsert``` with ```unique_key``` definitions.
```yaml
mode: upsert
unique_key: [id]
```

* **Delete Propagation:**
Optional detection and handling of deleted source records.

* **Change Data Capture (CDC):**
Timestamp and high-watermark-based incremental extraction.

* **Extended Observability:**
Export metrics (records processed, duration, throughput)
to Prometheus or structured JSON logs.

* **dbt Integration (Exploration):**
  - Optionally trigger `dbt build` after successful pipelines.
  - Pass updated tables context to dbt via manifest handoff.

## v1.3 — Enterprise & Observability

**Focus:** Interoperability, orchestration, and monitoring.

### Targets

* **Airflow** / **Prefect** / **Dagster Integrations**:
Native operators for orchestrator workflows.

* **Alerting**:
Configurable webhooks and Slack notifications for job success/failure.

* **Metrics Integration**:
Expose internal metrics to Datadog, Grafana, or OpenTelemetry.

* **Connector SDK Enhancements**:
Easier API for third-party connector developers.

## v1.4 – Connectors Expansion

**Focus:** Broaden connector coverage for databases, warehouses, and APIs.

### Planned Connectors

* **Databases:** MySQL (source+dest), Azure SQL, MongoDB
* **Warehouses:** Redshift, ClickHouse
* **Other Systems:** REST APIs (source), Kafka (source), Google Sheets

### Framework Enhancements

* Modular connector registration
* Pluggable authentication and retry strategies

## v2.0 – Enterprise & Scale

**Focus:** Scale, real-time workloads, and enterprise usability.

### Long-Term Goals

* **Distributed Execution Engine:**
Parallelize workloads across nodes with centralized orchestration.

* **Real-Time Streaming Mode:**
Micro-batched ingestion from Kafka/Kinesis with checkpointed commits.

* **Web UI & API Layer (Conduit Cloud):**

    * Monitor and manage pipelines visually

    * Multi-tenant project management

    * Team-level RBAC and usage analytics

* **Performance Profiling & Auto-Tuning:**
Runtime suggestions for optimal batch sizes and parallelism.

## Version Summary
| Version  | Focus                                      | Status         |
|----------|--------------------------------------------|----------------|
| **v1.0** | Core Stability & Data Integrity            | Current        |
| **v1.2** | Incremental & Cloud Readiness              | Planned        |
| **v1.3** | Enterprise & Observability                 | Planned        |
| **v1.4** | Connectors Expansion                       | Future Vision  |

---
Conduit Core follows semantic versioning. Minor versions (v1.x) add functionality without breaking changes.  
Major versions (v2.x) may include API updates.




























