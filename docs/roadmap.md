# Conduit Core Roadmap

This document outlines the planned features and direction for future Conduit Core versions.

## v1.0 - User Experience Polish (Current - 90% Complete)
*Status:* In Development

* ✅ Checkpoint/Resume system
* ✅ Connection validation for all connectors
* ✅ Dry-run mode
* ✅ Real-time progress bars
* ✅ Production-ready JSON connector
* ⏳ Schema inference integration 

## v1.2 - Data Quality & Schema Management 
*Focus:* Ensuring data integrity and handling schema changes gracefully.

* **Schema Evolution:**
    * Auto-detect schema changes (new/removed columns, type changes).
    * Configurable handling: `auto` (add nullable), `warn`, `fail`, `ignore`.
    * Automatic `ALTER TABLE` generation for supported databases.
* **Data Quality Checks:**
    * Define rules in YAML (Regex, Range, Not Null, Unique).
    * Configurable actions: `dlq` (send to Dead Letter Queue), `fail` pipeline, `warn`.
    * Support for custom validation functions.
* **Schema Validation:**
    * Pre-flight checks for type compatibility between source and destination.
    * Detection of missing or extra columns based on a target schema.
    * Ability to export inferred or defined schemas.

## v1.3 - Advanced Sync Strategies 
*Focus:* Supporting more complex incremental loading patterns beyond simple append.

* **Upsert Operations:** Efficiently INSERT new records and UPDATE existing ones based on a unique key (`mode: upsert`, `unique_key: [id]`).
* **Delete Detection:** Propagate deletes from source to destination (e.g., via `is_deleted` flag or tracking table).
* **Advanced Checkpointing:** Support for composite keys and multiple checkpoint columns.
* **Change Data Capture (CDC) - Initial Support:**
    * Timestamp-based CDC (`WHERE updated_at > last_watermark`).
    * High-watermark persistence and management.

## v1.4 - Integrations & Observability 
*Focus:* Making Conduit Core work seamlessly within the broader data ecosystem.

* **dbt Integration:**
    * Post-hook to trigger `dbt run` or `dbt build` after successful Conduit runs.
    * Pass context (e.g., tables updated) between Conduit and dbt.
* **Orchestrator Support:**
    * Official Airflow provider/operator.
    * Prefect task library integration.
    * Dagster asset integration.
* **Observability:**
    * Export core metrics (rows processed, duration, errors) to Prometheus.
    * Integration with Datadog or similar monitoring platforms.
    * Option for structured JSON logging (for CloudWatch, ELK stack, etc.).
* **Alerting:**
    * Basic webhook support on pipeline failure/success.
    * Potential Slack/Email notification options.

## v1.5 - More Connectors 
*Focus:* Expanding the range of supported systems. High-priority targets include:

* **Databases:** MySQL (Source + Dest), Azure SQL (Dest), MongoDB (Dest)
* **Warehouses:** Redshift (Dest)
* **Other:** REST APIs (Source), Kafka (Source), Google Sheets (Source + Dest)
* **Connector Framework:** Improvements to make third-party connector development easier.

## v2.0 - Enterprise & Scale 
*Focus:* Addressing larger deployments, real-time needs, and usability at scale.

* **Distributed Execution:** Scale processing across multiple workers/nodes.
* **Real-Time Streaming:** Ingest from Kafka/Kinesis with micro-batching.
* **Web UI & API:** A graphical interface for monitoring, managing, and configuring pipelines.
* **Multi-Tenancy / RBAC:** Support for multiple teams and users with permissions.
* **Advanced Features:** Auto-schema mapping, AI-driven suggestions, performance profiling.

*(This roadmap is subject to change based on community feedback and priorities.)*