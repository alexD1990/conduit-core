# Conduit Core Roadmap

This document outlines completed features and the vision for future releases. Conduit Core follows semantic versioning and prioritizes stability, reliability, and developer experience.

---

## Version Overview

| Version    | Focus                                          | Status              | Target Date |
|------------|------------------------------------------------|---------------------|-------------|
| **v1.0**   | Core Stability & Data Integrity                | ✅ Released         | 2025-10-31  |
| **v1.1**   | Bug Fixes & Simple UI                          | 🚧 In Development   | 2025-Q1     |
| **v1.2**   | Incremental Sync & CDC                         | 📋 Planned          | 2025-Q2     |
| **v1.3**   | Enterprise & Observability                     | 📋 Planned          | 2025-Q3     |
| **v2.0**   | Distributed Scale & Real-Time                  | 🔮 Future Vision    | 2026+       |

---

## v1.0 — Core Stability & Data Integrity ✅

**Status:** Released (Current)  
**Focus:** Reliability, schema intelligence, and pre-flight validation

### Key Features Delivered

#### Schema Intelligence
- **Schema Validation:** Pre-flight compatibility checks between source and destination schemas
- **Schema Evolution:** Detects schema drift with configurable strategies (`auto`/`manual`/`warn`)
- **Schema Compare CLI:** `conduit schema-compare` for drift detection
- **Schema Inference:** Automatic type detection from sample data

#### Data Quality Framework
Define column-level validation rules directly in YAML:
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

**Built-in Checks:** `not_null`, `unique`, `regex`, `range`, `enum`  
**Actions:** `fail` (halt), `warn` (log), `dlq` (dead letter queue)

#### Reliability Features
- **Atomic Writes:** Temp file → rename pattern for files; transactions for databases
- **Retry Logic:** Exponential backoff for transient failures
- **Dead Letter Queue (DLQ):** Failed records isolated with full context
- **Checkpoint/Resume:** Fault-tolerant execution for long-running jobs
- **Batch Processing:** Memory-efficient streaming (configurable batch size)

#### Developer Experience
- **Enhanced CLI Suite:**
  - `conduit validate` — Pre-flight validation
  - `conduit preflight` — Health checks without execution
  - `conduit schema` — Infer and export schemas
  - `conduit manifest` — View run history with unique run IDs
- **Rich Progress Bars:** Real-time throughput, ETA, and batch tracking
- **Dry-Run Mode:** Preview pipelines without writes
- **Pipeline Manifest:** Complete audit trail (JSON)

#### Connectors (7 total)
**Sources:** CSV, JSON, Parquet, S3, PostgreSQL  
**Destinations:** CSV, JSON, Parquet, S3, PostgreSQL, Snowflake, BigQuery  
**Testing:** DummySource, DummyDestination

---

## v1.1 — Bug Fixes & Simple UI 🚧

**Status:** In Development  
**Focus:** Stabilization and basic web interface  
**Target:** Q1 2025

### Primary Goals

#### 1. Bug Fixes & Stability
Address issues discovered in v1.0 production usage:

**High Priority:**
- Fix edge cases in CSV delimiter detection
- Improve S3 connection retry logic
- Resolve schema evolution type mapping issues
- Handle malformed JSON records gracefully
- Fix checkpoint recovery for interrupted pipelines
- Improve error messages for configuration validation

**Medium Priority:**
- Optimize memory usage for large batch processing
- Improve concurrent pipeline execution
- Better handling of timezone-aware timestamps
- Enhanced null value normalization across connectors
- Fix DLQ file rotation issues

**Low Priority:**
- Minor UI polish in CLI output
- Documentation corrections
- Test coverage improvements

#### 2. Simple Web UI (Beta)

**Goal:** Provide a lightweight web interface for pipeline management and monitoring

**Core Features:**
```
┌─────────────────────────────────────────────────────────────┐
│                    Conduit Core UI                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Dashboard                                                  │
│  ├─ Active Pipelines (status, progress)                     │
│  ├─ Recent Runs (last 10, with status badges)               │
│  ├─ System Health (connector status)                        │
│  └─ Quick Stats (records processed, success rate)           │
│                                                             │
│  Pipeline Management                                        │
│  ├─ List Pipelines (from ingest.yml)                        │
│  ├─ Run Pipeline (trigger execution)                        │
│  ├─ View Configuration (YAML viewer)                        │
│  └─ View Manifest (run history)                             │
│                                                             │
│  Monitoring                                                 │
│  ├─ Real-time Progress (live updates)                       │
│  ├─ Error Logs (DLQ viewer)                                 │
│  ├─ Schema Drift Alerts                                     │
│  └─ Quality Check Results                                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Technical Stack:**
- **Backend:** FastAPI (Python, minimal dependencies)
- **Frontend:** HTMX + Alpine.js (no build step, simple deployment)
- **Styling:** Tailwind CSS
- **Database:** SQLite (manifest + run history)
- **Authentication:** Optional basic auth (for v1.1)

**Installation:**
```bash
# Install with UI support
pip install conduit-core[ui]

# Start web server
conduit ui start --port 8080

# Open browser
http://localhost:8080
```

**Features:**
- ✅ View pipeline configurations
- ✅ Trigger pipeline runs from UI
- ✅ Real-time progress tracking
- ✅ View run history (manifest)
- ✅ Inspect DLQ errors
- ✅ Schema drift notifications
- ⚠️ **Not included in v1.1:** Pipeline editing, multi-user, RBAC, cloud hosting

**Design Principles:**
- **Lightweight:** Single-file deployment, minimal dependencies
- **Local-first:** Runs alongside CLI on local machine
- **Read-mostly:** Primarily for monitoring, not configuration
- **No lock-in:** CLI remains primary interface

#### 3. Documentation Improvements
- Add troubleshooting guides for common errors
- Video tutorials for getting started
- More YAML configuration examples
- Connector-specific setup guides
- FAQ section

#### 4. Performance Optimizations
- Optimize schema inference for large datasets
- Reduce memory footprint for batch processing
- Improve startup time
- Parallel schema validation

### Non-Goals for v1.1
❌ New connectors (unless explicitly requested by users)  
❌ Advanced UI features (editing, orchestration)  
❌ Breaking API changes  
❌ Distributed execution  
❌ Cloud hosting

---

## v1.2 — Incremental Sync & CDC 📋

**Status:** Planned  
**Focus:** Change Data Capture, upsert operations, and enhanced monitoring  
**Target:** Q2 2025

### Incremental Loading Enhancements

#### 1. Advanced Incremental Strategies
**Current (v1.0):** Basic high-water mark tracking
```yaml
resources:
  - name: orders
    incremental_column: updated_at
```

**New (v1.2):** Enhanced strategies with lookback and gap detection
```yaml
resources:
  - name: orders
    incremental:
      column: updated_at
      strategy: timestamp          # or: sequential, cursor
      lookback_seconds: 3600       # Reprocess last hour
      detect_gaps: true            # Warn on missing values
      initial_value: "2025-01-01"  # Bootstrap value
```

**Strategies:**
- `timestamp`: Datetime-based (with optional lookback)
- `sequential`: Integer-based (with gap detection)
- `cursor`: Opaque cursor tokens (API sources)

#### 2. Upsert Mode (MERGE)
**Current:** `append` or `full_refresh`  
**New:** `upsert` for incremental updates

```yaml
destinations:
  - name: warehouse
    type: postgresql
    table: users
    mode: upsert
    unique_key: [id]              # Define merge key
    update_columns: [name, email] # Columns to update (optional)
```

**Behavior:**
```sql
-- Generated SQL (PostgreSQL)
INSERT INTO users (id, name, email, updated_at)
VALUES (...)
ON CONFLICT (id) 
DO UPDATE SET 
  name = EXCLUDED.name,
  email = EXCLUDED.email,
  updated_at = EXCLUDED.updated_at;
```

**Supported Connectors:**
- PostgreSQL (ON CONFLICT)
- Snowflake (MERGE)
- BigQuery (MERGE)

#### 3. Delete Propagation
Track and propagate deletions from source:
```yaml
resources:
  - name: users
    incremental:
      column: updated_at
      detect_deletes: true         # Track deleted records
      delete_strategy: soft        # soft (flag) or hard (DELETE)
```

**Soft Delete:**
```sql
UPDATE users SET _deleted = true WHERE id IN (...)
```

**Hard Delete:**
```sql
DELETE FROM users WHERE id IN (...)
```

#### 4. Change Data Capture (CDC)
**Goal:** Native CDC support for databases

**Example (PostgreSQL Logical Replication):**
```yaml
sources:
  - name: postgres_cdc
    type: postgresql
    host: localhost
    database: mydb
    cdc_enabled: true
    replication_slot: conduit_slot
    publication: conduit_pub
```

**Captured Events:**
- `INSERT` — New records
- `UPDATE` — Changed records
- `DELETE` — Removed records

**Challenges:**
- Requires database-specific setup (permissions, slots)
- Complex state management
- Schema change handling

**Priority:** Lower (depends on user demand)

### Extended Observability

#### 1. Metrics Export
Expose pipeline metrics to monitoring systems:
```yaml
observability:
  metrics:
    enabled: true
    format: prometheus       # or: json, statsd
    port: 9090
    endpoint: /metrics
```

**Metrics:**
- `conduit_records_processed_total`
- `conduit_records_failed_total`
- `conduit_pipeline_duration_seconds`
- `conduit_batch_size_bytes`

#### 2. Alerting Framework
```yaml
observability:
  alerts:
    - type: slack
      webhook_url: ${SLACK_WEBHOOK}
      events: [failed, schema_drift]
    
    - type: email
      smtp_host: smtp.gmail.com
      recipients: [team@example.com]
      events: [failed]
```

#### 3. Enhanced Manifest
Richer metadata in pipeline manifest:
- Source/destination latency
- Record sampling (first/last records)
- Column-level statistics (null counts, value distributions)
- Performance profiling (slowest operations)

### dbt Integration (Exploration)

**Goal:** Trigger dbt after successful Conduit runs

**Example:**
```yaml
resources:
  - name: orders
    post_hooks:
      - type: dbt
        command: "dbt run --models orders"
        on_success: true
```

**Challenges:**
- dbt requires separate configuration
- Environment management
- Error handling across tools

---

## v1.3 — Enterprise & Observability 📋

**Status:** Planned  
**Focus:** Orchestration integrations, team collaboration, advanced monitoring  
**Target:** Q3 2025

### Orchestration Integrations

#### 1. Native Operators
**Airflow Operator:**
```python
from conduit_core.integrations.airflow import ConduitOperator

run_orders = ConduitOperator(
    task_id='ingest_orders',
    config_file='/path/to/ingest.yml',
    resource='orders_to_warehouse',
    dry_run=False
)
```

**Prefect Task:**
```python
from conduit_core.integrations.prefect import run_conduit_pipeline

@task
def ingest_data():
    run_conduit_pipeline(
        config='ingest.yml',
        resource='users'
    )
```

**Dagster Op:**
```python
from conduit_core.integrations.dagster import conduit_op

@conduit_op(config='ingest.yml', resource='events')
def ingest_events():
    pass
```

#### 2. Workflow Orchestration
**Conduit-native DAGs:**
```yaml
workflows:
  - name: daily_etl
    schedule: "0 2 * * *"  # 2 AM daily
    steps:
      - resource: raw_orders
      - resource: enriched_orders
        depends_on: [raw_orders]
      - resource: aggregated_metrics
        depends_on: [enriched_orders]
```

### Advanced UI Features

#### 1. Multi-User Support
- User authentication (OAuth, SSO)
- Role-based access control (RBAC)
- Team workspaces
- Audit logs

#### 2. Pipeline Editing
- Visual YAML editor
- Configuration templates
- Schema mapping UI
- Connector configuration wizard

#### 3. Collaboration
- Shared pipeline library
- Comments on runs
- Change history
- Approval workflows

### Connector SDK

**Goal:** Make it easier for third parties to build connectors

**Example:**
```python
from conduit_core.sdk import BaseSource, register_connector

@register_connector('stripe')
class StripeSource(BaseSource):
    """Stripe API source connector"""
    
    def read(self, query: str = None):
        # SDK handles retry, auth, pagination
        for record in self.sdk.paginate('charges'):
            yield record
```

**SDK Features:**
- Built-in retry logic
- Automatic pagination
- Common authentication patterns
- Schema inference helpers
- Test utilities

---

## v1.4 — Connector Expansion (On-Demand) 📋

**Status:** Future (User-Driven)  
**Focus:** Add connectors based on community requests  
**Target:** 2025-Q4

### Potential Connectors

**Databases:**
- MySQL (source + destination)
- Azure SQL (source + destination)
- MongoDB (source)
- Cassandra (destination)
- DynamoDB (destination)

**Data Warehouses:**
- Redshift (destination)
- ClickHouse (destination)
- Databricks (destination)

**Cloud Storage:**
- Azure Blob Storage (source + destination)
- Google Cloud Storage (source + destination)

**Streaming:**
- Apache Kafka (source)
- AWS Kinesis (source)
- Pub/Sub (source)

**APIs & SaaS:**
- REST API (source, generic)
- Salesforce (source)
- HubSpot (source)
- Google Sheets (source + destination)
- Stripe (source)

**File Formats:**
- Avro (source + destination)
- ORC (source + destination)
- Excel (source)

### Connector Prioritization

**Criteria:**
1. User requests (GitHub issues, discussions)
2. Use case frequency
3. Implementation complexity
4. Maintenance burden

**Community Process:**
- Users request connectors via GitHub issues
- Vote on priorities (👍 reactions)
- Maintainers evaluate feasibility
- Community contributions welcome

---

## v2.0 — Distributed Scale & Real-Time 🔮

**Status:** Future Vision  
**Focus:** Horizontal scalability, real-time streaming, enterprise platform  
**Target:** 2026+

### Distributed Execution

**Goal:** Parallelize pipelines across multiple nodes

**Architecture:**
```
┌─────────────────────────────────────────────────────────┐
│                 Conduit Coordinator                     │
│  • Job scheduling                                       │
│  • Resource allocation                                  │
│  • Failure recovery                                     │
└─────────────────────────────────────────────────────────┘
           ↓                ↓                ↓
    ┌──────────┐      ┌──────────┐      ┌──────────┐
    │ Worker 1 │      │ Worker 2 │      │ Worker N │
    │ (Task A) │      │ (Task B) │      │ (Task C) │
    └──────────┘      └──────────┘      └──────────┘
```

**Features:**
- Horizontal scaling (add workers)
- Automatic load balancing
- Centralized state management
- Fault-tolerant execution

### Real-Time Streaming Mode

**Goal:** Micro-batch ingestion from streaming sources

**Example:**
```yaml
sources:
  - name: kafka_events
    type: kafka
    topics: [orders, clicks]
    consumer_group: conduit
    
resources:
  - name: stream_processing
    source: kafka_events
    destination: warehouse
    mode: streaming
    window_seconds: 5        # 5-second micro-batches
    checkpoint_interval: 10  # Checkpoint every 10 batches
```

**Challenges:**
- Exactly-once semantics
- Watermarking & late data
- Stateful processing
- Backpressure handling

### Conduit Cloud (SaaS Platform)

**Vision:** Managed Conduit hosting with enterprise features

**Features:**
- **Web UI:** Full-featured pipeline management
- **Multi-tenancy:** Isolated workspaces per team
- **Managed Infrastructure:** No deployment required
- **RBAC:** Fine-grained permissions
- **Usage Analytics:** Cost tracking, performance insights
- **Integrations:** Native connections to cloud services
- **SLA:** 99.9% uptime guarantee

**Pricing Model:**
- Free tier (limited pipelines)
- Pro tier (advanced features)
- Enterprise (custom, on-premise option)

### Performance Profiling

**Goal:** Automatic performance optimization

**Features:**
- Runtime profiling (identify bottlenecks)
- Auto-tuning (suggest optimal batch sizes)
- Query optimization (rewrite inefficient queries)
- Resource recommendations (CPU, memory)

---

## How to Influence the Roadmap

### Community Feedback

**We listen to:**
- GitHub Issues (bug reports, feature requests)
- GitHub Discussions (ideas, questions)
- Production usage patterns (telemetry, if opted-in)
- Direct user interviews

### Request a Feature

1. **Search existing issues** (avoid duplicates)
2. **Create feature request** with:
   - Use case description
   - Expected behavior
   - Example configuration (if applicable)
3. **Engage with community** (discuss, refine)
4. **Upvote** (👍) features you want

### Contribute

- See [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines
- Pick issues labeled `good first issue`
- Build connectors for your use cases
- Share your success stories

---

## Versioning Policy

### Semantic Versioning (SemVer)

```
MAJOR.MINOR.PATCH

1.0.0 → 1.1.0 → 1.1.1 → 2.0.0
```

- **MAJOR (v2.0):** Breaking changes, API updates
- **MINOR (v1.1, v1.2):** New features, backward compatible
- **PATCH (v1.0.1):** Bug fixes only

### Compatibility Guarantee

**Within v1.x:**
- Configuration files (YAML) remain compatible
- CLI commands maintain backward compatibility
- Connector interfaces stable (no breaking changes)

**Upgrade path:**
- `v1.0` → `v1.1` → `v1.2` → `v1.3` (seamless)
- `v1.x` → `v2.0` (migration guide provided)

### Deprecation Policy

- Features marked deprecated in `v1.x`
- Removed in `v2.0`
- At least 6 months notice
- Migration guide provided

---

## License Transition

**Current:** Business Source License 1.1 (BSL 1.1)  
**Future:** Apache 2.0 (automatically on **January 1, 2030**)

**What this means:**
- Full open-source after 2030
- Use in production freely (non-competing use)
- Contribute without restrictions

---

## Summary

| Version | Key Focus                          | Timeline   |
|---------|------------------------------------|------------|
| v1.0    | ✅ Core features delivered          | Released   |
| v1.1    | 🚧 Bug fixes + Simple UI            | Q1 2025    |
| v1.2    | 📋 Incremental sync + CDC           | Q2 2025    |
| v1.3    | 📋 Enterprise + Orchestration       | Q3 2025    |
| v1.4    | 📋 User-requested connectors        | Q4 2025    |
| v2.0    | 🔮 Distributed + Streaming + Cloud  | 2026+      |

---

**Questions about the roadmap?** Open a GitHub Discussion.  
**Want to see a specific feature?** Create a feature request issue.

*This roadmap is a living document and subject to change based on community feedback and priorities.*

























