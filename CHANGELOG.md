# Changelog

## [1.0.0] - 2025-10-31

### Added
- 8 connectors (CSV, JSON, Parquet, S3, PostgreSQL, MySQL, Snowflake, BigQuery)
- Schema inference, validation, evolution
- Quality checks framework
- Checkpoint/resume functionality
- Dead letter queue
- CLI with preflight validation
- 141+ test suite

## [1.1.0] - 2025-01-18

### Added
- Template system with 12 pre-built connector combinations
- CLI commands: `conduit template list/info/<name>`
- Zero-config YAML generation for all connector pairs
- Simplified documentation (README + QUICK_START + CONNECTORS)

### Changed
- README rewritten with template-first workflow
- Documentation reduced to essentials
- Archived advanced docs (contributing, roadmap, architecture)