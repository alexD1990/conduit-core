# Conduit Core Architecture

Conduit Core is designed with modularity, reliability, and developer experience in mind.

## Design Patterns

* **Strategy Pattern:** `BaseSource` and `BaseDestination` define common interfaces (contracts) implemented by specific connectors (strategies).
* **Factory Pattern:** The connector `registry` dynamically discovers and provides instances of connectors based on configuration type.
* **Context Manager:** `ManifestTracker` uses Python's `with` statement to automatically handle start/end timing and status updates for the audit trail.
* **Atomic Write:** File destinations use a temporary file → rename pattern to ensure writes are crash-safe. Database destinations use transactions.
* **Retry Pattern:** A decorator (`@retry_with_backoff`) provides configurable exponential backoff for network-dependent operations.
* **Batch Processing:** Data is processed in configurable batches using Python generators for memory efficiency.

## Tech Stack

* **Core:** Python 3.12+, Poetry, Pydantic (validation), Typer + Rich (CLI)
* **Testing:** PyTest
* **Connectors:** boto3 (S3), psycopg2 (PostgreSQL), snowflake-connector-python, google-cloud-bigquery, pyarrow (Parquet)

## File Structure

## Project Structure

* `conduit-core/`
    * `src/conduit_core/`
        * `connectors/` # All data connectors
            * `base.py` # Abstract base classes
            * `csv.py`
            * `json.py`
            * `s3.py`
            * `postgresql.py`
            * `snowflake.py`
            * `parquet.py`
            * `bigquery.py`
            * `registry.py` # Auto-Discovery
            * `utils/`
                * `retry.py` # Exponential backoff decorator
        * `engine.py` # Core pipeline orchestration logic
        * `checkpoint.py` # Checkpoint/Resume system
        * `manifest.py` # Audit trail / History logging
        * `config.py` # Pydantic models for YAML validation
        * `cli.py` # Typer CLI commands
        * `batch.py` # Batch processing utilities
        * `errors.py` # DLQ (ErrorLog) and custom exceptions
        * `state.py` # Incremental state management
        * `logging_utils.py` # Rich logging and progress bar logic
        * `tests/` # 141+ tests
            * `connectors/` # Per-connector tests
            * `integration/` # E2E tests
            * `test_*.py` # Unit tests
    * `pyproject.toml` # Poetry config
    * `README.md` # Main project overview
    * `LICENSE` # Project license
    * `.gitignore` # Files ignored by Git