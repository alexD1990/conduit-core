# Pipeline Manifest (Audit Trail)

Conduit Core automatically records a detailed history of every pipeline execution in a file named `manifest.json` (by default, in the directory where you run `conduit`). This manifest serves as a crucial audit trail and lineage tracker.

## Manifest Contents

Each entry in the `runs` array of `manifest.json` contains the following information for a single resource execution:

* `pipeline_name`: The name of the resource from `ingest.yml`.
* `source_type`: The type of the source connector (e.g., `csv`, `postgresql`).
* `destination_type`: The type of the destination connector (e.g., `s3`, `snowflake`).
* `started_at`: ISO timestamp when the resource started processing.
* `completed_at`: ISO timestamp when the resource finished processing.
* `status`: Final status (`success`, `failed`, `partial`).
    * `success`: All records processed without errors.
    * `failed`: A critical error occurred, stopping the pipeline.
    * `partial`: The pipeline completed, but some records failed (logged to DLQ).
* `records_read`: Total number of records read from the source.
* `records_written`: Total number of records successfully written to the destination (always 0 in dry-run).
* `records_failed`: Total number of records that failed processing (sent to DLQ).
* `duration_seconds`: Total execution time in seconds.
* `error_message`: The error message if the status is `failed`.
* `metadata`: Additional context, currently includes the `mode` (`append` or `full_refresh`).

## CLI Command: `conduit manifest`

You can easily view and query the execution history using the CLI:

```bash
# Show the last 20 runs for all pipelines
conduit manifest

# Show history only for a specific pipeline
conduit manifest --pipeline-name users_to_snowflake

# Show only failed runs
conduit manifest --failed

# Use a different manifest file location
conduit manifest --manifest-path /path/to/shared/manifest.json
```
| Pipeline | Status | Records | Failed | Duration | Completed |
| :--- | :--- | ---: | ---: | ---: | ---: |
| users_pipeline | ✅ success | 1000 | 0 | 12.5s | 2025-10-18 |
| orders_pipeline | ⚠️ partial | 5000 | 15 | 45.1s | 2025-10-18 |
| failed_run | ❌ failed | 500 | 500 | 5.2s | 2025-10-17 |