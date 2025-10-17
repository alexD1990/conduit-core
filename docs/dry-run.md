# Dry-Run Mode

Dry-run mode (`conduit run --dry-run`) allows you to preview a pipeline's execution without making any actual changes to your data or systems. It's an invaluable tool for testing configurations, estimating workload, and debugging safely.

## Usage

Simply add the `--dry-run` flag to your `run` command:

```bash
conduit run --file ingest.yml --dry-run
```

## Example Output
The output clearly indicates that dry-run mode is active and simulates the steps:
```bash
 DRY RUN MODE - No data will be written

 Conduit Core
 Data ingestion pipeline starting...

14:30:00 START resource users_pipeline
14:30:00 ⚠ DRY RUN MODE - Data will be read but not written
14:30:00 → Source: csv (users_csv)
14:30:00 → Destination: bigquery (warehouse)
Processing users_pipeline: |████████████████████| 2234/2234 • 0:03 • 698 rows/sec
14:30:03 [DRY RUN] Would write 1000 records (batch 1)
14:30:03 [DRY RUN] Would write 1000 records (batch 2)
14:30:03 [DRY RUN] Would write 234 records (batch 3)
14:30:03 [DRY RUN] Skipping destination.finalize()
14:30:03 [DRY RUN] Would update state: id=2234
14:30:03 [DRY RUN] Would clear checkpoint on successful completion.
14:30:03 [DRY RUN] DONE resource users_pipeline [in 3.2s]
14:30:03      → 2234 would be written

────────────────────────────────────────────────────────────────────────────────
 Completed successfully! Ran 1 resource(s) in 3.25s
```

## What Dry-Run Does vs. Does Not Do

| Action | Normal Run | Dry Run |
| :--- | :---: | :---: |
| Read source data | ✅ | ✅ |
| Validate configuration | ✅ | ✅ |
| Test connections (implicit) | ✅ | ✅ |
| Process data in batches | ✅ | ✅ |
| Show progress bar/logging | ✅ | ✅ |
| Count records processed | ✅ | ✅ |
| Write to destination | ✅ | ❌ |
| Call destination.finalize() | ✅ | ❌ |
| Create/Overwrite files | ✅ | ❌ |
| Modify database tables | ✅ | ❌ |
| Update state file | ✅ | ❌ |
| Save/Clear checkpoints | ✅ | ❌ |

## Use Cases for Dry-Run
* **Test new pipelines:** Verify configuration and logic before running on production data.
* **Estimate record counts:** See how many records a source query will return.
* **Check performance:** Get an idea of read speed and processing time without write overhead.
* **Validate configurations safely:** Ensure connections work and parameters are correct.
* **Debug issues:** Trace data flow and identify potential problems without side effects.

