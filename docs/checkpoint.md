# Checkpoint & Resume

Conduit Core's checkpoint system allows long-running pipelines to automatically resume from their last successful point after a failure. This is crucial for large datasets where restarting from scratch is inefficient.

## How it Works

1.  **Enable:** Set `resume: true` and specify a `checkpoint_column` in your `source` configuration. The chosen column **must** contain values that consistently increase as the pipeline progresses (e.g., a primary key `id`, a timestamp like `updated_at`).
2.  **Track Progress:** As the pipeline runs, the engine keeps track of the maximum value seen in the `checkpoint_column` for each batch of records processed.
3.  **Save Periodically:** After each batch is successfully *written* to the destination, the engine saves a small checkpoint file (e.g., `.checkpoints/my_pipeline.json`) containing the pipeline name, the checkpoint column, the last successfully processed value, and the total records processed so far. This save operation uses an atomic write pattern.
4.  **Resume on Restart:** If a pipeline run fails (e.g., due to a network error, database issue), the checkpoint file remains. When you run `conduit run` again, the engine detects the checkpoint file, reads the `last_value`, and automatically modifies the source query to fetch only records *after* that value (e.g., appending `WHERE id > 12345`).
5.  **Auto-Cleanup:** If a pipeline completes successfully from start to finish, the corresponding checkpoint file is automatically deleted.

## Configuration

Enable resume by adding these two parameters to a `source` in your `ingest.yml`:

```yaml
sources:
  - name: large_database_table
    type: postgresql
    database: production
    user: etl_user
    password: ${PG_PASSWORD}
    host: db.example.com
    schema: public
    # --- Checkpoint Configuration ---
    resume: true                # Enable checkpoint/resume
    checkpoint_column: event_timestamp # Column to track progress
```
**Resource Query:** Your ```query``` in the ```resources``` section should be written to handle the checkpoint. Conduit Core will automatically append a ```WHERE``` clause if one doesn't exist, or an ```AND``` clause if it does. It assumes a simple ```>``` comparison.
```yaml
resources:
  - name: incremental_events
    source: large_database_table
    destination: data_lake_s3
    # Initial query might look like this:
    query: "SELECT id, event_type, payload, event_timestamp FROM events ORDER BY event_timestamp"
    # On resume, engine will effectively run:
    # query: "SELECT ... FROM events ORDER BY event_timestamp WHERE event_timestamp > '2025-10-18T10:30:00Z'"
```
(**Note:** The ```:last_valu``` placeholder used for basic incremental loads is separate from the checkpoint/resume mechanism.)

## Supported Checkpoint Types
The system automatically detects the data type of the last_value being saved:
* **Integer:** e.g., ```id```, ```row_number```
* **String:** e.g., ```batch_id```, ```filename``` (lexicographical comparison)
* **Datetime:** e.g., ```updated_at```, ```created_at``` (ISO format string)
* **Float:** e.g., ```version```, ```score```

## CLI Commands
Manage checkpoints directly from the command line:
```bash
# View all currently saved checkpoints
conduit checkpoints

# Clear the checkpoint for a specific pipeline
conduit clear-checkpoints --pipeline-name my_failed_pipeline

# Clear all saved checkpoints
conduit clear-checkpoints
```

## Best Practices
* ✅ **Use monotonically increasing columns:** The ```checkpoint_column``` must reliably increase as you process data (e.g., auto-incrementing IDs, accurate timestamps).
* ✅ **Ensure data is ordered:** If reading from a database, add an ```ORDER BY``` clause on your ```checkpoint_column``` to ensure consistent processing order between runs.
* ✅ **Test resume logic:** Simulate failures during development with small datasets to verify resume works as expected.
* ✅ **Monitor checkpoint files:** Periodically check the ```.checkpoints/``` directory, especially after failures.
* ❌ **Avoid non-unique columns:** Don't use a column for checkpointing if its values might repeat or are not ordered.
* ❌ **Don't manually edit checkpoint files:** This can lead to data loss or corruption. ```Use conduit clear-checkpoints``` if you need to reset.


