# Checkpoints & Resume

Conduit Core's checkpoint system allows long-running pipelines to automatically resume from their last successful point after a failure. This is crucial for large datasets where restarting from scratch is inefficient.

## How it Works

1.  **Enable:** Set `resume: true` and specify a `checkpoint_column` in your `source` configuration. The chosen column must be monotonically increasing (e.g., an auto-incrementing ID or reliable `updated_at`).
2.  **Track Progress:** As the pipeline runs, the engine keeps track of the maximum value seen in the `checkpoint_column` for each batch of records processed.
3.  **Save Periodically:** After each batch is successfully *written* to the destination, the engine saves a small checkpoint file (e.g., `.checkpoints/my_pipeline.json`) containing the pipeline name, the checkpoint column, the last successfully processed value, and the total records processed so far. This save operation uses an atomic write pattern.
4.  **Resume on Restart:** If a pipeline run fails (e.g., due to a network error, database issue), the checkpoint file remains. When you run `conduit run` again, the engine detects the checkpoint file, reads the `last_value`, and automatically filters to records after that value (e.g., appending `WHERE id > 12345`).
5.  **Auto-Cleanup:** If a pipeline completes successfully from start to finish, the corresponding checkpoint file is automatically deleted. If a checkpoint file remains after a run, it indicates the previous run did not complete successfully.
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
    db_schema: public
    # --- Checkpoint Configuration ---
    resume: true                # Enable checkpoint/resume
    checkpoint_column: event_timestamp # Column to track progress
```
The engine automatically adds `WHERE <checkpoint_column> > <last_value>` (or an `AND` if a `WHERE` already exists) at runtime.

**Resource Query:** Your ```query``` in the ```resources``` section should be written to handle the checkpoint. Conduit Core will automatically append a ```WHERE``` clause if one doesn't exist, or an ```AND``` clause if it does. It assumes a simple ```>``` comparison.
```yaml
resources:
  - name: incremental_events
    source: large_database_table
    destination: data_lake_s3
    # Initial query might look like this:
    query: "SELECT id, event_type, payload, event_timestamp FROM events ORDER BY event_timestamp"
    # On resume, engine will effectively run:
    # query: "SELECT ... FROM events WHERE event_timestamp > '2025-10-18T10:30:00Z' ORDER BY event_timestamp"
```

## Supported Checkpoint Types
The system automatically detects the data type of the last_value being saved:
* **Integer:** e.g., ```id```, ```row_number```
* **String:** e.g., ```batch_id```, ```filename``` (lexicographical comparison)
* **Datetime:** e.g., ```updated_at```, ```created_at``` (ISO format string)
* **Float:** e.g., ```version```, ```score```

### Clearing checkpoints manually
Checkpoint files live under `.checkpoints/`. To force a full rerun, delete them:

```bash
rm -rf .checkpoints/
```

## Best Practices
* ✅ **Use monotonically increasing columns:** The ```checkpoint_column``` must reliably increase as you process data (e.g., auto-incrementing IDs, accurate timestamps).
* ✅ **Ensure data is ordered:** If reading from a database, add an ```ORDER BY``` clause on your ```checkpoint_column``` to ensure consistent processing order between runs.
* ✅ **Test resume logic:** Simulate failures during development with small datasets to verify resume works as expected.
* ✅ **Monitor checkpoint files:** Periodically check the ```.checkpoints/``` directory, especially after failures.
* ✅ Add `.checkpoints/` to `.gitignore` — they are runtime artifacts.
* ❌ **Avoid non-unique columns:** Don't use a column for checkpointing if its values might repeat or are not ordered.
* ❌ **Don't manually edit checkpoint files:** To reset, delete them using `rm -rf .checkpoints/`.



