# State Management & Incremental Loading

Conduit Core supports **incremental loading**, allowing you to process only *new* records from a source since the last successful run. This is essential for efficiency when dealing with large, frequently updated datasets.

## How it Works

1.  **Configuration:**
    * In your `source` definition, ensure you have a column that reliably indicates new data (e.g., an auto-incrementing `id`, a `created_at` timestamp).
    * In the `resource` definition using that source:
        * Set the `incremental_column` parameter to the name of that tracking column (e.g., `id`).
        * Write your source `query` using the special placeholder `:last_value`. This placeholder will be automatically replaced by the engine with the highest value seen for the `incremental_column` during the *previous* successful run.

2.  **Execution:**
    * The first time the resource runs, `:last_value` is replaced with `0` (or an appropriate default). All data matching the query is fetched.
    * During processing, the engine tracks the maximum value encountered in the specified `incremental_column` among all successfully processed records.
    * Upon successful completion, the engine saves this maximum value to a state file (default: `.conduit_state.json`), associated with the resource name.

3.  **Subsequent Runs:**
    * The next time the resource runs, the engine loads the saved state.
    * The `:last_value` placeholder in the query is replaced with the value loaded from the state file.
    * The source query now only fetches records *newer* than the last run (e.g., `WHERE id > 12345`).
    * The process repeats, updating the state file with the new maximum value upon success.

## Example `ingest.yml`

```yaml
sources:
  - name: production_db
    type: postgresql
    database: appdb
    user: readonly
    password: ${PG_PASSWORD}
    host: db.example.com
    schema: public

destinations:
  - name: data_warehouse
    type: snowflake
    # ... snowflake config ...
    table: raw_users
    mode: append # Important for incremental loads

resources:
  - name: sync_new_users
    source: production_db
    destination: data_warehouse
    incremental_column: id # Track the 'id' column
    query: "SELECT id, name, email, created_at FROM users WHERE id > :last_value ORDER BY id"
    # Mode is implicitly 'incremental' because incremental_column is set
```

## State File (```.conduit_state.json```)
The state file is a simple JSON mapping resource names to their last successfully processed value for the ```incremental_column```:
```json
{
  "sync_new_users": 15789,
  "sync_orders": 98765
}
```
## Important Considerations

* **Column Choice:** The ```incremental_column``` must be reliably increasing. Auto-incrementing primary keys or accurate ```created_at```/```updated_at``` timestamps are ideal.
* **Ordering:** For database sources, always include an ```ORDER BY``` clause on your ```incremental_column``` in the query to ensure consistent processing order.
* **Deletes/Updates:** This basic incremental loading only captures new records (based on the ```>``` comparison). It does not automatically handle records that were updated or deleted in the source.
* **vs. Checkpoints:** State management is for tracking the high-water mark between successful runs. Checkpoints are for resuming a single run after a failure. They can be used together.

