# Error Handling & Dead Letter Queue (DLQ)

Conduit Core is designed for reliability. A key part of this is how it handles records that fail during processing. Instead of crashing the entire pipeline, individual failed records are sent to a Dead Letter Queue (DLQ) for later inspection, allowing the rest of the data to flow through.

## How it Works

1.  **Detection:** During the batch processing loop in the engine, each record is processed individually (or validated before batch operations). If an exception occurs while processing or writing a single record (e.g., data type mismatch, validation error, `write_one` failure), the engine catches it.
2.  **Logging:** The failed record, along with the error message, error type, original row number (if available), and a timestamp, is logged using the `ErrorLog` class. A warning is also printed to the console.
3.  **Continuation:** The engine **continues** processing the remaining records in the batch and subsequent batches.
4.  **Saving:** If *any* errors occurred during the entire resource run, the `ErrorLog` saves all collected errors to a timestamped JSON file in the `./errors/` directory (e.g., `errors/my_pipeline_errors_20251018_143000.json`).
5.  **Status:** If a pipeline completes but had record-level failures, its final status in the [Manifest](manifest.md) will be marked as `partial`.

## Error Log Format

The JSON file in the `./errors/` directory contains:

```json
{
  "resource": "pipeline_name",
  "total_errors": 15,
  "timestamp": "2025-10-18T14:30:45.123Z",
  "errors": [
    {
      "row_number": 101,
      "record": { "id": 101, "name": "Alice", "age": "thirty" },
      "error_type": "ValueError",
      "error_message": "invalid literal for int() with base 10: 'thirty'",
      "timestamp": "2025-10-18T14:29:55.456Z"
    },
    // ... more errors ...
  ]
}
```

## Benefits

* **Prevents Pipeline Stoppage:** Ensures that a few bad records don't halt the entire data flow for potentially millions of good records.
* **Data Loss Prevention:** Failed records aren't discarded; they are preserved for analysis and potential reprocessing.
* **Debugging:** Provides clear context (original record + error message) to understand why specific records failed.

## Limitations (v1.0)

* **Batch Failures:** If an entire batch write operation fails (e.g., ```destination.write()``` fails for a connector that doesn't support ```write_one```), all records in that batch might be logged as errors, or the pipeline might halt depending on the connector's implementation and retry logic. DLQ is most effective for record-level issues.
* **No Automatic Replay:** There is currently no built-in mechanism to automatically reprocess records from the DLQ files. This requires manual intervention or a separate process.
