# Conduit Core CLI Commands

Conduit Core provides a rich command-line interface for managing and running your data pipelines.

## `conduit run`

Runs the data ingestion pipeline defined in your configuration file.

**Usage:**
```bash
conduit run [OPTIONS]
```

**Options:**
* ```--file TEXT```: Path to your ```ingest.yml``` configuration file. (Default: ```ingest.yml```)
* ```--batch-size INTEGER```: Number of records to process per batch. (Default: 1000)
* ```--dry-run```: Preview the pipeline run without writing any data. Ideal for testing.
* ```--no-progress```: Disable the real-time progress bars (useful for CI/CD logs).

**Example:**
```bash
# Run using default ingest.yml
conduit run

# Run a specific config in dry-run mode with smaller batches
conduit run --file production.yml --dry-run --batch-size 500
```
Tests the connections for all sources and destinations defined in your configuration file before running the pipeline. Fails fast with helpful error messages and suggestions if a connection is invalid.

**Usage:**
```bash
conduit test [OPTIONS]
```

**Options:**
* ```--file TEXT```: Path to the configuration file to test. (Default: ```ingest.yml```)

**Example:**
```bash
conduit test --file staging.yml
```

```conduit validate```

Validates the syntax and structure of your ```ingest.yml``` configuration file using Pydantic models. Catches typos and structural errors early.

**Usage:**
```bash
conduit validate [OPTIONS]
```

**Options:**

* ```--file TEXT```: Path to the configuration file to validate. (Default: ```ingest.yml```)

```conduit manifest```

Displays the history of pipeline executions recorded in the ```manifest.json``` file (audit trail).

**Usage:**
```bash
conduit manifest [OPTIONS]
```

**Options:**

* ```--pipeline-name TEXT:``` Filter history to show only runs for a specific pipeline (resource name).
* ```--failed:``` Show only failed or partially failed runs.
* ```--manifest-path TEXT:``` Specify the path to the manifest file. (Default: ```manifest.json```)

**Example:**
```bash
# Show history for the 'users_sync' pipeline
conduit manifest --pipeline-name users_sync
```

```conduit checkpoints```

Lists all currently saved checkpoints used for resuming failed pipelines.

**Usage:**
```bash
conduit checkpoints
```

```conduit clear-checkpoints```

Deletes saved checkpoint files. Useful for forcing a pipeline to run from the beginning.

**Usage:**
```bash
conduit clear-checkpoints [OPTIONS]
```

**Options:**

* ```--pipeline-name TEXT```: Clear the checkpoint only for a specific pipeline. If omitted, clears all checkpoints. 

**Example:**
```bash
# Clear the checkpoint for 'large_orders_pipeline'
conduit clear-checkpoints --pipeline-name large_orders_pipeline
```