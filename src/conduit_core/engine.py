# src/conduit_core/engine.py
import time
import sys
import os
from pathlib import Path
from typing import Optional, List, Dict, Any, Union # Added Union
from datetime import datetime, date # Added date
from rich import print
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
)
import itertools
import json
import yaml

from .schema import SchemaInferrer, TableAutoCreator
from .schema_store import SchemaStore
from .schema_evolution import SchemaEvolutionManager, SchemaEvolutionError
from .quality import QualityValidator
from .errors import DataQualityError, ErrorLog

from .config import IngestConfig, Resource
from .state import load_state, save_state
from .batch import read_in_batches
from .logging_utils import ConduitLogger
from .connectors.registry import get_source_connector_map, get_destination_connector_map
from .manifest import PipelineManifest, ManifestTracker
from .checkpoint import CheckpointManager


def run_resource(
    resource: Resource,
    config: IngestConfig,
    batch_size: int = 1000,
    manifest_path: Optional[Path] = None,
    dry_run: bool = False,
):
    """Runs a single data pipeline resource with all features."""

    logger = ConduitLogger(resource.name)
    logger.start_resource()

    if dry_run:
        logger.info("DRY RUN MODE - Data will be read but not written", prefix="⚠")

    manifest = PipelineManifest(manifest_path)
    checkpoint_mgr = CheckpointManager()
    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(d for d in config.destinations if d.name == resource.destination)

    with ManifestTracker(
        manifest=manifest, pipeline_name=resource.name,
        source_type=source_config.type, destination_type=destination_config.type,
    ) as tracker:
        total_processed = 0 # Records read from source
        total_written = 0   # Records successfully written to destination after validation
        error_log = ErrorLog(resource.name)
        max_value_seen: Union[int, str, datetime, date, None] = None # Initialize outside loop, type hint for clarity
        
        try:
            # --- Setup Phase ---
            last_checkpoint_value = None
            if source_config.resume and source_config.checkpoint_column:
                checkpoint = checkpoint_mgr.load_checkpoint(resource.name)
                if checkpoint:
                    last_checkpoint_value = checkpoint['last_value']
                    # total_processed = checkpoint.get('records_processed', 0) # Reset processed count
                    logger.info(f"Resuming from checkpoint: {source_config.checkpoint_column} > {last_checkpoint_value}")

            current_state = load_state()
            # Use checkpoint value if resuming, otherwise use state
            incremental_start_value = last_checkpoint_value if last_checkpoint_value is not None else current_state.get(resource.name) # Use None default if not in state
            max_value_seen = incremental_start_value # Initialize max_value_seen here

            final_query = resource.query
            # Apply incremental filter if column is defined AND we have a value to filter from
            if resource.incremental_column and incremental_start_value is not None:
                 # Ensure value is correctly quoted if it's a string-like type (e.g., timestamp, date, str)
                 wrapped_value = f"'{incremental_start_value}'" if isinstance(incremental_start_value, (str, datetime, date)) else incremental_start_value
                 filter_condition = f"{resource.incremental_column} > {wrapped_value}"

                 # Append filter condition smartly
                 if "WHERE" in final_query.upper():
                      final_query += f" AND {filter_condition}"
                 else:
                      final_query += f" WHERE {filter_condition}"
                 # Add ORDER BY to ensure consistent processing for incremental loads
                 if "ORDER BY" not in final_query.upper():
                      final_query += f" ORDER BY {resource.incremental_column}"
            elif resource.incremental_column:
                 logger.info(f"Incremental column '{resource.incremental_column}' defined, but no previous state found. Performing full load.")
                 # Ensure ORDER BY is still added if doing initial load
                 if "ORDER BY" not in final_query.upper():
                      final_query += f" ORDER BY {resource.incremental_column}"


            source_class = get_source_connector_map().get(source_config.type)
            source = source_class(source_config)
            destination_class = get_destination_connector_map().get(destination_config.type)
            destination = destination_class(destination_config)

            # --- Schema Operations ---
            inferred_schema = None
            source_iterator = None # Define variable before the conditional block

            if source_config.infer_schema:
                logger.info("Inferring schema from source data...")
                # Important: Use the potentially filtered final_query for inference
                sample_records_iter = source.read(final_query)
                sample_records = list(itertools.islice(sample_records_iter, source_config.schema_sample_size))

                # Logic to reconstruct iterator after sampling
                try:
                     first_after_sample = next(sample_records_iter)
                     # If successful, reconstruct the full iterator
                     logger.debug("Reconstructing source iterator after schema sample.")
                     source_iterator = itertools.chain(iter(sample_records), [first_after_sample], sample_records_iter)
                except StopIteration:
                     # The iterator was fully consumed by islice, just use the sample
                     logger.debug("Source iterator consumed by schema sample, using sampled records.")
                     source_iterator = iter(sample_records)

                if sample_records:
                     inferred_schema = SchemaInferrer.infer_schema(sample_records, source_config.schema_sample_size)
                     logger.info(f"Schema inferred: {len(inferred_schema.get('columns',[]))} columns")
                else:
                     logger.warning("No records returned for schema inference sample.")
                     # Ensure source_iterator is an empty iterator if no records
                     source_iterator = iter([])
                     inferred_schema = {"columns": []} # Set empty schema
            else:
                 # If not inferring, initialize the main iterator directly
                 source_iterator = source.read(final_query)


            # --- Schema Evolution ---
            if (
                destination_config.schema_evolution and
                destination_config.schema_evolution.enabled and
                inferred_schema and inferred_schema.get("columns") and # Check schema has columns
                destination_config.type in ['postgresql', 'snowflake', 'bigquery']
            ):
                schema_store = SchemaStore()
                last_schema = schema_store.load_last_schema(resource.name)

                if last_schema:
                    logger.info("Comparing inferred schema with last known schema...")
                    evolution_mgr = SchemaEvolutionManager()
                    changes = evolution_mgr.compare_schemas(last_schema, inferred_schema)

                    if changes.has_changes():
                        logger.warning(f"Schema changes detected: {changes.summary()}")
                        if not dry_run:
                            try:
                                evolution_mgr.apply_evolution(
                                    destination,
                                    destination_config.table,
                                    changes,
                                    destination_config.schema_evolution
                                )
                            except SchemaEvolutionError as e:
                                logger.error(f"Schema evolution failed: {e}. Halting pipeline.")
                                raise # Re-raise to stop processing
                        else:
                            logger.info("DRY RUN: Skipping schema evolution actions.", prefix="⚠")
                    else:
                         logger.info("No schema changes detected.")
                else:
                    logger.info("No previous schema found, skipping comparison.")

                # Save schema only if evolution didn't fail and not dry run
                if not dry_run and inferred_schema.get("columns"):
                    schema_store.save_schema(resource.name, inferred_schema)
                elif dry_run:
                    logger.debug("DRY RUN: Skipping schema save.")
                elif not inferred_schema.get("columns"):
                     logger.warning("Skipping schema save as inferred schema is empty.")


            # Schema validation for DB destinations
            # (Existing schema validation logic remains here)
            if destination_config.validate_schema and inferred_schema:
                # *** FIX: Added pass statement ***
                pass # Placeholder for Phase 3 logic


            # Auto-create table
            # (Existing auto-create logic remains here)
            if destination_config.auto_create_table and inferred_schema:
                if destination_config.type in ['postgresql', 'snowflake', 'bigquery']:
                    logger.info(f"Auto-creating table in {destination_config.type}...")
                    dialect_map = {'postgresql': 'postgresql', 'snowflake': 'snowflake', 'bigquery': 'bigquery'}
                    try:
                        create_sql = TableAutoCreator.generate_create_table_sql(
                            destination_config.table,
                            inferred_schema,
                            dialect_map[destination_config.type]
                        )
                        logger.info(f"Generated SQL:\n{create_sql}")

                        if not dry_run:
                             try:
                                 destination.execute_ddl(create_sql)
                                 logger.info(f"Table '{destination_config.table}' created successfully.")
                             except Exception as e:
                                 logger.error(f"Failed to auto-create table: {e}")
                                 raise
                        else:
                             logger.info("DRY RUN: Skipping table creation.", prefix="⚠")
                    except ValueError as e:
                         logger.warning(f"Could not generate CREATE TABLE SQL (schema likely empty): {e}")
                    except Exception as e: # Catch other potential errors during generation
                         logger.error(f"Error during CREATE TABLE SQL generation: {e}", exc_info=True)


            # --- End Schema Operations ---

            destination.mode = resource.mode or destination_config.mode or "append"
            tracker.metadata = {"mode": destination.mode}
            supports_batch_write = hasattr(destination, "write") and callable(getattr(destination, "write"))
            supports_write_one = hasattr(destination, "write_one") and callable(getattr(destination, "write_one"))


            estimated_total = source.estimate_total_records()

            show_progress = not dry_run and sys.stdout.isatty() and os.getenv('CONDUDUIT_NO_PROGRESS') != '1'

            # --- Initialize Quality Validator ---
            validator = None
            if resource.quality_checks:
                logger.info(f"Initializing Quality Validator with {len(resource.quality_checks)} check(s)...")
                validator = QualityValidator(resource.quality_checks)

            # --- Processing Loop ---
            processing_loop = read_in_batches(source_iterator, batch_size=batch_size) # Use source_iterator

            with Progress(
                TextColumn("[progress.description]{task.description}"), BarColumn(),
                TaskProgressColumn(), MofNCompleteColumn(), TimeRemainingColumn(),
                disable=not show_progress
            ) as progress:
                task = progress.add_task(f"Processing {resource.name}", total=estimated_total)

                for i, raw_batch in enumerate(processing_loop):
                    # Ensure raw_batch is a list for multiple uses
                    raw_batch_list = list(raw_batch)
                    if not raw_batch_list: continue # Skip empty batches

                    records_in_raw_batch = len(raw_batch_list)
                    current_batch_offset = total_processed # Track starting row number for errors
                    total_processed += records_in_raw_batch # Increment total read count

                    valid_records_for_write: List[Dict[str, Any]] = []
                    batch_start_time = time.time()

                    # --- Quality Check Logic ---
                    if validator:
                        validation_result = validator.validate_batch(raw_batch_list)
                        valid_records_for_write = validation_result.valid_records

                        for invalid_result in validation_result.invalid_records:
                            try:
                                 # Find original index for row number - best effort
                                 record_index = next(idx for idx, rec in enumerate(raw_batch_list) if rec is invalid_result.record)
                            except StopIteration:
                                 record_index = -1 # Should not happen if record came from batch

                            row_number = current_batch_offset + record_index + 1 if record_index != -1 else None

                            # Determine highest severity action
                            highest_action: QualityAction = QualityAction.DLQ # Default
                            relevant_checks = [qc for qc in resource.quality_checks if qc.column in [fc.column for fc in invalid_result.failed_checks]]
                            
                            for failure in invalid_result.failed_checks:
                                 # Find the specific check config that caused this failure
                                 check_config = next((qc for qc in relevant_checks if qc.column == failure.column and qc.check == failure.check_name), None)
                                 action = check_config.action if check_config else QualityAction.DLQ

                                 if action == QualityAction.FAIL:
                                      highest_action = QualityAction.FAIL
                                      break # Fail is highest priority
                                 elif action == QualityAction.WARN:
                                      highest_action = QualityAction.WARN # Keep checking for Fail

                            failure_summary = "; ".join([f"{fc.column}({fc.check_name}): {fc.details or 'Failed'}" for fc in invalid_result.failed_checks])

                            if highest_action == QualityAction.FAIL:
                                logger.error(f"Record #{row_number or '?'} failed critical quality check. Halting pipeline.")
                                raise DataQualityError(f"Record #{row_number or '?'} failed validation: {failure_summary}")
                            elif highest_action == QualityAction.WARN:
                                logger.warning(f"Record #{row_number or '?'} failed quality check (Warn): {failure_summary}")
                                # Don't add to DLQ if only warning
                            else: # Default is DLQ
                                logger.debug(f"Record #{row_number or '?'} failed quality check (DLQ): {failure_summary}")
                                error_log.add_quality_error(invalid_result.record, failure_summary, row_number=row_number)
                    else:
                        valid_records_for_write = raw_batch_list
                    # --- End Quality Check Logic ---

                    successful_in_batch = 0
                    if not dry_run and valid_records_for_write:
                        try:
                            if supports_batch_write:
                                destination.write(valid_records_for_write)
                                successful_in_batch = len(valid_records_for_write)
                            elif supports_write_one:
                                for record in valid_records_for_write:
                                     try:
                                          destination.write_one(record)
                                          successful_in_batch += 1
                                     except Exception as e_one:
                                          # Find original index for row number
                                          try: record_index = next(idx for idx, rec in enumerate(raw_batch_list) if rec is record)
                                          except StopIteration: record_index = -1
                                          row_number = current_batch_offset + record_index + 1 if record_index != -1 else None
                                          logger.error(f"Error writing record #{row_number or '?'}: {e_one}")
                                          error_log.add_error(record, e_one, row_number=row_number)
                            else:
                                 logger.error(f"Destination connector {destination.__class__.__name__} supports neither write() nor write_one(). Cannot write data.")
                                 raise NotImplementedError(f"{destination.__class__.__name__} must implement write() or write_one()")

                        except Exception as e_batch:
                            logger.error(f"Error writing batch {i+1}: {e_batch}", exc_info=True) # Log traceback for batch errors
                            for record in valid_records_for_write:
                                try: record_index = next(idx for idx, rec in enumerate(raw_batch_list) if rec is record)
                                except StopIteration: record_index = -1
                                row_number = current_batch_offset + record_index + 1 if record_index != -1 else None
                                error_log.add_error(record, e_batch, row_number=row_number)
                            successful_in_batch = 0 # Ensure none counted successful

                    elif dry_run:
                        successful_in_batch = len(valid_records_for_write) # In dry run, valid records count as "successful"

                    total_written += successful_in_batch

                    # Update max_value_seen for incremental loads
                    if resource.incremental_column:
                         current_batch_max = max_value_seen # Keep track of max within batch
                         for record in valid_records_for_write: # Only consider written records
                              if resource.incremental_column in record:
                                   current_val = record[resource.incremental_column]
                                   # Robust comparison (handle None, type differences)
                                   try:
                                        if current_val is not None:
                                             if current_batch_max is None or current_val > current_batch_max:
                                                  current_batch_max = current_val
                                   except TypeError:
                                        # Handle potential comparison errors between different types
                                        logger.warning(f"Could not compare incremental column value '{current_val}' ({type(current_val)}) with current max '{current_batch_max}' ({type(current_batch_max)}). Skipping update for this record.")
                         max_value_seen = current_batch_max # Update overall max after batch


                    batch_duration = time.time() - batch_start_time
                    records_failed_quality = records_in_raw_batch - len(valid_records_for_write)
                    records_failed_write = len(valid_records_for_write) - successful_in_batch

                    if not show_progress: # Log batch summary if progress bar is disabled
                         log_func = logger.info if not dry_run else logger.debug
                         log_func(
                              f"Batch {i+1}: Read={records_in_raw_batch}, "
                              f"Valid={len(valid_records_for_write)}, "
                              f"Written={successful_in_batch if not dry_run else '(Dry Run)'}, "
                              f"FailedQuality={records_failed_quality}, "
                              f"FailedWrite={records_failed_write} "
                              f"(Took {batch_duration:.2f}s)"
                         )

                    progress.update(task, advance=records_in_raw_batch)
            # --- End Processing Loop ---

            # Export schema
            if resource.export_schema_path and inferred_schema:
                schema_path = Path(resource.export_schema_path)
                schema_path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    logger.info(f"Exporting schema to {schema_path}...")
                    if schema_path.suffix == '.json':
                         with open(schema_path, 'w') as f:
                              json.dump(inferred_schema, f, indent=2, default=str) # Add default=str for safety
                    elif schema_path.suffix in ['.yaml', '.yml']:
                         with open(schema_path, 'w') as f:
                              yaml.dump(inferred_schema, f, default_flow_style=False)
                    else:
                         logger.warning(f"Unsupported schema export format: {schema_path.suffix}. Defaulting to JSON.")
                         json_path = schema_path.with_suffix(".json")
                         with open(json_path, 'w') as f:
                              json.dump(inferred_schema, f, indent=2, default=str)
                         logger.info(f"Schema exported to {json_path} instead.")

                    logger.info(f"Schema exported successfully.")
                except Exception as e:
                     logger.error(f"Failed to export schema to {schema_path}: {e}", exc_info=True)


            if hasattr(destination, "finalize") and not dry_run:
                logger.info("Finalizing destination...")
                destination.finalize()

            if not dry_run:
                # Save errors if any occurred
                if error_log.has_errors():
                     error_log.save()

                # Save state only if incremental column is defined and new max value found
                if resource.incremental_column:
                     if max_value_seen is not None and (incremental_start_value is None or max_value_seen > incremental_start_value):
                          logger.info(f"Saving new incremental state: {resource.incremental_column} = {max_value_seen}")
                          save_state({**current_state, resource.name: max_value_seen})
                     else: # No new max value found or initial value was None
                          logger.info(f"No new records found based on incremental column '{resource.incremental_column}'. State remains at {incremental_start_value}.")

                # Clear checkpoint only on successful completion of the *entire* run (moved outside try block?)
                # Decision: Keep here. If finalize() fails, we might still want to clear checkpoint.
                # If quality check fails, exception is raised before this point.
                if source_config.resume:
                    logger.info("Clearing checkpoint...")
                    checkpoint_mgr.clear_checkpoint(resource.name)

            total_failed = error_log.error_count() # Includes write errors and DLQ quality errors
            tracker.records_read = total_processed
            tracker.records_written = total_written if not dry_run else 0
            tracker.records_failed = total_failed
            logger.complete_resource(total_processed, total_written, total_failed, dry_run=dry_run)

        except DataQualityError as dq_err: # Catch specific quality error to halt pipeline
             logger.error(f"Pipeline halted due to DataQualityError: {dq_err}")
             tracker.status = "failed"
             tracker.error_message = str(dq_err)
             if not dry_run and error_log.has_errors(): error_log.save() # Save errors accumulated so far
             # Do not clear checkpoint on quality failure
             raise # Re-raise to stop execution

        except SchemaEvolutionError as se_err: # Catch specific schema evolution error
             logger.error(f"Pipeline halted due to SchemaEvolutionError: {se_err}")
             tracker.status = "failed"
             tracker.error_message = str(se_err)
             if not dry_run and error_log.has_errors(): error_log.save()
             # Do not clear checkpoint on schema evolution failure
             raise

        except Exception as e:
            logger.error(f"Resource '{resource.name}' failed with unexpected error: {e}", exc_info=True) # Log traceback
            tracker.status = "failed"
            tracker.error_message = str(e)
            # Preserve checkpoint on unexpected failure if resuming
            if source_config.resume and not dry_run: logger.warning("Pipeline failed unexpectedly, checkpoint preserved.")
            if not dry_run and error_log.has_errors(): error_log.save() # Save any errors logged before crash
            raise # Re-raise to stop execution