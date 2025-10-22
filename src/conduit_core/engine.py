# src/conduit_core/engine.py
import time
import sys
import os
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date
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
from .quality import QualityValidator, QualityAction
from .errors import DataQualityError, ErrorLog, SchemaValidationError
from .schema_validator import SchemaValidator, ValidationReport, ValidationError

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
        logger.info("DRY RUN MODE - Data will be read but not written", prefix="[WARN]")

    manifest = PipelineManifest(manifest_path)
    checkpoint_mgr = CheckpointManager()
    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(d for d in config.destinations if d.name == resource.destination)

    with ManifestTracker(
        manifest=manifest, pipeline_name=resource.name,
        source_type=source_config.type, destination_type=destination_config.type,
    ) as tracker:
        total_processed = 0  # Records read from source
        total_written = 0    # Records successfully written to destination after validation
        error_log = ErrorLog(resource.name)
        max_value_seen: Union[int, str, datetime, date, None] = None  # Initialize outside loop

        try:
            # --- Setup Phase ---
            last_checkpoint_value = None
            if source_config.resume and source_config.checkpoint_column:
                checkpoint = checkpoint_mgr.load_checkpoint(resource.name)
                if checkpoint:
                    last_checkpoint_value = checkpoint['last_value']
                    logger.info(f"Resuming from checkpoint: {source_config.checkpoint_column} > {last_checkpoint_value}")

            current_state = load_state()
            incremental_start_value = last_checkpoint_value if last_checkpoint_value is not None else current_state.get(resource.name)
            max_value_seen = incremental_start_value

            final_query = resource.query
            if resource.incremental_column and incremental_start_value is not None:
                wrapped_value = f"'{incremental_start_value}'" if isinstance(incremental_start_value, (str, datetime, date)) else incremental_start_value
                filter_condition = f"{resource.incremental_column} > {wrapped_value}"
                if "WHERE" in final_query.upper():
                    final_query += f" AND {filter_condition}"
                else:
                    final_query += f" WHERE {filter_condition}"
                if "ORDER BY" not in final_query.upper():
                    final_query += f" ORDER BY {resource.incremental_column}"
            elif resource.incremental_column:
                logger.info(f"Incremental column '{resource.incremental_column}' defined, but no previous state found. Performing full load.")
                if "ORDER BY" not in final_query.upper():
                    final_query += f" ORDER BY {resource.incremental_column}"

            source_class = get_source_connector_map().get(source_config.type)
            source = source_class(source_config)
            destination_class = get_destination_connector_map().get(destination_config.type)
            destination = destination_class(destination_config)

            # --- Schema Operations ---
            inferred_schema = None
            source_iterator = None

            if source_config.infer_schema:
                logger.info("Inferring schema from source data...")
                sample_records_iter = source.read(final_query)
                sample_records = list(itertools.islice(sample_records_iter, source_config.schema_sample_size))
                try:
                    first_after_sample = next(sample_records_iter)
                    logger.debug("Reconstructing source iterator after schema sample.")
                    source_iterator = itertools.chain(iter(sample_records), [first_after_sample], sample_records_iter)
                except StopIteration:
                    logger.debug("Source iterator consumed by schema sample, using sampled records.")
                    source_iterator = iter(sample_records)

                if sample_records:
                    inferred_schema = SchemaInferrer.infer_schema(sample_records, source_config.schema_sample_size)
                    logger.info(f"Schema inferred: {len(inferred_schema.get('columns', []))} columns")
                else:
                    logger.warning("No records returned for schema inference sample.")
                    source_iterator = iter([])
                    inferred_schema = {"columns": []}
            else:
                source_iterator = source.read(final_query)

            # --- Pre-flight Schema Validation (Phase 3) ---
            # After: inferred_schema = SchemaInferrer.infer_schema(...)
            if destination_config.validate_schema and inferred_schema:

                validator = SchemaValidator()
                logger.info("Running pre-flight schema validation...")

                # Get destination table schema if it exists
                if hasattr(destination, 'get_table_schema'):
                    try:
                        dest_schema = destination.get_table_schema()

                        if dest_schema:
                            logger.debug(f"Destination table exists with {len(dest_schema)} columns")

                            # Type compatibility check
                            report = validator.validate_type_compatibility(inferred_schema, dest_schema)

                            if report.has_errors():
                                logger.error("Schema validation failed:")
                                logger.error(report.format_errors())
                                raise SchemaValidationError(report)

                            if report.has_warnings():
                                logger.warning("Schema validation warnings:")
                                logger.warning(report.format_warnings())

                                if destination_config.strict_validation:
                                    logger.error("Strict validation enabled - treating warnings as errors")
                                    raise SchemaValidationError(report)

                            logger.info("[OK] Schema validation passed")
                        else:
                            logger.debug("Destination table does not exist - skipping schema validation")

                    except SchemaValidationError:
                        raise  # Re-raise validation errors
                    except Exception as e:
                        logger.warning(f"Could not validate schema: {e}")
                        # Don't fail the pipeline if schema retrieval fails

                # Required columns check
                if destination_config.required_columns:
                    missing = validator.check_required_columns(inferred_schema, destination_config.required_columns)
                    if missing:
                        error_msg = f"Missing required columns: {', '.join(missing)}"
                        logger.error(error_msg)
                        raise SchemaValidationError(
                            ValidationReport(
                                is_valid=False,
                                errors=[ValidationError(
                                    column=col,
                                    issue="missing_column",
                                    expected="required",
                                    actual="not present",
                                    severity="error"
                                ) for col in missing]
                            )
                        )

            # --- Schema Evolution ---
            if (
                destination_config.schema_evolution and
                destination_config.schema_evolution.enabled and
                inferred_schema and inferred_schema.get("columns") and
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
                                evolution_mgr.apply_evolution(destination, destination_config.table, changes, destination_config.schema_evolution)
                            except SchemaEvolutionError as e:
                                logger.error(f"Schema evolution failed: {e}. Halting pipeline.")
                                raise
                        else:
                            logger.info("DRY RUN: Skipping schema evolution actions.", prefix="[WARN]")
                    else:
                        logger.info("No schema changes detected.")
                else:
                    logger.info("No previous schema found, skipping comparison.")
                if not dry_run and inferred_schema.get("columns"):
                    schema_store.save_schema(resource.name, inferred_schema)
                elif dry_run:
                    logger.debug("DRY RUN: Skipping schema save.")
                elif not inferred_schema.get("columns"):
                    logger.warning("Skipping schema save as inferred schema is empty.")

            # Auto-create table
            if destination_config.auto_create_table and inferred_schema:
                if destination_config.type in ['postgres', 'snowflake', 'bigquery']:
                    logger.info(f"Auto-creating table in {destination_config.type}...")
                    dialect_map = {'postgres': 'postgresql', 'snowflake': 'snowflake', 'bigquery': 'bigquery'}
                    try:
                        create_sql = TableAutoCreator.generate_create_table_sql(destination_config.table, inferred_schema, dialect_map[destination_config.type])
                        logger.info(f"Generated SQL:\n{create_sql}")
                        if not dry_run:
                            try:
                                destination.execute_ddl(create_sql)
                                logger.info(f"Table '{destination_config.table}' created successfully.")
                            except Exception as e:
                                logger.error(f"Failed to auto-create table: {e}")
                                raise
                        else:
                            logger.info("DRY RUN: Skipping table creation.", prefix="[WARN]")
                    except ValueError as e:
                        logger.warning(f"Could not generate CREATE TABLE SQL (schema likely empty): {e}")
                    except Exception as e:
                        logger.error(f"Error during CREATE TABLE SQL generation: {e}", exc_info=True)

            # --- End Schema Operations ---

            destination.mode = resource.mode or destination_config.mode or "append"
            tracker.metadata = {"mode": destination.mode}
            supports_batch_write = hasattr(destination, "write") and callable(getattr(destination, "write"))
            supports_write_one = hasattr(destination, "write_one") and callable(getattr(destination, "write_one"))

            estimated_total = source.estimate_total_records()
            show_progress = not dry_run and sys.stdout.isatty() and os.getenv('CONDUDUIT_NO_PROGRESS') != '1'

            validator = None
            if resource.quality_checks:
                logger.info(f"Initializing Quality Validator with {len(resource.quality_checks)} check(s)...")
                validator = QualityValidator(resource.quality_checks)

            # --- Processing Loop ---
            processing_loop = read_in_batches(source_iterator, batch_size=batch_size)

            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                MofNCompleteColumn(),
                TimeRemainingColumn(),
                disable=not show_progress
            ) as progress:
                task = progress.add_task(f"Processing {resource.name}", total=estimated_total)

                for i, raw_batch in enumerate(processing_loop):
                    raw_batch_list = list(raw_batch)
                    if not raw_batch_list:
                        continue

                    records_in_raw_batch = len(raw_batch_list)
                    current_batch_offset = total_processed
                    total_processed += records_in_raw_batch

                    valid_records_for_write: List[Dict[str, Any]] = []
                    batch_start_time = time.time()

                    if validator:
                        validation_result = validator.validate_batch(raw_batch_list)
                        valid_records_for_write = validation_result.valid_records

                        for invalid_result in validation_result.invalid_records:
                            try:
                                record_index = next(idx for idx, rec in enumerate(raw_batch_list) if rec is invalid_result.record)
                            except StopIteration:
                                record_index = -1
                            row_number = current_batch_offset + record_index + 1 if record_index != -1 else None

                            # The following references QualityAction from quality module; leaving logic intact.
                            highest_action: QualityAction = QualityAction.DLQ
                            relevant_checks = [qc for qc in resource.quality_checks if qc.column in [fc.column for fc in invalid_result.failed_checks]]
                            for failure in invalid_result.failed_checks:
                                check_config = next((qc for qc in relevant_checks if qc.column == failure.column and qc.check == failure.check_name), None)
                                action = check_config.action if check_config else QualityAction.DLQ
                                if action == QualityAction.FAIL:
                                    highest_action = QualityAction.FAIL
                                    break
                                elif action == QualityAction.WARN:
                                    highest_action = QualityAction.WARN

                            failure_summary = "; ".join([f"{fc.column}({fc.check_name}): {fc.details or 'Failed'}" for fc in invalid_result.failed_checks])

                            if highest_action == QualityAction.FAIL:
                                logger.error(f"Record #{row_number or '?'} failed critical quality check. Halting pipeline.")
                                raise DataQualityError(f"Record #{row_number or '?'} failed validation: {failure_summary}")
                            elif highest_action == QualityAction.WARN:
                                logger.warning(f"Record #{row_number or '?'} failed quality check (Warn): {failure_summary}")
                            else:
                                logger.debug(f"Record #{row_number or '?'} failed quality check (DLQ): {failure_summary}")
                                error_log.add_quality_error(invalid_result.record, failure_summary, row_number=row_number)
                    else:
                        valid_records_for_write = raw_batch_list

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
                                        try:
                                            record_index = next(idx for idx, rec in enumerate(raw_batch_list) if rec is record)
                                        except StopIteration:
                                            record_index = -1
                                        row_number = current_batch_offset + record_index + 1 if record_index != -1 else None
                                        logger.error(f"Error writing record #{row_number or '?'}: {e_one}")
                                        error_log.add_error(record, e_one, row_number=row_number)
                            else:
                                raise NotImplementedError(f"{destination.__class__.__name__} must implement write() or write_one()")
                        except Exception as e_batch:
                            logger.error(f"Error writing batch {i+1}: {e_batch}", exc_info=True)
                            for record in valid_records_for_write:
                                try:
                                    record_index = next(idx for idx, rec in enumerate(raw_batch_list) if rec is record)
                                except StopIteration:
                                    record_index = -1
                                row_number = current_batch_offset + record_index + 1 if record_index != -1 else None
                                error_log.add_error(record, e_batch, row_number=row_number)
                            successful_in_batch = 0
                    elif dry_run:
                        successful_in_batch = len(valid_records_for_write)

                    total_written += successful_in_batch

                    if resource.incremental_column:
                        current_batch_max = max_value_seen
                        for record in valid_records_for_write:
                            if resource.incremental_column in record:
                                current_val = record[resource.incremental_column]
                                try:
                                    if current_val is not None:
                                        if current_batch_max is None or current_val > current_batch_max:
                                            current_batch_max = current_val
                                except TypeError:
                                    logger.warning(f"Could not compare incremental value '{current_val}' with max '{current_batch_max}'.")
                        max_value_seen = current_batch_max

                    batch_duration = time.time() - batch_start_time
                    records_failed_quality = records_in_raw_batch - len(valid_records_for_write)
                    records_failed_write = len(valid_records_for_write) - successful_in_batch
                    if not show_progress:
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
                            json.dump(inferred_schema, f, indent=2, default=str)
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
                if error_log.has_errors():
                    error_log.save()

                if resource.incremental_column:
                    if max_value_seen is not None and (incremental_start_value is None or max_value_seen > incremental_start_value):
                        logger.info(f"Saving new incremental state: {resource.incremental_column} = {max_value_seen}")
                        save_state({**current_state, resource.name: max_value_seen})
                    else:
                        logger.info(f"No new records found. State remains at {incremental_start_value}.")

                if source_config.resume:
                    logger.info("Clearing checkpoint...")
                    checkpoint_mgr.clear_checkpoint(resource.name)

            total_failed = error_log.error_count()
            tracker.records_read = total_processed
            tracker.records_written = total_written if not dry_run else 0
            tracker.records_failed = total_failed
            logger.complete_resource(total_processed, total_written, total_failed, dry_run=dry_run)

        except (DataQualityError, SchemaValidationError, SchemaEvolutionError) as specific_err:
            error_type = type(specific_err).__name__
            logger.error(f"Pipeline halted due to {error_type}: {specific_err}")
            tracker.status = "failed"
            tracker.error_message = str(specific_err)
            if not dry_run and error_log.has_errors():
                error_log.save()
            raise

        except Exception as e:
            logger.error(f"Resource '{resource.name}' failed with unexpected error: {e}", exc_info=True)
            tracker.status = "failed"
            tracker.error_message = str(e)
            if source_config.resume and not dry_run:
                logger.warning("Pipeline failed unexpectedly, checkpoint preserved.")
            if not dry_run and error_log.has_errors():
                error_log.save()
            raise
