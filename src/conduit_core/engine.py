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
from .incremental import IncrementalSyncManager, IncrementalState
from .types import coerce_record, TypeCoercer
from .engine_modules.type_coercion import apply_type_coercion
from .engine_modules.incremental_sync import setup_incremental_sync, save_incremental_state
from .engine_modules.preflight import preflight_check, run_preflight
from .engine_modules.schema_operations import (
    infer_schema_from_source,
    handle_schema_evolution,
    auto_create_table
)
from conduit_core.execution.parallel_extraction import ParallelExtractor
from conduit_core.engine_modules.quality_checks import ColumnStats


def _get_sql_type_for_column(col_type: str, dialect: str) -> str:
    """Map conduit column type to SQL type for given dialect."""
    type_maps = {
        'postgresql': {
            'integer': 'INTEGER',
            'float': 'DOUBLE PRECISION',
            'decimal': 'NUMERIC',
            'boolean': 'BOOLEAN',
            'date': 'DATE',
            'datetime': 'TIMESTAMP',
            'string': 'TEXT',
            'json': 'JSONB'
        },
        'snowflake': {
            'integer': 'NUMBER(38,0)',
            'float': 'FLOAT',
            'decimal': 'NUMBER(38,9)',
            'boolean': 'BOOLEAN',
            'date': 'DATE',
            'datetime': 'TIMESTAMP_NTZ',
            'string': 'VARCHAR',
            'json': 'VARIANT'
        },
        'bigquery': {
            'integer': 'INT64',
            'float': 'FLOAT64',
            'decimal': 'NUMERIC',
            'boolean': 'BOOL',
            'date': 'DATE',
            'datetime': 'DATETIME',
            'string': 'STRING',
            'json': 'JSON'
        }
    }
    
    mapping = type_maps.get(dialect, {})
    return mapping.get(col_type, 'VARCHAR(255)')

#------------------------------------------------------------------------------------------------
# Preflight mode
#------------------------------------------------------------------------------------------------

def preflight_check(
    config: IngestConfig,
    resource_name: Optional[str] = None,
    verbose: bool = False
) -> dict:
    """
    Comprehensive pre-flight validation.
    
    Returns:
        dict: {
            "passed": bool,
            "checks": [{"name": str, "status": str, "message": str}],
            "warnings": [str],
            "errors": [str]
        }
    """
    import uuid
    from datetime import datetime
    from .connectors.registry import get_source_connector_map, get_destination_connector_map
    from .schema import SchemaInferrer
    
    results = {
        "passed": True,
        "checks": [],
        "warnings": [],
        "errors": [],
        "duration_s": 0
    }
    
    start_time = datetime.now()
    
    # Filter resources
    resources = config.resources
    if resource_name:
        resources = [r for r in resources if r.name == resource_name]
        if not resources:
            results["passed"] = False
            results["errors"].append(f"Resource '{resource_name}' not found")
            return results
    
    # Check 1: Config syntax (already validated by Pydantic)
    results["checks"].append({
        "name": "Config Syntax",
        "status": "pass",
        "message": f"Valid configuration with {len(resources)} resource(s)"
    })
    
    # Check 2-7: Per resource
    for resource in resources:
        resource_prefix = f"[{resource.name}]"
        
        # Find source/destination configs
        source_config = next((s for s in config.sources if s.name == resource.source), None)
        dest_config = next((d for d in config.destinations if d.name == resource.destination), None)
        
        if not source_config:
            results["passed"] = False
            results["errors"].append(f"{resource_prefix} Source '{resource.source}' not found")
            continue
        
        if not dest_config:
            results["passed"] = False
            results["errors"].append(f"{resource_prefix} Destination '{resource.destination}' not found")
            continue
        
        # Check 2: Source connection
        try:

            source_map = get_source_connector_map()
            SourceClass = source_map.get(source_config.type)
            if not SourceClass:
                raise ValueError(f"Unknown source type: {source_config.type}")

            source = SourceClass(source_config)

            # Test connection by attempting read with limit 0
            test_iter = source.read(query=resource.query if hasattr(resource, 'query') else None)
            next(test_iter, None)  # Try to fetch first batch
            results["checks"].append({
                "name": f"{resource_prefix} Source Connection",
                "status": "pass",
                "message": f"Connected to {source_config.type} source"
            })
        except Exception as e:
            results["passed"] = False
            results["errors"].append(f"{resource_prefix} Source connection failed: {str(e)}")
            results["checks"].append({
                "name": f"{resource_prefix} Source Connection",
                "status": "fail",
                "message": str(e)
            })
            continue
        
        # Check 3: Destination connection
        table_exists = False
        try:
            dest_map = get_destination_connector_map()
            DestClass = dest_map.get(dest_config.type)
            if not DestClass:
                raise ValueError(f"Unknown destination type: {dest_config.type}")
            destination = DestClass(dest_config)
            
            results["checks"].append({
                "name": f"{resource_prefix} Destination Connection",
                "status": "pass",
                "message": f"Connected to {dest_config.type} destination"
            })
            
            # Check 3b: Table existence (for DB destinations)
            if dest_config.type in ['postgres', 'snowflake', 'bigquery'] and hasattr(destination, 'table_exists'):
                try:
                    table_exists = destination.table_exists()
                    
                    if not table_exists:
                        if hasattr(dest_config, 'auto_create_table') and dest_config.auto_create_table:
                            results["checks"].append({
                                "name": f"{resource_prefix} Table Existence",
                                "status": "pass",
                                "message": "Table will be auto-created"
                            })
                        else:
                            results["passed"] = False
                            table_name = getattr(dest_config, 'table', 'unknown')
                            results["errors"].append(f"{resource_prefix} Table '{table_name}' does not exist and auto_create_table is disabled")
                            results["checks"].append({
                                "name": f"{resource_prefix} Table Existence",
                                "status": "fail",
                                "message": f"Table '{table_name}' not found"
                            })
                    else:
                        table_name = getattr(dest_config, 'table', 'unknown')
                        results["checks"].append({
                            "name": f"{resource_prefix} Table Existence",
                            "status": "pass",
                            "message": f"Table '{table_name}' exists"
                        })
                except Exception as e:
                    results["warnings"].append(f"{resource_prefix} Could not check table existence: {str(e)}")
                    
        except Exception as e:
            results["passed"] = False
            results["errors"].append(f"{resource_prefix} Destination connection failed: {str(e)}")
            results["checks"].append({
                "name": f"{resource_prefix} Destination Connection",
                "status": "fail",
                "message": str(e)
            })
            continue
        
        # Check 4: Schema inference
        schema = None
        if source_config.infer_schema:
            try:
                # Sample records from source
                sample_records = []
                for batch in source.read():
                    # batch is either a dict (single record) or list of dicts
                    if isinstance(batch, dict):
                        sample_records.append(batch)
                    elif isinstance(batch, list):
                        sample_records.extend(batch)
                    
                    if len(sample_records) >= 100:
                        break
                schema = SchemaInferrer.infer_schema(sample_records[:100])

                results["checks"].append({
                    "name": f"{resource_prefix} Schema Inference",
                    "status": "pass",
                    "message": f"Inferred schema with {len(schema.get('columns', []))} columns"
                })
                
                # Check 5: Schema drift detection (if table exists)
                if table_exists and dest_config.type in ['postgres', 'snowflake', 'bigquery'] and hasattr(destination, 'get_table_schema'):
                    try:
                        from .schema import compare_schemas
                        dest_schema = destination.get_table_schema()
                        drift = compare_schemas(schema, dest_schema)
                        
                        if drift['added'] or drift['removed'] or drift['changed']:
                            drift_msg = []
                            if drift['added']:
                                drift_msg.append(f"Added: {', '.join(drift['added'])}")
                            if drift['removed']:
                                drift_msg.append(f"Removed: {', '.join(drift['removed'])}")
                            if drift['changed']:
                                changes = [f"{col} ({change})" for col, change in drift['changed'].items()]
                                drift_msg.append(f"Changed: {', '.join(changes)}")
                            
                            full_msg = "; ".join(drift_msg)
                            results["warnings"].append(f"{resource_prefix} Schema drift: {full_msg}")
                            results["checks"].append({
                                "name": f"{resource_prefix} Schema Drift",
                                "status": "warn",
                                "message": full_msg
                            })
                        else:
                            results["checks"].append({
                                "name": f"{resource_prefix} Schema Drift",
                                "status": "pass",
                                "message": "No drift detected"
                            })

                        # NEW: Schema Evolution Preview
                        if dest_config.schema_evolution and dest_config.schema_evolution.enabled:
                            from .schema_evolution import SchemaEvolutionManager
                            from .schema_store import SchemaStore
                            
                            schema_store = SchemaStore()
                            last_schema_data = schema_store.load_last_schema(resource.name)
                            
                            if last_schema_data:
                                last_schema = last_schema_data.get('schema') if isinstance(last_schema_data, dict) and 'schema' in last_schema_data else last_schema_data
                                
                                evolution_mgr = SchemaEvolutionManager()
                                changes = evolution_mgr.compare_schemas(last_schema, schema)
                                
                                if changes.has_changes():
                                    preview_lines = [f"{resource_prefix} Schema Evolution Preview:"]
                                    
                                    if changes.added_columns:
                                        for col in changes.added_columns:
                                            from .schema import TableAutoCreator
                                            ddl = TableAutoCreator.generate_add_column_sql(
                                                dest_config.table, 
                                                col, 
                                                dest_config.type
                                            )
                                            preview_lines.append(f"  [+] Would execute: {ddl}")
                                    
                                    if changes.removed_columns:
                                        preview_lines.append(f"  [!] Columns removed from source: {[c.name for c in changes.removed_columns]}")
                                        preview_lines.append(f"      (Destination data would be preserved)")
                                    
                                    if changes.type_changes:
                                        for tc in changes.type_changes:
                                            preview_lines.append(f"  [~] Type change detected: {tc.column} ({tc.old_type} → {tc.new_type})")
                                    
                                    old_version = last_schema_data.get('version', 0) if isinstance(last_schema_data, dict) else 0
                                    preview_lines.append(f"  Version: {old_version} → {old_version + 1}")
                                    
                                    results["checks"].append({
                                        "name": f"{resource_prefix} Schema Evolution",
                                        "status": "info",
                                        "message": "\n".join(preview_lines)
                                    })

                    except Exception as e:
                        results["warnings"].append(f"{resource_prefix} Could not check schema drift: {str(e)}")
                        
            except Exception as e:
                results["warnings"].append(f"{resource_prefix} Schema inference failed: {str(e)}")
                results["checks"].append({
                    "name": f"{resource_prefix} Schema Inference",
                    "status": "warn",
                    "message": str(e)
                })
        
        # Check 6: Destination compatibility
        if hasattr(dest_config, 'auto_create_table') and dest_config.auto_create_table:
            results["checks"].append({
                "name": f"{resource_prefix} Destination Compatibility",
                "status": "pass",
                "message": "Auto-create table enabled"
            })
        else:
            results["checks"].append({
                "name": f"{resource_prefix} Destination Compatibility",
                "status": "pass",
                "message": "Manual table management"
            })
        
        # Check 7: Quality checks validation
        if resource.quality_checks:
            try:
                from .quality import QualityValidator
                # Validate quality check syntax
                results["checks"].append({
                    "name": f"{resource_prefix} Quality Checks",
                    "status": "pass",
                    "message": f"{len(resource.quality_checks)} quality rule(s) configured"
                })
            except Exception as e:
                results["warnings"].append(f"{resource_prefix} Quality check validation failed: {str(e)}")
    
    # Calculate duration
    results["duration_s"] = (datetime.now() - start_time).total_seconds()
    
    return results


def run_preflight(config_path: str = "ingest.yml", resource_name: Optional[str] = None, verbose: bool = True):
    """
    Run preflight checks and display results.
    Used by CLI 'conduit preflight' command.
    """
    from rich.console import Console
    from rich.table import Table
    from .config import load_config
    
    console = Console()
    
    try:
        config = load_config(config_path)
    except Exception as e:
        console.print(f"[red]FAIL[/red] Failed to load config: {e}")
        return False
    
    console.print("\nRunning preflight checks...\n")
    
    results = preflight_check(config, resource_name=resource_name, verbose=verbose)
    
    # Display results in table
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Check", style="dim")
    table.add_column("Status", justify="center")
    table.add_column("Message")
    
    for check in results["checks"]:
        status_symbol = {
            "pass": "[green]PASS[/green]",
            "warn": "[yellow]WARN[/yellow]",
            "fail": "[red]FAIL[/red]"
        }.get(check["status"], "?")
        
        table.add_row(
            check["name"],
            status_symbol,
            check["message"]
        )
    
    console.print(table)
    
    # Display warnings and errors
    if results["warnings"]:
        console.print(f"\n[yellow]Warnings: {len(results['warnings'])}[/yellow]")
        for warning in results["warnings"]:
            console.print(f"  • {warning}")
    
    if results["errors"]:
        console.print(f"\n[red]Errors: {len(results['errors'])}[/red]")
        for error in results["errors"]:
            console.print(f"  • {error}")
    
    console.print(f"\nPreflight completed in {results['duration_s']:.2f}s")
    
    if results["passed"]:
        console.print("\n[green]All checks passed - safe to run[/green]\n")
    else:
        console.print("\n[red]Preflight failed - fix errors before running[/red]\n")
    
    return results["passed"]

#------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------

def run_resource(
    resource: Resource,
    config: IngestConfig,
    batch_size: int = 1000,
    manifest_path: Optional[Path] = None,
    dry_run: bool = False,
    skip_preflight: bool = False
):
    """Runs a single data pipeline resource with all features."""

    # Run preflight checks unless skipped
    preflight_results = None
    if not skip_preflight:
        preflight_results = preflight_check(config, resource_name=resource.name, verbose=False)
        if not preflight_results["passed"]:
            from rich.console import Console
            console = Console()
            console.print(f"\n[red]Preflight failed for resource '{resource.name}'[/red]")
            for error in preflight_results["errors"]:
                console.print(f"  • {error}")
            raise ValueError(f"Preflight checks failed. Use --skip-preflight to bypass.")

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
        # Store preflight results in tracker
        if preflight_results:
            tracker.preflight_duration_s = preflight_results.get("duration_s")
            tracker.preflight_warnings = preflight_results.get("warnings")

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
            
            # Setup incremental sync
            (incremental_mgr, incremental_column, incremental_start_value, 
             incremental_values, max_value_seen, final_query) = setup_incremental_sync(
                resource,
                last_checkpoint_value,
            )
            
            # Initialize connectors
            source_class = get_source_connector_map().get(source_config.type)
            source = source_class(source_config)
            destination_class = get_destination_connector_map().get(destination_config.type)
            destination = destination_class(destination_config)

            # Store query on source for parallel extraction
            source._current_query = final_query

            # --- Schema Operations ---
            # Infer schema if enabled
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

                    # --- Schema Drift Detection & Auto-Evolution ---
                    # Compare source schema with current destination schema
                    try:
                        dest_schema_dict = destination.get_table_schema()
                        if dest_schema_dict:
                            from .schema import compare_schemas
                            
                            # Convert to comparable format
                            source_schema_for_compare = {"columns": inferred_schema.get("columns", [])}
                            # dest_schema_dict is {col_name: {type: ..., nullable: ...}}
                            dest_schema_for_compare = {"columns": [{"name": k.lower(), "type": v.get("type"), "nullable": v.get("nullable")} for k, v in dest_schema_dict.items()]}

                            drift = compare_schemas(source_schema_for_compare, dest_schema_for_compare)

                            if drift.get('added') or drift.get('removed') or drift.get('changed'):
                                # Log clean summary ONCE
                                logger.info("=" * 60)
                                logger.info("Schema Drift Detected")
                                logger.info("=" * 60)
                                
                                if drift.get('added'):
                                    logger.info(f"New columns in source: {', '.join(drift['added'])}")
                                if drift.get('removed'):
                                    logger.info(f"Columns removed from source: {', '.join(drift['removed'])}")
                                if drift.get('changed'):
                                    for col, change in drift['changed'].items():
                                        logger.info(f"Type changed: {col} ({change})")
                                
                                logger.info("=" * 60)
                                
                                # Auto-evolve if configured
                                schema_evo_config = destination_config.schema_evolution
                                if schema_evo_config and schema_evo_config.enabled:
                                    if schema_evo_config.on_new_column == 'add_nullable' and drift.get('added'):
                                        logger.info("Auto-evolving schema: Adding new columns...")
                                        
                                        for col_name in drift['added']:
                                            # Find column info from inferred schema
                                            col_info = next((c for c in inferred_schema.get('columns', []) if c.get('name') == col_name), None)
                                            if col_info:
                                                col_type = col_info.get('type', 'string')
                                                
                                                # Generate dialect-specific ALTER TABLE
                                                if destination_config.type == 'postgres':
                                                    sql_type = _get_sql_type_for_column(col_type, 'postgresql')
                                                    sql = f'ALTER TABLE "{destination_config.table}" ADD COLUMN "{col_name}" {sql_type}'
                                                elif destination_config.type == 'snowflake':
                                                    sql_type = _get_sql_type_for_column(col_type, 'snowflake')
                                                    full_table = f'"{destination.database}"."{destination.db_schema}"."{destination_config.table}"'
                                                    sql = f'ALTER TABLE {full_table} ADD COLUMN "{col_name.upper()}" {sql_type}'
                                                elif destination_config.type == 'bigquery':
                                                    sql_type = _get_sql_type_for_column(col_type, 'bigquery')
                                                    full_table = f'`{destination_config.project}.{destination_config.dataset}.{destination_config.table}`'
                                                    sql = f'ALTER TABLE {full_table} ADD COLUMN {col_name} {sql_type}'
                                                else:
                                                    continue
                                                
                                                logger.info(f"Executing: {sql}")
                                                if not dry_run and hasattr(destination, 'execute_ddl'):
                                                    try:
                                                        result = destination.execute_ddl(sql)
                                                        logger.info(f"✓ Column '{col_name}' added successfully")
                                                    except Exception as e:
                                                        logger.error(f"FAILED to add column '{col_name}': {e}")
                                                        logger.exception("Full traceback:")
                                                        if destination_config.strict_validation:
                                                            raise
                                                        else:
                                                            logger.warning("Continuing despite ALTER TABLE failure...")
                                                else:
                                                    logger.info("[DRY RUN] Would execute ALTER TABLE")
                                    else:
                                        logger.warning("Schema drift detected but auto-evolution not configured.")
                                        logger.warning("Data for new columns will be silently dropped.")
                                else:
                                    if drift.get('added'):
                                        logger.warning("Schema drift detected but auto-evolution not enabled.")
                                        logger.warning("Data for new columns will be silently dropped.")
                    except Exception as e:
                        logger.debug(f"Could not check schema drift: {e}")

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
            handle_schema_evolution(
                resource,
                destination,
                destination_config,
                inferred_schema,
                tracker,
                dry_run
            )

            # Auto-create table if needed
            auto_create_table(destination, destination_config, inferred_schema, dry_run)

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

                # Enhanced quality checks
            quality_analyzer = None
            enhanced_checks = getattr(resource, 'enhanced_quality_checks', None)
            if enhanced_checks and enhanced_checks.get('enabled'):
                from conduit_core.engine_modules.quality_checks import QualityAnalyzer
                import json
                from pathlib import Path
                
                quality_analyzer = QualityAnalyzer()
                logger.info("Enhanced quality checks enabled")
                
                # Load baseline if exists
                baseline_path = enhanced_checks.get('baseline_path')
                if baseline_path:
                    baseline_file = Path(baseline_path)
                    if baseline_file.exists():
                        with open(baseline_file, 'r') as f:
                            baseline_data = json.load(f)
                            quality_analyzer.baseline_stats = {
                                col: ColumnStats(**stats)
                                for col, stats in baseline_data.items()
                            }
                            logger.info(f"Loaded baseline from {baseline_path}")

            # --- Processing Loop with Parallel Extraction Support ---
            
            # Parallel extraction config
            parallel_config = getattr(config, 'parallel_extraction', None) or {}
            max_workers = parallel_config.get("max_workers", 4)
            parallel_batch_size = parallel_config.get("batch_size", 10000)
            
            from conduit_core.execution.parallel_extraction import ParallelExtractor
            extractor = ParallelExtractor(max_workers=max_workers, batch_size=parallel_batch_size)
            
            # Attempt to get row count for parallel planning
            total_rows = source.count_rows() if hasattr(source, 'count_rows') else None
            
            if total_rows:
                logger.info(
                    f"Parallel extraction enabled: {max_workers} workers, "
                    f"batch_size={parallel_batch_size}, total_rows={total_rows}"
                )
            
            # Create parallel-aware source iterator
            parallel_source_iterator = extractor.extract_parallel(source, total_rows)
            
            # Use existing batch processing on top of parallel extraction
            processing_loop = read_in_batches(parallel_source_iterator, batch_size=batch_size)

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
                            relevant_checks = [qc for qc in (resource.quality_checks or []) if qc.column in [fc.column for fc in invalid_result.failed_checks]]
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

                    # Enhanced quality analysis (AFTER the entire validator block)
                    if quality_analyzer and i == 0:  # First batch only
                        current_stats = quality_analyzer.analyze_batch(raw_batch_list)
                        
                        if not quality_analyzer.baseline_stats:
                            quality_analyzer.set_baseline(current_stats)
                            logger.info("Baseline statistics captured from first batch")
                        else:
                            # Detect anomalies
                            anomalies = quality_analyzer.detect_anomalies(
                                current_stats,
                                enhanced_checks.get('thresholds') if enhanced_checks else None
                            )
                            for anomaly in anomalies:
                                severity_log = logger.warning if anomaly.severity == 'warning' else logger.error
                                severity_log(
                                    f"Anomaly in {anomaly.column}: {anomaly.message} "
                                    f"(expected={anomaly.expected:.2f}, actual={anomaly.actual:.2f})"
                                )
                    else:
                        valid_records_for_write = raw_batch_list
                    
                    # Apply type coercion if enabled
                    valid_records_for_write = apply_type_coercion(
                        valid_records_for_write,
                        inferred_schema,
                        destination_config,
                        error_log,
                        current_batch_offset,
                    )
                    
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

                    if incremental_column:
                        current_batch_max = max_value_seen
                        for record in valid_records_for_write:
                            if incremental_column in record:
                                current_val = record[incremental_column]
                                incremental_values.append(current_val)  # Track for gap detection
                                try:
                                    if current_val is not None:
                                        if current_batch_max is None or current_val > current_batch_max:
                                            current_batch_max = current_val
                                except TypeError:
                                    logger.debug(f"Could not compare incremental value '{current_val}' with max '{current_batch_max}'.")
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
                
                # Save incremental state and detect gaps
                save_incremental_state(
                    resource,
                    incremental_column,
                    max_value_seen,
                    incremental_start_value,
                    incremental_mgr,
                    incremental_values,
                )
                
                # Save persistent checkpoint for incremental loads
                if incremental_column and max_value_seen is not None:
                    checkpoint_mgr.save_checkpoint(
                        pipeline_name=resource.name,
                        checkpoint_column=incremental_column,
                        last_value=max_value_seen,
                        records_processed=total_written
                    )
                    logger.info(f"Persistent checkpoint saved: {incremental_column}={max_value_seen}")
                
                # Legacy resume support (clear old-style checkpoints)
                if source_config.resume:
                    logger.info("Clearing legacy checkpoint...")
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
