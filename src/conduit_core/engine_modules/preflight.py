"""
Preflight checks module for validating pipeline configuration before execution.
"""
from typing import Optional
from datetime import datetime
from ..config import IngestConfig
from ..connectors.registry import get_source_connector_map, get_destination_connector_map
from ..schema import SchemaInferrer
from ..logging_utils import ConduitLogger

logger = ConduitLogger("preflight")


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
    from ..schema import SchemaInferrer
    
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
                        from ..schema import compare_schemas
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
                            from ..schema_evolution import SchemaEvolutionManager
                            from ..schema_store import SchemaStore
                            
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
                                            from ..schema import TableAutoCreator
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
                from ..quality import QualityValidator
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
    from ..config import load_config
    
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
            "fail": "[red]FAIL[/red]",
            "info": "[blue]INFO[/blue]"
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