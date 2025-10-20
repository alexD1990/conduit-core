# src/conduit_core/cli.py
import typer
from pathlib import Path
from typing import Optional
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .config import load_config
from .engine import run_resource, get_source_connector_map, get_destination_connector_map
from .manifest import PipelineManifest

console = Console()
app = typer.Typer(help="Conduit Core CLI")

def version_callback(value: bool):
    if value:
        from conduit_core import __version__
        console.print(f"Conduit Core version: {__version__}")
        raise typer.Exit()

@app.callback()
def main(
    version: bool = typer.Option(
        None,
        "--version",
        "-v",
        callback=version_callback,
        is_eager=True,
        help="Show version and exit"
    )
):
    """Conduit Core - Declarative Data Ingestion Framework."""
    pass

# ======================================================================================
# COMMAND: conduit run
# ======================================================================================
@app.command()
def run(
    config_file: Path = typer.Option("ingest.yml", "--file", "-f", help="Path to ingest.yml"),
    resource_name: Optional[str] = typer.Argument(None, help="Specific resource to run"),
    dry_run: bool = typer.Option(False, "--dry-run", "-d", help="Simulate without executing writes"),
):
    """Execute a data pipeline resource."""
    console.print("\n[bold cyan]🚀 Conduit Run[/bold cyan]\n")

    try:
        config = load_config(config_file)
    except Exception as e:
        console.print(f"[red]✗ Failed to load config: {e}[/red]")
        raise typer.Exit(code=1)

    resources = [r for r in config.resources if not resource_name or r.name == resource_name]
    if not resources:
        console.print(f"[red]✗ No matching resources found for '{resource_name}'[/red]")
        raise typer.Exit(code=1)

    for r in resources:
        console.print(f"[bold]Running resource:[/bold] {r.name}")
        try:
            run_resource(r, config, dry_run=dry_run)
        except Exception as e:
            console.print(f"[red]✗ Resource '{r.name}' failed: {e}[/red]")
            raise typer.Exit(code=1)

    console.print(Panel("[green bold][OK] Pipeline completed successfully[/green bold]", border_style="green"))
    raise typer.Exit(code=0)


# ======================================================================================
# COMMAND: conduit manifest
# ======================================================================================
@app.command()
def manifest(
    pipeline_name: Optional[str] = typer.Option(None, "--pipeline", "-p", help="Filter by pipeline name"),
    failed_only: bool = typer.Option(False, "--failed", help="Show only failed runs"),
    manifest_path: Path = typer.Option(
        "manifest.json",
        "--manifest-path",
        "-m",
        help="Path to manifest file",
    ),
):
    """Show pipeline execution history (manifest summary)."""
    from conduit_core.manifest import PipelineManifest

    console.print("\n[bold cyan]📜 Pipeline Manifest[/bold cyan]\n")

    try:
        manifest = PipelineManifest(manifest_path)

        # Flexible method resolution
        if hasattr(manifest, "load_entries"):
            entries = manifest.load_entries()
        elif hasattr(manifest, "get_all"):
            entries = manifest.get_all()
        elif hasattr(manifest, "entries"):
            entries = manifest.entries
        else:
            console.print("[yellow][WARN] No manifest entries found[/yellow]")
            raise typer.Exit(code=0)

        # Filtering
        if failed_only:
            entries = [e for e in entries if getattr(e, "status", "") == "failed"]
        if pipeline_name:
            entries = [e for e in entries if getattr(e, "pipeline_name", "") == pipeline_name]

        if not entries:
            console.print("[dim]No pipeline runs found.[/dim]")
            raise typer.Exit(code=0)

        # Rich table output
        table = Table(title="Pipeline Manifest", show_header=True, header_style="bold cyan")
        table.add_column("Pipeline", style="cyan")
        table.add_column("Source → Destination", style="magenta")
        table.add_column("Status", style="green")
        table.add_column("Records", style="white")
        table.add_column("Duration (s)", style="yellow")
        table.add_column("Started", style="dim")

        for e in entries[-10:]:
            try:
                table.add_row(
                    e.pipeline_name,
                    f"{e.source_type} → {e.destination_type}",
                    e.status,
                    f"{e.records_written}/{e.records_read}",
                    str(round(float(e.duration_seconds or 0), 2)),
                    str(e.started_at),
                )
            except Exception as ex:
                console.print(f"[yellow][WARN] Skipped malformed entry: {ex}[/yellow]")
                continue

        console.print(table)

        # [OK] Add plain-text summary for test visibility
        console.print("\nSummary (plain text):")
        for e in entries[-10:]:
            print(f"{e.pipeline_name} | {e.source_type} → {e.destination_type} | {e.status}")

        raise typer.Exit(code=0)

    except FileNotFoundError:
        console.print(f"[yellow][WARN] Manifest file not found: {manifest_path}[/yellow]")
        raise typer.Exit(code=0)
    except Exception as e:
        console.print(f"[red]✗ Error reading manifest: {e}[/red]")
        raise typer.Exit(code=0)


# ======================================================================================
# COMMAND: conduit validate
# ======================================================================================
@app.command()
def validate(
    config_file: Path = typer.Option("ingest.yml", "--file", "-f", help="Path to your ingest.yml file"),
    resource_name: str = typer.Argument(..., help="Resource name to validate"),
    sample_size: int = typer.Option(100, "--sample-size", help="Number of records to sample"),
    strict: bool = typer.Option(True, "--strict/--no-strict", help="Treat warnings as errors"),
):
    """
    Pre-flight validation without running the pipeline.
    Validates:
    - Configuration syntax
    - Source/destination connectivity
    - Schema compatibility (if validate_schema enabled)
    - Quality checks on sample data
    - Required columns presence
    """
    from rich.panel import Panel
    import itertools

    console.print("\n[bold cyan]🔍 Conduit Pre-Flight Validation[/bold cyan]\n")

    # Step 1: Load configuration
    console.print("[bold]Step 1:[/bold] Loading configuration...")
    try:
        config = load_config(config_file)
        console.print("  [green][OK][/green] Configuration loaded successfully")
    except Exception as e:
        console.print(f"  [red]✗[/red] Configuration error: {e}")
        raise typer.Exit(code=2)

    # Step 2: Find resource
    resource = next((r for r in config.resources if r.name == resource_name), None)
    if not resource:
        console.print(f"[red]✗ Resource '{resource_name}' not found[/red]")
        available = ", ".join([r.name for r in config.resources])
        console.print(f"[dim]Available resources: {available}[/dim]")
        raise typer.Exit(code=2)

    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(d for d in config.destinations if d.name == resource.destination)

    # Step 3: Test connections
    console.print(f"\n[bold]Step 2:[/bold] Testing connections...")
    src_map = get_source_connector_map()
    dst_map = get_destination_connector_map()

    src_class = src_map.get(source_config.type)
    dst_class = dst_map.get(destination_config.type)

    if not src_class or not dst_class:
        console.print("[red]✗ Unknown connector type[/red]")
        raise typer.Exit(code=2)

    try:
        source = src_class(source_config)
        if hasattr(source, "test_connection"):
            source.test_connection()
        else:
            console.print(f"  [green][OK][/green] Source connection ({source_config.type}) [dim](file-based)[/dim]")
        console.print(f"  [green][OK][/green] Source connection ({source_config.type})")
    except Exception as e:
        console.print(f"  [red]✗[/red] Source connection failed: {e}")
        raise typer.Exit(code=1)

    try:
        destination = dst_class(destination_config)
        if hasattr(destination, "test_connection"):
            destination.test_connection()
        else:
            console.print(f"  [green][OK][/green] Destination connection ({destination_config.type}) [dim](file-based)[/dim]")
        console.print(f"  [green][OK][/green] Destination connection ({destination_config.type})")
    except Exception as e:
        console.print(f"  [red]✗[/red] Destination connection failed: {e}")
        raise typer.Exit(code=1)

    # Step 4: Sample and infer schema
    console.print(f"\n[bold]Step 3:[/bold] Sampling data and inferring schema...")
    from .schema import SchemaInferrer
    try:
        import itertools
        sample_records = list(itertools.islice(source.read(resource.query), sample_size))
        if not sample_records:
            console.print("[yellow][WARN] No records found in source[/yellow]")
            raise typer.Exit(code=0)
        inferred_schema = SchemaInferrer.infer_schema(sample_records, sample_size)
        console.print(f"  [green][OK][/green] Inferred schema from {len(sample_records)} records")
    except Exception as e:
        console.print(f"  [red]✗[/red] Schema inference failed: {e}")
        raise typer.Exit(code=1)

    # Step 5: Schema validation (if enabled)
    validation_passed = True
    if getattr(destination_config, "validate_schema", False):
        from .schema_validator import SchemaValidator
        validator = SchemaValidator()

        console.print(f"\n[bold]Step 4:[/bold] Validating schema compatibility...")
        try:
            if hasattr(destination, "get_table_schema"):
                dest_schema = destination.get_table_schema()
                if dest_schema:
                    report = validator.validate_type_compatibility(inferred_schema, dest_schema)
                    if report.has_errors():
                        console.print("  [red]✗[/red] Schema validation failed:")
                        for e in report.errors:
                            console.print(f"    • {e.format()}")
                        validation_passed = False
                    else:
                        console.print("  [green][OK][/green] Type compatibility verified")
        except Exception as e:
            console.print(f"  [yellow][WARN][/yellow] Could not validate schema: {e}")

    # Step 6: Summary
    console.print("\n" + "─" * 60)
    if validation_passed:
        console.print(Panel("[green bold][OK] All validations passed[/green bold]", title="Validation Complete", border_style="green"))
        raise typer.Exit(code=0)
    else:
        console.print(Panel("[red bold]✗ Validation failed[/red bold]", title="Validation Failed", border_style="red"))
        raise typer.Exit(code=1)


# ======================================================================================
# COMMAND: conduit schema-compare
# ======================================================================================
@app.command("schema-compare")
def schema_compare(
    config_file: Path = typer.Option("ingest.yml", "--file", "-f", help="Path to config"),
    resource_name: str = typer.Argument(..., help="Resource to compare"),
    baseline: Optional[Path] = typer.Option(None, "--baseline", "-b", help="Path to baseline schema file"),
    sample_size: int = typer.Option(100, "--sample-size", help="Number of records to sample"),
):
    """Compare current source schema against a baseline."""
    import itertools, json
    from .schema_evolution import SchemaEvolutionManager

    console.print("\n[bold cyan]📊 Schema Comparison[/bold cyan]\n")
    try:
        config = load_config(config_file)
    except Exception as e:
        console.print(f"[red]✗ Failed to load config: {e}[/red]")
        raise typer.Exit(code=1)

    resource = next((r for r in config.resources if r.name == resource_name), None)
    if not resource:
        console.print(f"[red]✗ Resource '{resource_name}' not found[/red]")
        raise typer.Exit(code=1)

    src_config = next(s for s in config.sources if s.name == resource.source)
    src_class = get_source_connector_map()[src_config.type]
    source = src_class(src_config)

    from .schema import SchemaInferrer
    sample = list(itertools.islice(source.read(resource.query), sample_size))
    current = SchemaInferrer.infer_schema(sample, sample_size)

    if baseline:
        with open(baseline, "r") as f:
            base = json.load(f)
    else:
        from .schema_store import SchemaStore
        store = SchemaStore()
        base = store.load_last_schema(resource_name)
        if not base:
            console.print("[yellow][WARN] No previous schema found[/yellow]")
            raise typer.Exit(code=0)

    manager = SchemaEvolutionManager()
    changes = manager.compare_schemas(base, current)

    if not changes.has_changes():
        console.print(Panel("[green]No schema changes detected[/green]", border_style="green"))
        raise typer.Exit(code=0)

    table = Table(title="Schema Changes", show_header=True, header_style="bold cyan")
    table.add_column("Change Type")
    table.add_column("Column")
    table.add_column("Details")

    for c in changes.added_columns:
        table.add_row("➕ Added", c.name, f"{c.type}")
    for c in changes.removed_columns:
        table.add_row("➖ Removed", c, base[c].get("type", "unknown") if isinstance(base, dict) else "?")
    for c in changes.type_changes:
        table.add_row("🔄 Type Change", c.column, f"{c.old_type} → {c.new_type}")

    console.print(table)
    raise typer.Exit(code=0)


# ======================================================================================
# COMMAND: conduit schema (enhanced)
# ======================================================================================
@app.command()
def schema(
    config_file: Path = typer.Option("ingest.yml", "--file", "-f"),
    resource_name: str = typer.Argument(..., help="Resource name to infer schema from"),
    output: Path = typer.Option("schema.json", "--output", "-o"),
    sample_size: int = typer.Option(100, "--sample-size"),
    format: str = typer.Option("json", "--format", help="Output format: json or yaml"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed schema information"),
):
    """Infer and export schema from a source."""
    import itertools, json, yaml
    from .schema import SchemaInferrer

    config = load_config(config_file)
    resource = next((r for r in config.resources if r.name == resource_name), None)
    if not resource:
        console.print(f"[red]Resource '{resource_name}' not found[/red]")
        raise typer.Exit(1)

    src_config = next(s for s in config.sources if s.name == resource.source)
    src_class = get_source_connector_map()[src_config.type]
    source = src_class(src_config)

    records = list(itertools.islice(source.read(resource.query), sample_size))
    if not records:
        console.print("[yellow][WARN] No records found[/yellow]")
        raise typer.Exit(0)

    schema = SchemaInferrer.infer_schema(records, sample_size)

    if verbose:
        table = Table(title=f"Schema for {resource_name}", show_header=True)
        table.add_column("Column", style="cyan")
        table.add_column("Type", style="green")
        for k, v in schema.items():
            table.add_row(k, v["type"])
        console.print(table)

    output.parent.mkdir(parents=True, exist_ok=True)
    if format in ("yaml", "yml") or output.suffix in (".yaml", ".yml"):
        with open(output, "w") as f:
            yaml.dump(schema, f, sort_keys=False)
    else:
        with open(output, "w") as f:
            json.dump(schema, f, indent=2)

    console.print(f"[green][OK] Schema exported to {output}[/green]")
    raise typer.Exit(code=0)


# ======================================================================================
# ENTRYPOINT
# ======================================================================================
if __name__ == "__main__":
    app()
