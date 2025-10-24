# src/conduit_core/cli.py
import typer
from pathlib import Path
from typing import Optional
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .config import load_config
from .engine import run_resource, get_source_connector_map, get_destination_connector_map, run_preflight
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
    config_file: Path = typer.Argument("ingest.yml", help="Path to ingest.yml"),
    resource_name: Optional[str] = typer.Option(None, "--resource", "-r", help="Specific resource to run"),
    dry_run: bool = typer.Option(False, "--dry-run", "-d", help="Simulate without executing writes"),
    skip_preflight: bool = typer.Option(False, "--skip-preflight", help="Skip preflight checks"),
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
            run_resource(r, config, dry_run=dry_run, skip_preflight=skip_preflight)
        except Exception as e:
            console.print(f"[red]✗ Resource '{r.name}' failed: {e}[/red]")
            raise typer.Exit(code=1)

    console.print(Panel("[green bold][OK] Pipeline completed successfully[/green bold]", border_style="green"))
    raise typer.Exit(code=0)

# ======================================================================================
# COMMAND: conduit preflight
# ======================================================================================
@app.command()
def preflight(
    config_file: Path = typer.Argument("ingest.yml", help="Path to ingest.yml"),
    resource_name: Optional[str] = typer.Option(None, "--resource", "-r", help="Specific resource to check"),
):
    """Run preflight health checks without executing pipeline."""
    from .engine import run_preflight
    
    try:
        passed = run_preflight(str(config_file), resource_name=resource_name, verbose=True)
    except typer.Exit:
        raise  # Re-raise typer.Exit without catching it
    except Exception as e:
        console.print(f"[red]✗ Preflight failed: {e}[/red]")
        raise typer.Exit(code=1)
    
    # Exit with appropriate code
    raise typer.Exit(code=0 if passed else 1)
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
