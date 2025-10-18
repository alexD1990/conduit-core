# src/conduit_core/cli.py

import logging
import time
import os
import typer
import itertools
from pathlib import Path
from typing import Optional
from rich import print
from rich.console import Console
from rich.table import Table

from .config import load_config
from .engine import run_resource
from .logging_utils import print_header, print_summary
from .errors import ConnectionError

app = typer.Typer()

@app.command()
def validate(
    config_file: Path = typer.Option(
        "ingest.yml",
        "--file",
        "-f",
        help="Stien til din ingest.yml fil.",
    )
):
    """Validerer konfigurasjonsfilen ingest.yml."""
    if not config_file.is_file():
        print(f"❌ [bold red]Feil:[/bold red] Filen '{config_file}' ble ikke funnet.")
        raise typer.Exit(code=1)

    print(f"🔍 Validerer konfigurasjonsfil: [bold green]{config_file}[/bold green]")

    try:
        config = load_config(config_file)
        print("✅ Konfigurasjon er gyldig!")
    except Exception as e:
        print(f"❌ [bold red]Feil i konfigurasjon:[/bold red]\n{e}")

@app.command()
def test(
    config_file: Path = typer.Option(
        "ingest.yml",
        "--file",
        "-f",
        help="Path to configuration file"
    )
):
    """Test all connections before running pipeline."""
    console = Console()
    try:
        config = load_config(config_file)
    except Exception as e:
        console.print(f"[red]❌ Config error:[/red] {e}")
        raise typer.Exit(code=1)

    console.print("\n[bold]Testing Connections...[/bold]\n")
    all_passed = True

    from conduit_core.connectors.registry import get_source_connector_map, get_destination_connector_map
    source_map = get_source_connector_map()
    dest_map = get_destination_connector_map()

    for source_config in config.sources:
        SourceClass = source_map.get(source_config.type)
        if not SourceClass:
            console.print(f"[yellow]⚠[/yellow]  Source '{source_config.name}': Unknown type '{source_config.type}'")
            continue
        try:
            source = SourceClass(source_config)
            source.test_connection()
            console.print(f"[green]✓[/green]  Source '{source_config.name}' ({source_config.type})")
        except ConnectionError as e:
            console.print(f"[red]✗[/red]  Source '{source_config.name}' failed:\n[dim]{e}[/dim]\n")
            all_passed = False
        except Exception as e:
            console.print(f"[red]✗[/red]  Source '{source_config.name}': Unexpected error: {e}")
            all_passed = False

    for dest_config in config.destinations:
        DestClass = dest_map.get(dest_config.type)
        if not DestClass:
            console.print(f"[yellow]⚠[/yellow]  Destination '{dest_config.name}': Unknown type '{dest_config.type}'")
            continue
        try:
            dest = DestClass(dest_config)
            dest.test_connection()
            console.print(f"[green]✓[/green]  Destination '{dest_config.name}' ({dest_config.type})")
        except ConnectionError as e:
            console.print(f"[red]✗[/red]  Destination '{dest_config.name}' failed:\n[dim]{e}[/dim]\n")
            all_passed = False
        except Exception as e:
            console.print(f"[red]✗[/red]  Destination '{dest_config.name}': Unexpected error: {e}")
            all_passed = False

    if all_passed:
        console.print("\n[green bold]✅ All connections successful![/green bold]")
    else:
        console.print("\n[red bold]❌ Some connections failed.[/red bold]")
        raise typer.Exit(code=1)

@app.command()
def schema(
    config_file: Path = typer.Option("ingest.yml", "--file", "-f"),
    resource_name: str = typer.Argument(..., help="Resource name to infer schema from"),
    output: Path = typer.Option("schema.json", "--output", "-o"),
    sample_size: int = typer.Option(100, "--sample-size"),
):
    """Infer and export schema from a source."""
    from .schema import SchemaInferrer
    from .connectors.registry import get_source_connector_map
    import json
    
    console = Console()
    config = load_config(config_file)
    resource = next((r for r in config.resources if r.name == resource_name), None)
    if not resource:
        console.print(f"[red]Resource '{resource_name}' not found[/red]")
        raise typer.Exit(1)
        
    source_config = next(s for s in config.sources if s.name == resource.source)
    source_class = get_source_connector_map()[source_config.type]
    source = source_class(source_config)
        
    console.print(f"Sampling {sample_size} records...")
    records = list(itertools.islice(source.read(resource.query), sample_size))
    schema = SchemaInferrer.infer_schema(records, sample_size)
        
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, 'w') as f:
        json.dump(schema, f, indent=2)
        
    console.print(f"[green]✓[/green] Schema exported to {output}")

@app.command()
def run(
    config_file: Path = typer.Option(
        "ingest.yml",
        "--file",
        "-f",
        help="Stien til din ingest.yml fil.",
    ),
    batch_size: int = typer.Option(
        1000,
        "--batch-size",
        "-b",
        help="Antall records per batch (default: 1000)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview pipeline without writing data",
    ),
    no_progress: bool = typer.Option(
        False,
        "--no-progress",
        help="Disable progress bars (useful for logs/CI)",
    ),
):
    """Kjører data-innsamlingen basert på ingest.yml."""
    if no_progress:
        os.environ['CONDUIT_NO_PROGRESS'] = '1'

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    if dry_run:
        print("\n[bold yellow]🔍 DRY RUN MODE - No data will be written[/bold yellow]\n")

    pipeline_start = time.time()
    try:
        config = load_config(config_file)
        print_header()
        for resource in config.resources:
            run_resource(resource, config, batch_size=batch_size, dry_run=dry_run)
        pipeline_elapsed = time.time() - pipeline_start
        print_summary(len(config.resources), pipeline_elapsed)
    except Exception as e:
        print(f"\n❌ [bold red]Pipeline failed:[/bold red]")
        print(e)
        raise typer.Exit(code=1)

@app.command()
def manifest(
    pipeline_name: Optional[str] = typer.Option(None, help="Filter by pipeline name"),
    failed_only: bool = typer.Option(False, "--failed", help="Show only failed runs"),
    manifest_path: Path = typer.Option("manifest.json", help="Path to manifest file"),
):
    """Show pipeline execution history."""
    from conduit_core.manifest import PipelineManifest
    
    manifest = PipelineManifest(manifest_path)
    if failed_only:
        entries = manifest.get_failed_runs()
    elif pipeline_name:
        entries = manifest.get_all(pipeline_name)
    else:
        entries = manifest.get_all()

    if not entries:
        typer.echo("No pipeline runs found.")
        return

    console = Console()
    table = Table(title="Pipeline Execution History")
    # ... (rest of manifest command is unchanged)
    table.add_column("Pipeline", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Records", justify="right")
    table.add_column("Failed", justify="right")
    table.add_column("Duration", justify="right")
    table.add_column("Completed At")

    for entry in entries[-20:]:
        status_style = "green" if entry.status == "success" else "red"
        if entry.status == "partial":
            status_style = "yellow"
        table.add_row(
            entry.pipeline_name,
            f"[{status_style}]{entry.status}[/{status_style}]",
            str(entry.records_written),
            str(entry.records_failed),
            f"{entry.duration_seconds:.2f}s",
            entry.completed_at.split("T")[0],
        )
    console.print(table)


@app.command()
def checkpoints():
    """List all saved checkpoints."""
    from conduit_core.checkpoint import CheckpointManager
    
    mgr = CheckpointManager()
    checkpoints = mgr.list_checkpoints()

    if not checkpoints:
        typer.echo("No checkpoints found.")
        return

    console = Console()
    table = Table(title="Saved Checkpoints")
    # ... (rest of checkpoints command is unchanged)
    table.add_column("Pipeline", style="cyan")
    table.add_column("Column", style="green")
    table.add_column("Last Value", justify="right")
    table.add_column("Records", justify="right")
    table.add_column("Timestamp")

    for cp in checkpoints:
        table.add_row(
            cp['pipeline_name'],
            cp['checkpoint_column'],
            str(cp['last_value']),
            str(cp['records_processed']),
            cp['timestamp'].split('T')[0]
        )
    console.print(table)

@app.command()
def clear_checkpoints(
    pipeline_name: Optional[str] = typer.Option(None, help="Clear specific pipeline checkpoint")
):
    """Clear saved checkpoints."""
    from conduit_core.checkpoint import CheckpointManager

    mgr = CheckpointManager()
    if pipeline_name:
        if mgr.clear_checkpoint(pipeline_name):
            typer.echo(f"✅ Cleared checkpoint for: {pipeline_name}")
        else:
            typer.echo(f"❌ No checkpoint found for: {pipeline_name}")
    else:
        checkpoints = mgr.list_checkpoints()
        for cp in checkpoints:
            mgr.clear_checkpoint(cp['pipeline_name'])
        typer.echo(f"✅ Cleared {len(checkpoints)} checkpoint(s)")

if __name__ == "__main__":
    app()