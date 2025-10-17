# src/conduit_core/cli.py

import logging
import time
import typer
from pathlib import Path
from typing import Optional
from rich import print
from .config import load_config
from .engine import run_resource
from .logging_utils import print_header, print_summary

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
):
    """Kjører data-innsamlingen basert på ingest.yml."""
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    pipeline_start = time.time()

    try:
        config = load_config(config_file)
        print_header()
        for resource in config.resources:
            run_resource(resource, config, batch_size=batch_size)
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
    from rich.table import Table
    from rich.console import Console

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
    from rich.table import Table
    from rich.console import Console

    mgr = CheckpointManager()
    checkpoints = mgr.list_checkpoints()

    if not checkpoints:
        typer.echo("No checkpoints found.")
        return

    console = Console()
    table = Table(title="Saved Checkpoints")
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