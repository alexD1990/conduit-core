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
    # Disable standard logging to stdout (we use our custom logger)
    logging.basicConfig(
        level=logging.WARNING,  # Only show warnings and errors from standard logging
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    pipeline_start = time.time()

    try:
        config = load_config(config_file)

        # Print header
        print_header()

        # Run all resources
        for resource in config.resources:
            run_resource(resource, config, batch_size=batch_size)

        # Print summary
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

    for entry in entries[-20:]:  # Last 20 runs
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


# VIKTIG: Sørg for at disse to linjene er med helt til slutt, uten innrykk
if __name__ == "__main__":
    app()