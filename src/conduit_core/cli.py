# src/conduit_core/cli.py

import typer
from pathlib import Path
from rich import print

# Riktige importer med punktum foran
from .config import load_config
from .engine import run_resource

app = typer.Typer()

@app.command()
def validate(
    config_file: Path = typer.Option(
        "ingest.yml", 
        "--file", 
        "-f", 
        help="Stien til din ingest.yml fil."
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
        help="Stien til din ingest.yml fil."
    )
):
    """Kjører data-innsamlingen basert på ingest.yml."""
    config = load_config(config_file)
    print("🚀 Starter Conduit Core run...")
    for resource in config.resources:
        run_resource(resource, config)
    print("✨ Conduit Core run fullført!")

if __name__ == "__main__":
    app()