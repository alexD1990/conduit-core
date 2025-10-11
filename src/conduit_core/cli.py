# src/conduit_core/cli.py

import logging
import typer
from pathlib import Path
from rich import print
from .config import load_config
from .engine import run_resource
from .connectors.registry import get_source_connector_map, get_destination_connector_map

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
def test(
    config_file: Path = typer.Option(
        "ingest.yml", 
        "--file", 
        "-f", 
        help="Stien til din ingest.yml fil."
    )
):
    """Tester alle tilkoblinger definert i ingest.yml."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s"
    )
    
    if not config_file.is_file():
        print(f"❌ [bold red]Feil:[/bold red] Filen '{config_file}' ble ikke funnet.")
        raise typer.Exit(code=1)

    try:
        config = load_config(config_file)
        print("🔌 Tester tilkoblinger...\n")
        
        source_map = get_source_connector_map()
        destination_map = get_destination_connector_map()
        
        all_passed = True
        
        # Test sources
        print("[bold cyan]Sources:[/bold cyan]")
        for source in config.sources:
            try:
                SourceConnector = source_map.get(source.type)
                if not SourceConnector:
                    print(f"  ❌ {source.name} ({source.type}): Connector ikke funnet")
                    all_passed = False
                    continue
                
                connector = SourceConnector(source)
                connector.test_connection()
                print(f"  ✅ {source.name} ({source.type}): Tilkoblet")
            except Exception as e:
                print(f"  ❌ {source.name} ({source.type}): {e}")
                all_passed = False
        
        # Test destinations
        print("\n[bold cyan]Destinations:[/bold cyan]")
        for destination in config.destinations:
            try:
                DestConnector = destination_map.get(destination.type)
                if not DestConnector:
                    print(f"  ❌ {destination.name} ({destination.type}): Connector ikke funnet")
                    all_passed = False
                    continue
                
                connector = DestConnector(destination)
                connector.test_connection()
                print(f"  ✅ {destination.name} ({destination.type}): Tilkoblet")
            except Exception as e:
                print(f"  ❌ {destination.name} ({destination.type}): {e}")
                all_passed = False
        
        if all_passed:
            print("\n✨ [bold green]Alle tilkoblinger vellykket![/bold green]")
        else:
            print("\n⚠️  [bold yellow]Noen tilkoblinger feilet. Se detaljer ovenfor.[/bold yellow]")
            raise typer.Exit(code=1)
            
    except Exception as e:
        print(f"❌ [bold red]En feil oppstod:[/bold red]")
        print(e)
        raise typer.Exit(code=1)

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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    try:
        config = load_config(config_file)
        print("🚀 Starter Conduit Core run...")
        for resource in config.resources:
            run_resource(resource, config)
        print("✨ Conduit Core run fullført!")

    except Exception as e:
        print(f"❌ [bold red]En feil oppstod under kjøringen:[/bold red]")
        print(e)
        raise typer.Exit(code=1)

# VIKTIG: Sørg for at disse to linjene er med helt til slutt, uten innrykk
if __name__ == "__main__":
    app()