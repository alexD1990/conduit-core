# src/conduit_core/cli.py

import typer
import logging
from pathlib import Path
from .config import load_config
from .engine import run_resource

# Configure logging as the first action
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# This is the main application object
app = typer.Typer()

@app.command()
def validate(
    config_file: Path = typer.Option(
        "ingest.yml", 
        "--file", 
        "-f", 
        help="Path to your ingest.yml file."
    )
):
    """Validates the ingest.yml configuration file."""
    # This command is not fully implemented yet, but it's here as a placeholder
    logging.info(f"Validating {config_file}...")
    # Add validation logic here in the future
    logging.info("Validation complete (not yet implemented).")

@app.command()
def run(
    config_file: Path = typer.Option(
        "ingest.yml", 
        "--file", 
        "-f", 
        help="Path to your ingest.yml file."
    )
):
    """Runs the data ingestion based on the ingest.yml file."""
    try:
        config = load_config(config_file)
        logging.info("🚀 Starting Conduit Core run...")
        for resource in config.resources:
            run_resource(resource, config)
        logging.info("✨ Conduit Core run complete!")

    except Exception as e:
        logging.error("❌ An error occurred during the run:")
        logging.error(e, exc_info=True)
        raise typer.Exit(code=1)

# This function is not called directly by the script entry point,
# but it's what the entry point in pyproject.toml ("conduit_core.cli:app") refers to.
def main():
    app()