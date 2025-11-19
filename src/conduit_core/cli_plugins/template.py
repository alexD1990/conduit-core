import sys
import typer
from rich.console import Console
from rich.table import Table
from pathlib import Path
from conduit_core.templates.registry import TEMPLATE_REGISTRY, CATEGORIES, get_template, load_template_yaml

console = Console()
template_app = typer.Typer(help="Generate YAML configuration templates")

# ======================================================================================
# COMMAND: list
# ======================================================================================
@template_app.command("list")
def list_templates():
    """List all available templates grouped by category."""
    console.print("[bold]AVAILABLE TEMPLATES[/bold]")
    by_category: dict[str, list[tuple[str, str]]] = {}

    for name, meta in TEMPLATE_REGISTRY.items():
        by_category.setdefault(meta["category"], []).append((name, meta["description"]))

    for category_key, category_title in CATEGORIES.items():
        rows = by_category.get(category_key, [])
        if not rows:
            continue
        table = Table(show_header=True, header_style="bold")
        table.title = category_title
        table.add_column("Template Name", style="cyan", no_wrap=True)
        table.add_column("Description", style="white")
        for name, desc in sorted(rows, key=lambda x: x[0]):
            table.add_row(name, desc)
        console.print()
        console.print(table)

    console.print("\nTip: Run `conduit template info <name>` for details")

# ======================================================================================
# COMMAND: info
# ======================================================================================
@template_app.command("info")
def template_info(name: str = typer.Argument(..., help="Template name to inspect")):
    """Show detailed information about a template."""
    if name not in TEMPLATE_REGISTRY:
        console.print(f"[red]Error: Template '{name}' not found[/red]")
        console.print("Run `conduit template list` to see available templates")
        raise typer.Exit(code=1)

    meta = get_template(name)
    console.print(f"\n[bold]TEMPLATE:[/bold] {name}")
    console.print("─" * 50)
    console.print(f"Description: {meta['description']}")
    console.print(f"Source: {meta['source_type']}")
    console.print(f"Destination: {meta['destination_type']}")

    console.print("\n[bold]CAPABILITIES[/bold]")
    for cap in meta.get("capabilities", []):
        console.print(f"• {cap}")

    console.print("\n[bold]REQUIRED CONFIGURATION[/bold]")
    for req in meta.get("required_config", []):
        console.print(f"• {req}")

    console.print(
        f"\nQuick start: `conduit template {name} > pipeline.yml` "
        "then edit fields and run `conduit run pipeline.yml`"
    )

# ======================================================================================
# COMMAND: generate
# ======================================================================================
@template_app.command("generate")
def generate(name: str = typer.Argument(..., help="Template name to output")):
    """Generate a template YAML (print to stdout)."""
    if name not in TEMPLATE_REGISTRY:
        console.print(f"[red]Error: Template '{name}' not found[/red]")
        raise typer.Exit(code=1)

    yaml_content = load_template_yaml(name)
    sys.stdout.write(yaml_content)

# ======================================================================================
# REGISTER WITH MAIN APP
# ======================================================================================
from conduit_core.cli import app
app.add_typer(template_app, name="template")
