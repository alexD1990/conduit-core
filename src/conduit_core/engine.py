# src/conduit_core/engine.py
import logging
from rich import print
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn, TimeRemainingColumn
from .config import IngestConfig, Resource
from .state import load_state, save_state
from .connectors.registry import get_source_connector_map, get_destination_connector_map


def run_resource(resource: Resource, config: IngestConfig):
    """Kjører en enkelt dataflyt-ressurs med state management og progress tracking."""
    logging.info(f"--- 🚀 Kjører ressurs: [bold blue]{resource.name}[/bold blue] ---")
    
    current_state = load_state()
    last_value = current_state.get(resource.name, 0)
    logging.info(f"Siste kjente verdi for '{resource.name}': {last_value}")
    logging.debug(f"Full state: {current_state}")
    
    final_query = resource.query.replace(":last_value", str(last_value))
    logging.debug(f"Final query after substitution: {final_query}")
    
    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(d for d in config.destinations if d.name == resource.destination)

    logging.debug(f"Source config: {source_config}")
    logging.debug(f"Destination config: {destination_config}")
    
    source_map = get_source_connector_map()
    destination_map = get_destination_connector_map()
    
    SourceConnector = source_map.get(source_config.type)
    DestinationConnector = destination_map.get(destination_config.type)
    
    if not SourceConnector:
        raise ValueError(f"Kilde-konnektor av typen '{source_config.type}' ble ikke funnet.")
    if not DestinationConnector:
        raise ValueError(f"Destinasjons-konnektor av typen '{destination_config.type}' ble ikke funnet.")
    
    logging.debug(f"Instantiating connectors...")
    source = SourceConnector(source_config)
    destination = DestinationConnector(destination_config)
    
    # Create progress bar
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        TextColumn("•"),
        TextColumn("[cyan]{task.fields[rows_per_sec]:.1f} rows/sec"),
    ) as progress:
        
        task = progress.add_task(
            f"[cyan]Processing {resource.name}...",
            total=None,  # Unknown total initially
            rows_per_sec=0.0
        )
        
        import time
        start_time = time.time()
        records = []
        row_count = 0
        
        # Read records with progress tracking
        for record in source.read(final_query):
            records.append(record)
            row_count += 1
            
            # Update progress every 100 rows for performance
            if row_count % 100 == 0:
                elapsed = time.time() - start_time
                rows_per_sec = row_count / elapsed if elapsed > 0 else 0
                progress.update(task, advance=100, rows_per_sec=rows_per_sec)
        
        # Final update
        elapsed = time.time() - start_time
        rows_per_sec = row_count / elapsed if elapsed > 0 else 0
        progress.update(
            task, 
            description=f"[green]✓ {resource.name}",
            completed=row_count,
            rows_per_sec=rows_per_sec
        )
    
    if not records:
        logging.info("Ingen nye rader funnet.")
        destination.write([])
    else:
        destination.write(records)
        
        if resource.incremental_column and resource.incremental_column in records[0]:
            new_max_value = max(int(r[resource.incremental_column]) for r in records)
            current_state[resource.name] = new_max_value
            save_state(current_state)
            logging.info(f"Ny state lagret for '{resource.name}': {new_max_value}")
    
    # Print summary
    elapsed = time.time() - start_time if 'start_time' in locals() else 0
    print(f"\n📊 Summary for {resource.name}:")
    print(f"   • Rows processed: {row_count}")
    print(f"   • Time elapsed: {elapsed:.2f}s")
    print(f"   • Throughput: {row_count/elapsed:.1f} rows/sec" if elapsed > 0 else "   • Throughput: N/A")
    
    logging.info(f"--- ✅ Ferdig med ressurs: [bold blue]{resource.name}[/bold blue] ---\n")

def run_resource_dry_run(resource: Resource, config: IngestConfig):
    """Dry run - viser hva som ville blitt gjort uten å skrive data."""
    print(f"\n[bold cyan]DRY RUN: {resource.name}[/bold cyan]")
    
    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(d for d in config.destinations if d.name == resource.destination)
    
    print(f"  Source: {source_config.name} ({source_config.type})")
    print(f"  Destination: {destination_config.name} ({destination_config.type})")
    print(f"  Query: {resource.query}")
    
    # Try to read and count records without writing
    source_map = get_source_connector_map()
    SourceConnector = source_map.get(source_config.type)
    
    if not SourceConnector:
        raise ValueError(f"Kilde-konnektor av typen '{source_config.type}' ble ikke funnet.")
    
    source = SourceConnector(source_config)
    
    current_state = load_state()
    last_value = current_state.get(resource.name, 0)
    final_query = resource.query.replace(":last_value", str(last_value))
    
    row_count = 0
    for _ in source.read(final_query):
        row_count += 1
    
    print(f"  [green]Would process {row_count} rows[/green]")
    print(f"  [yellow]No data written (dry run)[/yellow]\n")