# src/conduit_core/engine.py
import time
import sys
import os
from pathlib import Path
from typing import Optional
from rich import print
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
)

from .config import IngestConfig, Resource
from .state import load_state, save_state
from .errors import ErrorLog
from .batch import read_in_batches
from .logging_utils import ConduitLogger
from .connectors.registry import get_source_connector_map, get_destination_connector_map
from .manifest import PipelineManifest, ManifestTracker
from .checkpoint import CheckpointManager


def run_resource(
    resource: Resource,
    config: IngestConfig,
    batch_size: int = 1000,
    manifest_path: Optional[Path] = None,
    dry_run: bool = False,
):
    """Kjører en enkelt dataflyt-ressurs med alle funksjoner."""

    logger = ConduitLogger(resource.name)
    logger.start_resource()

    if dry_run:
        logger.info("DRY RUN MODE - Data will be read but not written", prefix="⚠")

    manifest = PipelineManifest(manifest_path)
    checkpoint_mgr = CheckpointManager()
    source_config = next(s for s in config.sources if s.name == resource.source)
    destination_config = next(d for d in config.destinations if d.name == resource.destination)

    with ManifestTracker(
        manifest=manifest, pipeline_name=resource.name,
        source_type=source_config.type, destination_type=destination_config.type,
    ) as tracker:
        total_processed = 0
        total_successful = 0
        error_log = ErrorLog(resource.name)
        
        try:
            # --- Setup Phase ---
            last_checkpoint_value = None
            if source_config.resume and source_config.checkpoint_column:
                checkpoint = checkpoint_mgr.load_checkpoint(resource.name)
                if checkpoint:
                    last_checkpoint_value = checkpoint['last_value']
                    total_processed = checkpoint.get('records_processed', 0)
                    logger.info(f"Resuming from checkpoint: {source_config.checkpoint_column} > {last_checkpoint_value}")

            current_state = load_state()
            last_value = current_state.get(resource.name, 0)
            final_query = resource.query.replace(":last_value", str(last_value))
            
            if last_checkpoint_value is not None:
                wrapped_value = f"'{last_checkpoint_value}'" if isinstance(last_checkpoint_value, str) else last_checkpoint_value
                if "WHERE" in final_query.upper(): final_query += f" AND {source_config.checkpoint_column} > {wrapped_value}"
                else: final_query += f" WHERE {source_config.checkpoint_column} > {wrapped_value}"

            source = get_source_connector_map().get(source_config.type)(source_config)
            destination = get_destination_connector_map().get(destination_config.type)(destination_config)
            destination.mode = resource.mode or destination_config.mode or "append"
            tracker.metadata = {"mode": destination.mode}
            supports_write_one = hasattr(destination, "write_one") and callable(getattr(destination, "write_one"))

            # --- CRITICAL FIX: Initialize max_value_seen ---
            max_value_seen = last_value
            current_checkpoint_max = last_checkpoint_value

            estimated_total = source.estimate_total_records()

            show_progress = not dry_run and sys.stdout.isatty() and os.getenv('CONDUDUIT_NO_PROGRESS') != '1'

            # --- Processing Loop ---
            processing_loop = read_in_batches(source.read(final_query), batch_size=batch_size)
            if show_progress:
                with Progress(
                    TextColumn("[progress.description]{task.description}"), BarColumn(),
                    TaskProgressColumn(), MofNCompleteColumn(), TimeRemainingColumn()
                ) as progress:
                    task = progress.add_task(f"Processing {resource.name}", total=estimated_total)
                    for batch in processing_loop:
                        total_processed, total_successful = _process_batch(
                            batch, destination, error_log, total_processed, total_successful,
                            dry_run, supports_write_one, resource, max_value_seen,
                            checkpoint_mgr, source_config, current_checkpoint_max
                        )
                        progress.update(task, completed=total_processed)
            else:
                logger.info(f"Processing in batches (batch_size={batch_size})...", prefix="→")
                for i, batch in enumerate(processing_loop):
                    total_processed, total_successful = _process_batch(
                        batch, destination, error_log, total_processed, total_successful,
                        dry_run, supports_write_one, resource, max_value_seen,
                        checkpoint_mgr, source_config, current_checkpoint_max
                    )
                    if not dry_run: logger.info(f"Batch {i+1} processed (Total: {total_processed})")
            # --- End Loop ---

            if hasattr(destination, "finalize") and not dry_run:
                destination.finalize()

            if not dry_run:
                if error_log.has_errors(): error_log.save()
                if total_successful > 0 and resource.incremental_column and max_value_seen > last_value:
                    save_state({**current_state, resource.name: max_value_seen})
                if source_config.resume:
                    checkpoint_mgr.clear_checkpoint(resource.name)
            
            total_failed = error_log.error_count()
            tracker.records_read = total_processed
            tracker.records_written = total_successful if not dry_run else 0
            tracker.records_failed = total_failed
            logger.complete_resource(total_processed, total_successful, total_failed, dry_run=dry_run)
        except Exception as e:
            if source_config.resume and not dry_run: logger.warning("Pipeline failed, checkpoint preserved.")
            logger.error(f"Resource '{resource.name}' failed: {e}")
            raise

def _process_batch(batch, destination, error_log, total_processed, total_successful, dry_run, supports_write_one, resource, max_value_seen, checkpoint_mgr, source_config, current_checkpoint_max):
    """Helper function to process a single batch of records."""
    successful_in_batch = 0
    records_in_this_batch = 0
    successful_batch_for_writing = []

    for record in batch:
        records_in_this_batch += 1
        try:
            if not isinstance(record, dict): raise ValueError("Record is not a dictionary")
            
            # --- CRITICAL FIX: Restore incremental_column logic ---
            if resource.incremental_column and resource.incremental_column in record:
                max_value_seen = max(max_value_seen, int(record[resource.incremental_column]))

            if dry_run:
                successful_in_batch += 1
            elif supports_write_one:
                destination.write_one(record)
                successful_in_batch += 1
            else:
                successful_batch_for_writing.append(record)
        except Exception as e:
            error_log.add_error(record, e, row_number=(total_processed + records_in_this_batch))
    
    if not dry_run and not supports_write_one and successful_batch_for_writing:
        try:
            destination.write(successful_batch_for_writing)
            successful_in_batch = len(successful_batch_for_writing)
        except Exception as e:
            for record in successful_batch_for_writing:
                error_log.add_error(record, e, row_number=(total_processed + records_in_this_batch))
    
    total_processed += records_in_this_batch
    total_successful += successful_in_batch
    
    # Checkpoint logic remains similar, handled after the batch
    
    return total_processed, total_successful