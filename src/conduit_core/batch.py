# src/conduit_core/batch.py

import logging
from typing import Iterable, Dict, Any, List, Callable

logger = logging.getLogger(__name__)


def read_in_batches(
    source_iterable: Iterable[Dict[str, Any]], 
    batch_size: int = 1000
) -> Iterable[List[Dict[str, Any]]]:
    """
    Leser data i batches for å unngå å laste alt i minnet.
    
    Args:
        source_iterable: En iterable som yielder records (f.eks. source.read())
        batch_size: Antall records per batch
    
    Yields:
        Lister med records (batches)
    
    Example:
        for batch in read_in_batches(source.read(), batch_size=500):
            destination.write(batch)
    """
    batch = []
    record_count = 0
    
    for record in source_iterable:
        batch.append(record)
        record_count += 1
        
        if len(batch) >= batch_size:
            logger.debug(f"Yielding batch of {len(batch)} records")
            yield batch
            batch = []
    
    # Yield siste batch hvis den ikke er tom
    if batch:
        logger.debug(f"Yielding final batch of {len(batch)} records")
        yield batch


def process_batches_with_callback(
    source_iterable: Iterable[Dict[str, Any]],
    batch_size: int,
    process_fn: Callable[[List[Dict[str, Any]]], None],
    on_batch_complete: Callable[[int, int], None] = None
) -> int:
    """
    Prosesserer data i batches med callback-støtte.
    
    Args:
        source_iterable: Iterable som yielder records
        batch_size: Størrelse på hver batch
        process_fn: Funksjon som prosesserer én batch
        on_batch_complete: Optional callback som kalles etter hver batch
                          Får (batch_number, total_records_so_far) som args
    
    Returns:
        Totalt antall records prosessert
    
    Example:
        def write_batch(batch):
            destination.write(batch)
        
        def checkpoint(batch_num, total):
            save_checkpoint(batch_num, total)
        
        total = process_batches_with_callback(
            source.read(),
            batch_size=1000,
            process_fn=write_batch,
            on_batch_complete=checkpoint
        )
    """
    batch_number = 0
    total_records = 0
    
    for batch in read_in_batches(source_iterable, batch_size):
        batch_number += 1
        batch_size_actual = len(batch)
        
        # Prosesser batchen
        process_fn(batch)
        
        total_records += batch_size_actual
        
        # Callback etter vellykket prosessering
        if on_batch_complete:
            on_batch_complete(batch_number, total_records)
        
        logger.info(f"Batch {batch_number} complete: {batch_size_actual} records (total: {total_records})")
    
    return total_records