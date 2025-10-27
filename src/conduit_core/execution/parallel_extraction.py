"""Parallel extraction with configurable workers and batch coordination."""
from typing import Any, Iterator, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ExtractionTask:
    """Single extraction task for parallel processing."""
    batch_id: int
    offset: int
    limit: int
    

class ParallelExtractor:
    """Orchestrates parallel extraction from sources."""
    
    def __init__(self, max_workers: int = 4, batch_size: int = 10000):
        self.max_workers = max_workers
        self.batch_size = batch_size
        
    def extract_parallel(
        self,
        source,
        total_rows: Optional[int] = None,
    ) -> Iterator[dict[str, Any]]:
        """
        Extract data in parallel batches.
        
        Falls back to serial if source doesn't support parallel or total_rows unknown.
        """
        if not self._can_parallelize(source, total_rows):
            logger.info("Parallel extraction not available, using serial mode")
            # Get the query from the source instance
            query = getattr(source, '_current_query', None)
            yield from source.read(query=query)
            return
            
        tasks = self._create_tasks(total_rows)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._extract_batch, source, task): task
                for task in tasks
            }
            
            for future in as_completed(futures):
                task = futures[future]
                try:
                    batch_data = future.result()
                    yield from batch_data
                except Exception as e:
                    logger.error(f"Batch {task.batch_id} failed: {e}")
                    raise
                    
    def _can_parallelize(self, source, total_rows: Optional[int]) -> bool:
        """Check if source supports parallel extraction."""
        return (
            hasattr(source, 'read_batch') and
            callable(getattr(source, 'read_batch')) and
            total_rows is not None and
            total_rows > self.batch_size
        )
        
    def _create_tasks(self, total_rows: int) -> list[ExtractionTask]:
        """Generate extraction tasks for parallel execution."""
        tasks = []
        offset = 0
        batch_id = 0
        
        while offset < total_rows:
            limit = min(self.batch_size, total_rows - offset)
            tasks.append(ExtractionTask(batch_id, offset, limit))
            offset += limit
            batch_id += 1
            
        return tasks
        
    def _extract_batch(self, source, task: ExtractionTask) -> list[dict[str, Any]]:
        """Extract a single batch (thread-safe)."""
        logger.debug(f"Extracting batch {task.batch_id}: offset={task.offset}, limit={task.limit}")
        return list(source.read_batch(offset=task.offset, limit=task.limit))