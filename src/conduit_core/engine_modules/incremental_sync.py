"""
Incremental sync module for managing incremental state and query modification.
"""
from typing import Any, List, Optional, Tuple
from datetime import datetime, date
from ..incremental import IncrementalSyncManager
from ..state import load_state, save_state
from ..logging_utils import ConduitLogger

logger = ConduitLogger("incremental_sync")


def setup_incremental_sync(
    resource: Any,
    last_checkpoint_value: Any
) -> Tuple[Any, Optional[str], Any, List[Any], Any, str]:
    """
    Setup incremental sync configuration and modify query if needed.
    
    Args:
        resource: Resource configuration
        last_checkpoint_value: Last checkpoint value (if resuming)
    
    Returns:
        Tuple of (incremental_mgr, incremental_column, incremental_start_value, 
                  incremental_values, max_value_seen, final_query)
    """
    # Initialize incremental manager
    incremental_mgr = IncrementalSyncManager()
    incremental_start_value = None
    incremental_column = None
    incremental_values = []  # Track values for gap detection
    
    # Support both old (incremental_column) and new (incremental config) formats
    if resource.incremental:
        incremental_column = resource.incremental.column
        incremental_start_value = incremental_mgr.calculate_start_value(
            resource.name,
            resource.incremental.strategy,
            resource.incremental.lookback_seconds,
            resource.incremental.initial_value
        )
    elif resource.incremental_column:
        # Legacy support
        incremental_column = resource.incremental_column
        current_state = load_state()
        incremental_start_value = last_checkpoint_value if last_checkpoint_value is not None else current_state.get(resource.name)
    
    max_value_seen = incremental_start_value
    final_query = resource.query
    
    # Modify query to add incremental filter
    if incremental_column and incremental_start_value is not None:
        wrapped_value = f"'{incremental_start_value}'" if isinstance(incremental_start_value, (str, datetime, date)) else incremental_start_value
        filter_condition = f"{incremental_column} > {wrapped_value}"
        
        # Check if ORDER BY exists and insert WHERE before it
        if "ORDER BY" in final_query.upper():
            order_by_pos = final_query.upper().find("ORDER BY")
            base_query = final_query[:order_by_pos].strip()
            order_clause = final_query[order_by_pos:]
            
            if "WHERE" in base_query.upper():
                final_query = f"{base_query} AND {filter_condition} {order_clause}"
            else:
                final_query = f"{base_query} WHERE {filter_condition} {order_clause}"
        else:
            if "WHERE" in final_query.upper():
                final_query += f" AND {filter_condition}"
            else:
                final_query += f" WHERE {filter_condition}"
            final_query += f" ORDER BY {incremental_column}"
    elif incremental_column:
        logger.info(f"Incremental column '{incremental_column}' defined, but no previous state found. Performing full load.")
        if "ORDER BY" not in final_query.upper():
            final_query += f" ORDER BY {incremental_column}"
    
    return (incremental_mgr, incremental_column, incremental_start_value, 
            incremental_values, max_value_seen, final_query)


def save_incremental_state(
    resource: Any,
    incremental_column: Optional[str],
    max_value_seen: Any,
    incremental_start_value: Any,
    incremental_mgr: Any,
    incremental_values: List[Any]
) -> None:
    """
    Save incremental sync state and detect gaps.
    
    Args:
        resource: Resource configuration
        incremental_column: Name of incremental column
        max_value_seen: Maximum value seen in this run
        incremental_start_value: Starting value for this run
        incremental_mgr: Incremental sync manager instance
        incremental_values: List of all incremental values seen (for gap detection)
    """
    if not incremental_column:
        return
    
    if max_value_seen is not None and (incremental_start_value is None or max_value_seen > incremental_start_value):
        logger.info(f"Saving new incremental state: {incremental_column} = {max_value_seen}")
        max_value_str = max_value_seen.isoformat() if isinstance(max_value_seen, (datetime, date)) else max_value_seen
        
        # Use new manager if using incremental config
        if resource.incremental:
            incremental_mgr.state.save_last_value(resource.name, max_value_str)
            
            # Detect gaps if enabled
            if resource.incremental.detect_gaps and resource.incremental.strategy == 'sequential':
                gaps = incremental_mgr.detect_gaps(incremental_values, resource.incremental.strategy)
                if gaps:
                    logger.warning(f"[GAP DETECTION] Found {len(gaps)} gap(s) in {incremental_column}")
                    for gap in gaps[:5]:  # Show first 5 gaps
                        logger.warning(f"  Missing {gap['missing_count']} value(s) between {gap['after']} and {gap['before']}")
        else:
            # Legacy support
            save_state({**load_state(), resource.name: max_value_str})
    else:
        logger.info(f"No new records found. State remains at {incremental_start_value}.")