"""
Incremental sync management with lookback windows and gap detection.
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional, List, Dict
import json
from .logging_utils import ConduitLogger

logger = ConduitLogger("incremental")


class IncrementalState:
    """Manages incremental sync state (last processed value)."""
    
    def __init__(self, base_dir: Path = None):
        """Initialize state manager."""
        if base_dir is None:
            base_dir = Path.home() / '.conduit' / 'state'
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def get_last_value(self, resource_name: str) -> Optional[Any]:
        """Get the last processed value for a resource."""
        state_file = self.base_dir / f"{resource_name}_state.json"
        if not state_file.exists():
            return None
        
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
                return state.get('last_value')
        except Exception as e:
            logger.warning(f"Failed to load incremental state: {e}")
            return None
    
    def save_last_value(self, resource_name: str, value: Any):
        """Save the last processed value for a resource."""
        state_file = self.base_dir / f"{resource_name}_state.json"
        
        try:
            state = {
                'last_value': value,
                'last_updated': datetime.utcnow().isoformat(),
                'resource': resource_name
            }
            with open(state_file, 'w') as f:
                json.dump(state, f, indent=2)
            logger.debug(f"Saved incremental state: {resource_name} = {value}")
        except Exception as e:
            logger.error(f"Failed to save incremental state: {e}")


class IncrementalSyncManager:
    """Manages incremental sync with lookback windows and gap detection."""
    
    def __init__(self, state: IncrementalState = None):
        """Initialize sync manager."""
        self.state = state or IncrementalState()
    
    def calculate_start_value(
        self,
        resource_name: str,
        strategy: str,
        lookback_seconds: Optional[int] = None,
        initial_value: Optional[Any] = None
    ) -> Optional[Any]:
        """
        Calculate the starting value for incremental sync.
        
        Args:
            resource_name: Name of the resource
            strategy: 'timestamp' or 'sequential'
            lookback_seconds: Reprocess last N seconds (timestamp strategy only)
            initial_value: Default value if no state exists
        
        Returns:
            Starting value for the incremental sync
        """
        last_value = self.state.get_last_value(resource_name)
        
        # First run - use initial_value or None
        if last_value is None:
            if initial_value is not None:
                logger.info(f"[INCREMENTAL] First run, starting from initial_value: {initial_value}")
                return initial_value
            logger.info(f"[INCREMENTAL] First run, full sync (no initial_value)")
            return None
        
        # Apply lookback window for timestamp strategy
        if strategy == 'timestamp' and lookback_seconds:
            try:
                # Parse last_value as ISO timestamp
                last_dt = datetime.fromisoformat(last_value.replace('Z', '+00:00'))
                lookback_dt = last_dt - timedelta(seconds=lookback_seconds)
                lookback_value = lookback_dt.isoformat()
                logger.info(f"[INCREMENTAL] Lookback: {lookback_seconds}s, start from {lookback_value}")
                return lookback_value
            except Exception as e:
                logger.warning(f"Failed to apply lookback: {e}, using last_value")
                return last_value
        
        # No lookback - continue from last value
        logger.info(f"[INCREMENTAL] Continuing from last value: {last_value}")
        return last_value
    
    def detect_gaps(self, values: List[Any], strategy: str) -> List[Dict[str, Any]]:
        """
        Detect gaps in sequential data.
        
        Args:
            values: List of values from the incremental column
            strategy: 'sequential' or 'timestamp'
        
        Returns:
            List of detected gaps
        """
        if strategy != 'sequential' or len(values) < 2:
            return []
        
        gaps = []
        sorted_values = sorted(values)
        
        for i in range(len(sorted_values) - 1):
            current = sorted_values[i]
            next_val = sorted_values[i + 1]
            
            # Check for gap (assuming integer IDs)
            try:
                if isinstance(current, int) and isinstance(next_val, int):
                    if next_val - current > 1:
                        gaps.append({
                            'after': current,
                            'before': next_val,
                            'missing_count': next_val - current - 1
                        })
            except:
                pass
        
        return gaps