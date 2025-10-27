"""Advanced quality checks with anomaly detection and column statistics."""
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
import logging
from statistics import mean, stdev, median
from collections import Counter

logger = logging.getLogger(__name__)


@dataclass
class ColumnStats:
    """Statistics for a single column."""
    name: str
    null_count: int
    null_percentage: float
    distinct_count: int
    min_value: Any
    max_value: Any
    mean_value: Optional[float]
    median_value: Optional[float]
    std_dev: Optional[float]
    top_values: List[tuple]  # [(value, count), ...]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return asdict(self)


@dataclass
class AnomalyDetection:
    """Anomaly detection result."""
    column: str
    anomaly_type: str  # 'null_spike', 'value_range', 'distribution_shift'
    severity: str  # 'warning', 'critical'
    message: str
    expected: Any
    actual: Any


class QualityAnalyzer:
    """Performs advanced quality analysis on batches."""
    
    def __init__(self, baseline_stats: Optional[Dict[str, Dict[str, Any]]] = None):
        # Convert dict baseline to ColumnStats objects
        if baseline_stats:
            self.baseline_stats = {
                col: ColumnStats(**stats) if isinstance(stats, dict) else stats
                for col, stats in baseline_stats.items()
            }
        else:
            self.baseline_stats = {}
        self.batch_history = []
        
    def analyze_batch(self, batch: List[Dict[str, Any]]) -> Dict[str, ColumnStats]:
        """Generate statistics for a batch."""
        if not batch:
            return {}
            
        stats = {}
        columns = batch[0].keys()
        
        for col in columns:
            values = [row.get(col) for row in batch]
            stats[col] = self._compute_column_stats(col, values)
            
        return stats
        
    def _compute_column_stats(self, col_name: str, values: List[Any]) -> ColumnStats:
        """Compute statistics for a single column."""
        total = len(values)
        null_count = sum(1 for v in values if v is None)
        non_null_values = [v for v in values if v is not None]
        
        # Distinct count
        distinct_count = len(set(str(v) for v in non_null_values))
        
        # Min/Max
        try:
            min_val = min(non_null_values) if non_null_values else None
            max_val = max(non_null_values) if non_null_values else None
        except (TypeError, ValueError):
            min_val = max_val = None
            
        # Numeric stats
        mean_val = median_val = std_val = None
        try:
            numeric_values = [float(v) for v in non_null_values if isinstance(v, (int, float))]
            if numeric_values:
                mean_val = mean(numeric_values)
                median_val = median(numeric_values)
                std_val = stdev(numeric_values) if len(numeric_values) > 1 else 0.0
        except (TypeError, ValueError):
            pass
            
        # Top values
        counter = Counter(str(v) for v in non_null_values)
        top_values = counter.most_common(5)
        
        return ColumnStats(
            name=col_name,
            null_count=null_count,
            null_percentage=(null_count / total * 100) if total > 0 else 0,
            distinct_count=distinct_count,
            min_value=min_val,
            max_value=max_val,
            mean_value=mean_val,
            median_value=median_val,
            std_dev=std_val,
            top_values=top_values
        )
        
    def detect_anomalies(
        self,
        current_stats: Dict[str, ColumnStats],
        thresholds: Optional[Dict[str, Any]] = None
    ) -> List[AnomalyDetection]:
        """Detect anomalies by comparing current stats to baseline."""
        anomalies = []
        thresholds = thresholds or {
            'null_spike_threshold': 20.0,  # % increase in nulls
            'value_range_multiplier': 3.0,  # std devs from mean
        }
        
        for col, current in current_stats.items():
            if col not in self.baseline_stats:
                continue
                
            baseline = self.baseline_stats[col]
            
            # Null spike detection
            null_diff = current.null_percentage - baseline.null_percentage
            if null_diff > thresholds['null_spike_threshold']:
                anomalies.append(AnomalyDetection(
                    column=col,
                    anomaly_type='null_spike',
                    severity='critical' if null_diff > 50 else 'warning',
                    message=f"Null percentage increased by {null_diff:.1f}%",
                    expected=baseline.null_percentage,
                    actual=current.null_percentage
                ))
                
            # Value range anomaly (for numeric columns)
            if (baseline.mean_value is not None and 
                baseline.std_dev is not None and 
                baseline.std_dev > 0 and
                current.mean_value is not None):
                
                z_score = abs(current.mean_value - baseline.mean_value) / baseline.std_dev
                if z_score > thresholds['value_range_multiplier']:
                    anomalies.append(AnomalyDetection(
                        column=col,
                        anomaly_type='value_range',
                        severity='warning',
                        message=f"Mean shifted by {z_score:.1f} standard deviations",
                        expected=baseline.mean_value,
                        actual=current.mean_value
                    ))
                    
        return anomalies
        
    def set_baseline(self, stats: Dict[str, ColumnStats]):
        """Set baseline statistics for anomaly detection."""
        self.baseline_stats = stats
        logger.info(f"Baseline set for {len(stats)} columns")
        
    def export_baseline(self, filepath: str):
        """Export baseline to JSON file."""
        import json
        from pathlib import Path
        
        baseline_dict = {
            col: stats.to_dict() 
            for col, stats in self.baseline_stats.items()
        }
        
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(baseline_dict, f, indent=2, default=str)
        
        logger.info(f"Baseline exported to {filepath}")