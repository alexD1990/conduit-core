# src/conduit_core/validators.py

import logging
from typing import Dict, Any, List, Optional
from .errors import DataValidationError
from .types import TypeConverter, sanitize_dict

logger = logging.getLogger(__name__)


class RecordValidator:
    """Validates and sanitizes records before writing"""
    
    def __init__(self, skip_invalid: bool = True, max_errors: int = 100):
        """
        Args:
            skip_invalid: If True, skip invalid records. If False, raise exception.
            max_errors: Maximum number of errors before stopping (prevents log spam)
        """
        self.skip_invalid = skip_invalid
        self.max_errors = max_errors
        self.error_count = 0
        self.skipped_records: List[Dict[str, Any]] = []
    
    def validate_record(self, record: Dict[str, Any], row_number: int) -> Optional[Dict[str, Any]]:
        """
        Validate a single record.
        
        Returns:
            The sanitized record if valid, None if invalid and skip_invalid=True
        
        Raises:
            DataValidationError if invalid and skip_invalid=False
        """
        try:
            # Check if record is empty
            if not record or all(v is None or v == '' for v in record.values()):
                raise ValueError("Record is empty or contains only NULL values")
            
            # Sanitize the record
            sanitized = self._sanitize_record(record)
            
            return sanitized
            
        except Exception as e:
            self.error_count += 1
            
            error_msg = f"Row {row_number}: {str(e)}"
            
            if self.error_count <= self.max_errors:
                logger.warning(f"⚠️  Invalid record skipped. {error_msg}")
            elif self.error_count == self.max_errors + 1:
                logger.warning(f"⚠️  Too many errors. Suppressing further validation warnings...")
            
            self.skipped_records.append({
                'row_number': row_number,
                'record': record,
                'error': str(e)
            })
            
            if not self.skip_invalid:
                raise DataValidationError(
                    f"Data validation failed at row {row_number}: {e}",
                    suggestions=[
                        "Check the source data for corruption",
                        "Enable 'skip_invalid' to skip bad records",
                        "Review the error log for specific issues"
                    ]
                )
            
            return None
    
    def _sanitize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize record values to handle edge cases"""
        converter = TypeConverter()
        sanitized = {}
        
        for key, value in record.items():
            # Handle empty strings as None
            if value == '':
                sanitized[key] = None
            # Handle "NULL", "null", "None" strings as None
            elif isinstance(value, str) and value.lower() in ('null', 'none', 'n/a', 'na'):
                sanitized[key] = None
            # Strip whitespace from strings
            elif isinstance(value, str):
                sanitized[key] = value.strip()
            # Convert types to safe formats
            else:
                sanitized[key] = converter.to_safe_type(value)
        
        return sanitized
    
    def get_summary(self) -> Dict[str, Any]:
        """Get validation summary"""
        return {
            'total_errors': self.error_count,
            'skipped_count': len(self.skipped_records),
            'has_errors': self.error_count > 0
        }