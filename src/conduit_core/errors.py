# src/conduit_core/errors.py

import json
import logging
from pathlib import Path
from datetime import datetime, UTC # Use UTC for consistency
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class ConnectionError(Exception):
    """Raised when a connector fails to connect to its source/destination."""
    pass


class DataValidationError(Exception):
    """Raised when data validation fails."""
    def __init__(self, message, **kwargs):
        super().__init__(message)
        for key, value in kwargs.items():
            setattr(self, key, value)

# --- New Exception for Quality Checks ---
class DataQualityError(Exception):
    """Raised when quality check fails with action='fail'"""
    pass
# --- End New Exception ---

class ErrorLog:
    """Handles logging of failed records to a Dead-Letter Queue (JSON file)."""

    def __init__(self, resource_name: str, error_dir: Path = Path("./errors")):
        self.resource_name = resource_name
        self.error_dir = error_dir
        self.errors: List[Dict[str, Any]] = []
        self.quality_errors: List[Dict[str, Any]] = [] # Track quality errors separately
        # Directory creation moved to save()

    def add_error(self, record: Dict[str, Any], error: Exception, row_number: int = None):
        """Adds a failed record due to processing/write error."""
        error_entry = {
            "row_number": row_number,
            "record": record,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": datetime.now(UTC).isoformat(), # Use UTC
            "failure_type": "processing_error"
        }
        self.errors.append(error_entry)
        logger.debug(f"Row {row_number} failed (Processing Error): {type(error).__name__}: {error}") # Log debug, engine logs warning

    # --- New Method for Quality Errors ---
    def add_quality_error(self, record: Dict[str, Any], failed_checks: str, row_number: int = None):
        """Adds a failed record due to quality checks (action='dlq')."""
        error_entry = {
            "row_number": row_number,
            "record": record,
            "error_type": "DataQualityError",
            "error_message": failed_checks, # Store the summary of failed checks
            "timestamp": datetime.now(UTC).isoformat(), # Use UTC
            "failure_type": "quality_check"
        }
        self.quality_errors.append(error_entry)
        # Debug log here, the engine handles the warning/error level based on action
        logger.debug(f"Row {row_number} failed (Quality Check - DLQ): {failed_checks}")
    # --- End New Method ---

    def save(self) -> Path:
        """Saves all errors (processing and quality) to a JSON file."""
        all_errors = self.errors + self.quality_errors
        if not all_errors:
            return None

        # Create the directory only when we actually have something to save.
        self.error_dir.mkdir(parents=True, exist_ok=True)

        timestamp_str = datetime.now(UTC).strftime("%Y%m%d_%H%M%S") # Use UTC
        error_file = self.error_dir / f"{self.resource_name}_errors_{timestamp_str}.json"
        error_summary = {
            "resource": self.resource_name,
            "total_errors": len(all_errors),
            "processing_errors_count": len(self.errors),
            "quality_errors_count": len(self.quality_errors),
            "timestamp": datetime.now(UTC).isoformat(), # Use UTC
            "errors": all_errors, # Combine both lists
        }
        try:
            with open(error_file, "w", encoding="utf-8") as f:
                # Use default=str to handle non-serializable types like datetime in records
                json.dump(error_summary, f, indent=2, ensure_ascii=False, default=str)
            logger.info(f"Error log saved to: {error_file}")
            return error_file
        except TypeError as e:
             logger.error(f"Failed to serialize error log to JSON: {e}. Check records for non-standard data types.", exc_info=True)
             # Optionally, try saving with errors or omitting problematic records
             return None # Indicate save failure
        except Exception as e:
            logger.error(f"Failed to save error log to {error_file}: {e}", exc_info=True)
            return None


    def has_errors(self) -> bool:
        """Checks if there are any processing or quality errors."""
        return bool(self.errors or self.quality_errors)

    def error_count(self) -> int:
        """Returns the total count of processing and quality errors."""
        return len(self.errors) + len(self.quality_errors)