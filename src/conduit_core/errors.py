# src/conduit_core/errors.py

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class DataValidationError(Exception):
    """Raised when data validation fails."""
    def __init__(self, message, **kwargs):
        super().__init__(message)
        for key, value in kwargs.items():
            setattr(self, key, value)


class ErrorLog:
    """Håndterer logging av feilede records til en Dead-Letter Queue."""

    def __init__(self, resource_name: str, error_dir: Path = Path("./errors")):
        self.resource_name = resource_name
        self.error_dir = error_dir
        self.errors: List[Dict[str, Any]] = []

        # Opprett error directory hvis den ikke finnes
        self.error_dir.mkdir(parents=True, exist_ok=True)

    def add_error(self, record: Dict[str, Any], error: Exception, row_number: int = None):
        """Legger til en feilet record i error loggen."""
        error_entry = {
            "row_number": row_number,
            "record": record,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": datetime.now().isoformat(),
        }
        self.errors.append(error_entry)

        logger.warning(f"Row {row_number} failed: {type(error).__name__}: {error}")

    def save(self) -> Path:
        """Lagrer alle errors til en JSON-fil."""
        if not self.errors:
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_file = self.error_dir / f"{self.resource_name}_errors_{timestamp}.json"

        error_summary = {
            "resource": self.resource_name,
            "total_errors": len(self.errors),
            "timestamp": datetime.now().isoformat(),
            "errors": self.errors,
        }

        with open(error_file, "w", encoding="utf-8") as f:
            json.dump(error_summary, f, indent=2, ensure_ascii=False)

        logger.info(f"Error log saved to: {error_file}")
        return error_file

    def has_errors(self) -> bool:
        """Sjekker om det er noen errors."""
        return len(self.errors) > 0

    def error_count(self) -> int:
        """Returnerer antall errors."""
        return len(self.errors)