# src/conduit_core/schema_validator.py
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)


class ValidationError(BaseModel):
    """Single validation error"""
    column: str
    issue: str  # 'type_mismatch', 'missing_column', 'constraint_violation', 'incompatible_type'
    expected: Optional[str] = None
    actual: Optional[str] = None
    severity: str = "error"  # 'error' or 'warning'

    def format(self) -> str:
        """Return a human-readable string."""
        base = f"Column '{self.column}': {self.issue.replace('_', ' ').title()}"
        if self.expected or self.actual:
            base += f" (expected: {self.expected or 'n/a'}, actual: {self.actual or 'n/a'})"
        if self.severity == "warning":
            base = f"[WARNING] {base}"
        return base


class ValidationReport(BaseModel):
    """Complete validation report"""
    is_valid: bool
    errors: List[ValidationError] = []
    warnings: List[ValidationError] = []

    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def has_warnings(self) -> bool:
        return len(self.warnings) > 0

    def format_errors(self) -> str:
        """Format all errors as multi-line string"""
        if not self.errors:
            return "No errors."
        return "\n".join(e.format() for e in self.errors)

    def format_warnings(self) -> str:
        """Format all warnings as multi-line string"""
        if not self.warnings:
            return "No warnings."
        return "\n".join(w.format() for w in self.warnings)


class SchemaValidator:
    """Pre-flight schema compatibility validation"""

    TYPE_COMPATIBILITY = {
        'integer': ['integer', 'float', 'decimal', 'string'],
        'float': ['float', 'decimal', 'string'],
        'decimal': ['decimal', 'string'],
        'string': ['string'],
        'boolean': ['boolean', 'integer', 'string'],
        'date': ['date', 'datetime', 'string'],
        'datetime': ['datetime', 'string'],
    }

    def _is_compatible_type(self, source_type: str, dest_type: str) -> bool:
        compatible = self.TYPE_COMPATIBILITY.get(source_type, [])
        return dest_type in compatible

    def validate_type_compatibility(
        self,
        source_schema: Dict[str, Any],
        dest_schema: Dict[str, Any],
    ) -> ValidationReport:
        """
        Check if source types can be safely converted to destination types.
        Args:
            source_schema: {column: {type, nullable}}
            dest_schema: {column: {type, nullable}}
        """
        errors: List[ValidationError] = []
        warnings: List[ValidationError] = []

        for col_name, dest_info in dest_schema.items():
            if col_name not in source_schema:
                warnings.append(ValidationError(
                    column=col_name,
                    issue="missing_column",
                    expected=dest_info.get("type"),
                    actual="not present in source",
                    severity="warning",
                ))
                continue

            source_info = source_schema[col_name]
            source_type = source_info.get("type")
            dest_type = dest_info.get("type")

            # Type mismatch
            if not self._is_compatible_type(source_type, dest_type):
                errors.append(ValidationError(
                    column=col_name,
                    issue="type_mismatch",
                    expected=dest_type,
                    actual=source_type,
                    severity="error",
                ))

            # Nullable mismatch
            if not dest_info.get("nullable", True) and source_info.get("nullable", False):
                warnings.append(ValidationError(
                    column=col_name,
                    issue="constraint_violation",
                    expected="NOT NULL",
                    actual="nullable source data",
                    severity="warning",
                ))

        is_valid = len(errors) == 0
        return ValidationReport(is_valid=is_valid, errors=errors, warnings=warnings)

    def check_missing_columns(
        self,
        source_schema: Dict[str, Any],
        dest_schema: Dict[str, Any],
        strict: bool = True,
    ) -> ValidationReport:
        """
        Detect columns in destination not present in source.
        """
        errors: List[ValidationError] = []
        warnings: List[ValidationError] = []

        for col_name in dest_schema.keys():
            if col_name not in source_schema:
                err = ValidationError(
                    column=col_name,
                    issue="missing_column",
                    expected="present in source",
                    actual="missing",
                    severity="error" if strict else "warning",
                )
                (errors if strict else warnings).append(err)

        is_valid = len(errors) == 0
        return ValidationReport(is_valid=is_valid, errors=errors, warnings=warnings)

    def check_required_columns(
        self,
        source_schema: Dict[str, Any],
        required_columns: List[str],
    ) -> List[str]:
        """Ensure source has all required columns."""
        return [c for c in required_columns if c not in source_schema]

    def validate_constraints(
        self,
        source_schema: Dict[str, Any],
        dest_schema: Dict[str, Any],
    ) -> ValidationReport:
        """Check if NOT NULL and other constraints match."""
        errors: List[ValidationError] = []
        warnings: List[ValidationError] = []

        for col_name, dest_info in dest_schema.items():
            if col_name not in source_schema:
                continue
            source_info = source_schema[col_name]
            if not dest_info.get("nullable", True) and source_info.get("nullable", True):
                errors.append(ValidationError(
                    column=col_name,
                    issue="constraint_violation",
                    expected="NOT NULL in destination",
                    actual="nullable source",
                    severity="error",
                ))

        is_valid = len(errors) == 0
        return ValidationReport(is_valid=is_valid, errors=errors, warnings=warnings)
