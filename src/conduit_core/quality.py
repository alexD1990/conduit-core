# src/conduit_core/quality.py

import logging
import re
from typing import Dict, Any, List, Optional, Callable, Set, Tuple
from pydantic import BaseModel, field_validator, Field
from enum import Enum

logger = logging.getLogger(__name__)

# --- Enums ---

class QualityAction(str, Enum):
    DLQ = "dlq"      # Send to Dead Letter Queue (or error log)
    FAIL = "fail"    # Fail the entire pipeline run
    WARN = "warn"    # Log a warning but continue

class BuiltInCheck(str, Enum):
    REGEX = "regex"
    RANGE = "range"
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    ENUM = "enum"
    # CUSTOM = "custom" # Implicitly handled if check name isn't built-in

# --- Models ---

class QualityCheck(BaseModel):
    """Configuration for a single data validation rule."""
    column: str
    check: str  # Name of the check (e.g., 'regex', 'not_null', 'is_positive')
    action: QualityAction = QualityAction.DLQ

    # Rule-specific fields (using Field for potential future aliasing/validation)
    pattern: Optional[str] = Field(default=None)         # for regex
    min_value: Optional[float] = Field(default=None, alias='min') # for range
    max_value: Optional[float] = Field(default=None, alias='max') # for range
    allowed_values: Optional[List[Any]] = Field(default=None) # for enum

    model_config = { "populate_by_name": True } # Allows using 'min'/'max' in config YAML

    @field_validator('check')
    @classmethod
    def check_name_valid(cls, v: str) -> str:
        if not v:
            raise ValueError("Quality check name cannot be empty")
        # Basic validation, could add more allowed characters later
        if not re.match(r'^[a-zA-Z0-9_]+$', v):
             raise ValueError(f"Invalid check name: '{v}'. Use letters, numbers, underscores.")
        return v

    @field_validator('action', mode='before')
    @classmethod
    def action_to_enum(cls, v):
        if isinstance(v, str):
            try:
                return QualityAction(v.lower())
            except ValueError:
                raise ValueError(f"Invalid action '{v}'. Must be one of {[a.value for a in QualityAction]}.")
        return v

class ValidationFailure(BaseModel):
    """Details about a failed quality check for a record."""
    column: str
    check_name: str
    value: Any
    details: Optional[str] = None # e.g., "Value 'X' not in allowed list [...]"

class ValidationResult(BaseModel):
    """Result of validating a single record."""
    is_valid: bool
    record: Dict[str, Any]
    failed_checks: List[ValidationFailure] = []

class BatchValidationResult(BaseModel):
    """Result of validating a batch of records."""
    valid_records: List[Dict[str, Any]]
    invalid_records: List[ValidationResult] # List of results for invalid records


# --- Built-in Validator Functions ---

def not_null_validator(value: Any, **kwargs) -> bool:
    """Checks if a value is not None or an empty string."""
    return value is not None and value != ''

def regex_validator(value: Any, pattern: Optional[str] = None, **kwargs) -> bool:
    """Checks if a string value matches the regex pattern."""
    if not isinstance(value, str):
        return False # Regex only applies to strings
    if pattern is None:
         logger.warning("Regex check called without a 'pattern'.")
         return False # Cannot validate without pattern
    try:
        # Use re.fullmatch for stricter matching (entire string must match)
        return re.fullmatch(pattern, value) is not None
    except re.error as e:
        logger.warning(f"Invalid regex pattern '{pattern}': {e}")
        return False # Pattern error means check fails

def range_validator(value: Any, min_value: Optional[float] = None, max_value: Optional[float] = None, **kwargs) -> bool:
    """Checks if a numeric value is within the specified range (inclusive)."""
    # Attempt type coercion first thing
    numeric_value: Optional[float] = None
    if isinstance(value, (int, float)):
        numeric_value = value
    else:
        try:
            numeric_value = float(value)
        except (ValueError, TypeError):
             return False # Not convertible to a number

    # Check bounds only if they are defined
    if min_value is not None and numeric_value < min_value:
        return False # Failed min check
    if max_value is not None and numeric_value > max_value:
        return False # Failed max check

    # If we reach here, all defined checks passed (or no checks were defined)
    return True

def unique_validator(value: Any, seen_values: Set[Any], **kwargs) -> Tuple[bool, bool]:
    """
    Checks if a value is unique within the current batch context.
    Returns: (is_valid, should_add_to_seen)
    NOTE: This validator has state and needs special handling in the validator class.
    It returns a tuple: (isValid, shouldAddToSet).
    """
    if value in seen_values:
        return (False, False) # Not valid, don't add again
    else:
        # Consider how to handle None values - should multiple None be unique? Usually not.
        # If None is allowed, maybe don't add it to seen_values or handle separately.
        # Current logic treats None like any other value. If None should be unique, this is fine.
        # If multiple Nones are okay, the check should probably pass if value is None.
        # Let's assume for now None needs to be unique if checked.
        return (True, True)   # Valid, should be added to seen set

def enum_validator(value: Any, allowed_values: Optional[List[Any]] = None, **kwargs) -> bool:
    """Checks if a value is present in the allowed list."""
    if allowed_values is None:
         logger.warning("Enum check called without 'allowed_values' defined.")
         return False # Invalid configuration
    return value in allowed_values

# --- Validator Registry ---

class QualityCheckRegistry:
    """Registry for built-in and custom quality check functions."""
    _validators: Dict[str, Callable] = {
        BuiltInCheck.NOT_NULL.value: not_null_validator,
        BuiltInCheck.REGEX.value: regex_validator,
        BuiltInCheck.RANGE.value: range_validator,
        BuiltInCheck.UNIQUE.value: unique_validator,
        BuiltInCheck.ENUM.value: enum_validator,
    }

    @staticmethod
    def get_validator(check_name: str) -> Optional[Callable]:
        """Retrieves a validator function by name (case-insensitive)."""
        return QualityCheckRegistry._validators.get(check_name.lower())

    @staticmethod
    def register_custom(name: str, validator: Callable):
        """Registers a new custom validator function (case-insensitive name)."""
        if not callable(validator):
            raise TypeError("Custom validator must be a callable function.")

        normalized_name = name.lower()
        # Prevent overwriting built-ins? Or allow? Current allows.
        if normalized_name in QualityCheckRegistry._validators:
            logger.warning(f"Overwriting existing validator: '{normalized_name}'")

        logger.info(f"Registering custom quality validator: '{normalized_name}'")
        QualityCheckRegistry._validators[normalized_name] = validator

# --- Validator Class ---

class QualityValidator:
    """Applies a list of QualityCheck rules to records."""

    def __init__(self, checks: List[QualityCheck]):
        self.checks_by_column: Dict[str, List[QualityCheck]] = {}
        # unique_sets stores state *during* a batch validation run
        self._unique_keys_config: List[Tuple[str, str]] = [] # Store (column, unique_key)

        if not checks:
             logger.warning("QualityValidator initialized with no checks.")
             checks = [] # Ensure checks is iterable

        for check in checks:
            # Validate check config basics
            if not check.column or not check.check:
                 logger.warning(f"Skipping invalid QualityCheck with missing column or check name: {check}")
                 continue

            if check.column not in self.checks_by_column:
                self.checks_by_column[check.column] = []
            self.checks_by_column[check.column].append(check)

            # Store config needed to initialize sets for unique checks later
            if check.check.lower() == BuiltInCheck.UNIQUE.value:
                # Create a unique key for the state based on column and check name (e.g., 'user_id_unique')
                unique_key = f"{check.column}_{check.check.lower()}"
                self._unique_keys_config.append((check.column, unique_key))

    def _run_check(self, check: QualityCheck, value: Any, current_batch_unique_sets: Dict[str, Set[Any]]) -> Tuple[bool, Optional[str]]:
        """Runs a single check and returns (is_valid, failure_detail)."""
        validator = QualityCheckRegistry.get_validator(check.check)
        if not validator:
            logger.warning(f"Unknown quality check type: '{check.check}' for column '{check.column}'. Skipping.")
            return (True, f"Unknown check type '{check.check}'")

        # Prepare kwargs from the check configuration, excluding base fields
        # Use exclude_unset=True? No, want defaults like min/max=None if not set.
        # Use exclude_none? Yes, avoids passing pattern=None etc. if not defined.
        kwargs = check.model_dump(exclude={'column', 'check', 'action'}, exclude_none=True)

        try:
            # Handle unique check state explicitly using the batch-specific set
            if check.check.lower() == BuiltInCheck.UNIQUE.value:
                unique_key = f"{check.column}_{check.check.lower()}"
                seen_values = current_batch_unique_sets.get(unique_key)
                if seen_values is None:
                     logger.error(f"Internal Error: Unique set not found for {unique_key} during batch validation.")
                     return (False, "Internal error: unique set missing")

                # Pass only the specific state needed by unique_validator
                # Note: unique_validator ignores **kwargs
                is_valid, should_add = validator(value, seen_values=seen_values)
                if is_valid and should_add:
                    seen_values.add(value)

                details = None if is_valid else f"Value '{value!r}' is not unique in this batch"
                return (is_valid, details)
            else:
                # Call other validators with relevant kwargs
                is_valid = validator(value, **kwargs)
                details = None
                if not is_valid: # Generate basic details for known built-ins
                    check_lower = check.check.lower()
                    if check_lower == BuiltInCheck.REGEX.value:
                         details = f"Value '{value!r}' does not match pattern '{check.pattern}'"
                    elif check_lower == BuiltInCheck.RANGE.value:
                         min_str = f"min={check.min_value}" if check.min_value is not None else ""
                         max_str = f"max={check.max_value}" if check.max_value is not None else ""
                         range_str = ", ".join(filter(None, [min_str, max_str]))
                         details = f"Value '{value!r}' out of range ({range_str})"
                    elif check_lower == BuiltInCheck.NOT_NULL.value:
                         details = "Value is null or empty"
                    elif check_lower == BuiltInCheck.ENUM.value:
                         details = f"Value '{value!r}' not in allowed list: {check.allowed_values}"
                    # Custom validators might need custom detail generation later
                return (is_valid, details)

        except Exception as e:
            # Log error including value for easier debugging, but keep traceback minimal in logs
            logger.error(f"Error running check '{check.check}' on column '{check.column}' value '{value!r}': {e}", exc_info=False)
            return (False, f"Error during check execution: {e}")

    def validate_record(self, record: Dict[str, Any], current_batch_unique_sets: Dict[str, Set[Any]]) -> ValidationResult:
        """
        Validates a single record against all applicable checks.
        Requires the state for unique checks for the current batch.
        """
        failed_checks: List[ValidationFailure] = []

        # Iterate through columns that have checks defined
        for column, checks in self.checks_by_column.items():
            value = record.get(column) # Use .get() to handle missing columns gracefully

            for check in checks:
                # Run the check, passing the state for unique sets
                is_valid, details = self._run_check(check, value, current_batch_unique_sets)
                if not is_valid:
                    failed_checks.append(ValidationFailure(
                        column=column,
                        check_name=check.check,
                        value=value, # Log the original value that failed
                        details=details
                    ))
                    # Optional: Could break inner loop early if a check fails and action is 'fail'?
                    # if check.action == QualityAction.FAIL: break

        return ValidationResult(
            is_valid=not bool(failed_checks), # Valid only if failed_checks list is empty
            record=record,
            failed_checks=failed_checks
        )

    def validate_batch(self, records: List[Dict[str, Any]]) -> BatchValidationResult:
        """
        Validates a batch of records. Initializes and manages state for 'unique' checks
        specifically for this batch.
        """
        valid_records = []
        invalid_results = []

        # Initialize unique check sets required for this batch based on config
        # This ensures state is reset for every batch
        current_batch_unique_sets: Dict[str, Set[Any]] = {
             key: set() for _, key in self._unique_keys_config
        }

        for record in records:
            # Pass the batch-specific state to validate_record
            result = self.validate_record(record, current_batch_unique_sets)
            if result.is_valid:
                valid_records.append(record)
            else:
                invalid_results.append(result)

        return BatchValidationResult(
            valid_records=valid_records,
            invalid_records=invalid_results
        )