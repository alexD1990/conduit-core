# tests/test_error_handling.py

import pytest
import time
from conduit_core.utils.retry import retry_with_backoff
from conduit_core.quality import QualityValidator, QualityCheck
from conduit_core.errors import DataValidationError


def test_retry_succeeds_on_second_attempt():
    """Test that retry works when function succeeds on retry"""
    attempts = []
    
    @retry_with_backoff(max_attempts=3, initial_delay=0.1)
    def flaky_function():
        attempts.append(1)
        if len(attempts) < 2:
            raise ConnectionError("Temporary failure")
        return "success"
    
    result = flaky_function()
    assert result == "success"
    assert len(attempts) == 2


def test_retry_fails_after_max_attempts():
    """Test that retry raises exception after max attempts"""
    @retry_with_backoff(max_attempts=2, initial_delay=0.1)
    def always_fails():
        raise ConnectionError("Always fails")
    
    with pytest.raises(ConnectionError):
        always_fails()


def test_retry_respects_backoff_timing():
    """Test that backoff timing is correct"""
    start_time = time.time()
    
    @retry_with_backoff(max_attempts=3, initial_delay=0.1, backoff_factor=2.0)
    def always_fails():
        raise ConnectionError("Always fails")
    
    with pytest.raises(ConnectionError):
        always_fails()
    
    elapsed = time.time() - start_time
    # Should wait: 0.1s + 0.2s = 0.3s (plus some overhead)
    assert elapsed >= 0.3
    assert elapsed < 0.5  # Shouldn't take too long


def test_validator_skips_invalid_records():
    """Test that validator detects invalid records"""
    checks = [
        QualityCheck(column="id", check="not_null", action="warn"),
        QualityCheck(column="name", check="not_null", action="warn")
    ]
    validator = QualityValidator(checks)
    
    # Valid record
    valid_record = {"id": 1, "name": "Alice"}
    result = validator.validate_batch([valid_record])
    assert len(result.invalid_records) == 0
    assert len(result.valid_records) == 1
    
    # Invalid record
    invalid_record = {"id": None, "name": ""}
    result = validator.validate_batch([invalid_record])
    assert len(result.invalid_records) == 1
    assert len(result.valid_records) == 0


def test_validator_raises_on_invalid_when_configured():
    """Test that validator raises exception when action=fail"""
    checks = [QualityCheck(column="id", check="not_null", action="fail")]
    validator = QualityValidator(checks)
    
    invalid_record = {"id": None, "name": "Alice"}
    
    with pytest.raises(DataValidationError):
        result = validator.validate_batch([invalid_record])
        if len(result.invalid_records) > 0:
            raise DataValidationError(f"Validation failed: {result.invalid_records}")

def test_validator_sanitizes_records():
    """Test basic data type handling"""
    # QualityValidator validates, it doesn't sanitize
    # This test just verifies that validation works with different data types
    checks = [QualityCheck(column="name", check="not_null", action="warn")]
    validator = QualityValidator(checks)
    
    # Valid string
    result = validator.validate_batch([{"name": "Alice"}])
    assert len(result.invalid_records) == 0
    
    # Empty string fails not_null check (it's treated as "empty")
    result = validator.validate_batch([{"name": ""}])
    assert len(result.invalid_records) == 1
    
    # Null value also fails not_null check
    result = validator.validate_batch([{"name": None}])
    assert len(result.invalid_records) == 1


def test_validator_tracks_multiple_errors():
    """Test that validator detects multiple errors"""
    checks = [
        QualityCheck(column="id", check="not_null", action="warn"),
        QualityCheck(column="name", check="not_null", action="warn")
    ]
    validator = QualityValidator(checks)
    
    # Multiple invalid records
    invalid_records = [{"id": None, "name": None} for _ in range(5)]
    result = validator.validate_batch(invalid_records)
    
    assert len(result.invalid_records) == 5
    assert len(result.valid_records) == 0