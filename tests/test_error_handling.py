# tests/test_error_handling.py

import pytest
import time
from conduit_core.utils.retry import retry_with_backoff
from conduit_core.validators import RecordValidator
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
    """Test that validator skips invalid records when skip_invalid=True"""
    validator = RecordValidator(skip_invalid=True)
    
    # Valid record
    result = validator.validate_record({"id": 1, "name": "Alice"}, row_number=1)
    assert result is not None
    assert result == {"id": 1, "name": "Alice"}
    
    # Empty record
    result = validator.validate_record({"id": "", "name": ""}, row_number=2)
    assert result is None
    
    summary = validator.get_summary()
    assert summary['skipped_count'] == 1
    assert summary['total_errors'] == 1


def test_validator_raises_on_invalid_when_configured():
    """Test that validator raises exception when skip_invalid=False"""
    validator = RecordValidator(skip_invalid=False)
    
    with pytest.raises(DataValidationError):
        validator.validate_record({"id": "", "name": ""}, row_number=1)


def test_validator_sanitizes_records():
    """Test that validator properly sanitizes data"""
    validator = RecordValidator(skip_invalid=True)
    
    # Test whitespace stripping
    result = validator.validate_record({"name": "  Alice  ", "email": "test@example.com"}, row_number=1)
    assert result["name"] == "Alice"
    
    # Test NULL string conversion
    result = validator.validate_record({"id": 1, "value": "null"}, row_number=2)
    assert result["value"] is None
    
    # Test empty string to None
    result = validator.validate_record({"id": 1, "value": ""}, row_number=3)
    assert result["value"] is None


def test_validator_tracks_multiple_errors():
    """Test that validator tracks multiple errors correctly"""
    validator = RecordValidator(skip_invalid=True, max_errors=10)
    
    # Skip several invalid records
    for i in range(5):
        result = validator.validate_record({"id": "", "name": ""}, row_number=i)
        assert result is None
    
    summary = validator.get_summary()
    assert summary['total_errors'] == 5
    assert summary['skipped_count'] == 5
    assert summary['has_errors'] is True