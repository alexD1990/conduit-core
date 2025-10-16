# tests/test_retry.py

import pytest
import time
from conduit_core.utils.retry import retry_with_backoff


def test_retry_succeeds_on_second_attempt():
    """Test retry når funksjon lykkes på andre forsøk."""
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
    """Test at retry gir opp etter max forsøk."""
    @retry_with_backoff(max_attempts=2, initial_delay=0.1)
    def always_fails():
        raise ConnectionError("Always fails")
    
    with pytest.raises(ConnectionError):
        always_fails()


def test_retry_respects_backoff_timing():
    """Test exponential backoff timing."""
    start = time.time()
    
    @retry_with_backoff(max_attempts=3, initial_delay=0.1, backoff_factor=2.0)
    def always_fails():
        raise ConnectionError("Fail")
    
    with pytest.raises(ConnectionError):
        always_fails()
    
    elapsed = time.time() - start
    # 0.1s + 0.2s = 0.3s minimum
    assert elapsed >= 0.3


def test_retry_specific_exceptions():
    """Test at kun spesifikke exceptions retries."""
    @retry_with_backoff(max_attempts=3, exceptions=(ValueError,))
    def raises_type_error():
        raise TypeError("Wrong exception")
    
    # TypeError skal ikke retries, feiler med en gang
    with pytest.raises(TypeError):
        raises_type_error()