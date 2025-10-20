# src/conduit_core/utils/retry.py

import time
import logging
from functools import wraps
from typing import Callable, Type, Tuple

logger = logging.getLogger(__name__)


def retry_with_backoff(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
):
    """
    Decorator that retries a function with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        initial_delay: Initial delay in seconds before first retry
        backoff_factor: Multiplier for delay after each attempt
        exceptions: Tuple of exception types to catch and retry
    
    Example:
        @retry_with_backoff(max_attempts=3, initial_delay=1.0)
        def fetch_data():
            # This will retry up to 3 times with 1s, 2s, 4s delays
            return unreliable_api_call()
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_attempts:
                        logger.error(
                            f"[FAIL] Function '{func.__name__}' failed after {max_attempts} attempts. "
                            f"Last error: {e}"
                        )
                        raise
                    
                    logger.warning(
                        f"[WARN]️  Attempt {attempt}/{max_attempts} failed for '{func.__name__}': {e}. "
                        f"Retrying in {delay}s..."
                    )
                    
                    time.sleep(delay)
                    delay *= backoff_factor
            
            # This should never happen, but just in case
            raise last_exception
        
        return wrapper
    return decorator


# Convenience decorator with sensible defaults for network operations
def retry_on_network_error(func: Callable) -> Callable:
    """Retry decorator specifically for network-related errors"""
    import socket
    from urllib.error import URLError
    
    network_exceptions = (
        ConnectionError,
        TimeoutError,
        socket.timeout,
        URLError,
    )
    
    return retry_with_backoff(
        max_attempts=3,
        initial_delay=2.0,
        backoff_factor=2.0,
        exceptions=network_exceptions
    )(func)


# Convenience decorator for database operations
def retry_on_db_error(func: Callable) -> Callable:
    """Retry decorator specifically for database-related errors"""
    try:
        import pyodbc
        db_exceptions = (
            ConnectionError,
            TimeoutError,
            pyodbc.Error,
        )
    except ImportError:
        db_exceptions = (ConnectionError, TimeoutError)
    
    return retry_with_backoff(
        max_attempts=3,
        initial_delay=1.0,
        backoff_factor=2.0,
        exceptions=db_exceptions
    )(func)