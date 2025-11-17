"""
Retry utility for handling database locks and other transient errors
"""
import asyncio
import logging
from functools import wraps
from typing import Callable, Type, Tuple, Optional, Any
from .exceptions import DatabaseLockError

logger = logging.getLogger(__name__)


def retry_on_db_lock(
    max_retries: int = 3,
    base_delay: float = 0.1,
    exponential_backoff: bool = True,
    on_retry: Optional[Callable[[int, Exception], None]] = None
):
    """
    Decorator to retry async functions on database lock errors
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds before first retry
        exponential_backoff: If True, delay doubles with each retry
        on_retry: Optional callback function(attempt, exception) called before each retry
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    error_str = str(e).lower()
                    
                    # Check if it's a database locked error
                    is_db_lock = (
                        "database is locked" in error_str or 
                        "locked" in error_str or
                        isinstance(e, DatabaseLockError)
                    )
                    
                    if not is_db_lock:
                        # Not a database lock error, don't retry
                        raise
                    
                    if attempt < max_retries - 1:
                        # Calculate wait time
                        if exponential_backoff:
                            wait_time = base_delay * (2 ** attempt)
                        else:
                            wait_time = base_delay
                        
                        # Call retry callback if provided
                        if on_retry:
                            try:
                                on_retry(attempt + 1, e)
                            except Exception:
                                pass  # Don't fail on callback error
                        
                        logger.debug(
                            f"Database locked in {func.__name__}, retrying in {wait_time}s "
                            f"(attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        # All retries exhausted
                        logger.warning(
                            f"Failed {func.__name__} after {max_retries} attempts due to database lock"
                        )
                        raise DatabaseLockError(
                            f"Database lock error after {max_retries} retries: {str(e)}"
                        ) from e
            
            # Should never reach here, but just in case
            if last_exception:
                raise last_exception
        
        return wrapper
    return decorator


def retry_on_exception(
    exception_types: Tuple[Type[Exception], ...],
    max_retries: int = 3,
    base_delay: float = 0.1,
    exponential_backoff: bool = True,
    on_retry: Optional[Callable[[int, Exception], None]] = None
):
    """
    Decorator to retry async functions on specific exception types
    
    Args:
        exception_types: Tuple of exception types to retry on
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds before first retry
        exponential_backoff: If True, delay doubles with each retry
        on_retry: Optional callback function(attempt, exception) called before each retry
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except exception_types as e:
                    last_exception = e
                    
                    if attempt < max_retries - 1:
                        # Calculate wait time
                        if exponential_backoff:
                            wait_time = base_delay * (2 ** attempt)
                        else:
                            wait_time = base_delay
                        
                        # Call retry callback if provided
                        if on_retry:
                            try:
                                on_retry(attempt + 1, e)
                            except Exception:
                                pass  # Don't fail on callback error
                        
                        logger.debug(
                            f"Exception {type(e).__name__} in {func.__name__}, retrying in {wait_time}s "
                            f"(attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        # All retries exhausted
                        logger.warning(
                            f"Failed {func.__name__} after {max_retries} attempts: {e}"
                        )
                        raise
            
            # Should never reach here, but just in case
            if last_exception:
                raise last_exception
        
        return wrapper
    return decorator


class RetryContext:
    """
    Context manager for retrying operations with custom logic
    """
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 0.1,
        exponential_backoff: bool = True,
        exception_filter: Optional[Callable[[Exception], bool]] = None
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.exponential_backoff = exponential_backoff
        self.exception_filter = exception_filter or (lambda e: True)
        self.attempt = 0
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            return False
        
        if not self.exception_filter(exc_val):
            return False  # Don't retry, let exception propagate
        
        self.attempt += 1
        
        if self.attempt < self.max_retries:
            # Calculate wait time
            if self.exponential_backoff:
                wait_time = self.base_delay * (2 ** (self.attempt - 1))
            else:
                wait_time = self.base_delay
            
            logger.debug(
                f"Retrying after {exc_type.__name__}, attempt {self.attempt}/{self.max_retries}, "
                f"waiting {wait_time}s"
            )
            await asyncio.sleep(wait_time)
            return True  # Suppress exception, retry
        else:
            # All retries exhausted
            logger.warning(f"All {self.max_retries} retries exhausted for {exc_type.__name__}")
            return False  # Let exception propagate

