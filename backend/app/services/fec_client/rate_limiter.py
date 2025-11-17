"""
Rate limiting for FEC API requests
"""
import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class RateLimiter:
    """Handles rate limiting for API requests"""
    
    def __init__(
        self,
        rate_limit_delay: float = 0.5,
        rate_limit_retry_delay: int = 60,
        max_concurrent: int = 5
    ):
        """
        Initialize rate limiter
        
        Args:
            rate_limit_delay: Delay in seconds between requests (default: 0.5)
            rate_limit_retry_delay: Delay in seconds when rate limited (default: 60)
            max_concurrent: Maximum concurrent requests (default: 5)
        """
        self.rate_limit_delay = rate_limit_delay
        self.rate_limit_retry_delay = rate_limit_retry_delay
        self.last_request_time = 0
        self._semaphore = asyncio.Semaphore(max_concurrent)
    
    def get_semaphore(self) -> asyncio.Semaphore:
        """Get the semaphore for limiting concurrent requests"""
        return self._semaphore
    
    async def wait_for_rate_limit(self):
        """Ensure we respect rate limits"""
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.rate_limit_delay:
            await asyncio.sleep(self.rate_limit_delay - time_since_last)
        self.last_request_time = asyncio.get_event_loop().time()
    
    def get_retry_delay(self, attempt: int) -> float:
        """Get retry delay for a given attempt number"""
        return self.rate_limit_retry_delay * (attempt + 1)

