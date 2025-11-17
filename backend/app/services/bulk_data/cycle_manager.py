"""
Cycle availability management for bulk data
"""
import asyncio
import logging
from typing import List, Optional
from datetime import datetime
from app.services.bulk_data_config import DataType, get_config

logger = logging.getLogger(__name__)

# Module-level cache for FEC cycle availability (shared across all instances)
_available_cycles_cache: Optional[List[int]] = None
_available_cycles_cache_time: Optional[datetime] = None
_available_cycles_cache_ttl: int = 3600  # 1 hour
_available_cycles_cache_lock: asyncio.Lock = asyncio.Lock()


class CycleManager:
    """Manages cycle availability checking and caching"""
    
    def __init__(self, check_availability_func):
        """
        Initialize cycle manager
        
        Args:
            check_availability_func: Async function to check if a cycle is available
        """
        self.check_availability_func = check_availability_func
    
    async def get_available_cycles_from_fec(self) -> List[int]:
        """
        Query FEC bulk data URLs to determine which cycles have data available
        
        Results are cached for 1 hour to avoid repeated API calls.
        Uses module-level cache with locking to prevent concurrent API calls.
        """
        global _available_cycles_cache, _available_cycles_cache_time
        
        # Check cache first (without lock for fast path)
        now = datetime.utcnow()
        if (_available_cycles_cache is not None and 
            _available_cycles_cache_time is not None):
            cache_age = (now - _available_cycles_cache_time).total_seconds()
            if cache_age < _available_cycles_cache_ttl:
                logger.debug(f"Using cached available cycles (age: {cache_age:.1f}s)")
                return _available_cycles_cache
        
        # Cache miss or expired - use lock to prevent concurrent API calls
        async with _available_cycles_cache_lock:
            # Double-check cache after acquiring lock
            if (_available_cycles_cache is not None and 
                _available_cycles_cache_time is not None):
                cache_age = (now - _available_cycles_cache_time).total_seconds()
                if cache_age < _available_cycles_cache_ttl:
                    logger.debug(f"Using cached available cycles (age: {cache_age:.1f}s) after lock")
                    return _available_cycles_cache
            
            # Cache miss or expired - fetch from API
            logger.info("Cache miss or expired, fetching available cycles from FEC API")
            current_year = datetime.now().year
            # Check cycles from 2000 to current year + 6
            future_years = current_year + 6
            cycles_to_check = list(range(2000, future_years + 1, 2))
            
            available_cycles = []
            
            # Check in batches to avoid too many concurrent requests
            batch_size = 10
            for i in range(0, len(cycles_to_check), batch_size):
                batch = cycles_to_check[i:i + batch_size]
                # Check cycles in parallel
                tasks = [self.check_availability_func(cycle) for cycle in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for cycle, is_available in zip(batch, results):
                    if isinstance(is_available, bool) and is_available:
                        available_cycles.append(cycle)
                    elif isinstance(is_available, Exception):
                        logger.debug(f"Exception checking cycle {cycle}: {is_available}")
            
            sorted_cycles = sorted(available_cycles, reverse=True)  # Most recent first
            logger.info(f"FEC API check found {len(sorted_cycles)} available cycles: {sorted_cycles}")
            
            # Update module-level cache
            _available_cycles_cache = sorted_cycles
            _available_cycles_cache_time = now
            
            return sorted_cycles

