"""
Caching logic for FEC API responses
"""
import hashlib
import json
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from app.db.database import AsyncSessionLocal, APICache
from app.services.shared.retry import retry_on_db_lock, retry_on_exception
from app.config import config

logger = logging.getLogger(__name__)


class CacheManager:
    """Manages API response caching with metrics"""
    
    def __init__(self, cache_ttls: Optional[Dict[str, int]] = None):
        """
        Initialize cache manager
        
        Args:
            cache_ttls: Dictionary of cache TTLs by data type (in hours)
                      If None, uses values from centralized config
        """
        self.cache_ttls = cache_ttls or config.get_cache_ttls()
        
        # Cache metrics
        self._hits = 0
        self._misses = 0
        self._errors = 0
    
    def generate_cache_key(self, endpoint: str, params: Dict) -> str:
        """Generate cache key from endpoint and parameters"""
        key_string = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    @retry_on_exception(
        exception_types=(OperationalError,),
        max_retries=3,
        base_delay=0.1,
        exponential_backoff=True
    )
    async def get_from_cache(self, cache_key: str) -> Optional[Dict]:
        """Retrieve data from cache if not expired"""
        try:
            async with AsyncSessionLocal() as session:
                # Use composite index for efficient lookup
                result = await session.execute(
                    select(APICache).where(
                        APICache.cache_key == cache_key,
                        APICache.expires_at > datetime.utcnow()
                    )
                )
                cache_entry = result.scalar_one_or_none()
                if cache_entry:
                    self._hits += 1
                    return cache_entry.response_data
            self._misses += 1
            return None
        except Exception as e:
            self._errors += 1
            logger.debug(f"Error getting from cache: {e}")
            return None
    
    @retry_on_exception(
        exception_types=(OperationalError,),
        max_retries=3,
        base_delay=0.1,
        exponential_backoff=True
    )
    async def save_to_cache(self, cache_key: str, data: Dict, ttl_hours: int = 24):
        """Save response to cache with retry logic"""
        try:
            async with AsyncSessionLocal() as session:
                expires_at = datetime.utcnow() + timedelta(hours=ttl_hours)
                cache_entry = APICache(
                    cache_key=cache_key,
                    response_data=data,
                    expires_at=expires_at
                )
                session.add(cache_entry)
                try:
                    await session.commit()
                except Exception:
                    await session.rollback()
                    # If entry exists, update it
                    result = await session.execute(
                        select(APICache).where(APICache.cache_key == cache_key)
                    )
                    existing = result.scalar_one_or_none()
                    if existing:
                        existing.response_data = data
                        existing.expires_at = expires_at
                        await session.commit()
        except Exception as e:
            logger.debug(f"Error saving to cache: {e}")
    
    def get_cache_ttl(self, endpoint: str) -> int:
        """Get appropriate cache TTL for endpoint type"""
        if "candidate" in endpoint and "totals" not in endpoint:
            return self.cache_ttls["candidates"]
        elif "committee" in endpoint:
            return self.cache_ttls["committees"]
        elif "totals" in endpoint:
            return self.cache_ttls["financials"]
        elif "schedule_a" in endpoint or "contribution" in endpoint:
            return self.cache_ttls["contributions"]
        elif "schedule_b" in endpoint or "expenditure" in endpoint:
            return self.cache_ttls["expenditures"]
        return self.cache_ttls["default"]
    
    async def check_cache_staleness(self, cache_key: str, cache_ttl: int) -> bool:
        """
        Check if cache is stale (more than 50% expired)
        
        Returns:
            True if cache is stale, False otherwise
        """
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(APICache).where(APICache.cache_key == cache_key)
                )
                cache_entry = result.scalar_one_or_none()
                if cache_entry:
                    age = (datetime.utcnow() - cache_entry.created_at).total_seconds()
                    ttl_seconds = cache_ttl * 3600
                    return age > ttl_seconds * 0.5
            return False
        except Exception as e:
            logger.debug(f"Error checking cache staleness: {e}")
            return False
    
    async def cleanup_expired_cache(self) -> int:
        """
        Remove expired cache entries to free up space
        
        Returns:
            Number of entries removed
        """
        try:
            async with AsyncSessionLocal() as session:
                from sqlalchemy import delete
                result = await session.execute(
                    delete(APICache).where(APICache.expires_at < datetime.utcnow())
                )
                await session.commit()
                removed_count = result.rowcount
                if removed_count > 0:
                    logger.info(f"Cleaned up {removed_count} expired cache entries")
                return removed_count
        except Exception as e:
            logger.warning(f"Error cleaning up expired cache: {e}")
            return 0
    
    def get_cache_metrics(self) -> Dict[str, any]:
        """
        Get cache performance metrics
        
        Returns:
            Dictionary with cache statistics
        """
        total_requests = self._hits + self._misses
        hit_rate = (self._hits / total_requests * 100) if total_requests > 0 else 0.0
        
        return {
            "hits": self._hits,
            "misses": self._misses,
            "errors": self._errors,
            "total_requests": total_requests,
            "hit_rate_percent": round(hit_rate, 2),
            "miss_rate_percent": round(100 - hit_rate, 2)
        }
    
    async def get_cache_size(self) -> Dict[str, int]:
        """
        Get cache size statistics
        
        Returns:
            Dictionary with cache size information
        """
        try:
            async with AsyncSessionLocal() as session:
                from sqlalchemy import func
                
                # Total cache entries
                total_query = select(func.count(APICache.id))
                result = await session.execute(total_query)
                total_entries = result.scalar() or 0
                
                # Expired entries
                expired_query = select(func.count(APICache.id)).where(
                    APICache.expires_at < datetime.utcnow()
                )
                result = await session.execute(expired_query)
                expired_entries = result.scalar() or 0
                
                # Active entries
                active_entries = total_entries - expired_entries
                
                return {
                    "total_entries": total_entries,
                    "active_entries": active_entries,
                    "expired_entries": expired_entries
                }
        except Exception as e:
            logger.warning(f"Error getting cache size: {e}")
            return {
                "total_entries": 0,
                "active_entries": 0,
                "expired_entries": 0
            }

