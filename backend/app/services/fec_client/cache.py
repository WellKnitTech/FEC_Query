"""
Caching logic for FEC API responses
"""
import hashlib
import json
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta
from sqlalchemy import select
from app.db.database import AsyncSessionLocal, APICache

logger = logging.getLogger(__name__)


class CacheManager:
    """Manages API response caching"""
    
    def __init__(self, cache_ttls: Optional[Dict[str, int]] = None):
        """
        Initialize cache manager
        
        Args:
            cache_ttls: Dictionary of cache TTLs by data type (in hours)
        """
        self.cache_ttls = cache_ttls or {
            "candidates": 168,  # 7 days
            "committees": 168,  # 7 days
            "financials": 24,  # 24 hours
            "contributions": 24,  # 24 hours
            "expenditures": 24,  # 24 hours
            "default": 24,  # 24 hours default
        }
    
    def generate_cache_key(self, endpoint: str, params: Dict) -> str:
        """Generate cache key from endpoint and parameters"""
        key_string = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    async def get_from_cache(self, cache_key: str) -> Optional[Dict]:
        """Retrieve data from cache if not expired"""
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(APICache).where(
                        APICache.cache_key == cache_key,
                        APICache.expires_at > datetime.utcnow()
                    )
                )
                cache_entry = result.scalar_one_or_none()
                if cache_entry:
                    return cache_entry.response_data
            return None
        except Exception as e:
            logger.debug(f"Error getting from cache: {e}")
            return None
    
    async def save_to_cache(self, cache_key: str, data: Dict, ttl_hours: int = 24):
        """Save response to cache"""
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

