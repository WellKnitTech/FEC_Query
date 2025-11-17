"""
FEC Client - Main orchestration class using refactored modules
"""
import asyncio
import os
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from app.api.dependencies import get_fec_api_key, get_fec_api_base_url
from app.db.database import (
    AsyncSessionLocal, Contribution, BulkDataMetadata,
    Candidate, Committee, FinancialTotal
)
from sqlalchemy import select, and_, or_, func
import json
from collections import defaultdict

from .api_client import APIClient
from .cache import CacheManager
from .rate_limiter import RateLimiter
from .storage import StorageManager

logger = logging.getLogger(__name__)

# Module-level cache for contact info checks (prevents duplicate API calls)
_contact_info_check_cache: Dict[str, Tuple[bool, datetime]] = {}
_contact_info_check_cache_ttl = 300  # 5 minutes
_contact_info_check_cache_lock = asyncio.Lock()

# Track ongoing backfill operations to prevent duplicates
_backfill_in_progress: Dict[str, asyncio.Task] = {}
_backfill_lock = asyncio.Lock()


class FECClient:
    """Client for interacting with OpenFEC API with caching and rate limiting"""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize FEC client.
        
        Args:
            api_key: Optional API key. If not provided, will be fetched from database or environment on first use.
        """
        self.base_url = get_fec_api_base_url()
        self.bulk_data_enabled = os.getenv("BULK_DATA_ENABLED", "true").lower() == "true"
        self.contribution_lookback_days = int(os.getenv("CONTRIBUTION_LOOKBACK_DAYS", "30"))
        
        # Initialize modules
        cache_ttls = {
            "candidates": int(os.getenv("CACHE_TTL_CANDIDATES_HOURS", "168")),
            "committees": int(os.getenv("CACHE_TTL_COMMITTEES_HOURS", "168")),
            "financials": int(os.getenv("CACHE_TTL_FINANCIALS_HOURS", "24")),
            "contributions": int(os.getenv("CACHE_TTL_CONTRIBUTIONS_HOURS", "24")),
            "expenditures": int(os.getenv("CACHE_TTL_EXPENDITURES_HOURS", "24")),
            "default": int(os.getenv("CACHE_TTL_HOURS", "24")),
        }
        
        self.rate_limiter = RateLimiter(
            rate_limit_delay=0.5,
            rate_limit_retry_delay=60,
            max_concurrent=5
        )
        self.cache_manager = CacheManager(cache_ttls)
        self.storage_manager = StorageManager()
        self.api_client = APIClient(
            base_url=self.base_url,
            api_key=api_key,
            rate_limiter=self.rate_limiter,
            cache_manager=self.cache_manager
        )
        
        # Database write semaphore (shared with storage manager)
        self._db_write_semaphore = self.storage_manager._db_write_semaphore
    
    async def _make_request(
        self,
        endpoint: str,
        params: Dict[str, Any],
        use_cache: bool = True,
        cache_ttl: Optional[int] = None,
        max_retries: int = 3,
        data_type: Optional[str] = None
    ) -> Dict:
        """Make API request using API client module"""
        return await self.api_client.make_request(
            endpoint=endpoint,
            params=params,
            get_api_key_func=get_fec_api_key,
            use_cache=use_cache,
            cache_ttl=cache_ttl,
            max_retries=max_retries,
            data_type=data_type
        )
    
    async def _store_candidate(self, candidate_data: Dict):
        """Store candidate using storage manager"""
        await self.storage_manager.store_candidate(candidate_data)
    
    async def _store_financial_total(self, candidate_id: str, financial_data: Dict):
        """Store financial total using storage manager"""
        await self.storage_manager.store_financial_total(candidate_id, financial_data)
    
    async def _store_contribution(self, contribution_data: Dict):
        """Store contribution using storage manager"""
        await self.storage_manager.store_contribution(
            contribution_data,
            self._smart_merge_contribution
        )
    
    async def _store_committee(self, committee_data: Dict):
        """Store committee using storage manager"""
        await self.storage_manager.store_committee(committee_data)
    
    def _extract_candidate_contact_info(self, candidate_data: Dict) -> Dict:
        """Extract contact information from candidate API response"""
        return self.storage_manager._extract_candidate_contact_info(candidate_data)
    
    def _extract_committee_contact_info(self, committee_data: Dict) -> Dict:
        """Extract contact information from committee API response"""
        return self.storage_manager._extract_committee_contact_info(committee_data)
    
    # Continue with all the query and public methods from original file
    # For now, import the rest from the original file to maintain functionality
    # This will be gradually migrated

