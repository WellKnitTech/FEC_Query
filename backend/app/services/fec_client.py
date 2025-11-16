import httpx
import asyncio
import os
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from app.api.dependencies import get_fec_api_key, get_fec_api_base_url
from app.db.database import (
    AsyncSessionLocal, APICache, Contribution, BulkDataMetadata,
    Candidate, Committee, FinancialTotal
)
from sqlalchemy import select, and_, or_, func
import json
import hashlib
from collections import defaultdict

logger = logging.getLogger(__name__)

# Module-level cache for contact info checks (prevents duplicate API calls)
# Maps candidate_id -> (has_contact_info: bool, checked_at: datetime)
_contact_info_check_cache: Dict[str, Tuple[bool, datetime]] = {}
_contact_info_check_cache_ttl = 300  # 5 minutes
_contact_info_check_cache_lock = asyncio.Lock()

class FECClient:
    """Client for interacting with OpenFEC API with caching and rate limiting"""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize FEC client.
        
        Args:
            api_key: Optional API key. If not provided, will be fetched from database or environment on first use.
        """
        if api_key:
            self.api_key = api_key
            self._api_key_pending = False
        else:
            # Will be fetched asynchronously on first API call
            self.api_key = None
            self._api_key_pending = True
        
        self.base_url = get_fec_api_base_url()
        self.client = httpx.AsyncClient(timeout=30.0)
        self.rate_limit_delay = 0.5  # 500ms between requests (slower to avoid rate limits)
        self.last_request_time = 0
        self._semaphore = None  # Will be initialized lazily
        self.rate_limit_retry_delay = 60  # Wait 60 seconds if rate limited
        self.bulk_data_enabled = os.getenv("BULK_DATA_ENABLED", "true").lower() == "true"
        
        # Request deduplication - track in-flight requests
        self._in_flight_requests: Dict[str, asyncio.Task] = {}
        
        # Database write semaphore to serialize writes and prevent "database is locked" errors
        self._db_write_semaphore = asyncio.Semaphore(1)  # Only one write at a time
        
        # Extended cache TTLs by data type (in hours)
        self.cache_ttls = {
            "candidates": int(os.getenv("CACHE_TTL_CANDIDATES_HOURS", "168")),  # 7 days
            "committees": int(os.getenv("CACHE_TTL_COMMITTEES_HOURS", "168")),  # 7 days
            "financials": int(os.getenv("CACHE_TTL_FINANCIALS_HOURS", "24")),  # 24 hours
            "contributions": int(os.getenv("CACHE_TTL_CONTRIBUTIONS_HOURS", "24")),  # 24 hours
            "expenditures": int(os.getenv("CACHE_TTL_EXPENDITURES_HOURS", "24")),  # 24 hours
            "default": int(os.getenv("CACHE_TTL_HOURS", "24")),  # 24 hours default
        }
        
        # Lookback window for catching late-filed contributions (in days)
        # FEC allows late filings and amendments, so we fetch contributions from
        # (latest_date - lookback_days) to catch any that were filed late
        self.contribution_lookback_days = int(os.getenv("CONTRIBUTION_LOOKBACK_DAYS", "30"))  # 30 days default
    
    def _get_semaphore(self):
        """Get or create the semaphore for limiting concurrent requests"""
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent requests
        return self._semaphore
        
    async def _wait_for_rate_limit(self):
        """Ensure we respect rate limits"""
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.rate_limit_delay:
            await asyncio.sleep(self.rate_limit_delay - time_since_last)
        self.last_request_time = asyncio.get_event_loop().time()
    
    def _generate_cache_key(self, endpoint: str, params: Dict) -> str:
        """Generate cache key from endpoint and parameters"""
        key_string = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    async def _get_from_cache(self, cache_key: str) -> Optional[Dict]:
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
    
    async def _save_to_cache(self, cache_key: str, data: Dict, ttl_hours: int = 24):
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
    
    def _get_cache_ttl(self, endpoint: str) -> int:
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
    
    async def _ensure_api_key(self):
        """Ensure API key is loaded (for async initialization)"""
        if self.api_key is None or getattr(self, '_api_key_pending', False):
            self.api_key = await get_fec_api_key()
            self._api_key_pending = False
    
    async def _make_request(
        self, 
        endpoint: str, 
        params: Dict[str, Any],
        use_cache: bool = True,
        cache_ttl: Optional[int] = None,
        max_retries: int = 3,
        data_type: Optional[str] = None
    ) -> Dict:
        """Make API request with caching, rate limit handling, and request deduplication"""
        # Ensure API key is loaded before using it
        await self._ensure_api_key()
        
        # Add API key to params
        params["api_key"] = self.api_key
        # FEC API requires per_page to be between 1 and 100
        # Store original limit if provided for pagination (remove from params before API call)
        original_limit = params.pop("_original_limit", None)
        requested_per_page = params.get("per_page", 100)
        params["per_page"] = min(max(1, requested_per_page), 100)
        # Restore _original_limit for pagination logic
        if original_limit:
            params["_original_limit"] = original_limit
        
        # Determine cache TTL
        if cache_ttl is None:
            cache_ttl = self._get_cache_ttl(endpoint)
        
        # Check cache first
        cache_key = None
        cached_data = None
        if use_cache:
            cache_key = self._generate_cache_key(endpoint, params)
            cached_data = await self._get_from_cache(cache_key)
            
            # Request deduplication - check if same request is in flight
            if cache_key in self._in_flight_requests:
                try:
                    logger.debug(f"Waiting for in-flight request: {cache_key}")
                    return await self._in_flight_requests[cache_key]
                except Exception as e:
                    logger.warning(f"In-flight request failed: {e}")
                    # Remove failed request and continue
                    self._in_flight_requests.pop(cache_key, None)
            
            # Stale-while-revalidate: return cached data immediately if available
            if cached_data:
                # Check if cache is stale but not expired (for background refresh)
                try:
                    async with AsyncSessionLocal() as session:
                        result = await session.execute(
                            select(APICache).where(APICache.cache_key == cache_key)
                        )
                        cache_entry = result.scalar_one_or_none()
                        if cache_entry:
                            # If cache is more than 50% expired, refresh in background
                            age = (datetime.utcnow() - cache_entry.created_at).total_seconds()
                            ttl_seconds = cache_ttl * 3600
                            if age > ttl_seconds * 0.5:
                                # Trigger background refresh (don't wait)
                                asyncio.create_task(self._refresh_cache_in_background(endpoint, params, cache_key, cache_ttl))
                except Exception as e:
                    logger.debug(f"Error checking cache staleness: {e}")
                return cached_data
        
        # Ensure API key is loaded
        await self._ensure_api_key()
        
        # Create request task for deduplication
        async def _do_request():
            # Use semaphore to limit concurrent requests
            async with self._get_semaphore():
                # Wait for rate limit
                await self._wait_for_rate_limit()
                
                # Make request with retry logic for rate limits
                url = f"{self.base_url}/{endpoint}"
                last_exception = None
                
                for attempt in range(max_retries):
                    try:
                        response = await self.client.get(url, params=params)
                    
                        # Handle rate limit (429) with retry
                        if response.status_code == 429:
                            if attempt < max_retries - 1:
                                # Wait longer before retrying
                                wait_time = self.rate_limit_retry_delay * (attempt + 1)
                                await asyncio.sleep(wait_time)
                                continue
                            else:
                                # Last attempt failed, try to return cached data if available
                                if cache_key:
                                    cached_data = await self._get_from_cache(cache_key)
                                    if cached_data:
                                        return cached_data
                                
                                # Parse error message
                                error_detail = "Rate limit exceeded. Please try again later."
                                try:
                                    error_data = response.json()
                                    if isinstance(error_data, dict):
                                        if "message" in error_data:
                                            error_detail = error_data["message"]
                                        elif "error" in error_data:
                                            error_detail = error_data["error"]
                                except Exception as e:
                                    logger.debug(f"Could not parse error response: {e}")
                                raise Exception(f"FEC API error: {error_detail}")
                        
                        response.raise_for_status()
                        data = response.json()
                        
                        # Handle pagination if needed
                        # Extract original limit from params if it was stored
                        max_results = params.get("_original_limit")
                        if "pagination" in data and data["pagination"].get("pages", 0) > 1:
                            data = await self._handle_pagination(endpoint, params.copy(), data, max_results=max_results)
                        
                        # Save to cache on success
                        if use_cache and cache_key:
                            await self._save_to_cache(cache_key, data, cache_ttl)
                        
                        return data
                        
                    except httpx.HTTPStatusError as e:
                        # Handle other HTTP errors
                        if e.response.status_code == 429:
                            # Rate limit - will be retried above
                            last_exception = e
                            if attempt < max_retries - 1:
                                wait_time = self.rate_limit_retry_delay * (attempt + 1)
                                await asyncio.sleep(wait_time)
                                continue
                        else:
                            error_detail = f"FEC API error: {e.response.status_code}"
                            try:
                                error_data = e.response.json()
                                if isinstance(error_data, dict):
                                    if "message" in error_data:
                                        error_detail = f"FEC API error: {error_data['message']}"
                                    elif "error" in error_data:
                                        error_detail = f"FEC API error: {error_data['error']}"
                            except:
                                pass
                            raise Exception(error_detail) from e
                    except httpx.RequestError as e:
                        raise Exception(f"Failed to connect to FEC API: {str(e)}") from e
                
                # All retries exhausted
                if last_exception:
                    # Try to return cached data as fallback
                    if cache_key:
                        cached_data = await self._get_from_cache(cache_key)
                        if cached_data:
                            return cached_data
                    
                    error_detail = "Rate limit exceeded. Please try again later."
                    try:
                        if hasattr(last_exception, 'response') and last_exception.response:
                            error_data = last_exception.response.json()
                            if isinstance(error_data, dict) and "message" in error_data:
                                error_detail = error_data["message"]
                    except Exception as e:
                        logger.debug(f"Could not parse error response: {e}")
                    raise Exception(f"FEC API error: {error_detail}")
                
                # This should never be reached
                raise Exception("Unexpected error in API request")
        
        # Register request for deduplication
        if cache_key and use_cache:
            request_task = asyncio.create_task(_do_request())
            self._in_flight_requests[cache_key] = request_task
            
            try:
                result = await request_task
                return result
            finally:
                # Clean up
                self._in_flight_requests.pop(cache_key, None)
        else:
            # No cache, just execute directly
            return await _do_request()
    
    async def _refresh_cache_in_background(
        self,
        endpoint: str,
        params: Dict[str, Any],
        cache_key: str,
        cache_ttl: int
    ):
        """Refresh cache in background without blocking"""
        try:
            # Remove API key from params for cache key generation
            params_copy = {k: v for k, v in params.items() if k != "api_key"}
            cache_key_check = self._generate_cache_key(endpoint, params_copy)
            
            # Only refresh if not already in flight
            if cache_key_check not in self._in_flight_requests:
                logger.debug(f"Background refresh for {endpoint}")
                # Make request without waiting
                asyncio.create_task(self._make_request(
                    endpoint, params_copy, use_cache=True, cache_ttl=cache_ttl
                ))
        except Exception as e:
            logger.debug(f"Background refresh failed (non-critical): {e}")
    
    async def _handle_pagination(
        self,
        endpoint: str,
        params: Dict[str, Any],
        initial_data: Dict,
        max_results: Optional[int] = None
    ) -> Dict:
        """Handle pagination for API responses"""
        if "pagination" not in initial_data:
            return initial_data
            
        pagination = initial_data["pagination"]
        total_pages = pagination.get("pages", 1)
        
        if total_pages <= 1:
            return initial_data
        
        # Collect all results
        all_results = initial_data.get("results", [])
        url = f"{self.base_url}/{endpoint}"
        
        # Fetch remaining pages
        # Calculate how many pages we need based on max_results
        per_page = params.get("per_page", 100)
        if max_results:
            pages_needed = min(total_pages, (max_results + per_page - 1) // per_page)
        else:
            pages_needed = total_pages
            
        for page in range(2, pages_needed + 1):
            # Stop if we've reached the max_results limit
            if max_results and len(all_results) >= max_results:
                break
                
            params["page"] = page
            await self._wait_for_rate_limit()
            
            async with self._get_semaphore():
                try:
                    response = await self.client.get(url, params=params)
                    response.raise_for_status()
                    page_data = response.json()
                    page_results = page_data.get("results", [])
                    all_results.extend(page_results)
                    
                    # Stop if we've reached the limit
                    if max_results and len(all_results) >= max_results:
                        all_results = all_results[:max_results]
                        break
                except Exception as e:
                    # If pagination fails, return what we have
                    break
        
        initial_data["results"] = all_results
        return initial_data
    
    async def _query_local_candidates(
        self,
        name: Optional[str] = None,
        office: Optional[str] = None,
        state: Optional[str] = None,
        party: Optional[str] = None,
        year: Optional[int] = None,
        district: Optional[str] = None,
        limit: int = 20
    ) -> Optional[List[Dict]]:
        """Query candidates from local database"""
        if not self.bulk_data_enabled:
            return None
        
        try:
            async with AsyncSessionLocal() as session:
                query = select(Candidate)
                conditions = []
                
                if name:
                    conditions.append(Candidate.name.ilike(f"%{name}%"))
                if office:
                    conditions.append(Candidate.office == office)
                if state:
                    conditions.append(Candidate.state == state)
                if party:
                    conditions.append(Candidate.party == party)
                if district:
                    conditions.append(Candidate.district == district)
                if year:
                    # Check if year is in election_years JSON array
                    conditions.append(Candidate.election_years.contains([year]))
                
                if conditions:
                    query = query.where(and_(*conditions))
                
                query = query.limit(limit)
                result = await session.execute(query)
                candidates = result.scalars().all()
                
                if candidates:
                    result_list = []
                    for c in candidates:
                        candidate_dict = {
                            "candidate_id": c.candidate_id,
                            "name": c.name,
                            "office": c.office,
                            "party": c.party,
                            "state": c.state,
                            "district": c.district,
                            "election_years": c.election_years or [],
                            "active_through": c.active_through,
                            # Include contact information for offline use
                            "street_address": c.street_address,
                            "city": c.city,
                            "zip": c.zip,
                            "email": c.email,
                            "phone": c.phone,
                            "website": c.website,
                            # Include timestamp of when contact info was last updated
                            "contact_info_updated_at": c.updated_at.isoformat() if c.updated_at else None
                        }
                        if c.raw_data:
                            candidate_dict.update(c.raw_data)
                        result_list.append(candidate_dict)
                    return result_list
                return None
        except Exception as e:
            logger.warning(f"Error querying local candidates: {e}")
            return None
    
    async def _store_candidate(self, candidate_data: Dict):
        """Store candidate in local database with retry logic for database locks"""
        max_retries = 3
        retry_delay = 0.1
        
        for attempt in range(max_retries):
            try:
                # Use semaphore to serialize database writes
                async with self._db_write_semaphore:
                    async with AsyncSessionLocal() as session:
                        candidate_id = candidate_data.get("candidate_id")
                        if not candidate_id:
                            return
                        
                        result = await session.execute(
                            select(Candidate).where(Candidate.candidate_id == candidate_id)
                        )
                        existing = result.scalar_one_or_none()
                        
                        if existing:
                            # Update existing
                            existing.name = candidate_data.get("name") or candidate_data.get("candidate_name", "")
                            existing.office = candidate_data.get("office")
                            existing.party = candidate_data.get("party")
                            existing.state = candidate_data.get("state")
                            existing.district = candidate_data.get("district")
                            existing.election_years = candidate_data.get("election_years")
                            existing.active_through = candidate_data.get("active_through")
                            # Update contact info if available
                            contact_info = self._extract_candidate_contact_info(candidate_data)
                            if contact_info.get("street_address"):
                                existing.street_address = contact_info["street_address"]
                            if contact_info.get("city"):
                                existing.city = contact_info["city"]
                            if contact_info.get("zip"):
                                existing.zip = contact_info["zip"]
                            if contact_info.get("email"):
                                existing.email = contact_info["email"]
                            if contact_info.get("phone"):
                                existing.phone = contact_info["phone"]
                            if contact_info.get("website"):
                                existing.website = contact_info["website"]
                            existing.raw_data = candidate_data
                            existing.updated_at = datetime.utcnow()
                        else:
                            # Create new
                            contact_info = self._extract_candidate_contact_info(candidate_data)
                            candidate = Candidate(
                                candidate_id=candidate_id,
                                name=candidate_data.get("name") or candidate_data.get("candidate_name", ""),
                                office=candidate_data.get("office"),
                                party=candidate_data.get("party"),
                                state=candidate_data.get("state"),
                                district=candidate_data.get("district"),
                                election_years=candidate_data.get("election_years"),
                                active_through=candidate_data.get("active_through"),
                                street_address=contact_info.get("street_address"),
                                city=contact_info.get("city"),
                                zip=contact_info.get("zip"),
                                email=contact_info.get("email"),
                                phone=contact_info.get("phone"),
                                website=contact_info.get("website"),
                                raw_data=candidate_data
                            )
                            session.add(candidate)
                        
                        await session.commit()
                        return  # Success, exit retry loop
                        
            except Exception as e:
                error_str = str(e).lower()
                # Check if it's a database locked error
                if "database is locked" in error_str or "locked" in error_str:
                    if attempt < max_retries - 1:
                        # Exponential backoff
                        wait_time = retry_delay * (2 ** attempt)
                        logger.debug(f"Database locked, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.warning(f"Error storing candidate after {max_retries} retries: {e}")
                elif "UNIQUE constraint" in error_str:
                    # Unique constraint error is expected in race conditions, try to update instead
                    try:
                        async with self._db_write_semaphore:
                            async with AsyncSessionLocal() as session:
                                candidate_id = candidate_data.get("candidate_id")
                                if not candidate_id:
                                    return
                                
                                result = await session.execute(
                                    select(Candidate).where(Candidate.candidate_id == candidate_id)
                                )
                                existing = result.scalar_one_or_none()
                                
                                if existing:
                                    existing.name = candidate_data.get("name") or candidate_data.get("candidate_name", "")
                                    existing.office = candidate_data.get("office")
                                    existing.party = candidate_data.get("party")
                                    existing.state = candidate_data.get("state")
                                    existing.district = candidate_data.get("district")
                                    existing.election_years = candidate_data.get("election_years")
                                    existing.active_through = candidate_data.get("active_through")
                                    existing.raw_data = candidate_data
                                    existing.updated_at = datetime.utcnow()
                                    await session.commit()
                                    return
                    except Exception as update_error:
                        logger.debug(f"Error updating candidate after unique constraint: {update_error}")
                else:
                    # Not a lock or unique constraint error, don't retry
                    logger.warning(f"Error storing candidate: {e}")
                    return
    
    async def search_candidates(
        self, 
        name: Optional[str] = None,
        office: Optional[str] = None,
        state: Optional[str] = None,
        party: Optional[str] = None,
        year: Optional[int] = None,
        district: Optional[str] = None,
        limit: int = 20
    ) -> List[Dict]:
        """Search for candidates - queries local DB first, falls back to API"""
        # Try local database first
        if self.bulk_data_enabled:
            try:
                local_data = await self._query_local_candidates(
                    name=name, office=office, state=state, party=party, year=year, district=district, limit=limit
                )
                if local_data and len(local_data) > 0:
                    logger.debug(f"Found {len(local_data)} candidates in local database")
                    return local_data
            except Exception as e:
                logger.warning(f"Error querying local candidates, falling back to API: {e}")
        
        # Fall back to API
        logger.debug(f"Querying FEC API for candidates: name={name}, office={office}, state={state}")
        try:
            params = {
                "per_page": limit,
                "sort": "-election_years"
            }
            if name:
                params["q"] = name
            if office:
                params["office"] = office
            if state:
                params["state"] = state
            if party:
                params["party"] = party
            if year:
                params["election_year"] = year
            if district:
                params["district"] = district
            
            data = await self._make_request("candidates", params)
            results = data.get("results", [])
            
            # Store results in local DB
            if results and self.bulk_data_enabled:
                for candidate in results:
                    asyncio.create_task(self._store_candidate(candidate))
            
            return results
        except Exception as e:
            logger.error(f"API fallback failed for candidate search: {e}")
            return []
    
    async def get_race_candidates(
        self,
        office: str,
        state: str,
        district: Optional[str] = None,
        year: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get all candidates for a specific race"""
        params = {
            "office": office,
            "state": state,
            "per_page": limit,
            "sort": "-election_years"
        }
        if district:
            params["district"] = district
        if year:
            params["election_year"] = year
        
        data = await self._make_request("candidates", params)
        return data.get("results", [])
    
    async def _query_local_candidate(self, candidate_id: str) -> Optional[Dict]:
        """Query candidate from local database"""
        if not self.bulk_data_enabled:
            return None
        
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(Candidate).where(Candidate.candidate_id == candidate_id)
                )
                candidate = result.scalar_one_or_none()
                
                if candidate:
                    candidate_dict = {
                        "candidate_id": candidate.candidate_id,
                        "name": candidate.name,
                        "office": candidate.office,
                        "party": candidate.party,
                        "state": candidate.state,
                        "district": candidate.district,
                        "election_years": candidate.election_years or [],
                        "active_through": candidate.active_through,
                        # Include contact information for offline use
                        "street_address": candidate.street_address,
                        "city": candidate.city,
                        "zip": candidate.zip,
                        "email": candidate.email,
                        "phone": candidate.phone,
                        "website": candidate.website,
                        # Include timestamp of when contact info was last updated
                        "contact_info_updated_at": candidate.updated_at.isoformat() if candidate.updated_at else None
                    }
                    if candidate.raw_data:
                        candidate_dict.update(candidate.raw_data)
                    return candidate_dict
                return None
        except Exception as e:
            logger.warning(f"Error querying local candidate: {e}")
            return None
    
    async def get_candidate(self, candidate_id: str) -> Optional[Dict]:
        """Get candidate details by ID - queries local DB first, falls back to API"""
        # Try local database first
        if self.bulk_data_enabled:
            try:
                local_data = await self._query_local_candidate(candidate_id)
                if local_data:
                    logger.debug(f"Found candidate {candidate_id} in local database")
                    return local_data
            except Exception as e:
                logger.warning(f"Error querying local candidate {candidate_id}, falling back to API: {e}")
        
        # Fall back to API
        logger.debug(f"Querying FEC API for candidate {candidate_id}")
        try:
            params = {}
            data = await self._make_request(f"candidate/{candidate_id}", params)
            result = data.get("results", [{}])[0] if data.get("results") else None
            
            # Store in local DB
            if result and self.bulk_data_enabled:
                asyncio.create_task(self._store_candidate(result))
            
            return result
        except Exception as e:
            logger.error(f"API fallback failed for candidate {candidate_id}: {e}")
            return None
    
    async def _query_local_financial_totals(
        self,
        candidate_id: str,
        cycle: Optional[int] = None
    ) -> Optional[List[Dict]]:
        """Query financial totals from local database"""
        if not self.bulk_data_enabled:
            return None
        
        try:
            async with AsyncSessionLocal() as session:
                query = select(FinancialTotal).where(
                    FinancialTotal.candidate_id == candidate_id
                )
                if cycle:
                    query = query.where(FinancialTotal.cycle == cycle)
                else:
                    # Get most recent cycle
                    query = query.order_by(FinancialTotal.cycle.desc())
                
                result = await session.execute(query)
                financials = result.scalars().all()
                
                if financials:
                    result_list = []
                    for f in financials:
                        financial_dict = {
                            "candidate_id": f.candidate_id,
                            "cycle": f.cycle,
                            "receipts": f.total_receipts,
                            "disbursements": f.total_disbursements,
                            "cash_on_hand_end_period": f.cash_on_hand,
                            "contributions": f.total_contributions,
                            "individual_contributions": f.individual_contributions,
                            "pac_contributions": f.pac_contributions,
                            "party_contributions": f.party_contributions,
                            "loan_contributions": getattr(f, 'loan_contributions', 0.0)
                        }
                        if f.raw_data:
                            financial_dict.update(f.raw_data)
                        result_list.append(financial_dict)
                    return result_list
                return None
        except Exception as e:
            logger.warning(f"Error querying local financial totals: {e}")
            return None
    
    async def _store_financial_total(self, candidate_id: str, financial_data: Dict):
        """Store financial total in local database with retry logic for database locks"""
        max_retries = 3
        retry_delay = 0.1  # Start with 100ms
        
        for attempt in range(max_retries):
            try:
                # Use semaphore to serialize database writes
                async with self._db_write_semaphore:
                    async with AsyncSessionLocal() as session:
                        cycle = financial_data.get("cycle") or financial_data.get("two_year_transaction_period")
                        if not cycle:
                            return
                        
                        result = await session.execute(
                            select(FinancialTotal).where(
                                and_(
                                    FinancialTotal.candidate_id == candidate_id,
                                    FinancialTotal.cycle == cycle
                                )
                            )
                        )
                        existing = result.scalar_one_or_none()
                        
                        if existing:
                            # Update existing
                            existing.total_receipts = float(financial_data.get("receipts", 0))
                            existing.total_disbursements = float(financial_data.get("disbursements", 0))
                            existing.cash_on_hand = float(financial_data.get("cash_on_hand_end_period", 0))
                            existing.total_contributions = float(financial_data.get("contributions", 0))
                            existing.individual_contributions = float(financial_data.get("individual_contributions", 0))
                            existing.pac_contributions = float(financial_data.get("pac_contributions", 0))
                            existing.party_contributions = float(financial_data.get("party_contributions", 0))
                            existing.loan_contributions = float(financial_data.get("loan_contributions", 0) or financial_data.get("loans_received", 0) or 0)
                            existing.raw_data = financial_data
                            existing.updated_at = datetime.utcnow()
                        else:
                            # Create new
                            financial = FinancialTotal(
                                candidate_id=candidate_id,
                                cycle=cycle,
                                total_receipts=float(financial_data.get("receipts", 0)),
                                total_disbursements=float(financial_data.get("disbursements", 0)),
                                cash_on_hand=float(financial_data.get("cash_on_hand_end_period", 0)),
                                total_contributions=float(financial_data.get("contributions", 0)),
                                individual_contributions=float(financial_data.get("individual_contributions", 0)),
                                pac_contributions=float(financial_data.get("pac_contributions", 0)),
                                party_contributions=float(financial_data.get("party_contributions", 0)),
                                loan_contributions=float(financial_data.get("loan_contributions", 0) or financial_data.get("loans_received", 0) or 0),
                                raw_data=financial_data
                            )
                            session.add(financial)
                        
                        await session.commit()
                        return  # Success, exit retry loop
                        
            except Exception as e:
                error_str = str(e).lower()
                # Check if it's a database locked error
                if "database is locked" in error_str or "locked" in error_str:
                    if attempt < max_retries - 1:
                        # Exponential backoff
                        wait_time = retry_delay * (2 ** attempt)
                        logger.debug(f"Database locked, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.warning(f"Error storing financial total after {max_retries} retries: {e}")
                else:
                    # Not a lock error, don't retry
                    logger.warning(f"Error storing financial total: {e}")
                    return
    
    async def get_candidate_totals(
        self, 
        candidate_id: str,
        cycle: Optional[int] = None
    ) -> List[Dict]:
        """Get candidate financial totals - queries local DB first, falls back to API"""
        # Try local database first
        if self.bulk_data_enabled:
            try:
                local_data = await self._query_local_financial_totals(candidate_id, cycle)
                if local_data and len(local_data) > 0:
                    logger.debug(f"Found financial totals for candidate {candidate_id} (cycle {cycle}) in local database")
                    return local_data
            except Exception as e:
                logger.warning(f"Error querying local financial totals for {candidate_id}, falling back to API: {e}")
        
        # Fall back to API
        logger.debug(f"Querying FEC API for financial totals: candidate {candidate_id}, cycle {cycle}")
        try:
            params = {}
            if cycle:
                params["cycle"] = cycle
            
            data = await self._make_request(f"candidate/{candidate_id}/totals", params)
            results = data.get("results", [])
            
            # Store results in local DB
            if results and self.bulk_data_enabled:
                for financial in results:
                    asyncio.create_task(self._store_financial_total(candidate_id, financial))
            
            return results
        except Exception as e:
            logger.error(f"API fallback failed for financial totals (candidate {candidate_id}, cycle {cycle}): {e}")
            return []
    
    async def _get_latest_contribution_date(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        contributor_name: Optional[str] = None
    ) -> Optional[datetime]:
        """
        Get the latest contribution date from database for a given query to determine what's new.
        
        This method queries the contributions table which includes:
        - Contributions imported from bulk CSV files
        - Contributions fetched from the FEC API
        - Any other contributions stored in the database
        
        Returns the most recent contribution_date for the given filters, which is used
        to determine what new contributions need to be fetched from the API.
        """
        try:
            async with AsyncSessionLocal() as session:
                query = select(func.max(Contribution.contribution_date))
                conditions = []
                
                if candidate_id:
                    conditions.append(Contribution.candidate_id == candidate_id)
                if committee_id:
                    conditions.append(Contribution.committee_id == committee_id)
                if contributor_name:
                    conditions.append(
                        Contribution.contributor_name.ilike(f"%{contributor_name}%")
                    )
                
                if conditions:
                    query = query.where(and_(*conditions))
                
                result = await session.execute(query)
                latest_date = result.scalar_one_or_none()
                
                if latest_date:
                    logger.debug(f"Latest contribution date in DB: {latest_date.strftime('%Y-%m-%d')} "
                              f"(candidate_id={candidate_id}, committee_id={committee_id}, "
                              f"contributor_name={contributor_name})")
                
                return latest_date
        except Exception as e:
            logger.debug(f"Error getting latest contribution date: {e}")
            return None
    
    async def _fetch_contribution_by_id(
        self,
        contribution_id: str,
        committee_id: Optional[str] = None,
        candidate_id: Optional[str] = None
    ) -> Optional[Dict]:
        """
        Fetch a single contribution from FEC API by contribution ID (sub_id).
        
        Args:
            contribution_id: The contribution sub_id to fetch
            committee_id: Optional committee ID to narrow the search
            candidate_id: Optional candidate ID to narrow the search
            
        Returns:
            Dictionary with contribution data, or None if not found
        """
        try:
            params = {
                "per_page": 1,
                "sub_id": contribution_id
            }
            
            # Add optional filters to narrow search
            if committee_id:
                params["committee_id"] = committee_id
            if candidate_id:
                params["candidate_id"] = candidate_id
            
            # Add two_year_transaction_period if we have candidate_id
            if candidate_id:
                current_year = datetime.now().year
                two_year_transaction_period = (current_year // 2) * 2
                params["two_year_transaction_period"] = two_year_transaction_period
            
            logger.debug(f"Fetching contribution {contribution_id} from FEC API")
            data = await self._make_request("schedules/schedule_a", params, use_cache=False)
            results = data.get("results", [])
            
            if results:
                contrib = results[0]
                logger.debug(f"Found contribution {contribution_id} in FEC API")
                return contrib
            else:
                logger.debug(f"Contribution {contribution_id} not found in FEC API")
                return None
                
        except Exception as e:
            logger.warning(f"Error fetching contribution {contribution_id} from FEC API: {e}")
            return None
    
    async def get_contribution_date(
        self,
        contribution_id: str,
        contribution_obj: Optional[Contribution] = None,
        committee_id: Optional[str] = None,
        candidate_id: Optional[str] = None,
        raw_data: Optional[Dict] = None
    ) -> Optional[datetime]:
        """
        Get contribution date following the pattern: DB -> raw_data -> API -> store in DB.
        This is the centralized method for all date extraction that ensures API responses are stored.
        
        Args:
            contribution_id: The contribution sub_id
            contribution_obj: Optional Contribution object (if already loaded)
            committee_id: Optional committee ID for API queries
            candidate_id: Optional candidate ID for API queries
            raw_data: Optional raw_data dict (if already loaded)
            
        Returns:
            datetime object if date found, None otherwise
        """
        from app.utils.date_utils import extract_date_from_raw_data
        
        # Step 1: Check database field first
        if contribution_obj and contribution_obj.contribution_date:
            logger.debug(f"get_contribution_date: Found date in DB field for {contribution_id}")
            return contribution_obj.contribution_date
        
        # Step 2: Check raw_data
        if raw_data is None and contribution_obj and contribution_obj.raw_data:
            raw_data = contribution_obj.raw_data
        
        if raw_data:
            date_from_raw = extract_date_from_raw_data(raw_data)
            if date_from_raw:
                logger.debug(f"get_contribution_date: Found date in raw_data for {contribution_id}")
                # If we found date in raw_data, update DB field for future queries (in background to avoid blocking)
                # Schedule background update to avoid blocking the current query
                if contribution_obj:
                    asyncio.create_task(
                        self._update_contribution_date_from_raw_data(contribution_id, date_from_raw)
                    )
                return date_from_raw
        
        # Step 3: Schedule API query in background (non-blocking)
        # Don't block - schedule background task to fetch and store
        logger.info(f"get_contribution_date: Date not in DB or raw_data, scheduling API fetch for {contribution_id}")
        asyncio.create_task(
            self._backfill_contribution_date(contribution_id, committee_id, candidate_id)
        )
        
        # Return None immediately - date will be available on next query after background task completes
        return None
    
    async def _update_contribution_date_from_raw_data(
        self,
        contribution_id: str,
        date_value: datetime
    ):
        """
        Background task to update contribution_date field from raw_data.
        This ensures dates found in raw_data are persisted to the DB field.
        """
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(Contribution).where(Contribution.contribution_id == contribution_id)
                )
                contrib = result.scalar_one_or_none()
                if contrib and not contrib.contribution_date:
                    contrib.contribution_date = date_value
                    await session.commit()
                    logger.debug(f"_update_contribution_date_from_raw_data: Updated DB field for {contribution_id}")
        except Exception as e:
            logger.warning(f"_update_contribution_date_from_raw_data: Error updating date for {contribution_id}: {e}")
    
    async def _store_api_response_in_db(
        self,
        contribution_id: str,
        api_response: Dict,
        extracted_date: Optional[datetime] = None
    ):
        """
        Store FEC API response in database to avoid future API calls.
        This ensures we never query the API twice for the same contribution.
        
        Args:
            contribution_id: The contribution sub_id
            api_response: The full API response dictionary
            extracted_date: Optional extracted date from the API response
        """
        try:
            async with AsyncSessionLocal() as session:
                # Find the contribution
                result = await session.execute(
                    select(Contribution).where(Contribution.contribution_id == contribution_id)
                )
                contrib = result.scalar_one_or_none()
                
                if contrib:
                    # Update the date if we found one
                    if extracted_date:
                        contrib.contribution_date = extracted_date
                        logger.info(f"_store_api_response_in_db: Storing date {extracted_date} for {contribution_id}")
                    
                    # Store the complete API response in raw_data
                    # Merge with existing raw_data to preserve any existing fields
                    if contrib.raw_data and isinstance(contrib.raw_data, dict):
                        # Merge API response into existing raw_data (API data takes precedence)
                        contrib.raw_data.update(api_response)
                    else:
                        # If no existing raw_data, use the API response directly
                        contrib.raw_data = api_response
                    
                    # Mark JSON column as modified so SQLAlchemy detects the change
                    from sqlalchemy.orm.attributes import flag_modified
                    flag_modified(contrib, 'raw_data')
                    
                    # Commit the update
                    await session.commit()
                    logger.info(f"_store_api_response_in_db: Successfully stored API response for contribution {contribution_id}")
                else:
                    logger.warning(f"_store_api_response_in_db: Contribution {contribution_id} not found in database")
        except Exception as e:
            logger.error(f"_store_api_response_in_db: Error storing API response for {contribution_id}: {e}", exc_info=True)
    
    async def _backfill_contribution_date(
        self,
        contribution_id: str,
        committee_id: Optional[str] = None,
        candidate_id: Optional[str] = None
    ):
        """
        Background task to fetch contribution date from FEC API and update database.
        This runs asynchronously and doesn't block the main query.
        Always stores the API response in DB to avoid future API calls.
        
        Args:
            contribution_id: The contribution sub_id to fetch
            committee_id: Optional committee ID to narrow the search
            candidate_id: Optional candidate ID to narrow the search
        """
        try:
            logger.info(f"_backfill_contribution_date: Starting background fetch for contribution_id: {contribution_id}")
            
            # Fetch from FEC API
            api_contrib = await self._fetch_contribution_by_id(contribution_id, committee_id, candidate_id)
            
            if not api_contrib:
                logger.warning(f"_backfill_contribution_date: FEC API returned no data for contribution_id: {contribution_id}")
                return
            
            # Extract date from API response
            from app.utils.date_utils import extract_date_from_raw_data
            api_date = extract_date_from_raw_data(api_contrib)
            
            # Always store API response in DB (even if no date found) to avoid future API calls
            await self._store_api_response_in_db(contribution_id, api_contrib, api_date)
            
            if api_date:
                logger.info(f"_backfill_contribution_date: Successfully fetched and stored date {api_date} for {contribution_id}")
            else:
                logger.warning(f"_backfill_contribution_date: No date found for {contribution_id} (API response was still stored)")
                        
        except Exception as e:
            logger.error(f"_backfill_contribution_date: Error in background task for contribution_id {contribution_id}: {e}", exc_info=True)
    
    async def _query_local_contributions(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        contributor_name: Optional[str] = None,
        min_amount: Optional[float] = None,
        max_amount: Optional[float] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        limit: int = 100,
        two_year_transaction_period: Optional[int] = None
    ) -> Optional[List[Dict]]:
        """Query contributions from local database"""
        if not self.bulk_data_enabled:
            return None
        
        try:
            async with AsyncSessionLocal() as session:
                # Build query - use load_only to only load columns that exist
                # Some columns (amendment_indicator, report_type, etc.) may not exist if migrations haven't been run
                from sqlalchemy.orm import load_only
                query = select(Contribution).options(
                    load_only(
                        Contribution.id,
                        Contribution.contribution_id,
                        Contribution.candidate_id,
                        Contribution.committee_id,
                        Contribution.contributor_name,
                        Contribution.contributor_city,
                        Contribution.contributor_state,
                        Contribution.contributor_zip,
                        Contribution.contributor_employer,
                        Contribution.contributor_occupation,
                        Contribution.contribution_amount,
                        Contribution.contribution_date,
                        Contribution.contribution_type,
                        Contribution.raw_data,
                        Contribution.created_at
                    )
                )
                conditions = []
                
                if candidate_id:
                    conditions.append(Contribution.candidate_id == candidate_id)
                if committee_id:
                    conditions.append(Contribution.committee_id == committee_id)
                if contributor_name:
                    conditions.append(
                        Contribution.contributor_name.ilike(f"%{contributor_name}%")
                    )
                if min_amount is not None:
                    conditions.append(Contribution.contribution_amount >= min_amount)
                if max_amount is not None:
                    conditions.append(Contribution.contribution_amount <= max_amount)
                if min_date:
                    try:
                        min_date_obj = datetime.strptime(min_date, "%Y-%m-%d")
                        conditions.append(Contribution.contribution_date >= min_date_obj)
                    except ValueError:
                        pass
                if max_date:
                    try:
                        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                        conditions.append(Contribution.contribution_date <= max_date_obj)
                    except ValueError:
                        pass
                
                if conditions:
                    query = query.where(and_(*conditions))
                
                # Order by date descending
                query = query.order_by(Contribution.contribution_date.desc().nulls_last())
                
                # Apply limit
                query = query.limit(limit)
                
                result = await session.execute(query)
                contributions = result.scalars().all()
                
                # Log how many contributions were found
                logger.debug(f"Database query found {len(contributions)} contributions for candidate_id={candidate_id}, limit={limit}")
                
                if contributions:
                    # Convert to dict format matching API response
                    result_list = []
                    for c in contributions:
                        # Try to get amount from raw_data if stored amount is 0
                        amount = float(c.contribution_amount) if c.contribution_amount else 0.0
                        if amount == 0.0 and c.raw_data and isinstance(c.raw_data, dict):
                            # Try to extract from raw_data (for data imported with old mapping)
                            for amt_key in ['TRANSACTION_AMT', 'CONTB_AMT', 'contribution_amount', 'transaction_amt']:
                                if amt_key in c.raw_data:
                                    try:
                                        amt_val = str(c.raw_data[amt_key]).strip()
                                        amt_val = amt_val.replace('$', '').replace(',', '').strip()
                                        if amt_val:
                                            amount = float(amt_val)
                                            break
                                    except (ValueError, TypeError):
                                        continue
                        
                        # Try to get candidate_id from raw_data if missing
                        contrib_candidate_id = c.candidate_id
                        if not contrib_candidate_id and c.raw_data and isinstance(c.raw_data, dict):
                            contrib_candidate_id = c.raw_data.get('CAND_ID') or c.raw_data.get('candidate_id')
                        
                        # Use the candidate_id from the contribution, or fall back to query parameter
                        final_candidate_id = contrib_candidate_id or candidate_id
                        
                        # Extract date using centralized method: DB -> raw_data -> API (background) -> store
                        # This method ensures:
                        # 1. Checks DB field first
                        # 2. Checks raw_data second
                        # 3. If missing, schedules background API fetch (non-blocking)
                        # 4. API response is always stored in DB for future queries
                        contrib_date = await self.get_contribution_date(
                            contribution_id=c.contribution_id,
                            contribution_obj=c,
                            committee_id=c.committee_id,
                            candidate_id=final_candidate_id
                        )
                        
                        # Format date as string
                        date_str = None
                        if contrib_date:
                            if isinstance(contrib_date, datetime):
                                date_str = contrib_date.strftime("%Y-%m-%d")
                                logger.debug(f"_query_local_contributions: Formatted datetime to string: {date_str}")
                            else:
                                from app.utils.date_utils import serialize_date
                                date_str = serialize_date(contrib_date)
                                if date_str:
                                    logger.debug(f"_query_local_contributions: Serialized date to string: {date_str}")
                                else:
                                    logger.warning(f"_query_local_contributions: serialize_date returned None for: {contrib_date}")
                        else:
                            logger.warning(f"_query_local_contributions: contrib_date is None, date_str will be None")
                        
                        contrib_dict = {
                            "sub_id": c.contribution_id,
                            "contribution_id": c.contribution_id,
                            "candidate_id": final_candidate_id,
                            "committee_id": c.committee_id,
                            "contributor_name": c.contributor_name,
                            "contributor_city": c.contributor_city,
                            "contributor_state": c.contributor_state,
                            "contributor_zip": c.contributor_zip,
                            "contributor_employer": c.contributor_employer,
                            "contributor_occupation": c.contributor_occupation,
                            "contribution_amount": amount,
                            "contribution_receipt_date": date_str,
                            "contribution_date": date_str,
                            "contribution_type": c.contribution_type,
                            "receipt_type": None
                        }
                        # Add raw_data fields if available (but don't overwrite corrected values)
                        if c.raw_data and isinstance(c.raw_data, dict):
                            # Only add fields that aren't already set
                            for key, value in c.raw_data.items():
                                if key not in contrib_dict or not contrib_dict[key]:
                                    contrib_dict[key] = value
                        result_list.append(contrib_dict)
                    return result_list
                
                return None
                
        except Exception as e:
            logger.warning(f"Error querying local contributions: {e}")
            return None
    
    async def get_contributions(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        contributor_name: Optional[str] = None,
        min_amount: Optional[float] = None,
        max_amount: Optional[float] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        limit: int = 100,
        two_year_transaction_period: Optional[int] = None,
        fetch_new_only: bool = True
    ) -> List[Dict]:
        """Get contributions/schedules/schedule_a - queries local DB first, falls back to API for new data only"""
        local_data = []
        latest_db_date = None
        
        # Try local database first if bulk data is enabled
        if self.bulk_data_enabled:
            try:
                # First, try querying by candidate_id directly
                local_data = await self._query_local_contributions(
                    candidate_id=candidate_id,
                    committee_id=committee_id,
                    contributor_name=contributor_name,
                    min_amount=min_amount,
                    max_amount=max_amount,
                    min_date=min_date,
                    max_date=max_date,
                    limit=limit,
                    two_year_transaction_period=two_year_transaction_period
                ) or []
                
                # If we have a candidate_id but no committee_id, also query by committees
                # Many contributions in bulk data are linked via committee_id, not candidate_id
                if candidate_id and not committee_id and len(local_data) < limit:
                    try:
                        # Get committees for this candidate
                        committees = await self.get_committees(candidate_id=candidate_id, limit=100)
                        if committees:
                            # Query contributions for each committee and merge
                            committee_ids = [c.get('committee_id') for c in committees if c.get('committee_id')]
                            for comm_id in committee_ids[:50]:  # Limit to avoid too many queries
                                if len(local_data) >= limit:
                                    break
                                committee_contribs = await self._query_local_contributions(
                                    committee_id=comm_id,
                                    contributor_name=contributor_name,
                                    min_amount=min_amount,
                                    max_amount=max_amount,
                                    min_date=min_date,
                                    max_date=max_date,
                                    limit=limit - len(local_data),
                                    two_year_transaction_period=two_year_transaction_period
                                ) or []
                                
                                # Merge, avoiding duplicates and ONLY including contributions that match the candidate_id
                                existing_ids = {c.get('contribution_id') or c.get('sub_id') for c in local_data}
                                for contrib in committee_contribs:
                                    contrib_id = contrib.get('contribution_id') or contrib.get('sub_id')
                                    if contrib_id and contrib_id not in existing_ids:
                                        # Only include if candidate_id matches or is missing (will be set to requested candidate_id)
                                        contrib_candidate_id = contrib.get('candidate_id')
                                        if not contrib_candidate_id or contrib_candidate_id == candidate_id:
                                            if not contrib.get('candidate_id'):
                                                contrib['candidate_id'] = candidate_id
                                            local_data.append(contrib)
                                            existing_ids.add(contrib_id)
                    except Exception as e:
                        logger.debug(f"Error querying contributions by committees: {e}")
                
                if local_data:
                    logger.debug(f"Found {len(local_data)} contributions in local database (after committee queries)")
                    
                    # If we have local data and fetch_new_only is True, determine latest date
                    # to only fetch new contributions from API
                    if fetch_new_only:
                        latest_db_date = await self._get_latest_contribution_date(
                            candidate_id=candidate_id,
                            committee_id=committee_id,
                            contributor_name=contributor_name
                        )
                        
                        if latest_db_date:
                            # Fetch contributions with a lookback window to catch late-filed contributions
                            # FEC allows committees to file late or amend previous filings, so contributions
                            # with earlier dates may appear in the API after we've already fetched newer ones.
                            # The lookback window ensures we catch these late-filed contributions.
                            # The database's unique constraint on contribution_id will handle deduplication
                            # and amendments (updated data for same contribution_id).
                            lookback_date = latest_db_date - timedelta(days=self.contribution_lookback_days)
                            new_min_date = lookback_date.strftime("%Y-%m-%d")
                            
                            if not min_date or new_min_date >= min_date:
                                min_date = new_min_date
                                logger.info(f"Fetching contributions from {min_date} onwards "
                                          f"(latest in DB: {latest_db_date.strftime('%Y-%m-%d')}, "
                                          f"lookback: {self.contribution_lookback_days} days) "
                                          f"- duplicates and amendments will be handled via contribution_id")
                            else:
                                logger.debug(f"User-specified min_date {min_date} is earlier than calculated lookback date, fetching all requested data")
                        else:
                            logger.debug("No latest date found in database, will fetch all contributions")
                    
                    # If we have enough local data and fetch_new_only is False, we can return early
                    # Otherwise, we'll fetch new data and merge
                    if len(local_data) >= limit and not fetch_new_only:
                        logger.debug(f"Have enough local data ({len(local_data)}), returning without API call")
                        return local_data[:limit]
            except Exception as e:
                logger.warning(f"Error querying local contributions, falling back to API: {e}")
        
        # Fetch new contributions from API (if any)
        logger.debug(f"Querying FEC API for contributions: candidate_id={candidate_id}, committee_id={committee_id}, min_date={min_date}")
        try:
            committees = None
            # If only candidate_id is provided, we need to get committees first
            # or provide two_year_transaction_period
            if candidate_id and not committee_id and not two_year_transaction_period:
                # Try to determine the appropriate cycle from candidate's election years
                candidate = await self.get_candidate(candidate_id)
                if candidate and candidate.get('election_years'):
                    # Use the most recent election year as the cycle
                    # For FEC, the cycle is the election year itself
                    election_years = candidate.get('election_years', [])
                    if election_years:
                        # Get the most recent election year
                        most_recent_election = max(election_years)
                        # For FEC API, two_year_transaction_period should be the election year
                        two_year_transaction_period = most_recent_election
                        logger.debug(f"Using election year {most_recent_election} as cycle for candidate {candidate_id}")
                
                # Get committees for the candidate to use committee_id filter
                committees = await self.get_committees(candidate_id=candidate_id, limit=100)
                logger.debug(f"Found {len(committees) if committees else 0} committees for candidate {candidate_id}, cycle {two_year_transaction_period}")
            
            if committees and len(committees) > 0:
                # Query contributions for each committee and combine
                all_contributions = []
                # Use determined cycle or fallback to current election cycle
                if not two_year_transaction_period:
                    current_year = datetime.now().year
                    # Round down to nearest even year
                    two_year_transaction_period = (current_year // 2) * 2
                
                # Query all committees, but limit per-committee requests to avoid rate limits
                # For large limits, we'll query more committees
                max_committees = 50 if limit > 1000 else 10
                for committee in committees[:max_committees]:
                    comm_id = committee.get('committee_id')
                    if comm_id:
                        params = {
                            "per_page": 100,  # FEC API max is 100
                            "sort": "-contribution_receipt_date",
                            "committee_id": comm_id,
                            "two_year_transaction_period": two_year_transaction_period,
                            "_original_limit": limit  # Store original limit for pagination
                        }
                        if contributor_name:
                            params["contributor_name"] = contributor_name
                        if min_amount:
                            params["min_amount"] = min_amount
                        if max_amount:
                            params["max_amount"] = max_amount
                        if min_date:
                            params["min_date"] = min_date
                        if max_date:
                            params["max_date"] = max_date
                        
                        try:
                            logger.debug(f"Querying contributions for committee {comm_id} with cycle {two_year_transaction_period}")
                            data = await self._make_request("schedules/schedule_a", params)
                            committee_contribs = data.get("results", [])
                            logger.debug(f"API returned {len(committee_contribs)} contributions for committee {comm_id}")
                            
                            # Store contributions in database for caching
                            if committee_contribs:
                                logger.debug(f"Storing {len(committee_contribs)} contributions from committee {comm_id} in database")
                                for contrib in committee_contribs:
                                    # Ensure candidate_id is set when storing contributions fetched via committee
                                    if candidate_id and not contrib.get('candidate_id'):
                                        contrib['candidate_id'] = candidate_id
                                    asyncio.create_task(self._store_contribution(contrib))
                            
                            # Merge with local data, avoiding duplicates
                            existing_ids = {c.get('contribution_id') or c.get('sub_id') for c in all_contributions}
                            for contrib in committee_contribs:
                                contrib_id = contrib.get('contribution_id') or contrib.get('sub_id')
                                if contrib_id and contrib_id not in existing_ids:
                                    all_contributions.append(contrib)
                                    existing_ids.add(contrib_id)
                            
                            # Continue querying committees until we have enough contributions
                            # Don't break early - we want to get all contributions across all committees
                            if len(all_contributions) >= limit * 2:  # Get extra to account for duplicates
                                logger.debug(f"Collected {len(all_contributions)} contributions, stopping committee queries")
                                break
                        except Exception:
                            continue  # Skip if this committee fails
                
                # Merge with local data
                if local_data:
                    existing_ids = {c.get('contribution_id') or c.get('sub_id') for c in all_contributions}
                    for contrib in local_data:
                        contrib_id = contrib.get('contribution_id') or contrib.get('sub_id')
                        if contrib_id and contrib_id not in existing_ids:
                            all_contributions.append(contrib)
                            existing_ids.add(contrib_id)
                
                # Sort by date descending
                all_contributions.sort(
                    key=lambda x: (
                        datetime.strptime(x.get('contribution_date') or x.get('contribution_receipt_date') or '1900-01-01', '%Y-%m-%d')
                        if isinstance(x.get('contribution_date') or x.get('contribution_receipt_date'), str)
                        else (x.get('contribution_date') or x.get('contribution_receipt_date') or datetime(1900, 1, 1))
                    ),
                    reverse=True
                )
                
                new_count = len(all_contributions) - len(local_data) if local_data else len(all_contributions)
                logger.info(f"Returning {len(all_contributions)} total contributions ({len(local_data)} from DB, {new_count} new from API)")
                return all_contributions[:limit]
            else:
                # No committees found, try with two_year_transaction_period
                logger.debug(f"No committees found for candidate {candidate_id}, trying direct API query")
                # If we haven't determined it from candidate yet, try to get it
                if not two_year_transaction_period and candidate_id:
                    candidate = await self.get_candidate(candidate_id)
                    if candidate and candidate.get('election_years'):
                        election_years = candidate.get('election_years', [])
                        if election_years:
                            most_recent_election = max(election_years)
                            two_year_transaction_period = most_recent_election
                            logger.debug(f"Using election year {most_recent_election} as cycle for candidate {candidate_id}")
                
                # Fallback to current election cycle if still not set
                if not two_year_transaction_period:
                    current_year = datetime.now().year
                    # Round down to nearest even year
                    two_year_transaction_period = (current_year // 2) * 2
                    logger.debug(f"Using default cycle {two_year_transaction_period} for candidate {candidate_id}")
            
            params = {
                "per_page": 100,  # FEC API max is 100, pagination will handle more
                "sort": "-contribution_receipt_date",
                "_original_limit": limit  # Store original limit for pagination
            }
            
            # Always add two_year_transaction_period if not provided (must be even year)
            if not two_year_transaction_period:
                current_year = datetime.now().year
                # Round down to nearest even year
                two_year_transaction_period = (current_year // 2) * 2
            
            # Add two_year_transaction_period - API requires it for schedule_a
            params["two_year_transaction_period"] = two_year_transaction_period
            
            if candidate_id:
                params["candidate_id"] = candidate_id
            if committee_id:
                params["committee_id"] = committee_id
            if contributor_name:
                params["contributor_name"] = contributor_name
            if min_amount:
                params["min_amount"] = min_amount
            if max_amount:
                params["max_amount"] = max_amount
            if min_date:
                params["min_date"] = min_date
            if max_date:
                params["max_date"] = max_date
            
            logger.debug(f"Querying contributions directly with candidate_id={candidate_id}, cycle={two_year_transaction_period}")
            data = await self._make_request("schedules/schedule_a", params)
            api_results = data.get("results", [])
            logger.debug(f"Direct API query returned {len(api_results)} contributions for candidate {candidate_id}")
            
            # Store new contributions in database for caching
            if api_results:
                logger.info(f"Storing {len(api_results)} new contributions from API in database")
                # Store contributions in batches to avoid exhausting connection pool
                # Limit concurrent storage operations to prevent connection pool exhaustion
                batch_size = 50  # Store 50 contributions at a time
                semaphore = asyncio.Semaphore(10)  # Max 10 concurrent storage operations
                
                async def store_with_semaphore(contrib_data):
                    async with semaphore:
                        try:
                            await self._store_contribution(contrib_data)
                        except Exception as e:
                            # Log but don't fail - connection pool exhaustion is expected under load
                            if "QueuePool" in str(e) or "timeout" in str(e).lower():
                                logger.debug(f"Connection pool exhausted while storing contribution, skipping: {e}")
                            else:
                                logger.warning(f"Error storing contribution: {e}")
                
                # Process in batches to avoid overwhelming the database
                for i in range(0, len(api_results), batch_size):
                    batch = api_results[i:i + batch_size]
                    tasks = []
                    for contrib in batch:
                        # Ensure candidate_id is set when storing contributions
                        if candidate_id and not contrib.get('candidate_id'):
                            contrib['candidate_id'] = candidate_id
                        tasks.append(store_with_semaphore(contrib))
                    
                    # Wait for batch to complete before starting next batch
                    await asyncio.gather(*tasks, return_exceptions=True)
                    # Small delay between batches to allow connections to be released
                    if i + batch_size < len(api_results):
                        await asyncio.sleep(0.1)
            
            # Merge local and API results, avoiding duplicates
            all_results = local_data.copy() if local_data else []
            existing_ids = {c.get('contribution_id') or c.get('sub_id') for c in all_results}
            
            for contrib in api_results:
                contrib_id = contrib.get('contribution_id') or contrib.get('sub_id')
                if contrib_id and contrib_id not in existing_ids:
                    all_results.append(contrib)
                    existing_ids.add(contrib_id)
            
            # Sort by date descending and limit
            all_results.sort(
                key=lambda x: (
                    datetime.strptime(x.get('contribution_date') or x.get('contribution_receipt_date') or '1900-01-01', '%Y-%m-%d')
                    if isinstance(x.get('contribution_date') or x.get('contribution_receipt_date'), str)
                    else (x.get('contribution_date') or x.get('contribution_receipt_date') or datetime(1900, 1, 1))
                ),
                reverse=True
            )
            
            logger.info(f"Returning {len(all_results)} total contributions ({len(local_data)} from DB, {len(api_results)} new from API)")
            return all_results[:limit]
        except Exception as e:
            logger.error(f"API fallback failed for contributions: {e}")
            # If we have local data, return it even if API failed
            if local_data:
                logger.info(f"Returning {len(local_data)} contributions from database cache (API failed)")
                return local_data[:limit]
            return []
    
    async def get_expenditures(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_amount: Optional[float] = None,
        max_amount: Optional[float] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        limit: int = 100,
        two_year_transaction_period: Optional[int] = None
    ) -> List[Dict]:
        """Get expenditures/schedules/schedule_b - queries local DB first, falls back to API"""
        # Note: Local query for expenditures not yet implemented, will add if needed
        # For now, always query API
        logger.debug(f"Querying FEC API for expenditures: candidate_id={candidate_id}, committee_id={committee_id}")
        try:
            # If only candidate_id is provided, get committees first
            if candidate_id and not committee_id and not two_year_transaction_period:
                committees = await self.get_committees(candidate_id=candidate_id, limit=100)
                if committees:
                    all_expenditures = []
                    # Get current election cycle for two_year_transaction_period (must be even year)
                    current_year = datetime.now().year
                    # Round down to nearest even year
                    default_cycle = (current_year // 2) * 2
                    
                    for committee in committees[:10]:
                        comm_id = committee.get('committee_id')
                        if comm_id:
                            params = {
                                "per_page": min(limit, 1000),
                                "sort": "-disbursement_date",
                                "committee_id": comm_id,
                                "two_year_transaction_period": two_year_transaction_period or default_cycle
                            }
                            if min_amount:
                                params["min_amount"] = min_amount
                            if max_amount:
                                params["max_amount"] = max_amount
                            if min_date:
                                params["min_date"] = min_date
                            if max_date:
                                params["max_date"] = max_date
                            
                            try:
                                data = await self._make_request("schedules/schedule_b", params)
                                all_expenditures.extend(data.get("results", []))
                                if len(all_expenditures) >= limit:
                                    break
                            except Exception:
                                continue
                    
                    return all_expenditures[:limit]
                else:
                    # No committees found, try with two_year_transaction_period
                    # Use current election cycle (must be even year)
                    current_year = datetime.now().year
                    # Round down to nearest even year
                    two_year_transaction_period = (current_year // 2) * 2
            
            params = {
                "per_page": 100,  # FEC API max is 100, pagination will handle more
                "sort": "-disbursement_date",  # Fixed: use disbursement_date not expenditure_date
                "_original_limit": limit  # Store original limit for pagination
            }
            
            # Always add two_year_transaction_period if not provided (must be even year)
            current_year = datetime.now().year
            # Round down to nearest even year
            default_cycle = (current_year // 2) * 2
            if not two_year_transaction_period:
                two_year_transaction_period = default_cycle
            
            # Add two_year_transaction_period - API requires it for schedule_b
            params["two_year_transaction_period"] = two_year_transaction_period
            
            if candidate_id:
                params["candidate_id"] = candidate_id
            if committee_id:
                params["committee_id"] = committee_id
            if min_amount:
                params["min_amount"] = min_amount
            if max_amount:
                params["max_amount"] = max_amount
            if min_date:
                params["min_date"] = min_date
            if max_date:
                params["max_date"] = max_date
            
            data = await self._make_request("schedules/schedule_b", params)
            results = data.get("results", [])
            # Limit results to requested limit
            return results[:limit]
        except Exception as e:
            logger.error(f"API query failed for expenditures: {e}")
            return []
    
    async def _query_local_committees(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        limit: int = 100
    ) -> Optional[List[Dict]]:
        """Query committees from local database"""
        if not self.bulk_data_enabled:
            return None
        
        try:
            async with AsyncSessionLocal() as session:
                query = select(Committee)
                conditions = []
                
                if committee_id:
                    conditions.append(Committee.committee_id == committee_id)
                if candidate_id:
                    # Check if candidate_id is in candidate_ids JSON array
                    conditions.append(Committee.candidate_ids.contains([candidate_id]))
                
                if conditions:
                    query = query.where(and_(*conditions))
                
                query = query.limit(limit)
                result = await session.execute(query)
                committees = result.scalars().all()
                
                if committees:
                    result_list = []
                    for c in committees:
                        committee_dict = {
                            "committee_id": c.committee_id,
                            "name": c.name,
                            "committee_type": c.committee_type,
                            "committee_type_full": c.committee_type_full,
                            "candidate_ids": c.candidate_ids or [],
                            "party": c.party,
                            "state": c.state,
                            # Include contact information for offline use
                            "street_address": c.street_address,
                            "street_address_2": c.street_address_2,
                            "city": c.city,
                            "zip": c.zip,
                            "email": c.email,
                            "phone": c.phone,
                            "website": c.website,
                            "treasurer_name": c.treasurer_name
                        }
                        if c.raw_data:
                            committee_dict.update(c.raw_data)
                        result_list.append(committee_dict)
                    return result_list
                return None
        except Exception as e:
            logger.warning(f"Error querying local committees: {e}")
            return None
    
    async def _store_contribution(self, contribution_data: Dict):
        """Store contribution in local database with retry logic for database locks"""
        max_retries = 3
        retry_delay = 0.1
        
        for attempt in range(max_retries):
            try:
                async with self._db_write_semaphore:
                    async with AsyncSessionLocal() as session:
                        # Extract contribution ID - FEC API uses 'sub_id' as unique identifier
                        contrib_id = (
                            contribution_data.get('sub_id') or 
                            contribution_data.get('contribution_id') or
                            contribution_data.get('transaction_id')
                        )
                        
                        if not contrib_id:
                            logger.debug(f"Skipping contribution without ID")
                            return
                        
                        # Check if contribution already exists
                        existing = await session.execute(
                            select(Contribution).where(Contribution.contribution_id == contrib_id)
                        )
                        existing_contrib = existing.scalar_one_or_none()
                        
                        # Extract amount from multiple possible fields
                        amount = 0.0
                        for amt_key in ['contb_receipt_amt', 'contribution_amount', 'contribution_receipt_amount', 'amount', 'contribution_receipt_amt']:
                            amt_val = contribution_data.get(amt_key)
                            if amt_val is not None:
                                try:
                                    amount = float(amt_val)
                                    if amount > 0:
                                        break
                                except (ValueError, TypeError):
                                    continue
                        
                        # Extract contributor name from multiple fields
                        contrib_name = (
                            contribution_data.get('contributor_name') or 
                            contribution_data.get('contributor') or 
                            contribution_data.get('name') or
                            contribution_data.get('contributor_name_1')
                        )
                        
                        # Parse contribution date using centralized utility
                        # This handles multiple date fields and formats (MMDDYYYY, YYYY-MM-DD, ISO, etc.)
                        from app.utils.date_utils import extract_date_from_raw_data
                        contrib_date = extract_date_from_raw_data(contribution_data)
                        
                        # If centralized extraction didn't find a date, try direct field access as fallback
                        if not contrib_date:
                            date_str = (
                                contribution_data.get('contribution_receipt_date') or 
                                contribution_data.get('contribution_date') or 
                                contribution_data.get('receipt_date')
                            )
                            if date_str:
                                try:
                                    # Try parsing various date formats
                                    if isinstance(date_str, str):
                                        if 'T' in date_str:
                                            contrib_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                        else:
                                            contrib_date = datetime.strptime(date_str, '%Y-%m-%d')
                                    else:
                                        contrib_date = date_str
                                except (ValueError, TypeError):
                                    pass
                        
                        if existing_contrib:
                            # Update existing contribution - this handles amendments to previous filings
                            # FEC allows committees to amend filings, which may update contribution details
                            # We always update to ensure we have the most current information
                            
                            # Always update these fields if new data is available (amendments may correct them)
                            if contrib_name:
                                existing_contrib.contributor_name = contrib_name
                            if amount > 0:
                                existing_contrib.contribution_amount = amount
                            if contrib_date:
                                existing_contrib.contribution_date = contrib_date
                            
                            # Update other fields - prefer new data over existing (handles amendments)
                            if contribution_data.get('contributor_city'):
                                existing_contrib.contributor_city = contribution_data.get('contributor_city')
                            if contribution_data.get('contributor_state'):
                                existing_contrib.contributor_state = contribution_data.get('contributor_state')
                            if contribution_data.get('contributor_zip'):
                                existing_contrib.contributor_zip = contribution_data.get('contributor_zip')
                            if contribution_data.get('contributor_employer'):
                                existing_contrib.contributor_employer = contribution_data.get('contributor_employer')
                            if contribution_data.get('contributor_occupation'):
                                existing_contrib.contributor_occupation = contribution_data.get('contributor_occupation')
                            if contribution_data.get('candidate_id'):
                                existing_contrib.candidate_id = contribution_data.get('candidate_id')
                            if contribution_data.get('committee_id'):
                                existing_contrib.committee_id = contribution_data.get('committee_id')
                            if contribution_data.get('contribution_type') or contribution_data.get('transaction_type'):
                                existing_contrib.contribution_type = (
                                    contribution_data.get('contribution_type') or 
                                    contribution_data.get('transaction_type')
                                )
                            
                            # Always update raw_data to preserve the most recent version (may contain amendments)
                            if contribution_data:
                                existing_contrib.raw_data = contribution_data
                            
                            logger.debug(f"Updated existing contribution {contrib_id} (may be an amendment)")
                        else:
                            # Create new contribution
                            contribution = Contribution(
                                contribution_id=contrib_id,
                                candidate_id=contribution_data.get('candidate_id'),
                                committee_id=contribution_data.get('committee_id'),
                                contributor_name=contrib_name,
                                contributor_city=contribution_data.get('contributor_city'),
                                contributor_state=contribution_data.get('contributor_state'),
                                contributor_zip=contribution_data.get('contributor_zip'),
                                contributor_employer=contribution_data.get('contributor_employer'),
                                contributor_occupation=contribution_data.get('contributor_occupation'),
                                contribution_amount=amount,
                                contribution_date=contrib_date,
                                contribution_type=contribution_data.get('contribution_type') or contribution_data.get('transaction_type'),
                                raw_data=contribution_data
                            )
                            session.add(contribution)
                        
                        await session.commit()
                        return  # Success, exit retry loop
                        
            except Exception as e:
                error_str = str(e).lower()
                # Check if it's a database locked error
                if "database is locked" in error_str or "locked" in error_str:
                    if attempt < max_retries - 1:
                        # Exponential backoff
                        wait_time = retry_delay * (2 ** attempt)
                        logger.debug(f"Database locked storing contribution, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.warning(f"Failed to store contribution after {max_retries} attempts due to database lock")
                elif "unique constraint" in error_str or "UNIQUE constraint" in error_str:
                    # Another task already inserted it, that's fine
                    logger.debug(f"Contribution {contrib_id} already exists, skipping")
                    return
                else:
                    if attempt < max_retries - 1:
                        logger.debug(f"Retry {attempt + 1}/{max_retries} for storing contribution: {e}")
                        await asyncio.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                    else:
                        logger.warning(f"Failed to store contribution after {max_retries} attempts: {e}")
    
    async def _store_committee(self, committee_data: Dict):
        """Store committee in local database - handles race conditions with retry logic"""
        max_retries = 3
        retry_delay = 0.1
        
        for attempt in range(max_retries):
            try:
                # Use semaphore to serialize database writes
                async with self._db_write_semaphore:
                    async with AsyncSessionLocal() as session:
                        committee_id = committee_data.get("committee_id")
                        if not committee_id:
                            return
                        
                        result = await session.execute(
                            select(Committee).where(Committee.committee_id == committee_id)
                        )
                        existing = result.scalar_one_or_none()
                        
                        if existing:
                            # Update existing
                            existing.name = committee_data.get("name", "")
                            existing.committee_type = committee_data.get("committee_type")
                            existing.committee_type_full = committee_data.get("committee_type_full")
                            existing.candidate_ids = committee_data.get("candidate_ids") or []
                            existing.party = committee_data.get("party")
                            existing.state = committee_data.get("state")
                            # Update contact info if available
                            contact_info = self._extract_committee_contact_info(committee_data)
                            if contact_info.get("street_address"):
                                existing.street_address = contact_info["street_address"]
                            if contact_info.get("street_address_2"):
                                existing.street_address_2 = contact_info["street_address_2"]
                            if contact_info.get("city"):
                                existing.city = contact_info["city"]
                            if contact_info.get("zip"):
                                existing.zip = contact_info["zip"]
                            if contact_info.get("email"):
                                existing.email = contact_info["email"]
                            if contact_info.get("phone"):
                                existing.phone = contact_info["phone"]
                            if contact_info.get("website"):
                                existing.website = contact_info["website"]
                            if contact_info.get("treasurer_name"):
                                existing.treasurer_name = contact_info["treasurer_name"]
                            existing.raw_data = committee_data
                            existing.updated_at = datetime.utcnow()
                            await session.commit()
                        else:
                            # Create new - handle race condition where another task might have inserted it
                            try:
                                contact_info = self._extract_committee_contact_info(committee_data)
                                committee = Committee(
                                    committee_id=committee_id,
                                    name=committee_data.get("name", ""),
                                    committee_type=committee_data.get("committee_type"),
                                    committee_type_full=committee_data.get("committee_type_full"),
                                    candidate_ids=committee_data.get("candidate_ids") or [],
                                    party=committee_data.get("party"),
                                    state=committee_data.get("state"),
                                    street_address=contact_info.get("street_address"),
                                    street_address_2=contact_info.get("street_address_2"),
                                    city=contact_info.get("city"),
                                    zip=contact_info.get("zip"),
                                    email=contact_info.get("email"),
                                    phone=contact_info.get("phone"),
                                    website=contact_info.get("website"),
                                    treasurer_name=contact_info.get("treasurer_name"),
                                    raw_data=committee_data
                                )
                                session.add(committee)
                                await session.commit()
                            except Exception as insert_error:
                                # If insert fails due to unique constraint, another task inserted it
                                # Try to update it instead
                                await session.rollback()
                                result = await session.execute(
                                    select(Committee).where(Committee.committee_id == committee_id)
                                )
                                existing = result.scalar_one_or_none()
                                if existing:
                                    existing.name = committee_data.get("name", "")
                                    existing.committee_type = committee_data.get("committee_type")
                                    existing.committee_type_full = committee_data.get("committee_type_full")
                                    existing.candidate_ids = committee_data.get("candidate_ids") or []
                                    existing.party = committee_data.get("party")
                                    existing.state = committee_data.get("state")
                                    # Update contact info if available
                                    contact_info = self._extract_committee_contact_info(committee_data)
                                    if contact_info.get("street_address"):
                                        existing.street_address = contact_info["street_address"]
                                    if contact_info.get("street_address_2"):
                                        existing.street_address_2 = contact_info["street_address_2"]
                                    if contact_info.get("city"):
                                        existing.city = contact_info["city"]
                                    if contact_info.get("zip"):
                                        existing.zip = contact_info["zip"]
                                    if contact_info.get("email"):
                                        existing.email = contact_info["email"]
                                    if contact_info.get("phone"):
                                        existing.phone = contact_info["phone"]
                                    if contact_info.get("website"):
                                        existing.website = contact_info["website"]
                                    if contact_info.get("treasurer_name"):
                                        existing.treasurer_name = contact_info["treasurer_name"]
                                    existing.raw_data = committee_data
                                    existing.updated_at = datetime.utcnow()
                                    await session.commit()
                                else:
                                    # Re-raise if it's not a unique constraint error
                                    raise
                        return  # Success, exit retry loop
                        
            except Exception as e:
                error_str = str(e).lower()
                # Check if it's a database locked error
                if "database is locked" in error_str or "locked" in error_str:
                    if attempt < max_retries - 1:
                        # Exponential backoff
                        wait_time = retry_delay * (2 ** attempt)
                        logger.debug(f"Database locked, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.warning(f"Error storing committee after {max_retries} retries: {e}")
                elif "UNIQUE constraint" in error_str:
                    # Unique constraint error is expected in race conditions, try to update instead
                    try:
                        async with self._db_write_semaphore:
                            async with AsyncSessionLocal() as session:
                                committee_id = committee_data.get("committee_id")
                                if not committee_id:
                                    return
                                
                                result = await session.execute(
                                    select(Committee).where(Committee.committee_id == committee_id)
                                )
                                existing = result.scalar_one_or_none()
                                if existing:
                                    existing.name = committee_data.get("name", "")
                                    existing.committee_type = committee_data.get("committee_type")
                                    existing.committee_type_full = committee_data.get("committee_type_full")
                                    existing.candidate_ids = committee_data.get("candidate_ids") or []
                                    existing.party = committee_data.get("party")
                                    existing.state = committee_data.get("state")
                                    existing.raw_data = committee_data
                                    existing.updated_at = datetime.utcnow()
                                    await session.commit()
                                    return
                    except Exception as update_error:
                        logger.debug(f"Error updating committee after unique constraint: {update_error}")
                else:
                    # Not a lock or unique constraint error, don't retry
                    logger.warning(f"Error storing committee: {e}")
                    return
    
    async def get_committees(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        name: Optional[str] = None,
        committee_type: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get committees - queries local DB first, falls back to API"""
        # Try local database first
        if self.bulk_data_enabled:
            try:
                local_data = await self._query_local_committees(
                    candidate_id=candidate_id, committee_id=committee_id, limit=limit
                )
                if local_data and len(local_data) > 0:
                    # Filter by name/type/state if provided
                    filtered = local_data
                    if name:
                        filtered = [c for c in filtered if name.lower() in (c.get('name', '') or '').lower()]
                    if committee_type:
                        filtered = [c for c in filtered if c.get('committee_type') == committee_type]
                    if state:
                        filtered = [c for c in filtered if c.get('state') == state]
                    if filtered and len(filtered) > 0:
                        logger.debug(f"Found {len(filtered)} committees in local database")
                        return filtered[:limit]
            except Exception as e:
                logger.warning(f"Error querying local committees, falling back to API: {e}")
        
        # Fall back to API
        logger.debug(f"Querying FEC API for committees: candidate_id={candidate_id}, committee_id={committee_id}")
        try:
            params = {"per_page": limit}
            if candidate_id:
                params["candidate_id"] = candidate_id
            if committee_id:
                params["committee_id"] = committee_id
            if name:
                params["q"] = name
            if committee_type:
                params["committee_type"] = committee_type
            if state:
                params["state"] = state
            
            data = await self._make_request("committees", params)
            results = data.get("results", [])
            
            # Store results in local DB
            if results and self.bulk_data_enabled:
                for committee in results:
                    asyncio.create_task(self._store_committee(committee))
            
            return results
        except Exception as e:
            logger.error(f"API fallback failed for committees: {e}")
            return []
    
    async def get_committee_totals(
        self,
        committee_id: str,
        cycle: Optional[int] = None
    ) -> List[Dict]:
        """Get committee financial totals"""
        params = {}
        if cycle:
            params["cycle"] = cycle
        
        data = await self._make_request(f"committee/{committee_id}/totals", params)
        return data.get("results", [])
    
    async def get_independent_expenditures(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        support_oppose: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        min_amount: Optional[float] = None,
        max_amount: Optional[float] = None,
        limit: int = 1000,
        two_year_transaction_period: Optional[int] = None
    ) -> List[Dict]:
        """Get independent expenditures/schedules/schedule_e"""
        params = {
            "per_page": 100,
            "sort": "-expenditure_date",
            "_original_limit": limit
        }
        
        # Always add two_year_transaction_period if not provided (must be even year)
        current_year = datetime.now().year
        default_cycle = (current_year // 2) * 2
        if not two_year_transaction_period:
            two_year_transaction_period = default_cycle
        
        params["two_year_transaction_period"] = two_year_transaction_period
        
        if candidate_id:
            params["candidate_id"] = candidate_id
        if committee_id:
            params["committee_id"] = committee_id
        if support_oppose:
            params["support_oppose_indicator"] = support_oppose
        if min_amount:
            params["min_amount"] = min_amount
        if max_amount:
            params["max_amount"] = max_amount
        if min_date:
            params["min_date"] = min_date
        if max_date:
            params["max_date"] = max_date
        
        data = await self._make_request("schedules/schedule_e", params)
        results = data.get("results", [])
        return results[:limit]
    
    def _extract_candidate_contact_info(self, candidate_data: Dict) -> Dict:
        """Extract contact information from candidate API response
        
        FEC API may return contact info in various field names:
        - Direct candidate fields: street_address, city, zip, email, phone, website
        - Principal committee fields: principal_committee_street_1, principal_committee_city, etc.
        - Mailing address fields: mailing_address, mailing_city, mailing_zip
        - Generic fields: street_1, address, telephone, web_site, url
        """
        contact_info = {}
        
        # Street address - try multiple variations
        contact_info["street_address"] = (
            candidate_data.get("street_address") or
            candidate_data.get("principal_committee_street_1") or 
            candidate_data.get("principal_committee_street_address") or
            candidate_data.get("mailing_address") or
            candidate_data.get("street_1") or
            candidate_data.get("address") or
            candidate_data.get("principal_committee_street") or
            candidate_data.get("candidate_street_1")
        )
        
        # City - try multiple variations
        contact_info["city"] = (
            candidate_data.get("city") or
            candidate_data.get("principal_committee_city") or
            candidate_data.get("mailing_city") or
            candidate_data.get("candidate_city")
        )
        
        # State - try multiple variations
        contact_info["state"] = (
            candidate_data.get("state") or
            candidate_data.get("principal_committee_state") or
            candidate_data.get("mailing_state")
        )
        
        # ZIP - try multiple variations
        contact_info["zip"] = (
            candidate_data.get("zip") or
            candidate_data.get("principal_committee_zip") or
            candidate_data.get("mailing_zip") or
            candidate_data.get("zip_code") or
            candidate_data.get("candidate_zip")
        )
        
        # Email - try multiple variations
        contact_info["email"] = (
            candidate_data.get("email") or
            candidate_data.get("principal_committee_email") or
            candidate_data.get("candidate_email") or
            candidate_data.get("e_mail")
        )
        
        # Phone - try multiple variations
        contact_info["phone"] = (
            candidate_data.get("phone") or
            candidate_data.get("principal_committee_phone") or
            candidate_data.get("telephone") or
            candidate_data.get("candidate_phone") or
            candidate_data.get("phone_number")
        )
        
        # Website - try multiple variations
        contact_info["website"] = (
            candidate_data.get("website") or
            candidate_data.get("principal_committee_website") or
            candidate_data.get("web_site") or
            candidate_data.get("url") or
            candidate_data.get("candidate_website") or
            candidate_data.get("web_url")
        )
        
        return contact_info
    
    def _extract_committee_contact_info(self, committee_data: Dict) -> Dict:
        """Extract contact information from committee API response
        
        FEC API committee endpoints return contact info in these fields:
        - street_1, street_2: Address lines
        - city, state, zip: Location
        - email, phone, website: Contact methods
        - treasurer_name: Committee treasurer
        
        Also handles variations like street_address, mailing_address, etc.
        """
        contact_info = {}
        
        # Street address - try multiple variations
        contact_info["street_address"] = (
            committee_data.get("street_1") or
            committee_data.get("street_address") or
            committee_data.get("mailing_address") or
            committee_data.get("address") or
            committee_data.get("street")
        )
        
        # Street address line 2
        contact_info["street_address_2"] = (
            committee_data.get("street_2") or
            committee_data.get("street_address_2") or
            committee_data.get("mailing_address_2")
        )
        
        # City - try multiple variations
        contact_info["city"] = (
            committee_data.get("city") or
            committee_data.get("mailing_city")
        )
        
        # State
        contact_info["state"] = committee_data.get("state")
        
        # ZIP - try multiple variations
        contact_info["zip"] = (
            committee_data.get("zip") or
            committee_data.get("zip_code") or
            committee_data.get("mailing_zip")
        )
        
        # Email - try multiple variations
        contact_info["email"] = (
            committee_data.get("email") or
            committee_data.get("e_mail") or
            committee_data.get("committee_email")
        )
        
        # Phone - try multiple variations
        contact_info["phone"] = (
            committee_data.get("phone") or
            committee_data.get("telephone") or
            committee_data.get("phone_number") or
            committee_data.get("committee_phone")
        )
        
        # Website - try multiple variations
        contact_info["website"] = (
            committee_data.get("website") or
            committee_data.get("web_site") or
            committee_data.get("url") or
            committee_data.get("web_url") or
            committee_data.get("committee_website")
        )
        
        # Treasurer name
        contact_info["treasurer_name"] = committee_data.get("treasurer_name")
        
        # Log what we found for debugging
        found_fields = [k for k, v in contact_info.items() if v]
        if found_fields:
            logger.debug(f"Extracted committee contact fields: {found_fields}")
        
        return contact_info
    
    async def refresh_candidate_contact_info_if_needed(
        self,
        candidate_id: str,
        force_refresh: bool = False
    ) -> bool:
        """Refresh candidate contact info from API if missing or stale"""
        global _contact_info_check_cache
        
        # Check short-term cache first to prevent duplicate API calls (skip if force_refresh)
        now = datetime.utcnow()
        if not force_refresh:
            async with _contact_info_check_cache_lock:
                # Clean up old cache entries
                expired_keys = [
                    k for k, (_, checked_at) in _contact_info_check_cache.items()
                    if (now - checked_at).total_seconds() > _contact_info_check_cache_ttl
                ]
                for k in expired_keys:
                    del _contact_info_check_cache[k]
                
                # Check if we recently checked this candidate
                if candidate_id in _contact_info_check_cache:
                    has_contact_info, checked_at = _contact_info_check_cache[candidate_id]
                    cache_age = (now - checked_at).total_seconds()
                    if cache_age < _contact_info_check_cache_ttl:
                        if has_contact_info:
                            logger.debug(f"Skipping contact info refresh for {candidate_id} (recently checked, has contact info)")
                            return False
                        # If we recently checked and it had no contact info, still skip to avoid spam
                        logger.debug(f"Skipping contact info refresh for {candidate_id} (recently checked, no contact info)")
                        return False
        
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(Candidate).where(Candidate.candidate_id == candidate_id)
                )
                candidate = result.scalar_one_or_none()
                
                if not candidate:
                    # Candidate doesn't exist in database - fetch and create it first
                    logger.info(f"Candidate {candidate_id} not found in database, fetching from API to create record")
                    params = {}
                    data = await self._make_request(f"candidate/{candidate_id}", params, use_cache=False)
                    result_data = data.get("results", [{}])[0] if data.get("results") else None
                    
                    if not result_data:
                        logger.warning(f"Could not fetch candidate {candidate_id} from API")
                        async with _contact_info_check_cache_lock:
                            _contact_info_check_cache[candidate_id] = (False, now)
                        return False
                    
                    # Store candidate in database
                    await self._store_candidate(result_data)
                    
                    # Re-fetch the candidate from database
                    result = await session.execute(
                        select(Candidate).where(Candidate.candidate_id == candidate_id)
                    )
                    candidate = result.scalar_one_or_none()
                    
                    if not candidate:
                        logger.error(f"Failed to create candidate {candidate_id} in database")
                        async with _contact_info_check_cache_lock:
                            _contact_info_check_cache[candidate_id] = (False, now)
                        return False
                    
                    logger.info(f"Created candidate {candidate_id} in database, proceeding with contact info refresh")
                
                # Check if contact info already exists (from bulk import or previous API call)
                # Only refresh if contact info is completely missing
                has_contact_info = any([
                    candidate.street_address,
                    candidate.city,
                    candidate.zip,
                    candidate.email,
                    candidate.phone,
                    candidate.website
                ])
                
                # If contact info exists, don't refresh (even if stale) unless force_refresh is True
                # This preserves contact info from bulk import or previous API calls
                if has_contact_info and not force_refresh:
                    # Ensure updated_at is set for tracking purposes
                    if not candidate.updated_at:
                        candidate.updated_at = datetime.utcnow()
                        await session.commit()
                    # Cache the positive result
                    async with _contact_info_check_cache_lock:
                        _contact_info_check_cache[candidate_id] = (True, now)
                    return False  # Already have contact info, don't refresh
                
                # No contact info - need to fetch from API
                # Cache that we're checking (prevents concurrent checks)
                async with _contact_info_check_cache_lock:
                    _contact_info_check_cache[candidate_id] = (False, now)
                
                logger.info(f"Fetching candidate {candidate_id} from FEC API to refresh contact info (force_refresh={force_refresh})")
                # Fetch from API
                params = {}
                data = await self._make_request(f"candidate/{candidate_id}", params, use_cache=False)
                result_data = data.get("results", [{}])[0] if data.get("results") else None
                
                if not result_data:
                    logger.warning(f"No candidate data returned from API for {candidate_id}")
                    return False
                
                # Log all keys to help debug what fields are available
                all_keys = list(result_data.keys())
                logger.debug(f"Received candidate data from API for {candidate_id}, total keys: {len(all_keys)}")
                logger.debug(f"API response keys (first 50): {all_keys[:50]}")
                
                # Log any contact-related keys found
                contact_related_keys = [k for k in all_keys if any(term in k.lower() for term in 
                    ['street', 'address', 'city', 'zip', 'email', 'phone', 'website', 'mailing', 'telephone', 'web'])]
                if contact_related_keys:
                    logger.debug(f"Contact-related keys in API response: {contact_related_keys}")
                    # Log values for these keys
                    for key in contact_related_keys[:10]:  # Log first 10
                        value = result_data.get(key)
                        if value:
                            logger.debug(f"  {key} = {str(value)[:100]}")
                
                # Extract and update contact info from candidate response
                contact_info = self._extract_candidate_contact_info(result_data)
                logger.debug(f"Extracted contact info from candidate response for {candidate_id}: {contact_info}")
                
                # If no contact info found in candidate response, fetch from committees directly
                if not any([
                    contact_info.get("street_address"),
                    contact_info.get("city"),
                    contact_info.get("zip"),
                    contact_info.get("email"),
                    contact_info.get("phone"),
                    contact_info.get("website")
                ]):
                    logger.info(f"No contact info in candidate response for {candidate_id}, fetching from committees")
                    try:
                        # Fetch all committees for this candidate directly
                        all_committees = await self.get_committees(candidate_id=candidate_id, limit=100)
                        if all_committees:
                            logger.info(f"Found {len(all_committees)} committees for candidate {candidate_id}, checking for contact info")
                            
                            # Try each committee until we find one with contact info
                            # First try the list response, then fetch individual committee details if needed
                            for committee_data in all_committees:
                                committee_id = committee_data.get("committee_id")
                                if not committee_id:
                                    continue
                                
                                # Extract from list response first
                                committee_contact = self._extract_committee_contact_info(committee_data)
                                logger.debug(f"Committee {committee_id} contact info from list: {committee_contact}")
                                
                                # If list response doesn't have contact info, fetch individual committee details
                                if not any([
                                    committee_contact.get("street_address"),
                                    committee_contact.get("city"),
                                    committee_contact.get("zip"),
                                    committee_contact.get("email"),
                                    committee_contact.get("phone"),
                                    committee_contact.get("website")
                                ]):
                                    logger.debug(f"Committee {committee_id} list response has no contact info, fetching individual details")
                                    try:
                                        # Fetch individual committee details which may have more complete contact info
                                        params = {}
                                        committee_detail_data = await self._make_request(f"committee/{committee_id}", params, use_cache=False)
                                        committee_detail = committee_detail_data.get("results", [{}])[0] if committee_detail_data.get("results") else None
                                        if committee_detail:
                                            committee_contact = self._extract_committee_contact_info(committee_detail)
                                            logger.debug(f"Committee {committee_id} contact info from detail: {committee_contact}")
                                    except Exception as detail_error:
                                        logger.warning(f"Error fetching committee {committee_id} details: {detail_error}")
                                        # Continue with list response data
                                
                                # Use committee contact info if available
                                if any([
                                    committee_contact.get("street_address"),
                                    committee_contact.get("city"),
                                    committee_contact.get("zip"),
                                    committee_contact.get("email"),
                                    committee_contact.get("phone"),
                                    committee_contact.get("website")
                                ]):
                                    contact_info = {
                                        "street_address": committee_contact.get("street_address"),
                                        "city": committee_contact.get("city"),
                                        "zip": committee_contact.get("zip"),
                                        "email": committee_contact.get("email"),
                                        "phone": committee_contact.get("phone"),
                                        "website": committee_contact.get("website")
                                    }
                                    logger.info(f"Found contact info from committee {committee_id} for candidate {candidate_id}")
                                    break  # Found contact info, stop searching
                            else:
                                logger.warning(f"No contact info found in any of the {len(all_committees)} committees for candidate {candidate_id}")
                        else:
                            logger.warning(f"No committees found for candidate {candidate_id}")
                    except Exception as e:
                        logger.error(f"Error fetching committees contact info for candidate {candidate_id}: {e}", exc_info=True)
                
                # Update candidate with contact info
                candidate.street_address = contact_info.get("street_address")
                candidate.city = contact_info.get("city")
                candidate.zip = contact_info.get("zip")
                candidate.email = contact_info.get("email")
                candidate.phone = contact_info.get("phone")
                candidate.website = contact_info.get("website")
                candidate.updated_at = datetime.utcnow()
                
                # Log what we're saving
                saved_fields = []
                if candidate.street_address:
                    saved_fields.append(f"address={candidate.street_address[:50]}")
                if candidate.city:
                    saved_fields.append(f"city={candidate.city}")
                if candidate.zip:
                    saved_fields.append(f"zip={candidate.zip}")
                if candidate.email:
                    saved_fields.append(f"email={candidate.email}")
                if candidate.phone:
                    saved_fields.append(f"phone={candidate.phone}")
                if candidate.website:
                    saved_fields.append(f"website={candidate.website}")
                
                if saved_fields:
                    logger.info(f"Saving contact info to database for candidate {candidate_id}: {', '.join(saved_fields)}")
                else:
                    logger.warning(f"No contact info to save for candidate {candidate_id} after extraction")
                
                await session.commit()
                
                # Update cache with result
                has_contact_after_refresh = any([
                    candidate.street_address,
                    candidate.city,
                    candidate.zip,
                    candidate.email,
                    candidate.phone,
                    candidate.website
                ])
                async with _contact_info_check_cache_lock:
                    _contact_info_check_cache[candidate_id] = (has_contact_after_refresh, now)
                
                if has_contact_after_refresh:
                    logger.info(
                        f"Successfully refreshed contact info for candidate {candidate_id}: "
                        f"address={bool(candidate.street_address)}, city={bool(candidate.city)}, "
                        f"zip={bool(candidate.zip)}, email={bool(candidate.email)}, "
                        f"phone={bool(candidate.phone)}, website={bool(candidate.website)}"
                    )
                else:
                    logger.warning(f"No contact info found for candidate {candidate_id} after checking candidate API and all committees")
                return True
                
        except Exception as e:
            logger.warning(f"Error refreshing contact info for candidate {candidate_id}: {e}")
            return False
    
    async def refresh_committee_contact_info_if_needed(
        self,
        committee_id: str,
        stale_threshold_days: int = 14  # Cache for 2 weeks
    ) -> bool:
        """Refresh committee contact info from API if missing or stale"""
        global _contact_info_check_cache
        
        # Check short-term cache first to prevent duplicate API calls
        # Use same cache but with "committee:" prefix to avoid conflicts
        cache_key = f"committee:{committee_id}"
        now = datetime.utcnow()
        async with _contact_info_check_cache_lock:
            # Clean up old cache entries
            expired_keys = [
                k for k, (_, checked_at) in _contact_info_check_cache.items()
                if (now - checked_at).total_seconds() > _contact_info_check_cache_ttl
            ]
            for k in expired_keys:
                del _contact_info_check_cache[k]
            
            # Check if we recently checked this committee
            if cache_key in _contact_info_check_cache:
                has_contact_info, checked_at = _contact_info_check_cache[cache_key]
                cache_age = (now - checked_at).total_seconds()
                if cache_age < _contact_info_check_cache_ttl:
                    if has_contact_info:
                        logger.debug(f"Skipping contact info refresh for committee {committee_id} (recently checked, has contact info)")
                        return False
                    # If we recently checked and it had no contact info, still skip to avoid spam
                    logger.debug(f"Skipping contact info refresh for committee {committee_id} (recently checked, no contact info)")
                    return False
        
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(Committee).where(Committee.committee_id == committee_id)
                )
                committee = result.scalar_one_or_none()
                
                if not committee:
                    # Cache the negative result
                    async with _contact_info_check_cache_lock:
                        _contact_info_check_cache[cache_key] = (False, now)
                    return False
                
                # Check if contact info already exists (from bulk import or previous API call)
                # Only refresh if contact info is completely missing
                has_contact_info = any([
                    committee.street_address,
                    committee.city,
                    committee.zip,
                    committee.email,
                    committee.phone,
                    committee.website
                ])
                
                # If contact info exists, don't refresh (even if stale)
                # This preserves contact info from bulk import or previous API calls
                if has_contact_info:
                    # Ensure updated_at is set for tracking purposes
                    if not committee.updated_at:
                        committee.updated_at = datetime.utcnow()
                        await session.commit()
                    # Cache the positive result
                    async with _contact_info_check_cache_lock:
                        _contact_info_check_cache[cache_key] = (True, now)
                    return False  # Already have contact info, don't refresh
                
                # No contact info - need to fetch from API
                # Cache that we're checking (prevents concurrent checks)
                async with _contact_info_check_cache_lock:
                    _contact_info_check_cache[cache_key] = (False, now)
                
                # Fetch from API
                params = {}
                data = await self._make_request(f"committee/{committee_id}", params, use_cache=False)
                result_data = data.get("results", [{}])[0] if data.get("results") else None
                
                if not result_data:
                    return False
                
                # Extract and update contact info
                contact_info = self._extract_committee_contact_info(result_data)
                
                committee.street_address = contact_info.get("street_address")
                committee.street_address_2 = contact_info.get("street_address_2")
                committee.city = contact_info.get("city")
                committee.zip = contact_info.get("zip")
                committee.email = contact_info.get("email")
                committee.phone = contact_info.get("phone")
                committee.website = contact_info.get("website")
                committee.treasurer_name = contact_info.get("treasurer_name")
                committee.updated_at = datetime.utcnow()
                
                await session.commit()
                
                # Update cache with result
                has_contact_after_refresh = any([
                    committee.street_address,
                    committee.city,
                    committee.zip,
                    committee.email,
                    committee.phone,
                    committee.website
                ])
                async with _contact_info_check_cache_lock:
                    _contact_info_check_cache[cache_key] = (has_contact_after_refresh, now)
                
                logger.info(f"Refreshed contact info for committee {committee_id}")
                return True
                
        except Exception as e:
            logger.warning(f"Error refreshing contact info for committee {committee_id}: {e}")
            return False
    
    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()

