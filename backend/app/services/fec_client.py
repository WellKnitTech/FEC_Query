import httpx
import asyncio
import os
import logging
from typing import Dict, List, Optional, Any
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

class FECClient:
    """Client for interacting with OpenFEC API with caching and rate limiting"""
    
    def __init__(self):
        self.api_key = get_fec_api_key()
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
    
    async def _save_to_cache(self, cache_key: str, data: Dict, ttl_hours: int = 24):
        """Save response to cache"""
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
                return cached_data
        
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
                            "active_through": c.active_through
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
                            existing.raw_data = candidate_data
                            existing.updated_at = datetime.utcnow()
                        else:
                            # Create new
                            candidate = Candidate(
                                candidate_id=candidate_id,
                                name=candidate_data.get("name") or candidate_data.get("candidate_name", ""),
                                office=candidate_data.get("office"),
                                party=candidate_data.get("party"),
                                state=candidate_data.get("state"),
                                district=candidate_data.get("district"),
                                election_years=candidate_data.get("election_years"),
                                active_through=candidate_data.get("active_through"),
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
            local_data = await self._query_local_candidates(
                name=name, office=office, state=state, party=party, year=year, district=district, limit=limit
            )
            if local_data and len(local_data) > 0:
                return local_data
        
        # Fall back to API
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
        if self.bulk_data_enabled:
            for candidate in results:
                asyncio.create_task(self._store_candidate(candidate))
        
        return results
    
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
                        "active_through": candidate.active_through
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
            local_data = await self._query_local_candidate(candidate_id)
            if local_data:
                return local_data
        
        # Fall back to API
        params = {}
        data = await self._make_request(f"candidate/{candidate_id}", params)
        result = data.get("results", [{}])[0] if data.get("results") else None
        
        # Store in local DB
        if result and self.bulk_data_enabled:
            asyncio.create_task(self._store_candidate(result))
        
        return result
    
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
                            "party_contributions": f.party_contributions
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
            local_data = await self._query_local_financial_totals(candidate_id, cycle)
            if local_data:
                return local_data
        
        # Fall back to API
        params = {}
        if cycle:
            params["cycle"] = cycle
        
        data = await self._make_request(f"candidate/{candidate_id}/totals", params)
        results = data.get("results", [])
        
        # Store results in local DB
        if self.bulk_data_enabled:
            for financial in results:
                asyncio.create_task(self._store_financial_total(candidate_id, financial))
        
        return results
    
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
                # Build query
                query = select(Contribution)
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
                        candidate_id = c.candidate_id
                        if not candidate_id and c.raw_data and isinstance(c.raw_data, dict):
                            candidate_id = c.raw_data.get('CAND_ID') or c.raw_data.get('candidate_id')
                        
                        contrib_dict = {
                            "sub_id": c.contribution_id,
                            "contribution_id": c.contribution_id,
                            "candidate_id": candidate_id,
                            "committee_id": c.committee_id,
                            "contributor_name": c.contributor_name,
                            "contributor_city": c.contributor_city,
                            "contributor_state": c.contributor_state,
                            "contributor_zip": c.contributor_zip,
                            "contributor_employer": c.contributor_employer,
                            "contributor_occupation": c.contributor_occupation,
                            "contribution_amount": amount,
                            "contribution_receipt_date": c.contribution_date.strftime("%Y-%m-%d") if c.contribution_date else None,
                            "contribution_date": c.contribution_date.strftime("%Y-%m-%d") if c.contribution_date else None,
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
        two_year_transaction_period: Optional[int] = None
    ) -> List[Dict]:
        """Get contributions/schedules/schedule_a - queries local DB first, falls back to API"""
        # Try local database first if bulk data is enabled
        if self.bulk_data_enabled:
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
            )
            if local_data:
                return local_data
        
        # Fall back to API
        # If only candidate_id is provided, we need to get committees first
        # or provide two_year_transaction_period
        if candidate_id and not committee_id and not two_year_transaction_period:
            # Get committees for the candidate to use committee_id filter
            committees = await self.get_committees(candidate_id=candidate_id, limit=100)
            if committees:
                # Query contributions for each committee and combine
                all_contributions = []
                # Get current election cycle for two_year_transaction_period (must be even year)
                current_year = datetime.now().year
                # Round down to nearest even year
                default_cycle = (current_year // 2) * 2
                
                for committee in committees[:10]:  # Limit to first 10 committees to avoid too many requests
                    comm_id = committee.get('committee_id')
                    if comm_id:
                        params = {
                            "per_page": 100,  # FEC API max is 100
                            "sort": "-contribution_receipt_date",
                            "committee_id": comm_id,
                            "two_year_transaction_period": two_year_transaction_period or default_cycle
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
                            data = await self._make_request("schedules/schedule_a", params)
                            all_contributions.extend(data.get("results", []))
                            if len(all_contributions) >= limit:
                                break
                        except Exception:
                            continue  # Skip if this committee fails
                
                return all_contributions[:limit]
            else:
                # No committees found, try with two_year_transaction_period
                # Use current election cycle (must be even year)
                current_year = datetime.now().year
                # Round down to nearest even year
                two_year_transaction_period = (current_year // 2) * 2
        
        params = {
            "per_page": 100,  # FEC API max is 100, pagination will handle more
            "sort": "-contribution_receipt_date",
            "_original_limit": limit  # Store original limit for pagination
        }
        
        # Always add two_year_transaction_period if not provided (must be even year)
        current_year = datetime.now().year
        # Round down to nearest even year
        default_cycle = (current_year // 2) * 2
        if not two_year_transaction_period:
            two_year_transaction_period = default_cycle
        
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
        
        data = await self._make_request("schedules/schedule_a", params)
        results = data.get("results", [])
        # Limit results to requested limit
        return results[:limit]
    
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
        """Get expenditures/schedules/schedule_b"""
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
                            "state": c.state
                        }
                        if c.raw_data:
                            committee_dict.update(c.raw_data)
                        result_list.append(committee_dict)
                    return result_list
                return None
        except Exception as e:
            logger.warning(f"Error querying local committees: {e}")
            return None
    
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
                            existing.raw_data = committee_data
                            existing.updated_at = datetime.utcnow()
                            await session.commit()
                        else:
                            # Create new - handle race condition where another task might have inserted it
                            try:
                                committee = Committee(
                                    committee_id=committee_id,
                                    name=committee_data.get("name", ""),
                                    committee_type=committee_data.get("committee_type"),
                                    committee_type_full=committee_data.get("committee_type_full"),
                                    candidate_ids=committee_data.get("candidate_ids") or [],
                                    party=committee_data.get("party"),
                                    state=committee_data.get("state"),
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
            local_data = await self._query_local_committees(
                candidate_id=candidate_id, committee_id=committee_id, limit=limit
            )
            if local_data:
                # Filter by name/type/state if provided
                filtered = local_data
                if name:
                    filtered = [c for c in filtered if name.lower() in (c.get('name', '') or '').lower()]
                if committee_type:
                    filtered = [c for c in filtered if c.get('committee_type') == committee_type]
                if state:
                    filtered = [c for c in filtered if c.get('state') == state]
                if filtered:
                    return filtered[:limit]
        
        # Fall back to API
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
        if self.bulk_data_enabled:
            for committee in results:
                asyncio.create_task(self._store_committee(committee))
        
        return results
    
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
    
    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()

