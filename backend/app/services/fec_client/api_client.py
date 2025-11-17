"""
HTTP client for FEC API requests
"""
import asyncio
import httpx
import logging
from typing import Dict, Any, Optional
from app.services.shared.exceptions import FECAPIError, RateLimitError
from .rate_limiter import RateLimiter
from .cache import CacheManager

logger = logging.getLogger(__name__)


class APIClient:
    """Handles HTTP requests to FEC API"""
    
    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        rate_limiter: Optional[RateLimiter] = None,
        cache_manager: Optional[CacheManager] = None,
        timeout: float = 30.0
    ):
        """
        Initialize API client
        
        Args:
            base_url: Base URL for FEC API
            api_key: Optional API key (can be set later)
            rate_limiter: Optional rate limiter instance
            cache_manager: Optional cache manager instance
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.api_key = api_key
        self._api_key_pending = api_key is None
        self.rate_limiter = rate_limiter or RateLimiter()
        self.cache_manager = cache_manager or CacheManager()
        self.client = httpx.AsyncClient(timeout=timeout)
        
        # Request deduplication - track in-flight requests
        self._in_flight_requests: Dict[str, asyncio.Task] = {}
    
    async def ensure_api_key(self, get_api_key_func):
        """Ensure API key is loaded (for async initialization)"""
        if self.api_key is None or self._api_key_pending:
            self.api_key = await get_api_key_func()
            self._api_key_pending = False
    
    async def make_request(
        self,
        endpoint: str,
        params: Dict[str, Any],
        get_api_key_func,
        use_cache: bool = True,
        cache_ttl: Optional[int] = None,
        max_retries: int = 3,
        data_type: Optional[str] = None
    ) -> Dict:
        """
        Make API request with caching, rate limit handling, and request deduplication
        
        Args:
            endpoint: API endpoint path
            params: Request parameters
            get_api_key_func: Async function to get API key
            use_cache: Whether to use cache
            cache_ttl: Optional cache TTL override
            max_retries: Maximum retry attempts
            data_type: Optional data type for logging
        """
        # Ensure API key is loaded
        await self.ensure_api_key(get_api_key_func)
        
        # Add API key to params
        params = params.copy()
        params["api_key"] = self.api_key
        
        # FEC API requires per_page to be between 1 and 100
        original_limit = params.pop("_original_limit", None)
        requested_per_page = params.get("per_page", 100)
        params["per_page"] = min(max(1, requested_per_page), 100)
        if original_limit:
            params["_original_limit"] = original_limit
        
        # Determine cache TTL
        if cache_ttl is None:
            cache_ttl = self.cache_manager.get_cache_ttl(endpoint)
        
        # Check cache first
        cache_key = None
        cached_data = None
        if use_cache:
            cache_key = self.cache_manager.generate_cache_key(endpoint, params)
            cached_data = await self.cache_manager.get_from_cache(cache_key)
            
            # Request deduplication - check if same request is in flight
            if cache_key in self._in_flight_requests:
                try:
                    logger.debug(f"Waiting for in-flight request: {cache_key}")
                    return await self._in_flight_requests[cache_key]
                except Exception as e:
                    logger.warning(f"In-flight request failed: {e}")
                    self._in_flight_requests.pop(cache_key, None)
            
            # Stale-while-revalidate: return cached data immediately if available
            if cached_data:
                # Check if cache is stale but not expired (for background refresh)
                is_stale = await self.cache_manager.check_cache_staleness(cache_key, cache_ttl)
                if is_stale:
                    # Trigger background refresh (don't wait)
                    asyncio.create_task(self._refresh_cache_in_background(
                        endpoint, params, cache_key, cache_ttl, get_api_key_func
                    ))
                return cached_data
        
        # Create request task for deduplication
        async def _do_request():
            # Use semaphore to limit concurrent requests
            async with self.rate_limiter.get_semaphore():
                # Wait for rate limit
                await self.rate_limiter.wait_for_rate_limit()
                
                # Make request with retry logic for rate limits
                url = f"{self.base_url}/{endpoint}"
                last_exception = None
                
                for attempt in range(max_retries):
                    try:
                        response = await self.client.get(url, params=params)
                        
                        # Handle rate limit (429) with retry
                        if response.status_code == 429:
                            if attempt < max_retries - 1:
                                wait_time = self.rate_limiter.get_retry_delay(attempt)
                                await asyncio.sleep(wait_time)
                                continue
                            else:
                                # Last attempt failed, try to return cached data if available
                                if cache_key:
                                    cached_data = await self.cache_manager.get_from_cache(cache_key)
                                    if cached_data:
                                        return cached_data
                                
                                error_detail = "Rate limit exceeded. Please try again later."
                                try:
                                    error_data = response.json()
                                    if isinstance(error_data, dict):
                                        if "message" in error_data:
                                            error_detail = error_data["message"]
                                        elif "error" in error_data:
                                            error_detail = error_data["error"]
                                except Exception:
                                    pass
                                raise RateLimitError(error_detail)
                        
                        response.raise_for_status()
                        data = response.json()
                        
                        # Handle pagination if needed
                        max_results = params.get("_original_limit")
                        if "pagination" in data and data["pagination"].get("pages", 0) > 1:
                            data = await self._handle_pagination(endpoint, params.copy(), data, max_results)
                        
                        # Save to cache on success
                        if use_cache and cache_key:
                            await self.cache_manager.save_to_cache(cache_key, data, cache_ttl)
                        
                        return data
                        
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 429:
                            last_exception = e
                            if attempt < max_retries - 1:
                                wait_time = self.rate_limiter.get_retry_delay(attempt)
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
                            except Exception:
                                pass
                            raise FECAPIError(error_detail, status_code=e.response.status_code) from e
                    except httpx.RequestError as e:
                        raise FECAPIError(f"Failed to connect to FEC API: {str(e)}") from e
                
                # All retries exhausted
                if last_exception:
                    # Try to return cached data as fallback
                    if cache_key:
                        cached_data = await self.cache_manager.get_from_cache(cache_key)
                        if cached_data:
                            return cached_data
                    
                    error_detail = "Rate limit exceeded. Please try again later."
                    try:
                        if hasattr(last_exception, 'response') and last_exception.response:
                            error_data = last_exception.response.json()
                            if isinstance(error_data, dict) and "message" in error_data:
                                error_detail = error_data["message"]
                    except Exception:
                        pass
                    raise RateLimitError(error_detail)
                
                raise FECAPIError("Unexpected error in API request")
        
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
        cache_ttl: int,
        get_api_key_func
    ):
        """Refresh cache in background without blocking"""
        try:
            # Remove API key from params for cache key generation
            params_copy = {k: v for k, v in params.items() if k != "api_key"}
            cache_key_check = self.cache_manager.generate_cache_key(endpoint, params_copy)
            
            # Only refresh if not already in flight
            if cache_key_check not in self._in_flight_requests:
                logger.debug(f"Background refresh for {endpoint}")
                # Make request without waiting
                asyncio.create_task(self.make_request(
                    endpoint, params_copy, get_api_key_func,
                    use_cache=True, cache_ttl=cache_ttl
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
            await self.rate_limiter.wait_for_rate_limit()
            
            async with self.rate_limiter.get_semaphore():
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
                    logger.debug(f"Pagination error on page {page}: {e}")
                    break
        
        initial_data["results"] = all_results
        return initial_data
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()

