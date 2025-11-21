"""
Custom exceptions for FEC services
"""
from typing import Optional


class FECServiceError(Exception):
    """Base exception for all FEC service errors"""
    pass


class FECAPIError(FECServiceError):
    """Error from FEC API requests"""
    def __init__(self, message: str, status_code: Optional[int] = None, response_data: Optional[dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data


class RateLimitError(FECAPIError):
    """Rate limit exceeded from FEC API"""
    def __init__(self, message: str = "Rate limit exceeded", retry_after: Optional[int] = None):
        super().__init__(message, status_code=429)
        self.retry_after = retry_after


class DatabaseLockError(FECServiceError):
    """Database is locked error"""
    def __init__(self, message: str = "Database is locked", retry_after: Optional[float] = None):
        super().__init__(message)
        self.retry_after = retry_after


class BulkDataError(FECServiceError):
    """Error in bulk data operations"""
    def __init__(self, message: str, cycle: Optional[int] = None, data_type: Optional[str] = None):
        super().__init__(message)
        self.cycle = cycle
        self.data_type = data_type


class DonorSearchError(FECServiceError):
    """Error in donor search operations"""
    def __init__(self, message: str, search_term: Optional[str] = None):
        super().__init__(message)
        self.search_term = search_term


class QueryTimeoutError(DonorSearchError):
    """Query execution timed out"""
    def __init__(self, message: str = "Query execution timed out", search_term: Optional[str] = None, timeout: Optional[float] = None):
        super().__init__(message, search_term=search_term)
        self.timeout = timeout


class FTS5UnavailableError(DonorSearchError):
    """FTS5 full-text search is not available"""
    def __init__(self, message: str = "FTS5 full-text search is not available", search_term: Optional[str] = None):
        super().__init__(message, search_term=search_term)

