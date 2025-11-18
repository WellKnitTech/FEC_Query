"""
Standardized API exception classes
"""
from typing import Optional, Dict, Any


class APIError(Exception):
    """Base API exception"""
    def __init__(
        self, 
        message: str, 
        status_code: int = 500,
        code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.code = code or self.__class__.__name__
        self.details = details or {}


class ValidationError(APIError):
    """Input validation errors (400)"""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, status_code=400, code="VALIDATION_ERROR", details=details)


class NotFoundError(APIError):
    """Resource not found (404)"""
    def __init__(self, message: str = "Resource not found", details: Optional[Dict[str, Any]] = None):
        super().__init__(message, status_code=404, code="NOT_FOUND", details=details)


class ServiceUnavailableError(APIError):
    """Service errors (503)"""
    def __init__(self, message: str = "Service unavailable", details: Optional[Dict[str, Any]] = None):
        super().__init__(message, status_code=503, code="SERVICE_UNAVAILABLE", details=details)

