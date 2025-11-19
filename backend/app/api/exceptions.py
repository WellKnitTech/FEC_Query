"""
Standardized API exception classes with error classification
"""
from typing import Optional, Dict, Any
import uuid


class APIError(Exception):
    """Base API exception with error classification"""
    def __init__(
        self, 
        message: str, 
        status_code: int = 500,
        code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        is_transient: bool = False,
        request_id: Optional[str] = None
    ):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.code = code or self.__class__.__name__
        self.details = details or {}
        self.is_transient = is_transient  # True if error is temporary and retry might help
        self.request_id = request_id or str(uuid.uuid4())[:8]  # Short request ID for tracking
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for JSON response"""
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
            "request_id": self.request_id,
            "is_transient": self.is_transient
        }


class ValidationError(APIError):
    """Input validation errors (400) - permanent error"""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None, request_id: Optional[str] = None):
        super().__init__(
            message, 
            status_code=400, 
            code="VALIDATION_ERROR", 
            details=details,
            is_transient=False,
            request_id=request_id
        )


class NotFoundError(APIError):
    """Resource not found (404) - permanent error"""
    def __init__(self, message: str = "Resource not found", details: Optional[Dict[str, Any]] = None, request_id: Optional[str] = None):
        super().__init__(
            message, 
            status_code=404, 
            code="NOT_FOUND", 
            details=details,
            is_transient=False,
            request_id=request_id
        )


class ServiceUnavailableError(APIError):
    """Service errors (503) - transient error"""
    def __init__(self, message: str = "Service unavailable", details: Optional[Dict[str, Any]] = None, request_id: Optional[str] = None):
        super().__init__(
            message, 
            status_code=503, 
            code="SERVICE_UNAVAILABLE", 
            details=details,
            is_transient=True,  # Service unavailable is typically transient
            request_id=request_id
        )


class DatabaseError(APIError):
    """Database operation errors - may be transient"""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None, is_transient: bool = True, request_id: Optional[str] = None):
        super().__init__(
            message,
            status_code=503,
            code="DATABASE_ERROR",
            details=details,
            is_transient=is_transient,
            request_id=request_id
        )


class TimeoutError(APIError):
    """Timeout errors - transient"""
    def __init__(self, message: str = "Request timeout", details: Optional[Dict[str, Any]] = None, request_id: Optional[str] = None):
        super().__init__(
            message,
            status_code=504,
            code="TIMEOUT_ERROR",
            details=details,
            is_transient=True,
            request_id=request_id
        )

