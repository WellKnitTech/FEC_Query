"""
Security utilities for rate limiting and resource management.
"""
from slowapi.util import get_remote_address
from functools import wraps
import logging
import os
from typing import Callable, Any
from fastapi import Request, HTTPException

logger = logging.getLogger(__name__)

# Rate limit configurations
# Read operations: more permissive
READ_RATE_LIMIT = "100/minute"
# Write operations: more restrictive
WRITE_RATE_LIMIT = "30/minute"
# Expensive operations: very restrictive
EXPENSIVE_RATE_LIMIT = "10/minute"
# Bulk operations: most restrictive
BULK_RATE_LIMIT = "5/minute"

# Resource limits
MAX_CONCURRENT_JOBS = int(os.getenv("MAX_CONCURRENT_JOBS", "3"))
MAX_REQUEST_SIZE_MB = int(os.getenv("MAX_REQUEST_SIZE_MB", "50"))
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "1000"))

# Track concurrent operations
_concurrent_operations = {"bulk_imports": 0, "exports": 0}

def check_resource_limits(operation_type: str = "bulk_imports") -> bool:
    """
    Check if resource limits are exceeded.
    
    Args:
        operation_type: Type of operation to check
        
    Returns:
        True if within limits, False otherwise
    """
    if operation_type == "bulk_imports":
        current = _concurrent_operations.get("bulk_imports", 0)
        if current >= MAX_CONCURRENT_JOBS:
            logger.warning(f"Resource limit exceeded: {current} concurrent bulk imports (max: {MAX_CONCURRENT_JOBS})")
            return False
    elif operation_type == "exports":
        current = _concurrent_operations.get("exports", 0)
        if current >= MAX_CONCURRENT_JOBS:
            logger.warning(f"Resource limit exceeded: {current} concurrent exports (max: {MAX_CONCURRENT_JOBS})")
            return False
    return True

def increment_operation(operation_type: str = "bulk_imports"):
    """Increment concurrent operation counter."""
    _concurrent_operations[operation_type] = _concurrent_operations.get(operation_type, 0) + 1
    logger.debug(f"Incremented {operation_type}: {_concurrent_operations[operation_type]}")

def decrement_operation(operation_type: str = "bulk_imports"):
    """Decrement concurrent operation counter."""
    _concurrent_operations[operation_type] = max(0, _concurrent_operations.get(operation_type, 1) - 1)
    logger.debug(f"Decremented {operation_type}: {_concurrent_operations[operation_type]}")

def log_security_event(event_type: str, details: dict, request: Request = None):
    """
    Log security-related events.
    
    Args:
        event_type: Type of event (rate_limit, resource_limit, etc.)
        details: Event details
        request: Optional request object for additional context
    """
    log_data = {
        "event_type": event_type,
        **details
    }
    if request:
        log_data.update({
            "client_ip": get_remote_address(request),
            "path": request.url.path,
            "method": request.method
        })
    logger.warning(f"Security event: {log_data}")

