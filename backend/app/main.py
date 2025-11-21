from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from app.api.routes import candidates, contributions, analysis, fraud, bulk_data, export, independent_expenditures, committees, saved_searches, trends, settings
from app.api.exceptions import APIError, ValidationError, NotFoundError, ServiceUnavailableError
from app.services.shared.exceptions import FECServiceError, FECAPIError, RateLimitError, DatabaseLockError, BulkDataError
from app.db.database import init_db
from app.services.bulk_data import _running_tasks
from app.lifecycle import setup_startup_tasks, setup_shutdown_handlers
import os
import logging
import asyncio
from dotenv import load_dotenv
from starlette.middleware.base import BaseHTTPMiddleware

from app.config import config
from app.utils.structured_logging import setup_structured_logging
import uuid

# Configure structured logging
# Use JSON format in production (when LOG_JSON=true), human-readable in development
use_json_logging = os.getenv("LOG_JSON", "false").lower() == "true"
setup_structured_logging(
    level=config.LOG_LEVEL,
    use_json=use_json_logging,
    include_console=True,
    log_dir=config.LOG_DIR,
    log_to_file=config.LOG_TO_FILE,
    max_bytes=config.LOG_FILE_MAX_BYTES,
    backup_count=config.LOG_FILE_BACKUP_COUNT
)

logger = logging.getLogger(__name__)

# Apply filter to suppress CancelledError in SQLAlchemy pool during shutdown
# This filter is applied to the root logger to catch all SQLAlchemy pool errors
class SuppressCancelledErrorFilter(logging.Filter):
    def filter(self, record):
        # Suppress CancelledError exceptions in connection pool cleanup during shutdown
        # These are expected when tasks are cancelled and connections are being closed
        try:
            if record.exc_info:
                exc_type = record.exc_info[0]
                exc_value = record.exc_info[1]
                # Check if it's a CancelledError
                if exc_type and issubclass(exc_type, asyncio.CancelledError):
                    # Suppress all CancelledError in pool operations (connection cleanup during shutdown)
                    return False  # Suppress this log
                # Also check the exception value directly
                if exc_value and isinstance(exc_value, asyncio.CancelledError):
                    return False  # Suppress this log
        except Exception:
            # If we can't check, allow the log through
            pass
        return True  # Allow other logs

# Apply filter to SQLAlchemy pool logger
sqlalchemy_pool_logger = logging.getLogger("sqlalchemy.pool")
sqlalchemy_pool_logger.addFilter(SuppressCancelledErrorFilter())

# Our application loggers should be more verbose
logging.getLogger("app").setLevel(logging.DEBUG if config.LOG_LEVEL == "DEBUG" else logging.INFO)
logging.getLogger("app.services.bulk_data").setLevel(logging.INFO)
logging.getLogger("app.services.bulk_data_parsers").setLevel(logging.INFO)
logging.getLogger("app.services.bulk_updater").setLevel(logging.INFO)

# Create logger for this module
logger = logging.getLogger(__name__)

app = FastAPI(
    title="FEC Campaign Finance Analysis API",
    description="API for querying and analyzing Federal Election Commission data",
    version="1.0.0"
)

# Initialize rate limiter after app creation
# Use default_storage (in-memory) which is simpler and doesn't require Redis
try:
    from slowapi import Limiter
    
    # Initialize limiter - for FastAPI, we don't pass app to constructor
    # Instead, we assign it to app.state.limiter and slowapi handles the rest
    limiter = Limiter(
        key_func=get_remote_address,
        default_limits=["1000/hour"]
    )
    app.state.limiter = limiter
    
    # Add exception handler for rate limit exceeded with security logging
    @app.exception_handler(RateLimitExceeded)
    async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
        from app.api.security import log_security_event
        log_security_event("rate_limit", {
            "limit": str(exc.detail) if hasattr(exc, 'detail') else "unknown"
        }, request)
        return JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded. Please try again later."}
        )
    
    logger.info("Rate limiter initialized successfully")
except Exception as e:
    logger.warning(f"Failed to initialize rate limiter: {e}. Continuing without rate limiting.")
    import traceback
    logger.debug(traceback.format_exc())
    # Create a dummy limiter to avoid AttributeError
    class DummyLimiter:
        def limit(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator
    app.state.limiter = DummyLimiter()

# Security headers middleware
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        # Add security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        # Only add HSTS if using HTTPS (check via environment variable)
        if os.getenv("USE_HTTPS", "false").lower() == "true":
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        
        # Log security events for admin operations (lazy import to avoid circular dependencies)
        try:
            if request.method in ["DELETE", "POST"] and "/api/bulk-data" in request.url.path:
                from app.api.security import log_security_event
                log_security_event("admin_operation", {
                    "method": request.method,
                    "path": request.url.path,
                }, request)
        except Exception as e:
            logger.debug(f"Could not log security event: {e}")
        
        return response

# Add security headers middleware
app.add_middleware(SecurityHeadersMiddleware)

# CORS configuration - restrict to specific origins and methods
cors_origins = config.CORS_ORIGINS
# Clean up origins (remove empty strings)
cors_origins = [origin.strip() for origin in cors_origins if origin.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],  # Explicit methods instead of *
    allow_headers=["Content-Type", "Authorization", "Accept"],  # Explicit headers instead of *
)

# Include routers
app.include_router(candidates.router, prefix="/api/candidates", tags=["candidates"])
app.include_router(contributions.router, prefix="/api/contributions", tags=["contributions"])
app.include_router(analysis.router, prefix="/api/analysis", tags=["analysis"])
app.include_router(fraud.router, prefix="/api/fraud", tags=["fraud"])
app.include_router(bulk_data.router, prefix="/api/bulk-data", tags=["bulk-data"])
app.include_router(export.router, prefix="/api/export", tags=["export"])
app.include_router(independent_expenditures.router, prefix="/api/independent-expenditures", tags=["independent-expenditures"])
app.include_router(committees.router, prefix="/api/committees", tags=["committees"])
app.include_router(saved_searches.router, prefix="/api/saved-searches", tags=["saved-searches"])
app.include_router(trends.router, prefix="/api/trends", tags=["trends"])
app.include_router(settings.router, prefix="/api/settings", tags=["settings"])


@app.on_event("startup")
async def startup_event():
    """Initialize database and set up startup tasks"""
    logger.info("Starting application startup...")
    
    # Validate configuration
    config_warnings = config.validate()
    if config_warnings:
        for warning in config_warnings:
            logger.warning(f"Config warning: {warning}")
    
    # Initialize thread pool for CPU-bound operations
    from app.utils.thread_pool import get_thread_pool
    thread_pool = get_thread_pool()
    logger.info(f"Thread pool initialized with {config.THREAD_POOL_WORKERS} workers")
    
    # Log uvicorn worker configuration (if available)
    import multiprocessing
    try:
        # Try to detect worker count from process name or environment
        logger.info(f"Uvicorn workers: {config.UVICORN_WORKERS}")
    except Exception:
        pass
    
    await init_db()
    logger.info("Database initialization complete")
    
    # Set up all startup tasks (incomplete jobs check, background tasks, contact updater, signal handlers)
    await setup_startup_tasks(_running_tasks)


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    # Shutdown thread pool
    from app.utils.thread_pool import shutdown_thread_pool
    shutdown_thread_pool()
    
    await setup_shutdown_handlers()


# Global exception handlers
@app.exception_handler(APIError)
async def api_error_handler(request: Request, exc: APIError):
    """Handle custom API exceptions with structured error response"""
    # Log error with context
    logger.error(
        f"API Error [{exc.request_id}]: {exc.code} - {exc.message}",
        extra={
            "request_id": exc.request_id,
            "error_code": exc.code,
            "status_code": exc.status_code,
            "is_transient": exc.is_transient,
            "path": request.url.path,
            "method": request.method,
            "details": exc.details
        }
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.to_dict()
        }
    )


@app.exception_handler(FECServiceError)
async def fec_service_error_handler(request: Request, exc: FECServiceError):
    """Handle FEC service errors with structured response"""
    request_id = str(uuid.uuid4())[:8]
    status_code = 500
    code = "SERVICE_ERROR"
    is_transient = False
    error_details = {}
    
    if isinstance(exc, FECAPIError):
        status_code = exc.status_code or 500
        code = "FEC_API_ERROR"
        is_transient = True  # API errors are often transient
        if isinstance(exc, RateLimitError):
            status_code = 429
            code = "RATE_LIMIT_ERROR"
            error_details = {
                "suggestion": "Please wait before making more requests",
                "retry_after": 60
            }
    elif isinstance(exc, DatabaseLockError):
        status_code = 503
        code = "DATABASE_LOCK_ERROR"
        is_transient = True
        error_details = {
            "suggestion": "Database is busy. Please try again in a moment."
        }
    elif isinstance(exc, BulkDataError):
        status_code = 500
        code = "BULK_DATA_ERROR"
        is_transient = False  # Bulk data errors are usually permanent
        error_details = {
            "error_type": "BulkDataError"
        }
    
    logger.error(
        f"FEC Service Error [{request_id}]: {code} - {str(exc)}",
        extra={
            "request_id": request_id,
            "error_code": code,
            "status_code": status_code,
            "is_transient": is_transient,
            "path": request.url.path,
            "method": request.method
        }
    )
    
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "message": str(exc),
                "details": error_details,
                "request_id": request_id,
                "is_transient": is_transient
            }
        }
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Handle generic exceptions (500) with error classification"""
    import traceback
    from sqlalchemy.exc import OperationalError, DatabaseError as SQLAlchemyDatabaseError
    from httpx import TimeoutException, RequestError
    import asyncio
    
    # Generate request ID for tracking
    request_id = str(uuid.uuid4())[:8]
    
    # Classify error type
    error_code = "INTERNAL_SERVER_ERROR"
    status_code = 500
    is_transient = False
    error_message = "An internal server error occurred"
    error_details = {}
    
    # Classify specific error types
    if isinstance(exc, (OperationalError, SQLAlchemyDatabaseError)):
        error_code = "DATABASE_ERROR"
        status_code = 503
        is_transient = True
        error_message = "Database operation failed"
        error_details = {
            "error_type": type(exc).__name__,
            "suggestion": "This may be a temporary issue. Please try again."
        }
    elif isinstance(exc, (TimeoutException, asyncio.TimeoutError)):
        error_code = "TIMEOUT_ERROR"
        status_code = 504
        is_transient = True
        error_message = "Request timeout"
        error_details = {
            "error_type": type(exc).__name__,
            "suggestion": "The operation took too long. Please try again with a smaller dataset."
        }
    elif isinstance(exc, RequestError):
        error_code = "NETWORK_ERROR"
        status_code = 503
        is_transient = True
        error_message = "Network request failed"
        error_details = {
            "error_type": type(exc).__name__,
            "suggestion": "Network issue detected. Please try again."
        }
    elif isinstance(exc, ValueError):
        error_code = "VALIDATION_ERROR"
        status_code = 400
        is_transient = False
        error_message = str(exc) or "Invalid input"
        error_details = {
            "error_type": "ValueError"
        }
    elif isinstance(exc, KeyError):
        error_code = "MISSING_FIELD_ERROR"
        status_code = 400
        is_transient = False
        error_message = f"Missing required field: {exc}"
        error_details = {
            "error_type": "KeyError",
            "missing_field": str(exc)
        }
    
    # Log error with full context
    logger.error(
        f"Unhandled exception [{request_id}]: {type(exc).__name__}: {exc}",
        exc_info=True,
        extra={
            "request_id": request_id,
            "error_code": error_code,
            "status_code": status_code,
            "is_transient": is_transient,
            "path": request.url.path,
            "method": request.method,
            "error_type": type(exc).__name__
        }
    )
    
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": error_code,
                "message": error_message,
                "details": error_details,
                "request_id": request_id,
                "is_transient": is_transient
            }
        }
    )


@app.get("/")
async def root():
    return {"message": "FEC Campaign Finance Analysis API", "version": "1.0.0"}


@app.get("/health")
async def health():
    """Basic health check endpoint"""
    return {"status": "healthy"}


@app.get("/health/database")
async def health_database():
    """Database health check with pool monitoring"""
    from app.services.shared.db_pool import get_db_pool_manager
    
    pool_manager = get_db_pool_manager()
    health_info = await pool_manager.health_check()
    
    return health_info

