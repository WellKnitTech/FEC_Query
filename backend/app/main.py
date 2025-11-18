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

load_dotenv()

# Configure logging with more detail
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Console output
    ]
)

# Set specific loggers
logging.getLogger("httpx").setLevel(logging.WARNING)  # Reduce httpx verbosity
logging.getLogger("httpcore").setLevel(logging.WARNING)  # Reduce httpcore verbosity
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)  # Reduce access log verbosity

# Filter to suppress CancelledError in SQLAlchemy pool during shutdown (expected behavior)
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
logging.getLogger("app").setLevel(logging.DEBUG if log_level == "DEBUG" else logging.INFO)
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
    
    limiter = Limiter(key_func=get_remote_address, app=app, default_limits=["1000/hour"])
    app.state.limiter = limiter
    
    # Add exception handler for rate limit exceeded
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
cors_origins = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:5173").split(",")
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
    await init_db()
    logger.info("Database initialization complete")
    
    # Set up all startup tasks (incomplete jobs check, background tasks, contact updater, signal handlers)
    await setup_startup_tasks(_running_tasks)


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    await setup_shutdown_handlers()


# Global exception handlers
@app.exception_handler(APIError)
async def api_error_handler(request: Request, exc: APIError):
    """Handle custom API exceptions"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": exc.code,
                "message": exc.message,
                "details": exc.details
            }
        }
    )


@app.exception_handler(FECServiceError)
async def fec_service_error_handler(request: Request, exc: FECServiceError):
    """Handle FEC service errors"""
    status_code = 500
    code = "SERVICE_ERROR"
    
    if isinstance(exc, FECAPIError):
        status_code = exc.status_code or 500
        code = "FEC_API_ERROR"
        if isinstance(exc, RateLimitError):
            status_code = 429
            code = "RATE_LIMIT_ERROR"
    elif isinstance(exc, DatabaseLockError):
        status_code = 503
        code = "DATABASE_LOCK_ERROR"
    elif isinstance(exc, BulkDataError):
        status_code = 500
        code = "BULK_DATA_ERROR"
    
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "message": str(exc),
                "details": {}
            }
        }
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Handle generic exceptions (500)"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "INTERNAL_SERVER_ERROR",
                "message": "An internal server error occurred",
                "details": {}
            }
        }
    )


@app.get("/")
async def root():
    return {"message": "FEC Campaign Finance Analysis API", "version": "1.0.0"}


@app.get("/health")
async def health():
    return {"status": "healthy"}

