from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routes import candidates, contributions, analysis, fraud, bulk_data, export, independent_expenditures
from app.db.database import init_db
from app.services.bulk_data import _cancelled_jobs, _running_tasks
import os
import logging
import asyncio
import signal
from dotenv import load_dotenv

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

# Our application loggers should be more verbose
logging.getLogger("app").setLevel(logging.DEBUG if log_level == "DEBUG" else logging.INFO)
logging.getLogger("app.services.bulk_data").setLevel(logging.INFO)
logging.getLogger("app.services.bulk_data_parsers").setLevel(logging.INFO)
logging.getLogger("app.services.bulk_updater").setLevel(logging.INFO)

app = FastAPI(
    title="FEC Campaign Finance Analysis API",
    description="API for querying and analyzing Federal Election Commission data",
    version="1.0.0"
)

# CORS configuration
cors_origins = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:5173").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(candidates.router, prefix="/api/candidates", tags=["candidates"])
app.include_router(contributions.router, prefix="/api/contributions", tags=["contributions"])
app.include_router(analysis.router, prefix="/api/analysis", tags=["analysis"])
app.include_router(fraud.router, prefix="/api/fraud", tags=["fraud"])
app.include_router(bulk_data.router, prefix="/api/bulk-data", tags=["bulk-data"])
app.include_router(export.router, prefix="/api/export", tags=["export"])
app.include_router(independent_expenditures.router, prefix="/api/independent-expenditures", tags=["independent-expenditures"])


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    await init_db()
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        # Get logger in the signal handler scope
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        # Cancel all running jobs
        for job_id in list(_cancelled_jobs):
            _cancelled_jobs.add(job_id)
        # Cancel all running tasks
        for task in list(_running_tasks):
            if not task.done():
                task.cancel()
        logger.info(f"Cancelled {len(_running_tasks)} running tasks")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger = logging.getLogger(__name__)
    logger.info("Application shutting down, cancelling all running jobs...")
    
    # Get all running jobs from database and mark them as cancelled
    from app.services.bulk_data import BulkDataService
    from app.db.database import AsyncSessionLocal, BulkImportJob
    from sqlalchemy import select
    
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BulkImportJob).where(BulkImportJob.status.in_(['pending', 'running']))
            )
            running_jobs = result.scalars().all()
            for job in running_jobs:
                _cancelled_jobs.add(job.id)
                job.status = 'cancelled'
            await session.commit()
            logger.info(f"Marked {len(running_jobs)} running jobs as cancelled")
    except Exception as e:
        logger.warning(f"Error cancelling jobs in database: {e}")
    
    # Cancel all running tasks
    cancelled_count = 0
    for task in list(_running_tasks):
        if not task.done():
            task.cancel()
            cancelled_count += 1
    
    # Wait a bit for tasks to finish cancelling
    if _running_tasks:
        logger.info(f"Waiting for {len(_running_tasks)} tasks to cancel...")
        try:
            await asyncio.wait(_running_tasks, timeout=5.0, return_when=asyncio.ALL_COMPLETED)
        except Exception as e:
            logger.warning(f"Error waiting for tasks to cancel: {e}")
    
    logger.info(f"Shutdown complete. Cancelled {cancelled_count} tasks.")


@app.get("/")
async def root():
    return {"message": "FEC Campaign Finance Analysis API", "version": "1.0.0"}


@app.get("/health")
async def health():
    return {"status": "healthy"}

