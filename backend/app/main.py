from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routes import candidates, contributions, analysis, fraud, bulk_data, export, independent_expenditures, committees, saved_searches, trends, settings
from app.db.database import init_db
from app.services.bulk_data import _cancelled_jobs, _running_tasks
import os
import logging
import asyncio
import signal
from dotenv import load_dotenv

# Global contact updater service
_contact_updater_service = None

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
app.include_router(committees.router, prefix="/api/committees", tags=["committees"])
app.include_router(saved_searches.router, prefix="/api/saved-searches", tags=["saved-searches"])
app.include_router(trends.router, prefix="/api/trends", tags=["trends"])
app.include_router(settings.router, prefix="/api/settings", tags=["settings"])


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    logger.info("Starting application startup...")
    await init_db()
    logger.info("Database initialization complete")
    
    # Check for incomplete import jobs and log them
    try:
        logger.info("Checking for incomplete import jobs...")
        from app.services.bulk_data import BulkDataService
        bulk_data_service = BulkDataService()
        incomplete_jobs = await bulk_data_service.get_incomplete_jobs()
        if incomplete_jobs:
            logger.info(f"Found {len(incomplete_jobs)} incomplete import job(s) on startup:")
            for job in incomplete_jobs:
                logger.info(
                    f"  - Job {job.id}: {job.data_type or 'unknown'} cycle {job.cycle}, "
                    f"status={job.status}, imported={job.imported_records} records, "
                    f"file_position={job.file_position}"
                )
                logger.info(f"    To resume: Use the resume endpoint with job_id={job.id}")
        else:
            logger.info("No incomplete import jobs found")
    except Exception as e:
        logger.warning(f"Could not check for incomplete jobs on startup: {e}")
    
    # Periodic WAL checkpoint to prevent I/O errors
    logger.info("Setting up WAL checkpoint task...")
    async def periodic_wal_checkpoint():
        """Periodically checkpoint WAL file to prevent I/O errors"""
        import asyncio
        from app.db.database import engine
        from sqlalchemy import text
        logger = logging.getLogger(__name__)
        
        while True:
            try:
                await asyncio.sleep(180)  # Every 3 minutes (more frequent)
                async with engine.begin() as conn:
                    # Quick integrity check before checkpoint
                    try:
                        result = await conn.execute(text("PRAGMA quick_check"))
                        check_result = result.fetchone()
                        if check_result and check_result[0] != "ok":
                            logger.error(f"Database integrity issue detected: {check_result[0]}")
                            # Don't checkpoint if database is corrupted
                            continue
                    except Exception as e:
                        if "disk I/O error" in str(e).lower():
                            logger.error("Database corruption detected during periodic check!")
                            logger.error("Application may need to be restarted with a fresh database")
                            continue
                    
                    # Perform checkpoint - use PASSIVE mode first (non-blocking)
                    # If that fails, try RESTART mode, then TRUNCATE as last resort
                    checkpoint_success = False
                    for checkpoint_mode in ["PASSIVE", "RESTART", "TRUNCATE"]:
                        try:
                            if checkpoint_mode == "PASSIVE":
                                result = await conn.execute(text("PRAGMA wal_checkpoint"))
                            elif checkpoint_mode == "RESTART":
                                result = await conn.execute(text("PRAGMA wal_checkpoint(RESTART)"))
                            else:  # TRUNCATE
                                result = await conn.execute(text("PRAGMA wal_checkpoint(TRUNCATE)"))
                            
                            checkpoint_info = result.fetchone()
                            if checkpoint_info:
                                # Return format: (busy, log, checkpointed)
                                # busy: 0 = checkpoint completed, 1 = checkpoint still in progress
                                # log: number of frames in WAL
                                # checkpointed: number of frames checkpointed
                                busy, log, checkpointed = checkpoint_info
                                if busy == 0:
                                    logger.debug(f"WAL checkpoint completed successfully ({checkpoint_mode}): {checkpointed}/{log} frames checkpointed")
                                    checkpoint_success = True
                                    break
                                else:
                                    logger.debug(f"WAL checkpoint in progress ({checkpoint_mode}): {checkpointed}/{log} frames checkpointed")
                                    # If checkpoint is in progress, that's okay - it will complete
                                    checkpoint_success = True
                                    break
                        except Exception as checkpoint_error:
                            logger.debug(f"WAL checkpoint {checkpoint_mode} failed: {checkpoint_error}, trying next mode...")
                            continue
                    
                    if not checkpoint_success:
                        logger.warning("All WAL checkpoint modes failed - database may be busy with transactions")
            except Exception as e:
                if "disk I/O error" in str(e).lower() or "corrupted" in str(e).lower():
                    logger.error(f"Database corruption detected: {e}")
                    logger.error("Please see backend/migrations/REPAIR_INSTRUCTIONS.md")
                else:
                    logger.warning(f"Error during WAL checkpoint: {e}")
            except asyncio.CancelledError:
                logger.debug("WAL checkpoint task cancelled")
                break
    
    # Start WAL checkpoint task
    checkpoint_task = asyncio.create_task(periodic_wal_checkpoint())
    _running_tasks.add(checkpoint_task)
    
    # Start contact updater service
    logger.info("Starting contact updater service...")
    from app.services.contact_updater import ContactUpdaterService
    global _contact_updater_service
    _contact_updater_service = ContactUpdaterService()
    await _contact_updater_service.start_background_updates()
    logger.info("Contact updater service started")
    
    # Set up signal handlers for graceful shutdown
    logger.info("Setting up signal handlers...")
    _shutdown_initiated = False
    
    def signal_handler(signum, frame):
        # Prevent multiple shutdown attempts
        nonlocal _shutdown_initiated
        if _shutdown_initiated:
            return
        _shutdown_initiated = True
        
        # Get logger in the signal handler scope
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        # Mark all jobs for cancellation (actual cancellation happens in shutdown event)
        for job_id in list(_cancelled_jobs):
            _cancelled_jobs.add(job_id)
        logger.info(f"Marked jobs for cancellation. Shutdown event will handle cleanup.")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    logger.info("Application startup complete - ready to accept requests")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger = logging.getLogger(__name__)
    logger.info("Application shutting down, initiating graceful shutdown...")
    
    # Step 1: Mark all jobs as cancelled in database (do this before cancelling tasks)
    from app.services.bulk_data import BulkDataService
    from app.db.database import AsyncSessionLocal, BulkImportJob, engine
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
    except asyncio.CancelledError:
        logger.debug("Database operation cancelled during shutdown")
    
    # Step 2: Stop contact updater service
    global _contact_updater_service
    if _contact_updater_service:
        try:
            await _contact_updater_service.stop_background_updates()
        except Exception as e:
            logger.warning(f"Error stopping contact updater: {e}")
        except asyncio.CancelledError:
            logger.debug("Contact updater stop cancelled")
    
    # Step 3: Cancel all running tasks gracefully
    cancelled_count = 0
    tasks_to_wait = []
    
    for task in list(_running_tasks):
        if not task.done():
            task.cancel()
            cancelled_count += 1
            tasks_to_wait.append(task)
    
    # Step 4: Wait for tasks to finish cancelling (with timeout)
    if tasks_to_wait:
        logger.info(f"Waiting for {len(tasks_to_wait)} tasks to cancel...")
        try:
            # Wait for tasks with a timeout, but don't fail if they don't complete
            done, pending = await asyncio.wait(
                tasks_to_wait, 
                timeout=3.0, 
                return_when=asyncio.ALL_COMPLETED
            )
            if pending:
                logger.debug(f"{len(pending)} tasks still pending after timeout, continuing shutdown")
        except Exception as e:
            logger.debug(f"Error waiting for tasks to cancel: {e}")
        except asyncio.CancelledError:
            logger.debug("Task wait cancelled during shutdown")
    
    # Step 5: Close database connections gracefully
    try:
        logger.info("Closing database connections...")
        # Dispose of the engine, which will close all connections
        await engine.dispose()
        logger.info("Database connections closed")
    except Exception as e:
        logger.warning(f"Error closing database connections: {e}")
    except asyncio.CancelledError:
        logger.debug("Database connection close cancelled")
    
    logger.info(f"Shutdown complete. Cancelled {cancelled_count} tasks.")


@app.get("/")
async def root():
    return {"message": "FEC Campaign Finance Analysis API", "version": "1.0.0"}


@app.get("/health")
async def health():
    return {"status": "healthy"}

