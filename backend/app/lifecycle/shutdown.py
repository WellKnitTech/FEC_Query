"""
Application shutdown handlers
"""
import asyncio
import logging
from app.services.bulk_data import _cancelled_jobs, _running_tasks
from app.lifecycle.startup import get_contact_updater_service
from app.db.database import engine

logger = logging.getLogger(__name__)


async def cancel_running_jobs():
    """Mark all running jobs as cancelled in database"""
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
    except asyncio.CancelledError:
        logger.debug("Database operation cancelled during shutdown")


async def stop_contact_updater():
    """Stop contact updater service"""
    contact_updater = get_contact_updater_service()
    if contact_updater:
        try:
            await contact_updater.stop_background_updates()
        except Exception as e:
            logger.warning(f"Error stopping contact updater: {e}")
        except asyncio.CancelledError:
            logger.debug("Contact updater stop cancelled")


async def cancel_background_tasks():
    """Cancel all running background tasks gracefully"""
    cancelled_count = 0
    tasks_to_wait = []
    
    for task in list(_running_tasks):
        if not task.done():
            task.cancel()
            cancelled_count += 1
            tasks_to_wait.append(task)
    
    # Wait for tasks to finish cancelling (with timeout)
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
    
    return cancelled_count


async def close_database_connections():
    """Close database connections gracefully"""
    try:
        logger.info("Closing database connections...")
        # Dispose of the engine, which will close all connections
        await engine.dispose()
        logger.info("Database connections closed")
    except Exception as e:
        logger.warning(f"Error closing database connections: {e}")
    except asyncio.CancelledError:
        logger.debug("Database connection close cancelled")


async def setup_shutdown_handlers():
    """Set up all shutdown handlers"""
    logger.info("Application shutting down, initiating graceful shutdown...")
    
    # Step 1: Mark all jobs as cancelled in database
    await cancel_running_jobs()
    
    # Step 2: Stop contact updater service
    await stop_contact_updater()
    
    # Step 3: Cancel all running tasks gracefully
    cancelled_count = await cancel_background_tasks()
    
    # Step 4: Clean up bulk data service resources
    try:
        from app.services.container import get_service_container
        container = get_service_container()
        if container._bulk_data_service:
            await container._bulk_data_service.cleanup()
            logger.info("Bulk data service cleaned up")
    except Exception as e:
        logger.warning(f"Error cleaning up bulk data service: {e}")
    
    # Step 5: Close database connections gracefully
    await close_database_connections()
    
    logger.info(f"Shutdown complete. Cancelled {cancelled_count} tasks.")

