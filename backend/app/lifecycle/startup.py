"""
Application startup tasks
"""
import logging
import signal
from app.services.bulk_data import BulkDataService, _cancelled_jobs
from app.services.contact_updater import ContactUpdaterService

logger = logging.getLogger(__name__)

# Global contact updater service
_contact_updater_service = None


async def check_incomplete_jobs():
    """Check for incomplete import jobs and log them"""
    try:
        logger.info("Checking for incomplete import jobs...")
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


async def start_contact_updater():
    """Start contact updater service"""
    global _contact_updater_service
    logger.info("Starting contact updater service...")
    _contact_updater_service = ContactUpdaterService()
    await _contact_updater_service.start_background_updates()
    logger.info("Contact updater service started")
    return _contact_updater_service


def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown"""
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
    logger.info("Signal handlers set up")


async def setup_startup_tasks(running_tasks: set):
    """Set up all startup tasks"""
    from app.lifecycle.tasks import start_background_tasks
    
    # Check for incomplete jobs
    await check_incomplete_jobs()
    
    # Start background tasks (WAL checkpoint)
    start_background_tasks(running_tasks)
    
    # Start contact updater
    await start_contact_updater()
    
    # Set up signal handlers
    setup_signal_handlers()
    
    logger.info("Application startup complete - ready to accept requests")


def get_contact_updater_service():
    """Get the global contact updater service instance"""
    return _contact_updater_service

