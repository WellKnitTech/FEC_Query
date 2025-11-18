"""
Background tasks for the application
"""
import asyncio
import logging
from app.db.database import engine
from sqlalchemy import text

logger = logging.getLogger(__name__)


async def periodic_wal_checkpoint():
    """Periodically checkpoint WAL file to prevent I/O errors"""
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


def start_background_tasks(running_tasks: set) -> asyncio.Task:
    """Start background tasks and add them to the running tasks set"""
    logger.info("Setting up WAL checkpoint task...")
    checkpoint_task = asyncio.create_task(periodic_wal_checkpoint())
    running_tasks.add(checkpoint_task)
    return checkpoint_task

