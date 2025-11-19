"""
Background tasks for the application
"""
import asyncio
import logging
from datetime import datetime
from app.db.database import engine, AsyncSessionLocal, Contribution, Candidate, Committee
from app.config import config
from sqlalchemy import text, select, func, and_

logger = logging.getLogger(__name__)

# Data integrity check interval from config
INTEGRITY_CHECK_INTERVAL = config.INTEGRITY_CHECK_INTERVAL_HOURS * 3600


async def periodic_wal_checkpoint():
    """Periodically checkpoint WAL file to prevent I/O errors and WAL file growth
    
    Runs every 30 minutes to balance between performance and WAL file management.
    Also triggered after large bulk imports.
    """
    while True:
        try:
            await asyncio.sleep(config.WAL_CHECKPOINT_INTERVAL_SECONDS)
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


async def checkpoint_wal_after_import():
    """Checkpoint WAL file after large bulk imports to prevent WAL file growth"""
    try:
        async with engine.begin() as conn:
            # Use TRUNCATE mode to aggressively checkpoint after large imports
            result = await conn.execute(text("PRAGMA wal_checkpoint(TRUNCATE)"))
            checkpoint_info = result.fetchone()
            if checkpoint_info:
                busy, log, checkpointed = checkpoint_info
                if busy == 0:
                    logger.info(f"WAL checkpoint after import: {checkpointed}/{log} frames checkpointed")
                else:
                    logger.debug(f"WAL checkpoint in progress: {checkpointed}/{log} frames checkpointed")
    except Exception as e:
        logger.warning(f"Error during WAL checkpoint after import: {e}")


async def periodic_data_integrity_check():
    """Periodically check database integrity and data consistency
    
    Runs daily (configurable via INTEGRITY_CHECK_INTERVAL_HOURS) to detect:
    - Orphaned records (contributions without valid candidates/committees)
    - Missing required fields
    - Invalid data relationships
    - Duplicate records
    """
    while True:
        try:
            await asyncio.sleep(INTEGRITY_CHECK_INTERVAL)
            logger.info("Starting periodic data integrity check...")
            
            async with AsyncSessionLocal() as session:
                issues = []
                
                # Check 1: Orphaned contributions (no valid candidate_id or committee_id)
                orphaned_query = select(func.count(Contribution.id)).where(
                    and_(
                        Contribution.candidate_id.is_(None),
                        Contribution.committee_id.is_(None)
                    )
                )
                result = await session.execute(orphaned_query)
                orphaned_count = result.scalar() or 0
                if orphaned_count > 0:
                    issues.append(f"Found {orphaned_count} orphaned contributions (no candidate_id or committee_id)")
                
                # Check 2: Contributions with invalid candidate_id (not in candidates table)
                # Only check a sample to avoid performance issues
                invalid_candidate_query = select(func.count(Contribution.id)).where(
                    and_(
                        Contribution.candidate_id.isnot(None),
                        ~select(Candidate.candidate_id).where(
                            Candidate.candidate_id == Contribution.candidate_id
                        ).exists()
                    )
                ).limit(1000)  # Sample check
                result = await session.execute(invalid_candidate_query)
                invalid_candidate_count = result.scalar() or 0
                if invalid_candidate_count > 0:
                    issues.append(f"Found {invalid_candidate_count}+ contributions with invalid candidate_id (sample check)")
                
                # Check 3: Contributions with missing required fields
                missing_amount_query = select(func.count(Contribution.id)).where(
                    and_(
                        Contribution.contribution_amount.is_(None),
                        Contribution.contribution_amount == 0
                    )
                )
                result = await session.execute(missing_amount_query)
                missing_amount_count = result.scalar() or 0
                if missing_amount_count > 0:
                    issues.append(f"Found {missing_amount_count} contributions with missing or zero amount")
                
                # Check 4: Duplicate contribution IDs (should be unique)
                duplicate_query = select(
                    Contribution.contribution_id,
                    func.count(Contribution.id).label('count')
                ).group_by(Contribution.contribution_id).having(
                    func.count(Contribution.id) > 1
                ).limit(100)  # Sample check
                result = await session.execute(duplicate_query)
                duplicates = result.all()
                if duplicates:
                    duplicate_count = len(duplicates)
                    issues.append(f"Found {duplicate_count}+ duplicate contribution_ids (sample check)")
                
                # Check 5: Database integrity (PRAGMA integrity_check)
                try:
                    integrity_result = await session.execute(text("PRAGMA integrity_check"))
                    integrity_row = integrity_result.fetchone()
                    if integrity_row and integrity_row[0] != "ok":
                        issues.append(f"Database integrity check failed: {integrity_row[0]}")
                except Exception as e:
                    logger.warning(f"Could not run integrity check: {e}")
                
                # Log results
                if issues:
                    logger.warning(f"Data integrity check found {len(issues)} issue(s):")
                    for issue in issues:
                        logger.warning(f"  - {issue}")
                else:
                    logger.info("Data integrity check passed - no issues found")
                
                # Log summary statistics
                total_contributions_query = select(func.count(Contribution.id))
                result = await session.execute(total_contributions_query)
                total_contributions = result.scalar() or 0
                
                total_candidates_query = select(func.count(Candidate.candidate_id))
                result = await session.execute(total_candidates_query)
                total_candidates = result.scalar() or 0
                
                total_committees_query = select(func.count(Committee.committee_id))
                result = await session.execute(total_committees_query)
                total_committees = result.scalar() or 0
                
                logger.info(
                    f"Data integrity check complete. Database stats: "
                    f"{total_contributions} contributions, "
                    f"{total_candidates} candidates, "
                    f"{total_committees} committees"
                )
                
        except Exception as e:
            logger.error(f"Error during data integrity check: {e}", exc_info=True)
        except asyncio.CancelledError:
            logger.debug("Data integrity check task cancelled")
            break


def start_background_tasks(running_tasks: set) -> asyncio.Task:
    """Start background tasks and add them to the running tasks set"""
    logger.info("Setting up background tasks...")
    
    # Start WAL checkpoint task
    checkpoint_task = asyncio.create_task(periodic_wal_checkpoint())
    running_tasks.add(checkpoint_task)
    
    # Start data integrity check task
    integrity_task = asyncio.create_task(periodic_data_integrity_check())
    running_tasks.add(integrity_task)
    
    logger.info(f"Background tasks started: WAL checkpoint (every 30 min), integrity check (every {INTEGRITY_CHECK_INTERVAL // 3600} hours)")
    
    return checkpoint_task

