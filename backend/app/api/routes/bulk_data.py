from fastapi import APIRouter, HTTPException, Query, BackgroundTasks, WebSocket, WebSocketDisconnect, Request
from typing import Optional, List
from app.services.bulk_data import BulkDataService, _running_tasks, _cancelled_jobs
from app.services.bulk_updater import BulkUpdaterService
from app.services.bulk_data_config import DataType, get_config, get_high_priority_types, DATA_TYPE_CONFIGS
from app.services.backfill_candidate_ids import backfill_candidate_ids_from_committees, get_backfill_stats
from app.db.database import BulkImportJob
from app.api.security import (
    BULK_RATE_LIMIT, EXPENSIVE_RATE_LIMIT, READ_RATE_LIMIT,
    check_resource_limits, increment_operation, decrement_operation,
    log_security_event, MAX_CONCURRENT_JOBS
)
from app.utils.date_utils import serialize_datetime
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from datetime import datetime
import logging
import asyncio
import json

logger = logging.getLogger(__name__)

router = APIRouter()

# Rate limiter instance (will be set from app state)
limiter = None

def get_limiter():
    """Get rate limiter from app state."""
    global limiter
    if limiter is None:
        from fastapi import Request
        # This will be set when router is included in main.py
        pass
    return limiter

# Global services (will be initialized on first use)
_bulk_data_service: Optional[BulkDataService] = None
_bulk_updater_service: Optional[BulkUpdaterService] = None


def get_bulk_data_service() -> BulkDataService:
    """Get bulk data service instance"""
    global _bulk_data_service
    if _bulk_data_service is None:
        _bulk_data_service = BulkDataService()
    return _bulk_data_service


def get_bulk_updater_service() -> BulkUpdaterService:
    """Get bulk updater service instance"""
    global _bulk_updater_service
    if _bulk_updater_service is None:
        _bulk_updater_service = BulkUpdaterService()
    return _bulk_updater_service


# Rate limiting will be handled via decorators on routes when needed
# For now, we rely on resource limits and basic protection

@router.post("/import-multiple")
async def import_multiple_data_types(
    request: Request,
    cycle: int = Query(..., description="Election cycle to download (e.g., 2024)"),
    data_types: List[str] = Query(..., description="List of data types to import"),
    force_download: bool = Query(False, description="Force download even if file size matches"),
    background_tasks: BackgroundTasks = None
):
    """Import multiple data types for a cycle"""
    # Rate limiting is handled by slowapi middleware via app.state.limiter
    # Check resource limits
    if not check_resource_limits("bulk_imports"):
        log_security_event("resource_limit", {
            "operation": "import_multiple",
            "cycle": cycle,
            "max_concurrent": MAX_CONCURRENT_JOBS
        }, request)
        raise HTTPException(
            status_code=429,
            detail=f"Too many concurrent bulk import operations. Maximum {MAX_CONCURRENT_JOBS} allowed."
        )
    
    try:
        bulk_data_service = get_bulk_data_service()
        
        # Validate data types
        validated_types = []
        for dt_str in data_types:
            try:
                dt = DataType(dt_str)
                validated_types.append(dt)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid data type: {dt_str}. Valid types: {[d.value for d in DataType]}"
                )
        
        if not validated_types:
            raise HTTPException(
                status_code=400,
                detail="At least one data type must be specified"
            )
        
        # Create job
        job_id = await bulk_data_service.create_job('multiple_types', cycle=cycle)
        
        async def _import_multiple():
            task = None
            try:
                task = asyncio.create_task(_import_multiple_internal())
                _running_tasks.add(task)
                await task
            except asyncio.CancelledError:
                logger.info(f"Import multiple job {job_id} was cancelled")
                await bulk_data_service._update_job_progress(job_id, status='cancelled')
            except Exception as e:
                logger.error(f"Error in import multiple job {job_id}: {e}", exc_info=True)
                await bulk_data_service._update_job_progress(job_id, status='failed', error_message=str(e))
            finally:
                if task:
                    _running_tasks.discard(task)
                decrement_operation("bulk_imports")
        
        async def _import_multiple_internal():
            await bulk_data_service._update_job_progress(job_id, status='running')
            results = await bulk_data_service.import_multiple_data_types(cycle, validated_types, job_id=job_id, force_download=force_download)
            
            # Check if all succeeded
            all_succeeded = all(r.get("success", False) for r in results.values())
            if all_succeeded:
                await bulk_data_service._update_job_progress(job_id, status='completed', completed_at=datetime.utcnow())
            else:
                failed = [dt for dt, r in results.items() if not r.get("success", False)]
                error_msg = f"Some imports failed: {', '.join(failed)}"
                await bulk_data_service._update_job_progress(job_id, status='failed', error_message=error_msg)
        
        # Increment operation counter before starting
        increment_operation("bulk_imports")
        
        # Run in background
        task = asyncio.create_task(_import_multiple())
        _running_tasks.add(task)
        
        return {
            "message": f"Import started for {len(validated_types)} data types, cycle {cycle}",
            "data_types": [dt.value for dt in validated_types],
            "cycle": cycle,
            "job_id": job_id,
            "status": "started"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error importing multiple data types for cycle {cycle}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to import multiple data types: {str(e)}"
        )


@router.post("/import-all-types")
async def import_all_data_types(
    request: Request,
    cycle: int = Query(..., description="Election cycle to download (e.g., 2024)", ge=2000, le=2100),
    background_tasks: BackgroundTasks = None
):
    """Import all implemented data types for a cycle"""
    # Check resource limits
    if not check_resource_limits("bulk_imports"):
        log_security_event("resource_limit", {
            "operation": "import_all_types",
            "cycle": cycle,
            "max_concurrent": MAX_CONCURRENT_JOBS
        }, request)
        raise HTTPException(
            status_code=429,
            detail=f"Too many concurrent bulk import operations. Maximum {MAX_CONCURRENT_JOBS} allowed."
        )
    
    try:
        bulk_data_service = get_bulk_data_service()
        
        # Create job
        job_id = await bulk_data_service.create_job('all_types', cycle=cycle)
        
        async def _import_all_types():
            task = None
            try:
                task = asyncio.create_task(_import_all_types_internal())
                _running_tasks.add(task)
                await task
            except asyncio.CancelledError:
                logger.info(f"Import all types job {job_id} was cancelled")
                await bulk_data_service._update_job_progress(job_id, status='cancelled')
            except Exception as e:
                logger.error(f"Error in import all types job {job_id}: {e}", exc_info=True)
                await bulk_data_service._update_job_progress(job_id, status='failed', error_message=str(e))
            finally:
                if task:
                    _running_tasks.discard(task)
        
        async def _import_all_types_internal():
            await bulk_data_service._update_job_progress(job_id, status='running')
            results = await bulk_data_service.import_all_data_types_for_cycle(cycle, job_id=job_id)
            
            # Check if all succeeded
            all_succeeded = all(r.get("success", False) for r in results.values())
            if all_succeeded:
                await bulk_data_service._update_job_progress(job_id, status='completed', completed_at=datetime.utcnow())
            else:
                failed = [dt for dt, r in results.items() if not r.get("success", False)]
                error_msg = f"Some imports failed: {', '.join(failed)}"
                await bulk_data_service._update_job_progress(job_id, status='failed', error_message=error_msg)
        
        # Run in background
        task = asyncio.create_task(_import_all_types())
        _running_tasks.add(task)
        
        return {
            "message": f"Import started for all implemented data types, cycle {cycle}",
            "cycle": cycle,
            "job_id": job_id,
            "status": "started"
        }
    except Exception as e:
        logger.error(f"Error importing all data types for cycle {cycle}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to import all data types: {str(e)}"
        )


@router.post("/download")
async def download_bulk_data(
    request: Request,
    cycle: int = Query(..., description="Election cycle to download (e.g., 2024)", ge=2000, le=2100),
    data_type: Optional[str] = Query(None, description="Data type to download (default: individual_contributions)", max_length=50),
    force_download: bool = Query(False, description="Force download even if file size matches"),
    background_tasks: BackgroundTasks = None
):
    """Manually trigger download for a specific cycle and data type with job tracking"""
    try:
        bulk_data_service = get_bulk_data_service()
        
        # Determine data type
        if data_type:
            try:
                dt = DataType(data_type)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid data type: {data_type}. Valid types: {[d.value for d in DataType]}"
                )
        else:
            # Default to individual contributions for backward compatibility
            dt = DataType.INDIVIDUAL_CONTRIBUTIONS
        
        # Create job
        job_id = await bulk_data_service.create_job('single_cycle', cycle=cycle)
        
        async def _download_with_job():
            task = None
            try:
                # Create asyncio task for better cancellation control
                task = asyncio.create_task(_download_with_job_internal())
                _running_tasks.add(task)
                await task
            except asyncio.CancelledError:
                logger.info(f"Download job {job_id} was cancelled")
                await bulk_data_service._update_job_progress(job_id, status='cancelled')
            except Exception as e:
                logger.error(f"Error in download job {job_id}: {e}", exc_info=True)
                await bulk_data_service._update_job_progress(job_id, status='failed', error_message=str(e))
            finally:
                if task:
                    _running_tasks.discard(task)
        
        async def _download_with_job_internal():
            await bulk_data_service._update_job_progress(job_id, status='running')
            result = await bulk_data_service.download_and_import_data_type(dt, cycle, job_id=job_id, force_download=force_download)
            if not result.get("success"):
                await bulk_data_service._update_job_progress(
                    job_id,
                    status='failed',
                    error_message=result.get("error", "Unknown error")
                )
        
        # Always run in background for long operations
        if background_tasks:
            # Use asyncio task instead of BackgroundTasks for better cancellation
            task = asyncio.create_task(_download_with_job())
            _running_tasks.add(task)
            # Don't await, let it run in background
        else:
            # Run synchronously but still track job
            await _download_with_job()
        
        return {
            "message": f"Download started for {dt.value}, cycle {cycle}",
            "data_type": dt.value,
            "cycle": cycle,
            "job_id": job_id,
            "status": "started"
        }
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading bulk data for cycle {cycle}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to download bulk data: {str(e)}"
        )


@router.get("/data-types")
async def get_data_types():
    """Get list of available data types with their configurations"""
    try:
        data_types = []
        for dt, config in DATA_TYPE_CONFIGS.items():
            data_types.append({
                "data_type": dt.value,
                "description": config.description,
                "file_format": config.file_format.value,
                "priority": config.priority,
                "min_cycle": config.min_cycle,
                "max_cycle": config.max_cycle,
                "header_file_url": config.header_file_url
            })
        
        # Sort by priority (highest first)
        data_types.sort(key=lambda x: x["priority"], reverse=True)
        
        return {
            "data_types": data_types,
            "count": len(data_types)
        }
    except Exception as e:
        logger.error(f"Error getting data types: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get data types: {str(e)}"
        )


@router.get("/status")
async def get_bulk_data_status():
    """Get current status of bulk data downloads"""
    try:
        bulk_updater_service = get_bulk_updater_service()
        status = await bulk_updater_service.get_status()
        return status
    except Exception as e:
        logger.error(f"Error getting bulk data status: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get status: {str(e)}"
        )


@router.get("/status/{cycle}")
async def get_cycle_status(cycle: int):
    """Get status for all data types for a specific cycle"""
    # Validate cycle range
    if cycle < 2000 or cycle > 2100:
        raise HTTPException(
            status_code=400,
            detail="Cycle must be between 2000 and 2100"
        )
    try:
        bulk_data_service = get_bulk_data_service()
        from app.services.bulk_data_config import DATA_TYPE_CONFIGS
        from app.services.bulk_data_parsers import GenericBulkDataParser
        
        # Get all statuses from database
        statuses = await bulk_data_service.get_all_data_type_statuses(cycle)
        
        # Get download dates from metadata
        from app.db.database import BulkDataMetadata
        from sqlalchemy import select
        from app.db.database import AsyncSessionLocal
        
        async with AsyncSessionLocal() as session:
            metadata_result = await session.execute(
                select(BulkDataMetadata).where(
                    BulkDataMetadata.cycle == cycle
                )
            )
            metadata_list = metadata_result.scalars().all()
            metadata_dict = {m.data_type: m for m in metadata_list}
        
        # Build response with all data types
        result = []
        for data_type, config in DATA_TYPE_CONFIGS.items():
            status_record = statuses.get(data_type.value)
            metadata_record = metadata_dict.get(data_type.value)
            is_implemented = GenericBulkDataParser.is_parser_implemented(data_type)
            
            result.append({
                "data_type": data_type.value,
                "description": config.description,
                "file_format": config.file_format.value,
                "priority": config.priority,
                "is_implemented": is_implemented,
                "status": status_record.status if status_record else "not_imported",
                "record_count": status_record.record_count if status_record else 0,
                "last_imported_at": serialize_datetime(status_record.last_imported_at) if status_record and status_record.last_imported_at else None,
                "download_date": serialize_datetime(metadata_record.download_date) if metadata_record and metadata_record.download_date else None,
                "error_message": status_record.error_message if status_record else None,
            })
        
        # Sort by priority (highest first)
        result.sort(key=lambda x: x["priority"], reverse=True)
        
        return {
            "cycle": cycle,
            "data_types": result,
            "count": len(result)
        }
    except Exception as e:
        logger.error(f"Error getting cycle status: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get cycle status: {str(e)}"
        )


@router.get("/cycles")
async def get_available_cycles(
    use_fec_api: bool = Query(False, description="Force refresh from FEC API (default: use database)")
):
    """List available cycles with bulk data. Uses database by default, only queries FEC API if DB is empty or >2 years old."""
    try:
        bulk_data_service = get_bulk_data_service()
        cycles = await bulk_data_service.get_available_cycles(use_fec_api=use_fec_api)
        return {
            "cycles": cycles,
            "count": len(cycles),
            "source": "fec_api" if use_fec_api else "database"
        }
    except Exception as e:
        logger.error(f"Error getting available cycles: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get cycles: {str(e)}"
        )


@router.post("/cycles/refresh")
async def refresh_available_cycles():
    """Manually refresh available cycles from FEC API and update database."""
    try:
        bulk_data_service = get_bulk_data_service()
        cycles = await bulk_data_service.refresh_available_cycles_from_fec()
        return {
            "message": f"Successfully refreshed {len(cycles)} available cycles",
            "cycles": cycles,
            "count": len(cycles)
        }
    except Exception as e:
        logger.error(f"Error refreshing available cycles: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to refresh cycles: {str(e)}"
        )


@router.post("/update")
async def trigger_update(
    cycles: Optional[List[int]] = Query(None, description="Specific cycles to update (defaults to current and recent)"),
    background_tasks: BackgroundTasks = None
):
    """Trigger update check for bulk data"""
    try:
        bulk_updater_service = get_bulk_updater_service()
        
        if background_tasks:
            background_tasks.add_task(
                bulk_updater_service.check_and_update_cycles,
                cycles
            )
            return {
                "message": "Update check started",
                "status": "started"
            }
        else:
            results = await bulk_updater_service.check_and_update_cycles(cycles)
            return {
                "message": "Update check completed",
                "results": results,
                "status": "completed"
            }
            
    except Exception as e:
        logger.error(f"Error triggering update: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger update: {str(e)}"
        )


@router.post("/backfill-candidate-ids")
async def backfill_candidate_ids_endpoint(
    batch_size: int = Query(10000, ge=1, le=100000, description="Number of contributions to update per batch"),
    limit: Optional[int] = Query(None, ge=1, description="Optional limit on total contributions to update"),
    background_tasks: BackgroundTasks = None
):
    """Backfill candidate_id in contributions using committee linkages"""
    try:
        # Get stats first
        stats = await get_backfill_stats()
        
        if stats["contributions_missing_candidate_id"] == 0:
            return {
                "message": "No contributions need candidate_id backfill",
                "stats": stats
            }
        
        # Run backfill in background if it's a large operation
        if stats["contributions_missing_candidate_id"] > 10000:
            async def _backfill_task():
                try:
                    result = await backfill_candidate_ids_from_committees(batch_size=batch_size, limit=limit)
                    logger.info(f"Backfill completed: {result}")
                except Exception as e:
                    logger.error(f"Error in backfill task: {e}", exc_info=True)
            
            task = asyncio.create_task(_backfill_task())
            _running_tasks.add(task)
            
            return {
                "message": f"Backfill started for {stats['contributions_missing_candidate_id']} contributions",
                "stats": stats,
                "status": "started"
            }
        else:
            # Run synchronously for small operations
            result = await backfill_candidate_ids_from_committees(batch_size=batch_size, limit=limit)
            return {
                "message": "Backfill completed",
                "stats": stats,
                "result": result
            }
    except Exception as e:
        logger.error(f"Error backfilling candidate_ids: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to backfill candidate_ids: {str(e)}"
        )


@router.get("/backfill-candidate-ids/stats")
async def get_backfill_stats_endpoint():
    """Get statistics about contributions missing candidate_id"""
    try:
        stats = await get_backfill_stats()
        return stats
    except Exception as e:
        logger.error(f"Error getting backfill stats: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get backfill stats: {str(e)}"
        )


@router.delete("/contributions")
async def clear_contributions(
    request: Request,
    cycle: Optional[int] = Query(None, description="Clear contributions for specific cycle (optional, clears all if not specified)", ge=2000, le=2100)
):
    """Clear all contributions from the database (or for a specific cycle)"""
    try:
        bulk_data_service = get_bulk_data_service()
        deleted_count = await bulk_data_service.clear_contributions(cycle=cycle)
        
        return {
            "message": f"Cleared {deleted_count} contributions from database",
            "deleted_count": deleted_count,
            "cycle": cycle
        }
    except Exception as e:
        logger.error(f"Error clearing contributions: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to clear contributions: {str(e)}"
        )


@router.delete("/all-data")
async def clear_all_data(request: Request):
    """Clear ALL data from the database (contributions, committees, candidates, etc.)"""
    try:
        bulk_data_service = get_bulk_data_service()
        deleted_counts = await bulk_data_service.clear_all_data()
        
        total_deleted = sum(deleted_counts.values())
        
        return {
            "message": f"Cleared all data from database ({total_deleted} total records)",
            "deleted_counts": deleted_counts,
            "total_deleted": total_deleted
        }
    except Exception as e:
        logger.error(f"Error clearing all data: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to clear all data: {str(e)}"
        )


@router.post("/import-all")
async def import_all_cycles(
    background_tasks: BackgroundTasks,
    start_year: Optional[int] = Query(None, description="Start year (default: auto-detect from database)"),
    end_year: Optional[int] = Query(None, description="End year (default: auto-detect from database)"),
    use_fec_api: bool = Query(False, description="Force refresh from FEC API (default: use database)")
):
    """Import all available cycles - always runs in background. Uses database by default to determine which cycles have data."""
    try:
        from datetime import datetime
        bulk_data_service = get_bulk_data_service()
        bulk_updater_service = get_bulk_updater_service()
        
        # Get available cycles from database (or FEC API if requested)
        try:
            cycles_data = await bulk_data_service.get_available_cycles(use_fec_api=use_fec_api)
            available_cycles = [c["cycle"] for c in cycles_data]
            
            if available_cycles:
                # Filter by start_year and end_year if provided
                if start_year:
                    available_cycles = [c for c in available_cycles if c >= start_year]
                if end_year:
                    available_cycles = [c for c in available_cycles if c <= end_year]
                cycles = available_cycles
            else:
                # Fallback if no cycles found
                current_year = datetime.now().year
                if end_year is None:
                    end_year = current_year + 6
                if start_year is None:
                    start_year = 2000
                cycles = list(range(start_year, end_year + 1, 2))
        except Exception as e:
            logger.warning(f"Failed to get cycles, using fallback: {e}")
            current_year = datetime.now().year
            if end_year is None:
                end_year = current_year + 6
            if start_year is None:
                start_year = 2000
            cycles = list(range(start_year, end_year + 1, 2))
        
        # Create job
        job_id = await bulk_data_service.create_job('all_cycles', cycles=cycles)
        
        async def _import_all():
            """Internal function to import all cycles"""
            task = None
            try:
                # Create asyncio task for better cancellation control
                task = asyncio.create_task(_import_all_internal())
                _running_tasks.add(task)
                await task
            except asyncio.CancelledError:
                logger.info(f"Import all job {job_id} was cancelled")
                await bulk_data_service._update_job_progress(job_id, status='cancelled')
            except Exception as e:
                logger.error(f"Error in import all job {job_id}: {e}", exc_info=True)
                await bulk_data_service._update_job_progress(job_id, status='failed', error_message=str(e))
            finally:
                if task:
                    _running_tasks.discard(task)
        
        async def _import_all_internal():
            """Internal function to import all cycles"""
            await bulk_data_service._update_job_progress(
                job_id,
                status='running',
                total_cycles=len(cycles)
            )
            
            completed = 0
            for idx, cycle in enumerate(cycles, 1):
                # Check for cancellation frequently
                if job_id in _cancelled_jobs:
                    logger.info(f"Import cancelled for job {job_id}")
                    await bulk_data_service._update_job_progress(job_id, status='cancelled')
                    return
                
                # Check if task was cancelled
                task = asyncio.current_task()
                if task and task.cancelled():
                    logger.info(f"Import task cancelled for job {job_id}")
                    await bulk_data_service._update_job_progress(job_id, status='cancelled')
                    return
                
                try:
                    logger.info(f"Importing cycle {cycle} ({idx}/{len(cycles)})")
                    await bulk_data_service._update_job_progress(
                        job_id,
                        current_cycle=cycle,
                        completed_cycles=completed
                    )
                    
                    result = await bulk_updater_service.download_and_import_cycle(cycle, job_id=job_id)
                    if result.get("success"):
                        completed += 1
                        logger.info(f"Successfully imported cycle {cycle}: {result.get('record_count', 0)} records")
                    else:
                        logger.warning(f"Failed to import cycle {cycle}: {result.get('error')}")
                except asyncio.CancelledError:
                    logger.info(f"Import cancelled during cycle {cycle}")
                    await bulk_data_service._update_job_progress(job_id, status='cancelled')
                    raise
                except Exception as e:
                    logger.error(f"Error importing cycle {cycle}: {e}", exc_info=True)
            
            await bulk_data_service._update_job_progress(
                job_id,
                status='completed',
                completed_cycles=completed,
                completed_at=datetime.utcnow()
            )
            logger.info(f"Completed importing all {len(cycles)} cycles")
        
        # Always run in background since this is a long-running operation
        # Use asyncio task instead of BackgroundTasks for better cancellation
        task = asyncio.create_task(_import_all())
        _running_tasks.add(task)
        return {
            "message": f"Import started for {len(cycles)} cycles. This will run in the background and may take several hours.",
            "cycles": cycles,
            "count": len(cycles),
            "job_id": job_id,
            "status": "started",
            "source": "fec_api" if use_fec_api else "fallback"
        }
                
    except Exception as e:
        logger.error(f"Error starting import all cycles: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start import all cycles: {str(e)}"
        )


@router.post("/cleanup-and-reimport")
async def cleanup_and_reimport(
    cycle: int = Query(..., description="Election cycle to reimport (e.g., 2024)"),
    background_tasks: BackgroundTasks = None
):
    """Clear all contributions and reimport for a specific cycle"""
    try:
        from datetime import datetime
        bulk_data_service = get_bulk_data_service()
        bulk_updater_service = get_bulk_updater_service()
        
        # Create job
        job_id = await bulk_data_service.create_job('cleanup_reimport', cycle=cycle)
        
        async def _cleanup_and_reimport():
            """Internal function to perform cleanup and reimport"""
            task = None
            try:
                # Create asyncio task for better cancellation control
                task = asyncio.create_task(_cleanup_and_reimport_internal())
                _running_tasks.add(task)
                await task
            except asyncio.CancelledError:
                logger.info(f"Cleanup and reimport job {job_id} was cancelled")
                await bulk_data_service._update_job_progress(job_id, status='cancelled')
            except Exception as e:
                logger.error(f"Error in cleanup and reimport: {e}", exc_info=True)
                await bulk_data_service._update_job_progress(job_id, status='failed', error_message=str(e))
            finally:
                if task:
                    _running_tasks.discard(task)
        
        async def _cleanup_and_reimport_internal():
            """Internal function to perform cleanup and reimport"""
            # Check for cancellation
            if job_id in _cancelled_jobs:
                logger.info(f"Cleanup and reimport cancelled for job {job_id}")
                await bulk_data_service._update_job_progress(job_id, status='cancelled')
                return
            
            await bulk_data_service._update_job_progress(
                job_id,
                status='running',
                progress_data={"status": "clearing", "cycle": cycle}
            )
            
            # Step 1: Clear all contributions
            logger.info(f"Clearing all contributions before reimporting cycle {cycle}")
            deleted_count = await bulk_data_service.clear_contributions()
            logger.info(f"Cleared {deleted_count} contributions")
            
            # Check for cancellation again
            if job_id in _cancelled_jobs:
                logger.info(f"Cleanup and reimport cancelled after clearing contributions")
                await bulk_data_service._update_job_progress(job_id, status='cancelled')
                return
            
            await bulk_data_service._update_job_progress(
                job_id,
                progress_data={"status": "downloading", "cycle": cycle, "deleted_count": deleted_count}
            )
            
            # Step 2: Download and import the cycle
            result = await bulk_updater_service.download_and_import_cycle(cycle, job_id=job_id)
            logger.info(f"Cleanup and reimport completed for cycle {cycle}: {result.get('record_count', 0)} records imported")
            
            if result.get("success"):
                await bulk_data_service._update_job_progress(
                    job_id,
                    status='completed',
                    imported_records=result.get('record_count', 0),
                    completed_at=datetime.utcnow()
                )
            else:
                await bulk_data_service._update_job_progress(
                    job_id,
                    status='failed',
                    error_message=result.get('error', 'Unknown error')
                )
        
        if background_tasks:
            # Use asyncio task instead of BackgroundTasks for better cancellation
            task = asyncio.create_task(_cleanup_and_reimport())
            _running_tasks.add(task)
        else:
            await _cleanup_and_reimport()
        
        return {
            "message": f"Cleanup and reimport started for cycle {cycle}",
            "cycle": cycle,
            "job_id": job_id,
            "status": "started"
        }
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in cleanup and reimport for cycle {cycle}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cleanup and reimport: {str(e)}"
        )


@router.get("/jobs/{job_id}/status")
async def get_job_status(job_id: str):
    """Get status of a bulk import job (polling endpoint)"""
    try:
        bulk_data_service = get_bulk_data_service()
        job = await bulk_data_service.get_job(job_id)
        
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Calculate overall progress
        overall_progress = 0.0
        if job.total_chunks > 0:
            overall_progress = (job.current_chunk / job.total_chunks) * 100
        elif job.total_cycles > 0:
            overall_progress = (job.completed_cycles / job.total_cycles) * 100
        
        # Enhance progress_data with file_position if available
        progress_data = job.progress_data or {}
        if job.file_position and job.file_position > 0:
            progress_data['file_position'] = job.file_position
        
        return {
            "job_id": job.id,
            "job_type": job.job_type,
            "status": job.status,
            "cycle": job.cycle,
            "cycles": job.cycles,
            "total_cycles": job.total_cycles,
            "completed_cycles": job.completed_cycles,
            "current_cycle": job.current_cycle,
            "total_records": job.total_records,
            "imported_records": job.imported_records,
            "skipped_records": job.skipped_records,
            "current_chunk": job.current_chunk,
            "total_chunks": job.total_chunks,
            "error_message": job.error_message,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "progress_data": progress_data,
            "overall_progress": overall_progress
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job status: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get job status: {str(e)}"
        )


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Cancel a running bulk import job"""
    try:
        bulk_data_service = get_bulk_data_service()
        success = await bulk_data_service.cancel_job(job_id)
        
        if not success:
            raise HTTPException(status_code=400, detail="Job cannot be cancelled (not running or not found)")
        
        return {
            "message": f"Job {job_id} cancelled",
            "job_id": job_id,
            "status": "cancelled"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling job: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cancel job: {str(e)}"
        )


@router.post("/jobs/{job_id}/resume")
async def resume_job(job_id: str, background_tasks: BackgroundTasks):
    """Resume an incomplete import job"""
    try:
        bulk_data_service = get_bulk_data_service()
        
        # Check if job exists and is resumable
        async def _resume_job():
            task = None
            try:
                task = asyncio.create_task(bulk_data_service.resume_job(job_id))
                _running_tasks.add(task)
                success = await task
                if success:
                    logger.info(f"Job {job_id} resumed successfully")
                else:
                    logger.warning(f"Job {job_id} could not be resumed")
            except asyncio.CancelledError:
                logger.info(f"Resume job {job_id} was cancelled")
            except Exception as e:
                logger.error(f"Error resuming job {job_id}: {e}", exc_info=True)
                await bulk_data_service._update_job_progress(job_id, status='failed', error_message=str(e))
            finally:
                if task:
                    _running_tasks.discard(task)
        
        # Run in background
        task = asyncio.create_task(_resume_job())
        _running_tasks.add(task)
        
        return {
            "message": f"Resuming job {job_id}",
            "job_id": job_id,
            "status": "resuming"
        }
    except Exception as e:
        logger.error(f"Error resuming job {job_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to resume job: {str(e)}"
        )


@router.get("/jobs/incomplete")
async def get_incomplete_jobs():
    """Get all incomplete import jobs"""
    try:
        bulk_data_service = get_bulk_data_service()
        jobs = await bulk_data_service.get_incomplete_jobs()
        
        return {
            "jobs": [
                {
                    "job_id": job.id,
                    "job_type": job.job_type,
                    "status": job.status,
                    "cycle": job.cycle,
                    "data_type": job.data_type,
                    "file_path": job.file_path,
                    "imported_records": job.imported_records,
                    "file_position": job.file_position,
                    "started_at": job.started_at.isoformat() if job.started_at else None,
                    "error_message": job.error_message,
                    "progress_data": job.progress_data
                }
                for job in jobs
            ],
            "count": len(jobs)
        }
    except Exception as e:
        logger.error(f"Error getting incomplete jobs: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get incomplete jobs: {str(e)}"
        )


@router.get("/jobs/recent")
async def get_recent_jobs(limit: int = Query(10, ge=1, le=50, description="Number of recent jobs to return")):
    """Get recent import jobs (all statuses)"""
    try:
        bulk_data_service = get_bulk_data_service()
        jobs = await bulk_data_service.get_recent_jobs(limit=limit)
        
        # Convert jobs to full status format (same as get_job_status)
        result_jobs = []
        for job in jobs:
            # Calculate overall progress
            overall_progress = 0.0
            if job.total_chunks > 0:
                overall_progress = (job.current_chunk / job.total_chunks) * 100
            elif job.total_cycles > 0:
                overall_progress = (job.completed_cycles / job.total_cycles) * 100
            
            # Enhance progress_data with file_position if available
            progress_data = job.progress_data or {}
            if job.file_position and job.file_position > 0:
                progress_data['file_position'] = job.file_position
            
            result_jobs.append({
                "job_id": job.id,
                "job_type": job.job_type,
                "status": job.status,
                "cycle": job.cycle,
                "cycles": job.cycles,
                "total_cycles": job.total_cycles,
                "completed_cycles": job.completed_cycles,
                "current_cycle": job.current_cycle,
                "total_records": job.total_records,
                "imported_records": job.imported_records,
                "skipped_records": job.skipped_records,
                "current_chunk": job.current_chunk,
                "total_chunks": job.total_chunks,
                "error_message": job.error_message,
                "started_at": job.started_at.isoformat() if job.started_at else None,
                "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                "progress_data": progress_data,
                "overall_progress": overall_progress
            })
        
        return {
            "jobs": result_jobs,
            "count": len(result_jobs)
        }
    except Exception as e:
        logger.error(f"Error getting recent jobs: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get recent jobs: {str(e)}"
        )


@router.get("/committee-ids/invalid")
async def get_invalid_committee_ids():
    """Get list of invalid committee IDs in contributions"""
    try:
        bulk_data_service = get_bulk_data_service()
        
        from app.db.database import AsyncSessionLocal, Contribution
        from sqlalchemy import distinct, select
        
        async with AsyncSessionLocal() as session:
            # Get all unique committee IDs
            result = await session.execute(
                select(distinct(Contribution.committee_id)).where(
                    Contribution.committee_id.isnot(None)
                )
            )
            all_ids = [row[0] for row in result if row[0]]
            
            # Separate valid and invalid
            invalid_ids = []
            valid_ids = []
            corrections = {}
            
            for cid in all_ids:
                if bulk_data_service._is_valid_committee_id(cid):
                    valid_ids.append(cid)
                else:
                    invalid_ids.append(cid)
                    # Try to correct it
                    corrected = bulk_data_service._attempt_correct_committee_id(cid)
                    if corrected:
                        corrections[cid] = corrected
            
            return {
                "total": len(all_ids),
                "valid": len(valid_ids),
                "invalid": len(invalid_ids),
                "correctable": len(corrections),
                "invalid_ids": invalid_ids,
                "corrections": corrections
            }
    except Exception as e:
        logger.error(f"Error getting invalid committee IDs: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get invalid committee IDs: {str(e)}"
        )


@router.post("/committee-ids/fix")
async def fix_invalid_committee_ids():
    """Fix invalid committee IDs in contributions database"""
    try:
        bulk_data_service = get_bulk_data_service()
        
        from app.db.database import AsyncSessionLocal, Contribution
        from sqlalchemy import distinct, select, text
        
        async with AsyncSessionLocal() as session:
            # Get all unique committee IDs
            result = await session.execute(
                select(distinct(Contribution.committee_id)).where(
                    Contribution.committee_id.isnot(None)
                )
            )
            all_ids = [row[0] for row in result if row[0]]
            
            corrections = {}
            uncorrectable = []
            
            for cid in all_ids:
                if not bulk_data_service._is_valid_committee_id(cid):
                    corrected = bulk_data_service._attempt_correct_committee_id(cid)
                    if corrected:
                        corrections[cid] = corrected
                    else:
                        uncorrectable.append(cid)
            
            # Apply corrections
            updated_count = 0
            for original_id, corrected_id in corrections.items():
                try:
                    result = await session.execute(
                        text("""
                            UPDATE contributions 
                            SET committee_id = :corrected_id 
                            WHERE committee_id = :original_id
                        """),
                        {"corrected_id": corrected_id, "original_id": original_id}
                    )
                    updated_count += result.rowcount
                except Exception as e:
                    logger.warning(f"Error updating committee ID '{original_id}' to '{corrected_id}': {e}")
            
            await session.commit()
            
            return {
                "message": f"Fixed {len(corrections)} invalid committee IDs",
                "updated_records": updated_count,
                "corrections_applied": corrections,
                "uncorrectable": uncorrectable,
                "uncorrectable_count": len(uncorrectable)
            }
    except Exception as e:
        logger.error(f"Error fixing invalid committee IDs: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fix invalid committee IDs: {str(e)}"
        )


@router.websocket("/ws/{job_id}")
async def websocket_job_status(websocket: WebSocket, job_id: str):
    """WebSocket endpoint for real-time job progress updates"""
    await websocket.accept()
    
    try:
        bulk_data_service = get_bulk_data_service()
        
        # Send initial status
        job = await bulk_data_service.get_job(job_id)
        if not job:
            await websocket.send_json({
                "type": "error",
                "message": "Job not found"
            })
            await websocket.close()
            return
        
        # Send updates every 1-2 seconds while job is running
        last_status = None
        while True:
            job = await bulk_data_service.get_job(job_id)
            if not job:
                await websocket.send_json({
                    "type": "error",
                    "message": "Job not found"
                })
                break
            
            # Calculate overall progress
            overall_progress = 0.0
            if job.total_chunks > 0:
                overall_progress = (job.current_chunk / job.total_chunks) * 100
            elif job.total_cycles > 0:
                overall_progress = (job.completed_cycles / job.total_cycles) * 100
            
            current_status = job.status
            message = {
                "type": "progress" if job.status == "running" else job.status,
                "job_id": job_id,
                "data": {
                    "status": job.status,
                    "cycle": job.cycle,
                    "current_cycle": job.current_cycle,
                    "total_cycles": job.total_cycles,
                    "completed_cycles": job.completed_cycles,
                    "imported_records": job.imported_records,
                    "skipped_records": job.skipped_records,
                    "current_chunk": job.current_chunk,
                    "total_chunks": job.total_chunks,
                    "overall_progress": overall_progress,
                    "progress_data": job.progress_data or {},
                    "error_message": job.error_message
                }
            }
            
            # Only send if status changed or every 2 seconds
            if current_status != last_status or job.status == "running":
                await websocket.send_json(message)
                last_status = current_status
            
            # Stop sending updates if job is completed/failed/cancelled
            if job.status in ["completed", "failed", "cancelled"]:
                await websocket.send_json(message)
                break
            
            # Wait before next update
            await asyncio.sleep(2)
            
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for job {job_id}")
    except Exception as e:
        logger.error(f"Error in WebSocket for job {job_id}: {e}", exc_info=True)
        try:
            await websocket.send_json({
                "type": "error",
                "message": str(e)
            })
        except Exception as ws_err:
            logger.debug(f"Error sending WebSocket message: {ws_err}")
    finally:
        try:
            await websocket.close()
        except Exception as close_err:
            logger.debug(f"Error closing WebSocket: {close_err}")

