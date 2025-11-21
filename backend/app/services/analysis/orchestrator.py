"""Analysis orchestrator service for coordinating post-import analysis computation"""
import logging
import asyncio
import uuid
from typing import Optional, List, Dict, Any, Set
from datetime import datetime
from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import AsyncSessionLocal, AnalysisComputationJob, Contribution, Candidate
from app.services.fec_client import FECClient
from app.services.analysis.computation import AnalysisComputationService
from app.config import config

logger = logging.getLogger(__name__)


class AnalysisOrchestratorService:
    """Service to coordinate analysis computation after data imports"""
    
    def __init__(self, fec_client: FECClient):
        self.fec_client = fec_client
        self._computation_service = AnalysisComputationService(fec_client)
        self._running_jobs: Set[str] = set()
    
    async def schedule_analysis_after_import(
        self,
        cycle: int,
        data_type: str,
        affected_candidates: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        Schedule analysis computation after a successful data import.
        
        Args:
            cycle: The cycle that was imported
            data_type: The type of data that was imported (e.g., 'individual_contributions')
            affected_candidates: Optional list of candidate IDs affected by the import
        
        Returns:
            Job ID if job was scheduled, None otherwise
        """
        if not config.ENABLE_PRECOMPUTED_ANALYSIS:
            logger.debug("Pre-computed analysis is disabled, skipping scheduling")
            return None
        
        # Only schedule for contribution-related data types
        if data_type not in ['individual_contributions', 'schedule_a']:
            logger.debug(f"Skipping analysis scheduling for data type: {data_type}")
            return None
        
        try:
            # Create a background job for analysis computation
            job_id = str(uuid.uuid4())
            
            # Store job in database
            async with AsyncSessionLocal() as session:
                job = AnalysisComputationJob(
                    id=job_id,
                    job_type='cycle',
                    status='pending',
                    cycle=cycle,
                    total_items=0,  # Will be updated when job starts
                    completed_items=0,
                    started_at=datetime.utcnow(),
                    progress_data={
                        'data_type': data_type,
                        'affected_candidates': affected_candidates or []
                    }
                )
                session.add(job)
                await session.commit()
            
            # Schedule the job to run in background
            asyncio.create_task(self._run_analysis_job(job_id, cycle, affected_candidates))
            
            logger.info(
                f"Scheduled analysis computation job {job_id} for cycle {cycle}, "
                f"data_type={data_type}"
            )
            
            return job_id
            
        except Exception as e:
            logger.error(
                f"Error scheduling analysis after import: {e}",
                exc_info=True
            )
            return None
    
    async def compute_analyses_for_cycle(
        self,
        cycle: int,
        analysis_types: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Compute all analyses for a cycle.
        
        Args:
            cycle: The cycle to compute analyses for
            analysis_types: Optional list of analysis types to compute (default: all)
        
        Returns:
            Dict with computation results
        """
        if not analysis_types:
            analysis_types = ['employer', 'velocity', 'donor_states']
        
        results = {
            'cycle': cycle,
            'analysis_types': {},
            'total_computed': 0,
            'total_failed': 0
        }
        
        try:
            # Get all candidates with contributions in this cycle
            # Cycle covers (cycle-1)-01-01 to cycle-12-31
            from datetime import datetime
            cycle_start = datetime(cycle - 1, 1, 1)
            cycle_end = datetime(cycle, 12, 31)
            
            async with AsyncSessionLocal() as session:
                query = select(Contribution.candidate_id).where(
                    and_(
                        Contribution.candidate_id.isnot(None),
                        or_(
                            and_(
                                Contribution.contribution_date >= cycle_start,
                                Contribution.contribution_date <= cycle_end
                            ),
                            Contribution.contribution_date.is_(None)  # Include undated contributions
                        )
                    )
                ).distinct()
                
                result = await session.execute(query)
                candidate_ids = [row[0] for row in result if row[0]]
            
            logger.info(
                f"Computing analyses for cycle {cycle}: "
                f"{len(candidate_ids)} candidates, types={analysis_types}"
            )
            
            # Compute analyses for each candidate
            for candidate_id in candidate_ids:
                for analysis_type in analysis_types:
                    try:
                        if analysis_type == 'donor_states' and not candidate_id:
                            continue
                        
                        result = await self._computation_service.compute_and_store_analysis(
                            analysis_type=analysis_type,
                            candidate_id=candidate_id,
                            cycle=cycle
                        )
                        
                        if result:
                            if analysis_type not in results['analysis_types']:
                                results['analysis_types'][analysis_type] = {
                                    'computed': 0,
                                    'failed': 0
                                }
                            results['analysis_types'][analysis_type]['computed'] += 1
                            results['total_computed'] += 1
                        else:
                            if analysis_type not in results['analysis_types']:
                                results['analysis_types'][analysis_type] = {
                                    'computed': 0,
                                    'failed': 0
                                }
                            results['analysis_types'][analysis_type]['failed'] += 1
                            results['total_failed'] += 1
                            
                    except Exception as e:
                        logger.error(
                            f"Error computing {analysis_type} for candidate {candidate_id}, "
                            f"cycle {cycle}: {e}",
                            exc_info=True
                        )
                        results['total_failed'] += 1
            
            # Also compute cycle-level analyses (without candidate_id)
            for analysis_type in ['employer', 'velocity']:
                try:
                    result = await self._computation_service.compute_and_store_analysis(
                        analysis_type=analysis_type,
                        cycle=cycle
                    )
                    if result:
                        results['total_computed'] += 1
                except Exception as e:
                    logger.error(
                        f"Error computing cycle-level {analysis_type} for cycle {cycle}: {e}",
                        exc_info=True
                    )
                    results['total_failed'] += 1
            
            logger.info(
                f"Completed analysis computation for cycle {cycle}: "
                f"{results['total_computed']} computed, {results['total_failed']} failed"
            )
            
            return results
            
        except Exception as e:
            logger.error(
                f"Error computing analyses for cycle {cycle}: {e}",
                exc_info=True
            )
            results['error'] = str(e)
            return results
    
    async def compute_analyses_for_candidate(
        self,
        candidate_id: str,
        cycles: Optional[List[int]] = None,
        analysis_types: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Compute analyses for a specific candidate.
        
        Args:
            candidate_id: The candidate ID
            cycles: Optional list of cycles to compute (default: all cycles with data)
            analysis_types: Optional list of analysis types to compute (default: all)
        
        Returns:
            Dict with computation results
        """
        if not analysis_types:
            analysis_types = ['employer', 'velocity', 'donor_states']
        
        results = {
            'candidate_id': candidate_id,
            'cycles': {},
            'total_computed': 0,
            'total_failed': 0
        }
        
        try:
            # Get cycles for this candidate if not provided
            # Extract cycles from contribution dates
            if not cycles:
                async with AsyncSessionLocal() as session:
                    query = select(Contribution.contribution_date).where(
                        Contribution.candidate_id == candidate_id
                    ).distinct()
                    
                    result = await session.execute(query)
                    dates = [row[0] for row in result if row[0]]
                    
                    # Extract unique cycles from dates
                    # Cycle is the even year in the two-year period
                    cycles_set = set()
                    for date in dates:
                        if date:
                            year = date.year
                            # Cycle is the even year (or next even year if odd)
                            cycle = (year // 2) * 2
                            if year % 2 == 1:
                                cycle += 2  # Odd years belong to next cycle
                            cycles_set.add(cycle)
                    cycles = sorted(list(cycles_set))
            
            if not cycles:
                logger.debug(f"No cycles found for candidate {candidate_id}")
                return results
            
            logger.info(
                f"Computing analyses for candidate {candidate_id}: "
                f"{len(cycles)} cycles, types={analysis_types}"
            )
            
            # Compute analyses for each cycle
            for cycle in cycles:
                cycle_results = {
                    'computed': 0,
                    'failed': 0
                }
                
                for analysis_type in analysis_types:
                    try:
                        result = await self._computation_service.compute_and_store_analysis(
                            analysis_type=analysis_type,
                            candidate_id=candidate_id,
                            cycle=cycle
                        )
                        
                        if result:
                            cycle_results['computed'] += 1
                            results['total_computed'] += 1
                        else:
                            cycle_results['failed'] += 1
                            results['total_failed'] += 1
                            
                    except Exception as e:
                        logger.error(
                            f"Error computing {analysis_type} for candidate {candidate_id}, "
                            f"cycle {cycle}: {e}",
                            exc_info=True
                        )
                        cycle_results['failed'] += 1
                        results['total_failed'] += 1
                
                results['cycles'][cycle] = cycle_results
            
            logger.info(
                f"Completed analysis computation for candidate {candidate_id}: "
                f"{results['total_computed']} computed, {results['total_failed']} failed"
            )
            
            return results
            
        except Exception as e:
            logger.error(
                f"Error computing analyses for candidate {candidate_id}: {e}",
                exc_info=True
            )
            results['error'] = str(e)
            return results
    
    async def _run_analysis_job(
        self,
        job_id: str,
        cycle: int,
        affected_candidates: Optional[List[str]] = None
    ) -> None:
        """Run an analysis computation job in the background"""
        self._running_jobs.add(job_id)
        
        try:
            # Update job status to running
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(AnalysisComputationJob).where(
                        AnalysisComputationJob.id == job_id
                    )
                )
                job = result.scalar_one_or_none()
                
                if not job:
                    logger.warning(f"Analysis job {job_id} not found in database")
                    return
                
                job.status = 'running'
                job.started_at = datetime.utcnow()
                await session.commit()
            
            logger.info(f"Starting analysis computation job {job_id} for cycle {cycle}")
            
            # Compute analyses
            if affected_candidates:
                # Compute for specific candidates
                for candidate_id in affected_candidates:
                    await self.compute_analyses_for_candidate(
                        candidate_id=candidate_id,
                        cycles=[cycle],
                        analysis_types=['employer', 'velocity', 'donor_states']
                    )
            else:
                # Compute for entire cycle
                await self.compute_analyses_for_cycle(
                    cycle=cycle,
                    analysis_types=['employer', 'velocity', 'donor_states']
                )
            
            # Update job status to completed
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(AnalysisComputationJob).where(
                        AnalysisComputationJob.id == job_id
                    )
                )
                job = result.scalar_one_or_none()
                
                if job:
                    job.status = 'completed'
                    job.completed_at = datetime.utcnow()
                    await session.commit()
            
            logger.info(f"Completed analysis computation job {job_id}")
            
        except Exception as e:
            logger.error(
                f"Error in analysis computation job {job_id}: {e}",
                exc_info=True
            )
            
            # Update job status to failed
            try:
                async with AsyncSessionLocal() as session:
                    result = await session.execute(
                        select(AnalysisComputationJob).where(
                            AnalysisComputationJob.id == job_id
                        )
                    )
                    job = result.scalar_one_or_none()
                    
                    if job:
                        job.status = 'failed'
                        job.error_message = str(e)
                        job.completed_at = datetime.utcnow()
                        await session.commit()
            except Exception as update_error:
                logger.error(
                    f"Error updating job status to failed: {update_error}",
                    exc_info=True
                )
        
        finally:
            self._running_jobs.discard(job_id)

