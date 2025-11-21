"""Analysis computation service for pre-computing and storing analysis results"""
import logging
import json
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import AsyncSessionLocal, PreComputedAnalysis
from app.services.fec_client import FECClient
from app.services.analysis.contribution_analysis import ContributionAnalysisService
from app.services.analysis.donor_analysis import DonorAnalysisService
from app.models.schemas import (
    EmployerAnalysis, ContributionVelocity, DonorStateAnalysis
)
from app.config import config

logger = logging.getLogger(__name__)


class AnalysisComputationService:
    """Service for computing, storing, and retrieving pre-computed analysis results"""
    
    def __init__(self, fec_client: FECClient):
        self.fec_client = fec_client
        self._contribution_service = ContributionAnalysisService(fec_client)
        self._donor_service = DonorAnalysisService(fec_client)
    
    async def compute_and_store_analysis(
        self,
        analysis_type: str,
        candidate_id: Optional[str] = None,
        cycle: Optional[int] = None,
        committee_id: Optional[str] = None,
        force_recompute: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Compute an analysis and store the result in the database.
        
        Args:
            analysis_type: Type of analysis ('donor_states', 'employer', 'velocity')
            candidate_id: Optional candidate ID
            cycle: Optional cycle year
            committee_id: Optional committee ID
            force_recompute: If True, recompute even if result exists
        
        Returns:
            The computed analysis result as a dict, or None if computation failed
        """
        if not config.ENABLE_PRECOMPUTED_ANALYSIS:
            logger.debug("Pre-computed analysis is disabled, skipping computation")
            return None
        
        try:
            # Check if result already exists and is fresh
            if not force_recompute:
                existing = await self.get_precomputed_analysis(
                    analysis_type, candidate_id, cycle, committee_id
                )
                if existing and not self._is_stale(existing):
                    logger.debug(
                        f"Analysis {analysis_type} already exists and is fresh, skipping computation"
                    )
                    return existing.get('result_data')
            
            # Compute the analysis
            logger.info(
                f"Computing {analysis_type} analysis for "
                f"candidate_id={candidate_id}, cycle={cycle}, committee_id={committee_id}"
            )
            
            result = None
            if analysis_type == 'employer':
                result = await self._contribution_service.analyze_by_employer(
                    candidate_id=candidate_id,
                    committee_id=committee_id,
                    cycle=cycle
                )
            elif analysis_type == 'velocity':
                result = await self._contribution_service.analyze_velocity(
                    candidate_id=candidate_id,
                    committee_id=committee_id,
                    cycle=cycle
                )
            elif analysis_type == 'donor_states':
                if not candidate_id:
                    logger.warning("donor_states analysis requires candidate_id")
                    return None
                result = await self._donor_service.analyze_donor_states(
                    candidate_id=candidate_id,
                    cycle=cycle
                )
            else:
                logger.error(f"Unknown analysis type: {analysis_type}")
                return None
            
            if result is None:
                logger.warning(f"Analysis computation returned None for {analysis_type}")
                return None
            
            # Convert Pydantic model to dict for JSON storage
            if hasattr(result, 'model_dump'):
                result_dict = result.model_dump()
            elif hasattr(result, 'dict'):
                result_dict = result.dict()
            else:
                result_dict = result
            
            # Store the result
            await self._store_analysis(
                analysis_type=analysis_type,
                candidate_id=candidate_id,
                cycle=cycle,
                committee_id=committee_id,
                result_data=result_dict
            )
            
            logger.info(
                f"Successfully computed and stored {analysis_type} analysis for "
                f"candidate_id={candidate_id}, cycle={cycle}"
            )
            
            return result_dict
            
        except Exception as e:
            logger.error(
                f"Error computing {analysis_type} analysis: {e}",
                exc_info=True
            )
            return None
    
    async def get_precomputed_analysis(
        self,
        analysis_type: str,
        candidate_id: Optional[str] = None,
        cycle: Optional[int] = None,
        committee_id: Optional[str] = None,
        allow_stale: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a pre-computed analysis result from the database.
        
        Args:
            analysis_type: Type of analysis ('donor_states', 'employer', 'velocity')
            candidate_id: Optional candidate ID
            cycle: Optional cycle year
            committee_id: Optional committee ID
            allow_stale: If True, return result even if stale
        
        Returns:
            Dict with keys 'result_data' and 'computed_at', or None if not found
        """
        if not config.ENABLE_PRECOMPUTED_ANALYSIS:
            return None
        
        try:
            async with AsyncSessionLocal() as session:
                # Build query conditions
                conditions = [PreComputedAnalysis.analysis_type == analysis_type]
                
                if candidate_id:
                    conditions.append(PreComputedAnalysis.candidate_id == candidate_id)
                else:
                    conditions.append(PreComputedAnalysis.candidate_id.is_(None))
                
                if committee_id:
                    conditions.append(PreComputedAnalysis.committee_id == committee_id)
                else:
                    conditions.append(PreComputedAnalysis.committee_id.is_(None))
                
                if cycle:
                    conditions.append(PreComputedAnalysis.cycle == cycle)
                else:
                    conditions.append(PreComputedAnalysis.cycle.is_(None))
                
                query = select(PreComputedAnalysis).where(and_(*conditions)).order_by(
                    PreComputedAnalysis.computed_at.desc()
                )
                result = await session.execute(query)
                all_analyses = result.scalars().all()
                
                if not all_analyses:
                    return None
                
                # Get the most recent one
                analysis = all_analyses[0]
                
                # If there are duplicates, log a warning (but don't delete here to avoid transaction issues)
                if len(all_analyses) > 1:
                    logger.warning(
                        f"Found {len(all_analyses)} duplicate pre-computed {analysis_type} analyses "
                        f"(candidate_id={candidate_id}, cycle={cycle}, committee_id={committee_id}). "
                        f"Using the most recent one. Consider running cleanup to remove duplicates."
                    )
                
                # Check if stale
                if not allow_stale and self._is_stale(analysis):
                    logger.debug(
                        f"Pre-computed {analysis_type} analysis is stale, will recompute"
                    )
                    return None
                
                return {
                    'result_data': analysis.result_data,
                    'computed_at': analysis.computed_at,
                    'last_updated': analysis.last_updated,
                    'data_version': analysis.data_version
                }
                
        except Exception as e:
            logger.error(
                f"Error retrieving pre-computed {analysis_type} analysis: {e}",
                exc_info=True
            )
            return None
    
    async def cleanup_duplicates(
        self,
        analysis_type: Optional[str] = None
    ) -> int:
        """
        Clean up duplicate pre-computed analyses, keeping only the most recent one for each unique combination.
        
        Args:
            analysis_type: Optional analysis type to clean (if None, cleans all types)
        
        Returns:
            Number of duplicate entries removed
        """
        try:
            async with AsyncSessionLocal() as session:
                # Build base query
                if analysis_type:
                    base_query = select(PreComputedAnalysis).where(
                        PreComputedAnalysis.analysis_type == analysis_type
                    )
                else:
                    base_query = select(PreComputedAnalysis)
                
                # Get all analyses grouped by their unique key
                all_analyses = (await session.execute(base_query)).scalars().all()
                
                # Group by unique combination
                seen = {}
                duplicates_to_delete = []
                
                for analysis in all_analyses:
                    key = (
                        analysis.analysis_type,
                        analysis.candidate_id,
                        analysis.committee_id,
                        analysis.cycle
                    )
                    
                    if key in seen:
                        # Compare timestamps - keep the newer one
                        existing = seen[key]
                        if analysis.computed_at > existing.computed_at:
                            duplicates_to_delete.append(existing)
                            seen[key] = analysis
                        else:
                            duplicates_to_delete.append(analysis)
                    else:
                        seen[key] = analysis
                
                # Delete duplicates
                for dup in duplicates_to_delete:
                    await session.delete(dup)
                
                await session.commit()
                
                if duplicates_to_delete:
                    logger.info(
                        f"Cleaned up {len(duplicates_to_delete)} duplicate pre-computed analyses"
                        + (f" for type {analysis_type}" if analysis_type else "")
                    )
                
                return len(duplicates_to_delete)
                
        except Exception as e:
            logger.error(
                f"Error cleaning up duplicate analyses: {e}",
                exc_info=True
            )
            return 0
    
    async def invalidate_analysis(
        self,
        analysis_type: str,
        candidate_id: Optional[str] = None,
        cycle: Optional[int] = None,
        committee_id: Optional[str] = None
    ) -> bool:
        """
        Mark an analysis as stale by deleting it from the database.
        
        Args:
            analysis_type: Type of analysis
            candidate_id: Optional candidate ID
            cycle: Optional cycle year
            committee_id: Optional committee ID
        
        Returns:
            True if analysis was invalidated, False otherwise
        """
        try:
            async with AsyncSessionLocal() as session:
                conditions = [PreComputedAnalysis.analysis_type == analysis_type]
                
                if candidate_id:
                    conditions.append(PreComputedAnalysis.candidate_id == candidate_id)
                else:
                    conditions.append(PreComputedAnalysis.candidate_id.is_(None))
                
                if committee_id:
                    conditions.append(PreComputedAnalysis.committee_id == committee_id)
                else:
                    conditions.append(PreComputedAnalysis.committee_id.is_(None))
                
                if cycle:
                    conditions.append(PreComputedAnalysis.cycle == cycle)
                else:
                    conditions.append(PreComputedAnalysis.cycle.is_(None))
                
                query = select(PreComputedAnalysis).where(and_(*conditions))
                result = await session.execute(query)
                analysis = result.scalar_one_or_none()
                
                if analysis:
                    await session.delete(analysis)
                    await session.commit()
                    logger.info(
                        f"Invalidated {analysis_type} analysis for "
                        f"candidate_id={candidate_id}, cycle={cycle}"
                    )
                    return True
                
                return False
                
        except Exception as e:
            logger.error(
                f"Error invalidating {analysis_type} analysis: {e}",
                exc_info=True
            )
            return False
    
    async def update_analysis_incremental(
        self,
        analysis_type: str,
        new_contributions: List[Dict[str, Any]],
        candidate_id: Optional[str] = None,
        cycle: Optional[int] = None,
        committee_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Update an analysis incrementally with new contributions.
        
        Only supports 'employer' and 'velocity' analyses.
        'donor_states' requires full recomputation.
        
        Args:
            analysis_type: Type of analysis ('employer' or 'velocity')
            new_contributions: List of new contribution dicts
            candidate_id: Optional candidate ID
            cycle: Optional cycle year
            committee_id: Optional committee ID
        
        Returns:
            Updated analysis result, or None if update failed
        """
        if analysis_type == 'donor_states':
            logger.warning("donor_states analysis does not support incremental updates, recomputing")
            return await self.compute_and_store_analysis(
                analysis_type, candidate_id, cycle, committee_id, force_recompute=True
            )
        
        if not new_contributions:
            logger.debug("No new contributions provided for incremental update")
            return None
        
        try:
            # Get existing analysis
            existing = await self.get_precomputed_analysis(
                analysis_type, candidate_id, cycle, committee_id, allow_stale=True
            )
            
            if not existing:
                # No existing analysis, compute from scratch
                logger.debug("No existing analysis found, computing from scratch")
                return await self.compute_and_store_analysis(
                    analysis_type, candidate_id, cycle, committee_id
                )
            
            # Update incrementally
            if analysis_type == 'employer':
                updated_result = self._update_employer_analysis_incremental(
                    new_contributions, existing['result_data']
                )
            elif analysis_type == 'velocity':
                updated_result = self._update_velocity_incremental(
                    new_contributions, existing['result_data']
                )
            else:
                logger.error(f"Incremental update not supported for {analysis_type}")
                return None
            
            # Store updated result
            await self._store_analysis(
                analysis_type=analysis_type,
                candidate_id=candidate_id,
                cycle=cycle,
                committee_id=committee_id,
                result_data=updated_result
            )
            
            logger.info(
                f"Successfully updated {analysis_type} analysis incrementally for "
                f"candidate_id={candidate_id}, cycle={cycle}"
            )
            
            return updated_result
            
        except Exception as e:
            logger.error(
                f"Error updating {analysis_type} analysis incrementally: {e}",
                exc_info=True
            )
            # Fall back to full recomputation
            logger.info("Falling back to full recomputation")
            return await self.compute_and_store_analysis(
                analysis_type, candidate_id, cycle, committee_id, force_recompute=True
            )
    
    def _update_employer_analysis_incremental(
        self,
        new_contributions: List[Dict[str, Any]],
        existing_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update employer analysis with new contributions"""
        # Extract employer totals from new contributions
        new_employer_totals = {}
        new_total_contributions = 0.0
        
        for contrib in new_contributions:
            employer = contrib.get('contributor_employer') or 'Unknown Employer'
            amount = float(contrib.get('contribution_amount', 0) or 0)
            
            if employer not in new_employer_totals:
                new_employer_totals[employer] = {'total': 0.0, 'count': 0}
            
            new_employer_totals[employer]['total'] += amount
            new_employer_totals[employer]['count'] += 1
            new_total_contributions += amount
        
        # Merge with existing analysis
        updated_total_by_employer = existing_analysis.get('total_by_employer', {}).copy()
        updated_top_employers = existing_analysis.get('top_employers', []).copy()
        
        # Update totals
        for employer, data in new_employer_totals.items():
            if employer in updated_total_by_employer:
                updated_total_by_employer[employer] += data['total']
            else:
                updated_total_by_employer[employer] = data['total']
        
        # Rebuild top employers list
        top_employers_dict = {}
        for emp in updated_top_employers:
            top_employers_dict[emp.get('employer', '')] = {
                'employer': emp.get('employer', ''),
                'total': emp.get('total', 0.0),
                'count': emp.get('count', 0)
            }
        
        # Update with new data
        for employer, data in new_employer_totals.items():
            if employer in top_employers_dict:
                top_employers_dict[employer]['total'] += data['total']
                top_employers_dict[employer]['count'] += data['count']
            else:
                top_employers_dict[employer] = {
                    'employer': employer,
                    'total': data['total'],
                    'count': data['count']
                }
        
        # Sort and take top 50
        updated_top_employers = sorted(
            top_employers_dict.values(),
            key=lambda x: x['total'],
            reverse=True
        )[:50]
        
        return {
            'total_by_employer': updated_total_by_employer,
            'top_employers': updated_top_employers,
            'employer_count': len(updated_total_by_employer),
            'total_contributions': existing_analysis.get('total_contributions', 0.0) + new_total_contributions
        }
    
    def _update_velocity_incremental(
        self,
        new_contributions: List[Dict[str, Any]],
        existing_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update velocity analysis with new contributions"""
        from datetime import datetime
        
        # Extract velocity data from new contributions
        new_velocity_by_date = {}
        new_velocity_by_week = {}
        new_peak_days = []
        
        for contrib in new_contributions:
            date_str = contrib.get('contribution_date')
            amount = float(contrib.get('contribution_amount', 0) or 0)
            
            if not date_str or amount <= 0:
                continue
            
            try:
                # Parse date
                if isinstance(date_str, str):
                    date_obj = datetime.strptime(date_str[:10], "%Y-%m-%d")
                else:
                    date_obj = date_str
                
                date_key = date_obj.strftime("%Y-%m-%d")
                
                # Update daily velocity
                if date_key not in new_velocity_by_date:
                    new_velocity_by_date[date_key] = 0.0
                new_velocity_by_date[date_key] += amount
                
                # Update weekly velocity
                week_key = date_obj.strftime("%Y-W%W")
                if week_key not in new_velocity_by_week:
                    new_velocity_by_week[week_key] = 0.0
                new_velocity_by_week[week_key] += amount
                
            except (ValueError, TypeError) as e:
                logger.debug(f"Could not parse date {date_str}: {e}")
                continue
        
        # Merge with existing analysis
        updated_velocity_by_date = existing_analysis.get('velocity_by_date', {}).copy()
        updated_velocity_by_week = existing_analysis.get('velocity_by_week', {}).copy()
        
        # Update daily velocity
        for date_key, amount in new_velocity_by_date.items():
            if date_key in updated_velocity_by_date:
                updated_velocity_by_date[date_key] += amount
            else:
                updated_velocity_by_date[date_key] = amount
        
        # Update weekly velocity
        for week_key, amount in new_velocity_by_week.items():
            if week_key in updated_velocity_by_week:
                updated_velocity_by_week[week_key] += amount
            else:
                updated_velocity_by_week[week_key] = amount
        
        # Recalculate peak days (top 10 by amount)
        peak_days_list = [
            {'date': date, 'amount': amount}
            for date, amount in updated_velocity_by_date.items()
        ]
        peak_days_list.sort(key=lambda x: x['amount'], reverse=True)
        updated_peak_days = peak_days_list[:10]
        
        # Recalculate average daily velocity
        if updated_velocity_by_date:
            updated_average_daily_velocity = sum(updated_velocity_by_date.values()) / len(updated_velocity_by_date)
        else:
            updated_average_daily_velocity = 0.0
        
        return {
            'velocity_by_date': updated_velocity_by_date,
            'velocity_by_week': updated_velocity_by_week,
            'peak_days': updated_peak_days,
            'average_daily_velocity': updated_average_daily_velocity
        }
    
    async def _store_analysis(
        self,
        analysis_type: str,
        result_data: Dict[str, Any],
        candidate_id: Optional[str] = None,
        cycle: Optional[int] = None,
        committee_id: Optional[str] = None
    ) -> None:
        """Store analysis result in the database"""
        try:
            async with AsyncSessionLocal() as session:
                # Check if analysis already exists
                conditions = [PreComputedAnalysis.analysis_type == analysis_type]
                
                if candidate_id:
                    conditions.append(PreComputedAnalysis.candidate_id == candidate_id)
                else:
                    conditions.append(PreComputedAnalysis.candidate_id.is_(None))
                
                if committee_id:
                    conditions.append(PreComputedAnalysis.committee_id == committee_id)
                else:
                    conditions.append(PreComputedAnalysis.committee_id.is_(None))
                
                if cycle:
                    conditions.append(PreComputedAnalysis.cycle == cycle)
                else:
                    conditions.append(PreComputedAnalysis.cycle.is_(None))
                
                query = select(PreComputedAnalysis).where(and_(*conditions)).order_by(
                    PreComputedAnalysis.computed_at.desc()
                )
                result = await session.execute(query)
                all_results = result.scalars().all()
                
                if all_results:
                    # Get the most recent one
                    existing = all_results[0]
                    
                    # If there are multiple entries, delete the older ones to prevent duplicates
                    if len(all_results) > 1:
                        logger.warning(
                            f"Found {len(all_results)} duplicate pre-computed {analysis_type} analyses "
                            f"(candidate_id={candidate_id}, cycle={cycle}, committee_id={committee_id}). "
                            f"Keeping the most recent one and removing {len(all_results) - 1} duplicate(s)."
                        )
                        # Delete all but the most recent
                        for dup in all_results[1:]:
                            await session.delete(dup)
                        await session.commit()
                    
                    if existing:
                    # Update existing
                    existing.result_data = result_data
                    existing.last_updated = datetime.utcnow()
                    existing.data_version += 1
                else:
                    # Create new
                    new_analysis = PreComputedAnalysis(
                        analysis_type=analysis_type,
                        candidate_id=candidate_id,
                        committee_id=committee_id,
                        cycle=cycle,
                        result_data=result_data,
                        computed_at=datetime.utcnow(),
                        last_updated=datetime.utcnow(),
                        data_version=1
                    )
                    session.add(new_analysis)
                
                await session.commit()
                
        except Exception as e:
            logger.error(
                f"Error storing {analysis_type} analysis: {e}",
                exc_info=True
            )
            raise
    
    def _is_stale(self, analysis: PreComputedAnalysis) -> bool:
        """Check if an analysis result is stale"""
        if not analysis.computed_at:
            return True
        
        threshold = timedelta(hours=config.ANALYSIS_STALE_THRESHOLD_HOURS)
        age = datetime.utcnow() - analysis.computed_at
        
        return age > threshold

