import asyncio
import os
import logging
from typing import Optional, Dict, List
from datetime import datetime, timedelta
from app.services.fec_client import FECClient
from app.db.database import AsyncSessionLocal, Candidate, Committee
from sqlalchemy import select, and_, or_

logger = logging.getLogger(__name__)


class ContactUpdaterService:
    """Background service for updating contact information for candidates and committees"""
    
    def __init__(self):
        self.fec_client = FECClient()
        self.update_interval_hours = int(os.getenv("CONTACT_INFO_UPDATE_INTERVAL_HOURS", "168"))  # 7 days default
        self.stale_threshold_days = int(os.getenv("CONTACT_INFO_STALE_THRESHOLD_DAYS", "30"))  # 30 days default
        self.batch_size = int(os.getenv("CONTACT_INFO_BATCH_SIZE", "50"))  # 50 per batch
        self._running = False
        self._task = None
    
    async def refresh_stale_contact_info(
        self,
        max_records: Optional[int] = None
    ) -> Dict:
        """Find and refresh contact info for candidates/committees with missing or stale contact info"""
        logger.info("Starting contact info refresh...")
        
        results = {
            "candidates_checked": 0,
            "candidates_updated": 0,
            "committees_checked": 0,
            "committees_updated": 0,
            "errors": []
        }
        
        try:
            # Refresh candidates
            candidate_results = await self._refresh_candidate_contact_info(max_records)
            results["candidates_checked"] = candidate_results["checked"]
            results["candidates_updated"] = candidate_results["updated"]
            results["errors"].extend(candidate_results.get("errors", []))
            
            # Refresh committees
            committee_results = await self._refresh_committee_contact_info(max_records)
            results["committees_checked"] = committee_results["checked"]
            results["committees_updated"] = committee_results["updated"]
            results["errors"].extend(committee_results.get("errors", []))
            
            logger.info(
                f"Contact info refresh complete: "
                f"{results['candidates_updated']}/{results['candidates_checked']} candidates, "
                f"{results['committees_updated']}/{results['committees_checked']} committees updated"
            )
        except Exception as e:
            logger.error(f"Error in refresh_stale_contact_info: {e}", exc_info=True)
            results["errors"].append(str(e))
        
        return results
    
    async def _refresh_candidate_contact_info(self, max_records: Optional[int] = None) -> Dict:
        """Refresh contact info for candidates that need it"""
        results = {
            "checked": 0,
            "updated": 0,
            "errors": []
        }
        
        try:
            async with AsyncSessionLocal() as session:
                # Find candidates with missing or stale contact info
                cutoff_date = datetime.utcnow() - timedelta(days=self.stale_threshold_days)
                
                # Query candidates where:
                # 1. All contact fields are NULL, OR
                # 2. updated_at is older than threshold
                query = select(Candidate).where(
                    or_(
                        and_(
                            Candidate.street_address.is_(None),
                            Candidate.city.is_(None),
                            Candidate.zip.is_(None),
                            Candidate.email.is_(None),
                            Candidate.phone.is_(None),
                            Candidate.website.is_(None)
                        ),
                        Candidate.updated_at < cutoff_date
                    )
                )
                
                if max_records:
                    query = query.limit(max_records)
                
                result = await session.execute(query)
                candidates = result.scalars().all()
                results["checked"] = len(candidates)
                
                logger.info(f"Found {len(candidates)} candidates needing contact info refresh")
                
                # Process in batches
                for i in range(0, len(candidates), self.batch_size):
                    batch = candidates[i:i + self.batch_size]
                    for candidate in batch:
                        try:
                            # Refresh contact info
                            refreshed = await self.fec_client.refresh_candidate_contact_info_if_needed(
                                candidate.candidate_id,
                                stale_threshold_days=self.stale_threshold_days
                            )
                            if refreshed:
                                results["updated"] += 1
                            
                            # Small delay to respect rate limits
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            logger.warning(f"Error refreshing contact info for candidate {candidate.candidate_id}: {e}")
                            results["errors"].append(f"Candidate {candidate.candidate_id}: {str(e)}")
                    
                    # Delay between batches
                    if i + self.batch_size < len(candidates):
                        await asyncio.sleep(1)
        
        except Exception as e:
            logger.error(f"Error in _refresh_candidate_contact_info: {e}", exc_info=True)
            results["errors"].append(str(e))
        
        return results
    
    async def _refresh_committee_contact_info(self, max_records: Optional[int] = None) -> Dict:
        """Refresh contact info for committees that need it"""
        results = {
            "checked": 0,
            "updated": 0,
            "errors": []
        }
        
        try:
            async with AsyncSessionLocal() as session:
                # Find committees with missing or stale contact info
                cutoff_date = datetime.utcnow() - timedelta(days=self.stale_threshold_days)
                
                # Query committees where:
                # 1. All contact fields are NULL, OR
                # 2. updated_at is older than threshold
                query = select(Committee).where(
                    or_(
                        and_(
                            Committee.street_address.is_(None),
                            Committee.city.is_(None),
                            Committee.zip.is_(None),
                            Committee.email.is_(None),
                            Committee.phone.is_(None),
                            Committee.website.is_(None),
                            Committee.treasurer_name.is_(None)
                        ),
                        Committee.updated_at < cutoff_date
                    )
                )
                
                if max_records:
                    query = query.limit(max_records)
                
                result = await session.execute(query)
                committees = result.scalars().all()
                results["checked"] = len(committees)
                
                logger.info(f"Found {len(committees)} committees needing contact info refresh")
                
                # Process in batches
                for i in range(0, len(committees), self.batch_size):
                    batch = committees[i:i + self.batch_size]
                    for committee in batch:
                        try:
                            # Refresh contact info
                            refreshed = await self.fec_client.refresh_committee_contact_info_if_needed(
                                committee.committee_id,
                                stale_threshold_days=self.stale_threshold_days
                            )
                            if refreshed:
                                results["updated"] += 1
                            
                            # Small delay to respect rate limits
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            logger.warning(f"Error refreshing contact info for committee {committee.committee_id}: {e}")
                            results["errors"].append(f"Committee {committee.committee_id}: {str(e)}")
                    
                    # Delay between batches
                    if i + self.batch_size < len(committees):
                        await asyncio.sleep(1)
        
        except Exception as e:
            logger.error(f"Error in _refresh_committee_contact_info: {e}", exc_info=True)
            results["errors"].append(str(e))
        
        return results
    
    async def start_background_updates(self):
        """Start background task for periodic updates"""
        if self._running:
            logger.warning("Contact updater is already running")
            return
        
        self._running = True
        self._task = asyncio.create_task(self._update_loop())
        logger.info("Background contact info updater started")
    
    async def stop_background_updates(self):
        """Stop background update task"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Background contact info updater stopped")
    
    async def _update_loop(self):
        """Background loop for periodic updates"""
        while self._running:
            try:
                await self.refresh_stale_contact_info()
            except Exception as e:
                logger.error(f"Error in background update loop: {e}", exc_info=True)
            
            # Wait for next update interval
            if self._running:
                await asyncio.sleep(self.update_interval_hours * 3600)
    
    async def get_status(self) -> Dict:
        """Get current status of contact updater"""
        return {
            "running": self._running,
            "update_interval_hours": self.update_interval_hours,
            "stale_threshold_days": self.stale_threshold_days,
            "batch_size": self.batch_size
        }

