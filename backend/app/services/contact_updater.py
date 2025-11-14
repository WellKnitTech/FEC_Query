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
        self.stale_threshold_days = int(os.getenv("CONTACT_INFO_STALE_THRESHOLD_DAYS", "14"))  # 14 days (2 weeks) default
        # Only refresh contact info if it's very old (1 year) - contact info doesn't change much
        self.very_old_threshold_days = int(os.getenv("CONTACT_INFO_VERY_OLD_THRESHOLD_DAYS", "365"))  # 1 year default
        # Only run on startup if explicitly enabled (default: False)
        self.run_on_startup = os.getenv("CONTACT_INFO_RUN_ON_STARTUP", "false").lower() == "true"
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
                # Find candidates with missing contact info that are very old
                # Don't refresh if contact info exists (even if stale) - preserve bulk import data
                # Only refresh if updated_at is very old (1+ year) - contact info doesn't change much
                # If updated_at is None, only refresh if created_at is also very old (candidate was created long ago)
                cutoff_date = datetime.utcnow() - timedelta(days=self.very_old_threshold_days)
                query = select(Candidate).where(
                    and_(
                        Candidate.street_address.is_(None),
                        Candidate.city.is_(None),
                        Candidate.zip.is_(None),
                        Candidate.email.is_(None),
                        Candidate.phone.is_(None),
                        Candidate.website.is_(None),
                        # Only refresh if:
                        # 1. updated_at exists and is very old (1+ year), OR
                        # 2. updated_at is None AND created_at is very old (candidate was created long ago and never checked)
                        or_(
                            and_(
                                Candidate.updated_at.isnot(None),
                                Candidate.updated_at < cutoff_date
                            ),
                            and_(
                                Candidate.updated_at.is_(None),
                                Candidate.created_at < cutoff_date
                            )
                        )
                    )
                )
                
                if max_records:
                    query = query.limit(max_records)
                
                result = await session.execute(query)
                candidates = result.scalars().all()
                results["checked"] = len(candidates)
                
                if len(candidates) > 0:
                    logger.info(f"Found {len(candidates)} candidates needing contact info refresh (missing contact info and not checked in {self.very_old_threshold_days} days)")
                else:
                    logger.debug("No candidates need contact info refresh (all have contact info or were recently checked)")
                
                # Process in batches
                for i in range(0, len(candidates), self.batch_size):
                    batch = candidates[i:i + self.batch_size]
                    for candidate in batch:
                        try:
                            # Refresh contact info (only if missing)
                            refreshed = await self.fec_client.refresh_candidate_contact_info_if_needed(
                                candidate.candidate_id
                            )
                            if refreshed:
                                results["updated"] += 1
                            
                            # Delay to respect rate limits (500ms between requests)
                            await asyncio.sleep(0.5)
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
                # Find committees with missing contact info that are very old
                # Don't refresh if contact info exists (even if stale) - preserve bulk import data
                # Only refresh if updated_at is very old (1+ year) - contact info doesn't change much
                # If updated_at is None, only refresh if created_at is also very old (committee was created long ago and never checked)
                cutoff_date = datetime.utcnow() - timedelta(days=self.very_old_threshold_days)
                query = select(Committee).where(
                    and_(
                        Committee.street_address.is_(None),
                        Committee.city.is_(None),
                        Committee.zip.is_(None),
                        Committee.email.is_(None),
                        Committee.phone.is_(None),
                        Committee.website.is_(None),
                        Committee.treasurer_name.is_(None),
                        # Only refresh if:
                        # 1. updated_at exists and is very old (1+ year), OR
                        # 2. updated_at is None AND created_at is very old (committee was created long ago and never checked)
                        or_(
                            and_(
                                Committee.updated_at.isnot(None),
                                Committee.updated_at < cutoff_date
                            ),
                            and_(
                                Committee.updated_at.is_(None),
                                Committee.created_at < cutoff_date
                            )
                        )
                    )
                )
                
                if max_records:
                    query = query.limit(max_records)
                
                result = await session.execute(query)
                committees = result.scalars().all()
                results["checked"] = len(committees)
                
                if len(committees) > 0:
                    logger.info(f"Found {len(committees)} committees needing contact info refresh (missing contact info and not checked in {self.very_old_threshold_days} days)")
                else:
                    logger.debug("No committees need contact info refresh (all have contact info or were recently checked)")
                
                # Process in batches
                for i in range(0, len(committees), self.batch_size):
                    batch = committees[i:i + self.batch_size]
                    for committee in batch:
                        try:
                            # Refresh contact info (only if missing)
                            refreshed = await self.fec_client.refresh_committee_contact_info_if_needed(
                                committee.committee_id
                            )
                            if refreshed:
                                results["updated"] += 1
                            
                            # Delay to respect rate limits (500ms between requests)
                            await asyncio.sleep(0.5)
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
                logger.debug("Contact updater task was cancelled")
        logger.info("Background contact info updater stopped")
    
    async def _update_loop(self):
        """Background loop for periodic updates"""
        # Don't run immediately on startup unless explicitly enabled
        if not self.run_on_startup:
            logger.info(f"Contact updater will start refreshing after {self.update_interval_hours} hours (skipping startup refresh)")
            if self._running:
                await asyncio.sleep(self.update_interval_hours * 3600)
        else:
            logger.info("Running contact info refresh on startup (CONTACT_INFO_RUN_ON_STARTUP=true)")
        
        while self._running:
            try:
                logger.info("Running scheduled contact info refresh...")
                await self.refresh_stale_contact_info()
            except Exception as e:
                logger.error(f"Error in background update loop: {e}", exc_info=True)
            
            # Wait for next update interval
            if self._running:
                logger.info(f"Next contact info refresh in {self.update_interval_hours} hours")
                await asyncio.sleep(self.update_interval_hours * 3600)
    
    async def get_status(self) -> Dict:
        """Get current status of contact updater"""
        return {
            "running": self._running,
            "update_interval_hours": self.update_interval_hours,
            "stale_threshold_days": self.stale_threshold_days,
            "batch_size": self.batch_size
        }

