"""
Service for fetching contributions from FEC API and storing them in the database.

This service handles:
- Fetching all contributions from FEC API with pagination
- Storing contributions in database (avoiding duplicates)
- Triggering analysis refresh after storing new data
"""
import logging
import asyncio
from typing import Optional, List, Dict, Any
from app.services.fec_client import FECClient
from app.services.analysis import AnalysisService

logger = logging.getLogger(__name__)


class ContributionFetcherService:
    """Service for fetching contributions from FEC API"""
    
    def __init__(self, fec_client: FECClient, analysis_service: Optional[AnalysisService] = None):
        """
        Initialize the contribution fetcher service.
        
        Args:
            fec_client: FEC client for API calls
            analysis_service: Optional analysis service for triggering reanalysis
        """
        self.fec_client = fec_client
        self.analysis_service = analysis_service
    
    async def fetch_all_contributions(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        cycle: Optional[int] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch all contributions from FEC API using pagination.
        
        Args:
            candidate_id: Candidate ID to fetch contributions for
            committee_id: Committee ID to fetch contributions for
            cycle: Election cycle (two_year_transaction_period)
            min_date: Minimum date filter
            max_date: Maximum date filter
            
        Returns:
            List of all contribution dictionaries from API
        """
        logger.info(
            f"Fetching all contributions from API: "
            f"candidate_id={candidate_id}, committee_id={committee_id}, cycle={cycle}"
        )
        
        # Use the FEC client's get_contributions with fetch_all=True
        # This will fetch all pages automatically
        all_contributions = await self.fec_client.get_contributions(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date,
            limit=999999,  # Very large limit to get all
            two_year_transaction_period=cycle,
            fetch_new_only=False,  # Get all contributions, not just new ones
            fetch_all=True  # Signal to fetch all pages
        )
        
        logger.info(f"Fetched {len(all_contributions)} total contributions from API")
        return all_contributions
    
    async def store_contributions(self, contributions: List[Dict[str, Any]]) -> int:
        """
        Store contributions in database, avoiding duplicates.
        
        Args:
            contributions: List of contribution dictionaries to store
            
        Returns:
            Number of newly stored contributions
        """
        if not contributions:
            return 0
        
        logger.info(f"Storing {len(contributions)} contributions in database")
        stored_count = 0
        
        # Use the FEC client's _store_contribution method
        # It already handles duplicate detection and smart merging
        # Process in batches to avoid overwhelming the database
        batch_size = 50
        storage_semaphore = asyncio.Semaphore(5)  # Max 5 concurrent storage operations
        
        async def store_with_semaphore(contrib_data):
            async with storage_semaphore:
                try:
                    await self.fec_client._store_contribution(contrib_data)
                    return 1
                except Exception as e:
                    # Log but continue - duplicate errors are expected
                    if "unique constraint" in str(e).lower() or "already exists" in str(e).lower() or "UNIQUE constraint" in str(e):
                        logger.debug(f"Contribution already exists, skipping: {e}")
                    else:
                        logger.warning(f"Error storing contribution: {e}")
                    return 0
        
        # Process in batches
        for i in range(0, len(contributions), batch_size):
            batch = contributions[i:i + batch_size]
            tasks = [store_with_semaphore(contrib) for contrib in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            stored_count += sum(r for r in results if isinstance(r, int))
            
            # Small delay between batches
            if i + batch_size < len(contributions):
                await asyncio.sleep(0.1)
        
        logger.info(f"Stored {stored_count} new contributions in database")
        return stored_count
    
    async def fetch_and_store(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        cycle: Optional[int] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetch all contributions from API and store them in database.
        After storing, triggers analysis refresh.
        
        Args:
            candidate_id: Candidate ID to fetch contributions for
            committee_id: Committee ID to fetch contributions for
            cycle: Election cycle
            min_date: Minimum date filter
            max_date: Maximum date filter
            
        Returns:
            Dictionary with fetch results and updated analysis
        """
        # Fetch all contributions
        contributions = await self.fetch_all_contributions(
            candidate_id=candidate_id,
            committee_id=committee_id,
            cycle=cycle,
            min_date=min_date,
            max_date=max_date
        )
        
        fetched_count = len(contributions)
        
        # Store contributions
        stored_count = await self.store_contributions(contributions)
        
        # Rerun analysis with updated data
        updated_analysis = None
        if self.analysis_service and (candidate_id or committee_id):
            try:
                logger.info("Rerunning contribution analysis with updated data")
                updated_analysis = await self.analysis_service.analyze_contributions(
                    candidate_id=candidate_id,
                    committee_id=committee_id,
                    cycle=cycle,
                    min_date=min_date,
                    max_date=max_date
                )
            except Exception as e:
                logger.warning(f"Error rerunning analysis: {e}")
        
        return {
            "fetched_count": fetched_count,
            "stored_count": stored_count,
            "analysis": updated_analysis
        }

