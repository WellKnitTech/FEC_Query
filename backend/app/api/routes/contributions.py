"""
API routes for contribution-related endpoints

This module provides FastAPI routes for querying and analyzing campaign contributions.
It handles both API and database queries, with support for filtering, aggregation,
and analysis operations.

Endpoints:
- GET /: List contributions with filtering
- GET /unique-contributors: Get unique contributors for a candidate
- GET /analysis: Get contribution analysis statistics
- GET /aggregated-donors: Get aggregated donor information

All endpoints support filtering by candidate_id, committee_id, date ranges, and amounts.
"""
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List, Dict, Any
import time
from datetime import datetime, timedelta
from app.services.fec_client import FECClient
from app.models.schemas import Contribution, ContributionAnalysis, AggregatedDonor
from app.services.analysis import AnalysisService
from app.services.donor_aggregation import DonorAggregationService
from app.services.contribution_fetcher import ContributionFetcherService
from app.api.dependencies import get_fec_client, get_analysis_service, get_donor_search_service
from app.services.donor_search import DonorSearchService
from app.services.shared.exceptions import DonorSearchError, QueryTimeoutError
from app.utils.field_mapping import map_contribution_fields, map_contribution_for_aggregation
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_model=List[Contribution])
async def get_contributions(
    candidate_id: Optional[str] = Query(None, description="Candidate ID", max_length=20, regex="^[A-Z0-9]*$"),
    committee_id: Optional[str] = Query(None, description="Committee ID", max_length=20, regex="^[A-Z0-9]*$"),
    contributor_name: Optional[str] = Query(None, description="Contributor name", max_length=200),
    min_amount: Optional[float] = Query(None, description="Minimum contribution amount", ge=0),
    max_amount: Optional[float] = Query(None, description="Maximum contribution amount", ge=0),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)", regex="^\\d{4}-\\d{2}-\\d{2}$"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)", regex="^\\d{4}-\\d{2}-\\d{2}$"),
    limit: int = Query(100, ge=1, le=10000, description="Maximum results"),
    fec_client: FECClient = Depends(get_fec_client)
):
    """Get contributions"""
    try:
        results = await fec_client.get_contributions(
            candidate_id=candidate_id,
            committee_id=committee_id,
            contributor_name=contributor_name,
            min_amount=min_amount,
            max_amount=max_amount,
            min_date=min_date,
            max_date=max_date,
            limit=limit
        )
        # Map API response to our schema using shared utility
        contributions = []
        for contrib in results:
            try:
                contrib_data = map_contribution_fields(contrib)
                contributions.append(Contribution(**contrib_data))
            except Exception as e:
                logger.warning(f"Skipping invalid contribution: {e}")
                continue
        
        return contributions
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting contributions: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get contributions: {str(e)}")


@router.get("/unique-contributors")
async def get_unique_contributors(
    search_term: str = Query(..., description="Search term for contributor name (e.g., 'Smith')", min_length=1, max_length=200),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    service: DonorSearchService = Depends(get_donor_search_service)
):
    """Get unique contributor names matching a search term from local database.
    
    Returns a list of unique contributors with their total contribution amounts and counts.
    Returns empty list if no results found.
    """
    logger.info(f"Search request for '{search_term}' (limit={limit})")
    
    try:
        contributors = await service.search_unique_contributors(search_term, limit)
        logger.info(f"Search completed for '{search_term}': found {len(contributors)} results")
        return contributors
        
    except QueryTimeoutError as e:
        logger.error(f"Search timed out for '{search_term}': {e}")
        raise HTTPException(
            status_code=504,
            detail=f"Search timed out. The search term '{search_term}' may be too broad. Try a more specific search."
        )
    except DonorSearchError as e:
        logger.error(f"Search error for '{search_term}': {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Search failed: {str(e)}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in unique-contributors endpoint: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}"
        )


@router.get("/aggregated-donors", response_model=List[AggregatedDonor])
async def get_aggregated_donors(
    candidate_id: Optional[str] = Query(None, description="Candidate ID"),
    committee_id: Optional[str] = Query(None, description="Committee ID"),
    contributor_name: Optional[str] = Query(None, description="Contributor name"),
    min_amount: Optional[float] = Query(None, description="Minimum contribution amount"),
    max_amount: Optional[float] = Query(None, description="Maximum contribution amount"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    fec_client: FECClient = Depends(get_fec_client)
):
    """Get aggregated donors (grouped by name variations)"""
    try:
        # Fetch contributions using existing filters
        contributions = await fec_client.get_contributions(
            candidate_id=candidate_id,
            committee_id=committee_id,
            contributor_name=contributor_name,
            min_amount=min_amount,
            max_amount=max_amount,
            min_date=min_date,
            max_date=max_date,
            limit=10000  # Get more contributions for better aggregation
        )
        
        logger.info(f"Fetching aggregated donors: contributor_name={contributor_name}, limit={limit}")
        
        if not contributions:
            logger.info(f"No contributions found for aggregated donors query")
            return []
        
        logger.info(f"Found {len(contributions)} contributions to aggregate")
        
        # Convert contributions to dictionaries for aggregation service using shared utility
        contrib_dicts = []
        for contrib in contributions:
            contrib_dict = map_contribution_for_aggregation(contrib)
            if contrib_dict:  # Only include if contributor_name is present
                contrib_dicts.append(contrib_dict)
        
        logger.info(f"Processing {len(contrib_dicts)} contributions for aggregation")
        
        if not contrib_dicts:
            logger.warning("No valid contributions with names found for aggregation")
            return []
        
        # Aggregate donors
        aggregation_service = DonorAggregationService()
        aggregated = aggregation_service.aggregate_donors(contrib_dicts)
        
        logger.info(f"Aggregated {len(contrib_dicts)} contributions into {len(aggregated)} unique donors")
        
        # Apply limit and return
        result = [AggregatedDonor(**donor) for donor in aggregated[:limit]]
        logger.info(f"Returning {len(result)} aggregated donors")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting aggregated donors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get aggregated donors: {str(e)}")


@router.get("/analysis", response_model=ContributionAnalysis)
async def analyze_contributions(
    candidate_id: Optional[str] = Query(None, description="Candidate ID"),
    committee_id: Optional[str] = Query(None, description="Committee ID"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    cycle: Optional[int] = Query(None, description="Election cycle (two_year_transaction_period)"),
    analysis_service: AnalysisService = Depends(get_analysis_service)
):
    """Analyze contributions with aggregations"""
    try:
        analysis = await analysis_service.analyze_contributions(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date,
            cycle=cycle
        )
        return analysis
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing contributions: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to analyze contributions: {str(e)}")


@router.post("/fetch-from-api")
async def fetch_contributions_from_api(
    candidate_id: Optional[str] = Query(None, description="Candidate ID"),
    committee_id: Optional[str] = Query(None, description="Committee ID"),
    cycle: Optional[int] = Query(None, description="Election cycle"),
    fec_client: FECClient = Depends(get_fec_client),
    analysis_service: AnalysisService = Depends(get_analysis_service)
) -> Dict[str, Any]:
    """
    Fetch all contributions from FEC API and store them in the database.
    After storing, automatically reruns contribution analysis.
    
    Returns:
        Dictionary with fetch results and updated analysis
    """
    if not candidate_id and not committee_id:
        raise HTTPException(
            status_code=400,
            detail="Either candidate_id or committee_id must be provided"
        )
    
    try:
        # Create fetcher service
        fetcher = ContributionFetcherService(fec_client, analysis_service)
        
        # Fetch and store contributions, then rerun analysis
        result = await fetcher.fetch_and_store(
            candidate_id=candidate_id,
            committee_id=committee_id,
            cycle=cycle
        )
        
        return {
            "success": True,
            "fetched_count": result["fetched_count"],
            "stored_count": result["stored_count"],
            "message": f"Fetched {result['fetched_count']} contributions, stored {result['stored_count']} new records",
            "analysis": result["analysis"]
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching contributions from API: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch contributions from API: {str(e)}"
        )

