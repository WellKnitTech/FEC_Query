from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from app.services.fec_client import FECClient
from app.models.schemas import Contribution, ContributionAnalysis
from app.services.analysis import AnalysisService
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

def get_fec_client():
    """Get FEC client instance"""
    try:
        return FECClient()
    except ValueError as e:
        logger.error(f"FEC API key not configured: {e}")
        raise HTTPException(
            status_code=500,
            detail="FEC API key not configured. Please set FEC_API_KEY in your .env file."
        )

def get_analysis_service():
    """Get analysis service instance"""
    return AnalysisService(get_fec_client())


@router.get("/", response_model=List[Contribution])
async def get_contributions(
    candidate_id: Optional[str] = Query(None, description="Candidate ID"),
    committee_id: Optional[str] = Query(None, description="Committee ID"),
    contributor_name: Optional[str] = Query(None, description="Contributor name"),
    min_amount: Optional[float] = Query(None, description="Minimum contribution amount"),
    max_amount: Optional[float] = Query(None, description="Maximum contribution amount"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=10000, description="Maximum results")
):
    """Get contributions"""
    try:
        fec_client = get_fec_client()
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
        # Map API response to our schema, handling missing fields
        contributions = []
        for contrib in results:
            try:
                # Map common field name variations
                contrib_data = {
                    "contribution_id": contrib.get("sub_id") or contrib.get("contribution_id"),
                    "candidate_id": contrib.get("candidate_id"),
                    "committee_id": contrib.get("committee_id"),
                    "contributor_name": contrib.get("contributor_name"),
                    "contributor_city": contrib.get("contributor_city"),
                    "contributor_state": contrib.get("contributor_state"),
                    "contributor_zip": contrib.get("contributor_zip"),
                    "contributor_employer": contrib.get("contributor_employer"),
                    "contributor_occupation": contrib.get("contributor_occupation"),
                    "contribution_amount": float(contrib.get("contribution_amount", 0) or 0),
                    "contribution_date": contrib.get("contribution_receipt_date") or contrib.get("contribution_date"),
                    "contribution_type": contrib.get("contribution_type"),
                    "receipt_type": contrib.get("receipt_type")
                }
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


@router.get("/analysis", response_model=ContributionAnalysis)
async def analyze_contributions(
    candidate_id: Optional[str] = Query(None, description="Candidate ID"),
    committee_id: Optional[str] = Query(None, description="Committee ID"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """Analyze contributions with aggregations"""
    try:
        analysis_service = get_analysis_service()
        analysis = await analysis_service.analyze_contributions(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date
        )
        return analysis
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing contributions: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to analyze contributions: {str(e)}")

