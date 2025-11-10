from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from app.services.fec_client import FECClient
from app.models.schemas import (
    MoneyFlowGraph, ExpenditureBreakdown, EmployerAnalysis, ContributionVelocity
)
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


@router.get("/money-flow", response_model=MoneyFlowGraph)
async def get_money_flow(
    candidate_id: str = Query(..., description="Candidate ID"),
    max_depth: int = Query(2, ge=1, le=3, description="Maximum depth for flow tracking"),
    min_amount: float = Query(100.0, description="Minimum amount to include")
):
    """Get money flow network graph"""
    try:
        analysis_service = get_analysis_service()
        graph = await analysis_service.build_money_flow_graph(
            candidate_id=candidate_id,
            max_depth=max_depth,
            min_amount=min_amount
        )
        return graph
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting money flow for {candidate_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get money flow: {str(e)}")


@router.get("/expenditure-breakdown", response_model=ExpenditureBreakdown)
async def get_expenditure_breakdown(
    candidate_id: Optional[str] = Query(None, description="Candidate ID"),
    committee_id: Optional[str] = Query(None, description="Committee ID"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """Get expenditure breakdown with category aggregation"""
    try:
        analysis_service = get_analysis_service()
        breakdown = await analysis_service.analyze_expenditures(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date
        )
        return breakdown
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting expenditure breakdown: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get expenditure breakdown: {str(e)}")


@router.get("/employer-breakdown", response_model=EmployerAnalysis)
async def get_employer_breakdown(
    candidate_id: Optional[str] = Query(None, description="Candidate ID"),
    committee_id: Optional[str] = Query(None, description="Committee ID"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """Get contribution breakdown by employer"""
    try:
        analysis_service = get_analysis_service()
        analysis = await analysis_service.analyze_by_employer(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date
        )
        return analysis
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting employer breakdown: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get employer breakdown: {str(e)}")


@router.get("/velocity", response_model=ContributionVelocity)
async def get_contribution_velocity(
    candidate_id: Optional[str] = Query(None, description="Candidate ID"),
    committee_id: Optional[str] = Query(None, description="Committee ID"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """Get contribution velocity (contributions per day/week)"""
    try:
        analysis_service = get_analysis_service()
        velocity = await analysis_service.analyze_velocity(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date
        )
        return velocity
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting contribution velocity: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get contribution velocity: {str(e)}")

