from fastapi import APIRouter, HTTPException, Query, Body, Depends
from typing import Optional, List
from pydantic import BaseModel
from app.services.trends import TrendAnalysisService
from app.api.dependencies import get_trend_service
from app.utils.logging import get_logger

logger = get_logger(__name__)

router = APIRouter()


class RaceTrendRequest(BaseModel):
    candidate_ids: List[str]
    min_cycle: Optional[int] = None
    max_cycle: Optional[int] = None


@router.get("/candidate/{candidate_id}")
async def get_candidate_trends(
    candidate_id: str,
    min_cycle: Optional[int] = Query(None, description="Minimum cycle"),
    max_cycle: Optional[int] = Query(None, description="Maximum cycle"),
    service: TrendAnalysisService = Depends(get_trend_service)
):
    """Get multi-cycle financial trends for a candidate"""
    try:
        trends = await service.get_candidate_trends(
            candidate_id=candidate_id,
            min_cycle=min_cycle,
            max_cycle=max_cycle
        )
        return trends
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting trends for candidate {candidate_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get trends: {str(e)}")


@router.post("/race")
async def get_race_trends(
    request: RaceTrendRequest,
    service: TrendAnalysisService = Depends(get_trend_service)
):
    """Compare multiple candidates across cycles"""
    try:
        trends = await service.get_race_trends(
            candidate_ids=request.candidate_ids,
            min_cycle=request.min_cycle,
            max_cycle=request.max_cycle
        )
        return trends
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting race trends: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get race trends: {str(e)}")


@router.get("/contribution-velocity/{candidate_id}")
async def get_contribution_trends(
    candidate_id: str,
    min_cycle: Optional[int] = Query(None, description="Minimum cycle"),
    max_cycle: Optional[int] = Query(None, description="Maximum cycle"),
    service: TrendAnalysisService = Depends(get_trend_service)
):
    """Get historical contribution velocity patterns"""
    try:
        trends = await service.get_contribution_trends(
            candidate_id=candidate_id,
            min_cycle=min_cycle,
            max_cycle=max_cycle
        )
        return trends
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting contribution trends for {candidate_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get contribution trends: {str(e)}")

