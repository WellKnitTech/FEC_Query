from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
from app.models.schemas import FraudAnalysis
from app.services.fraud_detection import FraudDetectionService
from app.api.dependencies import get_fraud_service
from app.utils.logging import get_logger

logger = get_logger(__name__)

router = APIRouter()


@router.get("/analyze", response_model=FraudAnalysis)
async def analyze_fraud(
    candidate_id: str = Query(..., description="Candidate ID"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    fraud_service: FraudDetectionService = Depends(get_fraud_service)
):
    """Analyze contributions for fraud patterns"""
    try:
        analysis = await fraud_service.analyze_candidate(
            candidate_id=candidate_id,
            min_date=min_date,
            max_date=max_date
        )
        return analysis
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing fraud for {candidate_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to analyze fraud: {str(e)}")


@router.get("/analyze-donors", response_model=FraudAnalysis)
async def analyze_fraud_by_donors(
    candidate_id: str = Query(..., description="Candidate ID"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    use_aggregation: bool = Query(True, description="Use donor aggregation for analysis"),
    fraud_service: FraudDetectionService = Depends(get_fraud_service)
):
    """Analyze fraud using donor aggregation for more accurate detection"""
    try:
        if use_aggregation:
            analysis = await fraud_service.analyze_candidate_with_aggregation(
                candidate_id=candidate_id,
                min_date=min_date,
                max_date=max_date
            )
        else:
            analysis = await fraud_service.analyze_candidate(
                candidate_id=candidate_id,
                min_date=min_date,
                max_date=max_date
            )
        return analysis
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing fraud with aggregation for {candidate_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to analyze fraud: {str(e)}")

