from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from app.services.fec_client import FECClient
from app.models.schemas import FraudAnalysis
from app.services.fraud_detection import FraudDetectionService
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

def get_fraud_service():
    """Get fraud detection service instance"""
    return FraudDetectionService(get_fec_client())


@router.get("/analyze", response_model=FraudAnalysis)
async def analyze_fraud(
    candidate_id: str = Query(..., description="Candidate ID"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """Analyze contributions for fraud patterns"""
    try:
        fraud_service = get_fraud_service()
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
    use_aggregation: bool = Query(True, description="Use donor aggregation for analysis")
):
    """Analyze fraud using donor aggregation for more accurate detection"""
    try:
        fraud_service = get_fraud_service()
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

