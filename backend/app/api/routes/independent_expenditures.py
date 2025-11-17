from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from app.services.fec_client import FECClient
from app.services.independent_expenditures import IndependentExpenditureService
from app.models.schemas import IndependentExpenditure, IndependentExpenditureAnalysis
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


def get_fec_client():
    """Get FEC client instance"""
    from app.services.container import get_service_container
    try:
        container = get_service_container()
        return container.get_fec_client()
    except ValueError as e:
        logger.error(f"FEC API key not configured: {e}")
        raise HTTPException(
            status_code=500,
            detail="FEC API key not configured. Please set FEC_API_KEY in your .env file."
        )


def get_independent_expenditure_service():
    """Get independent expenditure service instance"""
    return IndependentExpenditureService(get_fec_client())


@router.get("/", response_model=List[IndependentExpenditure])
async def get_independent_expenditures(
    candidate_id: Optional[str] = Query(None, description="Candidate ID"),
    committee_id: Optional[str] = Query(None, description="Committee ID"),
    support_oppose: Optional[str] = Query(None, regex="^(S|O)$", description="Support (S) or Oppose (O)"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    min_amount: Optional[float] = Query(None, description="Minimum amount"),
    max_amount: Optional[float] = Query(None, description="Maximum amount"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum results")
):
    """Get independent expenditures with filters"""
    try:
        service = get_independent_expenditure_service()
        results = await service.get_independent_expenditures(
            candidate_id=candidate_id,
            committee_id=committee_id,
            support_oppose=support_oppose,
            min_date=min_date,
            max_date=max_date,
            min_amount=min_amount,
            max_amount=max_amount,
            limit=limit
        )
        
        # Map to schema
        expenditures = []
        for exp in results:
            try:
                # Safely convert expenditure_amount to float, handling malformed values
                amount = exp.get("expenditure_amount", 0) or 0
                try:
                    if isinstance(amount, str):
                        # Try to clean the string first (remove $, commas, etc.)
                        amount = amount.replace('$', '').replace(',', '').strip()
                        # If it's a malformed string like "0.00.00.00...", take the first valid part
                        if '.' in amount and amount.count('.') > 1:
                            # Take only the first decimal part
                            first_dot = amount.find('.')
                            second_dot = amount.find('.', first_dot + 1)
                            if second_dot > 0:
                                amount = amount[:second_dot]
                    amount = float(amount)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid expenditure_amount value: {exp.get('expenditure_amount')}, using 0.0")
                    amount = 0.0
                
                exp_data = IndependentExpenditure(
                    expenditure_id=exp.get("expenditure_id") or exp.get("sub_id", ""),
                    cycle=exp.get("cycle"),
                    committee_id=exp.get("committee_id"),
                    candidate_id=exp.get("candidate_id"),
                    candidate_name=exp.get("candidate_name"),
                    support_oppose_indicator=exp.get("support_oppose_indicator"),
                    expenditure_amount=amount,
                    expenditure_date=exp.get("expenditure_date") or exp.get("expenditure_date_formatted"),
                    payee_name=exp.get("payee_name"),
                    expenditure_purpose=exp.get("expenditure_purpose")
                )
                expenditures.append(exp_data)
            except Exception as e:
                logger.warning(f"Skipping invalid expenditure: {e}")
                continue
        
        return expenditures
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting independent expenditures: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get independent expenditures: {str(e)}")


@router.get("/analysis", response_model=IndependentExpenditureAnalysis)
async def analyze_independent_expenditures(
    candidate_id: Optional[str] = Query(None, description="Candidate ID"),
    committee_id: Optional[str] = Query(None, description="Committee ID"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """Analyze independent expenditures with aggregations"""
    try:
        service = get_independent_expenditure_service()
        analysis = await service.analyze_independent_expenditures(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date
        )
        return IndependentExpenditureAnalysis(**analysis)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing independent expenditures: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to analyze independent expenditures: {str(e)}")


@router.get("/{candidate_id}/summary")
async def get_candidate_summary(
    candidate_id: str,
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """Get independent expenditure summary for a specific candidate"""
    try:
        service = get_independent_expenditure_service()
        summary = await service.get_candidate_summary(
            candidate_id=candidate_id,
            min_date=min_date,
            max_date=max_date
        )
        return summary
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting candidate summary for {candidate_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get candidate summary: {str(e)}")

