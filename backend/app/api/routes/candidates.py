from fastapi import APIRouter, HTTPException, Query, Body
from typing import Optional, List, Dict
import asyncio
from app.services.fec_client import FECClient
from app.models.schemas import CandidateSummary, FinancialSummary, BatchFinancialsRequest
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


@router.get("/search", response_model=List[CandidateSummary])
async def search_candidates(
    name: Optional[str] = Query(None, description="Candidate name to search"),
    office: Optional[str] = Query(None, description="Office type (P, S, H)"),
    state: Optional[str] = Query(None, description="State abbreviation"),
    party: Optional[str] = Query(None, description="Party"),
    year: Optional[int] = Query(None, description="Election year"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results")
):
    """Search for candidates"""
    try:
        fec_client = get_fec_client()
        results = await fec_client.search_candidates(
            name=name,
            office=office,
            state=state,
            party=party,
            year=year,
            limit=limit
        )
        
        # Convert API response to our schema, handling missing fields
        candidates = []
        for candidate in results:
            try:
                # Map API fields to our schema
                candidate_data = {
                    "candidate_id": candidate.get("candidate_id", ""),
                    "name": candidate.get("name", candidate.get("candidate_name", "Unknown")),
                    "office": candidate.get("office"),
                    "party": candidate.get("party"),
                    "state": candidate.get("state"),
                    "district": candidate.get("district"),
                    "election_years": candidate.get("election_years"),
                    "active_through": candidate.get("active_through")
                }
                candidates.append(CandidateSummary(**candidate_data))
            except Exception as e:
                logger.warning(f"Failed to parse candidate: {candidate}, error: {e}")
                continue
        
        return candidates
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error searching candidates: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to search candidates: {str(e)}")


@router.get("/race", response_model=List[CandidateSummary])
async def get_race_candidates(
    office: str = Query(..., description="Office type (P=President, S=Senate, H=House)"),
    state: str = Query(..., description="State abbreviation (e.g., TX)"),
    district: Optional[str] = Query(None, description="District number (for House races)"),
    year: Optional[int] = Query(None, description="Election year"),
    limit: int = Query(100, ge=1, le=200, description="Maximum results")
):
    """Get all candidates for a specific race"""
    try:
        fec_client = get_fec_client()
        results = await fec_client.get_race_candidates(
            office=office,
            state=state,
            district=district,
            year=year,
            limit=limit
        )
        
        # Convert API response to our schema
        candidates = []
        for candidate in results:
            try:
                candidate_data = {
                    "candidate_id": candidate.get("candidate_id", ""),
                    "name": candidate.get("name", candidate.get("candidate_name", "Unknown")),
                    "office": candidate.get("office"),
                    "party": candidate.get("party"),
                    "state": candidate.get("state"),
                    "district": candidate.get("district"),
                    "election_years": candidate.get("election_years"),
                    "active_through": candidate.get("active_through")
                }
                candidates.append(CandidateSummary(**candidate_data))
            except Exception as e:
                logger.warning(f"Failed to parse candidate: {candidate}, error: {e}")
                continue
        
        return candidates
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting race candidates: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get race candidates: {str(e)}")


@router.get("/{candidate_id}", response_model=CandidateSummary)
async def get_candidate(candidate_id: str):
    """Get candidate details by ID"""
    try:
        fec_client = get_fec_client()
        candidate = await fec_client.get_candidate(candidate_id)
        if not candidate:
            raise HTTPException(status_code=404, detail="Candidate not found")
        
        # Map API response to our schema
        candidate_data = {
            "candidate_id": candidate.get("candidate_id", candidate_id),
            "name": candidate.get("name", candidate.get("candidate_name", "Unknown")),
            "office": candidate.get("office"),
            "party": candidate.get("party"),
            "state": candidate.get("state"),
            "district": candidate.get("district"),
            "election_years": candidate.get("election_years"),
            "active_through": candidate.get("active_through")
        }
        return CandidateSummary(**candidate_data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting candidate {candidate_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get candidate: {str(e)}")


@router.get("/{candidate_id}/financials", response_model=List[FinancialSummary])
async def get_candidate_financials(
    candidate_id: str,
    cycle: Optional[int] = Query(None, description="Election cycle")
):
    """Get candidate financial summary"""
    try:
        fec_client = get_fec_client()
        totals = await fec_client.get_candidate_totals(candidate_id, cycle=cycle)
        
        # Map API response to our schema
        financials = []
        for total in totals:
            cycle_value = total.get("cycle")
            # Handle None cycle - try to extract from two_year_transaction_period or use 0
            if cycle_value is None:
                cycle_value = total.get("two_year_transaction_period")
                if cycle_value is None:
                    # Calculate from election year if available
                    election_year = total.get("election_year")
                    if election_year:
                        cycle_value = election_year
                    else:
                        cycle_value = 0  # Default fallback
            
            financial_data = {
                "candidate_id": total.get("candidate_id", candidate_id),
                "cycle": cycle_value if cycle_value is not None else 0,
                "total_receipts": float(total.get("receipts", 0)),
                "total_disbursements": float(total.get("disbursements", 0)),
                "cash_on_hand": float(total.get("cash_on_hand_end_period", 0)),
                "total_contributions": float(total.get("contributions", 0)),
                "individual_contributions": float(total.get("individual_contributions", 0)),
                "pac_contributions": float(total.get("pac_contributions", 0)),
                "party_contributions": float(total.get("party_contributions", 0))
            }
            financials.append(FinancialSummary(**financial_data))
        
        return financials
    except Exception as e:
        error_msg = str(e)
        # Check if it's a rate limit error
        if "rate limit" in error_msg.lower() or "429" in error_msg or "OVER_RATE_LIMIT" in error_msg:
            logger.warning(f"Rate limit hit for candidate {candidate_id}: {e}")
            raise HTTPException(
                status_code=429,
                detail="FEC API rate limit exceeded. Please wait a moment and try again. Consider using cached data or reducing the number of concurrent requests."
            )
        logger.error(f"Error getting financials for {candidate_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get financials: {str(e)}")


@router.post("/financials/batch")
async def get_batch_financials(request: BatchFinancialsRequest):
    """Get financial summaries for multiple candidates in one request"""
    try:
        fec_client = get_fec_client()
        
        # Limit batch size to prevent abuse
        max_batch_size = 50
        if len(request.candidate_ids) > max_batch_size:
            raise HTTPException(
                status_code=400,
                detail=f"Batch size exceeds maximum of {max_batch_size} candidates"
            )
        
        # Fetch financials for all candidates in parallel
        financial_tasks = [
            fec_client.get_candidate_totals(candidate_id, cycle=request.cycle)
            for candidate_id in request.candidate_ids
        ]
        
        results = await asyncio.gather(*financial_tasks, return_exceptions=True)
        
        # Map results to response format
        financials_map: Dict[str, List[FinancialSummary]] = {}
        
        for candidate_id, result in zip(request.candidate_ids, results):
            if isinstance(result, Exception):
                logger.warning(f"Error getting financials for {candidate_id}: {result}")
                financials_map[candidate_id] = []
                continue
            
            financials = []
            for total in result:
                cycle_value = total.get("cycle")
                if cycle_value is None:
                    cycle_value = total.get("two_year_transaction_period")
                    if cycle_value is None:
                        election_year = total.get("election_year")
                        if election_year:
                            cycle_value = election_year
                        else:
                            cycle_value = 0
                
                financial_data = {
                    "candidate_id": total.get("candidate_id", candidate_id),
                    "cycle": cycle_value if cycle_value is not None else 0,
                    "total_receipts": float(total.get("receipts", 0)),
                    "total_disbursements": float(total.get("disbursements", 0)),
                    "cash_on_hand": float(total.get("cash_on_hand_end_period", 0)),
                    "total_contributions": float(total.get("contributions", 0)),
                    "individual_contributions": float(total.get("individual_contributions", 0)),
                    "pac_contributions": float(total.get("pac_contributions", 0)),
                    "party_contributions": float(total.get("party_contributions", 0))
                }
                financials.append(FinancialSummary(**financial_data))
            
            financials_map[candidate_id] = financials
        
        return financials_map
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting batch financials: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get batch financials: {str(e)}")

