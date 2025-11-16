from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Union
from app.services.fec_client import FECClient
from app.models.schemas import (
    MoneyFlowGraph, ExpenditureBreakdown, EmployerAnalysis, ContributionVelocity, DonorStateAnalysis, Contribution, AggregatedDonor
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
    min_amount: float = Query(100.0, description="Minimum amount to include"),
    aggregate_by_employer: bool = Query(True, description="Group by employer instead of individual donors")
):
    """Get money flow network graph"""
    try:
        analysis_service = get_analysis_service()
        graph = await analysis_service.build_money_flow_graph(
            candidate_id=candidate_id,
            max_depth=max_depth,
            min_amount=min_amount,
            aggregate_by_employer=aggregate_by_employer
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


@router.get("/donor-states/out-of-state-contributions")
async def get_out_of_state_contributions(
    candidate_id: str = Query(..., description="Candidate ID"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    cycle: Optional[int] = Query(None, description="Election cycle"),
    limit: int = Query(10000, ge=1, le=50000, description="Maximum results"),
    aggregate: bool = Query(False, description="If true, return aggregated donors instead of individual contributions")
) -> Union[List[Contribution], List[AggregatedDonor]]:
    """Get contributions from out-of-state donors for human analysis
    
    Only applicable to Senate (S) and House (H) candidates, not President (P).
    Returns contributions sorted by amount (descending) then date (descending).
    If aggregate=true, returns aggregated donors grouped by name variations.
    """
    try:
        # Check candidate office type - only applicable to Senate and House
        fec_client = get_fec_client()
        candidate = await fec_client.get_candidate(candidate_id)
        
        if not candidate:
            raise HTTPException(status_code=404, detail="Candidate not found")
        
        office = candidate.get('office')
        if office and office.upper() == 'P':
            raise HTTPException(
                status_code=400,
                detail="Out-of-state contribution analysis is not applicable to Presidential candidates. Only Senate (S) and House (H) candidates are supported."
            )
        
        analysis_service = get_analysis_service()
        
        # If aggregate is requested, return aggregated donors
        if aggregate:
            aggregated_donors = await analysis_service.get_aggregated_out_of_state_donors(
                candidate_id=candidate_id,
                min_date=min_date,
                max_date=max_date,
                cycle=cycle,
                limit=limit
            )
            # Convert to AggregatedDonor schema
            return [AggregatedDonor(**donor) for donor in aggregated_donors]
        
        # Otherwise, return individual contributions
        contributions = await analysis_service.get_out_of_state_contributions(
            candidate_id=candidate_id,
            min_date=min_date,
            max_date=max_date,
            cycle=cycle,
            limit=limit
        )
        
        # Map to Contribution schema
        mapped_contributions = []
        for contrib in contributions:
            try:
                # Extract amount from multiple possible field names
                amount = 0.0
                for amt_key in ['contb_receipt_amt', 'contribution_amount', 'contribution_receipt_amount', 'amount', 'contribution_receipt_amt']:
                    amt_val = contrib.get(amt_key)
                    if amt_val is not None:
                        try:
                            amount = float(amt_val)
                            if amount > 0:
                                break
                        except (ValueError, TypeError):
                            continue
                
                # Extract contributor name
                contributor_name = contrib.get("contributor_name") or contrib.get("contributor") or contrib.get("name") or contrib.get("contributor_name_1")
                
                # Get date - service already formats it, so prefer those fields
                # Also check raw_data fields if main date fields are missing
                contribution_date = (
                    contrib.get("contribution_date") or 
                    contrib.get("contribution_receipt_date") or 
                    contrib.get("receipt_date") or
                    contrib.get("TRANSACTION_DT") or
                    contrib.get("TRANSACTION_DATE")
                )
                
                # If we got a date from raw_data, try to format it
                if contribution_date and contribution_date not in [None, '']:
                    from app.utils.date_utils import serialize_date
                    contribution_date = serialize_date(contribution_date)
                
                contrib_data = {
                    "contribution_id": contrib.get("sub_id") or contrib.get("contribution_id"),
                    "candidate_id": contrib.get("candidate_id"),
                    "committee_id": contrib.get("committee_id"),
                    "contributor_name": contributor_name,
                    "contributor_city": contrib.get("contributor_city") or contrib.get("city"),
                    "contributor_state": contrib.get("contributor_state") or contrib.get("state"),
                    "contributor_zip": contrib.get("contributor_zip") or contrib.get("zip_code") or contrib.get("zip"),
                    "contributor_employer": contrib.get("contributor_employer") or contrib.get("employer"),
                    "contributor_occupation": contrib.get("contributor_occupation") or contrib.get("occupation"),
                    "contribution_amount": amount,
                    "contribution_date": contribution_date,
                    "contribution_type": contrib.get("contribution_type") or contrib.get("transaction_type"),
                    "receipt_type": contrib.get("receipt_type")
                }
                mapped_contributions.append(Contribution(**contrib_data))
            except Exception as e:
                logger.warning(f"Skipping invalid contribution: {e}")
                continue
        
        return mapped_contributions
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting out-of-state contributions for {candidate_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get out-of-state contributions: {str(e)}")


@router.get("/donor-states", response_model=DonorStateAnalysis)
async def get_donor_states(
    candidate_id: str = Query(..., description="Candidate ID"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    cycle: Optional[int] = Query(None, description="Election cycle")
):
    """Get donor state analysis - percentage of donors and amounts by state
    
    Only applicable to Senate (S) and House (H) candidates, not President (P).
    """
    try:
        # Check candidate office type - only applicable to Senate and House
        fec_client = get_fec_client()
        candidate = await fec_client.get_candidate(candidate_id)
        
        if not candidate:
            raise HTTPException(status_code=404, detail="Candidate not found")
        
        office = candidate.get('office')
        if office and office.upper() == 'P':
            raise HTTPException(
                status_code=400,
                detail="Donor state analysis is not applicable to Presidential candidates. Only Senate (S) and House (H) candidates are supported."
            )
        
        analysis_service = get_analysis_service()
        analysis = await analysis_service.analyze_donor_states(
            candidate_id=candidate_id,
            min_date=min_date,
            max_date=max_date,
            cycle=cycle
        )
        return analysis
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting donor states for {candidate_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get donor states: {str(e)}")

