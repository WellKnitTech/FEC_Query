from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List
from app.services.fec_client import FECClient
from app.services.committees import CommitteeService
from app.models.schemas import CommitteeSummary, CommitteeFinancials, CommitteeTransfer, ContactInformation
from app.api.dependencies import get_fec_client, get_committee_service
from app.utils.logging import get_logger

logger = get_logger(__name__)

router = APIRouter()


@router.get("/search", response_model=List[CommitteeSummary])
async def search_committees(
    name: Optional[str] = Query(None, description="Committee name to search"),
    committee_type: Optional[str] = Query(None, description="Committee type"),
    state: Optional[str] = Query(None, description="State abbreviation"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results"),
    fec_client: FECClient = Depends(get_fec_client)
):
    """Search for committees"""
    try:
        results = await fec_client.get_committees(
            name=name,
            committee_type=committee_type,
            state=state,
            limit=limit
        )
        
        committees = []
        for committee in results:
            try:
                committee_data = {
                    "committee_id": committee.get("committee_id", ""),
                    "name": committee.get("name", ""),
                    "committee_type": committee.get("committee_type"),
                    "committee_type_full": committee.get("committee_type_full"),
                    "party": committee.get("party"),
                    "state": committee.get("state"),
                    "candidate_ids": committee.get("candidate_ids", [])
                }
                
                # Build contact information if available
                contact_info = None
                if any([
                    committee.get("street_address"),
                    committee.get("city"),
                    committee.get("zip"),
                    committee.get("email"),
                    committee.get("phone"),
                    committee.get("website"),
                    committee.get("treasurer_name")
                ]):
                    contact_info = ContactInformation(
                        street_address=committee.get("street_address"),
                        street_address_2=committee.get("street_address_2"),
                        city=committee.get("city"),
                        state=committee.get("state"),
                        zip=committee.get("zip"),
                        email=committee.get("email"),
                        phone=committee.get("phone"),
                        website=committee.get("website"),
                        treasurer_name=committee.get("treasurer_name")
                    )
                
                committee_data["contact_info"] = contact_info
                committees.append(CommitteeSummary(**committee_data))
            except Exception as e:
                logger.warning(f"Failed to parse committee: {committee}, error: {e}")
                continue
        
        return committees
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error searching committees: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to search committees: {str(e)}")


@router.get("/{committee_id}", response_model=CommitteeSummary)
async def get_committee(
    committee_id: str,
    fec_client: FECClient = Depends(get_fec_client)
):
    """Get committee details by ID"""
    try:
        committees = await fec_client.get_committees(committee_id=committee_id, limit=1)
        if not committees:
            raise HTTPException(status_code=404, detail="Committee not found")
        
        committee = committees[0]
        
        # Build contact information if available
        contact_info = None
        if any([
            committee.get("street_address"),
            committee.get("city"),
            committee.get("zip"),
            committee.get("email"),
            committee.get("phone"),
            committee.get("website"),
            committee.get("treasurer_name")
        ]):
            contact_info = ContactInformation(
                street_address=committee.get("street_address"),
                street_address_2=committee.get("street_address_2"),
                city=committee.get("city"),
                state=committee.get("state"),
                zip=committee.get("zip"),
                email=committee.get("email"),
                phone=committee.get("phone"),
                website=committee.get("website"),
                treasurer_name=committee.get("treasurer_name")
            )
        
        return CommitteeSummary(
            committee_id=committee.get("committee_id", committee_id),
            name=committee.get("name", ""),
            committee_type=committee.get("committee_type"),
            committee_type_full=committee.get("committee_type_full"),
            party=committee.get("party"),
            state=committee.get("state"),
            candidate_ids=committee.get("candidate_ids", []),
            contact_info=contact_info
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting committee {committee_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get committee: {str(e)}")


@router.get("/{committee_id}/financials", response_model=List[CommitteeFinancials])
async def get_committee_financials(
    committee_id: str,
    cycle: Optional[int] = Query(None, description="Election cycle"),
    service: CommitteeService = Depends(get_committee_service)
):
    """Get committee financial summary"""
    try:
        totals = await service.get_committee_financials(committee_id, cycle=cycle)
        
        financials = []
        for total in totals:
            cycle_value = total.get("cycle") or total.get("two_year_transaction_period") or 0
            financial_data = {
                "committee_id": total.get("committee_id", committee_id),
                "cycle": cycle_value,
                "total_receipts": float(total.get("receipts", 0)),
                "total_disbursements": float(total.get("disbursements", 0)),
                "cash_on_hand": float(total.get("cash_on_hand_end_period", 0)),
                "total_contributions": float(total.get("contributions", 0))
            }
            financials.append(CommitteeFinancials(**financial_data))
        
        return financials
    except Exception as e:
        logger.error(f"Error getting financials for committee {committee_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get financials: {str(e)}")


@router.get("/{committee_id}/contributions", response_model=List)
async def get_committee_contributions(
    committee_id: str,
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum results"),
    fec_client: FECClient = Depends(get_fec_client)
):
    """Get contributions received by committee"""
    try:
        contributions = await fec_client.get_contributions(
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date,
            limit=limit
        )
        return contributions
    except Exception as e:
        logger.error(f"Error getting contributions for committee {committee_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get contributions: {str(e)}")


@router.get("/{committee_id}/expenditures", response_model=List)
async def get_committee_expenditures(
    committee_id: str,
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum results"),
    fec_client: FECClient = Depends(get_fec_client)
):
    """Get expenditures made by committee"""
    try:
        expenditures = await fec_client.get_expenditures(
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date,
            limit=limit
        )
        return expenditures
    except Exception as e:
        logger.error(f"Error getting expenditures for committee {committee_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get expenditures: {str(e)}")


@router.get("/{committee_id}/transfers", response_model=List[CommitteeTransfer])
async def get_committee_transfers(
    committee_id: str,
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum results"),
    service: CommitteeService = Depends(get_committee_service)
):
    """Get committee-to-committee transfers"""
    try:
        transfers = await service.get_committee_transfers(
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date,
            limit=limit
        )
        
        transfer_list = []
        for transfer in transfers:
            transfer_data = {
                "transfer_id": transfer.get("sub_id") or transfer.get("expenditure_id", ""),
                "from_committee_id": committee_id,
                "to_committee_id": transfer.get("recipient_committee_id") or transfer.get("committee_id", ""),
                "amount": float(transfer.get("expenditure_amount", 0) or 0),
                "date": transfer.get("expenditure_date") or transfer.get("disbursement_date", ""),
                "purpose": transfer.get("expenditure_purpose", "")
            }
            transfer_list.append(CommitteeTransfer(**transfer_data))
        
        return transfer_list
    except Exception as e:
        logger.error(f"Error getting transfers for committee {committee_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get transfers: {str(e)}")

