from fastapi import APIRouter, HTTPException, Query, Body, Depends
from typing import Optional, List, Dict
import asyncio
from app.services.fec_client import FECClient
from app.models.schemas import CandidateSummary, FinancialSummary, BatchFinancialsRequest, ContactInformation
from app.services.analysis import AnalysisService
from app.api.dependencies import get_fec_client, get_analysis_service
from app.utils.date_utils import serialize_datetime
from app.utils.logging import get_logger

logger = get_logger(__name__)

router = APIRouter()


def extract_candidate_contact_info(candidate: Dict) -> Optional[ContactInformation]:
    """Extract contact information from candidate data.
    
    This handles both local DB format (already extracted) and API format (needs extraction).
    The FEC API may return contact info in fields like principal_committee_street_1,
    principal_committee_city, etc., rather than street_address, city, etc.
    
    Matches the extraction logic in fec_client._extract_candidate_contact_info()
    """
    # If candidate already has a contact_info dict (from local DB with processed data), use it
    if isinstance(candidate.get("contact_info"), dict):
        contact_dict = candidate.get("contact_info")
        if any([
            contact_dict.get("street_address"),
            contact_dict.get("city"),
            contact_dict.get("zip"),
            contact_dict.get("email"),
            contact_dict.get("phone"),
            contact_dict.get("website")
        ]):
            return ContactInformation(
                street_address=contact_dict.get("street_address"),
                city=contact_dict.get("city"),
                state=contact_dict.get("state"),
                zip=contact_dict.get("zip"),
                email=contact_dict.get("email"),
                phone=contact_dict.get("phone"),
                website=contact_dict.get("website")
            )
    
    # Try multiple field name variations to find contact info
    # This matches the logic in fec_client._extract_candidate_contact_info()
    # First check direct fields (from local DB), then check API field variations
    extracted_contact = {
        "street_address": (
            candidate.get("street_address") or
            candidate.get("principal_committee_street_1") or 
            candidate.get("principal_committee_street_address") or
            candidate.get("mailing_address") or
            candidate.get("street_1") or
            candidate.get("address") or
            candidate.get("principal_committee_street") or
            candidate.get("candidate_street_1") or
            candidate.get("street_address_1") or
            candidate.get("street_address_2")
        ),
        "city": (
            candidate.get("city") or
            candidate.get("principal_committee_city") or
            candidate.get("mailing_city") or
            candidate.get("candidate_city")
        ),
        "state": (
            candidate.get("state") or
            candidate.get("principal_committee_state") or
            candidate.get("mailing_state")
        ),
        "zip": (
            candidate.get("zip") or
            candidate.get("principal_committee_zip") or
            candidate.get("mailing_zip") or
            candidate.get("zip_code") or
            candidate.get("candidate_zip")
        ),
        "email": (
            candidate.get("email") or
            candidate.get("principal_committee_email") or
            candidate.get("candidate_email") or
            candidate.get("e_mail") or
            candidate.get("email_address")
        ),
        "phone": (
            candidate.get("phone") or
            candidate.get("principal_committee_phone") or
            candidate.get("telephone") or
            candidate.get("candidate_phone") or
            candidate.get("phone_number") or
            candidate.get("phone_1")
        ),
        "website": (
            candidate.get("website") or
            candidate.get("principal_committee_website") or
            candidate.get("web_site") or
            candidate.get("url") or
            candidate.get("candidate_website") or
            candidate.get("web_url") or
            candidate.get("website_url")
        )
    }
    
    # Only create ContactInformation if at least one field is present and not empty
    has_contact_info = any([
        extracted_contact.get("street_address"),
        extracted_contact.get("city"),
        extracted_contact.get("zip"),
        extracted_contact.get("email"),
        extracted_contact.get("phone"),
        extracted_contact.get("website")
    ])
    
    if has_contact_info:
        return ContactInformation(
            street_address=extracted_contact.get("street_address"),
            city=extracted_contact.get("city"),
            state=extracted_contact.get("state"),
            zip=extracted_contact.get("zip"),
            email=extracted_contact.get("email"),
            phone=extracted_contact.get("phone"),
            website=extracted_contact.get("website")
        )
    
    return None


@router.get("/search", response_model=List[CandidateSummary])
async def search_candidates(
    name: Optional[str] = Query(None, description="Candidate name to search"),
    office: Optional[str] = Query(None, description="Office type (P, S, H)"),
    state: Optional[str] = Query(None, description="State abbreviation"),
    party: Optional[str] = Query(None, description="Party"),
    year: Optional[int] = Query(None, description="Election year"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results"),
    fec_client: FECClient = Depends(get_fec_client)
):
    """Search for candidates"""
    try:
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
                # Extract contact information
                contact_info = extract_candidate_contact_info(candidate)
                
                # Map API fields to our schema
                candidate_data = {
                    "candidate_id": candidate.get("candidate_id", ""),
                    "name": candidate.get("name", candidate.get("candidate_name", "Unknown")),
                    "office": candidate.get("office"),
                    "party": candidate.get("party"),
                    "state": candidate.get("state"),
                    "district": candidate.get("district"),
                    "election_years": candidate.get("election_years"),
                    "active_through": candidate.get("active_through"),
                    "contact_info": contact_info
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
    limit: int = Query(100, ge=1, le=200, description="Maximum results"),
    fec_client: FECClient = Depends(get_fec_client)
):
    """Get all candidates for a specific race"""
    try:
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
                # Extract contact information
                contact_info = extract_candidate_contact_info(candidate)
                
                candidate_data = {
                    "candidate_id": candidate.get("candidate_id", ""),
                    "name": candidate.get("name", candidate.get("candidate_name", "Unknown")),
                    "office": candidate.get("office"),
                    "party": candidate.get("party"),
                    "state": candidate.get("state"),
                    "district": candidate.get("district"),
                    "election_years": candidate.get("election_years"),
                    "active_through": candidate.get("active_through"),
                    "contact_info": contact_info,
                    "contact_info_updated_at": serialize_datetime(candidate.get("contact_info_updated_at"))
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
async def get_candidate(
    candidate_id: str,
    fec_client: FECClient = Depends(get_fec_client)
):
    """Get candidate details by ID"""
    try:
        candidate = await fec_client.get_candidate(candidate_id)
        if not candidate:
            raise HTTPException(status_code=404, detail="Candidate not found")
        
        # Extract contact information
        contact_info = extract_candidate_contact_info(candidate)
        
        # If contact info is missing, trigger on-demand fetch in background
        if not contact_info:
            # Check if contact info is truly missing (not just empty fields)
            has_any_contact = any([
                candidate.get("street_address"),
                candidate.get("city"),
                candidate.get("zip"),
                candidate.get("email"),
                candidate.get("phone"),
                candidate.get("website"),
                candidate.get("principal_committee_street_1"),
                candidate.get("principal_committee_city"),
                candidate.get("principal_committee_zip"),
            ])
            
            if not has_any_contact:
                # Trigger background refresh (non-blocking)
                async def background_refresh():
                    try:
                        await fec_client.refresh_candidate_contact_info_if_needed(
                            candidate_id,
                            force_refresh=True  # Force refresh to ensure it actually runs
                        )
                        logger.info(f"Background contact info refresh completed for candidate {candidate_id}")
                    except Exception as e:
                        logger.warning(f"Error in background contact info refresh for candidate {candidate_id}: {e}")
                
                # Schedule background task (fire and forget)
                asyncio.create_task(background_refresh())
                logger.debug(f"Triggered on-demand contact info fetch for candidate {candidate_id}")
        
        # Map API response to our schema
        candidate_data = {
            "candidate_id": candidate.get("candidate_id", candidate_id),
            "name": candidate.get("name", candidate.get("candidate_name", "Unknown")),
            "office": candidate.get("office"),
            "party": candidate.get("party"),
            "state": candidate.get("state"),
            "district": candidate.get("district"),
            "election_years": candidate.get("election_years"),
            "active_through": candidate.get("active_through"),
            "contact_info": contact_info,
            "contact_info_updated_at": serialize_datetime(candidate.get("contact_info_updated_at"))
        }
        return CandidateSummary(**candidate_data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting candidate {candidate_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get candidate: {str(e)}")


@router.get("/{candidate_id}/debug-contact")
async def debug_candidate_contact_info(
    candidate_id: str,
    fec_client: FECClient = Depends(get_fec_client)
):
    """Debug endpoint to inspect contact information sources"""
    try:
        from app.db.database import AsyncSessionLocal, Candidate
        from sqlalchemy import select
        
        # Get candidate from database
        db_contact_info = None
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Candidate).where(Candidate.candidate_id == candidate_id)
            )
            db_candidate = result.scalar_one_or_none()
            if db_candidate:
                db_contact_info = {
                    "street_address": db_candidate.street_address,
                    "city": db_candidate.city,
                    "zip": db_candidate.zip,
                    "email": db_candidate.email,
                    "phone": db_candidate.phone,
                    "website": db_candidate.website,
                    "updated_at": db_candidate.updated_at.isoformat() if db_candidate.updated_at else None
                }
        
        # Get candidate from API
        api_candidate = await fec_client.get_candidate(candidate_id)
        api_keys = list(api_candidate.keys()) if api_candidate else []
        api_contact_fields = {}
        if api_candidate:
            for field in ["street_address", "city", "zip", "email", "phone", "website",
                         "principal_committee_street_1", "principal_committee_city", 
                         "principal_committee_zip", "principal_committee_email",
                         "principal_committee_phone", "principal_committee_website",
                         "mailing_address", "street_1", "address"]:
                if field in api_candidate and api_candidate[field]:
                    api_contact_fields[field] = api_candidate[field]
        
        # Extract contact info using our function
        extracted_contact = extract_candidate_contact_info(api_candidate) if api_candidate else None
        
        # Get committees
        committees = await fec_client.get_committees(candidate_id=candidate_id, limit=10)
        committee_contact_info = []
        for committee in committees:
            committee_id = committee.get("committee_id")
            committee_contact = fec_client._extract_committee_contact_info(committee)
            committee_contact_info.append({
                "committee_id": committee_id,
                "committee_name": committee.get("name"),
                "contact_info": committee_contact
            })
        
        # Convert ContactInformation to dict (compatible with both Pydantic v1 and v2)
        extracted_contact_dict = None
        if extracted_contact:
            try:
                # Try Pydantic v2 method first
                extracted_contact_dict = extracted_contact.model_dump()
            except AttributeError:
                # Fall back to Pydantic v1 method
                extracted_contact_dict = extracted_contact.dict()
        
        return {
            "candidate_id": candidate_id,
            "database_contact_info": db_contact_info,
            "api_response_keys": api_keys[:50],  # First 50 keys
            "api_contact_fields_found": api_contact_fields,
            "extracted_contact_info": extracted_contact_dict,
            "committees_found": len(committees),
            "committee_contact_info": committee_contact_info
        }
    except Exception as e:
        logger.error(f"Error in debug endpoint for candidate {candidate_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Debug failed: {str(e)}")


@router.post("/{candidate_id}/refresh-contact-info")
async def refresh_candidate_contact_info(
    candidate_id: str,
    fec_client: FECClient = Depends(get_fec_client)
):
    """Manually refresh contact information for a candidate from the FEC API"""
    logger.info(f"Received request to refresh contact info for candidate {candidate_id}")
    try:
        # Add timeout protection
        import asyncio
        logger.debug(f"Calling refresh_candidate_contact_info_if_needed for {candidate_id} with force_refresh=True")
        
        # Wrap in timeout to prevent hanging
        # Increased timeout to 35 seconds to account for FEC API slowness
        try:
            refreshed = await asyncio.wait_for(
                fec_client.refresh_candidate_contact_info_if_needed(
                    candidate_id,
                    force_refresh=True
                ),
                timeout=35.0  # 35 second timeout (less than 40s frontend timeout)
            )
        except asyncio.TimeoutError:
            logger.error(f"Timeout refreshing contact info for candidate {candidate_id} (exceeded 35s)")
            raise HTTPException(
                status_code=504,
                detail="Contact info refresh timed out after 35 seconds. The FEC API may be slow or unreachable. Please try again later."
            )
        
        logger.info(f"Contact info refresh completed for candidate {candidate_id}, refreshed={refreshed}")
        
        # Always get the current candidate data to return current contact info
        candidate = await fec_client.get_candidate(candidate_id)
        if candidate:
            # Extract contact information
            contact_info = extract_candidate_contact_info(candidate)
            
            if refreshed:
                return {
                    "success": True,
                    "message": "Contact information refreshed successfully",
                    "contact_info": contact_info,
                    "contact_info_updated_at": serialize_datetime(candidate.get("contact_info_updated_at"))
                }
            else:
                return {
                    "success": True,
                    "message": "Contact information is up to date (no refresh needed)",
                    "contact_info": contact_info,
                    "contact_info_updated_at": serialize_datetime(candidate.get("contact_info_updated_at"))
                }
        
        # Fallback if candidate not found
        logger.warning(f"Candidate {candidate_id} not found after refresh attempt")
        return {
            "success": False,
            "message": "Candidate not found",
            "contact_info": None,
            "contact_info_updated_at": None
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error refreshing contact info for candidate {candidate_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to refresh contact info: {str(e)}")


@router.get("/{candidate_id}/financials", response_model=List[FinancialSummary])
async def get_candidate_financials(
    candidate_id: str,
    cycle: Optional[int] = Query(None, description="Election cycle"),
    fec_client: FECClient = Depends(get_fec_client)
):
    """Get candidate financial summary"""
    try:
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
                "party_contributions": float(total.get("party_contributions", 0)),
                "loan_contributions": float(
                    total.get("loan_contributions", 0) or 
                    total.get("loans_received", 0) or 
                    total.get("other_loans_received", 0) or
                    total.get("loans", 0) or
                    total.get("other_loans", 0) or
                    0
                )
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
async def get_batch_financials(
    request: BatchFinancialsRequest,
    fec_client: FECClient = Depends(get_fec_client)
):
    """Get financial summaries for multiple candidates in one request"""
    try:
        
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
                    "party_contributions": float(total.get("party_contributions", 0)),
                    "loan_contributions": float(
                    total.get("loan_contributions", 0) or 
                    total.get("loans_received", 0) or 
                    total.get("other_loans_received", 0) or
                    total.get("loans", 0) or
                    total.get("other_loans", 0) or
                    0
                )
                }
                financials.append(FinancialSummary(**financial_data))
            
            financials_map[candidate_id] = financials
        
        return financials_map
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting batch financials: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get batch financials: {str(e)}")

