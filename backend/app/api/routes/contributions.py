from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List
from app.services.fec_client import FECClient
from app.models.schemas import Contribution, ContributionAnalysis, AggregatedDonor
from app.services.analysis import AnalysisService
from app.services.donor_aggregation import DonorAggregationService
from app.api.dependencies import get_fec_client, get_analysis_service
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_model=List[Contribution])
async def get_contributions(
    candidate_id: Optional[str] = Query(None, description="Candidate ID", max_length=20, regex="^[A-Z0-9]*$"),
    committee_id: Optional[str] = Query(None, description="Committee ID", max_length=20, regex="^[A-Z0-9]*$"),
    contributor_name: Optional[str] = Query(None, description="Contributor name", max_length=200),
    min_amount: Optional[float] = Query(None, description="Minimum contribution amount", ge=0),
    max_amount: Optional[float] = Query(None, description="Maximum contribution amount", ge=0),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)", regex="^\\d{4}-\\d{2}-\\d{2}$"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)", regex="^\\d{4}-\\d{2}-\\d{2}$"),
    limit: int = Query(100, ge=1, le=10000, description="Maximum results"),
    fec_client: FECClient = Depends(get_fec_client)
):
    """Get contributions"""
    try:
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
                # Extract amount from multiple possible field names (FEC API uses contb_receipt_amt)
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
                
                # Also check contributor_name field variations (FEC API uses contributor)
                contributor_name = contrib.get("contributor_name") or contrib.get("contributor") or contrib.get("name") or contrib.get("contributor_name_1")
                
                # Map common field name variations
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
                    "contribution_date": contrib.get("contribution_receipt_date") or contrib.get("contribution_date") or contrib.get("receipt_date"),
                    "contribution_type": contrib.get("contribution_type") or contrib.get("transaction_type"),
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


@router.get("/unique-contributors")
async def get_unique_contributors(
    search_term: str = Query(..., description="Search term for contributor name (e.g., 'Smith')", min_length=1, max_length=200),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    fec_client: FECClient = Depends(get_fec_client)
):
    """Get unique contributor names matching a search term"""
    # Validate and sanitize search term
    search_term = search_term.strip()
    if not search_term or len(search_term) > 200:
        raise HTTPException(
            status_code=400,
            detail="Search term must be between 1 and 200 characters"
        )
    # Remove any potentially dangerous characters (basic sanitization)
    # SQLAlchemy will handle parameterization, but we'll sanitize for safety
    search_term = search_term.replace(";", "").replace("--", "").replace("/*", "").replace("*/", "")
    
    try:
        from app.db.database import AsyncSessionLocal
        from app.db.database import Contribution
        from sqlalchemy import select, func, distinct
        from collections import defaultdict
        
        contributors_dict = defaultdict(lambda: {"total_amount": 0.0, "contribution_count": 0})
        
        # First, search local database
        try:
            async with AsyncSessionLocal() as session:
                query = select(
                    distinct(Contribution.contributor_name),
                    func.sum(Contribution.contribution_amount).label('total_amount'),
                    func.count(Contribution.id).label('contribution_count')
                ).where(
                    Contribution.contributor_name.ilike(f"%{search_term}%")
                ).group_by(
                    Contribution.contributor_name
                ).order_by(
                    func.sum(Contribution.contribution_amount).desc()
                ).limit(limit * 2)  # Get more to account for API results
                
                result = await session.execute(query)
                for row in result:
                    if row.contributor_name:  # Skip None/empty names
                        contributors_dict[row.contributor_name] = {
                            "total_amount": float(row.total_amount or 0),
                            "contribution_count": int(row.contribution_count or 0)
                        }
        except Exception as db_error:
            logger.warning(f"Error querying local database for unique contributors: {db_error}")
        
        # Also query FEC API to get contributions matching the search term
        # This ensures we find contributors even if they're not in the local database yet
        try:
            fec_client = get_fec_client()
            logger.info(f"Querying FEC API for contributions matching '{search_term}'")
            
            # The FEC API's contributor_name parameter may require exact matches
            # So we'll try multiple strategies:
            # 1. Try exact search first
            # 2. If that doesn't work well, fetch without name filter and filter client-side
            # 3. Try searching by last name variations
            
            # Strategy 1: Try exact/partial search via API
            api_contributions = await fec_client.get_contributions(
                contributor_name=search_term,
                limit=5000  # Get a good sample to extract unique names
            )
            
            logger.info(f"FEC API returned {len(api_contributions)} contributions for exact search '{search_term}'")
            
            # Strategy 2: If we got few results and search term looks like a last name (single word),
            # check the database first (which may have cached contributions from previous searches)
            # before doing expensive API calls
            if len(api_contributions) < 100 and len(search_term.split()) == 1:
                logger.info(f"Search term '{search_term}' appears to be a last name, checking database cache first")
                
                # First, try querying the database with a broader search
                try:
                    from app.db.database import AsyncSessionLocal
                    from app.db.database import Contribution
                    from sqlalchemy import select, func, distinct
                    
                    async with AsyncSessionLocal() as session:
                        # Search database for any contributions matching the search term
                        db_query = select(Contribution).where(
                            Contribution.contributor_name.ilike(f"%{search_term}%")
                        ).limit(5000)
                        
                        db_result = await session.execute(db_query)
                        db_contribs = db_result.scalars().all()
                        
                        if db_contribs:
                            logger.info(f"Found {len(db_contribs)} cached contributions in database matching '{search_term}'")
                            # Convert database contributions to dict format
                            for c in db_contribs:
                                contrib_dict = {
                                    'contribution_id': c.contribution_id,
                                    'sub_id': c.contribution_id,
                                    'contributor_name': c.contributor_name,
                                    'contributor_city': c.contributor_city,
                                    'contributor_state': c.contributor_state,
                                    'contributor_zip': c.contributor_zip,
                                    'contributor_employer': c.contributor_employer,
                                    'contributor_occupation': c.contributor_occupation,
                                    'contribution_amount': c.contribution_amount or 0,
                                    'contribution_date': c.contribution_date.strftime('%Y-%m-%d') if c.contribution_date else None,
                                    'contribution_receipt_date': c.contribution_date.strftime('%Y-%m-%d') if c.contribution_date else None,
                                    'candidate_id': c.candidate_id,
                                    'committee_id': c.committee_id,
                                    'contribution_type': c.contribution_type
                                }
                                if c.raw_data:
                                    contrib_dict.update(c.raw_data)
                                
                                # Check if already in api_contributions
                                contrib_id = contrib_dict.get('contribution_id') or contrib_dict.get('sub_id')
                                if not any((c.get('contribution_id') == contrib_id or c.get('sub_id') == contrib_id) 
                                          for c in api_contributions):
                                    api_contributions.append(contrib_dict)
                            
                            logger.info(f"Total contributions after database merge: {len(api_contributions)}")
                except Exception as db_error:
                    logger.warning(f"Error querying database cache: {db_error}")
                
                # Only do expensive API call if we still have very few results
                if len(api_contributions) < 50:
                    logger.info(f"Still have few results ({len(api_contributions)}), trying broader API search")
                    # Fetch contributions without name filter (but with reasonable limits)
                    # We'll filter by name client-side
                    # Use fetch_new_only=False to get all contributions for this search
                    broader_contributions = await fec_client.get_contributions(
                        limit=5000,  # Reduced from 10000 to be more reasonable
                        fetch_new_only=False  # For search purposes, get all matching contributions
                    )
                    logger.info(f"Fetched {len(broader_contributions)} contributions for client-side filtering")
                    
                    # Filter by search term client-side
                    filtered_contributions = []
                    search_term_lower = search_term.lower().strip()
                    existing_ids = {c.get('contribution_id') or c.get('sub_id') for c in api_contributions}
                    
                    for contrib in broader_contributions:
                        contrib_id = contrib.get('contribution_id') or contrib.get('sub_id')
                        if contrib_id in existing_ids:
                            continue
                            
                        contrib_name = (
                            contrib.get('contributor_name') or 
                            contrib.get('contributor') or 
                            contrib.get('name') or
                            contrib.get('contributor_name_1')
                        )
                        if contrib_name:
                            name_lower = str(contrib_name).lower()
                            name_words = name_lower.split()
                            # Match if search term is in name or matches any word
                            if (search_term_lower in name_lower or 
                                any(search_term_lower == word or word.startswith(search_term_lower) for word in name_words)):
                                filtered_contributions.append(contrib)
                                existing_ids.add(contrib_id)
                    
                    logger.info(f"Client-side filtering found {len(filtered_contributions)} matching contributions")
                    api_contributions.extend(filtered_contributions)
                    logger.info(f"Total unique contributions after merging: {len(api_contributions)}")
            
            # Aggregate by contributor name from API results
            search_term_lower = search_term.lower().strip()
            for contrib in api_contributions:
                # Try multiple field names for contributor name
                contrib_name = (
                    contrib.get('contributor_name') or 
                    contrib.get('contributor') or 
                    contrib.get('name') or
                    contrib.get('contributor_name_1') or
                    contrib.get('CONTRIBUTOR_NAME') or
                    contrib.get('contributor_last_name')  # Some APIs use this
                )
                
                if contrib_name:
                    contrib_name_str = str(contrib_name).strip()
                    # Check if search term matches (case-insensitive)
                    # Match if search term is in the name, or if name words contain the search term
                    name_lower = contrib_name_str.lower()
                    name_words = name_lower.split()
                    
                    # Match if:
                    # 1. Search term is a substring of the name
                    # 2. Search term matches any word in the name (for last name searches)
                    # 3. Search term matches the beginning of any word
                    matches = (
                        search_term_lower in name_lower or
                        any(search_term_lower == word or word.startswith(search_term_lower) for word in name_words)
                    )
                    
                    if matches:
                        amount = contrib.get('contribution_amount', 0) or 0
                        if isinstance(amount, str):
                            try:
                                amount = float(amount)
                            except (ValueError, TypeError):
                                amount = 0.0
                        
                        # Use the original name format (not lowercased) for consistency
                        if contrib_name_str not in contributors_dict:
                            contributors_dict[contrib_name_str] = {
                                "total_amount": 0.0,
                                "contribution_count": 0
                            }
                        
                        contributors_dict[contrib_name_str]["total_amount"] += amount
                        contributors_dict[contrib_name_str]["contribution_count"] += 1
        except Exception as api_error:
            logger.warning(f"Error querying FEC API for unique contributors: {api_error}")
        
        # Convert to list and sort by total amount
        contributors = [
            {
                "name": name,
                "total_amount": data["total_amount"],
                "contribution_count": data["contribution_count"]
            }
            for name, data in sorted(
                contributors_dict.items(),
                key=lambda x: x[1]["total_amount"],
                reverse=True
            )
        ]
        
        # Apply limit
        return contributors[:limit]
    except Exception as e:
        logger.error(f"Error getting unique contributors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get unique contributors: {str(e)}")


@router.get("/aggregated-donors", response_model=List[AggregatedDonor])
async def get_aggregated_donors(
    candidate_id: Optional[str] = Query(None, description="Candidate ID"),
    committee_id: Optional[str] = Query(None, description="Committee ID"),
    contributor_name: Optional[str] = Query(None, description="Contributor name"),
    min_amount: Optional[float] = Query(None, description="Minimum contribution amount"),
    max_amount: Optional[float] = Query(None, description="Maximum contribution amount"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    fec_client: FECClient = Depends(get_fec_client)
):
    """Get aggregated donors (grouped by name variations)"""
    try:
        # Fetch contributions using existing filters
        contributions = await fec_client.get_contributions(
            candidate_id=candidate_id,
            committee_id=committee_id,
            contributor_name=contributor_name,
            min_amount=min_amount,
            max_amount=max_amount,
            min_date=min_date,
            max_date=max_date,
            limit=10000  # Get more contributions for better aggregation
        )
        
        logger.info(f"Fetching aggregated donors: contributor_name={contributor_name}, limit={limit}")
        
        if not contributions:
            logger.info(f"No contributions found for aggregated donors query")
            return []
        
        logger.info(f"Found {len(contributions)} contributions to aggregate")
        
        # Convert contributions to dictionaries for aggregation service
        contrib_dicts = []
        for contrib in contributions:
            # Extract contributor name from multiple possible fields
            contrib_name = (
                contrib.get('contributor_name') or 
                contrib.get('contributor') or 
                contrib.get('name') or
                contrib.get('contributor_name_1')
            )
            
            # Only include contributions with a valid name
            if not contrib_name:
                continue
            
            # Extract amount from multiple possible field names (FEC API uses contb_receipt_amt)
            amount = 0.0
            for amt_key in ['contb_receipt_amt', 'contribution_amount', 'contribution_receipt_amount', 'amount', 'contribution_receipt_amt']:
                amt_val = contrib.get(amt_key)
                if amt_val is not None:
                    try:
                        amount = float(amt_val)
                        # Use the first valid numeric value found, even if it's 0
                        break
                    except (ValueError, TypeError):
                        continue
                
            contrib_dict = {
                'contribution_id': contrib.get('contribution_id') or contrib.get('sub_id'),
                'contributor_name': contrib_name,
                'contributor_city': contrib.get('contributor_city') or contrib.get('city'),
                'contributor_state': contrib.get('contributor_state') or contrib.get('state'),
                'contributor_zip': contrib.get('contributor_zip') or contrib.get('zip'),
                'contributor_employer': contrib.get('contributor_employer') or contrib.get('employer'),
                'contributor_occupation': contrib.get('contributor_occupation') or contrib.get('occupation'),
                'contribution_amount': amount,
                'contribution_date': contrib.get('contribution_date') or contrib.get('contribution_receipt_date')
            }
            contrib_dicts.append(contrib_dict)
        
        logger.info(f"Processing {len(contrib_dicts)} contributions for aggregation")
        
        if not contrib_dicts:
            logger.warning("No valid contributions with names found for aggregation")
            return []
        
        # Aggregate donors
        aggregation_service = DonorAggregationService()
        aggregated = aggregation_service.aggregate_donors(contrib_dicts)
        
        logger.info(f"Aggregated {len(contrib_dicts)} contributions into {len(aggregated)} unique donors")
        
        # Apply limit and return
        result = [AggregatedDonor(**donor) for donor in aggregated[:limit]]
        logger.info(f"Returning {len(result)} aggregated donors")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting aggregated donors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get aggregated donors: {str(e)}")


@router.get("/analysis", response_model=ContributionAnalysis)
async def analyze_contributions(
    candidate_id: Optional[str] = Query(None, description="Candidate ID"),
    committee_id: Optional[str] = Query(None, description="Committee ID"),
    min_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    max_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    cycle: Optional[int] = Query(None, description="Election cycle (two_year_transaction_period)"),
    analysis_service: AnalysisService = Depends(get_analysis_service)
):
    """Analyze contributions with aggregations"""
    try:
        analysis = await analysis_service.analyze_contributions(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date,
            cycle=cycle
        )
        return analysis
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing contributions: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to analyze contributions: {str(e)}")

