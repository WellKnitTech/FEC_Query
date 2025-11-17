"""
Database storage operations for FEC data
"""
import asyncio
import logging
from typing import Dict, Optional
from datetime import datetime
from sqlalchemy import select, and_
from sqlalchemy.orm.attributes import flag_modified
from app.db.database import AsyncSessionLocal, Candidate, Committee, Contribution, FinancialTotal
from app.services.shared.retry import retry_on_db_lock
from app.utils.date_utils import extract_date_from_raw_data

logger = logging.getLogger(__name__)


class StorageManager:
    """Manages database storage operations for FEC data"""
    
    def __init__(self, db_write_semaphore: Optional[asyncio.Semaphore] = None):
        """
        Initialize storage manager
        
        Args:
            db_write_semaphore: Optional semaphore to serialize database writes
        """
        self._db_write_semaphore = db_write_semaphore or asyncio.Semaphore(1)
    
    @retry_on_db_lock(max_retries=3, base_delay=0.1)
    async def store_candidate(self, candidate_data: Dict):
        """Store candidate in local database"""
        async with self._db_write_semaphore:
            async with AsyncSessionLocal() as session:
                candidate_id = candidate_data.get("candidate_id")
                if not candidate_id:
                    return
                
                result = await session.execute(
                    select(Candidate).where(Candidate.candidate_id == candidate_id)
                )
                existing = result.scalar_one_or_none()
                
                contact_info = self._extract_candidate_contact_info(candidate_data)
                
                if existing:
                    # Update existing
                    existing.name = candidate_data.get("name") or candidate_data.get("candidate_name", "")
                    existing.office = candidate_data.get("office")
                    existing.party = candidate_data.get("party")
                    existing.state = candidate_data.get("state")
                    existing.district = candidate_data.get("district")
                    existing.election_years = candidate_data.get("election_years")
                    existing.active_through = candidate_data.get("active_through")
                    if contact_info.get("street_address"):
                        existing.street_address = contact_info["street_address"]
                    if contact_info.get("city"):
                        existing.city = contact_info["city"]
                    if contact_info.get("zip"):
                        existing.zip = contact_info["zip"]
                    if contact_info.get("email"):
                        existing.email = contact_info["email"]
                    if contact_info.get("phone"):
                        existing.phone = contact_info["phone"]
                    if contact_info.get("website"):
                        existing.website = contact_info["website"]
                    existing.raw_data = candidate_data
                    existing.updated_at = datetime.utcnow()
                else:
                    # Create new
                    try:
                        candidate = Candidate(
                            candidate_id=candidate_id,
                            name=candidate_data.get("name") or candidate_data.get("candidate_name", ""),
                            office=candidate_data.get("office"),
                            party=candidate_data.get("party"),
                            state=candidate_data.get("state"),
                            district=candidate_data.get("district"),
                            election_years=candidate_data.get("election_years"),
                            active_through=candidate_data.get("active_through"),
                            street_address=contact_info.get("street_address"),
                            city=contact_info.get("city"),
                            zip=contact_info.get("zip"),
                            email=contact_info.get("email"),
                            phone=contact_info.get("phone"),
                            website=contact_info.get("website"),
                            raw_data=candidate_data
                        )
                        session.add(candidate)
                    except Exception as e:
                        # Handle unique constraint race condition
                        if "UNIQUE constraint" in str(e) or "unique constraint" in str(e).lower():
                            await session.rollback()
                            # Try to update instead
                            result = await session.execute(
                                select(Candidate).where(Candidate.candidate_id == candidate_id)
                            )
                            existing = result.scalar_one_or_none()
                            if existing:
                                existing.name = candidate_data.get("name") or candidate_data.get("candidate_name", "")
                                existing.office = candidate_data.get("office")
                                existing.party = candidate_data.get("party")
                                existing.state = candidate_data.get("state")
                                existing.district = candidate_data.get("district")
                                existing.election_years = candidate_data.get("election_years")
                                existing.active_through = candidate_data.get("active_through")
                                existing.raw_data = candidate_data
                                existing.updated_at = datetime.utcnow()
                            else:
                                raise
                        else:
                            raise
                
                await session.commit()
    
    @retry_on_db_lock(max_retries=3, base_delay=0.1)
    async def store_financial_total(self, candidate_id: str, financial_data: Dict):
        """Store financial total in local database"""
        async with self._db_write_semaphore:
            async with AsyncSessionLocal() as session:
                cycle = financial_data.get("cycle") or financial_data.get("two_year_transaction_period")
                if not cycle:
                    return
                
                result = await session.execute(
                    select(FinancialTotal).where(
                        and_(
                            FinancialTotal.candidate_id == candidate_id,
                            FinancialTotal.cycle == cycle
                        )
                    )
                )
                existing = result.scalar_one_or_none()
                
                if existing:
                    # Update existing
                    existing.total_receipts = float(financial_data.get("receipts", 0))
                    existing.total_disbursements = float(financial_data.get("disbursements", 0))
                    existing.cash_on_hand = float(financial_data.get("cash_on_hand_end_period", 0))
                    existing.total_contributions = float(financial_data.get("contributions", 0))
                    existing.individual_contributions = float(financial_data.get("individual_contributions", 0))
                    existing.pac_contributions = float(financial_data.get("pac_contributions", 0))
                    existing.party_contributions = float(financial_data.get("party_contributions", 0))
                    existing.loan_contributions = float(
                        financial_data.get("loan_contributions", 0) or 
                        financial_data.get("loans_received", 0) or 0
                    )
                    existing.raw_data = financial_data
                    existing.updated_at = datetime.utcnow()
                else:
                    # Create new
                    financial = FinancialTotal(
                        candidate_id=candidate_id,
                        cycle=cycle,
                        total_receipts=float(financial_data.get("receipts", 0)),
                        total_disbursements=float(financial_data.get("disbursements", 0)),
                        cash_on_hand=float(financial_data.get("cash_on_hand_end_period", 0)),
                        total_contributions=float(financial_data.get("contributions", 0)),
                        individual_contributions=float(financial_data.get("individual_contributions", 0)),
                        pac_contributions=float(financial_data.get("pac_contributions", 0)),
                        party_contributions=float(financial_data.get("party_contributions", 0)),
                        loan_contributions=float(
                            financial_data.get("loan_contributions", 0) or 
                            financial_data.get("loans_received", 0) or 0
                        ),
                        raw_data=financial_data
                    )
                    session.add(financial)
                
                await session.commit()
    
    @retry_on_db_lock(max_retries=3, base_delay=0.1)
    async def store_contribution(self, contribution_data: Dict, smart_merge_func):
        """Store contribution in local database"""
        async with self._db_write_semaphore:
            async with AsyncSessionLocal() as session:
                # Extract contribution ID
                contrib_id = (
                    contribution_data.get('sub_id') or 
                    contribution_data.get('contribution_id') or
                    contribution_data.get('transaction_id')
                )
                
                if not contrib_id:
                    logger.debug("Skipping contribution without ID")
                    return
                
                # Check if contribution already exists
                existing = await session.execute(
                    select(Contribution).where(Contribution.contribution_id == contrib_id)
                )
                existing_contrib = existing.scalar_one_or_none()
                
                # Extract amount from multiple possible fields
                amount = 0.0
                for amt_key in ['contb_receipt_amt', 'contribution_amount', 'contribution_receipt_amount', 'amount', 'contribution_receipt_amt']:
                    amt_val = contribution_data.get(amt_key)
                    if amt_val is not None:
                        try:
                            amount = float(amt_val)
                            if amount > 0:
                                break
                        except (ValueError, TypeError):
                            continue
                
                # Extract contributor name
                contrib_name = (
                    contribution_data.get('contributor_name') or 
                    contribution_data.get('contributor') or 
                    contribution_data.get('name') or
                    contribution_data.get('contributor_name_1')
                )
                
                # Parse contribution date
                contrib_date = extract_date_from_raw_data(contribution_data)
                if not contrib_date:
                    date_str = (
                        contribution_data.get('contribution_receipt_date') or 
                        contribution_data.get('contribution_date') or 
                        contribution_data.get('receipt_date')
                    )
                    if date_str:
                        try:
                            if isinstance(date_str, str):
                                if 'T' in date_str:
                                    contrib_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                else:
                                    contrib_date = datetime.strptime(date_str, '%Y-%m-%d')
                            else:
                                contrib_date = date_str
                        except (ValueError, TypeError):
                            pass
                
                if existing_contrib:
                    # Use smart merge for existing contributions
                    smart_merge_func(existing_contrib, contribution_data, 'api')
                    
                    # Update date if extracted and existing doesn't have one
                    if contrib_date and not existing_contrib.contribution_date:
                        existing_contrib.contribution_date = contrib_date
                        logger.debug(f"Updated contribution_date for existing contribution {contrib_id} from API response: {contrib_date}")
                    elif contrib_date and existing_contrib.contribution_date != contrib_date:
                        existing_contrib.contribution_date = contrib_date
                        logger.debug(f"Updated contribution_date for existing contribution {contrib_id} from API response (was {existing_contrib.contribution_date}, now {contrib_date})")
                    
                    logger.debug(f"Updated existing contribution {contrib_id} using smart merge (may be an amendment)")
                else:
                    # Create new contribution
                    contribution = Contribution(
                        contribution_id=contrib_id,
                        candidate_id=contribution_data.get('candidate_id'),
                        committee_id=contribution_data.get('committee_id'),
                        contributor_name=contrib_name,
                        contributor_city=contribution_data.get('contributor_city'),
                        contributor_state=contribution_data.get('contributor_state'),
                        contributor_zip=contribution_data.get('contributor_zip'),
                        contributor_employer=contribution_data.get('contributor_employer'),
                        contributor_occupation=contribution_data.get('contributor_occupation'),
                        contribution_amount=amount,
                        contribution_date=contrib_date,
                        contribution_type=contribution_data.get('contribution_type') or contribution_data.get('transaction_type'),
                        raw_data=contribution_data,
                        data_source='api',
                        last_updated_from='api'
                    )
                    session.add(contribution)
                
                await session.commit()
    
    @retry_on_db_lock(max_retries=3, base_delay=0.1)
    async def store_committee(self, committee_data: Dict):
        """Store committee in local database"""
        async with self._db_write_semaphore:
            async with AsyncSessionLocal() as session:
                committee_id = committee_data.get("committee_id")
                if not committee_id:
                    return
                
                result = await session.execute(
                    select(Committee).where(Committee.committee_id == committee_id)
                )
                existing = result.scalar_one_or_none()
                
                contact_info = self._extract_committee_contact_info(committee_data)
                
                if existing:
                    # Update existing
                    existing.name = committee_data.get("name", "")
                    existing.committee_type = committee_data.get("committee_type")
                    existing.committee_type_full = committee_data.get("committee_type_full")
                    existing.candidate_ids = committee_data.get("candidate_ids") or []
                    existing.party = committee_data.get("party")
                    existing.state = committee_data.get("state")
                    if contact_info.get("street_address"):
                        existing.street_address = contact_info["street_address"]
                    if contact_info.get("street_address_2"):
                        existing.street_address_2 = contact_info["street_address_2"]
                    if contact_info.get("city"):
                        existing.city = contact_info["city"]
                    if contact_info.get("zip"):
                        existing.zip = contact_info["zip"]
                    if contact_info.get("email"):
                        existing.email = contact_info["email"]
                    if contact_info.get("phone"):
                        existing.phone = contact_info["phone"]
                    if contact_info.get("website"):
                        existing.website = contact_info["website"]
                    if contact_info.get("treasurer_name"):
                        existing.treasurer_name = contact_info["treasurer_name"]
                    existing.raw_data = committee_data
                    existing.updated_at = datetime.utcnow()
                    await session.commit()
                else:
                    # Create new - handle race condition
                    try:
                        committee = Committee(
                            committee_id=committee_id,
                            name=committee_data.get("name", ""),
                            committee_type=committee_data.get("committee_type"),
                            committee_type_full=committee_data.get("committee_type_full"),
                            candidate_ids=committee_data.get("candidate_ids") or [],
                            party=committee_data.get("party"),
                            state=committee_data.get("state"),
                            street_address=contact_info.get("street_address"),
                            street_address_2=contact_info.get("street_address_2"),
                            city=contact_info.get("city"),
                            zip=contact_info.get("zip"),
                            email=contact_info.get("email"),
                            phone=contact_info.get("phone"),
                            website=contact_info.get("website"),
                            treasurer_name=contact_info.get("treasurer_name"),
                            raw_data=committee_data
                        )
                        session.add(committee)
                        await session.commit()
                    except Exception as insert_error:
                        # Handle unique constraint race condition
                        if "UNIQUE constraint" in str(insert_error) or "unique constraint" in str(insert_error).lower():
                            await session.rollback()
                            result = await session.execute(
                                select(Committee).where(Committee.committee_id == committee_id)
                            )
                            existing = result.scalar_one_or_none()
                            if existing:
                                existing.name = committee_data.get("name", "")
                                existing.committee_type = committee_data.get("committee_type")
                                existing.committee_type_full = committee_data.get("committee_type_full")
                                existing.candidate_ids = committee_data.get("candidate_ids") or []
                                existing.party = committee_data.get("party")
                                existing.state = committee_data.get("state")
                                if contact_info.get("street_address"):
                                    existing.street_address = contact_info["street_address"]
                                if contact_info.get("street_address_2"):
                                    existing.street_address_2 = contact_info["street_address_2"]
                                if contact_info.get("city"):
                                    existing.city = contact_info["city"]
                                if contact_info.get("zip"):
                                    existing.zip = contact_info["zip"]
                                if contact_info.get("email"):
                                    existing.email = contact_info["email"]
                                if contact_info.get("phone"):
                                    existing.phone = contact_info["phone"]
                                if contact_info.get("website"):
                                    existing.website = contact_info["website"]
                                if contact_info.get("treasurer_name"):
                                    existing.treasurer_name = contact_info["treasurer_name"]
                                existing.raw_data = committee_data
                                existing.updated_at = datetime.utcnow()
                                await session.commit()
                            else:
                                raise
                        else:
                            raise
    
    def _extract_candidate_contact_info(self, candidate_data: Dict) -> Dict:
        """Extract contact information from candidate API response"""
        contact_info = {}
        
        contact_info["street_address"] = (
            candidate_data.get("street_address") or
            candidate_data.get("principal_committee_street_1") or 
            candidate_data.get("principal_committee_street_address") or
            candidate_data.get("mailing_address") or
            candidate_data.get("street_1") or
            candidate_data.get("address") or
            candidate_data.get("principal_committee_street") or
            candidate_data.get("candidate_street_1")
        )
        
        contact_info["city"] = (
            candidate_data.get("city") or
            candidate_data.get("principal_committee_city") or
            candidate_data.get("mailing_city") or
            candidate_data.get("candidate_city")
        )
        
        contact_info["zip"] = (
            candidate_data.get("zip") or
            candidate_data.get("principal_committee_zip") or
            candidate_data.get("mailing_zip") or
            candidate_data.get("zip_code") or
            candidate_data.get("candidate_zip")
        )
        
        contact_info["email"] = (
            candidate_data.get("email") or
            candidate_data.get("principal_committee_email") or
            candidate_data.get("candidate_email") or
            candidate_data.get("e_mail")
        )
        
        contact_info["phone"] = (
            candidate_data.get("phone") or
            candidate_data.get("principal_committee_phone") or
            candidate_data.get("telephone") or
            candidate_data.get("candidate_phone") or
            candidate_data.get("phone_number")
        )
        
        contact_info["website"] = (
            candidate_data.get("website") or
            candidate_data.get("principal_committee_website") or
            candidate_data.get("web_site") or
            candidate_data.get("url") or
            candidate_data.get("candidate_website") or
            candidate_data.get("web_url")
        )
        
        return contact_info
    
    def _extract_committee_contact_info(self, committee_data: Dict) -> Dict:
        """Extract contact information from committee API response"""
        contact_info = {}
        
        contact_info["street_address"] = (
            committee_data.get("street_1") or
            committee_data.get("street_address") or
            committee_data.get("address") or
            committee_data.get("mailing_address")
        )
        
        contact_info["street_address_2"] = (
            committee_data.get("street_2") or
            committee_data.get("street_address_2") or
            committee_data.get("address_2")
        )
        
        contact_info["city"] = (
            committee_data.get("city") or
            committee_data.get("mailing_city")
        )
        
        contact_info["zip"] = (
            committee_data.get("zip") or
            committee_data.get("zip_code") or
            committee_data.get("mailing_zip")
        )
        
        contact_info["email"] = (
            committee_data.get("email") or
            committee_data.get("e_mail")
        )
        
        contact_info["phone"] = (
            committee_data.get("phone") or
            committee_data.get("telephone") or
            committee_data.get("phone_number")
        )
        
        contact_info["website"] = (
            committee_data.get("website") or
            committee_data.get("web_site") or
            committee_data.get("url") or
            committee_data.get("web_url")
        )
        
        contact_info["treasurer_name"] = (
            committee_data.get("treasurer_name") or
            committee_data.get("treasurer")
        )
        
        return contact_info

