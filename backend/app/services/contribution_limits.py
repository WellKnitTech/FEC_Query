"""
Service for managing FEC contribution limits by year and contributor category
"""
import logging
from typing import Optional, Dict, List
from datetime import datetime
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import ContributionLimit
from app.utils.transaction_types import get_contributor_category_from_code

logger = logging.getLogger(__name__)


class ContributionLimitsService:
    """Service for managing and querying FEC contribution limits"""
    
    # Contributor categories
    CONTRIBUTOR_INDIVIDUAL = "individual"
    CONTRIBUTOR_MULTICANDIDATE_PAC = "multicandidate_pac"
    CONTRIBUTOR_NON_MULTICANDIDATE_PAC = "non_multicandidate_pac"
    CONTRIBUTOR_PARTY_COMMITTEE = "party_committee"
    CONTRIBUTOR_CANDIDATE_COMMITTEE = "candidate_committee"
    
    # Recipient categories
    RECIPIENT_CANDIDATE = "candidate"
    RECIPIENT_PAC = "pac"
    RECIPIENT_PARTY_COMMITTEE = "party_committee"
    RECIPIENT_NATIONAL_PARTY = "national_party"
    
    # Limit types
    LIMIT_TYPE_PER_ELECTION = "per_election"
    LIMIT_TYPE_PER_YEAR = "per_year"
    LIMIT_TYPE_PER_CALENDAR_YEAR = "per_calendar_year"
    
    def __init__(self, db_session: AsyncSession):
        self.db = db_session
    
    def _get_effective_year(self, date: datetime) -> int:
        """
        Get the effective year for contribution limits based on a date.
        Limits change on Jan 1 of odd-numbered years.
        
        Args:
            date: The date to check
            
        Returns:
            The effective year (the year in which limits took effect)
        """
        year = date.year
        
        # Limits are set in odd-numbered years, effective Jan 1
        # If the date is in an even year, use the previous odd year
        # If the date is in an odd year, use that year
        if year % 2 == 0:
            # Even year - use previous odd year's limits
            return year - 1
        else:
            # Odd year - use this year's limits
            return year
    
    async def get_limit(
        self,
        date: datetime,
        contributor_category: str,
        recipient_category: str = RECIPIENT_CANDIDATE,
        limit_type: str = LIMIT_TYPE_PER_ELECTION
    ) -> Optional[float]:
        """
        Get the contribution limit for a given date, contributor category, and recipient category.
        
        Args:
            date: The date of the contribution
            contributor_category: Type of contributor (individual, multicandidate_pac, etc.)
            recipient_category: Type of recipient (candidate, pac, etc.)
            limit_type: Type of limit (per_election, per_year, etc.)
            
        Returns:
            The limit amount in dollars, or None if not found
        """
        effective_year = self._get_effective_year(date)
        
        try:
            result = await self.db.execute(
                select(ContributionLimit.limit_amount).where(
                    and_(
                        ContributionLimit.effective_year == effective_year,
                        ContributionLimit.contributor_category == contributor_category,
                        ContributionLimit.recipient_category == recipient_category,
                        ContributionLimit.limit_type == limit_type
                    )
                )
            )
            limit = result.scalar_one_or_none()
            return float(limit) if limit is not None else None
        except Exception as e:
            logger.error(f"Error getting contribution limit: {e}")
            return None
    
    @staticmethod
    def _infer_contributor_category(
        contribution_type_code: Optional[str] = None,
        committee_type: Optional[str] = None,
        has_employer_occupation: bool = False
    ) -> str:
        """
        Infer contributor category from available FEC data fields.
        
        Uses centralized transaction type parser to determine if the contributor
        is an individual, PAC, party committee, etc.
        
        Args:
            contribution_type_code: FEC transaction type code (TRAN_TP)
            committee_type: Committee type code (CMTE_TP)
            has_employer_occupation: Whether employer/occupation fields are present
            
        Returns:
            Contributor category string
        """
        # Use centralized parser for consistent behavior
        return get_contributor_category_from_code(
            contribution_type_code=contribution_type_code,
            committee_type=committee_type,
            has_employer_occupation=has_employer_occupation
        )
    
    async def get_limit_for_contribution(
        self,
        contribution_date: datetime,
        contributor_type: Optional[str] = None,
        contribution_type_code: Optional[str] = None,
        committee_type: Optional[str] = None,
        has_employer_occupation: bool = False
    ) -> Optional[float]:
        """
        Get the appropriate contribution limit for a contribution based on available data.
        
        This method attempts to infer the contributor category from available data:
        - If contributor_type is provided, use it directly
        - Otherwise, try to infer from contribution_type_code or committee_type
        
        Args:
            contribution_date: Date of the contribution
            contributor_type: Explicit contributor type if known
            contribution_type_code: FEC transaction type code (TRAN_TP)
            committee_type: Committee type if contributor is a committee
            has_employer_occupation: Whether employer/occupation fields are present
            
        Returns:
            The limit amount in dollars, or None if cannot be determined
        """
        # Determine contributor category
        contributor_category = contributor_type
        
        if not contributor_category:
            # Infer from available data
            contributor_category = self._infer_contributor_category(
                contribution_type_code=contribution_type_code,
                committee_type=committee_type,
                has_employer_occupation=has_employer_occupation
            )
        
        # Default to candidate recipient and per_election limit
        return await self.get_limit(
            date=contribution_date,
            contributor_category=contributor_category,
            recipient_category=self.RECIPIENT_CANDIDATE,
            limit_type=self.LIMIT_TYPE_PER_ELECTION
        )
    
    async def add_limit(
        self,
        effective_year: int,
        contributor_category: str,
        recipient_category: str,
        limit_amount: float,
        limit_type: str,
        notes: Optional[str] = None
    ) -> ContributionLimit:
        """
        Add a new contribution limit to the database.
        
        Args:
            effective_year: Year the limit takes effect (Jan 1)
            contributor_category: Type of contributor
            recipient_category: Type of recipient
            limit_amount: Limit amount in dollars
            limit_type: Type of limit (per_election, per_year, etc.)
            notes: Optional notes about the limit
            
        Returns:
            The created ContributionLimit object
        """
        # Check if limit already exists
        existing = await self.db.execute(
            select(ContributionLimit).where(
                and_(
                    ContributionLimit.effective_year == effective_year,
                    ContributionLimit.contributor_category == contributor_category,
                    ContributionLimit.recipient_category == recipient_category,
                    ContributionLimit.limit_type == limit_type
                )
            )
        )
        existing_limit = existing.scalar_one_or_none()
        
        if existing_limit:
            # Update existing limit
            existing_limit.limit_amount = limit_amount
            existing_limit.notes = notes
            existing_limit.updated_at = datetime.utcnow()
            await self.db.commit()
            return existing_limit
        else:
            # Create new limit
            new_limit = ContributionLimit(
                effective_year=effective_year,
                contributor_category=contributor_category,
                recipient_category=recipient_category,
                limit_amount=limit_amount,
                limit_type=limit_type,
                notes=notes
            )
            self.db.add(new_limit)
            await self.db.commit()
            await self.db.refresh(new_limit)
            return new_limit
    
    async def populate_historical_limits(self):
        """
        Populate the database with historical FEC contribution limits.
        This should be called during database initialization or migration.
        """
        logger.info("Populating historical FEC contribution limits...")
        
        # Historical limits data
        # Format: (effective_year, contributor_category, recipient_category, limit_amount, limit_type, notes)
        limits_data = [
            # 2015-2016 cycle (effective 2015)
            (2015, self.CONTRIBUTOR_INDIVIDUAL, self.RECIPIENT_CANDIDATE, 2700.0, self.LIMIT_TYPE_PER_ELECTION, "2015-2016 cycle"),
            (2015, self.CONTRIBUTOR_INDIVIDUAL, self.RECIPIENT_NATIONAL_PARTY, 33400.0, self.LIMIT_TYPE_PER_YEAR, "2015-2016 cycle"),
            (2015, self.CONTRIBUTOR_MULTICANDIDATE_PAC, self.RECIPIENT_CANDIDATE, 5000.0, self.LIMIT_TYPE_PER_ELECTION, "2015-2016 cycle"),
            (2015, self.CONTRIBUTOR_MULTICANDIDATE_PAC, self.RECIPIENT_NATIONAL_PARTY, 15000.0, self.LIMIT_TYPE_PER_YEAR, "2015-2016 cycle"),
            (2015, self.CONTRIBUTOR_NON_MULTICANDIDATE_PAC, self.RECIPIENT_CANDIDATE, 2700.0, self.LIMIT_TYPE_PER_ELECTION, "2015-2016 cycle"),
            (2015, self.CONTRIBUTOR_NON_MULTICANDIDATE_PAC, self.RECIPIENT_NATIONAL_PARTY, 33400.0, self.LIMIT_TYPE_PER_YEAR, "2015-2016 cycle"),
            
            # 2017-2018 cycle (effective 2017)
            (2017, self.CONTRIBUTOR_INDIVIDUAL, self.RECIPIENT_CANDIDATE, 2700.0, self.LIMIT_TYPE_PER_ELECTION, "2017-2018 cycle"),
            (2017, self.CONTRIBUTOR_INDIVIDUAL, self.RECIPIENT_NATIONAL_PARTY, 33900.0, self.LIMIT_TYPE_PER_YEAR, "2017-2018 cycle"),
            (2017, self.CONTRIBUTOR_MULTICANDIDATE_PAC, self.RECIPIENT_CANDIDATE, 5000.0, self.LIMIT_TYPE_PER_ELECTION, "2017-2018 cycle"),
            (2017, self.CONTRIBUTOR_MULTICANDIDATE_PAC, self.RECIPIENT_NATIONAL_PARTY, 15000.0, self.LIMIT_TYPE_PER_YEAR, "2017-2018 cycle"),
            (2017, self.CONTRIBUTOR_NON_MULTICANDIDATE_PAC, self.RECIPIENT_CANDIDATE, 2700.0, self.LIMIT_TYPE_PER_ELECTION, "2017-2018 cycle"),
            (2017, self.CONTRIBUTOR_NON_MULTICANDIDATE_PAC, self.RECIPIENT_NATIONAL_PARTY, 33900.0, self.LIMIT_TYPE_PER_YEAR, "2017-2018 cycle"),
            
            # 2019-2020 cycle (effective 2019)
            (2019, self.CONTRIBUTOR_INDIVIDUAL, self.RECIPIENT_CANDIDATE, 2800.0, self.LIMIT_TYPE_PER_ELECTION, "2019-2020 cycle"),
            (2019, self.CONTRIBUTOR_INDIVIDUAL, self.RECIPIENT_NATIONAL_PARTY, 35500.0, self.LIMIT_TYPE_PER_YEAR, "2019-2020 cycle"),
            (2019, self.CONTRIBUTOR_MULTICANDIDATE_PAC, self.RECIPIENT_CANDIDATE, 5000.0, self.LIMIT_TYPE_PER_ELECTION, "2019-2020 cycle"),
            (2019, self.CONTRIBUTOR_MULTICANDIDATE_PAC, self.RECIPIENT_NATIONAL_PARTY, 15000.0, self.LIMIT_TYPE_PER_YEAR, "2019-2020 cycle"),
            (2019, self.CONTRIBUTOR_NON_MULTICANDIDATE_PAC, self.RECIPIENT_CANDIDATE, 2800.0, self.LIMIT_TYPE_PER_ELECTION, "2019-2020 cycle"),
            (2019, self.CONTRIBUTOR_NON_MULTICANDIDATE_PAC, self.RECIPIENT_NATIONAL_PARTY, 35500.0, self.LIMIT_TYPE_PER_YEAR, "2019-2020 cycle"),
            
            # 2021-2022 cycle (effective 2021)
            (2021, self.CONTRIBUTOR_INDIVIDUAL, self.RECIPIENT_CANDIDATE, 2900.0, self.LIMIT_TYPE_PER_ELECTION, "2021-2022 cycle"),
            (2021, self.CONTRIBUTOR_INDIVIDUAL, self.RECIPIENT_NATIONAL_PARTY, 36500.0, self.LIMIT_TYPE_PER_YEAR, "2021-2022 cycle"),
            (2021, self.CONTRIBUTOR_MULTICANDIDATE_PAC, self.RECIPIENT_CANDIDATE, 5000.0, self.LIMIT_TYPE_PER_ELECTION, "2021-2022 cycle"),
            (2021, self.CONTRIBUTOR_MULTICANDIDATE_PAC, self.RECIPIENT_NATIONAL_PARTY, 15000.0, self.LIMIT_TYPE_PER_YEAR, "2021-2022 cycle"),
            (2021, self.CONTRIBUTOR_NON_MULTICANDIDATE_PAC, self.RECIPIENT_CANDIDATE, 2900.0, self.LIMIT_TYPE_PER_ELECTION, "2021-2022 cycle"),
            (2021, self.CONTRIBUTOR_NON_MULTICANDIDATE_PAC, self.RECIPIENT_NATIONAL_PARTY, 36500.0, self.LIMIT_TYPE_PER_YEAR, "2021-2022 cycle"),
            
            # 2023-2024 cycle (effective 2023)
            (2023, self.CONTRIBUTOR_INDIVIDUAL, self.RECIPIENT_CANDIDATE, 3300.0, self.LIMIT_TYPE_PER_ELECTION, "2023-2024 cycle"),
            (2023, self.CONTRIBUTOR_INDIVIDUAL, self.RECIPIENT_NATIONAL_PARTY, 41300.0, self.LIMIT_TYPE_PER_YEAR, "2023-2024 cycle"),
            (2023, self.CONTRIBUTOR_MULTICANDIDATE_PAC, self.RECIPIENT_CANDIDATE, 5000.0, self.LIMIT_TYPE_PER_ELECTION, "2023-2024 cycle"),
            (2023, self.CONTRIBUTOR_MULTICANDIDATE_PAC, self.RECIPIENT_NATIONAL_PARTY, 15000.0, self.LIMIT_TYPE_PER_YEAR, "2023-2024 cycle"),
            (2023, self.CONTRIBUTOR_NON_MULTICANDIDATE_PAC, self.RECIPIENT_CANDIDATE, 3300.0, self.LIMIT_TYPE_PER_ELECTION, "2023-2024 cycle"),
            (2023, self.CONTRIBUTOR_NON_MULTICANDIDATE_PAC, self.RECIPIENT_NATIONAL_PARTY, 41300.0, self.LIMIT_TYPE_PER_YEAR, "2023-2024 cycle"),
            
            # 2025-2026 cycle (effective 2025)
            (2025, self.CONTRIBUTOR_INDIVIDUAL, self.RECIPIENT_CANDIDATE, 3500.0, self.LIMIT_TYPE_PER_ELECTION, "2025-2026 cycle"),
            (2025, self.CONTRIBUTOR_INDIVIDUAL, self.RECIPIENT_NATIONAL_PARTY, 44300.0, self.LIMIT_TYPE_PER_YEAR, "2025-2026 cycle"),
            (2025, self.CONTRIBUTOR_MULTICANDIDATE_PAC, self.RECIPIENT_CANDIDATE, 5000.0, self.LIMIT_TYPE_PER_ELECTION, "2025-2026 cycle"),
            (2025, self.CONTRIBUTOR_MULTICANDIDATE_PAC, self.RECIPIENT_NATIONAL_PARTY, 15000.0, self.LIMIT_TYPE_PER_YEAR, "2025-2026 cycle"),
            (2025, self.CONTRIBUTOR_NON_MULTICANDIDATE_PAC, self.RECIPIENT_CANDIDATE, 3500.0, self.LIMIT_TYPE_PER_ELECTION, "2025-2026 cycle"),
            (2025, self.CONTRIBUTOR_NON_MULTICANDIDATE_PAC, self.RECIPIENT_NATIONAL_PARTY, 44300.0, self.LIMIT_TYPE_PER_YEAR, "2025-2026 cycle"),
        ]
        
        added_count = 0
        updated_count = 0
        
        for effective_year, contributor_category, recipient_category, limit_amount, limit_type, notes in limits_data:
            try:
                existing = await self.db.execute(
                    select(ContributionLimit).where(
                        and_(
                            ContributionLimit.effective_year == effective_year,
                            ContributionLimit.contributor_category == contributor_category,
                            ContributionLimit.recipient_category == recipient_category,
                            ContributionLimit.limit_type == limit_type
                        )
                    )
                )
                existing_limit = existing.scalar_one_or_none()
                
                if existing_limit:
                    if existing_limit.limit_amount != limit_amount:
                        existing_limit.limit_amount = limit_amount
                        existing_limit.notes = notes
                        existing_limit.updated_at = datetime.utcnow()
                        updated_count += 1
                else:
                    new_limit = ContributionLimit(
                        effective_year=effective_year,
                        contributor_category=contributor_category,
                        recipient_category=recipient_category,
                        limit_amount=limit_amount,
                        limit_type=limit_type,
                        notes=notes
                    )
                    self.db.add(new_limit)
                    added_count += 1
            except Exception as e:
                logger.error(f"Error adding limit for {effective_year}, {contributor_category}, {recipient_category}: {e}")
        
        await self.db.commit()
        logger.info(f"Populated contribution limits: {added_count} added, {updated_count} updated")
        
        return added_count + updated_count

