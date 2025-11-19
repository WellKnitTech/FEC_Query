import pandas as pd
import re
import logging
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
from app.services.fec_client import FECClient
from app.models.schemas import (
    ContributionAnalysis, MoneyFlowNode, MoneyFlowEdge, MoneyFlowGraph,
    ExpenditureBreakdown, EmployerAnalysis, ContributionVelocity, DonorStateAnalysis, CumulativeTotals
)
from app.utils.date_utils import serialize_date, extract_date_from_raw_data

logger = logging.getLogger(__name__)


class AnalysisService:
    """Service for financial analysis"""
    
    def __init__(self, fec_client: FECClient):
        self.fec_client = fec_client
    
    async def analyze_contributions(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None
    ) -> ContributionAnalysis:
        """Analyze contributions with aggregations using efficient SQL queries"""
        from app.db.database import AsyncSessionLocal, Contribution
        from sqlalchemy import select, func, and_, or_, case
        
        try:
            async with AsyncSessionLocal() as session:
                # Convert cycle to date range if provided and no explicit dates given
                # FEC cycles: For cycle YYYY, the cycle includes contributions from (YYYY-1)-01-01 to YYYY-12-31
                if cycle and not min_date and not max_date:
                    cycle_year = cycle
                    min_date = f"{cycle_year - 1}-01-01"
                    max_date = f"{cycle_year}-12-31"
                    logger.debug(f"analyze_contributions: Converted cycle {cycle} to date range: {min_date} to {max_date}")
                
                # Build base query conditions
                conditions = []
                if candidate_id:
                    conditions.append(Contribution.candidate_id == candidate_id)
                if committee_id:
                    conditions.append(Contribution.committee_id == committee_id)
                
                # Date filters - only apply if dates are provided
                # If cycle is specified, also include contributions without dates (they belong to this cycle)
                date_conditions = []
                if min_date and max_date:
                    try:
                        min_date_obj = datetime.strptime(min_date, "%Y-%m-%d")
                        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                        # If cycle is specified, include contributions without dates OR within date range
                        if cycle:
                            # Include contributions with dates in range OR contributions without dates
                            date_conditions.append(
                                or_(
                                    and_(
                                        Contribution.contribution_date >= min_date_obj,
                                        Contribution.contribution_date <= max_date_obj
                                    ),
                                    Contribution.contribution_date.is_(None)
                                )
                            )
                        else:
                            # No cycle specified, only include contributions with dates in range
                            date_conditions.append(Contribution.contribution_date >= min_date_obj)
                            date_conditions.append(Contribution.contribution_date <= max_date_obj)
                    except ValueError:
                        pass
                elif min_date:
                    try:
                        min_date_obj = datetime.strptime(min_date, "%Y-%m-%d")
                        if cycle:
                            date_conditions.append(
                                or_(
                                    Contribution.contribution_date >= min_date_obj,
                                    Contribution.contribution_date.is_(None)
                                )
                            )
                        else:
                            date_conditions.append(Contribution.contribution_date >= min_date_obj)
                    except ValueError:
                        pass
                elif max_date:
                    try:
                        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                        if cycle:
                            date_conditions.append(
                                or_(
                                    Contribution.contribution_date <= max_date_obj,
                                    Contribution.contribution_date.is_(None)
                                )
                            )
                        else:
                            date_conditions.append(Contribution.contribution_date <= max_date_obj)
                    except ValueError:
                        pass
                
                # Combine all conditions
                all_conditions = conditions + date_conditions
                where_clause = and_(*all_conditions) if all_conditions else and_(*conditions) if conditions else True
                
                # Check if we have any contributions for this candidate (for debugging)
                if candidate_id:
                    total_check_query = select(func.count(Contribution.id)).where(
                        Contribution.candidate_id == candidate_id
                    )
                    total_check_result = await session.execute(total_check_query)
                    total_count = total_check_result.scalar() or 0
                    logger.debug(f"analyze_contributions: Found {total_count} total contributions for candidate {candidate_id}")
                    
                    # Check contributions with dates
                    dated_check_query = select(func.count(Contribution.id)).where(
                        and_(
                            Contribution.candidate_id == candidate_id,
                            Contribution.contribution_date.isnot(None)
                        )
                    )
                    dated_check_result = await session.execute(dated_check_query)
                    dated_count = dated_check_result.scalar() or 0
                    logger.debug(f"analyze_contributions: Found {dated_count} contributions with dates for candidate {candidate_id}")
                
                # Get total contributions and count using aggregation
                # Note: We require contribution_amount to be not None, but contribution_date can be None
                # if no date filters are applied
                total_query = select(
                    func.sum(Contribution.contribution_amount).label('total'),
                    func.count(Contribution.id).label('count'),
                    func.count(func.distinct(Contribution.contributor_name)).label('unique_donors')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contribution_amount.isnot(None)
                    )
                )
                
                total_result = await session.execute(total_query)
                total_row = total_result.first()
                
                total_contributions = float(total_row.total) if total_row.total else 0.0
                total_count = int(total_row.count) if total_row.count else 0
                total_contributors = int(total_row.unique_donors) if total_row.unique_donors else 0
                average_contribution = total_contributions / total_count if total_count > 0 else 0.0
                
                # Contributions by date (aggregated)
                date_query = select(
                    func.date(Contribution.contribution_date).label('date'),
                    func.sum(Contribution.contribution_amount).label('amount')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contribution_date.isnot(None),
                        Contribution.contribution_amount.isnot(None)
                    )
                ).group_by(func.date(Contribution.contribution_date))
                
                date_result = await session.execute(date_query)
                contributions_by_date = {
                    str(row.date): float(row.amount)
                    for row in date_result
                    if row.date
                }
                
                # Contributions by state (aggregated)
                state_query = select(
                    Contribution.contributor_state.label('state'),
                    func.sum(Contribution.contribution_amount).label('amount')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contributor_state.isnot(None),
                        Contribution.contribution_amount.isnot(None)
                    )
                ).group_by(Contribution.contributor_state)
                
                state_result = await session.execute(state_query)
                contributions_by_state = {
                    row.state: float(row.amount)
                    for row in state_result
                    if row.state
                }
                
                # Top donors (aggregated)
                top_donors_query = select(
                    Contribution.contributor_name.label('name'),
                    func.sum(Contribution.contribution_amount).label('total'),
                    func.count(Contribution.id).label('count')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contributor_name.isnot(None),
                        Contribution.contribution_amount.isnot(None)
                    )
                ).group_by(Contribution.contributor_name).order_by(
                    func.sum(Contribution.contribution_amount).desc()
                ).limit(20)
                
                top_donors_result = await session.execute(top_donors_query)
                top_donors = [
                    {
                        'name': row.name,
                        'total': float(row.total),
                        'count': int(row.count)
                    }
                    for row in top_donors_result
                    if row.name
                ]
                
                # Contribution distribution (bins) - need to fetch amounts for binning
                # This is the one part that still needs raw data, but we can limit it
                amount_query = select(Contribution.contribution_amount).where(
                    and_(
                        where_clause,
                        Contribution.contribution_amount.isnot(None),
                        Contribution.contribution_amount > 0
                    )
                ).limit(10000)  # Limit for binning calculation
                
                amount_result = await session.execute(amount_query)
                amounts = [float(row.contribution_amount) for row in amount_result if row.contribution_amount]
                
                # Calculate distribution bins
                if amounts:
                    df_amounts = pd.Series(amounts)
                    bins = [0, 50, 100, 200, 500, 1000, 2700, float('inf')]
                    labels = ['$0-50', '$50-100', '$100-200', '$200-500', '$500-1000', '$1000-2700', '$2700+']
                    df_amounts_binned = pd.cut(df_amounts, bins=bins, labels=labels, right=False)
                    contribution_distribution = df_amounts_binned.value_counts().to_dict()
                    contribution_distribution = {str(k): int(v) for k, v in contribution_distribution.items()}
                else:
                    contribution_distribution = {}
                
                return ContributionAnalysis(
                    total_contributions=total_contributions,
                    total_contributors=total_contributors,
                    average_contribution=average_contribution,
                    contributions_by_date=contributions_by_date,
                    contributions_by_state=contributions_by_state,
                    top_donors=top_donors,
                    contribution_distribution=contribution_distribution
                )
        except Exception as e:
            logger.warning(f"Error in optimized analyze_contributions, falling back to pandas method: {e}", exc_info=True)
            # Fallback to original pandas-based method
            contributions = await self.fec_client.get_contributions(
                candidate_id=candidate_id,
                committee_id=committee_id,
                min_date=min_date,
                max_date=max_date,
                limit=10000,
                two_year_transaction_period=cycle
            )
            
            if not contributions:
                return ContributionAnalysis(
                    total_contributions=0.0,
                    total_contributors=0,
                    average_contribution=0.0,
                    contributions_by_date={},
                    contributions_by_state={},
                    top_donors=[],
                    contribution_distribution={}
                )
            
            df = pd.DataFrame(contributions)
            
            # Ensure required columns exist with defaults
            if 'contribution_amount' not in df.columns:
                df['contribution_amount'] = 0.0
            if 'contributor_name' not in df.columns:
                df['contributor_name'] = 'Unknown'
            if 'contribution_date' not in df.columns:
                df['contribution_date'] = None
            if 'contributor_state' not in df.columns:
                df['contributor_state'] = None
            
            # Convert contribution_amount to float, handling None, strings, and other types
            df['contribution_amount'] = pd.to_numeric(df['contribution_amount'], errors='coerce').fillna(0.0)
            
            # Calculate totals
            total_contributions = df['contribution_amount'].sum()
            total_contributors = df['contributor_name'].fillna('Unknown').nunique()
            average_contribution = total_contributions / len(df) if len(df) > 0 else 0.0
            
            # Contributions by date
            df['date'] = pd.to_datetime(df['contribution_date'], errors='coerce')
            date_grouped = df[df['date'].notna()].groupby(df['date'].dt.date)['contribution_amount'].sum()
            contributions_by_date = {str(k): float(v) for k, v in date_grouped.items() if k is not None}
            
            # Contributions by state
            state_grouped = df[df['contributor_state'].notna()].groupby('contributor_state')['contribution_amount'].sum()
            contributions_by_state = {k: float(v) for k, v in state_grouped.items()}
            
            # Top donors
            if len(df) > 0:
                top_donors_df = df.groupby('contributor_name').agg({
                    'contribution_amount': ['sum', 'count']
                }).reset_index()
                top_donors_df.columns = ['name', 'total', 'count']
                top_donors_df = top_donors_df.sort_values('total', ascending=False).head(20)
                top_donors = top_donors_df.to_dict('records')
            else:
                top_donors = []
            
            # Contribution distribution (bins)
            if len(df) > 0:
                bins = [0, 50, 100, 200, 500, 1000, 2700, float('inf')]
                labels = ['$0-50', '$50-100', '$100-200', '$200-500', '$500-1000', '$1000-2700', '$2700+']
                df['amount_bin'] = pd.cut(df['contribution_amount'].fillna(0), bins=bins, labels=labels, right=False)
                contribution_distribution = df['amount_bin'].value_counts().to_dict()
                contribution_distribution = {str(k): int(v) for k, v in contribution_distribution.items()}
            else:
                contribution_distribution = {}
            
            return ContributionAnalysis(
                total_contributions=float(total_contributions),
                total_contributors=int(total_contributors),
                average_contribution=float(average_contribution),
                contributions_by_date=contributions_by_date,
                contributions_by_state=contributions_by_state,
                top_donors=top_donors,
                contribution_distribution=contribution_distribution
            )
    
    async def build_money_flow_graph(
        self,
        candidate_id: str,
        max_depth: int = 2,
        min_amount: float = 100.0,
        aggregate_by_employer: bool = True
    ) -> MoneyFlowGraph:
        """Build network graph of money flows
        
        Args:
            candidate_id: Candidate ID
            max_depth: Maximum depth for flow tracking
            min_amount: Minimum amount to include
            aggregate_by_employer: If True, group by employer; if False, group by donor name
        """
        nodes = []
        edges = []
        node_ids = set()
        
        # Get candidate info
        candidate = await self.fec_client.get_candidate(candidate_id)
        if candidate:
            candidate_node_id = f"candidate_{candidate_id}"
            nodes.append(MoneyFlowNode(
                id=candidate_node_id,
                name=candidate.get('name', 'Unknown'),
                type="candidate",
                amount=None
            ))
            node_ids.add(candidate_node_id)
        
        # Get committees
        committees = await self.fec_client.get_committees(candidate_id=candidate_id)
        committee_nodes = {}
        
        for committee in committees:
            committee_id = committee.get('committee_id')
            if committee_id:
                committee_node_id = f"committee_{committee_id}"
                committee_nodes[committee_id] = committee_node_id
                if committee_node_id not in node_ids:
                    nodes.append(MoneyFlowNode(
                        id=committee_node_id,
                        name=committee.get('name', 'Unknown Committee'),
                        type="committee",
                        amount=None
                    ))
                    node_ids.add(committee_node_id)
                
                # Add edge from committee to candidate
                if candidate:
                    edges.append(MoneyFlowEdge(
                        source=committee_node_id,
                        target=f"candidate_{candidate_id}",
                        amount=0.0,  # Will be calculated from contributions
                        type="committee_to_candidate"
                    ))
        
        # Get contributions
        contributions = await self.fec_client.get_contributions(
            candidate_id=candidate_id,
            min_amount=min_amount,
            limit=5000
        )
        
        if aggregate_by_employer:
            # Group contributions by employer
            employer_contributions = {}
            employer_donors = {}  # Track donors per employer for potential breakout
            
            for contrib in contributions:
                employer = contrib.get('contributor_employer') or 'Unknown Employer'
                donor_name = contrib.get('contributor_name')
                committee_id = contrib.get('committee_id')
                amount = contrib.get('contribution_amount', 0.0)
                
                if committee_id:
                    if employer not in employer_contributions:
                        employer_contributions[employer] = {}
                        employer_donors[employer] = set()
                    if committee_id not in employer_contributions[employer]:
                        employer_contributions[employer][committee_id] = 0.0
                    employer_contributions[employer][committee_id] += amount
                    if donor_name:
                        employer_donors[employer].add(donor_name)
            
            # Add employer nodes and edges
            for employer, committee_amounts in list(employer_contributions.items())[:50]:  # Limit to top 50 employers
                employer_node_id = f"employer_{hash(employer) % 1000000}"
                if employer_node_id not in node_ids:
                    total_employer_amount = sum(committee_amounts.values())
                    donor_count = len(employer_donors.get(employer, set()))
                    # Include donor count in name for context
                    display_name = f"{employer} ({donor_count} donor{'s' if donor_count != 1 else ''})"
                    nodes.append(MoneyFlowNode(
                        id=employer_node_id,
                        name=display_name[:50],  # Truncate long names
                        type="employer",
                        amount=total_employer_amount
                    ))
                    node_ids.add(employer_node_id)
                
                # Add edges from employer to committees
                for committee_id, amount in committee_amounts.items():
                    if committee_id in committee_nodes:
                        edges.append(MoneyFlowEdge(
                            source=employer_node_id,
                            target=committee_nodes[committee_id],
                            amount=float(amount),
                            type="contribution"
                        ))
        else:
            # Group contributions by donor (original behavior)
            donor_contributions = {}
            for contrib in contributions:
                donor_name = contrib.get('contributor_name')
                committee_id = contrib.get('committee_id')
                amount = contrib.get('contribution_amount', 0.0)
                
                if donor_name and committee_id:
                    if donor_name not in donor_contributions:
                        donor_contributions[donor_name] = {}
                    if committee_id not in donor_contributions[donor_name]:
                        donor_contributions[donor_name][committee_id] = 0.0
                    donor_contributions[donor_name][committee_id] += amount
            
            # Add donor nodes and edges
            for donor_name, committee_amounts in list(donor_contributions.items())[:100]:  # Limit to top 100 donors
                donor_node_id = f"donor_{hash(donor_name) % 1000000}"
                if donor_node_id not in node_ids:
                    total_donor_amount = sum(committee_amounts.values())
                    nodes.append(MoneyFlowNode(
                        id=donor_node_id,
                        name=donor_name[:50],  # Truncate long names
                        type="donor",
                        amount=total_donor_amount
                    ))
                    node_ids.add(donor_node_id)
                
                # Add edges from donor to committees
                for committee_id, amount in committee_amounts.items():
                    if committee_id in committee_nodes:
                        edges.append(MoneyFlowEdge(
                            source=donor_node_id,
                            target=committee_nodes[committee_id],
                            amount=float(amount),
                            type="contribution"
                        ))
        
        return MoneyFlowGraph(nodes=nodes, edges=edges)
    
    async def analyze_expenditures(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ) -> ExpenditureBreakdown:
        """Analyze expenditures with category aggregation"""
        expenditures = await self.fec_client.get_expenditures(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date,
            limit=10000
        )
        
        if not expenditures:
            return ExpenditureBreakdown(
                total_expenditures=0.0,
                total_transactions=0,
                average_expenditure=0.0,
                expenditures_by_date={},
                expenditures_by_category={},
                expenditures_by_recipient=[],
                top_recipients=[]
            )
        
        df = pd.DataFrame(expenditures)
        
        # Ensure required columns exist
        if 'expenditure_amount' not in df.columns:
            df['expenditure_amount'] = 0.0
        if 'expenditure_date' not in df.columns:
            df['expenditure_date'] = None
        if 'recipient_name' not in df.columns:
            df['recipient_name'] = 'Unknown'
        if 'expenditure_purpose' not in df.columns:
            df['expenditure_purpose'] = None
        
        # Convert expenditure_amount to float, handling None, strings, and other types
        df['expenditure_amount'] = pd.to_numeric(df['expenditure_amount'], errors='coerce').fillna(0.0)
        
        # Calculate totals
        total_expenditures = df['expenditure_amount'].sum()
        total_transactions = len(df)
        average_expenditure = total_expenditures / total_transactions if total_transactions > 0 else 0.0
        
        # Expenditures by date
        df['date'] = pd.to_datetime(df['expenditure_date'], errors='coerce')
        date_grouped = df[df['date'].notna()].groupby(df['date'].dt.date)['expenditure_amount'].sum()
        expenditures_by_date = {str(k): float(v) for k, v in date_grouped.items() if k is not None}
        
        # Categorize expenditures by purpose keywords
        def categorize_expenditure(purpose: str) -> str:
            if pd.isna(purpose) or not purpose:
                return 'Other'
            purpose_lower = str(purpose).lower()
            if any(word in purpose_lower for word in ['ad', 'advertising', 'media', 'tv', 'radio', 'digital']):
                return 'Advertising'
            elif any(word in purpose_lower for word in ['salary', 'payroll', 'staff', 'employee', 'wage']):
                return 'Staff/Payroll'
            elif any(word in purpose_lower for word in ['travel', 'hotel', 'airfare', 'lodging', 'mileage']):
                return 'Travel'
            elif any(word in purpose_lower for word in ['rent', 'office', 'facility', 'utilities']):
                return 'Office/Facilities'
            elif any(word in purpose_lower for word in ['consulting', 'consultant', 'professional']):
                return 'Consulting'
            elif any(word in purpose_lower for word in ['event', 'fundraising', 'reception', 'dinner']):
                return 'Events/Fundraising'
            elif any(word in purpose_lower for word in ['polling', 'survey', 'research']):
                return 'Polling/Research'
            elif any(word in purpose_lower for word in ['legal', 'attorney', 'law']):
                return 'Legal'
            else:
                return 'Other'
        
        df['category'] = df['expenditure_purpose'].apply(categorize_expenditure)
        category_grouped = df.groupby('category')['expenditure_amount'].sum()
        expenditures_by_category = {k: float(v) for k, v in category_grouped.items()}
        
        # Top recipients
        recipient_df = df.groupby('recipient_name').agg({
            'expenditure_amount': ['sum', 'count']
        }).reset_index()
        recipient_df.columns = ['name', 'total', 'count']
        recipient_df = recipient_df.sort_values('total', ascending=False).head(20)
        top_recipients = recipient_df.to_dict('records')
        
        # Expenditures by recipient (all)
        recipient_all = df.groupby('recipient_name')['expenditure_amount'].sum().to_dict()
        expenditures_by_recipient = [{'name': k, 'amount': float(v)} for k, v in recipient_all.items()]
        
        return ExpenditureBreakdown(
            total_expenditures=float(total_expenditures),
            total_transactions=int(total_transactions),
            average_expenditure=float(average_expenditure),
            expenditures_by_date=expenditures_by_date,
            expenditures_by_category=expenditures_by_category,
            expenditures_by_recipient=expenditures_by_recipient,
            top_recipients=top_recipients
        )
    
    def _normalize_employer_name(self, employer: str) -> str:
        """Normalize employer name for better aggregation"""
        if not employer or pd.isna(employer):
            return 'Unknown Employer'
        
        # Convert to string and strip whitespace
        normalized = str(employer).strip()
        
        # Convert to uppercase for consistent comparison
        normalized = normalized.upper()
        
        # Remove common business suffixes and variations (with optional comma before)
        # This handles cases like "COMPANY, INC.", "COMPANY INC", "COMPANY, INC", etc.
        normalized = re.sub(r',?\s*(INC|LLC|CORP|LTD|CO|CORPORATION|COMPANY|INCORPORATED)\.?$', '', normalized, flags=re.IGNORECASE)
        
        # Remove all punctuation (commas, periods, etc.)
        normalized = re.sub(r'[^\w\s]', '', normalized)
        
        # Normalize whitespace (multiple spaces to single space)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized if normalized else 'Unknown Employer'
    
    async def analyze_by_employer(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None
    ) -> EmployerAnalysis:
        """Analyze contributions by employer with name normalization using efficient SQL queries"""
        from app.db.database import AsyncSessionLocal, Contribution
        from sqlalchemy import select, func, and_, or_
        
        try:
            # Convert cycle to date range if provided
            if cycle and not min_date and not max_date:
                cycle_year = cycle
                min_date = f"{cycle_year - 1}-01-01"
                max_date = f"{cycle_year}-12-31"
            
            async with AsyncSessionLocal() as session:
                # Build base query conditions
                conditions = []
                if candidate_id:
                    conditions.append(Contribution.candidate_id == candidate_id)
                if committee_id:
                    conditions.append(Contribution.committee_id == committee_id)
                if min_date:
                    try:
                        min_date_obj = datetime.strptime(min_date, "%Y-%m-%d")
                        conditions.append(Contribution.contribution_date >= min_date_obj)
                    except ValueError:
                        pass
                if max_date:
                    try:
                        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                        conditions.append(Contribution.contribution_date <= max_date_obj)
                    except ValueError:
                        pass
                
                where_clause = and_(*conditions) if conditions else True
                
                # Get total contributions (for total_contributions field)
                total_query = select(
                    func.sum(Contribution.contribution_amount).label('total')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contribution_amount.isnot(None)
                    )
                )
                total_result = await session.execute(total_query)
                total_row = total_result.first()
                total_contributions = float(total_row.total) if total_row.total else 0.0
                
                # Get employer breakdown using SQL aggregation
                # We'll fetch employer names and amounts, then normalize in Python
                employer_query = select(
                    Contribution.contributor_employer.label('employer'),
                    func.sum(Contribution.contribution_amount).label('total'),
                    func.count(Contribution.id).label('count')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contributor_employer.isnot(None),
                        Contribution.contribution_amount.isnot(None)
                    )
                ).group_by(Contribution.contributor_employer)
                
                employer_result = await session.execute(employer_query)
                employer_rows = employer_result.all()
                
                if not employer_rows:
                    return EmployerAnalysis(
                        total_by_employer={},
                        top_employers=[],
                        employer_count=0,
                        total_contributions=total_contributions
                    )
                
                # Convert to DataFrame for normalization
                employer_data = [
                    {
                        'employer': row.employer,
                        'total': float(row.total),
                        'count': int(row.count)
                    }
                    for row in employer_rows
                    if row.employer
                ]
                
                df = pd.DataFrame(employer_data)
                
                # Normalize employer names for better aggregation
                df['normalized_employer'] = df['employer'].apply(self._normalize_employer_name)
                
                # Group by normalized employer name and aggregate
                employer_grouped = df.groupby('normalized_employer').agg({
                    'total': 'sum',
                    'count': 'sum',
                    'employer': 'first'  # Keep first original name for display
                }).reset_index()
                employer_grouped.columns = ['normalized_employer', 'total', 'count', 'display_name']
                employer_grouped = employer_grouped.sort_values('total', ascending=False)
                
                # Use display name (original) for the output, but grouping was done on normalized name
                total_by_employer = {row['display_name']: float(row['total']) for _, row in employer_grouped.iterrows()}
                top_employers = [
                    {
                        'employer': row['display_name'],
                        'total': float(row['total']),
                        'count': int(row['count'])
                    }
                    for _, row in employer_grouped.head(50).iterrows()
                ]
                
                return EmployerAnalysis(
                    total_by_employer=total_by_employer,
                    top_employers=top_employers,
                    employer_count=int(employer_grouped['normalized_employer'].nunique()),
                    total_contributions=total_contributions
                )
        except Exception as e:
            logger.warning(f"Error in optimized analyze_by_employer, falling back to pandas method: {e}", exc_info=True)
            # Fallback to original pandas-based method
            contributions = await self.fec_client.get_contributions(
                candidate_id=candidate_id,
                committee_id=committee_id,
                min_date=min_date,
                max_date=max_date,
                limit=10000
            )
            
            if not contributions:
                return EmployerAnalysis(
                    total_by_employer={},
                    top_employers=[],
                    employer_count=0,
                    total_contributions=0.0
                )
            
            df = pd.DataFrame(contributions)
            
            # Ensure required columns exist
            if 'contribution_amount' not in df.columns:
                df['contribution_amount'] = 0.0
            if 'contributor_employer' not in df.columns:
                df['contributor_employer'] = None
            
            # Convert contribution_amount to float, handling None, strings, and other types
            df['contribution_amount'] = pd.to_numeric(df['contribution_amount'], errors='coerce').fillna(0.0)
            
            # Filter out null employers
            df_with_employer = df[df['contributor_employer'].notna()].copy()
            
            if len(df_with_employer) == 0:
                return EmployerAnalysis(
                    total_by_employer={},
                    top_employers=[],
                    employer_count=0,
                    total_contributions=float(df['contribution_amount'].sum())
                )
            
            # Normalize employer names for better aggregation
            df_with_employer['normalized_employer'] = df_with_employer['contributor_employer'].apply(self._normalize_employer_name)
            
            # Group by normalized employer name
            employer_grouped = df_with_employer.groupby('normalized_employer').agg({
                'contribution_amount': ['sum', 'count'],
                'contributor_employer': 'first'  # Keep original name for display
            }).reset_index()
            employer_grouped.columns = ['employer', 'total', 'count', 'display_name']
            employer_grouped = employer_grouped.sort_values('total', ascending=False)
            
            # Use display name (original) for the output, but grouping was done on normalized name
            total_by_employer = {row['display_name']: float(row['total']) for _, row in employer_grouped.iterrows()}
            top_employers = [
                {
                    'employer': row['display_name'],
                    'total': float(row['total']),
                    'count': int(row['count'])
                }
                for _, row in employer_grouped.head(50).iterrows()
            ]
            
            return EmployerAnalysis(
                total_by_employer=total_by_employer,
                top_employers=top_employers,
                employer_count=int(employer_grouped['employer'].nunique()),
                total_contributions=float(df['contribution_amount'].sum())
            )
    
    async def analyze_velocity(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None
    ) -> ContributionVelocity:
        """Calculate contribution velocity (contributions per day/week) using efficient SQL queries"""
        from app.db.database import AsyncSessionLocal, Contribution
        from sqlalchemy import select, func, and_, or_
        
        try:
            # Convert cycle to date range if provided
            if cycle and not min_date and not max_date:
                cycle_year = cycle
                min_date = f"{cycle_year - 1}-01-01"
                max_date = f"{cycle_year}-12-31"
            
            async with AsyncSessionLocal() as session:
                # Build base query conditions
                conditions = []
                if candidate_id:
                    conditions.append(Contribution.candidate_id == candidate_id)
                if committee_id:
                    conditions.append(Contribution.committee_id == committee_id)
                # Date filters - if cycle is specified, include contributions without dates
                if min_date and max_date:
                    try:
                        min_date_obj = datetime.strptime(min_date, "%Y-%m-%d")
                        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                        if cycle:
                            conditions.append(
                                or_(
                                    and_(
                                        Contribution.contribution_date >= min_date_obj,
                                        Contribution.contribution_date <= max_date_obj
                                    ),
                                    Contribution.contribution_date.is_(None)
                                )
                            )
                        else:
                            conditions.append(Contribution.contribution_date >= min_date_obj)
                            conditions.append(Contribution.contribution_date <= max_date_obj)
                    except ValueError:
                        pass
                elif min_date:
                    try:
                        min_date_obj = datetime.strptime(min_date, "%Y-%m-%d")
                        if cycle:
                            conditions.append(
                                or_(
                                    Contribution.contribution_date >= min_date_obj,
                                    Contribution.contribution_date.is_(None)
                                )
                            )
                        else:
                            conditions.append(Contribution.contribution_date >= min_date_obj)
                    except ValueError:
                        pass
                elif max_date:
                    try:
                        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                        if cycle:
                            conditions.append(
                                or_(
                                    Contribution.contribution_date <= max_date_obj,
                                    Contribution.contribution_date.is_(None)
                                )
                            )
                        else:
                            conditions.append(Contribution.contribution_date <= max_date_obj)
                    except ValueError:
                        pass
                
                where_clause = and_(*conditions) if conditions else True
                
                # Get velocity by date using SQL aggregation
                # Note: For velocity, we only use contributions with dates (can't calculate velocity without dates)
                # But contributions without dates are still included in totals if cycle is specified
                date_query = select(
                    func.date(Contribution.contribution_date).label('date'),
                    func.sum(Contribution.contribution_amount).label('amount'),
                    func.count(Contribution.id).label('count')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contribution_date.isnot(None),
                        Contribution.contribution_amount.isnot(None)
                    )
                ).group_by(func.date(Contribution.contribution_date))
                
                date_result = await session.execute(date_query)
                date_rows = date_result.all()
                
                if not date_rows:
                    return ContributionVelocity(
                        velocity_by_date={},
                        velocity_by_week={},
                        peak_days=[],
                        average_daily_velocity=0.0
                    )
                
                # Convert to DataFrame for week grouping and peak day calculation
                velocity_data = [
                    {
                        'date': row.date,
                        'amount': float(row.amount),
                        'count': int(row.count)
                    }
                    for row in date_rows
                    if row.date
                ]
                
                df = pd.DataFrame(velocity_data)
                df['date'] = pd.to_datetime(df['date'])
                
                # Velocity by date
                velocity_by_date = {
                    str(row['date'].date()): float(row['amount'])
                    for _, row in df.iterrows()
                }
                
                # Velocity by week (group by week)
                df['week'] = df['date'].dt.to_period('W').astype(str)
                week_grouped = df.groupby('week')['amount'].sum()
                velocity_by_week = {str(k): float(v) for k, v in week_grouped.items()}
                
                # Peak days (top 10 by amount)
                peak_days_df = df.nlargest(10, 'amount')[['date', 'amount', 'count']]
                peak_days = [
                    {
                        'date': str(row['date'].date()),
                        'amount': float(row['amount']),
                        'count': int(row['count'])
                    }
                    for _, row in peak_days_df.iterrows()
                ]
                
                # Average daily velocity
                average_daily_velocity = float(df['amount'].mean()) if len(df) > 0 else 0.0
                
                return ContributionVelocity(
                    velocity_by_date=velocity_by_date,
                    velocity_by_week=velocity_by_week,
                    peak_days=peak_days,
                    average_daily_velocity=average_daily_velocity
                )
        except Exception as e:
            logger.warning(f"Error in optimized analyze_velocity, falling back to pandas method: {e}", exc_info=True)
            # Fallback to original pandas-based method
            contributions = await self.fec_client.get_contributions(
                candidate_id=candidate_id,
                committee_id=committee_id,
                min_date=min_date,
                max_date=max_date,
                limit=10000
            )
            
            if not contributions:
                return ContributionVelocity(
                    velocity_by_date={},
                    velocity_by_week={},
                    peak_days=[],
                    average_daily_velocity=0.0
                )
            
            df = pd.DataFrame(contributions)
            
            # Ensure required columns exist
            if 'contribution_date' not in df.columns:
                df['contribution_date'] = None
            if 'contribution_amount' not in df.columns:
                df['contribution_amount'] = 0.0
            
            # Convert contribution_amount to float, handling None, strings, and other types
            df['contribution_amount'] = pd.to_numeric(df['contribution_amount'], errors='coerce').fillna(0.0)
            
            df['date'] = pd.to_datetime(df['contribution_date'], errors='coerce')
            df = df[df['date'].notna()].copy()
            
            if len(df) == 0:
                return ContributionVelocity(
                    velocity_by_date={},
                    velocity_by_week={},
                    peak_days=[],
                    average_daily_velocity=0.0
                )
            
            # Daily velocity (amount per day)
            daily_amounts = df.groupby(df['date'].dt.date)['contribution_amount'].sum()
            velocity_by_date = {str(k): float(v) for k, v in daily_amounts.items()}
            
            # Weekly velocity
            df['week'] = df['date'].dt.to_period('W').astype(str)
            weekly_amounts = df.groupby('week')['contribution_amount'].sum()
            velocity_by_week = {k: float(v) for k, v in weekly_amounts.items()}
            
            # Peak days (top 10 by amount)
            peak_days_df = daily_amounts.sort_values(ascending=False).head(10).reset_index()
            daily_counts = df.groupby(df['date'].dt.date).size()
            peak_days = [
                {
                    'date': str(row['date']),
                    'amount': float(row['contribution_amount']),
                    'count': int(daily_counts.get(row['date'], 0))
                }
                for _, row in peak_days_df.iterrows()
            ]
            
            # Average daily velocity
            if len(velocity_by_date) > 0:
                average_daily_velocity = sum(velocity_by_date.values()) / len(velocity_by_date)
            else:
                average_daily_velocity = 0.0
            
            return ContributionVelocity(
                velocity_by_date=velocity_by_date,
                velocity_by_week=velocity_by_week,
                peak_days=peak_days,
                average_daily_velocity=float(average_daily_velocity)
            )
    
    async def analyze_donor_states(
        self,
        candidate_id: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None
    ) -> DonorStateAnalysis:
        """Analyze individual donors by state to identify out-of-state funding
        
        Args:
            candidate_id: Candidate ID (required)
            min_date: Optional start date filter
            max_date: Optional end date filter
            cycle: Optional election cycle filter
            
        Returns:
            DonorStateAnalysis with donor counts, amounts, and percentages by state
        """
        # Get candidate info to get their state
        candidate = await self.fec_client.get_candidate(candidate_id)
        candidate_state = candidate.get('state') if candidate else None
        
        # Convert cycle to date range if provided and no explicit dates given
        # FEC cycles: For cycle YYYY, the cycle includes contributions from (YYYY-1)-01-01 to YYYY-12-31
        if cycle and not min_date and not max_date:
            cycle_year = cycle
            min_date = f"{cycle_year - 1}-01-01"
            max_date = f"{cycle_year}-12-31"
            logger.debug(f"analyze_donor_states: Converted cycle {cycle} to date range: {min_date} to {max_date}")
        
        # Get contributions
        # Note: get_contributions will handle cycle filtering, but we also pass explicit dates if cycle was converted
        contributions = await self.fec_client.get_contributions(
            candidate_id=candidate_id,
            min_date=min_date,
            max_date=max_date,
            limit=10000,
            two_year_transaction_period=cycle if not min_date and not max_date else None  # Only pass cycle if dates weren't converted
        )
        
        if not contributions:
            return DonorStateAnalysis(
                donors_by_state={},
                donor_percentages_by_state={},
                amounts_by_state={},
                amount_percentages_by_state={},
                candidate_state=candidate_state,
                in_state_donor_percentage=0.0,
                in_state_amount_percentage=0.0,
                out_of_state_donor_percentage=0.0,
                out_of_state_amount_percentage=0.0,
                total_unique_donors=0,
                total_contributions=0.0,
                is_highly_out_of_state=False
            )
        
        df = pd.DataFrame(contributions)
        
        # Ensure required columns exist
        if 'contribution_amount' not in df.columns:
            df['contribution_amount'] = 0.0
        if 'contributor_name' not in df.columns:
            df['contributor_name'] = None
        if 'contributor_state' not in df.columns:
            df['contributor_state'] = None
        
        # Convert contribution_amount to float, and extract from raw_data if needed
        def extract_amount(row):
            # First try the contribution_amount field
            amount = row.get('contribution_amount')
            if amount is not None:
                try:
                    amount_float = float(amount)
                    if amount_float > 0:
                        return amount_float
                except (ValueError, TypeError):
                    pass
            
            # If amount is 0 or missing, try to extract from raw_data
            raw_data = row.get('raw_data')
            if raw_data and isinstance(raw_data, dict):
                # Try various amount field names
                for amt_key in ['TRANSACTION_AMT', 'CONTB_AMT', 'contb_receipt_amt', 'contribution_amount', 'transaction_amt', 'contribution_receipt_amount']:
                    if amt_key in raw_data:
                        try:
                            amt_val = str(raw_data[amt_key]).strip()
                            amt_val = amt_val.replace('$', '').replace(',', '').strip()
                            if amt_val:
                                amount_float = float(amt_val)
                                if amount_float > 0:
                                    return amount_float
                        except (ValueError, TypeError):
                            continue
            
            # Also check other possible fields in the row itself
            for amt_key in ['contb_receipt_amt', 'contribution_receipt_amount', 'amount']:
                if amt_key in row:
                    try:
                        amount_float = float(row[amt_key])
                        if amount_float > 0:
                            return amount_float
                    except (ValueError, TypeError):
                        continue
            
            return 0.0
        
        # Apply amount extraction
        df['contribution_amount'] = df.apply(extract_amount, axis=1)
        
        # Debug logging
        total_before_filter = df['contribution_amount'].sum()
        logger.debug(f"analyze_donor_states: Total contribution amount before filtering: ${total_before_filter:,.2f}")
        logger.debug(f"analyze_donor_states: Number of contributions: {len(df)}")
        logger.debug(f"analyze_donor_states: Contributions with amount > 0: {(df['contribution_amount'] > 0).sum()}")
        
        # Filter out rows without contributor_name
        df = df[df['contributor_name'].notna()].copy()
        
        if len(df) == 0:
            return DonorStateAnalysis(
                donors_by_state={},
                donor_percentages_by_state={},
                amounts_by_state={},
                amount_percentages_by_state={},
                candidate_state=candidate_state,
                in_state_donor_percentage=0.0,
                in_state_amount_percentage=0.0,
                out_of_state_donor_percentage=0.0,
                out_of_state_amount_percentage=0.0,
                total_unique_donors=0,
                total_contributions=0.0,
                is_highly_out_of_state=False
            )
        
        # Normalize state values - use "Unknown" for missing states
        df['contributor_state'] = df['contributor_state'].fillna('Unknown')
        
        # Create unique donor identifier: contributor_name + contributor_state
        # If state is missing, just use name
        df['donor_key'] = df.apply(
            lambda row: f"{row['contributor_name']}|{row['contributor_state']}" 
            if pd.notna(row['contributor_state']) and row['contributor_state'] != 'Unknown'
            else row['contributor_name'],
            axis=1
        )
        
        # Group by unique donor to get their primary state and total amount
        # For donors with multiple states, use the state with the most contributions
        donor_states = {}
        donor_amounts = {}
        
        for donor_key in df['donor_key'].unique():
            donor_df = df[df['donor_key'] == donor_key]
            # Get the state with the most contributions for this donor
            state_counts = donor_df.groupby('contributor_state')['contribution_amount'].sum()
            primary_state = state_counts.idxmax() if len(state_counts) > 0 else 'Unknown'
            total_amount = donor_df['contribution_amount'].sum()
            
            donor_states[donor_key] = primary_state
            donor_amounts[donor_key] = total_amount
        
        # Count unique donors by state
        state_donor_counts = {}
        state_amounts = {}
        
        for donor_key, state in donor_states.items():
            if state not in state_donor_counts:
                state_donor_counts[state] = 0
                state_amounts[state] = 0.0
            state_donor_counts[state] += 1
            state_amounts[state] += donor_amounts[donor_key]
        
        # Calculate totals
        total_unique_donors = len(donor_states)
        total_contributions = df['contribution_amount'].sum()
        
        # Calculate percentages
        donor_percentages = {}
        amount_percentages = {}
        
        for state, count in state_donor_counts.items():
            donor_percentages[state] = (count / total_unique_donors * 100) if total_unique_donors > 0 else 0.0
        
        for state, amount in state_amounts.items():
            amount_percentages[state] = (amount / total_contributions * 100) if total_contributions > 0 else 0.0
        
        # Calculate in-state vs out-of-state percentages
        in_state_donor_count = state_donor_counts.get(candidate_state, 0) if candidate_state else 0
        in_state_amount = state_amounts.get(candidate_state, 0.0) if candidate_state else 0.0
        
        in_state_donor_percentage = (in_state_donor_count / total_unique_donors * 100) if total_unique_donors > 0 else 0.0
        in_state_amount_percentage = (in_state_amount / total_contributions * 100) if total_contributions > 0 else 0.0
        
        out_of_state_donor_percentage = 100.0 - in_state_donor_percentage if candidate_state else 0.0
        out_of_state_amount_percentage = 100.0 - in_state_amount_percentage if candidate_state else 0.0
        
        # Flag as highly out-of-state if >50% of donors OR >50% of amounts are from outside state
        is_highly_out_of_state = False
        if candidate_state:
            is_highly_out_of_state = (
                out_of_state_donor_percentage > 50.0 or 
                out_of_state_amount_percentage > 50.0
            )
        
        return DonorStateAnalysis(
            donors_by_state={k: int(v) for k, v in state_donor_counts.items()},
            donor_percentages_by_state={k: float(v) for k, v in donor_percentages.items()},
            amounts_by_state={k: float(v) for k, v in state_amounts.items()},
            amount_percentages_by_state={k: float(v) for k, v in amount_percentages.items()},
            candidate_state=candidate_state,
            in_state_donor_percentage=float(in_state_donor_percentage),
            in_state_amount_percentage=float(in_state_amount_percentage),
            out_of_state_donor_percentage=float(out_of_state_donor_percentage),
            out_of_state_amount_percentage=float(out_of_state_amount_percentage),
            total_unique_donors=int(total_unique_donors),
            total_contributions=float(total_contributions),
            is_highly_out_of_state=is_highly_out_of_state
        )
    
    async def get_out_of_state_contributions(
        self,
        candidate_id: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None,
        limit: int = 10000
    ) -> List[Dict[str, Any]]:
        """Get contributions from out-of-state donors for human analysis
        
        Args:
            candidate_id: Candidate ID (required)
            min_date: Optional start date filter
            max_date: Optional end date filter
            cycle: Optional election cycle filter
            limit: Maximum number of contributions to return
            
        Returns:
            List of contribution dictionaries from out-of-state donors
        """
        # Get candidate info to get their state
        candidate = await self.fec_client.get_candidate(candidate_id)
        candidate_state = candidate.get('state') if candidate else None
        
        if not candidate_state:
            return []  # Can't determine out-of-state without candidate state
        
        # Query database directly for out-of-state contributions
        from app.db.database import AsyncSessionLocal, Contribution
        from sqlalchemy import select, and_, func
        
        try:
            async with AsyncSessionLocal() as session:
                # Build query for out-of-state contributions
                query = select(Contribution).where(
                    Contribution.candidate_id == candidate_id
                )
                
                # Add state filter - exclude candidate's state and NULL/Unknown states
                query = query.where(
                    and_(
                        Contribution.contributor_state.isnot(None),
                        Contribution.contributor_state != '',
                        Contribution.contributor_state != 'Unknown',
                        func.upper(Contribution.contributor_state) != candidate_state.upper()
                    )
                )
                
                # Add date filters
                if min_date:
                    try:
                        min_date_obj = datetime.strptime(min_date, "%Y-%m-%d")
                        query = query.where(Contribution.contribution_date >= min_date_obj)
                    except ValueError:
                        pass
                
                if max_date:
                    try:
                        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                        query = query.where(Contribution.contribution_date <= max_date_obj)
                    except ValueError:
                        pass
                
                # Order by amount descending, then date descending
                query = query.order_by(
                    Contribution.contribution_amount.desc().nulls_last(),
                    Contribution.contribution_date.desc().nulls_last()
                )
                
                # Apply limit
                query = query.limit(limit)
                
                result = await session.execute(query)
                contributions = result.scalars().all()
                
                # Convert to dict format
                out_of_state_contributions = []
                for c in contributions:
                    # Extract amount
                    amount = float(c.contribution_amount) if c.contribution_amount else 0.0
                    if amount == 0.0 and c.raw_data and isinstance(c.raw_data, dict):
                        # Try to extract from raw_data
                        for amt_key in ['TRANSACTION_AMT', 'CONTB_AMT', 'contribution_amount', 'transaction_amt', 'contb_receipt_amt']:
                            if amt_key in c.raw_data:
                                try:
                                    amt_val = str(c.raw_data[amt_key]).strip()
                                    amt_val = amt_val.replace('$', '').replace(',', '').strip()
                                    if amt_val:
                                        amount = float(amt_val)
                                        break
                                except (ValueError, TypeError):
                                    continue
                    
                    # Extract date - try database field first, then raw_data using helper function
                    contrib_date = None
                    if c.contribution_date:
                        logger.debug(f"get_out_of_state_contributions: Found date in database field: {c.contribution_date} (type: {type(c.contribution_date).__name__})")
                        contrib_date = c.contribution_date
                    elif c.raw_data:
                        logger.warning(f"get_out_of_state_contributions: Database field is None, checking raw_data for contribution_id: {c.contribution_id}")
                        logger.warning(f"get_out_of_state_contributions: raw_data type: {type(c.raw_data)}, is dict: {isinstance(c.raw_data, dict)}")
                        if isinstance(c.raw_data, dict):
                            logger.warning(f"get_out_of_state_contributions: raw_data keys (first 20): {list(c.raw_data.keys())[:20]}")
                        # Use centralized helper to extract date from raw_data
                        contrib_date = extract_date_from_raw_data(c.raw_data)
                        if contrib_date:
                            logger.warning(f"get_out_of_state_contributions: Extracted date from raw_data: {contrib_date}")
                        else:
                            logger.warning(f"get_out_of_state_contributions: Could not extract date from raw_data for contribution_id: {c.contribution_id}")
                    else:
                        logger.warning(f"get_out_of_state_contributions: No date field and no raw_data for contribution_id: {c.contribution_id}")
                    
                    # Format date as string using centralized utility
                    date_str_formatted = serialize_date(contrib_date)
                    if not date_str_formatted:
                        logger.warning(f"get_out_of_state_contributions: serialize_date returned None for contribution_id: {c.contribution_id}, contrib_date: {contrib_date}")
                    
                    contrib_dict = {
                        "sub_id": c.contribution_id,
                        "contribution_id": c.contribution_id,
                        "candidate_id": c.candidate_id,
                        "committee_id": c.committee_id,
                        "contributor_name": c.contributor_name,
                        "contributor_city": c.contributor_city,
                        "contributor_state": c.contributor_state,
                        "contributor_zip": c.contributor_zip,
                        "contributor_employer": c.contributor_employer,
                        "contributor_occupation": c.contributor_occupation,
                        "contribution_amount": amount,
                        "contribution_receipt_date": date_str_formatted,
                        "contribution_date": date_str_formatted,
                        "contribution_type": c.contribution_type,
                        "receipt_type": None
                    }
                    
                    # Add raw_data fields if available (but don't overwrite dates we just extracted)
                    if c.raw_data and isinstance(c.raw_data, dict):
                        for key, value in c.raw_data.items():
                            if key not in contrib_dict or not contrib_dict[key]:
                                contrib_dict[key] = value
                    
                    out_of_state_contributions.append(contrib_dict)
                
                return out_of_state_contributions
                
        except Exception as e:
            logger.warning(f"Error querying out-of-state contributions from database: {e}")
            # Fallback to old method if database query fails
            contributions = await self.fec_client.get_contributions(
                candidate_id=candidate_id,
                min_date=min_date,
                max_date=max_date,
                limit=limit,
                two_year_transaction_period=cycle
            )
            
            if not contributions:
                return []
            
            # Filter for out-of-state contributions and format dates
            out_of_state_contributions = []
            for contrib in contributions:
                contributor_state = contrib.get('contributor_state') or contrib.get('state')
                
                # Skip if no state provided
                if not contributor_state or contributor_state == 'Unknown':
                    continue
                
                # Include if state doesn't match candidate's state
                if contributor_state.upper() != candidate_state.upper():
                    # Format dates using centralized utility
                    date_value = contrib.get('contribution_receipt_date') or contrib.get('contribution_date') or contrib.get('receipt_date')
                    formatted_date = serialize_date(date_value)
                    
                    # Update contrib dict with formatted date
                    contrib['contribution_date'] = formatted_date
                    contrib['contribution_receipt_date'] = formatted_date
                    
                    out_of_state_contributions.append(contrib)
            
            # Sort by amount descending, then by date descending
            out_of_state_contributions.sort(
                key=lambda x: (
                    -float(x.get('contribution_amount') or x.get('contb_receipt_amt') or 0),
                    x.get('contribution_date') or x.get('contribution_receipt_date') or ''
                ),
                reverse=True
            )
            
            return out_of_state_contributions
    
    async def get_aggregated_out_of_state_donors(
        self,
        candidate_id: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get aggregated out-of-state donors for human analysis
        
        Args:
            candidate_id: Candidate ID (required)
            min_date: Optional start date filter
            max_date: Optional end date filter
            cycle: Optional election cycle filter
            limit: Maximum number of aggregated donors to return
            
        Returns:
            List of aggregated donor dictionaries from out-of-state
        """
        # Get out-of-state contributions first
        contributions = await self.get_out_of_state_contributions(
            candidate_id=candidate_id,
            min_date=min_date,
            max_date=max_date,
            cycle=cycle,
            limit=50000  # Get more contributions for better aggregation
        )
        
        if not contributions:
            return []
        
        # Use donor aggregation service
        from app.services.donor_aggregation import DonorAggregationService
        
        # Convert to format expected by aggregation service
        contrib_dicts = []
        for contrib in contributions:
            contrib_dict = {
                'contribution_id': contrib.get('contribution_id') or contrib.get('sub_id'),
                'contributor_name': contrib.get('contributor_name'),
                'contributor_city': contrib.get('contributor_city'),
                'contributor_state': contrib.get('contributor_state'),
                'contributor_zip': contrib.get('contributor_zip'),
                'contributor_employer': contrib.get('contributor_employer'),
                'contributor_occupation': contrib.get('contributor_occupation'),
                'contribution_amount': contrib.get('contribution_amount', 0) or 0,
                'contribution_date': contrib.get('contribution_date') or contrib.get('contribution_receipt_date')
            }
            if contrib_dict.get('contributor_name'):  # Only include if has name
                contrib_dicts.append(contrib_dict)
        
        if not contrib_dicts:
            return []
        
        # Aggregate donors
        aggregation_service = DonorAggregationService()
        aggregated = aggregation_service.aggregate_donors(contrib_dicts)
        
        # Sort by total amount descending and limit
        aggregated.sort(key=lambda x: x.get('total_amount', 0), reverse=True)
        
        return aggregated[:limit]
    
    async def get_cumulative_totals(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None
    ) -> CumulativeTotals:
        """Get cumulative contribution totals aggregated by date using efficient SQL queries
        
        This method uses database aggregation instead of fetching all contributions,
        making it much faster for large datasets.
        """
        from app.db.database import AsyncSessionLocal, Contribution
        from sqlalchemy import select, func, and_, or_, case
        from sqlalchemy.sql import cast
        from sqlalchemy.types import Date
        
        try:
            async with AsyncSessionLocal() as session:
                # Convert cycle to date range if provided and no explicit dates given
                # FEC cycles: For cycle YYYY, the cycle includes contributions from (YYYY-1)-01-01 to YYYY-12-31
                if cycle and not min_date and not max_date:
                    cycle_year = cycle
                    min_date = f"{cycle_year - 1}-01-01"
                    max_date = f"{cycle_year}-12-31"
                
                # Build base query conditions
                conditions = []
                if candidate_id:
                    conditions.append(Contribution.candidate_id == candidate_id)
                if committee_id:
                    conditions.append(Contribution.committee_id == committee_id)
                
                # Date filters - if cycle is specified, include contributions without dates
                date_conditions = []
                if min_date and max_date:
                    try:
                        min_date_obj = datetime.strptime(min_date, "%Y-%m-%d")
                        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                        if cycle:
                            # Include contributions with dates in range OR contributions without dates
                            date_conditions.append(
                                or_(
                                    and_(
                                        Contribution.contribution_date >= min_date_obj,
                                        Contribution.contribution_date <= max_date_obj
                                    ),
                                    Contribution.contribution_date.is_(None)
                                )
                            )
                        else:
                            # Only contributions with dates in range
                            date_conditions.append(Contribution.contribution_date >= min_date_obj)
                            date_conditions.append(Contribution.contribution_date <= max_date_obj)
                    except ValueError:
                        pass
                elif min_date:
                    try:
                        min_date_obj = datetime.strptime(min_date, "%Y-%m-%d")
                        if cycle:
                            date_conditions.append(
                                or_(
                                    Contribution.contribution_date >= min_date_obj,
                                    Contribution.contribution_date.is_(None)
                                )
                            )
                        else:
                            date_conditions.append(Contribution.contribution_date >= min_date_obj)
                    except ValueError:
                        pass
                elif max_date:
                    try:
                        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                        if cycle:
                            date_conditions.append(
                                or_(
                                    Contribution.contribution_date <= max_date_obj,
                                    Contribution.contribution_date.is_(None)
                                )
                            )
                        else:
                            date_conditions.append(Contribution.contribution_date <= max_date_obj)
                    except ValueError:
                        pass
                
                # Build base query - for cumulative totals, we only want contributions with dates
                # (contributions without dates can't be plotted on a timeline)
                # But we still include them in the total amount calculation
                all_conditions = conditions + date_conditions
                where_clause = and_(*all_conditions) if all_conditions else and_(*conditions) if conditions else True
                
                # Query for contributions with dates (for the timeline)
                query = select(
                    func.date(Contribution.contribution_date).label('date'),
                    func.sum(Contribution.contribution_amount).label('daily_total')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contribution_date.isnot(None),
                        Contribution.contribution_amount.isnot(None)
                )
                )
                
                # Group by date and order by date ascending
                query = query.group_by(func.date(Contribution.contribution_date)).order_by(
                    func.date(Contribution.contribution_date).asc()
                )
                
                result = await session.execute(query)
                rows = result.all()
                
                if not rows:
                    return CumulativeTotals(
                        totals_by_date={},
                        total_amount=0.0,
                        first_date=None,
                        last_date=None
                    )
                
                # Calculate cumulative totals from contributions with dates
                cumulative_total = 0.0
                totals_by_date = {}
                first_date = None
                last_date = None
                
                for row in rows:
                    date_str = row.date.strftime("%Y-%m-%d") if row.date else None
                    daily_total = float(row.daily_total) if row.daily_total else 0.0
                    
                    if date_str:
                        cumulative_total += daily_total
                        totals_by_date[date_str] = cumulative_total
                        
                        if first_date is None:
                            first_date = date_str
                        last_date = date_str
                
                # If cycle is specified, also include contributions without dates in the total
                # (they belong to this cycle but can't be plotted on timeline)
                if cycle:
                    undated_query = select(
                        func.sum(Contribution.contribution_amount).label('total')
                    ).where(
                        and_(
                            where_clause,
                            Contribution.contribution_date.is_(None),
                            Contribution.contribution_amount.isnot(None)
                        )
                    )
                    undated_result = await session.execute(undated_query)
                    undated_row = undated_result.first()
                    undated_total = float(undated_row.total) if undated_row and undated_row.total else 0.0
                    if undated_total > 0:
                        # Add undated contributions to the final total
                        cumulative_total += undated_total
                        logger.debug(f"get_cumulative_totals: Added ${undated_total:,.2f} from {cycle} contributions without dates")
                
                return CumulativeTotals(
                    totals_by_date=totals_by_date,
                    total_amount=cumulative_total,
                    first_date=first_date,
                    last_date=last_date
                )
        except Exception as e:
            logger.error(f"Error getting cumulative totals: {e}", exc_info=True)
            # Fallback to empty result
            return CumulativeTotals(
                totals_by_date={},
                total_amount=0.0,
                first_date=None,
                last_date=None
            )

