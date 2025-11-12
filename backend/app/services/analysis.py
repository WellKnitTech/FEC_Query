import pandas as pd
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
from app.services.fec_client import FECClient
from app.models.schemas import (
    ContributionAnalysis, MoneyFlowNode, MoneyFlowEdge, MoneyFlowGraph,
    ExpenditureBreakdown, EmployerAnalysis, ContributionVelocity
)


class AnalysisService:
    """Service for financial analysis"""
    
    def __init__(self, fec_client: FECClient):
        self.fec_client = fec_client
    
    async def analyze_contributions(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ) -> ContributionAnalysis:
        """Analyze contributions with aggregations"""
        contributions = await self.fec_client.get_contributions(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date,
            limit=10000
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
        
        # Calculate totals
        total_contributions = df['contribution_amount'].fillna(0).sum()
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
        min_amount: float = 100.0
    ) -> MoneyFlowGraph:
        """Build network graph of money flows"""
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
        
        # Group contributions by donor
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
        
        # Calculate totals
        total_expenditures = df['expenditure_amount'].fillna(0).sum()
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
    
    async def analyze_by_employer(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ) -> EmployerAnalysis:
        """Analyze contributions by employer"""
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
        
        # Filter out null employers
        df_with_employer = df[df['contributor_employer'].notna()].copy()
        
        if len(df_with_employer) == 0:
            return EmployerAnalysis(
                total_by_employer={},
                top_employers=[],
                employer_count=0,
                total_contributions=float(df['contribution_amount'].fillna(0).sum())
            )
        
        # Group by employer
        employer_grouped = df_with_employer.groupby('contributor_employer').agg({
            'contribution_amount': ['sum', 'count']
        }).reset_index()
        employer_grouped.columns = ['employer', 'total', 'count']
        employer_grouped = employer_grouped.sort_values('total', ascending=False)
        
        total_by_employer = {row['employer']: float(row['total']) for _, row in employer_grouped.iterrows()}
        top_employers = employer_grouped.head(50).to_dict('records')
        
        return EmployerAnalysis(
            total_by_employer=total_by_employer,
            top_employers=top_employers,
            employer_count=int(employer_grouped['employer'].nunique()),
            total_contributions=float(df['contribution_amount'].fillna(0).sum())
        )
    
    async def analyze_velocity(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ) -> ContributionVelocity:
        """Calculate contribution velocity (contributions per day/week)"""
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
        
        df['date'] = pd.to_datetime(df['contribution_date'], errors='coerce')
        df = df[df['date'].notna()].copy()
        
        if len(df) == 0:
            return ContributionVelocity(
                velocity_by_date={},
                velocity_by_week={},
                peak_days=[],
                average_daily_velocity=0.0
            )
        
        # Daily velocity (count of contributions per day)
        daily_counts = df.groupby(df['date'].dt.date).size()
        velocity_by_date = {str(k): float(v) for k, v in daily_counts.items()}
        
        # Weekly velocity
        df['week'] = df['date'].dt.to_period('W').astype(str)
        weekly_counts = df.groupby('week').size()
        velocity_by_week = {k: float(v) for k, v in weekly_counts.items()}
        
        # Peak days (days with most contributions)
        daily_amounts = df.groupby(df['date'].dt.date)['contribution_amount'].sum()
        peak_days_df = daily_amounts.sort_values(ascending=False).head(10).reset_index()
        peak_days = [
            {'date': str(row['date']), 'amount': float(row['contribution_amount']), 'count': int(daily_counts.get(row['date'], 0))}
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

