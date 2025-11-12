from typing import Optional, Dict, List, Any
from app.services.fec_client import FECClient
import logging

logger = logging.getLogger(__name__)


class CommitteeService:
    """Service for committee analysis"""
    
    def __init__(self, fec_client: FECClient):
        self.fec_client = fec_client
    
    async def get_committee_financials(
        self,
        committee_id: str,
        cycle: Optional[int] = None
    ) -> List[Dict]:
        """Get committee financial totals"""
        return await self.fec_client.get_committee_totals(committee_id, cycle=cycle)
    
    async def get_committee_transfers(
        self,
        committee_id: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict]:
        """Get committee-to-committee transfers"""
        # Transfers are in Schedule B with specific transaction types
        expenditures = await self.fec_client.get_expenditures(
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date,
            limit=limit
        )
        
        # Filter for transfers (transaction types: 24F, 24G, 24H, etc.)
        transfers = []
        transfer_types = ['24F', '24G', '24H', '24I', '24J', '24K']
        
        for exp in expenditures:
            transaction_type = exp.get('transaction_type', '')
            if transaction_type in transfer_types:
                transfers.append(exp)
        
        return transfers
    
    async def analyze_committee_spending(
        self,
        committee_id: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Analyze committee spending patterns"""
        expenditures = await self.fec_client.get_expenditures(
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date,
            limit=10000
        )
        
        if not expenditures:
            return {
                "total_spending": 0.0,
                "total_transactions": 0,
                "spending_by_date": {},
                "spending_by_category": {},
                "top_recipients": []
            }
        
        import pandas as pd
        df = pd.DataFrame(expenditures)
        
        total_spending = df['expenditure_amount'].fillna(0).sum() if 'expenditure_amount' in df.columns else 0.0
        total_transactions = len(df)
        
        # By date
        spending_by_date = {}
        if 'expenditure_date' in df.columns:
            date_grouped = df.groupby('expenditure_date')['expenditure_amount'].sum()
            spending_by_date = {str(date): float(amount) for date, amount in date_grouped.items()}
        
        # By category (using purpose field)
        spending_by_category = {}
        if 'expenditure_purpose' in df.columns:
            category_grouped = df.groupby('expenditure_purpose')['expenditure_amount'].sum()
            spending_by_category = {str(cat): float(amount) for cat, amount in category_grouped.items()}
        
        # Top recipients
        top_recipients = []
        if 'recipient_name' in df.columns:
            recipient_grouped = df.groupby('recipient_name').agg({
                'expenditure_amount': 'sum',
                'expenditure_id': 'count'
            }).reset_index()
            recipient_grouped.columns = ['recipient_name', 'total_amount', 'count']
            top_recipients = recipient_grouped.nlargest(10, 'total_amount').to_dict('records')
        
        return {
            "total_spending": float(total_spending),
            "total_transactions": total_transactions,
            "spending_by_date": spending_by_date,
            "spending_by_category": spending_by_category,
            "top_recipients": top_recipients
        }

