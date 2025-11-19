"""Expenditure analysis service"""
import pandas as pd
import logging
from typing import Optional

from app.services.fec_client import FECClient
from app.models.schemas import ExpenditureBreakdown
from app.utils.thread_pool import async_to_numeric, async_dataframe_operation

logger = logging.getLogger(__name__)


class ExpenditureAnalysisService:
    """Service for expenditure analysis"""
    
    def __init__(self, fec_client: FECClient):
        self.fec_client = fec_client
    
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
        df['expenditure_amount'] = await async_to_numeric(df['expenditure_amount'], errors='coerce')
        df['expenditure_amount'] = df['expenditure_amount'].fillna(0.0)
        
        # Calculate totals (offload to thread pool)
        total_expenditures = await async_dataframe_operation(df, lambda d: d['expenditure_amount'].sum())
        total_transactions = len(df)
        average_expenditure = total_expenditures / total_transactions if total_transactions > 0 else 0.0
        
        # Expenditures by date
        df['date'] = pd.to_datetime(df['expenditure_date'], errors='coerce')
        df_dated = df[df['date'].notna()].copy()
        date_grouped = await async_dataframe_operation(
            df_dated,
            lambda d: d.groupby(d['date'].dt.date)['expenditure_amount'].sum()
        )
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
        
        df['category'] = await async_dataframe_operation(
            df,
            lambda d: d['expenditure_purpose'].apply(categorize_expenditure)
        )
        category_grouped = await async_dataframe_operation(
            df,
            lambda d: d.groupby('category')['expenditure_amount'].sum()
        )
        expenditures_by_category = {k: float(v) for k, v in category_grouped.items()}
        
        # Top recipients
        recipient_df = await async_dataframe_operation(
            df,
            lambda d: d.groupby('recipient_name').agg({
                'expenditure_amount': ['sum', 'count']
            }).reset_index()
        )
        recipient_df.columns = ['name', 'total', 'count']
        recipient_df = await async_dataframe_operation(
            recipient_df,
            lambda d: d.sort_values('total', ascending=False).head(20)
        )
        top_recipients = recipient_df.to_dict('records')
        
        # Expenditures by recipient (all)
        recipient_all = await async_dataframe_operation(
            df,
            lambda d: d.groupby('recipient_name')['expenditure_amount'].sum().to_dict()
        )
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

