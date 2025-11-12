import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from app.services.fec_client import FECClient
from app.models.schemas import FraudPattern, FraudAnalysis


class FraudDetectionService:
    """Service for detecting fraud patterns in contributions"""
    
    def __init__(self, fec_client: FECClient):
        self.fec_client = fec_client
        self.reporting_threshold = 200.0  # FEC reporting threshold
        self.smurfing_threshold = 190.0  # Just under reporting threshold
        self.contribution_limit_individual = 2900.0  # Per election cycle
    
    def _similarity(self, str1: str, str2: str) -> float:
        """Calculate string similarity"""
        if not str1 or not str2:
            return 0.0
        return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()
    
    async def analyze_candidate(
        self,
        candidate_id: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ) -> FraudAnalysis:
        """Analyze candidate contributions for fraud patterns"""
        contributions = await self.fec_client.get_contributions(
            candidate_id=candidate_id,
            min_date=min_date,
            max_date=max_date,
            limit=10000
        )
        
        if not contributions:
            return FraudAnalysis(
                candidate_id=candidate_id,
                patterns=[],
                risk_score=0.0,
                total_suspicious_amount=0.0
            )
        
        df = pd.DataFrame(contributions)
        
        # Ensure required columns exist
        required_cols = ['contribution_amount', 'contributor_name', 'contributor_city', 
                        'contributor_state', 'contributor_employer', 'contribution_date']
        for col in required_cols:
            if col not in df.columns:
                df[col] = None
        
        patterns = []
        
        # Detect smurfing patterns
        smurfing_patterns = self._detect_smurfing(df)
        patterns.extend(smurfing_patterns)
        
        # Detect threshold clustering
        threshold_patterns = self._detect_threshold_clustering(df)
        patterns.extend(threshold_patterns)
        
        # Detect temporal anomalies
        temporal_patterns = self._detect_temporal_anomalies(df)
        patterns.extend(temporal_patterns)
        
        # Detect round number patterns
        round_patterns = self._detect_round_number_patterns(df)
        patterns.extend(round_patterns)
        
        # Detect same-day multiple contributions
        same_day_patterns = self._detect_same_day_patterns(df)
        patterns.extend(same_day_patterns)
        
        # Calculate risk score
        total_suspicious = sum(p.total_amount for p in patterns)
        # Convert to numeric and fill NaN values
        contribution_amounts = pd.to_numeric(df['contribution_amount'], errors='coerce').fillna(0)
        total_contributions = contribution_amounts.sum()
        risk_score = min(100.0, (total_suspicious / total_contributions * 100) if total_contributions > 0 else 0.0)
        
        return FraudAnalysis(
            candidate_id=candidate_id,
            patterns=patterns,
            risk_score=risk_score,
            total_suspicious_amount=total_suspicious
        )
    
    def _detect_smurfing(self, df: pd.DataFrame) -> List[FraudPattern]:
        """Detect smurfing: multiple contributions just under reporting threshold"""
        patterns = []
        
        # Convert to numeric and fill NaN values
        contribution_amounts = pd.to_numeric(df['contribution_amount'], errors='coerce').fillna(0)
        
        # Find contributions just under threshold
        smurf_contributions = df[
            (contribution_amounts >= self.smurfing_threshold) &
            (contribution_amounts < self.reporting_threshold)
        ].copy()
        
        if len(smurf_contributions) < 3:
            return patterns
        
        # Group by similar names, addresses, employers
        smurf_contributions['name_normalized'] = smurf_contributions['contributor_name'].fillna('').astype(str).str.lower().str.strip()
        smurf_contributions['city_normalized'] = smurf_contributions['contributor_city'].fillna('').astype(str).str.lower().str.strip()
        smurf_contributions['employer_normalized'] = smurf_contributions['contributor_employer'].fillna('').astype(str).str.lower().str.strip()
        
        # Find groups with similar attributes
        groups = []
        processed = set()
        
        for idx, row in smurf_contributions.iterrows():
            if idx in processed:
                continue
            
            group = [row.to_dict()]
            processed.add(idx)
            
            # Find similar contributions
            for idx2, row2 in smurf_contributions.iterrows():
                if idx2 in processed:
                    continue
                
                name_sim = self._similarity(
                    str(row['contributor_name']),
                    str(row2['contributor_name'])
                )
                city_sim = self._similarity(
                    str(row['contributor_city']),
                    str(row2['contributor_city'])
                )
                employer_sim = self._similarity(
                    str(row['contributor_employer']),
                    str(row2['contributor_employer'])
                )
                
                # If similar enough, add to group
                if (name_sim > 0.7 or (city_sim > 0.8 and employer_sim > 0.8)) and name_sim > 0.5:
                    group.append(row2.to_dict())
                    processed.add(idx2)
            
            if len(group) >= 3:
                groups.append(group)
        
        # Create patterns for significant groups
        for group in groups:
            total_amount = sum(c.get('contribution_amount', 0) for c in group)
            if total_amount >= self.reporting_threshold * 2:  # At least 2x threshold
                patterns.append(FraudPattern(
                    pattern_type="smurfing",
                    severity="high" if len(group) >= 5 else "medium",
                    description=f"Found {len(group)} contributions just under reporting threshold from similar sources",
                    affected_contributions=group[:10],  # Limit to first 10
                    total_amount=total_amount,
                    confidence_score=0.8 if len(group) >= 5 else 0.6
                ))
        
        return patterns
    
    def _detect_threshold_clustering(self, df: pd.DataFrame) -> List[FraudPattern]:
        """Detect contributions clustered near legal limits"""
        patterns = []
        
        # Convert to numeric and fill NaN values
        contribution_amounts = pd.to_numeric(df['contribution_amount'], errors='coerce').fillna(0)
        
        # Check for contributions near individual limit
        near_limit = df[
            (contribution_amounts >= self.contribution_limit_individual * 0.9) &
            (contribution_amounts <= self.contribution_limit_individual * 1.1)
        ]
        
        if len(near_limit) > 0:
            # Group by contributor
            grouped = near_limit.groupby('contributor_name').agg({
                'contribution_amount': ['sum', 'count']
            }).reset_index()
            grouped.columns = ['name', 'total', 'count']
            
            # Find contributors with multiple near-limit contributions
            suspicious = grouped[grouped['count'] >= 2]
            
            for _, row in suspicious.iterrows():
                contribs = near_limit[near_limit['contributor_name'] == row['name']].to_dict('records')
                patterns.append(FraudPattern(
                    pattern_type="threshold_clustering",
                    severity="medium",
                    description=f"Multiple contributions near legal limit from {row['name']}",
                    affected_contributions=contribs,
                    total_amount=float(row['total']),
                    confidence_score=0.7
                ))
        
        return patterns
    
    def _detect_temporal_anomalies(self, df: pd.DataFrame) -> List[FraudPattern]:
        """Detect unusual timing patterns"""
        patterns = []
        
        df['date'] = pd.to_datetime(df['contribution_date'], errors='coerce')
        df = df.dropna(subset=['date'])
        
        if len(df) == 0:
            return patterns
        
        # Group by contributor and date
        df['date_str'] = df['date'].dt.date.astype(str)
        grouped = df.groupby(['contributor_name', 'date_str']).agg({
            'contribution_amount': ['sum', 'count']
        }).reset_index()
        grouped.columns = ['name', 'date', 'total', 'count']
        
        # Find contributors with many contributions on same day
        same_day = grouped[grouped['count'] >= 5]
        
        for _, row in same_day.iterrows():
            contribs = df[
                (df['contributor_name'] == row['name']) &
                (df['date_str'] == row['date'])
            ].to_dict('records')
            
            patterns.append(FraudPattern(
                pattern_type="temporal_anomaly",
                severity="medium",
                description=f"{row['count']} contributions from {row['name']} on {row['date']}",
                affected_contributions=contribs,
                total_amount=float(row['total']),
                confidence_score=0.6
            ))
        
        return patterns
    
    def _detect_round_number_patterns(self, df: pd.DataFrame) -> List[FraudPattern]:
        """Detect excessive round number contributions"""
        patterns = []
        
        # Convert to numeric and fill NaN values
        contribution_amounts = pd.to_numeric(df['contribution_amount'], errors='coerce').fillna(0)
        
        # Round numbers: multiples of 100, 500, 1000
        df['is_round'] = (
            (contribution_amounts % 100 == 0) |
            (contribution_amounts % 500 == 0) |
            (contribution_amounts % 1000 == 0)
        )
        
        round_contribs = df[df['is_round']].copy()
        
        if len(round_contribs) > len(df) * 0.7:  # More than 70% are round numbers
            # Group by contributor
            grouped = round_contribs.groupby('contributor_name').agg({
                'contribution_amount': ['sum', 'count']
            }).reset_index()
            grouped.columns = ['name', 'total', 'count']
            
            # Find contributors with many round number contributions
            suspicious = grouped[grouped['count'] >= 10]
            
            for _, row in suspicious.iterrows():
                contribs = round_contribs[
                    round_contribs['contributor_name'] == row['name']
                ].head(20).to_dict('records')
                
                patterns.append(FraudPattern(
                    pattern_type="round_number_pattern",
                    severity="low",
                    description=f"Many round number contributions from {row['name']}",
                    affected_contributions=contribs,
                    total_amount=float(row['total']),
                    confidence_score=0.5
                ))
        
        return patterns
    
    def _detect_same_day_patterns(self, df: pd.DataFrame) -> List[FraudPattern]:
        """Detect multiple contributions from same source on same day"""
        patterns = []
        
        df['date'] = pd.to_datetime(df['contribution_date'], errors='coerce')
        df = df.dropna(subset=['date'])
        
        if len(df) == 0:
            return patterns
        
        # Group by name, address, and date
        df['key'] = (
            df['contributor_name'].astype(str) + "_" +
            df['contributor_city'].astype(str) + "_" +
            df['contributor_state'].astype(str) + "_" +
            df['date'].dt.date.astype(str)
        )
        
        grouped = df.groupby('key').agg({
            'contribution_amount': ['sum', 'count']
        }).reset_index()
        grouped.columns = ['key', 'total', 'count']
        
        # Find groups with multiple contributions
        multiple = grouped[grouped['count'] >= 3]
        
        for _, row in multiple.iterrows():
            key_parts = row['key'].split('_')
            contribs = df[df['key'] == row['key']].to_dict('records')
            
            patterns.append(FraudPattern(
                pattern_type="same_day_multiple",
                severity="medium" if row['count'] >= 5 else "low",
                description=f"{row['count']} contributions from same source on same day",
                affected_contributions=contribs,
                total_amount=float(row['total']),
                confidence_score=0.65 if row['count'] >= 5 else 0.5
            ))
        
        return patterns

