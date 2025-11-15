import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict
from difflib import SequenceMatcher
from app.services.fec_client import FECClient
from app.services.donor_aggregation import DonorAggregationService
from app.services.contribution_limits import ContributionLimitsService
from app.models.schemas import FraudPattern, FraudAnalysis


class FraudDetectionService:
    """Service for detecting fraud patterns in contributions"""
    
    def __init__(self, fec_client: FECClient, limits_service: Optional[ContributionLimitsService] = None):
        self.fec_client = fec_client
        self.limits_service = limits_service
        self.reporting_threshold = 200.0  # FEC reporting threshold
        self.smurfing_threshold = 190.0  # Just under reporting threshold
        # Default fallback limit (will be overridden by dynamic limits when available)
        self.contribution_limit_individual = 2900.0  # Per election cycle (fallback)
    
    def _determine_contributor_category(self, contribution: Dict[str, Any]) -> str:
        """
        Determine contributor category from contribution data.
        
        Args:
            contribution: Contribution dictionary with fields like contribution_type, 
                         contributor_employer, etc.
            
        Returns:
            Contributor category string
        """
        # If we have a limits service, we can use it to help determine category
        # For now, default to individual - this can be enhanced later with
        # logic to parse FEC transaction type codes or committee types
        contribution_type = contribution.get('contribution_type') or contribution.get('transaction_type')
        
        # TODO: Add logic to parse FEC transaction type codes to determine
        # if contributor is a PAC, party committee, etc.
        # For now, assume individual if we have employer/occupation info
        if contribution.get('contributor_employer') or contribution.get('contributor_occupation'):
            return ContributionLimitsService.CONTRIBUTOR_INDIVIDUAL
        
        # Default to individual
        return ContributionLimitsService.CONTRIBUTOR_INDIVIDUAL
    
    async def _get_contribution_limit(self, contribution_date: datetime, contribution: Dict[str, Any]) -> float:
        """
        Get the appropriate contribution limit for a contribution based on its date and type.
        
        Args:
            contribution_date: Date of the contribution
            contribution: Contribution dictionary
            
        Returns:
            Limit amount in dollars (uses fallback if limits service not available)
        """
        if not self.limits_service:
            return self.contribution_limit_individual
        
        contributor_category = self._determine_contributor_category(contribution)
        
        limit = await self.limits_service.get_limit_for_contribution(
            contribution_date=contribution_date,
            contributor_type=contributor_category,
            contribution_type_code=contribution.get('contribution_type'),
            committee_type=contribution.get('committee_type')
        )
        
        # Fallback to default if limit not found
        return limit if limit is not None else self.contribution_limit_individual
    
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
        threshold_patterns = await self._detect_threshold_clustering(df)
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
    
    async def _detect_threshold_clustering(self, df: pd.DataFrame) -> List[FraudPattern]:
        """Detect contributions clustered near legal limits"""
        patterns = []
        
        # Convert to numeric and fill NaN values
        contribution_amounts = pd.to_numeric(df['contribution_amount'], errors='coerce').fillna(0)
        
        # Get limits for each contribution based on date
        # Group by date to minimize database queries
        df['date'] = pd.to_datetime(df['contribution_date'], errors='coerce')
        df = df.dropna(subset=['date']).copy()
        
        if len(df) == 0:
            return patterns
        
        # Get unique dates and determine limits
        # For efficiency, we'll use the most common limit or check per contribution
        # For now, use a simpler approach: check each contribution's limit
        near_limit_rows = []
        
        for idx, row in df.iterrows():
            try:
                contrib_dict = row.to_dict()
                limit = await self._get_contribution_limit(row['date'], contrib_dict)
                
                amount = contribution_amounts.iloc[idx]
                if (amount >= limit * 0.9) and (amount <= limit * 1.1):
                    near_limit_rows.append((idx, row, limit))
            except Exception as e:
                # Skip if we can't determine limit
                continue
        
        if len(near_limit_rows) > 0:
            # Create DataFrame with near-limit contributions
            near_limit_indices = [idx for idx, _, _ in near_limit_rows]
            near_limit = df.loc[near_limit_indices].copy()
            
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
        df = df.dropna(subset=['date']).copy()
        
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
        df = df.dropna(subset=['date']).copy()
        
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
    
    async def analyze_candidate_with_aggregation(
        self,
        candidate_id: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ) -> FraudAnalysis:
        """Analyze candidate contributions for fraud patterns using donor aggregation"""
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
                total_suspicious_amount=0.0,
                aggregated_donors_count=0,
                aggregation_enabled=True
            )
        
        # Aggregate donors first
        contrib_dicts = []
        for contrib in contributions:
            contrib_dict = {
                'contribution_id': contrib.get('contribution_id'),
                'contributor_name': contrib.get('contributor_name'),
                'contributor_city': contrib.get('contributor_city'),
                'contributor_state': contrib.get('contributor_state'),
                'contributor_zip': contrib.get('contributor_zip'),
                'contributor_employer': contrib.get('contributor_employer'),
                'contributor_occupation': contrib.get('contributor_occupation'),
                'contribution_amount': contrib.get('contribution_amount', 0) or 0,
                'contribution_date': contrib.get('contribution_date')
            }
            contrib_dicts.append(contrib_dict)
        
        aggregation_service = DonorAggregationService()
        aggregated_donors = aggregation_service.aggregate_donors(contrib_dicts)
        aggregated_donors_count = len(aggregated_donors)
        
        patterns = []
        
        # Enhanced patterns using aggregated donors
        patterns.extend(await self._detect_aggregate_limit_evasion(aggregated_donors, contributions))
        patterns.extend(await self._detect_name_variation_fraud(aggregated_donors, contributions))
        patterns.extend(self._detect_coordinated_contributions(aggregated_donors))
        patterns.extend(self._detect_rapid_sequential_contributions(aggregated_donors))
        
        # Also run existing patterns on aggregated data
        df = pd.DataFrame(contributions)
        required_cols = ['contribution_amount', 'contributor_name', 'contributor_city', 
                        'contributor_state', 'contributor_employer', 'contribution_date']
        for col in required_cols:
            if col not in df.columns:
                df[col] = None
        
        # Enhanced smurfing with aggregation
        patterns.extend(self._detect_smurfing_with_aggregation(aggregated_donors))
        # Enhanced threshold clustering with aggregation
        patterns.extend(await self._detect_threshold_clustering_with_aggregation(aggregated_donors, contributions))
        
        # Keep existing temporal and round number patterns
        patterns.extend(self._detect_temporal_anomalies(df))
        patterns.extend(self._detect_round_number_patterns(df))
        patterns.extend(self._detect_same_day_patterns(df))
        
        # Calculate risk score
        total_suspicious = sum(p.total_amount for p in patterns)
        contribution_amounts = pd.to_numeric(df['contribution_amount'], errors='coerce').fillna(0)
        total_contributions = contribution_amounts.sum()
        risk_score = min(100.0, (total_suspicious / total_contributions * 100) if total_contributions > 0 else 0.0)
        
        return FraudAnalysis(
            candidate_id=candidate_id,
            patterns=patterns,
            risk_score=risk_score,
            total_suspicious_amount=total_suspicious,
            aggregated_donors_count=aggregated_donors_count,
            aggregation_enabled=True
        )
    
    async def _detect_aggregate_limit_evasion(self, aggregated_donors: List[Dict[str, Any]], contributions: List[Dict[str, Any]]) -> List[FraudPattern]:
        """Detect when aggregated donor exceeds contribution limit"""
        patterns = []
        
        # Create a mapping of contribution_id to contribution data for limit lookup
        contrib_map = {c.get('contribution_id'): c for c in contributions if c.get('contribution_id')}
        
        for donor in aggregated_donors:
            total_amount = donor.get('total_amount', 0)
            contrib_ids = donor.get('contribution_ids', [])
            
            # Get the most recent contribution date to determine which limit applies
            # For aggregated donors, we'll use the first contribution's date as a proxy
            # In practice, we might want to check each contribution's limit separately
            limit = self.contribution_limit_individual  # Default fallback
            
            if contrib_ids and self.limits_service:
                # Try to get limit from first contribution
                first_contrib_id = contrib_ids[0] if contrib_ids else None
                if first_contrib_id and first_contrib_id in contrib_map:
                    contrib = contrib_map[first_contrib_id]
                    contrib_date = contrib.get('contribution_date')
                    if contrib_date:
                        try:
                            if isinstance(contrib_date, str):
                                contrib_date = datetime.fromisoformat(contrib_date.replace('Z', '+00:00'))
                            elif isinstance(contrib_date, datetime):
                                pass  # Already a datetime
                            else:
                                contrib_date = None
                            
                            if contrib_date:
                                limit = await self._get_contribution_limit(contrib_date, contrib)
                        except Exception:
                            pass  # Use fallback limit
            
            if total_amount > limit:
                excess = total_amount - limit
                affected_contributions = [
                    {'contribution_id': cid, 'donor_name': donor.get('canonical_name')}
                    for cid in contrib_ids[:20]  # Limit to first 20
                ]
                
                patterns.append(FraudPattern(
                    pattern_type="aggregate_limit_evasion",
                    severity="high",
                    description=f"Aggregated donor {donor.get('canonical_name', 'Unknown')} exceeded limit by ${excess:.2f} "
                               f"(Total: ${total_amount:.2f}, Limit: ${limit:.2f})",
                    affected_contributions=affected_contributions,
                    total_amount=total_amount,
                    confidence_score=0.9
                ))
        
        return patterns
    
    async def _detect_name_variation_fraud(self, aggregated_donors: List[Dict[str, Any]], contributions: List[Dict[str, Any]]) -> List[FraudPattern]:
        """Flag donors with 3+ name variations exceeding limit"""
        patterns = []
        
        # Create a mapping of contribution_id to contribution data for limit lookup
        contrib_map = {c.get('contribution_id'): c for c in contributions if c.get('contribution_id')}
        
        for donor in aggregated_donors:
            all_names = donor.get('all_names', [])
            total_amount = donor.get('total_amount', 0)
            contrib_ids = donor.get('contribution_ids', [])
            
            # Get limit (similar to aggregate_limit_evasion)
            limit = self.contribution_limit_individual  # Default fallback
            
            if contrib_ids and self.limits_service:
                first_contrib_id = contrib_ids[0] if contrib_ids else None
                if first_contrib_id and first_contrib_id in contrib_map:
                    contrib = contrib_map[first_contrib_id]
                    contrib_date = contrib.get('contribution_date')
                    if contrib_date:
                        try:
                            if isinstance(contrib_date, str):
                                contrib_date = datetime.fromisoformat(contrib_date.replace('Z', '+00:00'))
                            if contrib_date:
                                limit = await self._get_contribution_limit(contrib_date, contrib)
                        except Exception:
                            pass
            
            if len(all_names) >= 3 and total_amount > limit:
                excess = total_amount - limit
                affected_contributions = [
                    {'contribution_id': cid, 'donor_name': name}
                    for cid, name in zip(contrib_ids[:20], all_names[:20])
                ]
                
                patterns.append(FraudPattern(
                    pattern_type="name_variation_fraud",
                    severity="high",
                    description=f"Donor appears with {len(all_names)} name variations: {', '.join(all_names[:5])}"
                               f"{'...' if len(all_names) > 5 else ''}, exceeding limit by ${excess:.2f}",
                    affected_contributions=affected_contributions,
                    total_amount=total_amount,
                    confidence_score=0.85
                ))
        
        return patterns
    
    def _detect_coordinated_contributions(self, aggregated_donors: List[Dict[str, Any]]) -> List[FraudPattern]:
        """Detect multiple people from same employer/location making similar contributions"""
        patterns = []
        
        # Group by employer + state
        employer_groups = defaultdict(list)
        for donor in aggregated_donors:
            employer = donor.get('canonical_employer', '')
            state = donor.get('canonical_state', '')
            if employer and state:
                key = f"{employer}|{state}"
                employer_groups[key].append(donor)
        
        for key, donors in employer_groups.items():
            if len(donors) >= 5:  # 5+ people from same employer/location
                # Check if they all contributed similar amounts (just under threshold)
                amounts = [d.get('total_amount', 0) for d in donors]
                if all(190 <= amt < 200 for amt in amounts):  # All just under threshold
                    total_amount = sum(amounts)
                    employer, state = key.split('|')
                    
                    affected_contributions = []
                    for donor in donors[:10]:  # Limit to first 10
                        affected_contributions.append({
                            'donor_name': donor.get('canonical_name'),
                            'amount': donor.get('total_amount'),
                            'employer': employer,
                            'state': state
                        })
                    
                    patterns.append(FraudPattern(
                        pattern_type="coordinated_contributions",
                        severity="high",
                        description=f"{len(donors)} contributors from {employer} ({state}) all contributing "
                                   f"just under reporting threshold (${190}-${200})",
                        affected_contributions=affected_contributions,
                        total_amount=total_amount,
                        confidence_score=0.8
                    ))
        
        return patterns
    
    def _detect_rapid_sequential_contributions(self, aggregated_donors: List[Dict[str, Any]]) -> List[FraudPattern]:
        """Detect same person making many contributions in short time period"""
        patterns = []
        
        for donor in aggregated_donors:
            contribution_count = donor.get('contribution_count', 0)
            first_date = donor.get('first_contribution_date')
            last_date = donor.get('last_contribution_date')
            
            if contribution_count >= 10 and first_date and last_date:
                try:
                    first = datetime.fromisoformat(first_date.replace('Z', '+00:00')) if isinstance(first_date, str) else first_date
                    last = datetime.fromisoformat(last_date.replace('Z', '+00:00')) if isinstance(last_date, str) else last_date
                    
                    if isinstance(first, datetime) and isinstance(last, datetime):
                        days_diff = (last - first).days
                        if days_diff <= 30:  # 10+ contributions in 30 days
                            avg_per_day = contribution_count / max(days_diff, 1)
                            if avg_per_day > 0.5:  # More than 1 contribution every 2 days
                                contrib_ids = donor.get('contribution_ids', [])
                                affected_contributions = [
                                    {'contribution_id': cid, 'donor_name': donor.get('canonical_name')}
                                    for cid in contrib_ids[:20]
                                ]
                                
                                patterns.append(FraudPattern(
                                    pattern_type="rapid_sequential",
                                    severity="medium",
                                    description=f"{donor.get('canonical_name', 'Unknown')} made {contribution_count} "
                                               f"contributions in {days_diff} days (avg {avg_per_day:.2f} per day)",
                                    affected_contributions=affected_contributions,
                                    total_amount=donor.get('total_amount', 0),
                                    confidence_score=0.7
                                ))
                except (ValueError, TypeError) as e:
                    # Skip if date parsing fails
                    continue
        
        return patterns
    
    def _detect_smurfing_with_aggregation(self, aggregated_donors: List[Dict[str, Any]]) -> List[FraudPattern]:
        """Enhanced smurfing detection using aggregated donors"""
        patterns = []
        
        for donor in aggregated_donors:
            total_amount = donor.get('total_amount', 0)
            contribution_count = donor.get('contribution_count', 0)
            
            # Check if donor has many small contributions just under threshold
            if contribution_count >= 3:
                avg_amount = total_amount / contribution_count
                # If average is just under threshold and total exceeds threshold significantly
                if (self.smurfing_threshold <= avg_amount < self.reporting_threshold and 
                    total_amount >= self.reporting_threshold * 2):
                    
                    contrib_ids = donor.get('contribution_ids', [])
                    affected_contributions = [
                        {'contribution_id': cid, 'donor_name': donor.get('canonical_name')}
                        for cid in contrib_ids[:10]
                    ]
                    
                    patterns.append(FraudPattern(
                        pattern_type="smurfing_aggregated",
                        severity="high" if contribution_count >= 5 else "medium",
                        description=f"Aggregated donor {donor.get('canonical_name', 'Unknown')} made "
                                   f"{contribution_count} contributions averaging ${avg_amount:.2f} "
                                   f"(just under threshold), total: ${total_amount:.2f}",
                        affected_contributions=affected_contributions,
                        total_amount=total_amount,
                        confidence_score=0.85 if contribution_count >= 5 else 0.7
                    ))
        
        return patterns
    
    async def _detect_threshold_clustering_with_aggregation(self, aggregated_donors: List[Dict[str, Any]], contributions: List[Dict[str, Any]]) -> List[FraudPattern]:
        """Enhanced threshold clustering using aggregated donors"""
        patterns = []
        
        # Create a mapping of contribution_id to contribution data for limit lookup
        contrib_map = {c.get('contribution_id'): c for c in contributions if c.get('contribution_id')}
        
        for donor in aggregated_donors:
            total_amount = donor.get('total_amount', 0)
            contrib_ids = donor.get('contribution_ids', [])
            
            # Get limit (similar to other methods)
            limit = self.contribution_limit_individual  # Default fallback
            
            if contrib_ids and self.limits_service:
                first_contrib_id = contrib_ids[0] if contrib_ids else None
                if first_contrib_id and first_contrib_id in contrib_map:
                    contrib = contrib_map[first_contrib_id]
                    contrib_date = contrib.get('contribution_date')
                    if contrib_date:
                        try:
                            if isinstance(contrib_date, str):
                                contrib_date = datetime.fromisoformat(contrib_date.replace('Z', '+00:00'))
                            if contrib_date:
                                limit = await self._get_contribution_limit(contrib_date, contrib)
                        except Exception:
                            pass
            
            # Check if aggregated total is near or over limit
            if (total_amount >= limit * 0.9 and total_amount <= limit * 1.1):
                affected_contributions = [
                    {'contribution_id': cid, 'donor_name': donor.get('canonical_name')}
                    for cid in contrib_ids[:20]
                ]
                
                patterns.append(FraudPattern(
                    pattern_type="threshold_clustering_aggregated",
                    severity="medium",
                    description=f"Aggregated donor {donor.get('canonical_name', 'Unknown')} has total "
                               f"contributions near legal limit: ${total_amount:.2f} (Limit: ${limit:.2f})",
                    affected_contributions=affected_contributions,
                    total_amount=total_amount,
                    confidence_score=0.75
                ))
        
        return patterns

