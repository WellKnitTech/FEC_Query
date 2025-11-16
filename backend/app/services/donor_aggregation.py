import re
import logging
from typing import List, Dict, Any, Optional
from collections import defaultdict
from datetime import datetime
from app.utils.date_utils import serialize_date
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)


class DonorAggregationService:
    """Service for aggregating contributions from the same donor across name variations"""
    
    def __init__(
        self,
        name_similarity_threshold: float = 0.85,
        min_fields_for_match: int = 2,
        max_name_variations: int = 10
    ):
        """
        Initialize the donor aggregation service
        
        Args:
            name_similarity_threshold: Minimum similarity score (0.0-1.0) to consider names a match
            min_fields_for_match: Minimum number of matching fields (name, state, employer, occupation) required
            max_name_variations: Maximum number of name variations to track per donor
        """
        self.name_similarity_threshold = name_similarity_threshold
        self.min_fields_for_match = min_fields_for_match
        self.max_name_variations = max_name_variations
    
    def normalize_text(self, text: Optional[str]) -> str:
        """Normalize text for comparison"""
        if not text:
            return ""
        # Lowercase, remove extra whitespace, remove punctuation
        text = str(text).lower().strip()
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text
    
    def normalize_name(self, name: Optional[str]) -> str:
        """Normalize name for better matching"""
        if not name:
            return ""
        normalized = self.normalize_text(name)
        # Remove common suffixes
        normalized = re.sub(r'\s+(jr|sr|ii|iii|iv|esq)\.?$', '', normalized, flags=re.IGNORECASE)
        # Handle initials (e.g., "J. Smith" -> "j smith")
        normalized = re.sub(r'\b([a-z])\.\s*', r'\1 ', normalized)
        return normalized
    
    def normalize_employer(self, employer: Optional[str]) -> str:
        """Normalize employer name"""
        if not employer:
            return ""
        normalized = self.normalize_text(employer)
        # Remove common business suffixes
        normalized = re.sub(r'\s+(inc|llc|corp|ltd|co)\.?$', '', normalized, flags=re.IGNORECASE)
        return normalized
    
    def create_donor_key(self, contrib: Dict[str, Any], strategy: str = "exact") -> str:
        """
        Create a unique key for donor matching
        
        Args:
            contrib: Contribution dictionary
            strategy: "exact", "fuzzy", or "partial"
        """
        name = self.normalize_name(contrib.get('contributor_name', ''))
        state = self.normalize_text(contrib.get('contributor_state', ''))
        employer = self.normalize_employer(contrib.get('contributor_employer', ''))
        occupation = self.normalize_text(contrib.get('contributor_occupation', ''))
        
        if strategy == "exact":
            # Primary key: name + state + employer + occupation
            key = f"{name}|{state}|{employer}|{occupation}"
        elif strategy == "partial":
            # Partial key: name + state + employer (occupation may vary)
            key = f"{name}|{state}|{employer}"
        else:  # fuzzy
            # Fuzzy key: name + state (employer/occupation may vary)
            key = f"{name}|{state}"
        
        return key
    
    def calculate_match_confidence(
        self,
        contrib1: Dict[str, Any],
        contrib2: Dict[str, Any]
    ) -> float:
        """
        Calculate confidence score (0.0-1.0) that two contributions are from the same donor
        
        Returns:
            Confidence score where 1.0 = exact match, 0.0 = no match
        """
        name1 = self.normalize_name(contrib1.get('contributor_name', ''))
        name2 = self.normalize_name(contrib2.get('contributor_name', ''))
        state1 = self.normalize_text(contrib1.get('contributor_state', ''))
        state2 = self.normalize_text(contrib2.get('contributor_state', ''))
        employer1 = self.normalize_employer(contrib1.get('contributor_employer', ''))
        employer2 = self.normalize_employer(contrib2.get('contributor_employer', ''))
        occupation1 = self.normalize_text(contrib1.get('contributor_occupation', ''))
        occupation2 = self.normalize_text(contrib2.get('contributor_occupation', ''))
        
        # Calculate similarity scores
        name_sim = fuzz.ratio(name1, name2) / 100.0 if name1 and name2 else 0.0
        state_match = 1.0 if state1 and state2 and state1 == state2 else 0.0
        employer_sim = fuzz.ratio(employer1, employer2) / 100.0 if employer1 and employer2 else 0.0
        occupation_sim = fuzz.ratio(occupation1, occupation2) / 100.0 if occupation1 and occupation2 else 0.0
        
        # Weighted confidence calculation
        # Name is most important (40%), state is required (30%), employer (20%), occupation (10%)
        confidence = (
            name_sim * 0.4 +
            state_match * 0.3 +
            employer_sim * 0.2 +
            occupation_sim * 0.1
        )
        
        # Require minimum name similarity
        if name_sim < self.name_similarity_threshold:
            confidence *= 0.5  # Penalize low name similarity
        
        # Require state match for high confidence
        if not state_match:
            confidence *= 0.7  # Penalize missing state match
        
        return min(1.0, max(0.0, confidence))
    
    def merge_similar_donors(
        self,
        donor_groups: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Merge donor groups with similar names using fuzzy matching
        
        Args:
            donor_groups: Dictionary mapping donor keys to lists of contributions
        
        Returns:
            Merged donor groups
        """
        merged = {}
        processed_keys = set()
        
        # Convert to list of (key, contributions) for easier processing
        groups = list(donor_groups.items())
        
        for i, (key1, contribs1) in enumerate(groups):
            if key1 in processed_keys:
                continue
            
            # Use first contribution as canonical
            canonical1 = contribs1[0] if contribs1 else {}
            merged_key = key1
            merged_contribs = contribs1.copy()
            
            # Check for similar groups
            for j, (key2, contribs2) in enumerate(groups[i+1:], start=i+1):
                if key2 in processed_keys:
                    continue
                
                canonical2 = contribs2[0] if contribs2 else {}
                confidence = self.calculate_match_confidence(canonical1, canonical2)
                
                # Merge if confidence is high enough
                if confidence >= self.name_similarity_threshold:
                    # Use the key with more complete information
                    if len(merged_key.split('|')) < len(key2.split('|')):
                        merged_key = key2
                    
                    merged_contribs.extend(contribs2)
                    processed_keys.add(key2)
            
            merged[merged_key] = merged_contribs
            processed_keys.add(key1)
        
        return merged
    
    def aggregate_donors(
        self,
        contributions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Group contributions by likely same donor
        
        Args:
            contributions: List of contribution dictionaries
        
        Returns:
            List of aggregated donor dictionaries
        """
        if not contributions:
            return []
        
        logger.info(f"Aggregating {len(contributions)} contributions into donors")
        
        # First pass: exact matches
        donor_groups = defaultdict(list)
        for contrib in contributions:
            key = self.create_donor_key(contrib, strategy="exact")
            donor_groups[key].append(contrib)
        
        initial_count = len(donor_groups)
        logger.debug(f"Initial grouping: {initial_count} donor groups")
        
        # Second pass: merge similar donors using fuzzy matching
        merged_groups = self.merge_similar_donors(dict(donor_groups))
        
        final_count = len(merged_groups)
        logger.info(f"After merging: {final_count} unique donors (reduced from {initial_count})")
        
        # Build aggregated donor objects
        aggregated = []
        for key, contribs in merged_groups.items():
            if not contribs:
                continue
            
            # Use the most complete record as canonical
            canonical = max(
                contribs,
                key=lambda c: sum(1 for v in [
                    c.get('contributor_name'),
                    c.get('contributor_employer'),
                    c.get('contributor_occupation'),
                    c.get('contributor_city')
                ] if v)
            )
            
            # Collect all unique names
            all_names = list(set(
                c.get('contributor_name')
                for c in contribs
                if c.get('contributor_name')
            ))
            
            # Calculate dates - parse and compare properly
            parsed_dates = []
            for c in contribs:
                date_str = c.get('contribution_date') or c.get('contribution_receipt_date')
                if date_str:
                    try:
                        # Try parsing as ISO format (YYYY-MM-DD)
                        if isinstance(date_str, str):
                            # Handle both date-only and datetime strings
                            if 'T' in date_str:
                                parsed = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                            elif len(date_str) == 10 and date_str.count('-') == 2:
                                parsed = datetime.strptime(date_str, '%Y-%m-%d')
                            else:
                                # Try other common formats
                                parsed = datetime.fromisoformat(date_str)
                            parsed_dates.append(parsed)
                        elif isinstance(date_str, datetime):
                            parsed_dates.append(date_str)
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Could not parse date '{date_str}' for donor {canonical.get('contributor_name', 'unknown')}: {e}")
                        continue
            
            first_date = min(parsed_dates) if parsed_dates else None
            last_date = max(parsed_dates) if parsed_dates else None
            
            # Log if no dates found for debugging
            if not parsed_dates and contribs:
                logger.debug(f"No valid dates found for donor {canonical.get('contributor_name', 'unknown')} with {len(contribs)} contributions")
                # Check what date fields are available
                sample_contrib = contribs[0]
                logger.debug(f"Sample contribution date fields: contribution_date={sample_contrib.get('contribution_date')}, contribution_receipt_date={sample_contrib.get('contribution_receipt_date')}")
            
            # Calculate match confidence (average of all pairwise confidences)
            if len(contribs) > 1:
                confidences = []
                for i, c1 in enumerate(contribs):
                    for c2 in contribs[i+1:]:
                        confidences.append(self.calculate_match_confidence(c1, c2))
                match_confidence = sum(confidences) / len(confidences) if confidences else 1.0
            else:
                match_confidence = 1.0
            
            # Calculate total amount, ensuring all values are converted to float
            total_amount = 0.0
            for c in contribs:
                amt = c.get('contribution_amount')
                if amt is not None:
                    try:
                        total_amount += float(amt)
                    except (ValueError, TypeError):
                        # Skip invalid amounts
                        continue
            
            # Format dates as YYYY-MM-DD strings using centralized utility
            first_date_str = serialize_date(first_date)
            last_date_str = serialize_date(last_date)
            
            aggregated.append({
                'donor_key': key,
                'canonical_name': canonical.get('contributor_name', ''),
                'canonical_state': canonical.get('contributor_state'),
                'canonical_city': canonical.get('contributor_city'),
                'canonical_employer': canonical.get('contributor_employer'),
                'canonical_occupation': canonical.get('contributor_occupation'),
                'total_amount': total_amount,
                'contribution_count': len(contribs),
                'first_contribution_date': first_date_str,
                'last_contribution_date': last_date_str,
                'contribution_ids': [c.get('contribution_id') for c in contribs if c.get('contribution_id')],
                'all_names': all_names[:self.max_name_variations],
                'match_confidence': round(match_confidence, 3)
            })
        
        # Sort by total amount descending
        aggregated.sort(key=lambda x: x['total_amount'], reverse=True)
        
        return aggregated

