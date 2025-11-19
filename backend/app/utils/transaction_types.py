"""
FEC Transaction Type Code Parser

This module provides utilities for parsing FEC transaction type codes and
committee types to determine contributor categories for contribution limits
and fraud detection.

FEC Transaction Type Codes (TRAN_TP):
- 10-19: Individual contributions
- 20-29: Party committee contributions
- 30-39: PAC contributions (multicandidate)
- 40-49: PAC contributions (non-multicandidate)
- Other codes for various transaction types (loans, refunds, etc.)

FEC Committee Types (CMTE_TP):
- N, Q, O, V, W: PACs (multicandidate or non-multicandidate)
- X, Y: Party committees
- H, S, P: Candidate committees (House, Senate, President)
- I: Independent expenditure committees

References:
- FEC Form 3X Schedule A: https://www.fec.gov/resources/cms-content/documents/fecfrm3x.pdf
- FEC Committee Types: https://www.fec.gov/campaign-finance-data/committee-type-code-descriptions/
"""

from typing import Optional

# Contributor categories (matching ContributionLimitsService constants)
CONTRIBUTOR_INDIVIDUAL = "individual"
CONTRIBUTOR_MULTICANDIDATE_PAC = "multicandidate_pac"
CONTRIBUTOR_NON_MULTICANDIDATE_PAC = "non_multicandidate_pac"
CONTRIBUTOR_PARTY_COMMITTEE = "party_committee"
CONTRIBUTOR_CANDIDATE_COMMITTEE = "candidate_committee"


def parse_transaction_type_code(transaction_type_code: Optional[str]) -> Optional[str]:
    """
    Parse FEC transaction type code to determine contributor category
    
    Args:
        transaction_type_code: FEC transaction type code (TRAN_TP)
        
    Returns:
        Contributor category string or None if code is invalid/unknown
        
    Examples:
        >>> parse_transaction_type_code("15")
        'individual'
        >>> parse_transaction_type_code("24")
        'party_committee'
        >>> parse_transaction_type_code("35")
        'multicandidate_pac'
    """
    if not transaction_type_code:
        return None
    
    code_str = str(transaction_type_code).strip()
    
    if not code_str or len(code_str) < 2:
        return None
    
    # Get first digit to determine category
    first_digit = code_str[0]
    
    if first_digit == '1':  # 10-19: Individual contributions
        return CONTRIBUTOR_INDIVIDUAL
    elif first_digit == '2':  # 20-29: Party contributions
        return CONTRIBUTOR_PARTY_COMMITTEE
    elif first_digit == '3':  # 30-39: Multicandidate PAC
        return CONTRIBUTOR_MULTICANDIDATE_PAC
    elif first_digit == '4':  # 40-49: Non-multicandidate PAC
        return CONTRIBUTOR_NON_MULTICANDIDATE_PAC
    else:
        # Other codes (loans, refunds, etc.) - return None for unknown
        return None


def parse_committee_type(committee_type: Optional[str]) -> Optional[str]:
    """
    Parse FEC committee type code to determine contributor category
    
    Args:
        committee_type: FEC committee type code (CMTE_TP)
        
    Returns:
        Contributor category string or None if type is invalid/unknown
        
    Examples:
        >>> parse_committee_type("X")
        'party_committee'
        >>> parse_committee_type("H")
        'candidate_committee'
        >>> parse_committee_type("N")
        'multicandidate_pac'
    """
    if not committee_type:
        return None
    
    committee_type_upper = str(committee_type).strip().upper()
    
    # Party committees
    if committee_type_upper in ['X', 'Y']:
        return CONTRIBUTOR_PARTY_COMMITTEE
    
    # Candidate committees
    if committee_type_upper in ['H', 'S', 'P']:
        return CONTRIBUTOR_CANDIDATE_COMMITTEE
    
    # PACs - most common types
    # Note: We default to multicandidate PAC as it's most common
    # In practice, you'd need to check committee data to determine
    # if a PAC is multicandidate or not
    if committee_type_upper in ['N', 'Q', 'O', 'V', 'W']:
        return CONTRIBUTOR_MULTICANDIDATE_PAC
    
    # Independent expenditure committees - treat as PAC
    if committee_type_upper == 'I':
        return CONTRIBUTOR_MULTICANDIDATE_PAC
    
    return None


def get_contributor_category_from_code(
    contribution_type_code: Optional[str] = None,
    committee_type: Optional[str] = None,
    has_employer_occupation: bool = False
) -> str:
    """
    Determine contributor category from transaction type code, committee type, and other indicators
    
    This function implements the logic for determining contributor categories used in
    contribution limits and fraud detection. It checks sources in priority order:
    1. Committee type (most reliable)
    2. Transaction type code
    3. Employer/occupation presence (indicates individual)
    4. Default to individual
    
    Args:
        contribution_type_code: FEC transaction type code (TRAN_TP)
        committee_type: FEC committee type code (CMTE_TP)
        has_employer_occupation: Whether contribution has employer/occupation info
        
    Returns:
        Contributor category string (never None - defaults to individual)
        
    Examples:
        >>> get_contributor_category_from_code(committee_type="X")
        'party_committee'
        >>> get_contributor_category_from_code(contribution_type_code="15")
        'individual'
        >>> get_contributor_category_from_code(has_employer_occupation=True)
        'individual'
    """
    # Priority 1: Check committee type first (most reliable)
    if committee_type:
        category = parse_committee_type(committee_type)
        if category:
            return category
    
    # Priority 2: Check transaction type code
    if contribution_type_code:
        category = parse_transaction_type_code(contribution_type_code)
        if category:
            return category
    
    # Priority 3: If we have employer/occupation info, it's likely an individual
    if has_employer_occupation:
        return CONTRIBUTOR_INDIVIDUAL
    
    # Default: assume individual contributor
    return CONTRIBUTOR_INDIVIDUAL

