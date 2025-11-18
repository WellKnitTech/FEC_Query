from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class CandidateSummary(BaseModel):
    candidate_id: str
    name: str
    office: Optional[str] = None
    party: Optional[str] = None
    state: Optional[str] = None
    district: Optional[str] = None
    election_years: Optional[List[int]] = None
    active_through: Optional[int] = None
    contact_info: Optional['ContactInformation'] = None
    contact_info_updated_at: Optional[str] = None  # ISO format timestamp of when contact info was last updated


class FinancialSummary(BaseModel):
    candidate_id: str
    cycle: Optional[int] = None
    total_receipts: float = 0.0
    total_disbursements: float = 0.0
    cash_on_hand: float = 0.0
    total_contributions: float = 0.0
    individual_contributions: float = 0.0
    pac_contributions: float = 0.0
    party_contributions: float = 0.0
    loan_contributions: float = 0.0


class Contribution(BaseModel):
    contribution_id: Optional[str] = None
    candidate_id: Optional[str] = None
    committee_id: Optional[str] = None
    contributor_name: Optional[str] = None
    contributor_city: Optional[str] = None
    contributor_state: Optional[str] = None
    contributor_zip: Optional[str] = None
    contributor_employer: Optional[str] = None
    contributor_occupation: Optional[str] = None
    contribution_amount: Optional[float] = 0.0
    contribution_date: Optional[str] = None
    contribution_type: Optional[str] = None
    receipt_type: Optional[str] = None
    # Additional FEC fields from Schedule A
    amendment_indicator: Optional[str] = None  # AMNDT_IND
    report_type: Optional[str] = None  # RPT_TP
    transaction_id: Optional[str] = None  # TRAN_ID
    entity_type: Optional[str] = None  # ENTITY_TP
    other_id: Optional[str] = None  # OTHER_ID
    file_number: Optional[str] = None  # FILE_NUM
    memo_code: Optional[str] = None  # MEMO_CD
    memo_text: Optional[str] = None  # MEMO_TEXT


class ContributionAnalysis(BaseModel):
    total_contributions: float
    total_contributors: int
    average_contribution: float
    contributions_by_date: Dict[str, float]
    contributions_by_state: Dict[str, float]
    top_donors: List[Dict[str, Any]]
    contribution_distribution: Dict[str, int]


class Expenditure(BaseModel):
    expenditure_id: Optional[str] = None
    candidate_id: Optional[str] = None
    committee_id: Optional[str] = None
    recipient_name: Optional[str] = None
    recipient_city: Optional[str] = None
    recipient_state: Optional[str] = None
    expenditure_amount: float
    expenditure_date: Optional[str] = None
    expenditure_purpose: Optional[str] = None
    expenditure_category: Optional[str] = None


class FraudPattern(BaseModel):
    pattern_type: str
    severity: str  # low, medium, high
    description: str
    affected_contributions: List[Dict[str, Any]]
    total_amount: float
    confidence_score: float


class FraudAnalysis(BaseModel):
    candidate_id: str
    patterns: List[FraudPattern]
    risk_score: float
    total_suspicious_amount: float
    aggregated_donors_count: Optional[int] = None  # Number of unique donors after aggregation
    aggregation_enabled: bool = False  # Whether aggregation was used


class MoneyFlowNode(BaseModel):
    id: str
    name: str
    type: str  # candidate, committee, donor
    amount: Optional[float] = None


class MoneyFlowEdge(BaseModel):
    source: str
    target: str
    amount: float
    type: Optional[str] = None


class MoneyFlowGraph(BaseModel):
    nodes: List[MoneyFlowNode]
    edges: List[MoneyFlowEdge]


class ExpenditureBreakdown(BaseModel):
    total_expenditures: float
    total_transactions: int
    average_expenditure: float
    expenditures_by_date: Dict[str, float]
    expenditures_by_category: Dict[str, float]
    expenditures_by_recipient: List[Dict[str, Any]]
    top_recipients: List[Dict[str, Any]]


class EmployerAnalysis(BaseModel):
    total_by_employer: Dict[str, float]
    top_employers: List[Dict[str, Any]]
    employer_count: int
    total_contributions: float


class BatchFinancialsRequest(BaseModel):
    candidate_ids: List[str]
    cycle: Optional[int] = None


class ContributionVelocity(BaseModel):
    velocity_by_date: Dict[str, float]  # contributions per day
    velocity_by_week: Dict[str, float]  # contributions per week
    peak_days: List[Dict[str, Any]]
    average_daily_velocity: float


class CumulativeTotals(BaseModel):
    """Cumulative contribution totals aggregated by date"""
    totals_by_date: Dict[str, float]  # Date -> cumulative total
    total_amount: float
    first_date: Optional[str] = None
    last_date: Optional[str] = None


class IndependentExpenditure(BaseModel):
    expenditure_id: Optional[str] = None
    cycle: Optional[int] = None
    committee_id: Optional[str] = None
    candidate_id: Optional[str] = None
    candidate_name: Optional[str] = None
    support_oppose_indicator: Optional[str] = None  # 'S' for support, 'O' for oppose
    expenditure_amount: float = 0.0
    expenditure_date: Optional[str] = None
    payee_name: Optional[str] = None
    expenditure_purpose: Optional[str] = None


class IndependentExpenditureAnalysis(BaseModel):
    total_expenditures: float
    total_support: float
    total_oppose: float
    total_transactions: int
    expenditures_by_date: Dict[str, float]
    expenditures_by_committee: Dict[str, float]
    expenditures_by_candidate: Dict[str, float]
    top_committees: List[Dict[str, Any]]
    top_candidates: List[Dict[str, Any]]


class CommitteeSummary(BaseModel):
    committee_id: str
    name: str
    committee_type: Optional[str] = None
    committee_type_full: Optional[str] = None
    party: Optional[str] = None
    state: Optional[str] = None
    candidate_ids: Optional[List[str]] = None
    contact_info: Optional['ContactInformation'] = None


class CommitteeFinancials(BaseModel):
    committee_id: str
    cycle: Optional[int] = None
    total_receipts: float = 0.0
    total_disbursements: float = 0.0
    cash_on_hand: float = 0.0
    total_contributions: float = 0.0


class CommitteeTransfer(BaseModel):
    transfer_id: Optional[str] = None
    from_committee_id: str
    to_committee_id: Optional[str] = None
    amount: float = 0.0
    date: Optional[str] = None
    purpose: Optional[str] = None


class ContactInformation(BaseModel):
    """Contact information for candidates and committees"""
    street_address: Optional[str] = None
    street_address_2: Optional[str] = None  # Only for committees
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    treasurer_name: Optional[str] = None  # Only for committees


class AggregatedDonor(BaseModel):
    """Aggregated donor information combining contributions from same person"""
    donor_key: str
    canonical_name: str
    canonical_state: Optional[str] = None
    canonical_city: Optional[str] = None
    canonical_employer: Optional[str] = None
    canonical_occupation: Optional[str] = None
    total_amount: float
    contribution_count: int
    first_contribution_date: Optional[str] = None
    last_contribution_date: Optional[str] = None
    contribution_ids: List[str]
    all_names: List[str]  # All name variations found
    match_confidence: float  # 0.0-1.0


class DonorStateAnalysis(BaseModel):
    """Analysis of individual donors by state"""
    donors_by_state: Dict[str, int]  # Count of unique donors per state
    donor_percentages_by_state: Dict[str, float]  # Percentage of donors per state
    amounts_by_state: Dict[str, float]  # Contribution amounts by state
    amount_percentages_by_state: Dict[str, float]  # Percentage of amounts per state
    candidate_state: Optional[str] = None  # Candidate's state for comparison
    in_state_donor_percentage: float = 0.0  # Percentage of donors from candidate's state
    in_state_amount_percentage: float = 0.0  # Percentage of contribution amount from candidate's state
    out_of_state_donor_percentage: float = 0.0  # Percentage of donors from outside candidate's state
    out_of_state_amount_percentage: float = 0.0  # Percentage of amount from outside candidate's state
    total_unique_donors: int = 0  # Total count of unique donors
    total_contributions: float = 0.0  # Total contribution amount
    is_highly_out_of_state: bool = False  # Flag if >50% of donors/amounts are from outside state


# Update forward references
CandidateSummary.model_rebuild()
CommitteeSummary.model_rebuild()

