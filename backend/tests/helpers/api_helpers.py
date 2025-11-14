"""
API helper functions for tests
"""
from httpx import AsyncClient
from typing import Optional, Dict, Any, List
import json


async def make_api_request(
    client: AsyncClient,
    method: str,
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    json_data: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """Make an API request and return JSON response"""
    if method.upper() == "GET":
        response = await client.get(endpoint, params=params, headers=headers)
    elif method.upper() == "POST":
        response = await client.post(endpoint, json=json_data, params=params, headers=headers)
    elif method.upper() == "PUT":
        response = await client.put(endpoint, json=json_data, params=params, headers=headers)
    elif method.upper() == "DELETE":
        response = await client.delete(endpoint, params=params, headers=headers)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")
    
    response.raise_for_status()
    return response.json()


async def get_candidates(
    client: AsyncClient,
    name: Optional[str] = None,
    office: Optional[str] = None,
    state: Optional[str] = None,
    party: Optional[str] = None,
    year: Optional[int] = None,
    limit: int = 20
) -> List[Dict[str, Any]]:
    """Get candidates via API"""
    params = {"limit": limit}
    if name:
        params["name"] = name
    if office:
        params["office"] = office
    if state:
        params["state"] = state
    if party:
        params["party"] = party
    if year:
        params["year"] = year
    
    return await make_api_request(client, "GET", "/api/candidates/search", params=params)


async def get_contributions(
    client: AsyncClient,
    candidate_id: Optional[str] = None,
    committee_id: Optional[str] = None,
    contributor_name: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None,
    min_date: Optional[str] = None,
    max_date: Optional[str] = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Get contributions via API"""
    params = {"limit": limit}
    if candidate_id:
        params["candidate_id"] = candidate_id
    if committee_id:
        params["committee_id"] = committee_id
    if contributor_name:
        params["contributor_name"] = contributor_name
    if min_amount:
        params["min_amount"] = min_amount
    if max_amount:
        params["max_amount"] = max_amount
    if min_date:
        params["min_date"] = min_date
    if max_date:
        params["max_date"] = max_date
    
    return await make_api_request(client, "GET", "/api/contributions/", params=params)


async def get_candidate_financials(
    client: AsyncClient,
    candidate_id: str,
    cycle: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Get candidate financials via API"""
    params = {}
    if cycle:
        params["cycle"] = cycle
    
    return await make_api_request(
        client, "GET", f"/api/candidates/{candidate_id}/financials", params=params
    )


async def analyze_fraud(
    client: AsyncClient,
    candidate_id: str,
    min_date: Optional[str] = None,
    max_date: Optional[str] = None,
    use_aggregation: bool = False
) -> Dict[str, Any]:
    """Analyze fraud patterns via API"""
    if use_aggregation:
        params = {}
        if min_date:
            params["min_date"] = min_date
        if max_date:
            params["max_date"] = max_date
        params["use_aggregation"] = True
        return await make_api_request(
            client, "GET", "/api/fraud/analyze-donors", params={**params, "candidate_id": candidate_id}
        )
    else:
        params = {"candidate_id": candidate_id}
        if min_date:
            params["min_date"] = min_date
        if max_date:
            params["max_date"] = max_date
        return await make_api_request(client, "GET", "/api/fraud/analyze", params=params)


def assert_valid_candidate_response(candidate: Dict[str, Any]) -> None:
    """Assert that a candidate response has required fields"""
    assert "candidate_id" in candidate
    assert "name" in candidate
    assert isinstance(candidate["candidate_id"], str)
    assert isinstance(candidate["name"], str)


def assert_valid_contribution_response(contribution: Dict[str, Any]) -> None:
    """Assert that a contribution response has required fields"""
    assert "contribution_id" in contribution or "contribution_amount" in contribution
    if "contribution_amount" in contribution:
        assert isinstance(contribution["contribution_amount"], (int, float))


def assert_valid_financial_summary(financial: Dict[str, Any]) -> None:
    """Assert that a financial summary has required fields"""
    assert "candidate_id" in financial
    assert "total_receipts" in financial
    assert "total_disbursements" in financial
    assert isinstance(financial["total_receipts"], (int, float))
    assert isinstance(financial["total_disbursements"], (int, float))


def assert_valid_fraud_analysis(analysis: Dict[str, Any]) -> None:
    """Assert that a fraud analysis has required fields"""
    assert "candidate_id" in analysis
    assert "patterns" in analysis
    assert "risk_score" in analysis
    assert isinstance(analysis["patterns"], list)
    assert isinstance(analysis["risk_score"], (int, float))

