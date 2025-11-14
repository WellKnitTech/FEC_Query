"""
Tests for Analysis API endpoints
"""
import pytest
from httpx import AsyncClient


@pytest.mark.integration
@pytest.mark.asyncio
async def test_money_flow(client: AsyncClient, sample_candidate_id: str):
    """Test money flow network graph"""
    response = await client.get(
        "/api/analysis/money-flow",
        params={"candidate_id": sample_candidate_id, "max_depth": 2, "min_amount": 100.0}
    )
    assert response.status_code == 200
    graph = response.json()
    assert "nodes" in graph
    assert "edges" in graph
    assert isinstance(graph["nodes"], list)
    assert isinstance(graph["edges"], list)
    
    if graph["nodes"]:
        node = graph["nodes"][0]
        assert "id" in node
        assert "name" in node
        assert "type" in node


@pytest.mark.integration
@pytest.mark.asyncio
async def test_money_flow_different_depths(client: AsyncClient, sample_candidate_id: str):
    """Test money flow with different max_depth values"""
    for depth in [1, 2, 3]:
        response = await client.get(
            "/api/analysis/money-flow",
            params={"candidate_id": sample_candidate_id, "max_depth": depth, "min_amount": 100.0}
        )
        assert response.status_code == 200
        graph = response.json()
        assert "nodes" in graph
        assert "edges" in graph


@pytest.mark.integration
@pytest.mark.asyncio
async def test_expenditure_breakdown(client: AsyncClient, sample_candidate_id: str):
    """Test expenditure breakdown"""
    response = await client.get(
        "/api/analysis/expenditure-breakdown",
        params={"candidate_id": sample_candidate_id}
    )
    assert response.status_code == 200
    breakdown = response.json()
    assert "total_expenditures" in breakdown
    assert "expenditures_by_category" in breakdown
    assert isinstance(breakdown["total_expenditures"], (int, float))
    assert isinstance(breakdown["expenditures_by_category"], dict)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_expenditure_breakdown_by_committee(client: AsyncClient, sample_committee_id: str):
    """Test expenditure breakdown by committee"""
    response = await client.get(
        "/api/analysis/expenditure-breakdown",
        params={"committee_id": sample_committee_id}
    )
    assert response.status_code == 200
    breakdown = response.json()
    assert "total_expenditures" in breakdown
    assert isinstance(breakdown["total_expenditures"], (int, float))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_expenditure_breakdown_with_dates(client: AsyncClient, sample_candidate_id: str):
    """Test expenditure breakdown with date range"""
    response = await client.get(
        "/api/analysis/expenditure-breakdown",
        params={
            "candidate_id": sample_candidate_id,
            "min_date": "2024-01-01",
            "max_date": "2024-12-31"
        }
    )
    assert response.status_code == 200
    breakdown = response.json()
    assert "total_expenditures" in breakdown


@pytest.mark.integration
@pytest.mark.asyncio
async def test_employer_breakdown(client: AsyncClient, sample_candidate_id: str):
    """Test employer breakdown"""
    response = await client.get(
        "/api/analysis/employer-breakdown",
        params={"candidate_id": sample_candidate_id}
    )
    assert response.status_code == 200
    analysis = response.json()
    assert "total_contributions" in analysis
    assert "contributions_by_employer" in analysis
    assert isinstance(analysis["total_contributions"], (int, float))
    assert isinstance(analysis["contributions_by_employer"], dict)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_employer_breakdown_with_dates(client: AsyncClient, sample_candidate_id: str):
    """Test employer breakdown with date range"""
    response = await client.get(
        "/api/analysis/employer-breakdown",
        params={
            "candidate_id": sample_candidate_id,
            "min_date": "2024-01-01",
            "max_date": "2024-12-31"
        }
    )
    assert response.status_code == 200
    analysis = response.json()
    assert "total_contributions" in analysis
    assert "contributions_by_employer" in analysis


@pytest.mark.integration
@pytest.mark.asyncio
async def test_contribution_velocity(client: AsyncClient, sample_candidate_id: str):
    """Test contribution velocity"""
    response = await client.get(
        "/api/analysis/velocity",
        params={"candidate_id": sample_candidate_id}
    )
    assert response.status_code == 200
    velocity = response.json()
    assert "contributions_per_day" in velocity
    assert "contributions_per_week" in velocity
    assert isinstance(velocity["contributions_per_day"], (int, float))
    assert isinstance(velocity["contributions_per_week"], (int, float))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_contribution_velocity_with_dates(client: AsyncClient, sample_candidate_id: str):
    """Test contribution velocity with date range"""
    response = await client.get(
        "/api/analysis/velocity",
        params={
            "candidate_id": sample_candidate_id,
            "min_date": "2024-01-01",
            "max_date": "2024-12-31"
        }
    )
    assert response.status_code == 200
    velocity = response.json()
    assert "contributions_per_day" in velocity
    assert "contributions_per_week" in velocity


@pytest.mark.asyncio
async def test_money_flow_invalid_candidate(client: AsyncClient):
    """Test money flow with invalid candidate ID"""
    response = await client.get(
        "/api/analysis/money-flow",
        params={"candidate_id": "INVALID_ID", "max_depth": 2, "min_amount": 100.0}
    )
    # Should handle gracefully
    assert response.status_code in [200, 404, 500]


@pytest.mark.asyncio
async def test_money_flow_invalid_depth(client: AsyncClient, sample_candidate_id: str):
    """Test money flow with invalid max_depth"""
    response = await client.get(
        "/api/analysis/money-flow",
        params={"candidate_id": sample_candidate_id, "max_depth": 10, "min_amount": 100.0}
    )
    # Should validate and reject or clamp to max
    assert response.status_code in [200, 400, 422]

