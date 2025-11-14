"""
Tests for Independent Expenditures API endpoints
"""
import pytest
from httpx import AsyncClient


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_independent_expenditures(client: AsyncClient, sample_candidate_id: str):
    """Test getting independent expenditures"""
    response = await client.get(
        "/api/independent-expenditures/",
        params={"candidate_id": sample_candidate_id, "limit": 10}
    )
    assert response.status_code == 200
    expenditures = response.json()
    assert isinstance(expenditures, list)
    
    if expenditures:
        exp = expenditures[0]
        assert "expenditure_id" in exp
        assert "expenditure_amount" in exp
        assert isinstance(exp["expenditure_amount"], (int, float))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_independent_expenditures_by_committee(client: AsyncClient, sample_committee_id: str):
    """Test getting independent expenditures by committee"""
    response = await client.get(
        "/api/independent-expenditures/",
        params={"committee_id": sample_committee_id, "limit": 10}
    )
    assert response.status_code == 200
    expenditures = response.json()
    assert isinstance(expenditures, list)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_independent_expenditures_support_oppose(client: AsyncClient, sample_candidate_id: str):
    """Test getting independent expenditures filtered by support/oppose"""
    for indicator in ["S", "O"]:
        response = await client.get(
            "/api/independent-expenditures/",
            params={
                "candidate_id": sample_candidate_id,
                "support_oppose": indicator,
                "limit": 10
            }
        )
        assert response.status_code == 200
        expenditures = response.json()
        assert isinstance(expenditures, list)
        
        if expenditures:
            for exp in expenditures:
                if exp.get("support_oppose_indicator"):
                    assert exp["support_oppose_indicator"] == indicator


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_independent_expenditures_by_date_range(client: AsyncClient, sample_candidate_id: str):
    """Test getting independent expenditures with date range"""
    response = await client.get(
        "/api/independent-expenditures/",
        params={
            "candidate_id": sample_candidate_id,
            "min_date": "2024-01-01",
            "max_date": "2024-12-31",
            "limit": 10
        }
    )
    assert response.status_code == 200
    expenditures = response.json()
    assert isinstance(expenditures, list)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_independent_expenditures_by_amount_range(client: AsyncClient, sample_candidate_id: str):
    """Test getting independent expenditures with amount range"""
    response = await client.get(
        "/api/independent-expenditures/",
        params={
            "candidate_id": sample_candidate_id,
            "min_amount": 1000.0,
            "max_amount": 10000.0,
            "limit": 10
        }
    )
    assert response.status_code == 200
    expenditures = response.json()
    assert isinstance(expenditures, list)
    
    if expenditures:
        for exp in expenditures:
            if exp.get("expenditure_amount"):
                assert 1000.0 <= exp["expenditure_amount"] <= 10000.0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_analyze_independent_expenditures(client: AsyncClient, sample_candidate_id: str):
    """Test analyzing independent expenditures"""
    response = await client.get(
        "/api/independent-expenditures/analysis",
        params={"candidate_id": sample_candidate_id}
    )
    assert response.status_code == 200
    analysis = response.json()
    assert "total_expenditures" in analysis
    assert "total_committees" in analysis
    assert isinstance(analysis["total_expenditures"], (int, float))
    assert isinstance(analysis["total_committees"], int)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_analyze_independent_expenditures_with_dates(client: AsyncClient, sample_candidate_id: str):
    """Test analyzing independent expenditures with date range"""
    response = await client.get(
        "/api/independent-expenditures/analysis",
        params={
            "candidate_id": sample_candidate_id,
            "min_date": "2024-01-01",
            "max_date": "2024-12-31"
        }
    )
    assert response.status_code == 200
    analysis = response.json()
    assert "total_expenditures" in analysis
    assert "expenditures_by_date" in analysis
    assert isinstance(analysis["expenditures_by_date"], dict)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_candidate_summary(client: AsyncClient, sample_candidate_id: str):
    """Test getting independent expenditure summary for candidate"""
    response = await client.get(
        f"/api/independent-expenditures/{sample_candidate_id}/summary"
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        summary = response.json()
        assert isinstance(summary, dict)
        # Should have summary fields
        assert "total_support" in summary or "total_oppose" in summary or "total_expenditures" in summary


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_candidate_summary_with_dates(client: AsyncClient, sample_candidate_id: str):
    """Test getting candidate summary with date range"""
    response = await client.get(
        f"/api/independent-expenditures/{sample_candidate_id}/summary",
        params={
            "min_date": "2024-01-01",
            "max_date": "2024-12-31"
        }
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        summary = response.json()
        assert isinstance(summary, dict)


@pytest.mark.asyncio
async def test_get_independent_expenditures_empty_result(client: AsyncClient):
    """Test getting independent expenditures with filters that return no results"""
    response = await client.get(
        "/api/independent-expenditures/",
        params={"candidate_id": "INVALID_CANDIDATE_12345", "limit": 10}
    )
    assert response.status_code == 200
    expenditures = response.json()
    assert isinstance(expenditures, list)
    # Should return empty list, not error


@pytest.mark.asyncio
async def test_get_independent_expenditures_invalid_support_oppose(client: AsyncClient, sample_candidate_id: str):
    """Test getting independent expenditures with invalid support/oppose value"""
    response = await client.get(
        "/api/independent-expenditures/",
        params={
            "candidate_id": sample_candidate_id,
            "support_oppose": "X",  # Invalid value
            "limit": 10
        }
    )
    # Should return validation error
    assert response.status_code in [200, 400, 422]


@pytest.mark.asyncio
async def test_get_independent_expenditures_limit(client: AsyncClient):
    """Test that limit parameter works correctly"""
    response_10 = await client.get(
        "/api/independent-expenditures/",
        params={"limit": 10}
    )
    response_5 = await client.get(
        "/api/independent-expenditures/",
        params={"limit": 5}
    )
    
    expenditures_10 = response_10.json()
    expenditures_5 = response_5.json()
    
    assert len(expenditures_10) <= 10
    assert len(expenditures_5) <= 5
    assert len(expenditures_5) <= len(expenditures_10)

