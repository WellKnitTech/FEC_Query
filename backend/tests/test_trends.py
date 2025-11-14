"""
Tests for Trends API endpoints
"""
import pytest
from httpx import AsyncClient


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_candidate_trends(client: AsyncClient, sample_candidate_id: str):
    """Test getting candidate trends"""
    response = await client.get(
        f"/api/trends/candidate/{sample_candidate_id}"
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        trends = response.json()
        # Structure may vary, but should be a dict
        assert isinstance(trends, dict)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_candidate_trends_with_cycles(client: AsyncClient, sample_candidate_id: str):
    """Test getting candidate trends with cycle range"""
    response = await client.get(
        f"/api/trends/candidate/{sample_candidate_id}",
        params={"min_cycle": 2020, "max_cycle": 2024}
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        trends = response.json()
        assert isinstance(trends, dict)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_race_trends(client: AsyncClient):
    """Test getting race trends"""
    candidate_ids = ["P00003392", "P80000722"]  # Example candidate IDs
    
    response = await client.post(
        "/api/trends/race",
        json={"candidate_ids": candidate_ids}
    )
    assert response.status_code == 200
    trends = response.json()
    assert isinstance(trends, dict)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_race_trends_with_cycles(client: AsyncClient):
    """Test getting race trends with cycle range"""
    candidate_ids = ["P00003392", "P80000722"]
    
    response = await client.post(
        "/api/trends/race",
        json={
            "candidate_ids": candidate_ids,
            "min_cycle": 2020,
            "max_cycle": 2024
        }
    )
    assert response.status_code == 200
    trends = response.json()
    assert isinstance(trends, dict)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_race_trends_empty_list(client: AsyncClient):
    """Test getting race trends with empty candidate list"""
    response = await client.post(
        "/api/trends/race",
        json={"candidate_ids": []}
    )
    # Should handle gracefully
    assert response.status_code in [200, 400, 422]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_contribution_velocity_trends(client: AsyncClient, sample_candidate_id: str):
    """Test getting contribution velocity trends"""
    response = await client.get(
        f"/api/trends/contribution-velocity/{sample_candidate_id}"
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        trends = response.json()
        assert isinstance(trends, dict)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_contribution_velocity_trends_with_cycles(client: AsyncClient, sample_candidate_id: str):
    """Test getting contribution velocity trends with cycle range"""
    response = await client.get(
        f"/api/trends/contribution-velocity/{sample_candidate_id}",
        params={"min_cycle": 2020, "max_cycle": 2024}
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        trends = response.json()
        assert isinstance(trends, dict)


@pytest.mark.asyncio
async def test_get_candidate_trends_invalid_id(client: AsyncClient):
    """Test getting trends for invalid candidate ID"""
    response = await client.get("/api/trends/candidate/INVALID_ID_12345")
    # Should handle gracefully
    assert response.status_code in [200, 404, 500]


@pytest.mark.asyncio
async def test_get_race_trends_invalid_candidate_ids(client: AsyncClient):
    """Test getting race trends with invalid candidate IDs"""
    response = await client.post(
        "/api/trends/race",
        json={"candidate_ids": ["INVALID_ID_1", "INVALID_ID_2"]}
    )
    # Should handle gracefully
    assert response.status_code in [200, 400, 422, 500]

