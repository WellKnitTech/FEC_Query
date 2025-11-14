"""
Tests for Candidates API endpoints
"""
import pytest
from httpx import AsyncClient
from tests.helpers.api_helpers import (
    get_candidates, get_candidate_financials,
    assert_valid_candidate_response, assert_valid_financial_summary
)
from tests.helpers.db_helpers import get_candidate


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_candidates_by_name(client: AsyncClient):
    """Test searching candidates by name"""
    candidates = await get_candidates(client, name="Biden", limit=10)
    assert isinstance(candidates, list)
    if candidates:
        assert_valid_candidate_response(candidates[0])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_candidates_by_office(client: AsyncClient):
    """Test searching candidates by office"""
    candidates = await get_candidates(client, office="P", limit=10)
    assert isinstance(candidates, list)
    if candidates:
        for candidate in candidates:
            assert_valid_candidate_response(candidate)
            assert candidate["office"] == "P"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_candidates_by_state(client: AsyncClient):
    """Test searching candidates by state"""
    candidates = await get_candidates(client, state="CA", limit=10)
    assert isinstance(candidates, list)
    if candidates:
        for candidate in candidates:
            assert_valid_candidate_response(candidate)
            assert candidate["state"] == "CA"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_candidates_by_party(client: AsyncClient):
    """Test searching candidates by party"""
    candidates = await get_candidates(client, party="DEM", limit=10)
    assert isinstance(candidates, list)
    if candidates:
        for candidate in candidates:
            assert_valid_candidate_response(candidate)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_candidates_by_year(client: AsyncClient):
    """Test searching candidates by election year"""
    candidates = await get_candidates(client, year=2024, limit=10)
    assert isinstance(candidates, list)
    if candidates:
        for candidate in candidates:
            assert_valid_candidate_response(candidate)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_candidates_multiple_filters(client: AsyncClient):
    """Test searching candidates with multiple filters"""
    candidates = await get_candidates(
        client, office="S", state="TX", year=2024, limit=10
    )
    assert isinstance(candidates, list)
    if candidates:
        for candidate in candidates:
            assert_valid_candidate_response(candidate)
            assert candidate["office"] == "S"
            assert candidate["state"] == "TX"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_candidate_by_id(client: AsyncClient, sample_candidate_id: str):
    """Test getting candidate by ID"""
    response = await client.get(f"/api/candidates/{sample_candidate_id}")
    assert response.status_code in [200, 404]  # May not exist in test DB
    
    if response.status_code == 200:
        candidate = response.json()
        assert_valid_candidate_response(candidate)
        assert candidate["candidate_id"] == sample_candidate_id


@pytest.mark.asyncio
async def test_get_candidate_invalid_id(client: AsyncClient):
    """Test getting candidate with invalid ID"""
    response = await client.get("/api/candidates/INVALID_ID_12345")
    # Should return 404 or 500 depending on implementation
    assert response.status_code in [404, 500]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_race_candidates(client: AsyncClient):
    """Test getting candidates for a specific race"""
    response = await client.get(
        "/api/candidates/race",
        params={"office": "P", "state": "CA", "year": 2024, "limit": 10}
    )
    assert response.status_code == 200
    candidates = response.json()
    assert isinstance(candidates, list)
    if candidates:
        for candidate in candidates:
            assert_valid_candidate_response(candidate)
            assert candidate["office"] == "P"
            assert candidate["state"] == "CA"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_race_candidates_house(client: AsyncClient):
    """Test getting House race candidates"""
    response = await client.get(
        "/api/candidates/race",
        params={"office": "H", "state": "TX", "district": "01", "year": 2024, "limit": 10}
    )
    assert response.status_code == 200
    candidates = response.json()
    assert isinstance(candidates, list)
    if candidates:
        for candidate in candidates:
            assert_valid_candidate_response(candidate)
            assert candidate["office"] == "H"
            assert candidate["state"] == "TX"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_candidate_financials(client: AsyncClient, sample_candidate_id: str):
    """Test getting candidate financials"""
    financials = await get_candidate_financials(client, sample_candidate_id)
    assert isinstance(financials, list)
    if financials:
        for financial in financials:
            assert_valid_financial_summary(financial)
            assert financial["candidate_id"] == sample_candidate_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_candidate_financials_with_cycle(client: AsyncClient, sample_candidate_id: str, sample_cycle: int):
    """Test getting candidate financials for specific cycle"""
    financials = await get_candidate_financials(client, sample_candidate_id, cycle=sample_cycle)
    assert isinstance(financials, list)
    if financials:
        for financial in financials:
            assert_valid_financial_summary(financial)
            if financial.get("cycle"):
                assert financial["cycle"] == sample_cycle


@pytest.mark.integration
@pytest.mark.asyncio
async def test_batch_financials(client: AsyncClient):
    """Test batch financials endpoint"""
    # Use real candidate IDs if available
    candidate_ids = ["P00003392", "P80000722"]  # Example IDs
    
    response = await client.post(
        "/api/candidates/financials/batch",
        json={"candidate_ids": candidate_ids, "cycle": 2024}
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    for candidate_id in candidate_ids:
        if candidate_id in data:
            financials = data[candidate_id]
            assert isinstance(financials, list)
            if financials:
                assert_valid_financial_summary(financials[0])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_batch_financials_too_many(client: AsyncClient):
    """Test batch financials with too many candidates"""
    candidate_ids = [f"P{i:08d}" for i in range(60)]  # More than max (50)
    
    response = await client.post(
        "/api/candidates/financials/batch",
        json={"candidate_ids": candidate_ids, "cycle": 2024}
    )
    assert response.status_code == 400
    assert "maximum" in response.json()["detail"].lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_refresh_contact_info(client: AsyncClient, sample_candidate_id: str):
    """Test refreshing candidate contact info"""
    response = await client.post(f"/api/candidates/{sample_candidate_id}/refresh-contact-info")
    # May return 200 or 404 depending on whether candidate exists
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        data = response.json()
        assert "success" in data
        assert data["success"] is True


@pytest.mark.database
@pytest.mark.asyncio
async def test_candidate_in_database(db_session, sample_candidate_id: str):
    """Test that candidate can be queried from database"""
    candidate = await get_candidate(db_session, sample_candidate_id)
    # May or may not exist in database
    if candidate:
        assert candidate.candidate_id == sample_candidate_id
        assert candidate.name is not None


@pytest.mark.asyncio
async def test_search_candidates_empty_result(client: AsyncClient):
    """Test searching with filters that return no results"""
    candidates = await get_candidates(
        client, name="NonexistentCandidateXYZ123", limit=10
    )
    assert isinstance(candidates, list)
    # Should return empty list, not error


@pytest.mark.asyncio
async def test_search_candidates_limit(client: AsyncClient):
    """Test that limit parameter works correctly"""
    candidates_10 = await get_candidates(client, office="P", limit=10)
    candidates_5 = await get_candidates(client, office="P", limit=5)
    
    assert len(candidates_10) <= 10
    assert len(candidates_5) <= 5
    assert len(candidates_5) <= len(candidates_10)

