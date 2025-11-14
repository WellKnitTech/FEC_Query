"""
Tests for Contributions API endpoints
"""
import pytest
from httpx import AsyncClient
from tests.helpers.api_helpers import (
    get_contributions, assert_valid_contribution_response
)
from tests.helpers.db_helpers import (
    get_contribution_count, get_contributions as db_get_contributions,
    get_unique_contributors
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_contributions_by_candidate(client: AsyncClient, sample_candidate_id: str):
    """Test getting contributions by candidate ID"""
    contributions = await get_contributions(client, candidate_id=sample_candidate_id, limit=10)
    assert isinstance(contributions, list)
    if contributions:
        for contrib in contributions:
            assert_valid_contribution_response(contrib)
            if contrib.get("candidate_id"):
                assert contrib["candidate_id"] == sample_candidate_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_contributions_by_committee(client: AsyncClient, sample_committee_id: str):
    """Test getting contributions by committee ID"""
    contributions = await get_contributions(client, committee_id=sample_committee_id, limit=10)
    assert isinstance(contributions, list)
    if contributions:
        for contrib in contributions:
            assert_valid_contribution_response(contrib)
            if contrib.get("committee_id"):
                assert contrib["committee_id"] == sample_committee_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_contributions_by_contributor_name(client: AsyncClient):
    """Test getting contributions by contributor name"""
    contributions = await get_contributions(client, contributor_name="Smith", limit=10)
    assert isinstance(contributions, list)
    if contributions:
        for contrib in contributions:
            assert_valid_contribution_response(contrib)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_contributions_by_amount_range(client: AsyncClient):
    """Test getting contributions by amount range"""
    contributions = await get_contributions(
        client, min_amount=1000.0, max_amount=5000.0, limit=10
    )
    assert isinstance(contributions, list)
    if contributions:
        for contrib in contributions:
            assert_valid_contribution_response(contrib)
            if contrib.get("contribution_amount"):
                assert 1000.0 <= contrib["contribution_amount"] <= 5000.0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_contributions_by_date_range(client: AsyncClient):
    """Test getting contributions by date range"""
    contributions = await get_contributions(
        client, min_date="2024-01-01", max_date="2024-12-31", limit=10
    )
    assert isinstance(contributions, list)
    if contributions:
        for contrib in contributions:
            assert_valid_contribution_response(contrib)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_contributions_multiple_filters(client: AsyncClient, sample_candidate_id: str):
    """Test getting contributions with multiple filters"""
    contributions = await get_contributions(
        client,
        candidate_id=sample_candidate_id,
        min_amount=100.0,
        min_date="2024-01-01",
        limit=10
    )
    assert isinstance(contributions, list)
    if contributions:
        for contrib in contributions:
            assert_valid_contribution_response(contrib)


@pytest.mark.database
@pytest.mark.asyncio
async def test_get_contributions_from_database(db_session, sample_candidate_id: str):
    """Test getting contributions from database directly"""
    count = await get_contribution_count(db_session, candidate_id=sample_candidate_id)
    assert isinstance(count, int)
    assert count >= 0
    
    if count > 0:
        contributions = await db_get_contributions(
            db_session, candidate_id=sample_candidate_id, limit=10
        )
        assert len(contributions) > 0
        for contrib in contributions:
            assert contrib.candidate_id == sample_candidate_id
            assert contrib.contribution_amount is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_unique_contributors(client: AsyncClient):
    """Test getting unique contributors"""
    response = await client.get(
        "/api/contributions/unique-contributors",
        params={"search_term": "Smith", "limit": 10}
    )
    assert response.status_code == 200
    contributors = response.json()
    assert isinstance(contributors, list)
    if contributors:
        for contributor in contributors:
            assert "name" in contributor
            assert "total_amount" in contributor
            assert "contribution_count" in contributor
            assert isinstance(contributor["total_amount"], (int, float))
            assert isinstance(contributor["contribution_count"], int)


@pytest.mark.database
@pytest.mark.asyncio
async def test_get_unique_contributors_from_database(db_session):
    """Test getting unique contributors from database"""
    contributors = await get_unique_contributors(db_session, "Smith", limit=10)
    assert isinstance(contributors, list)
    if contributors:
        for contributor in contributors:
            assert "name" in contributor
            assert "total_amount" in contributor
            assert "contribution_count" in contributor


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_aggregated_donors(client: AsyncClient, sample_candidate_id: str):
    """Test getting aggregated donors"""
    response = await client.get(
        "/api/contributions/aggregated-donors",
        params={"candidate_id": sample_candidate_id, "limit": 10}
    )
    assert response.status_code == 200
    donors = response.json()
    assert isinstance(donors, list)
    if donors:
        for donor in donors:
            assert "name" in donor
            assert "total_amount" in donor
            assert "contribution_count" in donor
            assert isinstance(donor["total_amount"], (int, float))
            assert isinstance(donor["contribution_count"], int)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_contribution_analysis(client: AsyncClient, sample_candidate_id: str):
    """Test contribution analysis endpoint"""
    response = await client.get(
        "/api/contributions/analysis",
        params={"candidate_id": sample_candidate_id}
    )
    assert response.status_code == 200
    analysis = response.json()
    assert "total_contributions" in analysis
    assert "total_contributors" in analysis
    assert "average_contribution" in analysis
    assert isinstance(analysis["total_contributions"], (int, float))
    assert isinstance(analysis["total_contributors"], int)
    assert isinstance(analysis["average_contribution"], (int, float))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_contribution_analysis_with_dates(client: AsyncClient, sample_candidate_id: str):
    """Test contribution analysis with date range"""
    response = await client.get(
        "/api/contributions/analysis",
        params={
            "candidate_id": sample_candidate_id,
            "min_date": "2024-01-01",
            "max_date": "2024-12-31"
        }
    )
    assert response.status_code == 200
    analysis = response.json()
    assert "total_contributions" in analysis
    assert "contributions_by_date" in analysis
    assert isinstance(analysis["contributions_by_date"], dict)


@pytest.mark.asyncio
async def test_get_contributions_empty_result(client: AsyncClient):
    """Test getting contributions with filters that return no results"""
    contributions = await get_contributions(
        client, candidate_id="INVALID_CANDIDATE_12345", limit=10
    )
    assert isinstance(contributions, list)
    # Should return empty list, not error


@pytest.mark.asyncio
async def test_get_contributions_limit(client: AsyncClient):
    """Test that limit parameter works correctly"""
    contributions_10 = await get_contributions(client, limit=10)
    contributions_5 = await get_contributions(client, limit=5)
    
    assert len(contributions_10) <= 10
    assert len(contributions_5) <= 5
    assert len(contributions_5) <= len(contributions_10)


@pytest.mark.asyncio
async def test_get_contributions_invalid_date_format(client: AsyncClient):
    """Test getting contributions with invalid date format"""
    response = await client.get(
        "/api/contributions/",
        params={"min_date": "invalid-date", "limit": 10}
    )
    # Should handle gracefully - either 400 or ignore invalid date
    assert response.status_code in [200, 400, 422]


@pytest.mark.asyncio
async def test_get_contributions_negative_amount(client: AsyncClient):
    """Test getting contributions with negative amount"""
    response = await client.get(
        "/api/contributions/",
        params={"min_amount": -100.0, "limit": 10}
    )
    # Should handle gracefully
    assert response.status_code in [200, 400, 422]

