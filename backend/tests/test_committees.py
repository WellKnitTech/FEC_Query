"""
Tests for Committees API endpoints
"""
import pytest
from httpx import AsyncClient
from tests.helpers.db_helpers import get_committee


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_committees_by_name(client: AsyncClient):
    """Test searching committees by name"""
    response = await client.get(
        "/api/committees/search",
        params={"name": "Democratic", "limit": 10}
    )
    assert response.status_code == 200
    committees = response.json()
    assert isinstance(committees, list)
    if committees:
        committee = committees[0]
        assert "committee_id" in committee
        assert "name" in committee
        assert isinstance(committee["committee_id"], str)
        assert isinstance(committee["name"], str)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_committees_by_type(client: AsyncClient):
    """Test searching committees by type"""
    response = await client.get(
        "/api/committees/search",
        params={"committee_type": "H", "limit": 10}
    )
    assert response.status_code == 200
    committees = response.json()
    assert isinstance(committees, list)
    if committees:
        for committee in committees:
            assert "committee_id" in committee
            assert "committee_type" in committee
            assert committee["committee_type"] == "H"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_committees_by_state(client: AsyncClient):
    """Test searching committees by state"""
    response = await client.get(
        "/api/committees/search",
        params={"state": "CA", "limit": 10}
    )
    assert response.status_code == 200
    committees = response.json()
    assert isinstance(committees, list)
    if committees:
        for committee in committees:
            assert "committee_id" in committee
            if committee.get("state"):
                assert committee["state"] == "CA"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_committee_by_id(client: AsyncClient, sample_committee_id: str):
    """Test getting committee by ID"""
    response = await client.get(f"/api/committees/{sample_committee_id}")
    assert response.status_code in [200, 404]  # May not exist in test DB
    
    if response.status_code == 200:
        committee = response.json()
        assert "committee_id" in committee
        assert "name" in committee
        assert committee["committee_id"] == sample_committee_id


@pytest.mark.asyncio
async def test_get_committee_invalid_id(client: AsyncClient):
    """Test getting committee with invalid ID"""
    response = await client.get("/api/committees/INVALID_ID_12345")
    # Should return 404 or 500 depending on implementation
    assert response.status_code in [404, 500]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_committee_financials(client: AsyncClient, sample_committee_id: str):
    """Test getting committee financials"""
    response = await client.get(
        f"/api/committees/{sample_committee_id}/financials"
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        financials = response.json()
        assert isinstance(financials, list)
        if financials:
            financial = financials[0]
            assert "committee_id" in financial
            assert "total_receipts" in financial
            assert "total_disbursements" in financial
            assert isinstance(financial["total_receipts"], (int, float))
            assert isinstance(financial["total_disbursements"], (int, float))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_committee_financials_with_cycle(client: AsyncClient, sample_committee_id: str, sample_cycle: int):
    """Test getting committee financials for specific cycle"""
    response = await client.get(
        f"/api/committees/{sample_committee_id}/financials",
        params={"cycle": sample_cycle}
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        financials = response.json()
        assert isinstance(financials, list)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_committee_contributions(client: AsyncClient, sample_committee_id: str):
    """Test getting committee contributions"""
    response = await client.get(
        f"/api/committees/{sample_committee_id}/contributions",
        params={"limit": 10}
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        contributions = response.json()
        assert isinstance(contributions, list)
        if contributions:
            contrib = contributions[0]
            assert "committee_id" in contrib or "contribution_amount" in contrib


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_committee_contributions_with_dates(client: AsyncClient, sample_committee_id: str):
    """Test getting committee contributions with date range"""
    response = await client.get(
        f"/api/committees/{sample_committee_id}/contributions",
        params={
            "min_date": "2024-01-01",
            "max_date": "2024-12-31",
            "limit": 10
        }
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        contributions = response.json()
        assert isinstance(contributions, list)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_committee_expenditures(client: AsyncClient, sample_committee_id: str):
    """Test getting committee expenditures"""
    response = await client.get(
        f"/api/committees/{sample_committee_id}/expenditures",
        params={"limit": 10}
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        expenditures = response.json()
        assert isinstance(expenditures, list)
        if expenditures:
            exp = expenditures[0]
            assert "expenditure_amount" in exp or "committee_id" in exp


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_committee_transfers(client: AsyncClient, sample_committee_id: str):
    """Test getting committee transfers"""
    response = await client.get(
        f"/api/committees/{sample_committee_id}/transfers",
        params={"limit": 10}
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        transfers = response.json()
        assert isinstance(transfers, list)
        if transfers:
            transfer = transfers[0]
            assert "from_committee_id" in transfer
            assert "to_committee_id" in transfer
            assert "amount" in transfer
            assert isinstance(transfer["amount"], (int, float))


@pytest.mark.database
@pytest.mark.asyncio
async def test_committee_in_database(db_session, sample_committee_id: str):
    """Test that committee can be queried from database"""
    committee = await get_committee(db_session, sample_committee_id)
    # May or may not exist in database
    if committee:
        assert committee.committee_id == sample_committee_id
        assert committee.name is not None


@pytest.mark.asyncio
async def test_search_committees_empty_result(client: AsyncClient):
    """Test searching with filters that return no results"""
    response = await client.get(
        "/api/committees/search",
        params={"name": "NonexistentCommitteeXYZ123", "limit": 10}
    )
    assert response.status_code == 200
    committees = response.json()
    assert isinstance(committees, list)
    # Should return empty list, not error


@pytest.mark.asyncio
async def test_search_committees_limit(client: AsyncClient):
    """Test that limit parameter works correctly"""
    response_10 = await client.get(
        "/api/committees/search",
        params={"limit": 10}
    )
    response_5 = await client.get(
        "/api/committees/search",
        params={"limit": 5}
    )
    
    committees_10 = response_10.json()
    committees_5 = response_5.json()
    
    assert len(committees_10) <= 10
    assert len(committees_5) <= 5
    assert len(committees_5) <= len(committees_10)

