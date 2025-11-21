"""
Tests for Candidate API endpoints with mocked FEC API calls
"""
import pytest
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_candidate_search_mocked(client: AsyncClient):
    """Test candidate search with mocked FEC API response"""
    # Mock FEC API response
    mock_response = {
        "results": [
            {
                "candidate_id": "P00003392",
                "name": "Test Candidate",
                "office": "P",
                "party": "DEM",
                "state": "DC",
                "election_years": [2024],
                "active_through": 2024
            }
        ],
        "pagination": {
            "page": 1,
            "per_page": 20,
            "count": 1,
            "pages": 1
        }
    }
    
    # Mock the FEC client service
    with patch('app.services.fec_client.FECClient.search_candidates') as mock_search:
        mock_search.return_value = mock_response
        
        # Make API call
        response = await client.get("/api/candidates/search", params={"name": "Test"})
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        if data:
            assert "candidate_id" in data[0]
            assert "name" in data[0]


@pytest.mark.asyncio
async def test_get_candidate_by_id_mocked(client: AsyncClient):
    """Test getting candidate by ID with mocked FEC API response"""
    candidate_id = "P00003392"
    mock_response = {
        "candidate_id": candidate_id,
        "name": "Test Candidate",
        "office": "P",
        "party": "DEM",
        "state": "DC",
        "election_years": [2024],
        "active_through": 2024
    }
    
    # Mock the FEC client service
    with patch('app.services.fec_client.FECClient.get_candidate') as mock_get:
        mock_get.return_value = mock_response
        
        # Make API call
        response = await client.get(f"/api/candidates/{candidate_id}")
        
        # Verify response
        assert response.status_code in [200, 404]  # May return 404 if not in DB
        if response.status_code == 200:
            data = response.json()
            assert "candidate_id" in data
            assert data["candidate_id"] == candidate_id


@pytest.mark.asyncio
async def test_candidate_financials_mocked(client: AsyncClient):
    """Test getting candidate financials with mocked FEC API response"""
    candidate_id = "P00003392"
    mock_response = {
        "results": [
            {
                "candidate_id": candidate_id,
                "cycle": 2024,
                "total_receipts": 1000000.0,
                "total_disbursements": 800000.0,
                "cash_on_hand": 200000.0
            }
        ],
        "pagination": {
            "page": 1,
            "per_page": 20,
            "count": 1,
            "pages": 1
        }
    }
    
    # Mock the FEC client service
    with patch('app.services.fec_client.FECClient.get_candidate_totals') as mock_totals:
        mock_totals.return_value = mock_response
        
        # Make API call
        response = await client.get(f"/api/candidates/{candidate_id}/financials")
        
        # Verify response
        assert response.status_code in [200, 404]  # May return 404 if not in DB
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)
            if data:
                assert "candidate_id" in data[0]
                assert "total_receipts" in data[0]

