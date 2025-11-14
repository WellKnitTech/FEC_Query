"""
Tests for Fraud Detection API endpoints
"""
import pytest
from httpx import AsyncClient
from tests.helpers.api_helpers import analyze_fraud, assert_valid_fraud_analysis


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fraud_analysis_basic(client: AsyncClient, sample_candidate_id: str):
    """Test basic fraud analysis"""
    analysis = await analyze_fraud(client, sample_candidate_id)
    assert_valid_fraud_analysis(analysis)
    assert analysis["candidate_id"] == sample_candidate_id
    assert 0 <= analysis["risk_score"] <= 100


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fraud_analysis_with_dates(client: AsyncClient, sample_candidate_id: str):
    """Test fraud analysis with date range"""
    analysis = await analyze_fraud(
        client, sample_candidate_id,
        min_date="2024-01-01",
        max_date="2024-12-31"
    )
    assert_valid_fraud_analysis(analysis)
    assert analysis["candidate_id"] == sample_candidate_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fraud_analysis_with_aggregation(client: AsyncClient, sample_candidate_id: str):
    """Test fraud analysis with donor aggregation"""
    analysis = await analyze_fraud(
        client, sample_candidate_id,
        use_aggregation=True
    )
    assert_valid_fraud_analysis(analysis)
    assert analysis["candidate_id"] == sample_candidate_id
    assert analysis.get("aggregation_enabled") is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fraud_analysis_patterns(client: AsyncClient, sample_candidate_id: str):
    """Test that fraud analysis returns patterns"""
    analysis = await analyze_fraud(client, sample_candidate_id)
    assert_valid_fraud_analysis(analysis)
    
    patterns = analysis["patterns"]
    assert isinstance(patterns, list)
    
    if patterns:
        for pattern in patterns:
            assert "pattern_type" in pattern
            assert "severity" in pattern
            assert "description" in pattern
            assert "confidence_score" in pattern
            assert pattern["severity"] in ["low", "medium", "high"]
            assert 0 <= pattern["confidence_score"] <= 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fraud_analysis_suspicious_amount(client: AsyncClient, sample_candidate_id: str):
    """Test that fraud analysis includes suspicious amount"""
    analysis = await analyze_fraud(client, sample_candidate_id)
    assert_valid_fraud_analysis(analysis)
    assert "total_suspicious_amount" in analysis
    assert isinstance(analysis["total_suspicious_amount"], (int, float))
    assert analysis["total_suspicious_amount"] >= 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fraud_analysis_aggregated_donors(client: AsyncClient, sample_candidate_id: str):
    """Test fraud analysis with aggregated donors count"""
    analysis = await analyze_fraud(
        client, sample_candidate_id,
        use_aggregation=True
    )
    assert_valid_fraud_analysis(analysis)
    
    if analysis.get("aggregation_enabled"):
        assert "aggregated_donors_count" in analysis
        assert isinstance(analysis["aggregated_donors_count"], int)
        assert analysis["aggregated_donors_count"] >= 0


@pytest.mark.asyncio
async def test_fraud_analysis_invalid_candidate(client: AsyncClient):
    """Test fraud analysis with invalid candidate ID"""
    response = await client.get(
        "/api/fraud/analyze",
        params={"candidate_id": "INVALID_CANDIDATE_12345"}
    )
    # Should handle gracefully
    assert response.status_code in [200, 404, 500]


@pytest.mark.asyncio
async def test_fraud_analysis_missing_candidate_id(client: AsyncClient):
    """Test fraud analysis without candidate ID"""
    response = await client.get("/api/fraud/analyze")
    # Should return 422 (validation error) or 400
    assert response.status_code in [400, 422]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fraud_analysis_analyze_donors_endpoint(client: AsyncClient, sample_candidate_id: str):
    """Test analyze-donors endpoint directly"""
    response = await client.get(
        "/api/fraud/analyze-donors",
        params={
            "candidate_id": sample_candidate_id,
            "use_aggregation": True
        }
    )
    assert response.status_code == 200
    analysis = response.json()
    assert_valid_fraud_analysis(analysis)
    assert analysis.get("aggregation_enabled") is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fraud_analysis_analyze_donors_without_aggregation(client: AsyncClient, sample_candidate_id: str):
    """Test analyze-donors endpoint without aggregation"""
    response = await client.get(
        "/api/fraud/analyze-donors",
        params={
            "candidate_id": sample_candidate_id,
            "use_aggregation": False
        }
    )
    assert response.status_code == 200
    analysis = response.json()
    assert_valid_fraud_analysis(analysis)
    assert analysis.get("aggregation_enabled") is False

