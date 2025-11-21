"""
Unit tests for DonorSearchService
"""
import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from app.services.donor_search import DonorSearchService
from app.services.shared.exceptions import (
    DonorSearchError,
    QueryTimeoutError,
    FTS5UnavailableError
)
from app.db.database import Contribution


@pytest.mark.asyncio
async def test_search_unique_contributors_single_word(test_db: AsyncSession):
    """Test searching with a single word (should use prefix search)"""
    # Create test data
    contrib1 = Contribution(
        contribution_id="TEST_001",
        contributor_name="Smith, John",
        contribution_amount=1000.0,
        contribution_date=datetime(2024, 1, 1)
    )
    contrib2 = Contribution(
        contribution_id="TEST_002",
        contributor_name="Smith, Jane",
        contribution_amount=2000.0,
        contribution_date=datetime(2024, 1, 2)
    )
    contrib3 = Contribution(
        contribution_id="TEST_003",
        contributor_name="Jones, Bob",
        contribution_amount=500.0,
        contribution_date=datetime(2024, 1, 3)
    )
    
    test_db.add_all([contrib1, contrib2, contrib3])
    await test_db.commit()
    
    # Search for "Smith"
    service = DonorSearchService(session=test_db)
    results = await service.search_unique_contributors("Smith", limit=10)
    
    # Should find 2 contributors
    assert len(results) == 2
    assert all("Smith" in r["name"] for r in results)
    assert all("name" in r and "total_amount" in r and "contribution_count" in r for r in results)
    
    # Results should be sorted by total_amount descending
    assert results[0]["total_amount"] >= results[1]["total_amount"]


@pytest.mark.asyncio
async def test_search_unique_contributors_multi_word(test_db: AsyncSession):
    """Test searching with multiple words (should use substring search)"""
    # Create test data
    contrib1 = Contribution(
        contribution_id="TEST_001",
        contributor_name="John Smith",
        contribution_amount=1000.0,
        contribution_date=datetime(2024, 1, 1)
    )
    contrib2 = Contribution(
        contribution_id="TEST_002",
        contributor_name="Jane Smith",
        contribution_amount=2000.0,
        contribution_date=datetime(2024, 1, 2)
    )
    
    test_db.add_all([contrib1, contrib2])
    await test_db.commit()
    
    # Search for "John Smith"
    service = DonorSearchService(session=test_db)
    results = await service.search_unique_contributors("John Smith", limit=10)
    
    # Should find 1 contributor
    assert len(results) == 1
    assert "John Smith" in results[0]["name"]


@pytest.mark.asyncio
async def test_search_no_results(test_db: AsyncSession):
    """Test searching with no matching results"""
    service = DonorSearchService(session=test_db)
    results = await service.search_unique_contributors("NonexistentName", limit=10)
    
    assert isinstance(results, list)
    assert len(results) == 0


@pytest.mark.asyncio
async def test_search_invalid_search_term(test_db: AsyncSession):
    """Test searching with invalid search term"""
    service = DonorSearchService(session=test_db)
    
    # Empty search term
    with pytest.raises(DonorSearchError):
        await service.search_unique_contributors("", limit=10)
    
    # Search term too long
    long_term = "a" * 201
    with pytest.raises(DonorSearchError):
        await service.search_unique_contributors(long_term, limit=10)


@pytest.mark.asyncio
async def test_process_query_results_row_object(test_db: AsyncSession):
    """Test result processing with Row objects"""
    from sqlalchemy import select, func, distinct
    
    # Create test data
    contrib = Contribution(
        contribution_id="TEST_001",
        contributor_name="Test Contributor",
        contribution_amount=1000.0,
        contribution_date=datetime(2024, 1, 1)
    )
    test_db.add(contrib)
    await test_db.commit()
    
    # Execute query that returns Row objects
    query = select(
        distinct(Contribution.contributor_name),
        func.sum(Contribution.contribution_amount).label('total_amount'),
        func.count(Contribution.id).label('contribution_count')
    ).where(
        Contribution.contributor_name == "Test Contributor"
    ).group_by(
        Contribution.contributor_name
    )
    
    result = await test_db.execute(query)
    rows = result.fetchall()
    
    # Process results
    service = DonorSearchService(session=test_db)
    contributors = service._process_query_results(rows)
    
    assert len(contributors) == 1
    assert contributors[0]["name"] == "Test Contributor"
    assert contributors[0]["total_amount"] == 1000.0
    assert contributors[0]["contribution_count"] == 1


@pytest.mark.asyncio
async def test_process_query_results_tuple(test_db: AsyncSession):
    """Test result processing with tuple results"""
    # Mock tuple results
    rows = [
        ("Contributor 1", 1000.0, 5),
        ("Contributor 2", 2000.0, 10),
    ]
    
    service = DonorSearchService(session=test_db)
    contributors = service._process_query_results(rows)
    
    assert len(contributors) == 2
    assert contributors[0]["name"] == "Contributor 1"
    assert contributors[0]["total_amount"] == 1000.0
    assert contributors[0]["contribution_count"] == 5


@pytest.mark.asyncio
async def test_process_query_results_dict(test_db: AsyncSession):
    """Test result processing with dict results"""
    # Mock dict results
    rows = [
        {"contributor_name": "Contributor 1", "total_amount": 1000.0, "contribution_count": 5},
        {"contributor_name": "Contributor 2", "total_amount": 2000.0, "contribution_count": 10},
    ]
    
    service = DonorSearchService(session=test_db)
    contributors = service._process_query_results(rows)
    
    assert len(contributors) == 2
    assert contributors[0]["name"] == "Contributor 1"
    assert contributors[0]["total_amount"] == 1000.0


@pytest.mark.asyncio
async def test_is_fts5_available_no_table(test_db: AsyncSession):
    """Test FTS5 availability check when table doesn't exist"""
    service = DonorSearchService(session=test_db)
    available = await service._is_fts5_available(test_db)
    
    # FTS5 table shouldn't exist in test database
    assert available is False


@pytest.mark.asyncio
async def test_search_with_prefix(test_db: AsyncSession):
    """Test prefix search strategy"""
    # Create test data
    contrib1 = Contribution(
        contribution_id="TEST_001",
        contributor_name="Smith, John",
        contribution_amount=1000.0,
        contribution_date=datetime(2024, 1, 1)
    )
    contrib2 = Contribution(
        contribution_id="TEST_002",
        contributor_name="Smith, Jane",
        contribution_amount=2000.0,
        contribution_date=datetime(2024, 1, 2)
    )
    
    test_db.add_all([contrib1, contrib2])
    await test_db.commit()
    
    service = DonorSearchService(session=test_db)
    results = await service._search_with_prefix(test_db, "Smith", limit=10)
    
    assert len(results) == 2
    assert all("Smith" in r["name"] for r in results)


@pytest.mark.asyncio
async def test_search_with_substring(test_db: AsyncSession):
    """Test substring search strategy"""
    # Create test data
    contrib1 = Contribution(
        contribution_id="TEST_001",
        contributor_name="John Smith",
        contribution_amount=1000.0,
        contribution_date=datetime(2024, 1, 1)
    )
    contrib2 = Contribution(
        contribution_id="TEST_002",
        contributor_name="Jane Smith",
        contribution_amount=2000.0,
        contribution_date=datetime(2024, 1, 2)
    )
    
    test_db.add_all([contrib1, contrib2])
    await test_db.commit()
    
    service = DonorSearchService(session=test_db)
    results = await service._search_with_substring(test_db, "Smith", limit=10)
    
    assert len(results) == 2
    assert all("Smith" in r["name"] for r in results)


@pytest.mark.asyncio
async def test_aggregation_correct(test_db: AsyncSession):
    """Test that aggregation (sum, count) works correctly"""
    # Create multiple contributions for same contributor
    contrib1 = Contribution(
        contribution_id="TEST_001",
        contributor_name="Test Contributor",
        contribution_amount=1000.0,
        contribution_date=datetime(2024, 1, 1)
    )
    contrib2 = Contribution(
        contribution_id="TEST_002",
        contributor_name="Test Contributor",
        contribution_amount=2000.0,
        contribution_date=datetime(2024, 1, 2)
    )
    contrib3 = Contribution(
        contribution_id="TEST_003",
        contributor_name="Test Contributor",
        contribution_amount=500.0,
        contribution_date=datetime(2024, 1, 3)
    )
    
    test_db.add_all([contrib1, contrib2, contrib3])
    await test_db.commit()
    
    service = DonorSearchService(session=test_db)
    results = await service.search_unique_contributors("Test Contributor", limit=10)
    
    assert len(results) == 1
    assert results[0]["name"] == "Test Contributor"
    assert results[0]["total_amount"] == 3500.0  # Sum of all contributions
    assert results[0]["contribution_count"] == 3  # Count of contributions


@pytest.mark.asyncio
async def test_limit_respected(test_db: AsyncSession):
    """Test that limit parameter is respected"""
    # Create many contributors
    contributors = []
    for i in range(20):
        contrib = Contribution(
            contribution_id=f"TEST_{i:03d}",
            contributor_name=f"Contributor {i}",
            contribution_amount=1000.0,
            contribution_date=datetime(2024, 1, 1)
        )
        contributors.append(contrib)
    
    test_db.add_all(contributors)
    await test_db.commit()
    
    service = DonorSearchService(session=test_db)
    results = await service.search_unique_contributors("Contributor", limit=5)
    
    assert len(results) <= 5


@pytest.mark.asyncio
async def test_service_without_session():
    """Test that service can work without pre-provided session"""
    service = DonorSearchService()
    # Should not raise error, will create session when needed
    assert service.session is None

