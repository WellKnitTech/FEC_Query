"""
Simplified Donor Search Service

Provides reliable, simple donor search functionality.
"""
import asyncio
import logging
from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from app.db.database import Contribution, AsyncSessionLocal
from app.services.shared.exceptions import DonorSearchError, QueryTimeoutError

logger = logging.getLogger(__name__)


class DonorSearchService:
    """Simple service for searching unique contributors/donors"""
    
    async def search_unique_contributors(
        self,
        search_term: str,
        limit: int = 100,
        timeout: float = 10.0
    ) -> List[Dict[str, Any]]:
        """
        Search for unique contributors matching the search term.
        
        Uses a simple ilike query with proper timeout and error handling.
        
        Args:
            search_term: Search term for contributor name
            limit: Maximum number of results to return
            timeout: Maximum time for search operation (default: 10s)
            
        Returns:
            List of contributor dicts with keys: name, total_amount, contribution_count
            
        Raises:
            DonorSearchError: If search fails
            QueryTimeoutError: If query times out
        """
        search_term = search_term.strip()
        
        if not search_term or len(search_term) > 200:
            raise DonorSearchError(
                f"Search term must be between 1 and 200 characters",
                search_term=search_term
            )
        
        # Sanitize search term to prevent SQL injection
        search_term = search_term.replace(";", "").replace("--", "").replace("/*", "").replace("*/", "")
        
        logger.info(f"Searching for donors matching '{search_term}' (limit={limit})")
        
        try:
            async with AsyncSessionLocal() as session:
                # For multi-word searches, match all words (AND logic)
                # This handles full names like "Angela Smith" or "Fredericksburg Tea Party"
                search_words = search_term.split()
                
                if len(search_words) > 1:
                    # Multi-word: all words must be present (case-insensitive)
                    # This handles "Angela Smith" matching "Smith, Angela" or "Angela M. Smith"
                    from sqlalchemy import and_
                    conditions = [
                        Contribution.contributor_name.ilike(f"%{word}%")
                        for word in search_words
                    ]
                    name_condition = and_(*conditions)
                else:
                    # Single word: simple substring match
                    name_condition = Contribution.contributor_name.ilike(f"%{search_term}%")
                
                query = select(
                    Contribution.contributor_name,
                    func.sum(Contribution.contribution_amount).label('total_amount'),
                    func.count(Contribution.id).label('contribution_count')
                ).where(
                    name_condition
                ).where(
                    Contribution.contributor_name.isnot(None),
                    Contribution.contributor_name != ''
                ).group_by(
                    Contribution.contributor_name
                ).order_by(
                    func.sum(Contribution.contribution_amount).desc()
                ).limit(limit)
                
                # Execute with timeout
                result = await asyncio.wait_for(
                    session.execute(query),
                    timeout=timeout
                )
                
                rows = result.fetchall()
                logger.info(f"Search completed, found {len(rows)} unique contributors")
                
                # Process results
                contributors = []
                for row in rows:
                    try:
                        name = row.contributor_name
                        total = row.total_amount or 0
                        count = row.contribution_count or 0
                        
                        if name:
                            contributors.append({
                                "name": str(name),
                                "total_amount": float(total),
                                "contribution_count": int(count)
                            })
                    except Exception as e:
                        logger.warning(f"Error processing result row: {e}")
                        continue
                
                return contributors
                
        except asyncio.TimeoutError:
            logger.error(f"Search timed out after {timeout}s for '{search_term}'")
            raise QueryTimeoutError(
                f"Search timed out after {timeout}s",
                search_term=search_term,
                timeout=timeout
            )
        except (DonorSearchError, QueryTimeoutError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in donor search: {e}", exc_info=True)
            raise DonorSearchError(
                f"Failed to search donors: {str(e)}",
                search_term=search_term
            )
