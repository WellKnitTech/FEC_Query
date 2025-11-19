"""
Utility for processing large datasets in chunks to avoid memory issues

This module provides the ChunkedProcessor class which enables efficient processing
of large database result sets by fetching and processing data in smaller chunks.
This prevents memory exhaustion when dealing with datasets containing tens of
thousands or millions of records.

Example:
    ```python
    processor = ChunkedProcessor(chunk_size=5000)
    
    async with AsyncSessionLocal() as session:
        base_query = select(Contribution).where(
            Contribution.candidate_id == candidate_id
        )
        
        def process_chunk(chunk_data):
            # Process chunk and return aggregated results
            return {'contributions': [convert_to_dict(c) for c in chunk_data]}
        
        results = await processor.process_contributions_in_chunks(
            session, base_query, process_chunk
        )
    ```
"""
import logging
from typing import List, Dict, Any, Optional, Callable, AsyncIterator
from sqlalchemy import select, func
from sqlalchemy.sql import Select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# Default chunk size for processing contributions
# This balances memory usage with query efficiency
DEFAULT_CHUNK_SIZE: int = 10000


class ChunkedProcessor:
    """
    Utility class for processing large datasets in chunks
    
    This class helps prevent memory exhaustion when processing large result sets
    by fetching data in smaller, manageable chunks and processing them incrementally.
    
    Attributes:
        chunk_size: Number of records to fetch and process per chunk
    """
    
    def __init__(self, chunk_size: int = DEFAULT_CHUNK_SIZE):
        """
        Initialize chunked processor
        
        Args:
            chunk_size: Number of records to process per chunk.
                       Defaults to DEFAULT_CHUNK_SIZE (10000).
                       Smaller values use less memory but require more queries.
        """
        self.chunk_size = chunk_size
        logger.debug(f"ChunkedProcessor initialized with chunk_size={chunk_size}")
    
    async def process_contributions_in_chunks(
        self,
        session: AsyncSession,
        base_query,
        process_chunk: Callable[[List[Any]], Dict[str, Any]],
        max_chunks: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Process contributions in chunks and aggregate results incrementally
        
        Args:
            session: Database session
            base_query: Base SQLAlchemy query (without limit/offset)
            process_chunk: Function that processes a chunk and returns aggregation results
            max_chunks: Maximum number of chunks to process (None = unlimited)
        
        Returns:
            Dictionary with aggregated results and metadata
        """
        aggregated_results = {}
        total_processed = 0
        chunk_count = 0
        offset = 0
        
        while True:
            if max_chunks and chunk_count >= max_chunks:
                logger.warning(f"Reached max_chunks limit ({max_chunks}), stopping processing")
                break
            
            # Create chunk query with limit and offset
            chunk_query = base_query.limit(self.chunk_size).offset(offset)
            result = await session.execute(chunk_query)
            chunk_data = result.scalars().all()
            
            if not chunk_data:
                break
            
            # Process this chunk
            chunk_results = process_chunk(chunk_data)
            
            # Merge results incrementally
            for key, value in chunk_results.items():
                if key == 'metadata':
                    # Handle metadata separately
                    if 'metadata' not in aggregated_results:
                        aggregated_results['metadata'] = {}
                    aggregated_results['metadata'].update(value)
                elif isinstance(value, dict):
                    # Merge dictionaries (e.g., state distributions)
                    if key not in aggregated_results:
                        aggregated_results[key] = {}
                    for sub_key, sub_value in value.items():
                        if isinstance(sub_value, (int, float)):
                            aggregated_results[key][sub_key] = aggregated_results[key].get(sub_key, 0) + sub_value
                        else:
                            aggregated_results[key][sub_key] = sub_value
                elif isinstance(value, (int, float)):
                    # Sum numeric values
                    aggregated_results[key] = aggregated_results.get(key, 0) + value
                elif isinstance(value, list):
                    # Extend lists
                    if key not in aggregated_results:
                        aggregated_results[key] = []
                    aggregated_results[key].extend(value)
                else:
                    # Replace other types
                    aggregated_results[key] = value
            
            total_processed += len(chunk_data)
            chunk_count += 1
            offset += self.chunk_size
            
            # If we got fewer records than chunk_size, we've reached the end
            if len(chunk_data) < self.chunk_size:
                break
            
            logger.debug(f"Processed chunk {chunk_count}: {len(chunk_data)} records (total: {total_processed})")
        
        # Add processing metadata
        if 'metadata' not in aggregated_results:
            aggregated_results['metadata'] = {}
        aggregated_results['metadata'].update({
            'total_processed': total_processed,
            'chunks_processed': chunk_count,
            'is_complete': len(chunk_data) < self.chunk_size if chunk_count > 0 else True
        })
        
        logger.info(f"Chunked processing complete: {total_processed} records in {chunk_count} chunks")
        return aggregated_results
    
    async def stream_contributions(
        self,
        session: AsyncSession,
        base_query: Select,
        max_records: Optional[int] = None
    ) -> AsyncIterator[Any]:
        """
        Stream contributions one at a time for memory-efficient processing
        
        Args:
            session: Database session
            base_query: Base SQLAlchemy query
            max_records: Maximum number of records to stream (None = unlimited)
        
        Yields:
            Individual contribution records
        """
        offset = 0
        total_yielded = 0
        
        while True:
            if max_records and total_yielded >= max_records:
                break
            
            chunk_query = base_query.limit(self.chunk_size).offset(offset)
            result = await session.execute(chunk_query)
            chunk_data = result.scalars().all()
            
            if not chunk_data:
                break
            
            for record in chunk_data:
                yield record
                total_yielded += 1
                if max_records and total_yielded >= max_records:
                    break
            
            offset += self.chunk_size
            
            # If we got fewer records than chunk_size, we've reached the end
            if len(chunk_data) < self.chunk_size:
                break

