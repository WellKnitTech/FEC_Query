"""
Thread pool utility for offloading CPU-bound operations from the event loop.

This module provides async wrappers for CPU-intensive pandas operations,
preventing them from blocking the async event loop.
"""
import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any, Optional
import functools

logger = logging.getLogger(__name__)

# Global thread pool executor
_thread_pool: Optional[ThreadPoolExecutor] = None


def get_thread_pool() -> ThreadPoolExecutor:
    """Get or create the global thread pool executor"""
    global _thread_pool
    if _thread_pool is None:
        max_workers = int(os.getenv("THREAD_POOL_WORKERS", "4"))
        _thread_pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="pandas-worker")
        logger.info(f"Initialized thread pool with {max_workers} workers")
    return _thread_pool


def shutdown_thread_pool():
    """Shutdown the global thread pool executor"""
    global _thread_pool
    if _thread_pool is not None:
        _thread_pool.shutdown(wait=True)
        _thread_pool = None
        logger.info("Thread pool shutdown complete")


async def run_in_thread_pool(func: Callable, *args, **kwargs) -> Any:
    """
    Run a synchronous function in the thread pool executor.
    
    Args:
        func: The synchronous function to execute
        *args: Positional arguments for the function
        **kwargs: Keyword arguments for the function
    
    Returns:
        The result of the function execution
    """
    executor = get_thread_pool()
    loop = asyncio.get_event_loop()
    
    # Use asyncio.to_thread() if available (Python 3.9+), otherwise use run_in_executor
    try:
        # Python 3.9+ has asyncio.to_thread()
        if hasattr(asyncio, 'to_thread'):
            return await asyncio.to_thread(func, *args, **kwargs)
        else:
            # Fallback for Python < 3.9
            return await loop.run_in_executor(executor, functools.partial(func, *args, **kwargs))
    except Exception as e:
        logger.error(f"Error executing function in thread pool: {e}", exc_info=True)
        raise


async def async_read_csv(*args, **kwargs):
    """
    Async wrapper for pd.read_csv() that runs in a thread pool.
    
    This prevents blocking the event loop when reading large CSV files.
    For chunked reads (chunksize specified), returns an async iterator.
    
    Args:
        *args: Positional arguments passed to pd.read_csv()
        **kwargs: Keyword arguments passed to pd.read_csv()
    
    Returns:
        DataFrame or async iterator of DataFrames (if chunksize is specified)
    """
    import pandas as pd
    
    # Check if chunksize is specified
    chunksize = kwargs.get('chunksize') or (args[1] if len(args) > 1 and isinstance(args[1], int) else None)
    
    if chunksize:
        # For chunked reads, create the iterator in thread pool
        # The iterator itself is created synchronously, but we'll read chunks async
        reader = await run_in_thread_pool(pd.read_csv, *args, **kwargs)
        
        # Return an async generator that reads chunks
        async def chunk_generator():
            try:
                for chunk in reader:
                    yield chunk
            finally:
                if hasattr(reader, 'close'):
                    reader.close()
        
        return chunk_generator()
    else:
        # For non-chunked reads, just return the DataFrame
        return await run_in_thread_pool(pd.read_csv, *args, **kwargs)


async def async_to_numeric(*args, **kwargs):
    """
    Async wrapper for pd.to_numeric() that runs in a thread pool.
    
    Args:
        *args: Positional arguments passed to pd.to_numeric()
        **kwargs: Keyword arguments passed to pd.to_numeric()
    
    Returns:
        Series or ndarray with numeric values
    """
    import pandas as pd
    return await run_in_thread_pool(pd.to_numeric, *args, **kwargs)


async def async_dataframe_operation(df, operation: Callable, *args, **kwargs):
    """
    Execute a DataFrame operation in a thread pool.
    
    Args:
        df: The DataFrame to operate on
        operation: A callable that takes df as first argument (e.g., lambda df: df.groupby(...))
        *args: Additional positional arguments for the operation
        **kwargs: Additional keyword arguments for the operation
    
    Returns:
        The result of the operation
    """
    def _execute():
        return operation(df, *args, **kwargs)
    
    return await run_in_thread_pool(_execute)


async def async_aggregation(df, groupby_cols, agg_dict=None, agg_func=None):
    """
    Async wrapper for DataFrame groupby aggregation operations.
    
    Args:
        df: The DataFrame to aggregate
        groupby_cols: Column(s) to group by (str, list, or Series)
        agg_dict: Dictionary mapping columns to aggregation functions (for .agg())
        agg_func: Single aggregation function (for .sum(), .mean(), etc.)
    
    Returns:
        Aggregated DataFrame
    """
    def _aggregate():
        grouped = df.groupby(groupby_cols)
        if agg_dict is not None:
            return grouped.agg(agg_dict)
        elif agg_func is not None:
            return agg_func(grouped)
        else:
            return grouped.sum()
    
    return await run_in_thread_pool(_aggregate)


async def async_series_operation(series, operation: Callable, *args, **kwargs):
    """
    Execute a Series operation in a thread pool.
    
    Args:
        series: The Series to operate on
        operation: A callable that takes series as first argument
        *args: Additional positional arguments for the operation
        **kwargs: Additional keyword arguments for the operation
    
    Returns:
        The result of the operation
    """
    def _execute():
        return operation(series, *args, **kwargs)
    
    return await run_in_thread_pool(_execute)

