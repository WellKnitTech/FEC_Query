"""
Prometheus metrics for the FEC Campaign Finance Analysis API

This module provides Prometheus metrics for monitoring application performance
and usage. Metrics are only exposed when METRICS_ENABLED is set to true.
"""
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from app.config import config


# Define metrics
request_count = Counter(
    'fec_api_requests_total',
    'Total number of API requests',
    ['method', 'endpoint', 'status']
)

error_count = Counter(
    'fec_api_errors_total',
    'Total number of API errors',
    ['method', 'endpoint', 'error_type']
)

fec_api_call_count = Counter(
    'fec_external_api_calls_total',
    'Total number of calls to FEC external API',
    ['endpoint']
)

bulk_import_counter = Counter(
    'fec_bulk_imports_total',
    'Total number of bulk data imports',
    ['data_type', 'cycle', 'status']
)


def get_metrics():
    """
    Get Prometheus metrics in text format
    
    Returns:
        bytes: Prometheus metrics in text format
    """
    return generate_latest()


def get_metrics_content_type():
    """
    Get the content type for Prometheus metrics
    
    Returns:
        str: Content type for Prometheus metrics
    """
    return CONTENT_TYPE_LATEST


def record_request(method: str, endpoint: str, status_code: int):
    """
    Record an API request
    
    Args:
        method: HTTP method (GET, POST, etc.)
        endpoint: API endpoint path
        status_code: HTTP status code
    """
    if config.METRICS_ENABLED:
        request_count.labels(method=method, endpoint=endpoint, status=str(status_code)).inc()


def record_error(method: str, endpoint: str, error_type: str):
    """
    Record an API error
    
    Args:
        method: HTTP method
        endpoint: API endpoint path
        error_type: Type of error (e.g., 'ValidationError', 'NotFoundError')
    """
    if config.METRICS_ENABLED:
        error_count.labels(method=method, endpoint=endpoint, error_type=error_type).inc()


def record_fec_api_call(endpoint: str):
    """
    Record a call to the external FEC API
    
    Args:
        endpoint: FEC API endpoint that was called
    """
    if config.METRICS_ENABLED:
        fec_api_call_count.labels(endpoint=endpoint).inc()


def record_bulk_import(data_type: str, cycle: int, status: str):
    """
    Record a bulk data import
    
    Args:
        data_type: Type of data imported (e.g., 'individual_contributions')
        cycle: Election cycle
        status: Import status ('success', 'failed', 'partial')
    """
    if config.METRICS_ENABLED:
        bulk_import_counter.labels(data_type=data_type, cycle=str(cycle), status=status).inc()

