"""
Bulk Data Service package - maintains backward compatibility
"""
# Import from the original bulk_data.py file (now bulk_data_original.py)
from app.services.bulk_data_original import (
    BulkDataService,
    _running_tasks,
    _cancelled_jobs
)

__all__ = ["BulkDataService", "_running_tasks", "_cancelled_jobs"]

