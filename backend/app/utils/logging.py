"""
Unified logging utility for the application

This module provides a simple interface for getting loggers that respect
the LOG_JSON environment variable for structured logging.

Example:
    ```python
    from app.utils.logging import get_logger
    
    logger = get_logger(__name__)
    logger.info("Application started")
    ```
"""
import os
import logging
from app.utils.structured_logging import setup_structured_logging


# Track if logging has been initialized
_logging_initialized = False


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with proper formatting based on LOG_JSON environment variable.
    
    This function ensures that logging is initialized with the correct format
    (JSON or human-readable) based on the LOG_JSON environment variable.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance configured for structured logging
        
    Example:
        ```python
        from app.utils.logging import get_logger
        
        logger = get_logger(__name__)
        logger.info("Application started")
        ```
    """
    global _logging_initialized
    
    # Initialize logging if not already done
    if not _logging_initialized:
        use_json_logging = os.getenv("LOG_JSON", "false").lower() == "true"
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        
        setup_structured_logging(
            level=log_level,
            use_json=use_json_logging,
            include_console=True,
            log_dir=os.getenv("LOG_DIR", "./logs"),
            log_to_file=os.getenv("LOG_TO_FILE", "true").lower() in ("true", "1", "yes"),
            max_bytes=int(os.getenv("LOG_FILE_MAX_BYTES", "10485760")),
            backup_count=int(os.getenv("LOG_FILE_BACKUP_COUNT", "5"))
        )
        _logging_initialized = True
    
    # Return logger (structured_logging already uses standard logging.getLogger)
    return logging.getLogger(name)

