"""
Structured logging utilities for consistent log formatting across the application
"""
import logging
import json
from typing import Any, Dict, Optional
from datetime import datetime


class StructuredFormatter(logging.Formatter):
    """
    Custom formatter that outputs structured JSON logs for better parsing and analysis
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as structured JSON
        
        Args:
            record: LogRecord to format
            
        Returns:
            JSON string with structured log data
        """
        # Extract structured data from extra fields
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add any extra fields from the log record
        if hasattr(record, "request_id"):
            log_data["request_id"] = record.request_id
        if hasattr(record, "error_code"):
            log_data["error_code"] = record.error_code
        if hasattr(record, "status_code"):
            log_data["status_code"] = record.status_code
        if hasattr(record, "is_transient"):
            log_data["is_transient"] = record.is_transient
        if hasattr(record, "path"):
            log_data["path"] = record.path
        if hasattr(record, "method"):
            log_data["method"] = record.method
        if hasattr(record, "details"):
            log_data["details"] = record.details
        if hasattr(record, "error_type"):
            log_data["error_type"] = record.error_type
        
        # Add any other extra fields
        for key, value in record.__dict__.items():
            if key not in [
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "message", "pathname", "process", "processName", "relativeCreated",
                "thread", "threadName", "exc_info", "exc_text", "stack_info"
            ] and not key.startswith("_"):
                if key not in log_data:
                    log_data[key] = value
        
        # Format as JSON
        try:
            return json.dumps(log_data, default=str)
        except (TypeError, ValueError):
            # Fallback to string representation if JSON serialization fails
            return json.dumps({
                "timestamp": log_data["timestamp"],
                "level": log_data["level"],
                "logger": log_data["logger"],
                "message": str(log_data["message"]),
                "error": "Failed to serialize log data"
            })


def setup_structured_logging(
    level: str = "INFO",
    use_json: bool = False,
    include_console: bool = True
) -> None:
    """
    Set up structured logging for the application
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        use_json: If True, use JSON formatting. If False, use human-readable format
        include_console: If True, output to console
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler
    if include_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
        
        if use_json:
            # Use structured JSON formatter
            formatter = StructuredFormatter()
        else:
            # Use human-readable formatter with structured fields
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - '
                '[%(filename)s:%(lineno)d] - %(message)s'
                '%(request_id)s%(error_code)s%(status_code)s'
            )
        
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # Set specific loggers to appropriate levels
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)


def log_with_context(
    logger: logging.Logger,
    level: int,
    message: str,
    **context: Any
) -> None:
    """
    Log a message with additional context fields
    
    Args:
        logger: Logger instance
        level: Logging level (logging.INFO, logging.ERROR, etc.)
        message: Log message
        **context: Additional context fields to include in log
    """
    logger.log(level, message, extra=context)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with structured logging support
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance configured for structured logging
    """
    return logging.getLogger(name)

