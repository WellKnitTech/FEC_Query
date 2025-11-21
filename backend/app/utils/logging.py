"""Compatibility logging helpers.

This module exposes ``get_logger`` from the structured logging utilities so
imports of ``app.utils.logging`` continue to work after the structured logging
refactor. Keeping this shim prevents import errors in the API routes and
startup logic.
"""

from app.utils.structured_logging import get_logger

__all__ = ["get_logger"]
