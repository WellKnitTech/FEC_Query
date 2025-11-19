"""
Application lifecycle management
Handles startup, shutdown, and background tasks
"""

from .startup import setup_startup_tasks
from .shutdown import setup_shutdown_handlers
from .tasks import start_background_tasks

__all__ = ["setup_startup_tasks", "setup_shutdown_handlers", "start_background_tasks"]

