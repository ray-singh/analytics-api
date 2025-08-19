"""
Schema definitions for error events and dead-letter queue items.
"""

from pydantic import Field
from typing import Optional, Any
from .base import BaseEvent


class ErrorEvent(BaseEvent):
    """Base schema for all error events."""
    event_type: str = "error"
    version: str = "1.0"
    service: str  # Service that generated the error
    error_type: str  # Classification of the error
    error_message: str
    severity: str = "error"  # "warning", "error", "critical"
    correlation_id: Optional[str] = None  # To trace related events


class DeadLetterEvent(BaseEvent):
    """Event that couldn't be processed, sent to dead-letter queue."""
    event_type: str = "error.dead_letter"
    version: str = "1.0"
    original_topic: str
    original_partition: int
    original_offset: int
    reason: str
    original_event: Any  # The raw event data that couldn't be processed
    processing_service: str  # Service that tried to process the event
    retry_count: int = 0