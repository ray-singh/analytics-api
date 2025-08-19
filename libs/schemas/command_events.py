"""
Schema definitions for command events that trigger actions.
"""

from pydantic import Field
from typing import Optional, List
from enum import Enum
from .base import BaseEvent
import uuid


class BackfillPriority(str, Enum):
    """Priority levels for backfill operations."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"


class BackfillStatus(str, Enum):
    """Status values for backfill operations."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


class BackfillRequestEvent(BaseEvent):
    """Request to backfill historical data."""
    event_type: str = "command.backfill.request"
    version: str = "1.0"
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    interval: str  # e.g., "1min", "5min", "15min", "1day"
    start_date: Optional[str] = None  # ISO format or null for max available
    end_date: Optional[str] = None  # ISO format or null for current date
    priority: BackfillPriority = BackfillPriority.NORMAL
    requester: Optional[str] = None  # Service or user that requested the backfill


class BackfillStatusEvent(BaseEvent):
    """Status update for a backfill operation."""
    event_type: str = "command.backfill.status"
    version: str = "1.0"
    request_id: str  # References the original request ID
    symbol: str
    interval: str
    status: BackfillStatus
    completion_percentage: float = 0.0
    message: Optional[str] = None  # Additional information about the status
    error: Optional[str] = None  # Error message if failed
    records_processed: Optional[int] = None