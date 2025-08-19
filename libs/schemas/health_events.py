"""
Schema definitions for service health and metrics events.
"""

from pydantic import Field
from typing import Dict, Any, Optional, List
from .base import BaseEvent


class HeartbeatEvent(BaseEvent):
    """Periodic heartbeat from services to indicate they're alive."""
    event_type: str = "health.heartbeat"
    version: str = "1.0"
    service_id: str
    service_type: str
    status: str = "healthy"  # "healthy", "degraded", "unhealthy"
    uptime_seconds: int
    instance_id: str  # Unique ID for this service instance


class MetricsEvent(BaseEvent):
    """Periodic metrics report from services."""
    event_type: str = "health.metrics"
    version: str = "1.0"
    service_id: str
    service_type: str
    metrics: Dict[str, Any]  # Flexible structure for various metrics
    reporting_period_seconds: int = 60  # How many seconds of data this represents
    instance_id: str  # Unique ID for this service instance