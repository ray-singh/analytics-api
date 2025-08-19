"""
Schema definitions for analytics events.
"""

from pydantic import Field
from typing import Dict, Any, Optional, List
from .base import BaseEvent


class AnalyticsEvent(BaseEvent):
    """Base schema for all analytics events."""
    symbol: str
    interval_minutes: int  # Time frame of the analysis
    reference_price: float  # The price this analysis is based on
    reference_timestamp: int  # When the reference price was recorded


class TechnicalIndicatorEvent(AnalyticsEvent):
    """Technical analysis indicators calculated from price data."""
    event_type: str = "analytics.technical"
    version: str = "1.0"
    indicators: Dict[str, Any]  # Flexible structure for various indicators


class SignalEvent(AnalyticsEvent):
    """Trading signal derived from analytics."""
    event_type: str = "analytics.signal"
    version: str = "1.0"
    signal_type: str  # e.g., "buy", "sell", "strong_buy"
    confidence: float  # 0.0 to 1.0
    reasoning: List[str]  # List of reasons for this signal
    indicators_used: List[str]  # List of indicators that generated this signal


class AlertEvent(AnalyticsEvent):
    """Alert triggered by price or indicator conditions."""
    event_type: str = "analytics.alert"
    version: str = "1.0"
    alert_id: str
    alert_type: str  # e.g., "price_above", "rsi_oversold"
    description: str
    severity: str  # e.g., "info", "warning", "critical"
    condition_value: float  # The value that triggered the alert
    threshold_value: float  # The threshold that was crossed