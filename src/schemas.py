from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum

class EventType(str, Enum):
    STOCK_PRICE_UPDATED = "stock_price_updated"
    ANALYTICS_UPDATED = "analytics_updated"
    ALERT_TRIGGERED = "alert_triggered"
    DATA_QUALITY_ISSUE = "data_quality_issue"

class StockPriceEvent(BaseModel):
    event_type: EventType = EventType.STOCK_PRICE_UPDATED
    event_version: str = "1.0"
    symbol: str = Field(..., min_length=1, max_length=10)
    price: float = Field(..., gt=0)
    volume: Optional[int] = Field(None, ge=0)
    timestamp: datetime
    source: str = Field(default="simulation", max_length=50)
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)

    @validator('symbol')
    def validate_symbol(cls, v):
        return v.upper()

    @validator('price')
    def validate_price(cls, v):
        if v <= 0:
            raise ValueError('Price must be positive')
        return round(v, 2)

class AnalyticsEvent(BaseModel):
    event_type: EventType = EventType.ANALYTICS_UPDATED
    event_version: str = "1.0"
    symbol: str = Field(..., min_length=1, max_length=10)
    analytics_type: str = Field(..., max_length=50)
    value: float
    window_size: Optional[int] = Field(None, gt=0)
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)

    @validator('symbol')
    def validate_symbol(cls, v):
        return v.upper()

class AlertEvent(BaseModel):
    event_type: EventType = EventType.ALERT_TRIGGERED
    event_version: str = "1.0"
    symbol: str = Field(..., min_length=1, max_length=10)
    alert_type: str = Field(..., max_length=50)
    message: str = Field(..., max_length=500)
    severity: str = Field(..., regex="^(LOW|MEDIUM|HIGH|CRITICAL)$")
    threshold: Optional[float] = None
    current_value: Optional[float] = None
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)

    @validator('symbol')
    def validate_symbol(cls, v):
        return v.upper()

class DataQualityEvent(BaseModel):
    event_type: EventType = EventType.DATA_QUALITY_ISSUE
    event_version: str = "1.0"
    symbol: Optional[str] = Field(None, min_length=1, max_length=10)
    issue_type: str = Field(..., max_length=50)
    description: str = Field(..., max_length=500)
    severity: str = Field(..., regex="^(LOW|MEDIUM|HIGH|CRITICAL)$")
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)

    @validator('symbol')
    def validate_symbol(cls, v):
        if v:
            return v.upper()
        return v

# Union type for all events
Event = StockPriceEvent | AnalyticsEvent | AlertEvent | DataQualityEvent 