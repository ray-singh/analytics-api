"""
Schema definitions for OHLCV (Open, High, Low, Close, Volume) events.
"""

from pydantic import Field, validator
from typing import Optional
from .base import BaseEvent


class OHLCVEvent(BaseEvent):
    """Base schema for all OHLCV bar events."""
    symbol: str
    interval_minutes: int
    interval_id: str  # Unique identifier for this interval (e.g., "2024-08-15T10:00:00Z")
    open: float
    high: float
    low: float
    close: float
    volume: int = 0
    open_timestamp: int  
    close_timestamp: int  
    num_trades: Optional[int] = None
    source: str = "aggregation-service"

    @validator('high')
    def high_greater_than_low(cls, v, values):
        """Validate that high is greater than or equal to low."""
        if 'low' in values and v < values['low']:
            raise ValueError('High price must be greater than or equal to low price')
        return v
    
    @validator('open', 'high', 'low', 'close')
    def validate_prices(cls, v):
        """Ensure prices are positive."""
        if v <= 0:
            raise ValueError('Price must be positive')
        return v


class IntradayBarEvent(OHLCVEvent):
    """Intraday OHLCV bar (minutes to hours)."""
    event_type: str = "ohlcv.intraday"
    version: str = "1.0"
    samples: int = 1  # Number of price samples that formed this bar


class DailyBarEvent(OHLCVEvent):
    """Daily OHLCV bar."""
    event_type: str = "ohlcv.daily"
    version: str = "1.0"
    interval_minutes: int = 1440  # 24 hours in minutes
    adjusted_close: Optional[float] = None  # For dividend/split adjusted data
    
    
class HistoricalBarsEvent(BaseEvent):
    """A collection of historical bars, typically used for backfill responses."""
    event_type: str = "ohlcv.historical"
    version: str = "1.0"
    symbol: str
    interval_minutes: int
    source: str
    bars: list[dict]  # List of bar data
    request_id: Optional[str] = None  # To match with a backfill request