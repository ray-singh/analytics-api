"""
Schema definitions for price events.
"""

from pydantic import BaseModel, Field, validator
from typing import Optional
from .base import BaseEvent

class PriceEvent(BaseEvent):
    """Base schema for all price-related events."""
    symbol: str
    source: str

class RawPriceEvent(PriceEvent):
    """Raw price tick from a market data source."""
    event_type: str = "price.raw"
    version: str = "1.0"
    price: float
    volume: Optional[int] = None
    trade_timestamp: Optional[int] = None  # Actual trade time if different from event time
    
    @validator('symbol')
    def validate_symbol(cls, v):
        """Ensure symbols are uppercase."""
        return v.upper() if v else v
    
    @validator('price')
    def validate_price(cls, v):
        """Ensure price is positive."""
        if v <= 0:
            raise ValueError('Price must be positive')
        return v

class LastPriceEvent(PriceEvent):
    """Last known price for a symbol, typically sent on request."""
    event_type: str = "price.last"
    version: str = "1.0"
    price: float
    timestamp: int  # When this price was recorded
    volume: Optional[int] = None