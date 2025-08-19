"""
Base schemas for all event types.
"""

from pydantic import BaseModel, Field
import time
from typing import Optional, Dict, Any


class BaseEvent(BaseModel):
    """Base class for all events in the system."""
    event_type: str
    version: str
    timestamp: int = Field(default_factory=lambda: int(time.time()))
    
    class Config:
        frozen = True  
        extra = "forbid"  