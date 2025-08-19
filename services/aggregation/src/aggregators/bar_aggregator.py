import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple

logger = logging.getLogger("aggregation-service.bar-aggregator")

class BarAggregator:
    """
    Aggregates real-time price ticks into OHLCV bars for multiple intervals.
    
    Maintains in-memory state of active bars and produces completed bars
    when their time windows are complete.
    """
    def __init__(self):
        # Supported intervals in minutes
        self.intervals = [1, 5, 15, 60]
        
        # Active bars storage: {symbol: {interval: {window_start: bar_data}}}
        self.active_bars = {}
        
    def get_window_start(self, timestamp: int, interval_minutes: int) -> int:
        """
        Calculates the start timestamp of the window that a given timestamp belongs to.
        
        Args:
            timestamp: Unix timestamp in seconds
            interval_minutes: Bar interval in minutes
            
        Returns:
            Unix timestamp of the start of the window
        """
        interval_seconds = interval_minutes * 60
        return timestamp - (timestamp % interval_seconds)
    
    def update(self, symbol: str, timestamp: int, price: float, volume: int = 0) -> Dict[int, Dict]:
        """
        Updates bars with a new price tick and returns any completed bars.
        
        Args:
            symbol: The ticker symbol
            timestamp: Unix timestamp in seconds
            price: The current price
            volume: Trading volume (optional)
            
        Returns:
            Dictionary of completed bars by interval {interval: bar_data}
        """
        completed_bars = {}
        
        # Initialize symbol dict if it doesn't exist
        if symbol not in self.active_bars:
            self.active_bars[symbol] = {}
        
        # Current time for detecting completed bars
        now = int(datetime.now().timestamp())
        
        # Update each interval
        for interval in self.intervals:
            if interval not in self.active_bars[symbol]:
                self.active_bars[symbol][interval] = {}
            
            # Calculate window for this interval
            window_start = self.get_window_start(timestamp, interval)
            window_end = window_start + (interval * 60) - 1
            
            # Get or create bar for this window
            if window_start not in self.active_bars[symbol][interval]:
                self.active_bars[symbol][interval][window_start] = {
                    "event_type": "price.ohlcv",
                    "version": "1.0",
                    "symbol": symbol,
                    "interval_minutes": interval,
                    "bar_start_ts": window_start,
                    "bar_end_ts": window_end,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": volume,
                    "num_samples": 1,
                    "source": "aggregation-service"
                }
            else:
                bar = self.active_bars[symbol][interval][window_start]
                bar["high"] = max(bar["high"], price)
                bar["low"] = min(bar["low"], price)
                bar["close"] = price
                bar["volume"] += volume
                bar["num_samples"] += 1
            
            # Check if there are any completed bars
            # A bar is completed if:
            # 1. The current time is past the end of the window
            # 2. The bar's window is not the current active window
            for bar_ts in list(self.active_bars[symbol][interval].keys()):
                # Skip current window
                if bar_ts == window_start:
                    continue
                    
                bar = self.active_bars[symbol][interval][bar_ts]
                
                # If time has passed the window's end, the bar is complete
                if now > bar["bar_end_ts"]:
                    # Add to completed bars
                    if interval not in completed_bars:
                        completed_bars[interval] = []
                    completed_bars[interval] = bar
                    
                    # Remove from active bars
                    del self.active_bars[symbol][interval][bar_ts]
                    logger.debug(f"Completed {interval}min bar for {symbol} at {bar_ts}")
        
        return completed_bars
    
    def get_active_bars(self, symbol: str = None):
        """
        Returns the currently active (incomplete) bars.
        Useful for debugging or status reporting.
        
        Args:
            symbol: Optional symbol filter
            
        Returns:
            Dict of active bars
        """
        if symbol:
            return self.active_bars.get(symbol, {})
        return self.active_bars