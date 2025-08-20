import logging
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger("aggregation.bar_aggregator")

class BarAggregator:
    """
    Aggregates real-time price updates into OHLCV bars for different time intervals.
    Supports multiple symbols and time frames simultaneously.
    """
    
    def __init__(self, intervals=None):
        """
        Initialize the bar aggregator with the specified intervals.
        
        Args:
            intervals: List of intervals in minutes (default: [1, 5, 15, 30, 60, 240, 1440])
        """
        # Default intervals if none provided
        self.intervals = intervals or [1, 5, 15, 30, 60, 240, 1440]
        
        # Data structure to store in-progress bars
        # Format: {symbol: {interval: {start_time: bar_data}}}
        self.current_bars = defaultdict(lambda: defaultdict(dict))
        
    def get_window_start(self, timestamp, interval):
        """
        Calculate the start time of a bar window based on timestamp and interval.
        
        Args:
            timestamp: ISO format timestamp string or Unix timestamp in milliseconds/seconds
            interval: Bar interval in minutes
            
        Returns:
            Unix timestamp of the bar's start time in seconds
        """
        # Convert timestamp to seconds if it's a string or in milliseconds
        if isinstance(timestamp, str):
            # Parse ISO format timestamp
            try:
                dt = datetime.fromisoformat(timestamp)
                timestamp_seconds = int(dt.timestamp())
            except ValueError:
                logger.error(f"Invalid timestamp format: {timestamp}")
                # Use current time as fallback
                timestamp_seconds = int(datetime.now().timestamp())
        elif timestamp > 1000000000000:  # If timestamp is in milliseconds
            timestamp_seconds = timestamp // 1000
        else:
            timestamp_seconds = int(timestamp)
        
        # Calculate interval in seconds
        interval_seconds = interval * 60
        
        # Calculate the start of the window
        return timestamp_seconds - (timestamp_seconds % interval_seconds)
        
    def update(self, symbol, timestamp, price, volume=0):
        """
        Update price data and check for completed bars.
        
        Args:
            symbol: The ticker symbol
            timestamp: ISO format timestamp string or Unix timestamp
            price: Current price
            volume: Trading volume (optional)
            
        Returns:
            Dictionary of completed bars: {interval: bar_data, ...}
        """
        price = float(price)  # Ensure price is a float
        volume = int(volume) if volume else 0  # Ensure volume is an integer
        
        completed_bars = {}
        
        # Update bars for each interval
        for interval in self.intervals:
            window_start = self.get_window_start(timestamp, interval)
            
            # Get the current bar for this symbol and interval
            if window_start not in self.current_bars[symbol][interval]:
                # Initialize a new bar
                self.current_bars[symbol][interval][window_start] = {
                    "symbol": symbol,
                    "interval": interval,
                    "timestamp": window_start,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": volume
                }
            else:
                # Update existing bar
                bar = self.current_bars[symbol][interval][window_start]
                bar["high"] = max(bar["high"], price)
                bar["low"] = min(bar["low"], price)
                bar["close"] = price
                bar["volume"] += volume
            
            # Check for completed bars (when a new window has started)
            current_window = window_start
            
            # Remove and return completed bars (bars with start time < current window)
            for start_time in list(self.current_bars[symbol][interval].keys()):
                if start_time < current_window - (interval * 60):
                    completed_bar = self.current_bars[symbol][interval].pop(start_time)
                    # Only include if not already in the result
                    if interval not in completed_bars:
                        completed_bars[interval] = completed_bar
        
        return completed_bars