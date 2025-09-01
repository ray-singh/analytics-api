import asyncio
import logging
import os
import json
import aiohttp
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger('market_data_service.twelvedata')

class Interval(Enum):
    """Supported intervals for TwelveData API"""
    MIN_1 = "1min"
    MIN_5 = "5min"
    MIN_15 = "15min"
    MIN_30 = "30min"
    HOUR_1 = "1h"
    HOUR_4 = "4h"
    DAY_1 = "1day"
    WEEK_1 = "1week"
    MONTH_1 = "1month"

@dataclass
class RateLimitConfig:
    """Configuration for rate limiting"""
    requests_per_minute: int = 8
    requests_per_day: int = 800  
    burst_delay: float = 0.5  

class TwelveDataError(Exception):
    """Custom exception for TwelveData API errors"""
    def __init__(self, message: str, status_code: Optional[int] = None):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

class TwelveDataClient:
    """Enhanced client for interacting with TwelveData API"""
    
    def __init__(self, api_key: Optional[str] = None, rate_limit_config: Optional[RateLimitConfig] = None):
        self.api_key = api_key or os.getenv("TWELVEDATA_API_KEY")
        if not self.api_key:
            raise ValueError("TwelveData API key is required")
            
        self.base_url = "https://api.twelvedata.com"
        self.rate_limit_config = rate_limit_config or RateLimitConfig()
        
        # Rate limiting tracking
        self._request_times = []
        self._daily_request_count = 0
        self._last_reset_date = datetime.now().date()
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={"User-Agent": "MarketDataService/1.0"}
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self._session:
            await self._session.close()
    
    def _reset_daily_counter_if_needed(self):
        """Reset daily request counter if it's a new day"""
        today = datetime.now().date()
        if today != self._last_reset_date:
            self._daily_request_count = 0
            self._last_reset_date = today
    
    async def _respect_rate_limit(self):
        """Enhanced rate limiting with both per-minute and daily limits"""
        now = datetime.now()
        
        # Reset daily counter if needed
        self._reset_daily_counter_if_needed()
        
        # Check daily limit
        if self._daily_request_count >= self.rate_limit_config.requests_per_day:
            raise TwelveDataError(f"Daily rate limit of {self.rate_limit_config.requests_per_day} requests exceeded")
        
        # Remove requests older than 1 minute
        one_minute_ago = now - timedelta(minutes=1)
        self._request_times = [t for t in self._request_times if t > one_minute_ago]
        
        # Check per-minute limit
        if len(self._request_times) >= self.rate_limit_config.requests_per_minute:
            # Calculate how long to wait
            oldest_request = min(self._request_times)
            wait_time = 60 - (now - oldest_request).total_seconds()
            
            if wait_time > 0:
                logger.info(f"Rate limit reached, waiting {wait_time:.1f} seconds")
                await asyncio.sleep(wait_time + 0.1)  # Small buffer
                
        if self._request_times and (now - max(self._request_times)).total_seconds() < self.rate_limit_config.burst_delay:
            await asyncio.sleep(self.rate_limit_config.burst_delay)
        
        # Record this request
        self._request_times.append(now)
        self._daily_request_count += 1
    
    async def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Make an authenticated request to the TwelveData API"""
        await self._respect_rate_limit()
        
        # Ensure we have a session
        if not self._session:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={"User-Agent": "MarketDataService/1.0"}
            )
        
        url = f"{self.base_url}/{endpoint}"
        params["apikey"] = self.api_key
        params["format"] = "JSON"
        
        logger.debug(f"Making request to {endpoint} with params: {params}")
        
        try:
            async with self._session.get(url, params=params) as response:
                response_text = await response.text()
                
                if response.status != 200:
                    logger.error(f"HTTP {response.status}: {response_text}")
                    raise TwelveDataError(f"HTTP {response.status}: {response_text}", response.status)
                
                try:
                    data = json.loads(response_text)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON response: {e}")
                    raise TwelveDataError(f"Invalid JSON response: {e}")
                
                # Check for API errors
                if isinstance(data, dict) and data.get("status") == "error":
                    error_msg = data.get("message", "Unknown API error")
                    logger.error(f"API error: {error_msg}")
                    raise TwelveDataError(f"API error: {error_msg}")
                
                return data
                
        except aiohttp.ClientError as e:
            logger.error(f"Network error: {e}")
            raise TwelveDataError(f"Network error: {e}")
    
    async def fetch_time_series(
        self,
        symbol: str,
        interval: str = "5min",
        outputsize: int = 5000,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetch time series data from TwelveData API
        
        Args:
            symbol: Ticker symbol
            interval: Data interval (use Interval enum values)
            outputsize: Number of data points to return (max 5000)
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Dictionary with time series data
        """
        # Validate interval
        if interval not in [i.value for i in Interval]:
            logger.warning(f"Invalid interval '{interval}', using default '5min'")
            interval = "5min"
        
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "outputsize": min(outputsize, 5000)  # Ensure we don't exceed max
        }
        
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
            
        logger.info(f"Fetching time series for {symbol} with interval {interval}")
        
        try:
            data = await self._make_request("time_series", params)
            
            # Handle empty response
            if not data.get("values"):
                logger.warning(f"No data received for {symbol}")
                return {"values": [], "meta": data.get("meta", {})}
                
            logger.info(f"Received {len(data.get('values', []))} records for {symbol}")
            return data
            
        except TwelveDataError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching data: {e}", exc_info=True)
            raise TwelveDataError(f"Unexpected error: {e}")
    
    async def fetch_historical_intraday_chunked(
        self,
        symbol: str,
        interval: str = "5min",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_retries: int = 3
    ) -> Dict[str, Any]:
        """
        Fetch historical intraday data month-by-month to minimize API calls
        
        Args:
            symbol: Ticker symbol
            interval: Data interval
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            max_retries: Maximum number of retries per chunk
            
        Returns:
            Dictionary with time series data merged from all months
        """
        # Set default dates if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            # Default to 1 year for intraday data
            start_date_dt = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=365)
            start_date = start_date_dt.strftime("%Y-%m-%d")
            
        # Convert to datetime for easier manipulation
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Generate month chunks
        chunks = []
        current_date = start_dt.replace(day=1)  # Start from first day of month
        
        while current_date <= end_dt:
            # Calculate end of current month
            if current_date.month == 12:
                next_month = current_date.replace(year=current_date.year + 1, month=1)
            else:
                next_month = current_date.replace(month=current_date.month + 1)
            
            month_end = next_month - timedelta(days=1)
            
            # Adjust for actual start/end dates
            chunk_start = max(current_date, start_dt)
            chunk_end = min(month_end, end_dt)
            
            chunks.append((chunk_start, chunk_end))
            current_date = next_month
        
        logger.info(f"Fetching {interval} data for {symbol} from {start_date} to {end_date} in {len(chunks)} monthly chunks")
        
        # Store all values
        all_values = []
        failed_chunks = []
        
        # Process monthly chunks with retry logic
        for i, (chunk_start, chunk_end) in enumerate(chunks):
            chunk_start_str = chunk_start.strftime("%Y-%m-%d")
            chunk_end_str = chunk_end.strftime("%Y-%m-%d")
            
            logger.info(f"Processing month {i+1}/{len(chunks)}: {chunk_start_str} to {chunk_end_str}")
            
            for attempt in range(max_retries):
                try:
                    chunk_data = await self.fetch_time_series(
                        symbol=symbol,
                        interval=interval,
                        outputsize=5000,
                        start_date=chunk_start_str,
                        end_date=chunk_end_str
                    )
                    
                    if "values" in chunk_data and chunk_data["values"]:
                        all_values.extend(chunk_data["values"])
                        logger.info(f"Added {len(chunk_data['values'])} records from month {i+1}")
                    break
                    
                except TwelveDataError as e:
                    logger.warning(f"Month {i+1} attempt {attempt+1} failed: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt) 
                    else:
                        failed_chunks.append((chunk_start_str, chunk_end_str))
                        logger.error(f"Month {i+1} failed after {max_retries} attempts")
            
            # Small delay between chunks
            if i < len(chunks) - 1:
                await asyncio.sleep(1)
        
        # Report results
        if failed_chunks:
            logger.warning(f"Failed to fetch {len(failed_chunks)} monthly chunks: {failed_chunks}")
        
        # Sort values by datetime (newest first typically)
        if all_values:
            try:
                all_values.sort(key=lambda x: datetime.fromisoformat(x.get('datetime', '').replace('Z', '+00:00')), reverse=True)
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not sort values by datetime: {e}")
        
        result = {
            "meta": {
                "symbol": symbol.upper(),
                "interval": interval,
                "start_date": start_date,
                "end_date": end_date,
                "total_chunks": len(chunks),
                "failed_chunks": len(failed_chunks)
            },
            "values": all_values
        }
        
        logger.info(f"Completed fetching historical data: {len(all_values)} total records, {len(failed_chunks)} failed chunks")
        return result
    
    async def get_quote(self, symbol: str) -> Dict[str, Any]:
        """Get real-time quote for a symbol"""
        params = {"symbol": symbol.upper()}
        return await self._make_request("quote", params)
    
    async def get_market_state(self, exchange: str = "NASDAQ") -> Dict[str, Any]:
        """Get current market state"""
        params = {"exchange": exchange}
        return await self._make_request("market_state", params)
    
    def get_rate_limit_status(self) -> Dict[str, Any]:
        """Get current rate limit status"""
        now = datetime.now()
        one_minute_ago = now - timedelta(minutes=1)
        recent_requests = len([t for t in self._request_times if t > one_minute_ago])
        
        return {
            "requests_this_minute": recent_requests,
            "requests_today": self._daily_request_count,
            "daily_limit": self.rate_limit_config.requests_per_day,
            "minute_limit": self.rate_limit_config.requests_per_minute,
            "can_make_request": recent_requests < self.rate_limit_config.requests_per_minute
        }