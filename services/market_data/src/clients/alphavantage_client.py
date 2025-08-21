import asyncio
import logging
import os
import aiohttp
import pandas as pd
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from io import StringIO

logger = logging.getLogger('market_data_service.alphavantage')

class AlphaVantageClient:
    """Client for interacting with Alpha Vantage API"""
    
    def __init__(self):
        self.api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        self.base_url = "https://www.alphavantage.co/query"
        self.rate_limit_per_minute = 5  # Free tier limit
        self._last_request_time = None
        self._request_count = 0
    
    async def _respect_rate_limit(self):
        """Enforce rate limiting to avoid API throttling"""
        now = datetime.now()
        
        # Reset counter if we're in a new minute
        if self._last_request_time and (now - self._last_request_time).seconds >= 60:
            self._request_count = 0
            
        # If we've hit the rate limit, wait until the next minute
        if self._request_count >= self.rate_limit_per_minute:
            wait_seconds = 60 - (now - self._last_request_time).seconds
            if wait_seconds > 0:
                logger.info(f"Rate limit reached, waiting {wait_seconds} seconds")
                await asyncio.sleep(wait_seconds + 1)
            self._request_count = 0
        
        self._last_request_time = now
        self._request_count += 1
    
    async def fetch_daily_adjusted(
        self,
        symbol: str,
        outputsize: str = "full"  # "compact" for 100 days, "full" for 20+ years
    ) -> pd.DataFrame:
        """
        Fetch daily OHLCV data with adjusted close from Alpha Vantage
        
        Args:
            symbol: Ticker symbol
            outputsize: "compact" for latest 100 data points, "full" for 20+ years
            
        Returns:
            DataFrame with daily OHLCV data
        """
        await self._respect_rate_limit()
        
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": outputsize,
            "datatype": "csv",  #
            "apikey": self.api_key
        }
        
        logger.info(f"Fetching daily data for {symbol} with outputsize {outputsize}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, params=params) as response:
                    if response.status == 200:
                        csv_text = await response.text()
                        # Check for error messages
                        if "Error Message" in csv_text:
                            logger.error(f"API error: {csv_text}")
                            return pd.DataFrame()
                        
                        # Convert CSV text to DataFrame
                        df = pd.read_csv(StringIO(csv_text))
                        
                        if df.empty or 'timestamp' not in df.columns:
                            logger.error(f"Invalid CSV data received: {csv_text[:100]}...")
                            return pd.DataFrame()
                        
                        # Set timestamp as index and convert to datetime
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        df.set_index('timestamp', inplace=True)
                        df.index.name = 'date'
            
                        df.sort_index(inplace=True)
                        
                        logger.info(f"Received {len(df)} daily records for {symbol}")
                        return df
                    else:
                        logger.error(f"HTTP error {response.status}: {await response.text()}")
                        return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching Alpha Vantage data: {e}", exc_info=True)
            return pd.DataFrame()
    
    async def fetch_historical_daily(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Fetch and filter historical daily data within date range
        
        Args:
            symbol: Ticker symbol
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with filtered daily OHLCV data
        """
        # Fetch full dataset - Alpha Vantage doesn't support date filtering directly
        df = await self.fetch_daily_adjusted(symbol, "full")
        
        if df.empty:
            return df
            
        # Filter by date range if provided
        if start_date:
            start_dt = pd.Timestamp(start_date)
            df = df[df.index >= start_dt]
            
        if end_date:
            end_dt = pd.Timestamp(end_date)
            df = df[df.index <= end_dt]
        
        logger.info(f"Filtered to {len(df)} records between {start_date or 'earliest'} and {end_date or 'latest'}")
        return df