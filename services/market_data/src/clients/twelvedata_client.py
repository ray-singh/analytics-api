import asyncio
import logging
import os
import json
import aiohttp
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta

logger = logging.getLogger('market_data_service.twelvedata')

class TwelveDataClient:
    """Client for interacting with TwelveData API"""
    
    def __init__(self):
        self.api_key = os.getenv("TWELVEDATA_API_KEY")
        self.base_url = "https://api.twelvedata.com"
        self.rate_limit_per_minute = 8  # Free tier limit
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
            interval: Data interval (1min, 5min, 15min, 30min, 1h, 4h, 1day)
            outputsize: Number of data points to return (max 5000)
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Dictionary with time series data
        """
        await self._respect_rate_limit()
        
        url = f"{self.base_url}/time_series"
        
        params = {
            "symbol": symbol,
            "interval": interval,
            "apikey": self.api_key,
            "format": "JSON",
            "outputsize": min(outputsize, 5000)  # Ensure we don't exceed max
        }
        
        # Add optional parameters
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
            
        logger.info(f"Fetching time series for {symbol} with interval {interval}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Check for API errors
                        if "status" in data and data["status"] == "error":
                            logger.error(f"API error: {data.get('message', 'Unknown error')}")
                            return {"values": []}
                            
                        logger.info(f"Received {len(data.get('values', []))} records for {symbol}")
                        return data
                    else:
                        logger.error(f"HTTP error {response.status}: {await response.text()}")
                        return {"values": []}
        except Exception as e:
            logger.error(f"Error fetching data: {e}", exc_info=True)
            return {"values": []}
            
    async def fetch_historical_intraday_chunked(
        self,
        symbol: str,
        interval: str = "5min",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        chunk_days: int = 60
    ) -> Dict[str, Any]:
        """
        Fetch historical intraday data in chunks to handle API limitations
        
        Args:
            symbol: Ticker symbol
            interval: Data interval (1min, 5min, 15min, 30min, 1h, 4h)
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            chunk_days: Number of days to fetch in each chunk
            
        Returns:
            Dictionary with time series data merged from all chunks
        """
        # Set default dates if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            # Default to 1 year of data
            start_date_dt = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=365)
            start_date = start_date_dt.strftime("%Y-%m-%d")
            
        # Convert to datetime for easier manipulation
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Calculate total days and number of chunks
        total_days = (end_dt - start_dt).days
        num_chunks = (total_days // chunk_days) + (1 if total_days % chunk_days > 0 else 0)
        
        logger.info(f"Fetching {interval} data for {symbol} from {start_date} to {end_date} in {num_chunks} chunks")
        
        # Store all values
        all_values = []
        
        # Process in chunks
        current_start = start_dt
        for chunk in range(num_chunks):
            # Calculate end of this chunk
            current_end = current_start + timedelta(days=min(chunk_days, end_dt - current_start))
            
            # Format dates
            chunk_start_str = current_start.strftime("%Y-%m-%d")
            chunk_end_str = current_end.strftime("%Y-%m-%d")
            
            logger.info(f"Fetching chunk {chunk+1}/{num_chunks}: {chunk_start_str} to {chunk_end_str}")
            
            # Fetch this chunk
            chunk_data = await self.fetch_time_series(
                symbol=symbol,
                interval=interval,
                outputsize=5000,  # Max allowed
                start_date=chunk_start_str,
                end_date=chunk_end_str
            )
            
            # Add values to our collection
            if "values" in chunk_data:
                all_values.extend(chunk_data["values"])
                logger.info(f"Added {len(chunk_data['values'])} records from chunk {chunk+1}")
            
            # Move to next chunk
            current_start = current_end + timedelta(days=1)
            
            # Respect rate limit between chunks
            await asyncio.sleep(1)
        
        # Create a complete result
        result = {
            "meta": {
                "symbol": symbol,
                "interval": interval,
                "start_date": start_date,
                "end_date": end_date
            },
            "values": all_values
        }
        
        logger.info(f"Completed fetching historical data with {len(all_values)} total records")
        return result