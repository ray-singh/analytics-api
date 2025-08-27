import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Optional, Callable, Dict, Any
import pandas as pd
import os
from aiokafka import AIOKafkaProducer
from ..clients.yfinance_client import YFinanceClient
from ..clients.twelvedata_client import TwelveDataClient
from ..clients.alphavantage_client import AlphaVantageClient
from ..repositories.ohlcv_repository import OHLCVRepository


logger = logging.getLogger('market_data_service.backfill')

PRICE_TOPIC = os.getenv("PRICE_TOPIC", "market.prices.raw")

class BackfillManager:
    """Manager for handling historical data backfilling"""
    
    def __init__(self):
        self.yfinance_client = YFinanceClient() 
        self.twelvedata_client = TwelveDataClient()
        self.alphavantage_client = AlphaVantageClient()
        self.kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.db_url = os.getenv("DATABASE_URL", "postgresql://postgres:password@timescaledb:5432/stockanalytics")
        self.repo = OHLCVRepository(self.db_url)
        self.producer = None

    async def init_producer(self):
        """Initialize the Kafka producer asynchronously"""
        if self.producer is None:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.kafka_bootstrap,
                retry_backoff_ms=500,
                request_timeout_ms=30000
            )
            await self.producer.start()
    
    async def close_producer(self):
        """Close the Kafka producer asynchronously"""
        if self.producer is not None:
            await self.producer.stop()
            self.producer = None

    async def fetch_intraday_data(self, symbol, interval, start_date, end_date):
        """
        Fetch intraday historical data using TwelveData with intelligent chunking
        to handle API rate limits and large date ranges
        
        Args:
            symbol: Ticker symbol
            interval: Data interval (e.g., '1min', '5min', '15min', '1h')
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            pandas.DataFrame: Combined historical data
        """
        try:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                
            try:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S') if end_date else datetime.now()
            except ValueError:
                # Fall back to date-only format
                end_dt = datetime.strptime(end_date, '%Y-%m-%d') if end_date else datetime.now()
            
            logger.info(f"Starting intraday backfill for {symbol} {interval} from {start_date} to {end_date}")
            
            # Calculate optimal chunk size based on interval
            # More frequent intervals = smaller chunks to avoid hitting API limits
            chunk_days = self._calculate_optimal_chunk_size(interval)
            
            # Calculate total number of API calls needed
            total_days = (end_dt - start_dt).days
            num_chunks = (total_days // chunk_days) + (1 if total_days % chunk_days > 0 else 0)
            
            logger.info(f"Will fetch data in {num_chunks} chunks of {chunk_days} days each")
            current_start = start_dt
            chunk_num = 1
            all_data = []
            api_calls = 0
            
            while current_start < end_dt:
                # Calculate end date for current chunk
                current_end = min(current_start + timedelta(days=chunk_days), end_dt)
                
                # Format dates for API call
                chunk_start_str = current_start.strftime('%Y-%m-%d %H:%M:%S')
                chunk_end_str = current_end.strftime('%Y-%m-%d %H:%M:%S')

                logger.info(f"Fetching chunk {chunk_num}/{num_chunks}: {chunk_start_str} to {chunk_end_str}")
                
                try:
                    # Call TwelveData API for this chunk
                    data = await self.twelvedata_client.fetch_time_series(
                        symbol=symbol,
                        interval=interval,
                        outputsize=5000,  # Maximum allowed
                        start_date=chunk_start_str,
                        end_date=chunk_end_str
                    )
                    
                    # Process the chunk data
                    if data and "values" in data and len(data["values"]) > 0:
                        # TwelveData format is different from YFinance
                        chunk_df = pd.DataFrame(data["values"])
                        
                        # Convert types
                        chunk_df["datetime"] = pd.to_datetime(chunk_df["datetime"])
                        for col in ["open", "high", "low", "close"]:
                            chunk_df[col] = pd.to_numeric(chunk_df[col])
                        chunk_df["volume"] = pd.to_numeric(chunk_df["volume"])
                        
                        # Set index to datetime for consistency
                        chunk_df.set_index("datetime", inplace=True)
                        
                        # Add to our list of chunks
                        all_data.append(chunk_df)
                        
                        logger.info(f"Successfully fetched {len(chunk_df)} data points for chunk {chunk_num}")
                    else:
                        logger.warning(f"No data returned for chunk {chunk_num}")
                    
                    # Rate limiting management
                    api_calls += 1
                    if api_calls >= 8:  # TwelveData free tier limit
                        logger.info("Rate limit approaching. Pausing for 65 seconds...")
                        await asyncio.sleep(65)  # Wait a bit more than a minute
                        api_calls = 0
                    else:
                        # Small pause between requests
                        await asyncio.sleep(1)
                        
                except Exception as e:
                    logger.error(f"Error fetching chunk {chunk_num}: {e}")
                    
                # Move to next chunk
                current_start = current_end + timedelta(days=1)
                chunk_num += 1
            
            # Combine all chunks into a single DataFrame
            if all_data:
                combined_df = pd.concat(all_data)
                combined_df = combined_df.sort_index()
                logger.info(f"Successfully fetched a total of {len(combined_df)} records for {symbol}")
                
                if len(combined_df) > 0:
                    interval_minutes = self._convert_interval_to_minutes(interval)

                    for idx, row in combined_df.iterrows():
                        try:
                            ts = idx
                            if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
                                ts = ts.tz_localize("UTC")
                            await self.repo.insert_intraday_ohlcv(
                                symbol=symbol,
                                timestamp=ts,
                                open_price=float(row['open']),
                                high=float(row['high']),
                                low=float(row['low']),
                                close=float(row['close']),
                                volume=int(row['volume']),
                                interval_minutes=interval_minutes,
                                source='twelvedata'
                            )
                            await self.init_producer()
                            event = {
                            "event_type": "price.ohlcv",
                            "symbol": symbol,
                            "interval_minutes":interval_minutes,
                            "open": float(row["open"]),
                            "high": float(row["high"]),
                            "low": float(row["low"]),
                            "close": float(row["close"]),
                            "volume": int(row["volume"]),
                            "timestamp": ts.isoformat()
                            }
                            await self.producer.send_and_wait("market.prices.ohlcv", json.dumps(event).encode())
                        except Exception as e:
                            logger.error(f"Error inserting intraday data: {e}")
        
                    logger.info(f"Successfully stored {len(combined_df)} intraday bars for {symbol} in database")
                await self.close_producer()
                return combined_df
            
            logger.warning(f"No historical intraday data found for {symbol}")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error in fetch_intraday_data: {e}", exc_info=True)
            return pd.DataFrame()

    async def fetch_daily_data(self, symbol, start_date=None, end_date=None):
        """Fetch daily historical data using AlphaVantage"""
        try:
            # Use AlphaVantage for daily data
            logger.info(f"Fetching daily data for {symbol} from AlphaVantage")
            if start_date and end_date:
                df = await self.alphavantage_client.fetch_historical_daily(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date
                )
            else:
                df = await self.alphavantage_client.fetch_daily_adjusted(symbol=symbol, outputsize="full")
            
            if len(df) > 0:
                logger.info(f"Using AlphaVantage data for {symbol} ({len(df)} records)")                
                for date, row in df.iterrows():
                    try:
                        await self.repo.insert_historical_daily(
                            symbol=symbol,
                            timestamp=date,
                            open_price=float(row['open']),
                            high=float(row['high']),
                            low=float(row['low']),
                            close=float(row['close']),
                            volume=int(row['volume']),
                            source='alphavantage'
                        )
                        await self.init_producer() 
                        event = {
                        "event_type": "price.ohlcv",
                        "symbol": symbol,
                        "interval_minutes": 1440,
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                        "volume": int(row["volume"]),
                        "timestamp": date.isoformat()
                        }
                        await self.producer.send_and_wait("market.prices.ohlcv", json.dumps(event).encode())
                    except Exception as e:
                        logger.error(f"Error inserting daily data: {e}")
                
                logger.info(f"Successfully stored {len(df)} daily bars for {symbol} in database")
                await self.close_producer()
                return df
            
            logger.warning(f"No historical daily data found for {symbol}")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error fetching daily historical data: {e}", exc_info=True)
            return pd.DataFrame()

    def _convert_interval_to_minutes(self, interval):
        """Convert interval string to minutes"""
        if "min" in interval:
            return int(interval.replace("min", ""))
        elif "h" in interval:
            return int(interval.replace("h", "")) * 60
        elif interval == "1day" or interval == "1d":
            return 1440  # 24 hours in minutes
        else:
            return 5  # Default to 5min
    
    def _count_business_days(self, start_date, end_date):
        """Helper method to count business days between two dates"""
        # Adjust to ensure dates are datetime objects
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)
        
        # Ensure start is before end
        if start_date > end_date:
            start_date, end_date = end_date, start_date
        
        # Count the days
        days = 0
        current = start_date
        while current <= end_date:
            # Monday = 0, Sunday = 6
            if current.weekday() < 5:
                days += 1
            current += timedelta(days=1)
        
        return days
    
    def _calculate_optimal_chunk_size(self, interval):
        """Calculate optimal chunk size in days based on the interval"""
        # Smaller intervals generate more data points per day
        # so we need to use smaller chunks to avoid hitting API limits
        if interval == "1min":
            return 2  # Very small chunks for 1-minute data
        elif interval in ["5min", "15min"]:
            return 7  # ~1 week for 5/15 minute data
        elif interval == "30min":
            return 14  # ~2 weeks for 30 minute data
        elif interval == "1h":
            return 30  # ~1 month for hourly data
        elif interval == "4h":
            return 60  # ~2 months for 4-hour data
        else:
            return 30  # Default to 30 days