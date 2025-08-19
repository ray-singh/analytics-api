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

logger = logging.getLogger('market_data_service.backfill')

class BackfillManager:
    """Manager for handling historical data backfilling"""
    
    def __init__(self):
        self.yfinance_client = YFinanceClient()  # Keep as fallback
        self.twelvedata_client = TwelveDataClient()
        self.alphavantage_client = AlphaVantageClient()
        self.kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        
    async def check_if_backfill_needed(self, symbol: str, interval: str) -> bool:
        """
        Check if backfill is needed for a symbol by querying the database
        
        Args:
            symbol: Ticker symbol
            interval: Data interval
            
        Returns:
            True if backfill is needed, False otherwise
        """
        try:
            import aiopg
            from datetime import datetime, timedelta
            import os
            
            logger.info(f"Checking if backfill is needed for {symbol} with interval {interval}")
            db_url = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/stockanalytics")
            
            # Determine which table to query based on interval
            if interval in ["1d", "1day"]:
                table_name = "historical_ohlcv"
            else:
                table_name = "intraday_ohlcv"
            
            # Convert interval to minutes for querying
            interval_minutes = self._convert_interval_to_minutes(interval)
            
            async with aiopg.create_pool(db_url) as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        # Check if any data exists for this symbol and interval
                        if table_name == "intraday_ohlcv":
                            await cur.execute(
                                """
                                SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
                                FROM intraday_ohlcv
                                WHERE symbol = %s AND interval_minutes = %s
                                """,
                                (symbol, interval_minutes)
                            )
                        else:
                            await cur.execute(
                                """
                                SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
                                FROM historical_ohlcv
                                WHERE symbol = %s
                                """,
                                (symbol,)
                            )
                        
                        result = await cur.fetchone()
                        count, min_date, max_date = result
                        
                        if count == 0 or not min_date or not max_date:
                            logger.info(f"No existing data for {symbol} with interval {interval}, backfill needed")
                            return True
                        
                        # Check if the latest data is recent enough
                        now = datetime.now()
                        
                        # For intraday data, we should have data from the last trading day
                        if table_name == "intraday_ohlcv":
                            # If the most recent data is more than 24 hours old, we need a backfill
                            if now - max_date > timedelta(hours=24):
                                logger.info(f"Most recent data for {symbol} is too old ({max_date}), backfill needed")
                                return True
                        else:
                            # For daily data, we should have data from the last few days
                            if now - max_date > timedelta(days=1):
                                logger.info(f"Most recent daily data for {symbol} is too old ({max_date}), backfill needed")
                                return True
                        
                        # Check for gaps in the data
                        # For this example, check if we have at least 80% of expected data points
                        
                        # Calculate expected number of data points
                        if table_name == "intraday_ohlcv":
                            # For intraday, calculate trading hours between min and max date
                            # Assuming 8 trading hours per day, Monday to Friday
                            business_days = self._count_business_days(min_date, max_date)
                            expected_bars_per_day = (8 * 60) // interval_minutes
                            expected_count = business_days * expected_bars_per_day
                        else:
                            # For daily, simply count business days
                            expected_count = self._count_business_days(min_date, max_date)
                        
                        # If we have less than 80% of expected data, we need to backfill
                        if count < 0.8 * expected_count:
                            logger.info(f"Data gaps detected for {symbol}. Found {count} bars, expected ~{expected_count}")
                            return True
                        
                        logger.info(f"Sufficient data exists for {symbol} with interval {interval}, no backfill needed")
                        return False
        
        except Exception as e:
            logger.error(f"Error checking if backfill needed: {e}", exc_info=True)
            # If we encounter an error, assume we need to backfill to be safe
            return True
    
    async def run_backfill(
        self,
        symbol: str,
        interval: str = "5min",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        price_callback: Optional[Callable] = None
    ):
        """
        Run a backfill operation for historical data
        
        Args:
            symbol: Ticker symbol
            interval: Data interval
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            price_callback: Optional callback for processing each price
        """
        logger.info(f"Starting backfill for {symbol} with interval {interval}")
        
        # Set default dates if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            # Default to 30 days of data
            start_date_dt = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=30)
            start_date = start_date_dt.strftime("%Y-%m-%d")
        
        # Determine which data provider to use based on interval
        if interval in ["1min", "5min", "15min", "30min", "1h", "4h"]:
            # Intraday data - use TwelveData
            data = await self.fetch_intraday_data(symbol, interval, start_date, end_date)
        else:
            # Daily data - use AlphaVantage
            data = await self.fetch_daily_data(symbol, start_date, end_date)
        
        # Process the data
        await self.process_backfill_data(symbol, interval, data, price_callback)
    
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
            
            logger.info(f"Fetching {interval} data for {symbol} from {start_date} to {end_date}")
            
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
                return combined_df
            
            logger.warning(f"No historical intraday data found for {symbol}")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error in fetch_intraday_data: {e}", exc_info=True)
            return pd.DataFrame()

    async def fetch_daily_data(self, symbol, start_date, end_date):
        """Fetch daily historical data using AlphaVantage"""
        try:
            # Use AlphaVantage for daily data
            logger.info(f"Fetching daily data for {symbol} from AlphaVantage")
            df = await self.alphavantage_client.fetch_historical_daily(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )
            
            if len(df) > 0:
                logger.info(f"Using AlphaVantage data for {symbol} ({len(df)} records)")
                return df
            
            # Fall back to YFinance if AlphaVantage fails
            logger.warning(f"No data from AlphaVantage, falling back to YFinance")
            df = await self.yfinance_client.fetch_historical_data(
                symbol=symbol,
                interval="1d",
                start_date=start_date,
                end_date=end_date
            )
            
            if len(df) > 0:
                logger.info(f"Using YFinance data for {symbol} ({len(df)} records)")
                return df
            
            logger.warning(f"No historical daily data found for {symbol}")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error fetching daily historical data: {e}", exc_info=True)
            return pd.DataFrame()
    
    async def process_backfill_data(self, symbol, interval, df, price_callback=None):
        """Process backfilled data and publish to Kafka or callback"""
        if df.empty:
            logger.warning(f"No data to process for {symbol}")
            return
        
        count = 0
        producer = None
        
        try:
            # Start Kafka producer if no callback provided
            if not price_callback:
                producer = AIOKafkaProducer(bootstrap_servers=self.kafka_bootstrap)
                await producer.start()
            
            # Process each record
            for timestamp, row in df.iterrows():
                # Get Unix timestamp
                unix_ts = int(timestamp.timestamp())
                
                # Process price - either through callback or direct to Kafka
                if price_callback:
                    await price_callback(symbol, row['close'], unix_ts, row.get('volume', 0))
                elif producer:
                    # Determine if this is daily data with adjusted close
                    is_daily = 'adjusted_close' in row
                    
                    # Prepare event
                    event = {
                        "event_type": "price.ohlcv",
                        "version": "1.0",
                        "symbol": symbol,
                        "interval_minutes": self._convert_interval_to_minutes(interval),
                        "bar_start_ts": unix_ts,
                        "bar_end_ts": unix_ts + (self._convert_interval_to_minutes(interval) * 60) - 1,
                        "open": float(row['open']),
                        "high": float(row['high']),
                        "low": float(row['low']),
                        "close": float(row['close']),
                        "volume": int(row.get('volume', 0)),
                        "source": "backfill"
                    }
                    
                    # Add adjusted close for daily data
                    if is_daily:
                        event["adjusted_close"] = float(row['adjusted_close'])
                    
                    # Publish to Kafka
                    key = f"{symbol}:{interval}".encode()
                    await producer.send_and_wait("market.prices.ohlcv", key=key, value=json.dumps(event).encode())
                
                count += 1
                
                # Log progress occasionally
                if count % 1000 == 0:
                    logger.info(f"Processed {count} historical records for {symbol}")
            
            logger.info(f"Completed processing {count} records for {symbol}")
            
        except Exception as e:
            logger.error(f"Error processing backfill data: {e}", exc_info=True)
        finally:
            if producer:
                await producer.stop()
    
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