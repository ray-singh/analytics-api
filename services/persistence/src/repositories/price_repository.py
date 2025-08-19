import asyncio
import asyncpg
import logging
from datetime import datetime, timezone, timedelta
from dateutil import parser

logger = logging.getLogger("persistence-service")

class PriceRepository:
    def __init__(self, database_url=None):
        """Initialize the repository with database connection URL"""
        self.pool = None
        self.database_url = database_url

    async def connect(self, database_url=None):
        """Connect to the database"""
        # Use the provided URL, or the one from constructor, or default
        connection_url = database_url or self.database_url
        if connection_url:
            self.pool = await asyncpg.create_pool(connection_url)
        else:
            # Default connection if URL not provided
            self.pool = await asyncpg.create_pool(
                host='timescaledb',
                port=5432,
                user='postgres',
                password='postgres',
                database='stockanalytics'
            )
        logger.info("Connected to database")
        
    async def disconnect(self):
        """Close the database connection pool"""
        if self.pool:
            await self.pool.close()
        logger.info("Disconnected from database")
    
    async def init_db(self):
        """Initialize the database schema for price data"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            # Enable TimescaleDB extension if not already enabled
            await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
            
            # Create real-time prices table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS realtime_prices (
                    symbol TEXT NOT NULL,
                    price NUMERIC(12, 4) NOT NULL,
                    volume BIGINT,
                    timestamp TIMESTAMPTZ NOT NULL,
                    source TEXT,
                    inserted_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            
            # Add indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_realtime_prices_symbol ON realtime_prices (symbol)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_realtime_prices_timestamp ON realtime_prices (timestamp)")
            
            # Convert to hypertable if not already
            try:
                await conn.execute("""
                    SELECT create_hypertable('realtime_prices', 'timestamp', 
                                          if_not_exists => TRUE)
                """)
            except Exception as e:
                logger.warning(f"Could not create hypertable for realtime_prices: {e}")
                
            # Create compression policy (optional)
            try:
                await conn.execute("""
                    ALTER TABLE realtime_prices SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'symbol'
                    )
                """)
                
                # Add compression policy - compress data older than 1 day
                await conn.execute("""
                    SELECT add_compression_policy('realtime_prices', 
                                              INTERVAL '1 day',
                                              if_not_exists => TRUE)
                """)
            except Exception as e:
                logger.warning(f"Could not set compression policy: {e}")
                
            # Add retention policy - drop data older than 7 days
            try:
                await conn.execute("""
                    SELECT add_retention_policy('realtime_prices', 
                                             INTERVAL '7 days',
                                             if_not_exists => TRUE)
                """)
            except Exception as e:
                logger.warning(f"Could not set retention policy: {e}")

    async def insert_realtime_price(self, symbol, price, timestamp, volume=0, source="unknown"):
        """Insert a real-time price into the database"""
        try:
            # Convert string timestamp to datetime object
            if isinstance(timestamp, str):
                try:
                    timestamp = parser.parse(timestamp)
                except Exception as e:
                    logger.error(f"Failed to parse timestamp '{timestamp}': {e}")
                    return False

            # Ensure we have a pool
            pool = await self.get_pool()
                
            async with pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO realtime_prices (symbol, price, timestamp, volume, source)
                    VALUES ($1, $2, $3, $4, $5)
                ''', symbol, price, timestamp, volume, source)
                    
                return True
        except Exception as e:
            logger.error(f"Error inserting real-time price: {e}", exc_info=True)
            return False

    async def get_latest_price(self, symbol):
        """Get the latest price for a symbol"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT price, timestamp, volume 
                FROM realtime_prices 
                WHERE symbol = $1 
                ORDER BY timestamp DESC 
                LIMIT 1
                """,
                symbol
            )
            
            if row:
                return {
                    "symbol": symbol,
                    "price": float(row["price"]),
                    "timestamp": row["timestamp"].isoformat(),
                    "volume": row["volume"]
                }
            return None
            
    async def get_prices_by_timerange(self, symbol, start_time=None, end_time=None, limit=1000):
        """
        Get real-time prices for a symbol within a time range
        
        Args:
            symbol (str): The ticker symbol
            start_time (datetime/int): Start timestamp (defaults to 24 hours ago)
            end_time (datetime/int): End timestamp (defaults to now)
            limit (int): Maximum number of records to return
            
        Returns:
            List of price dictionaries
        """
        pool = await self.get_pool()
        
        # Default time range if not specified
        if end_time is None:
            end_time = datetime.now(timezone.utc)
        if start_time is None:
            start_time = end_time - timedelta(hours=24)
            
        # Convert timestamps to datetime if needed
        if isinstance(start_time, (int, float)):
            start_time = datetime.fromtimestamp(start_time, tz=timezone.utc)
        if isinstance(end_time, (int, float)):
            end_time = datetime.fromtimestamp(end_time, tz=timezone.utc)
            
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT 
                    symbol, price, volume, timestamp 
                FROM realtime_prices 
                WHERE 
                    symbol = $1 AND 
                    timestamp BETWEEN $2 AND $3
                ORDER BY timestamp ASC
                LIMIT $4
                """,
                symbol, start_time, end_time, limit
            )
            
            result = []
            for row in rows:
                result.append({
                    "symbol": row["symbol"],
                    "price": float(row["price"]),
                    "timestamp": row["timestamp"].isoformat(),
                    "volume": row["volume"]
                })
            
            return result
            
    async def get_price_statistics(self, symbol, interval='1 hour'):
        """
        Get price statistics for a symbol over a time interval
        
        Args:
            symbol (str): The ticker symbol
            interval (str): Time interval for aggregation (e.g., '1 hour', '1 day')
            
        Returns:
            List of statistics by time bucket
        """
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT 
                    time_bucket($2, timestamp) AS bucket,
                    first(price, timestamp) AS open_price,
                    max(price) AS high_price,
                    min(price) AS low_price,
                    last(price, timestamp) AS close_price,
                    sum(volume) AS total_volume,
                    count(*) AS sample_count
                FROM realtime_prices 
                WHERE symbol = $1
                GROUP BY bucket
                ORDER BY bucket DESC
                LIMIT 24
                """,
                symbol, interval
            )
            
            result = []
            for row in rows:
                result.append({
                    "symbol": symbol,
                    "bucket": row["bucket"].isoformat(),
                    "open": float(row["open_price"]),
                    "high": float(row["high_price"]),
                    "low": float(row["low_price"]),
                    "close": float(row["close_price"]),
                    "volume": int(row["total_volume"]) if row["total_volume"] else 0,
                    "samples": row["sample_count"]
                })
            
            return result

    async def clean_old_data(self, older_than_days=7):
        """
        Manually clean up old data (useful for databases without TimescaleDB)
        
        Args:
            older_than_days (int): Delete data older than this many days
        """
        pool = await self.get_pool()
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=older_than_days)
        
        async with pool.acquire() as conn:
            result = await conn.execute(
                """
                DELETE FROM realtime_prices 
                WHERE timestamp < $1
                """,
                cutoff_date
            )
            logger.info(f"Deleted old price data: {result}")
            
    async def get_price_count_by_symbol(self):
        """Get count of prices by symbol"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT 
                    symbol, 
                    count(*) as count
                FROM realtime_prices 
                GROUP BY symbol
                ORDER BY count DESC
                """
            )
            
            result = {}
            for row in rows:
                result[row["symbol"]] = row["count"]
                
            return result
            
    async def bulk_insert_prices(self, prices):
        """
        Bulk insert multiple price records for efficiency
        
        Args:
            prices (list): List of price dictionaries with keys:
                          symbol, price, timestamp, volume, source
        """
        if not prices:
            return
            
        pool = await self.get_pool()
        records = []
        
        for price in prices:
            # Convert timestamp to datetime if it's a unix timestamp
            timestamp = price["timestamp"]
            if isinstance(timestamp, (int, float)):
                timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                
            records.append((
                price["symbol"],
                price["price"],
                price.get("volume"),
                timestamp,
                price.get("source")
            ))
            
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO realtime_prices (symbol, price, volume, timestamp, source)
                VALUES ($1, $2, $3, $4, $5)
                """,
                records
            )
            logger.info(f"Bulk inserted {len(records)} price records")
            
    async def get_pool(self):
        """Get the connection pool"""
        if not self.pool:
            await self.connect()
        return self.pool