import asyncio
import asyncpg
import logging
import json
from datetime import datetime, timezone
import dateutil.parser
import math

logger = logging.getLogger(__name__)

class AnalyticsRepository:
    def __init__(self, db_url):
        """Initialize the repository with database connection URL"""
        self.db_url = db_url
        self.pool = None

    async def get_pool(self):
        """Get or create a connection pool"""
        if self.pool is None:
            try:
                self.pool = await asyncpg.create_pool(self.db_url)
            except Exception as e:
                logger.error(f"Failed to create connection pool: {e}")
                raise
        return self.pool

    async def init_db(self):
        """Initialize the database schema for technical analysis data"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            # Enable TimescaleDB extension if not already enabled
            await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
            
            # Create intraday analytics table
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS intraday_analytics (
                id SERIAL,
                symbol VARCHAR(20) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                interval_minutes INTEGER NOT NULL,
                price NUMERIC(19,4) NOT NULL,
                indicators JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (id, timestamp),
                CONSTRAINT unique_intraday_analytics UNIQUE (symbol, timestamp, interval_minutes)
            );
        """)
            
            # Add indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_intraday_analytics_symbol_time ON intraday_analytics (symbol, timestamp)")
            
            # Convert to hypertable if not already
            try:
                await conn.execute("""
                    SELECT create_hypertable('intraday_analytics', 'timestamp', 
                                          if_not_exists => TRUE)
                """)
            except Exception as e:
                logger.warning(f"Could not create hypertable for intraday_analytics: {e}")
                
            # Create compression policy
            try:
                await conn.execute("""
                    ALTER TABLE intraday_analytics SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'symbol,interval_minutes'
                    )
                """)
                
                # Add compression policy - compress data older than 7 days
                await conn.execute("""
                    SELECT add_compression_policy('intraday_analytics', 
                                              INTERVAL '7 days',
                                              if_not_exists => TRUE)
                """)
            except Exception as e:
                logger.warning(f"Could not set compression policy: {e}")
                
            # Create daily analytics table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_analytics (
                id SERIAL,
                symbol VARCHAR(20) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                price NUMERIC(19,4) NOT NULL,
                indicators JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (id, timestamp),
                CONSTRAINT unique_daily_analytics UNIQUE (symbol, timestamp)
            );
            """)
            
            # Add indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_analytics_symbol ON daily_analytics (symbol)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_analytics_timestamp ON daily_analytics (timestamp)")

    async def insert_intraday_analytics(self, symbol, timestamp, price, interval_minutes, indicators):
        """Insert intraday technical analysis data"""
        pool = await self.get_pool()
        
        if isinstance(timestamp, str):
            timestamp = dateutil.parser.isoparse(timestamp)
        elif isinstance(timestamp, (int, float)):
            timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO intraday_analytics
                    (symbol, timestamp, interval_minutes, price, indicators)
                VALUES
                    ($1, $2, $3, $4, $5)
                ON CONFLICT (symbol, timestamp, interval_minutes) DO UPDATE
                SET
                    price = EXCLUDED.price,
                    indicators = EXCLUDED.indicators
                """,
                symbol, timestamp, interval_minutes, price, json.dumps(self.json_safe(indicators))
            )
            logger.debug(f"Inserted/updated intraday analytics for {symbol} at {timestamp} [{interval_minutes}min]")

    async def insert_daily_analytics(self, symbol, timestamp, price, indicators):
        """Insert daily technical analysis data"""
        pool = await self.get_pool()
        
        # Convert string timestamp to datetime
        if isinstance(timestamp, str):
            timestamp = dateutil.parser.isoparse(timestamp)
        elif isinstance(timestamp, (int, float)):
            timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        elif isinstance(timestamp, datetime):
            timestamp = timestamp
            
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO daily_analytics
                    (symbol, timestamp, price, indicators)
                VALUES
                    ($1, $2, $3, $4)
                ON CONFLICT (symbol, timestamp) DO UPDATE
                SET
                    price = EXCLUDED.price,
                    indicators = EXCLUDED.indicators
                """,
                symbol, timestamp, price, json.dumps(self.json_safe(indicators))
            )
            logger.debug(f"Inserted/updated daily analytics for {symbol} on {timestamp}")

    async def get_intraday_analytics(self, symbol, interval_minutes, indicators=None, from_timestamp=None, to_timestamp=None, limit=100):
        """Get intraday analytics data for a symbol"""
        pool = await self.get_pool()
        
        # Default to recent data if no range specified
        if not to_timestamp:
            to_timestamp = datetime.now(timezone.utc)
            
        # Convert timestamps to datetime if they're unix timestamps
        if isinstance(from_timestamp, (int, float)):
            from_timestamp = datetime.fromtimestamp(from_timestamp, tz=timezone.utc)
        if isinstance(to_timestamp, (int, float)):
            to_timestamp = datetime.fromtimestamp(to_timestamp, tz=timezone.utc)
            
        # Default indicators to retrieve
        if not indicators:
            indicators = ["sma_20", "ema_12", "rsi_14", "macd"]
            
        async with pool.acquire() as conn:
            # Build dynamic query based on requested indicators
            select_columns = ["symbol", "timestamp", "price"]
            select_columns.extend(indicators)
            
            query = f"""
                SELECT {', '.join(select_columns)}
                FROM intraday_analytics
                WHERE 
                    symbol = $1 AND
                    interval_minutes = $2
            """
            params = [symbol, interval_minutes]
            
            if from_timestamp:
                query += " AND timestamp >= $3"
                params.append(from_timestamp)
                
                if to_timestamp:
                    query += " AND timestamp <= $4"
                    params.append(to_timestamp)
            elif to_timestamp:
                query += " AND timestamp <= $3"
                params.append(to_timestamp)
                
            query += " ORDER BY timestamp DESC LIMIT $" + str(len(params) + 1)
            params.append(limit)
            
            rows = await conn.fetch(query, *params)
            
            result = []
            for row in rows:
                item = {
                    "symbol": row["symbol"],
                    "timestamp": row["timestamp"].isoformat(),
                    "price": float(row["price"])
                }
                
                # Add requested indicators
                for ind in indicators:
                    if ind in row and row[ind] is not None:
                        item[ind] = float(row[ind])
                    else:
                        item[ind] = None
                        
                result.append(item)
            
            return result
        
    @staticmethod
    def json_safe(obj):
        """Convert a dict with NumPy/special values to JSON-serializable dict"""
        if isinstance(obj, dict):
            return {k: AnalyticsRepository.json_safe(v) for k, v in obj.items()}
        elif hasattr(obj, 'item'):  # For NumPy types
            try:
                return obj.item()
            except (ValueError, AttributeError):
                return None
        elif isinstance(obj, (float, int)):
            try:
                # Check for NaN or Infinity
                if math.isnan(obj) or math.isinf(obj):
                    return None
                return obj
            except (TypeError, ValueError):
                return None
        elif obj is None:
            return None
        else:
            # Try conversion to normal Python type
            try:
                return float(obj) if isinstance(obj, (int, float)) else str(obj)
            except (ValueError, TypeError):
                return str(obj)