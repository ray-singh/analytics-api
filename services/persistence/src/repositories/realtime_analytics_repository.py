import os
import logging
import asyncpg
from datetime import datetime, timezone
import dateutil.parser

logger = logging.getLogger(__name__)

class RealTimeAnalyticsRepository:
    """Repository for real-time analytics data"""
    
    def __init__(self, db_url=None):
        """Initialize the repository with a database connection"""
        self.db_url = db_url or os.getenv("DATABASE_URL", "postgresql://postgres:password@timescaledb:5432/stockanalytics")
        self._pool = None
    
    async def get_pool(self):
        """Get or create the connection pool"""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self.db_url)
        return self._pool
    
    async def insert_realtime_analytics(self, symbol, timestamp, price, indicators):
        """Insert real-time analytics data into the database"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            # Convert timestamp string to datetime if needed
            if isinstance(timestamp, str):
                timestamp = dateutil.parser.isoparse(timestamp)

            # Build dynamic query based on available indicators
            columns = ["symbol", "timestamp", "price"]
            values = [symbol, timestamp, price]
            placeholders = ["$1", "$2", "$3"]
            
            # Add indicators that are present
            idx = 4
            for key, value in indicators.items():
                if value is not None:
                    columns.append(key)
                    values.append(value)
                    placeholders.append(f"${idx}")
                    idx += 1
            
            # Build the query
            query = f"""
                INSERT INTO realtime_analytics 
                ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT (symbol, timestamp)
                DO UPDATE SET
                price = EXCLUDED.price
                {"".join([f", {col} = EXCLUDED.{col}" for col in columns[3:]])}
                RETURNING id
            """
            
            try:
                record_id = await conn.fetchval(query, *values)
                return record_id
            except Exception as e:
                logger.error(f"Error inserting real-time analytics: {e}")
                raise

    async def init_db(self):
        """Initialize the database schema for real-time analytics"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
            
            # Create realtime_analytics table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS realtime_analytics (
                    id SERIAL,  
                    symbol VARCHAR(20) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    price NUMERIC(19,4) NOT NULL,
                    
                    price_velocity_10 NUMERIC(19,4),
                    price_velocity_50 NUMERIC(19,4),
                    tick_ratio NUMERIC(19,4),
                    tick_vwap_20 NUMERIC(19,4),
                    volume_momentum NUMERIC(19,4),
                    bb_bandwidth NUMERIC(19,4),
                    bb_position NUMERIC(19,4),
                    tick_rsi_14 NUMERIC(19,4),
                    
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (id, timestamp),
                    CONSTRAINT unique_realtime_analytics UNIQUE (symbol, timestamp)
                )
            """)
            
            # Convert to hypertable if not already
            try:
                await conn.execute("""
                    SELECT create_hypertable('realtime_analytics', 'timestamp', 
                                            if_not_exists => TRUE,
                                            chunk_time_interval => INTERVAL '1 day')
                """)
            except Exception as e:
                logger.warning(f"Could not create hypertable for realtime_analytics: {e}")
            
            # Add retention policy - keep for 1 hour
            try:
                await conn.execute("""
                    SELECT add_retention_policy('realtime_analytics', 
                                               INTERVAL '1 hour',
                                               if_not_exists => TRUE)
                """)
            except Exception as e:
                logger.warning(f"Could not set retention policy: {e}")