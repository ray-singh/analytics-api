import asyncio
import asyncpg
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

class OHLCVRepository:
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
        """Initialize the database schema for OHLCV data"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            # Enable TimescaleDB extension if not already enabled
            await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
            
            # Create intraday OHLCV table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS intraday_ohlcv (
                    symbol TEXT NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    interval_minutes INTEGER NOT NULL,
                    open NUMERIC(12, 4) NOT NULL,
                    high NUMERIC(12, 4) NOT NULL,
                    low NUMERIC(12, 4) NOT NULL,
                    close NUMERIC(12, 4) NOT NULL,
                    volume BIGINT NOT NULL,
                    num_samples INTEGER DEFAULT 1,
                    source TEXT,
                    inserted_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (symbol, timestamp, interval_minutes)
                )
            """)
            
            # Add indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_intraday_ohlcv_symbol_time ON intraday_ohlcv (symbol, timestamp)")
            
            # Convert to hypertable if not already
            try:
                await conn.execute("""
                    SELECT create_hypertable('intraday_ohlcv', 'timestamp', 
                                          if_not_exists => TRUE)
                """)
            except Exception as e:
                logger.warning(f"Could not create hypertable for intraday_ohlcv: {e}")
                
            # Create compression policy
            try:
                await conn.execute("""
                    ALTER TABLE intraday_ohlcv SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'symbol,interval_minutes'
                    )
                """)
                
                # Add compression policy - compress data older than 7 days
                await conn.execute("""
                    SELECT add_compression_policy('intraday_ohlcv', 
                                              INTERVAL '7 days',
                                              if_not_exists => TRUE)
                """)
            except Exception as e:
                logger.warning(f"Could not set compression policy: {e}")
                
            # Create daily historical OHLCV table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS historical_ohlcv (
                    symbol TEXT NOT NULL,
                    timestamp DATE NOT NULL,
                    open NUMERIC(12, 4) NOT NULL,
                    high NUMERIC(12, 4) NOT NULL,
                    low NUMERIC(12, 4) NOT NULL,
                    close NUMERIC(12, 4) NOT NULL,
                    volume BIGINT NOT NULL,
                    source TEXT,
                    inserted_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (symbol, timestamp)
                )
            """)
            
            # Add indexes
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_historical_ohlcv_symbol ON historical_ohlcv (symbol)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_historical_ohlcv_timestamp ON historical_ohlcv (timestamp)")

    async def insert_intraday_ohlcv(self, symbol, timestamp, open_price, high, low, close, volume, interval_minutes, source=None, num_samples=1):
        """
        Insert or update intraday OHLCV data
        Uses ON CONFLICT to handle duplicates by updating with more accurate data
        """
        pool = await self.get_pool()
        
        # Convert timestamp to datetime if it's a unix timestamp
        if isinstance(timestamp, (int, float)):
            timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO intraday_ohlcv 
                    (symbol, timestamp, interval_minutes, open, high, low, close, volume, num_samples, source)
                VALUES 
                    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (symbol, timestamp, interval_minutes) DO UPDATE
                SET 
                    high = GREATEST(intraday_ohlcv.high, EXCLUDED.high),
                    low = LEAST(intraday_ohlcv.low, EXCLUDED.low),
                    close = EXCLUDED.close,
                    volume = CASE 
                        WHEN intraday_ohlcv.source = EXCLUDED.source THEN EXCLUDED.volume 
                        ELSE intraday_ohlcv.volume + EXCLUDED.volume
                    END,
                    num_samples = intraday_ohlcv.num_samples + EXCLUDED.num_samples,
                    source = EXCLUDED.source
                """,
                symbol, timestamp, interval_minutes, open_price, high, low, close, volume, num_samples, source
            )
            logger.debug(f"Inserted/updated intraday OHLCV for {symbol} at {timestamp} [{interval_minutes}min]")

    async def insert_historical_daily(self, symbol, timestamp, open_price, high, low, close, volume, source=None):
        """
        Insert or update daily historical OHLCV data
        Uses ON CONFLICT to handle duplicates
        """
        pool = await self.get_pool()
        
        # Convert timestamp to date if it's a unix timestamp
        if isinstance(timestamp, (int, float)):
            timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc).date()
        elif isinstance(timestamp, datetime):
            timestamp = timestamp.date()
            
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO historical_ohlcv 
                    (symbol, timestamp, open, high, low, close, volume, source)
                VALUES 
                    ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (symbol, timestamp) DO UPDATE
                SET 
                    open = EXCLUDED.open,
                    high = GREATEST(historical_ohlcv.high, EXCLUDED.high),
                    low = LEAST(historical_ohlcv.low, EXCLUDED.low),
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    source = EXCLUDED.source
                """,
                symbol, timestamp, open_price, high, low, close, volume, source
            )
            logger.debug(f"Inserted/updated daily OHLCV for {symbol} on {timestamp}")

    async def get_intraday_ohlcv(self, symbol, interval_minutes, from_timestamp=None, to_timestamp=None, limit=100):
        """Get intraday OHLCV data for a symbol within a time range"""
        pool = await self.get_pool()
        
        # Default to recent data if no range specified
        if not to_timestamp:
            to_timestamp = datetime.now(timezone.utc)
            
        # Convert timestamps to datetime if they're unix timestamps
        if isinstance(from_timestamp, (int, float)):
            from_timestamp = datetime.fromtimestamp(from_timestamp, tz=timezone.utc)
        if isinstance(to_timestamp, (int, float)):
            to_timestamp = datetime.fromtimestamp(to_timestamp, tz=timezone.utc)
            
        async with pool.acquire() as conn:
            query = """
                SELECT 
                    symbol, timestamp, 
                    open, high, low, close, volume
                FROM intraday_ohlcv
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
                result.append({
                    "symbol": row["symbol"],
                    "timestamp": row["timestamp"].isoformat(),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row["volume"])
                })
            
            return result
            
    async def get_daily_ohlcv(self, symbol, from_date=None, to_date=None, limit=100):
        """Get daily OHLCV data for a symbol within a date range"""
        pool = await self.get_pool()
        
        # Default to recent data if no range specified
        if not to_date:
            to_date = datetime.now(timezone.utc).date()
            
        # Convert dates to date objects if necessary
        if isinstance(from_date, datetime):
            from_date = from_date.date()
        if isinstance(to_date, datetime):
            to_date = to_date.date()
            
        async with pool.acquire() as conn:
            query = """
                SELECT 
                    symbol, timestamp, 
                    open, high, low, close, volume
                FROM historical_ohlcv
                WHERE symbol = $1
            """
            params = [symbol]
            
            if from_date:
                query += " AND timestamp >= $2"
                params.append(from_date)
                
                if to_date:
                    query += " AND timestamp <= $3"
                    params.append(to_date)
            elif to_date:
                query += " AND timestamp <= $2"
                params.append(to_date)
                
            query += " ORDER BY timestamp DESC LIMIT $" + str(len(params) + 1)
            params.append(limit)
            
            rows = await conn.fetch(query, *params)
            
            result = []
            for row in rows:
                result.append({
                    "symbol": row["symbol"],
                    "date": row["timestamp"].isoformat(),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row["volume"])
                })
            
            return result
            
    async def bulk_insert_intraday(self, bars):
        """
        Bulk insert multiple intraday OHLCV records
        
        Args:
            bars (list): List of bar dictionaries with keys:
                        symbol, timestamp, interval_minutes, open, high, low, close, volume, source
        """
        if not bars:
            return
            
        pool = await self.get_pool()
        records = []
        
        for bar in bars:
            # Convert timestamp to datetime if it's a unix timestamp
            timestamp = bar["timestamp"]
            if isinstance(timestamp, (int, float)):
                timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                
            records.append((
                bar["symbol"],
                timestamp,
                bar["interval_minutes"],
                bar["open"],
                bar["high"],
                bar["low"],
                bar["close"],
                bar["volume"],
                bar.get("num_samples", 1),
                bar.get("source")
            ))
            
        async with pool.acquire() as conn:
            # Using connection.executemany() is more efficient but doesn't support ON CONFLICT
            # For upserts with many records, create a temp table and do a single MERGE operation
            await conn.execute("""
                CREATE TEMP TABLE temp_bars (
                    symbol TEXT, timestamp TIMESTAMPTZ, interval_minutes INTEGER,
                    open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC,
                    volume BIGINT, num_samples INTEGER, source TEXT
                ) ON COMMIT DROP
            """)
            
            # Copy records to temp table
            await conn.copy_records_to_table(
                'temp_bars',
                records=records,
                columns=['symbol', 'timestamp', 'interval_minutes', 'open', 'high', 'low', 
                         'close', 'volume', 'num_samples', 'source']
            )
            
            # Perform the upsert from temp table to main table
            await conn.execute("""
                INSERT INTO intraday_ohlcv
                    (symbol, timestamp, interval_minutes, open, high, low, close, volume, num_samples, source)
                SELECT symbol, timestamp, interval_minutes, open, high, low, close, volume, num_samples, source
                FROM temp_bars
                ON CONFLICT (symbol, timestamp, interval_minutes) DO UPDATE
                SET 
                    high = GREATEST(intraday_ohlcv.high, EXCLUDED.high),
                    low = LEAST(intraday_ohlcv.low, EXCLUDED.low),
                    close = EXCLUDED.close,
                    volume = CASE 
                        WHEN intraday_ohlcv.source = EXCLUDED.source THEN EXCLUDED.volume 
                        ELSE intraday_ohlcv.volume + EXCLUDED.volume
                    END,
                    num_samples = intraday_ohlcv.num_samples + EXCLUDED.num_samples,
                    source = EXCLUDED.source
            """)
            
            logger.info(f"Bulk inserted {len(records)} intraday OHLCV bars")
            
    async def bulk_insert_daily(self, bars):
        """
        Bulk insert multiple daily OHLCV records
        
        Args:
            bars (list): List of bar dictionaries with keys:
                        symbol, date, open, high, low, close, volume, source
        """
        if not bars:
            return
            
        pool = await self.get_pool()
        records = []
        
        for bar in bars:
            # Convert timestamp/date to date if necessary
            date = bar.get("date") or bar.get("timestamp")
            if isinstance(date, (int, float)):
                date = datetime.fromtimestamp(date, tz=timezone.utc).date()
            elif isinstance(date, datetime):
                date = date.date()
                
            records.append((
                bar["symbol"],
                date,
                bar["open"],
                bar["high"],
                bar["low"],
                bar["close"],
                bar["volume"],
                bar.get("source")
            ))
            
        async with pool.acquire() as conn:
            # Similar bulk upsert pattern as for intraday data
            await conn.execute("""
                CREATE TEMP TABLE temp_daily (
                    symbol TEXT, date DATE,
                    open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC,
                    volume BIGINT, source TEXT
                ) ON COMMIT DROP
            """)
            
            # Copy records to temp table
            await conn.copy_records_to_table(
                'temp_daily',
                records=records,
                columns=['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'source']
            )
            
            # Perform the upsert from temp table to main table
            await conn.execute("""
                INSERT INTO historical_ohlcv
                    (symbol, timestamp, open, high, low, close, volume, source)
                SELECT symbol, date, open, high, low, close, volume, source
                FROM temp_daily
                ON CONFLICT (symbol, timestamp) DO UPDATE
                SET 
                    open = EXCLUDED.open,
                    high = GREATEST(historical_ohlcv.high, EXCLUDED.high),
                    low = LEAST(historical_ohlcv.low, EXCLUDED.low),
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    source = EXCLUDED.source
            """)
            
            logger.info(f"Bulk inserted {len(records)} daily OHLCV bars")
            
    async def consolidate_intraday_to_daily(self, symbols=None, date=None):
        """
        Consolidate intraday data to daily OHLCV
        
        Args:
            symbols (list): List of symbols to consolidate (None for all symbols)
            date (date/str): Date to consolidate (None for yesterday)
            
        Returns:
            int: Number of daily bars created
        """
        pool = await self.get_pool()
        
        # Default to yesterday if no date specified
        if not date:
            date = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        elif isinstance(date, str):
            date = datetime.strptime(date, "%Y-%m-%d").date()
            
        symbol_filter = ""
        params = [date]
        
        # Add symbol filter if specified
        if symbols:
            if isinstance(symbols, str):
                symbols = [symbols]
            symbol_filter = "AND symbol = ANY($2)"
            params.append(symbols)
            
        async with pool.acquire() as conn:
            # First check if data already exists
            query = f"""
                SELECT symbol, COUNT(*) as count
                FROM historical_ohlcv
                WHERE timestamp = $1 {symbol_filter}
                GROUP BY symbol
            """
            
            existing = await conn.fetch(query, *params)
            existing_symbols = {row["symbol"] for row in existing}
            
            # Then consolidate from intraday data
            query = f"""
                INSERT INTO historical_ohlcv (symbol, timestamp, open, high, low, close, volume, source)
                SELECT 
                    symbol,
                    $1::date as timestamp,
                    first(open, timestamp) as open,
                    max(high) as high,
                    min(low) as low,
                    last(close, timestamp) as close,
                    sum(volume) as volume,
                    'consolidated' as source
                FROM intraday_ohlcv
                WHERE 
                    timestamp::date = $1 {symbol_filter}
                    AND symbol NOT IN (
                        SELECT symbol FROM historical_ohlcv WHERE timestamp = $1
                    )
                GROUP BY symbol
                RETURNING symbol
            """
            
            result = await conn.fetch(query, *params)
            consolidated_symbols = [row["symbol"] for row in result]
            
            logger.info(f"Consolidated daily OHLCV for {len(consolidated_symbols)} symbols on {date}")
            return len(consolidated_symbols)
            
    async def get_data_coverage(self, symbol, interval_minutes=None):
        """
        Get data coverage information for a symbol
        
        Args:
            symbol (str): Symbol to check
            interval_minutes (int): Specific interval to check (None for all intervals)
            
        Returns:
            dict: Coverage information
        """
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            # Get intraday coverage
            intraday_query = """
                SELECT 
                    interval_minutes,
                    count(*) as bar_count,
                    min(timestamp) as oldest_bar,
                    max(timestamp) as newest_bar,
                    (max(timestamp) - min(timestamp)) as span
                FROM intraday_ohlcv
                WHERE symbol = $1
            """
            params = [symbol]
            
            if interval_minutes:
                intraday_query += " AND interval_minutes = $2"
                params.append(interval_minutes)
                
            intraday_query += " GROUP BY interval_minutes ORDER BY interval_minutes"
            
            intraday_rows = await conn.fetch(intraday_query, *params)
            
            # Get daily coverage
            daily_row = await conn.fetchrow(
                """
                SELECT 
                    count(*) as bar_count,
                    min(timestamp) as oldest_bar,
                    max(timestamp) as newest_bar,
                    (max(timestamp) - min(timestamp)) as span
                FROM historical_ohlcv
                WHERE symbol = $1
                """,
                symbol
            )
            
            # Format results
            result = {
                "symbol": symbol,
                "intraday": {},
                "daily": None
            }
            
            for row in intraday_rows:
                result["intraday"][f"{row['interval_minutes']}min"] = {
                    "bar_count": row["bar_count"],
                    "oldest": row["oldest_bar"].isoformat() if row["oldest_bar"] else None,
                    "newest": row["newest_bar"].isoformat() if row["newest_bar"] else None,
                    "span_days": row["span"].days if row["span"] else 0
                }
                
            if daily_row and daily_row["bar_count"] > 0:
                result["daily"] = {
                    "bar_count": daily_row["bar_count"],
                    "oldest": daily_row["oldest_bar"].isoformat() if daily_row["oldest_bar"] else None,
                    "newest": daily_row["newest_bar"].isoformat() if daily_row["newest_bar"] else None,
                    "span_days": daily_row["span"].days if daily_row["span"] else 0
                }
                
            return result
            
    async def detect_data_gaps(self, symbol, interval_minutes, start_date, end_date=None):
        """
        Detect gaps in OHLCV data
        
        Args:
            symbol (str): Symbol to check
            interval_minutes (int): Interval to check for gaps
            start_date (datetime/str): Start of period to check
            end_date (datetime/str): End of period to check (defaults to now)
            
        Returns:
            list: List of gap periods (start, end)
        """
        # Convert dates if necessary
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)
        if isinstance(end_date, str) and end_date:
            end_date = datetime.fromisoformat(end_date)
        elif not end_date:
            end_date = datetime.now(timezone.utc)
            
        # Make sure dates are timezone-aware
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
            
        # Get actual data coverage
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT timestamp
                FROM intraday_ohlcv
                WHERE 
                    symbol = $1 AND
                    interval_minutes = $2 AND
                    timestamp BETWEEN $3 AND $4
                ORDER BY timestamp
                """,
                symbol, interval_minutes, start_date, end_date
            )
            
            # Extract timestamps
            timestamps = [row["timestamp"] for row in rows]
            
            if not timestamps:
                # No data at all, report entire period as gap
                return [{
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat(),
                    "minutes": int((end_date - start_date).total_seconds() / 60)
                }]
                
            # Expected interval in minutes
            minutes_delta = timedelta(minutes=interval_minutes)
            
            # Find gaps
            gaps = []
            last_ts = None
            
            for ts in timestamps:
                if last_ts:
                    # Check if gap is larger than expected interval (with some tolerance)
                    gap = ts - last_ts
                    if gap > minutes_delta * 1.5:  # Allow 50% tolerance
                        gaps.append({
                            "start": (last_ts + minutes_delta).isoformat(),
                            "end": ts.isoformat(),
                            "minutes": int((ts - last_ts).total_seconds() / 60) - interval_minutes
                        })
                last_ts = ts
                
            # Check for gap at the beginning
            if timestamps and timestamps[0] - start_date > minutes_delta * 1.5:
                gaps.insert(0, {
                    "start": start_date.isoformat(),
                    "end": timestamps[0].isoformat(),
                    "minutes": int((timestamps[0] - start_date).total_seconds() / 60)
                })
                
            # Check for gap at the end
            if timestamps and end_date - timestamps[-1] > minutes_delta * 1.5:
                gaps.append({
                    "start": (timestamps[-1] + minutes_delta).isoformat(),
                    "end": end_date.isoformat(),
                    "minutes": int((end_date - timestamps[-1]).total_seconds() / 60) - interval_minutes
                })
                
            return gaps

    async def get_dataframe(self, symbol, interval_minutes=None, from_timestamp=None, to_timestamp=None, limit=1000):
        """
        Get OHLCV data as a pandas DataFrame (convenient for analysis)
        
        Args:
            symbol (str): Symbol to retrieve
            interval_minutes (int): Interval in minutes (None for daily data)
            from_timestamp: Start timestamp
            to_timestamp: End timestamp
            limit (int): Maximum number of records
            
        Returns:
            DataFrame: Pandas DataFrame with OHLCV data
        """
        if interval_minutes:
            # Get intraday data
            data = await self.get_intraday_ohlcv(
                symbol, interval_minutes, from_timestamp, to_timestamp, limit
            )
        else:
            # Get daily data
            data = await self.get_daily_ohlcv(
                symbol, from_timestamp, to_timestamp, limit
            )
            
        if not data:
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Set timestamp as index
        if 'timestamp' in df:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
        elif 'date' in df:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
        # Sort by timestamp
        df.sort_index(inplace=True)
        
        return df
    
    async def get_latest_intraday(self, symbol, interval_minutes):
        """Get the latest intraday OHLCV record for a symbol and interval"""
        conn = await self.get_connection()
        try:
            query = """
                SELECT * FROM intraday_ohlcv
                WHERE symbol = $1 AND interval_minutes = $2
                ORDER BY timestamp DESC
                LIMIT 1
            """
            record = await conn.fetchrow(query, symbol, interval_minutes)
            return record
        finally:
            await conn.close()
    
    async def get_latest_daily(self, symbol):
        """Get the latest daily OHLCV record for a symbol"""
        conn = await self.get_connection()
        try:
            query = """
                SELECT * FROM historical_ohlcv
                WHERE symbol = $1
                ORDER BY timestamp DESC
                LIMIT 1
            """
            record = await conn.fetchrow(query, symbol)
            return record
        finally:
            await conn.close()
    