"""
Database module for the event-driven stock analytics system.

This module provides database operations for storing and retrieving:
- Raw stock price data
- Computed analytics (moving averages, etc.)
- Data quality issues and alerts
- Dead letter queue for failed events

Uses TimescaleDB (PostgreSQL extension) for time-series optimization.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime, timedelta

# Database connection configuration
# Can be overridden via environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")


def get_conn():
    """
    Create and return a database connection.
    
    Returns:
        psycopg2.connection: Database connection object
    """
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def init_db():
    """
    Initialize the database schema, handle migrations, and create TimescaleDB hypertables.
    """
    conn = get_conn()
    cur = conn.cursor()

    # Create tables
    try:
        print("Creating tables...")
        cur.execute('''
            CREATE TABLE IF NOT EXISTS analytics (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                analytics_type TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                window_size INTEGER,
                timestamp TIMESTAMPTZ NOT NULL,
                metadata JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS data_quality_issues (
                id SERIAL PRIMARY KEY,
                symbol TEXT,
                issue_type TEXT NOT NULL,
                description TEXT NOT NULL,
                severity TEXT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                metadata JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS dead_letter_queue (
                id SERIAL PRIMARY KEY,
                original_data JSONB NOT NULL,
                issues JSONB NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                processed BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        ''')
        conn.commit()
        print("Tables created successfully.")
    except Exception as e:
        print(f"Error creating tables: {e}")
        conn.rollback()

    # Create TimescaleDB hypertables
    try:
        print("Creating hypertable for analytics...")
        cur.execute("ALTER TABLE analytics DROP CONSTRAINT IF EXISTS analytics_pkey;")
        cur.execute("ALTER TABLE analytics ADD PRIMARY KEY (id, timestamp);")
        cur.execute("SELECT create_hypertable('analytics', 'timestamp', if_not_exists => TRUE);")

        print("Creating hypertable for data_quality_issues...")
        cur.execute("ALTER TABLE data_quality_issues DROP CONSTRAINT IF EXISTS data_quality_issues_pkey;")
        cur.execute("ALTER TABLE data_quality_issues ADD PRIMARY KEY (id, timestamp);")
        cur.execute("SELECT create_hypertable('data_quality_issues', 'timestamp', if_not_exists => TRUE);")
        conn.commit()
        print("Hypertables created successfully.")
    except psycopg2.Error as e:
        print(f"TimescaleDB hypertables not created: {e}")
        conn.rollback()

    # Create indexes
    try:
        print("Creating indexes...")
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_analytics_symbol_type_timestamp ON analytics(symbol, analytics_type, timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_dq_issues_symbol_severity ON data_quality_issues(symbol, severity);
        ''')
        conn.commit()
        print("Indexes created successfully.")
    except Exception as e:
        print(f"Error creating indexes: {e}")
        conn.rollback()

    cur.close()
    conn.close()
    init_tiered_tables() 

def init_tiered_tables():
    conn = get_conn()
    cur = conn.cursor()

    try:
        print("Creating three-tier data architecture tables...")
        
        # 1. Real-time prices table (volatile)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS realtime_prices (
                id SERIAL,
                symbol TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                volume BIGINT,
                timestamp TIMESTAMPTZ NOT NULL,
                source TEXT DEFAULT 'TwelveData',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        ''')
        
        # 2. Intraday OHLCV table (non-volatile)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS intraday_ohlcv (
                id SERIAL,
                symbol TEXT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                interval_minutes INTEGER NOT NULL DEFAULT 5,
                open DOUBLE PRECISION NOT NULL,
                high DOUBLE PRECISION NOT NULL,
                low DOUBLE PRECISION NOT NULL,
                close DOUBLE PRECISION NOT NULL,
                volume BIGINT,
                num_samples INTEGER DEFAULT 1,
                last_updated TIMESTAMPTZ DEFAULT NOW(),
                source TEXT DEFAULT 'Consolidated'
            );
        ''')
        
        # 3. Historical OHLCV table (non-volatile)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS historical_ohlcv (
                id SERIAL,
                symbol TEXT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                open DOUBLE PRECISION NOT NULL,
                high DOUBLE PRECISION NOT NULL,
                low DOUBLE PRECISION NOT NULL,
                close DOUBLE PRECISION NOT NULL,
                volume BIGINT,
                split_coefficient DOUBLE PRECISION DEFAULT 1,
                source TEXT DEFAULT 'AlphaVantage',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        ''')

        # Create hypertables
        print("Creating hypertables...")
        
        cur.execute("ALTER TABLE realtime_prices DROP CONSTRAINT IF EXISTS realtime_prices_pkey;")
        cur.execute("ALTER TABLE realtime_prices ADD PRIMARY KEY (id, timestamp);")
        cur.execute("SELECT create_hypertable('realtime_prices', 'timestamp', if_not_exists => TRUE);")
        
        cur.execute("ALTER TABLE intraday_ohlcv DROP CONSTRAINT IF EXISTS intraday_ohlcv_pkey;")
        cur.execute("ALTER TABLE intraday_ohlcv ADD PRIMARY KEY (symbol, timestamp, interval_minutes);")
        cur.execute("SELECT create_hypertable('intraday_ohlcv', 'timestamp', if_not_exists => TRUE);")
        
        cur.execute("ALTER TABLE historical_ohlcv DROP CONSTRAINT IF EXISTS historical_ohlcv_pkey;")
        cur.execute("ALTER TABLE historical_ohlcv ADD PRIMARY KEY (symbol, timestamp);")
        cur.execute("SELECT create_hypertable('historical_ohlcv', 'timestamp', if_not_exists => TRUE);")
        conn.commit()

        # Create indexes
        print("Creating indexes...")
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_realtime_symbol_timestamp 
            ON realtime_prices(symbol, timestamp DESC);
        ''')
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_intraday_symbol_timestamp 
            ON intraday_ohlcv(symbol, timestamp DESC);
        ''')
        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_historical_symbol_timestamp 
            ON historical_ohlcv(symbol, timestamp DESC);
        ''')
        conn.commit()

        # Retention policies
        cur.execute('''
            SELECT add_retention_policy('realtime_prices', INTERVAL '3 hours', if_not_exists => TRUE);
        ''')
        cur.execute('''
            SELECT add_retention_policy('intraday_ohlcv', INTERVAL '2 years', if_not_exists => TRUE);
        ''')
        conn.commit()

        print("Three-tier data architecture created successfully.")

    except Exception as e:
        print(f"Error creating tiered tables: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def insert_analytics(symbol, analytics_type, value, timestamp, window_size=None, metadata=None):
    """
    Insert a new analytics record into the database.
    
    Args:
        symbol (str): Stock symbol (e.g., 'AAPL', 'MSFT')
        analytics_type (str): Type of analytics (e.g., 'moving_average', 'volatility')
        value (float): Computed analytics value
        timestamp (float): Unix timestamp when analytics was computed
        window_size (int, optional): Window size used for calculation (e.g., 5 for 5-period MA)
        metadata (dict, optional): Additional analytics data as JSON
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO analytics (symbol, analytics_type, value, window_size, timestamp, metadata) VALUES (%s, %s, %s, %s, to_timestamp(%s), %s)",
        (symbol, analytics_type, value, window_size, timestamp, metadata)
    )
    conn.commit()
    cur.close()
    conn.close()

def insert_data_quality_issue(symbol, issue_type, description, severity, timestamp, metadata=None):
    """
    Insert a data quality issue or alert into the database.
    
    Args:
        symbol (str, optional): Stock symbol if issue is symbol-specific
        issue_type (str): Type of issue (e.g., 'missing_data', 'outlier', 'threshold_breach')
        description (str): Human-readable description of the issue
        severity (str): Issue severity ('low', 'medium', 'high', 'critical')
        timestamp (float): Unix timestamp when issue occurred
        metadata (dict, optional): Additional issue details as JSON
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO data_quality_issues (symbol, issue_type, description, severity, timestamp, metadata) VALUES (%s, %s, %s, %s, to_timestamp(%s), %s)",
        (symbol, issue_type, description, severity, timestamp, metadata)
    )
    conn.commit()
    cur.close()
    conn.close()

def get_latest_analytics(symbol, analytics_type=None):
    """
    Get the most recent analytics for a given symbol.
    
    Args:
        symbol (str): Stock symbol to query
        analytics_type (str, optional): Specific analytics type to filter by
        
    Returns:
        dict or None: Latest analytics record as dictionary, or None if not found
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    if analytics_type:
        cur.execute(
            "SELECT * FROM analytics WHERE symbol=%s AND analytics_type=%s ORDER BY timestamp DESC LIMIT 1",
            (symbol, analytics_type)
        )
    else:
        cur.execute(
            "SELECT * FROM analytics WHERE symbol=%s ORDER BY timestamp DESC LIMIT 1",
            (symbol,)
        )
    
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

def get_analytics_history(symbol, analytics_type=None, limit=100):
    """
    Get historical analytics for a given symbol.
    
    Args:
        symbol (str): Stock symbol to query
        analytics_type (str, optional): Specific analytics type to filter by
        limit (int): Maximum number of records to return (default: 100)
        
    Returns:
        list: List of analytics records as dictionaries, ordered by timestamp DESC
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    if analytics_type:
        cur.execute(
            "SELECT * FROM analytics WHERE symbol=%s AND analytics_type=%s ORDER BY timestamp DESC LIMIT %s",
            (symbol, analytics_type, limit)
        )
    else:
        cur.execute(
            "SELECT * FROM analytics WHERE symbol=%s ORDER BY timestamp DESC LIMIT %s",
            (symbol, limit)
        )
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def get_price_history(symbol, limit=100):
    """
    Get historical price data for a given symbol.
    
    Args:
        symbol (str): Stock symbol to query
        limit (int): Maximum number of records to return (default: 100)
        
    Returns:
        list: List of price records as dictionaries, ordered by timestamp DESC
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "SELECT * FROM prices WHERE symbol=%s ORDER BY timestamp DESC LIMIT %s",
        (symbol, limit)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def get_data_quality_issues(symbol=None, severity=None, limit=100):
    """
    Get data quality issues and alerts with optional filtering.
    
    Args:
        symbol (str, optional): Filter by specific stock symbol
        severity (str, optional): Filter by severity level ('low', 'medium', 'high', 'critical')
        limit (int): Maximum number of records to return (default: 100)
        
    Returns:
        list: List of data quality issue records as dictionaries, ordered by timestamp DESC
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    if symbol and severity:
        cur.execute(
            "SELECT * FROM data_quality_issues WHERE symbol=%s AND severity=%s ORDER BY timestamp DESC LIMIT %s",
            (symbol, severity, limit)
        )
    elif symbol:
        cur.execute(
            "SELECT * FROM data_quality_issues WHERE symbol=%s ORDER BY timestamp DESC LIMIT %s",
            (symbol, limit)
        )
    elif severity:
        cur.execute(
            "SELECT * FROM data_quality_issues WHERE severity=%s ORDER BY timestamp DESC LIMIT %s",
            (severity, limit)
        )
    else:
        cur.execute(
            "SELECT * FROM data_quality_issues ORDER BY timestamp DESC LIMIT %s",
            (limit,)
        )
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def get_pipeline_metrics():
    """
    Get overall pipeline metrics and statistics.
    
    Returns:
        dict: Dictionary containing:
            - total_prices: Total number of price records
            - total_analytics: Total number of analytics records
            - total_issues: Total number of data quality issues
            - total_dlq: Total number of dead letter queue items
            - recent_prices: Number of price records in last hour
            - recent_analytics: Number of analytics records in last hour
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get counts for each table
    cur.execute("SELECT COUNT(*) as total_prices FROM prices")
    total_prices = cur.fetchone()['total_prices']
    
    cur.execute("SELECT COUNT(*) as total_analytics FROM analytics")
    total_analytics = cur.fetchone()['total_analytics']
    
    cur.execute("SELECT COUNT(*) as total_issues FROM data_quality_issues")
    total_issues = cur.fetchone()['total_issues']
    
    cur.execute("SELECT COUNT(*) as total_dlq FROM dead_letter_queue")
    total_dlq = cur.fetchone()['total_dlq']
    
    # Get recent activity (last hour)
    cur.execute("SELECT COUNT(*) as recent_prices FROM prices WHERE timestamp > NOW() - INTERVAL '1 hour'")
    recent_prices = cur.fetchone()['recent_prices']
    
    cur.execute("SELECT COUNT(*) as recent_analytics FROM analytics WHERE timestamp > NOW() - INTERVAL '1 hour'")
    recent_analytics = cur.fetchone()['recent_analytics']
    
    cur.close()
    conn.close()
    
    return {
        'total_prices': total_prices,
        'total_analytics': total_analytics,
        'total_issues': total_issues,
        'total_dlq': total_dlq,
        'recent_prices': recent_prices,
        'recent_analytics': recent_analytics
    } 

def get_ohlc_history(symbol, start_timestamp=None, end_timestamp=None, limit=100):
    """
    Get historical OHLC data for a given symbol and time range.
    
    Args:
        symbol (str): Stock symbol to query
        start_timestamp (int, optional): Start of date range as Unix timestamp
        end_timestamp (int, optional): End of date range as Unix timestamp
        limit (int): Maximum number of records to return
        
    Returns:
        list: List of OHLC records as dictionaries, ordered by timestamp DESC
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = "SELECT * FROM historical_ohlc WHERE symbol=%s"
    params = [symbol]
    
    if start_timestamp:
        query += " AND timestamp >= to_timestamp(%s)"
        params.append(start_timestamp)
        
    if end_timestamp:
        query += " AND timestamp <= to_timestamp(%s)"
        params.append(end_timestamp)
        
    query += " ORDER BY timestamp DESC LIMIT %s"
    params.append(limit)
    
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def recreate_db_schema():
    """
    Drop and recreate all tables. Use with caution - this will delete all data!
    """
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Drop existing tables
        cur.execute("DROP TABLE IF EXISTS analytics CASCADE")
        cur.execute("DROP TABLE IF EXISTS data_quality_issues CASCADE")
        cur.execute("DROP TABLE IF EXISTS dead_letter_queue CASCADE")
        cur.execute("DROP TABLE IF EXISTS prices CASCADE")
        cur.execute("DROP TABLE IF EXISTS historical_ohlc CASCADE")
        cur.execute("DROP TABLE IF EXISTS intraday_ohlcv CASCADE")
        cur.execute("DROP TABLE IF EXISTS realtime_prices CASCADE")
        conn.commit()
        print("Dropped existing tables")
            
    except Exception as e:
        print(f"Error recreating schema: {e}")
        conn.rollback()
    finally:
        conn.close()

def insert_realtime_price(symbol, price, timestamp, volume=None, source="TwelveData"):
    """
    Insert a new real-time price record into the volatile realtime_prices table.
    
    Args:
        symbol (str): Stock symbol (e.g., 'AAPL', 'MSFT')
        price (float): Current stock price
        timestamp (float): Unix timestamp when price was recorded
        volume (int, optional): Trading volume
        source (str): Data source (default: 'TwelveData')
    """
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        cur.execute(
            "INSERT INTO realtime_prices (symbol, price, volume, timestamp, source) VALUES (%s, %s, %s, to_timestamp(%s), %s)",
            (symbol, price, volume, timestamp, source)
        )
        conn.commit()
    except Exception as e:
        print(f"Error inserting real-time price: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def insert_intraday_ohlcv(symbol, timestamp, open_price, high, low, close, volume=None, interval_minutes=5, source="AlphaVantage"):
    """
    Insert an intraday OHLCV record into the intraday_ohlcv table.
    
    Args:
        symbol (str): Stock symbol (e.g., 'AAPL', 'MSFT')
        timestamp (float): Unix timestamp for the start of the interval
        open_price (float): Opening price for the interval
        high (float): Highest price during the interval
        low (float): Lowest price during the interval
        close (float): Closing price for the interval
        volume (int, optional): Trading volume during the interval
        interval_minutes (int): Interval duration in minutes (default: 5)
        source (str): Data source (default: 'AlphaVantage')
    """
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Round timestamp to the nearest interval
        # This ensures we don't create duplicate intervals with slightly different timestamps
        query = """
            INSERT INTO intraday_ohlcv 
            (symbol, timestamp, interval_minutes, open, high, low, close, volume, source, last_updated) 
            VALUES (%s, date_trunc('minute', to_timestamp(%s) - 
                   (EXTRACT(MINUTE FROM to_timestamp(%s))::integer %% %s) * interval '1 minute'), 
                   %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (symbol, timestamp, interval_minutes)
            DO UPDATE SET 
                high = GREATEST(intraday_ohlcv.high, EXCLUDED.high),
                low = LEAST(intraday_ohlcv.low, EXCLUDED.low),
                close = EXCLUDED.close,
                volume = COALESCE(intraday_ohlcv.volume, 0) + COALESCE(EXCLUDED.volume, 0),
                num_samples = intraday_ohlcv.num_samples + 1,
                last_updated = NOW()
        """
        
        cur.execute(query, (symbol, timestamp, timestamp, interval_minutes, 
                           interval_minutes, open_price, high, low, close, 
                           volume, source))
        conn.commit()
    except Exception as e:
        print(f"Error inserting intraday OHLCV: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def insert_historical_daily(symbol, timestamp, open_price, high, low, close, volume=None, 
                           adjusted_close=None, dividend_amount=0, split_coefficient=1, source="AlphaVantage"):
    """
    Insert a historical daily OHLCV record into the historical_ohlcv table.
    
    Args:
        symbol (str): Stock symbol (e.g., 'AAPL', 'MSFT')
        timestamp (float): Unix timestamp for the day
        open_price (float): Opening price for the day
        high (float): Highest price during the day
        low (float): Lowest price during the day
        close (float): Closing price for the day
        volume (int, optional): Trading volume for the day
        adjusted_close (float, optional): Close price adjusted for dividends/splits
        dividend_amount (float): Dividend amount if applicable
        split_coefficient (float): Split coefficient if applicable
        source (str): Data source (default: 'AlphaVantage')
    """
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Round timestamp to start of day
        query = """
            INSERT INTO historical_ohlcv 
            (symbol, timestamp, open, high, low, close, volume, 
             adjusted_close, dividend_amount, split_coefficient, source) 
            VALUES (%s, date_trunc('day', to_timestamp(%s)), %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp)
            DO UPDATE SET 
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                adjusted_close = EXCLUDED.adjusted_close,
                dividend_amount = EXCLUDED.dividend_amount,
                split_coefficient = EXCLUDED.split_coefficient,
                source = EXCLUDED.source
        """
        
        cur.execute(query, (symbol, timestamp, open_price, high, low, close, 
                           volume, adjusted_close or close, dividend_amount, 
                           split_coefficient, source))
        conn.commit()
    except Exception as e:
        print(f"Error inserting historical daily data: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def consolidate_realtime_to_intraday(symbol=None, older_than_minutes=15):
    """
    Consolidate real-time price data into 5-minute OHLCV intervals in the intraday table,
    and optionally clean up older real-time data.
    
    Args:
        symbol (str, optional): Only process specific symbol
        older_than_minutes (int): Only process data older than this many minutes
        
    Returns:
        int: Number of intervals consolidated
    """
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Construct base query and params
        query_params = []
        
        # Get the cutoff timestamp
        cutoff_time = datetime.now() - timedelta(minutes=older_than_minutes)
        
        # Find the time buckets that need to be consolidated
        query = """
        WITH time_buckets AS (
            SELECT 
                symbol,
                date_trunc('minute', timestamp - 
                (EXTRACT(MINUTE FROM timestamp)::integer % 5) * interval '1 minute') AS interval_start,
                MIN(price) AS low,
                MAX(price) AS high,
                FIRST_VALUE(price) OVER (PARTITION BY symbol, 
                  date_trunc('minute', timestamp - 
                  (EXTRACT(MINUTE FROM timestamp)::integer % 5) * interval '1 minute')
                  ORDER BY timestamp) AS open,
                LAST_VALUE(price) OVER (PARTITION BY symbol, 
                  date_trunc('minute', timestamp - 
                  (EXTRACT(MINUTE FROM timestamp)::integer % 5) * interval '1 minute')
                  ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close,
                SUM(volume) AS volume,
                COUNT(*) AS num_samples
            FROM realtime_prices
            WHERE timestamp < %s
        """
        query_params.append(cutoff_time)
        
        if symbol:
            query += " AND symbol = %s"
            query_params.append(symbol)
        
        query += """
            GROUP BY symbol, interval_start
        )
        INSERT INTO intraday_ohlcv 
            (symbol, timestamp, interval_minutes, open, high, low, close, volume, num_samples, source)
        SELECT 
            symbol, interval_start, 5, open, high, low, close, volume, num_samples, 'Consolidated'
        FROM time_buckets
        ON CONFLICT (symbol, timestamp, interval_minutes)
        DO UPDATE SET
            high = GREATEST(intraday_ohlcv.high, EXCLUDED.high),
            low = LEAST(intraday_ohlcv.low, EXCLUDED.low),
            close = EXCLUDED.close,
            volume = COALESCE(intraday_ohlcv.volume, 0) + COALESCE(EXCLUDED.volume, 0),
            num_samples = intraday_ohlcv.num_samples + EXCLUDED.num_samples,
            last_updated = NOW()
        RETURNING symbol, timestamp
        """
        
        cur.execute(query, query_params)
        consolidated = cur.fetchall()
        
        # Delete consolidated data from realtime table
        if consolidated:
            # Build a query to delete the consolidated data
            placeholders = ', '.join(['%s'] * len(consolidated))
            symbols = [row[0] for row in consolidated]
            timestamps = [row[1] for row in consolidated]
            
            # For each consolidated interval, delete the corresponding realtime data
            for i in range(len(consolidated)):
                symbol = consolidated[i][0]
                interval_start = consolidated[i][1]
                interval_end = interval_start + timedelta(minutes=5)
                
                cur.execute(
                    "DELETE FROM realtime_prices WHERE symbol = %s AND timestamp >= %s AND timestamp < %s",
                    (symbol, interval_start, interval_end)
                )
                
        conn.commit()
        return len(consolidated)
    
    except Exception as e:
        print(f"Error consolidating realtime data: {e}")
        conn.rollback()
        return 0
    finally:
        cur.close()
        conn.close()

def consolidate_intraday_to_daily(symbol=None, days_old=1):
    """
    Consolidate intraday OHLCV data into daily OHLCV records in the historical table
    for data older than specified days.
    
    Args:
        symbol (str, optional): Only process specific symbol
        days_old (int): Only process data older than this many days
        
    Returns:
        int: Number of days consolidated
    """
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Get the cutoff timestamp
        cutoff_time = datetime.now() - timedelta(days=days_old)
        
        # Construct base query and params
        query_params = [cutoff_time]
        
        # Find daily data to consolidate
        query = """
        WITH daily_data AS (
            SELECT 
                symbol,
                date_trunc('day', timestamp) AS day_start,
                FIRST_VALUE(open) OVER (PARTITION BY symbol, date_trunc('day', timestamp) 
                                      ORDER BY timestamp) AS open_price,
                MAX(high) AS high,
                MIN(low) AS low,
                LAST_VALUE(close) OVER (PARTITION BY symbol, date_trunc('day', timestamp) 
                                      ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close_price,
                SUM(volume) AS volume
            FROM intraday_ohlcv
            WHERE timestamp < %s
        """
        
        if symbol:
            query += " AND symbol = %s"
            query_params.append(symbol)
        
        query += """
            GROUP BY symbol, day_start
        )
        INSERT INTO historical_ohlcv 
            (symbol, timestamp, open, high, low, close, volume, adjusted_close, source)
        SELECT 
            symbol, day_start, open_price, high, low, close_price, volume, close_price, 'Consolidated'
        FROM daily_data
        ON CONFLICT (symbol, timestamp)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = GREATEST(historical_ohlcv.high, EXCLUDED.high),
            low = LEAST(historical_ohlcv.low, EXCLUDED.low),
            close = EXCLUDED.close,
            volume = COALESCE(historical_ohlcv.volume, 0) + COALESCE(EXCLUDED.volume, 0)
        RETURNING symbol, timestamp
        """
        
        cur.execute(query, query_params)
        consolidated = cur.fetchall()
        
        # We don't delete intraday data as it's kept for 2 years according to retention policy
        
        conn.commit()
        return len(consolidated)
    
    except Exception as e:
        print(f"Error consolidating intraday data: {e}")
        conn.rollback()
        return 0
    finally:
        cur.close()
        conn.close()

def clean_realtime_data(idle_minutes=60):
    """
    Clean up the realtime_prices table when the API has been idle.
    
    Args:
        idle_minutes (int): Clean if no new data for this many minutes
        
    Returns:
        bool: True if data was cleaned, False otherwise
    """
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Check last entry timestamp
        cur.execute("SELECT MAX(timestamp) FROM realtime_prices")
        last_timestamp = cur.fetchone()[0]
        
        # If no data or last data is older than idle threshold, clean up
        if not last_timestamp or datetime.now() - last_timestamp > timedelta(minutes=idle_minutes):
            # First ensure data is consolidated
            consolidate_realtime_to_intraday()
            
            # Then clean the table
            cur.execute("TRUNCATE TABLE realtime_prices")
            conn.commit()
            print(f"Realtime table cleaned due to {idle_minutes} minutes of inactivity")
            return True
        
        return False
    
    except Exception as e:
        print(f"Error cleaning realtime data: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()