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

# Database connection configuration
# Can be overridden via environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stocks")
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
            CREATE TABLE IF NOT EXISTS prices (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                volume INTEGER,
                timestamp TIMESTAMPTZ NOT NULL,
                source TEXT DEFAULT 'simulation',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        ''')
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
        print("Creating hypertable for prices...")
        cur.execute("ALTER TABLE prices DROP CONSTRAINT IF EXISTS prices_pkey;")
        cur.execute("ALTER TABLE prices ADD PRIMARY KEY (id, timestamp);")
        cur.execute("SELECT create_hypertable('prices', 'timestamp', if_not_exists => TRUE);")

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
            CREATE INDEX IF NOT EXISTS idx_prices_symbol_timestamp ON prices(symbol, timestamp DESC);
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

def insert_price(symbol, price, timestamp, volume=None, source="simulation"):
    """
    Insert a new stock price record into the database.
    
    Args:
        symbol (str): Stock symbol (e.g., 'AAPL', 'MSFT')
        price (float): Current stock price
        timestamp (float): Unix timestamp when price was recorded
        volume (int, optional): Trading volume
        source (str): Data source (default: 'simulation')
    """
    conn = get_conn()
    cur = conn.cursor()
    
    # Check which columns exist in the prices table
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'prices'
        ORDER BY column_name
    """)
    
    existing_columns = [row[0] for row in cur.fetchall()]
    print(f"Existing columns in prices table: {existing_columns}")
    
    # Build dynamic INSERT query based on available columns
    columns = ['symbol', 'price', 'timestamp']
    values = [symbol, price, timestamp]
    
    if 'volume' in existing_columns:
        columns.append('volume')
        values.append(volume)
    
    if 'source' in existing_columns:
        columns.append('source')
        values.append(source)
    
    # Create the INSERT query
    column_list = ', '.join(columns)
    placeholders = ', '.join(['%s' if col != 'timestamp' else 'to_timestamp(%s)' for col in columns])

    query = f"INSERT INTO prices ({column_list}) VALUES ({placeholders})"

    print(f"Executing query: {query}")
    print(f"With values: {values}")

    cur.execute(query, values)
    conn.commit()
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
        
        print("Dropped existing tables")
        
        # Recreate tables
        
    except Exception as e:
        print(f"Error recreating schema: {e}")
        conn.rollback()
    finally:
        conn.close()