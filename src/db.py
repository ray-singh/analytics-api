import psycopg2
from psycopg2.extras import RealDictCursor
import os

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stocks")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")


def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    
    # Create tables with enhanced schema
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
        
        CREATE TABLE IF NOT EXISTS dead_letter_queue (
            id SERIAL PRIMARY KEY,
            original_data JSONB NOT NULL,
            issues JSONB NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            processed BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    ''')
    
    # Create indexes for better performance
    cur.execute('''
        CREATE INDEX IF NOT EXISTS idx_prices_symbol_timestamp ON prices(symbol, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON prices(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_analytics_symbol_type_timestamp ON analytics(symbol, analytics_type, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_analytics_timestamp ON analytics(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_dq_issues_symbol_severity ON data_quality_issues(symbol, severity);
        CREATE INDEX IF NOT EXISTS idx_dq_issues_timestamp ON data_quality_issues(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_dlq_processed ON dead_letter_queue(processed);
    ''')
    
    # Create TimescaleDB hypertables for time-series optimization
    try:
        cur.execute('''
            SELECT create_hypertable('prices', 'timestamp', if_not_exists => TRUE);
            SELECT create_hypertable('analytics', 'timestamp', if_not_exists => TRUE);
            SELECT create_hypertable('data_quality_issues', 'timestamp', if_not_exists => TRUE);
        ''')
    except psycopg2.Error as e:
        # TimescaleDB extension might not be available
        print(f"TimescaleDB hypertables not created: {e}")
    
    conn.commit()
    cur.close()
    conn.close()

def insert_price(symbol, price, timestamp, volume=None, source="simulation"):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO prices (symbol, price, volume, timestamp, source) VALUES (%s, %s, %s, to_timestamp(%s), %s)",
        (symbol, price, volume, timestamp, source)
    )
    conn.commit()
    cur.close()
    conn.close()

def insert_analytics(symbol, analytics_type, value, timestamp, window_size=None, metadata=None):
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
    
    # Get recent activity
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