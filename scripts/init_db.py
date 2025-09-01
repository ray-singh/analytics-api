"""
Initialize the TimescaleDB database with required tables and hypertables
Usage: python -m scripts.init_db [--drop-existing]
"""

import argparse
import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()
DEFAULT_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/stockanalytics")

def add_retention_policy_if_not_exists(cur, table, interval):
    cur.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM timescaledb_information.jobs j
                JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id
                WHERE j.proc_name = 'policy_retention'
                  AND js.hypertable_name = '{table}'
            ) THEN
                PERFORM add_retention_policy('{table}', INTERVAL '{interval}');
            END IF;
        END$$;
    """)

def init_database(db_url, drop_existing=False):
    """Initialize database schema"""
    conn = None
    try:
        # Connect to database
        print(f"Connecting to database: {db_url}")
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur = conn.cursor()
        
        if drop_existing:
            print("Dropping existing tables...")
            print("hehe")
            
            # Drop tables individually, starting with views/materialized views
            cur.execute("DROP MATERIALIZED VIEW IF EXISTS realtime_to_intraday_5min CASCADE;")
            cur.execute("DROP MATERIALIZED VIEW IF EXISTS intraday_to_daily CASCADE;")
            
            # Remove retention and compression policies
            tables = ["realtime_prices", "intraday_ohlcv", "historical_ohlcv"]
            for table in tables:
                try:
                    cur.execute(f"SELECT remove_retention_policy('{table}', if_exists => TRUE);")
                except:
                    pass
                try:
                    cur.execute(f"SELECT remove_compression_policy('{table}', if_exists => TRUE);")
                except:
                    pass
            
            # Drop hypertables one by one
            cur.execute("DROP TABLE IF EXISTS realtime_prices CASCADE;")
            cur.execute("DROP TABLE IF EXISTS intraday_ohlcv CASCADE;")
            cur.execute("DROP TABLE IF EXISTS historical_ohlcv CASCADE;")
            cur.execute("DROP TABLE IF EXISTS intraday_analytics CASCADE;")
            cur.execute("DROP TABLE IF EXISTS daily_analytics CASCADE;")
            cur.execute("DROP TABLE IF EXISTS realtime_analytics CASCADE;")
        
        # Create tables
        print("Creating tables...")
        
        # Real-time prices table (high frequency tick data)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS realtime_prices (
                id SERIAL, 
                symbol VARCHAR(20) NOT NULL,
                price NUMERIC(19,4) NOT NULL,
                volume BIGINT,
                timestamp TIMESTAMPTZ NOT NULL,
                source VARCHAR(50),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (id, timestamp)  
            );
        """)
        
        cur.execute("""
            SELECT create_hypertable('realtime_prices', 'timestamp', 
                                     if_not_exists => TRUE,
                                     chunk_time_interval => INTERVAL '1 day');
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_realtime_prices_symbol_timestamp 
            ON realtime_prices (symbol, timestamp DESC);
        """)
        
        # Intraday OHLCV table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS intraday_ohlcv (
                id SERIAL,  
                symbol VARCHAR(20) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                interval_minutes INTEGER NOT NULL,
                open NUMERIC(19,4) NOT NULL,
                high NUMERIC(19,4) NOT NULL,
                low NUMERIC(19,4) NOT NULL,
                close NUMERIC(19,4) NOT NULL,
                volume BIGINT,
                num_samples INTEGER DEFAULT 1,
                source VARCHAR(50),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (id, timestamp),  
                CONSTRAINT unique_bar UNIQUE (symbol, timestamp, interval_minutes) 
            );
        """)
        
        cur.execute("""
            SELECT create_hypertable('intraday_ohlcv', 'timestamp', 
                                     if_not_exists => TRUE,
                                     chunk_time_interval => INTERVAL '1 month');
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS historical_ohlcv (
                id SERIAL,  
                symbol VARCHAR(20) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                open NUMERIC(19,4) NOT NULL,
                high NUMERIC(19,4) NOT NULL,
                low NUMERIC(19,4) NOT NULL,
                close NUMERIC(19,4) NOT NULL,
                volume BIGINT,
                adjusted_close NUMERIC(19,4),
                source VARCHAR(50),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (id, timestamp),  
                CONSTRAINT unique_daily UNIQUE (symbol, timestamp)  
            );
        """)
        
        cur.execute("""
            SELECT create_hypertable('historical_ohlcv', 'timestamp', 
                                     if_not_exists => TRUE,
                                     chunk_time_interval => INTERVAL '1 year');
        """)
                
        # Intraday analytics
        cur.execute("""
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
        
        cur.execute("""
            SELECT create_hypertable('intraday_analytics', 'timestamp', 
                                     if_not_exists => TRUE,
                                     chunk_time_interval => INTERVAL '1 month');
        """)
        
        # Daily analytics
        cur.execute("""
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
        
        cur.execute("""
            SELECT create_hypertable('daily_analytics', 'timestamp', 
                                     if_not_exists => TRUE,
                                     chunk_time_interval => INTERVAL '1 year');
        """)

        print("Creating real-time analytics table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS realtime_analytics (
                id SERIAL,  
                symbol VARCHAR(20) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                price NUMERIC(19,4) NOT NULL,
                
                -- Real-time specific indicators
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
            );
        """)
        
        cur.execute("""
            SELECT create_hypertable('realtime_analytics', 'timestamp', 
                                     if_not_exists => TRUE,
                                     chunk_time_interval => INTERVAL '1 day');
        """)
        
        # Retention policies
        print("Setting retention policies...")
        
        # Raw prices - keep for 5 days
        add_retention_policy_if_not_exists(cur, "realtime_prices", "5 days")
        
        # Real-time analytics - keep for 1 hour
        add_retention_policy_if_not_exists(cur, "realtime_analytics", "1 hour")

        # Intraday OLHCV - keep for 2 years
        add_retention_policy_if_not_exists(cur, "intraday_ohlcv", "2 years")

        print("Setting compression policies...")
        cur.execute("""
            ALTER TABLE intraday_ohlcv SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'symbol,interval_minutes'
            );
            
            SELECT add_compression_policy('intraday_ohlcv', INTERVAL '3 days');
        """)
        
        # Compress historical data after 7 days
        cur.execute("""
            ALTER TABLE historical_ohlcv SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'symbol'
            );
            
            SELECT add_compression_policy('historical_ohlcv', INTERVAL '7 days');
        """)
        
        # Add continuous aggregates for automatic aggregation
        print("Setting up continuous aggregates...")

        # Continuous aggregate for 5-minute OHLCV from realtime prices
        cur.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS realtime_to_intraday_5min
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('5 minutes', timestamp) AS bucket,
                symbol,
                FIRST(price, timestamp) AS open,
                MAX(price) AS high,
                MIN(price) AS low,
                LAST(price, timestamp) AS close,
                SUM(volume) AS volume,
                COUNT(*) AS num_samples,
                'aggregator' AS source
            FROM realtime_prices
            GROUP BY bucket, symbol
            WITH NO DATA;

            SELECT add_continuous_aggregate_policy('realtime_to_intraday_5min',
                start_offset => INTERVAL '2 hours',  
                end_offset => INTERVAL '10 minutes',  
                schedule_interval => INTERVAL '5 minutes');
        """)

        # Add continuous aggregate for daily OHLCV from intraday
        cur.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS intraday_to_daily
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('1 day', timestamp) AS bucket,
                symbol,
                FIRST(open, timestamp) AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                LAST(close, timestamp) AS close,
                SUM(volume) AS volume,
                'aggregator' AS source
            FROM intraday_ohlcv
            WHERE interval_minutes = 5
            GROUP BY bucket, symbol
            WITH NO DATA;

            SELECT add_continuous_aggregate_policy('intraday_to_daily',
                start_offset => INTERVAL '3 days',  -- Increased from 1 day
                end_offset => INTERVAL '6 hours',   -- Increased from 1 hour
                schedule_interval => INTERVAL '1 hour');
        """)
        
        print("Database initialization completed successfully.")
        
    except Exception as e:
        print(f"Error initializing database: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

def main():
    parser = argparse.ArgumentParser(description='Initialize TimescaleDB for analytics-api')
    parser.add_argument('--db-url', default=DEFAULT_DB_URL,
                        help=f'Database connection URL (default: {DEFAULT_DB_URL})')
    parser.add_argument('--drop-existing', action='store_true',
                        help='Drop existing tables before creating new ones')
    
    args = parser.parse_args()
    init_database(args.db_url, args.drop_existing)

if __name__ == "__main__":
    main()