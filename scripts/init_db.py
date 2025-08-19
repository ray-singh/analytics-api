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
            cur.execute("""
                DROP TABLE IF EXISTS 
                    realtime_prices, 
                    intraday_ohlcv, 
                    historical_ohlcv,
                    intraday_analytics,
                    daily_analytics
                CASCADE;
            """)
        
        # Create tables
        print("Creating tables...")
        
        # Real-time prices table (high frequency tick data)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS realtime_prices (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                price NUMERIC(19,4) NOT NULL,
                volume BIGINT,
                timestamp TIMESTAMPTZ NOT NULL,
                source VARCHAR(50),
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        
        # Convert to TimescaleDB hypertable
        cur.execute("""
            SELECT create_hypertable('realtime_prices', 'timestamp', 
                                     if_not_exists => TRUE,
                                     chunk_time_interval => INTERVAL '1 day');
        """)
        
        # Add indexes
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_realtime_prices_symbol_timestamp 
            ON realtime_prices (symbol, timestamp DESC);
        """)
        
        # Create intraday OHLCV table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS intraday_ohlcv (
                id SERIAL PRIMARY KEY,
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
                CONSTRAINT unique_bar UNIQUE (symbol, timestamp, interval_minutes)
            );
        """)
        
        # Convert to TimescaleDB hypertable
        cur.execute("""
            SELECT create_hypertable('intraday_ohlcv', 'timestamp', 
                                     if_not_exists => TRUE,
                                     chunk_time_interval => INTERVAL '1 month');
        """)
        
        # Create historical daily OHLCV table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS historical_ohlcv (
                id SERIAL PRIMARY KEY,
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
                CONSTRAINT unique_daily UNIQUE (symbol, timestamp)
            );
        """)
        
        # Convert to TimescaleDB hypertable
        cur.execute("""
            SELECT create_hypertable('historical_ohlcv', 'timestamp', 
                                     if_not_exists => TRUE,
                                     chunk_time_interval => INTERVAL '1 year');
        """)
        
        # Create analytics tables
        
        # Intraday analytics
        cur.execute("""
            CREATE TABLE IF NOT EXISTS intraday_analytics (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                interval_minutes INTEGER NOT NULL,
                price NUMERIC(19,4) NOT NULL,
                
                -- Trend indicators
                sma_20 NUMERIC(19,4),
                sma_50 NUMERIC(19,4),
                ema_12 NUMERIC(19,4),
                ema_26 NUMERIC(19,4),
                
                -- Momentum indicators
                rsi_14 NUMERIC(19,4),
                macd NUMERIC(19,4),
                macd_signal NUMERIC(19,4),
                macd_hist NUMERIC(19,4),
                
                -- Volatility indicators
                bb_upper NUMERIC(19,4),
                bb_middle NUMERIC(19,4),
                bb_lower NUMERIC(19,4),
                atr_14 NUMERIC(19,4),
                
                -- Volume indicators
                obv NUMERIC(19,4),
                
                created_at TIMESTAMPTZ DEFAULT NOW(),
                CONSTRAINT unique_intraday_analytics UNIQUE (symbol, timestamp, interval_minutes)
            );
        """)
        
        # Convert to TimescaleDB hypertable
        cur.execute("""
            SELECT create_hypertable('intraday_analytics', 'timestamp', 
                                     if_not_exists => TRUE,
                                     chunk_time_interval => INTERVAL '1 month');
        """)
        
        # Daily analytics
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_analytics (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                price NUMERIC(19,4) NOT NULL,
                
                -- Trend indicators
                sma_20 NUMERIC(19,4),
                sma_50 NUMERIC(19,4),
                sma_200 NUMERIC(19,4),
                ema_12 NUMERIC(19,4),
                ema_26 NUMERIC(19,4),
                
                -- Momentum indicators
                rsi_14 NUMERIC(19,4),
                macd NUMERIC(19,4),
                macd_signal NUMERIC(19,4),
                macd_hist NUMERIC(19,4),
                
                -- Volatility indicators
                bb_upper NUMERIC(19,4),
                bb_middle NUMERIC(19,4),
                bb_lower NUMERIC(19,4),
                atr_14 NUMERIC(19,4),
                
                -- Volume indicators
                obv NUMERIC(19,4),
                
                created_at TIMESTAMPTZ DEFAULT NOW(),
                CONSTRAINT unique_daily_analytics UNIQUE (symbol, timestamp)
            );
        """)
        
        # Convert to TimescaleDB hypertable
        cur.execute("""
            SELECT create_hypertable('daily_analytics', 'timestamp', 
                                     if_not_exists => TRUE,
                                     chunk_time_interval => INTERVAL '1 year');
        """)
        
        # Retention policies
        print("Setting retention policies...")
        
        # Raw prices - keep for 7 days
        cur.execute("""
            SELECT add_retention_policy('realtime_prices', INTERVAL '7 days');
        """)
        
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