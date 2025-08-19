"""
Initial database schema for the stock analytics platform.

This migration creates the core tables needed for storing market data:
- realtime_prices: Raw price ticks from market data sources
- intraday_ohlcv: Aggregated OHLCV bars for intraday timeframes
- historical_ohlcv: Daily OHLCV data
- intraday_analytics: Technical indicators for intraday timeframes
- daily_analytics: Technical indicators for daily timeframes
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging

logger = logging.getLogger(__name__)

def create_extensions(conn):
    """Create required PostgreSQL extensions."""
    cur = conn.cursor()
    try:
        # Create TimescaleDB extension if it doesn't exist
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        conn.commit()
        logger.info("TimescaleDB extension created successfully")
    except Exception as e:
        logger.error(f"Error creating extensions: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()

def create_realtime_prices_table(conn):
    """Create table for storing raw realtime price ticks."""
    cur = conn.cursor()
    try:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS realtime_prices (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            price NUMERIC(18, 6) NOT NULL,
            volume BIGINT,
            timestamp TIMESTAMPTZ NOT NULL,
            source VARCHAR(50),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        
        -- Create index on symbol and timestamp for faster queries
        CREATE INDEX IF NOT EXISTS idx_realtime_prices_symbol_timestamp 
        ON realtime_prices (symbol, timestamp DESC);
        
        -- Convert to hypertable for TimescaleDB
        SELECT create_hypertable('realtime_prices', 'timestamp', 
                                if_not_exists => TRUE);
        """)
        conn.commit()
        logger.info("Created realtime_prices table")
    except Exception as e:
        logger.error(f"Error creating realtime_prices table: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()

def create_intraday_ohlcv_table(conn):
    """Create table for storing intraday OHLCV bars."""
    cur = conn.cursor()
    try:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS intraday_ohlcv (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            interval_minutes INTEGER NOT NULL,
            open NUMERIC(18, 6) NOT NULL,
            high NUMERIC(18, 6) NOT NULL,
            low NUMERIC(18, 6) NOT NULL,
            close NUMERIC(18, 6) NOT NULL,
            volume BIGINT NOT NULL,
            num_samples INTEGER DEFAULT 1,
            source VARCHAR(50),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            -- Composite unique constraint to prevent duplicates
            CONSTRAINT unique_symbol_timestamp_interval UNIQUE (symbol, timestamp, interval_minutes)
        );
        
        -- Create index for querying specific symbols and intervals
        CREATE INDEX IF NOT EXISTS idx_intraday_ohlcv_symbol_interval_timestamp 
        ON intraday_ohlcv (symbol, interval_minutes, timestamp DESC);
        
        -- Convert to hypertable for TimescaleDB
        SELECT create_hypertable('intraday_ohlcv', 'timestamp', 
                                if_not_exists => TRUE);
        """)
        conn.commit()
        logger.info("Created intraday_ohlcv table")
    except Exception as e:
        logger.error(f"Error creating intraday_ohlcv table: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()

def create_historical_ohlcv_table(conn):
    """Create table for storing daily historical OHLCV data."""
    cur = conn.cursor()
    try:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS historical_ohlcv (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,  -- This is the day
            open NUMERIC(18, 6) NOT NULL,
            high NUMERIC(18, 6) NOT NULL,
            low NUMERIC(18, 6) NOT NULL,
            close NUMERIC(18, 6) NOT NULL,
            volume BIGINT NOT NULL,
            adjusted_close NUMERIC(18, 6),
            source VARCHAR(50),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            -- Composite unique constraint to prevent duplicates
            CONSTRAINT unique_symbol_day UNIQUE (symbol, timestamp)
        );
        
        -- Create index for querying specific symbols
        CREATE INDEX IF NOT EXISTS idx_historical_ohlcv_symbol_timestamp 
        ON historical_ohlcv (symbol, timestamp DESC);
        
        -- Convert to hypertable for TimescaleDB
        SELECT create_hypertable('historical_ohlcv', 'timestamp', 
                               if_not_exists => TRUE);
        """)
        conn.commit()
        logger.info("Created historical_ohlcv table")
    except Exception as e:
        logger.error(f"Error creating historical_ohlcv table: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()

def create_intraday_analytics_table(conn):
    """Create table for storing intraday technical indicators."""
    cur = conn.cursor()
    try:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS intraday_analytics (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            interval_minutes INTEGER NOT NULL,
            price NUMERIC(18, 6) NOT NULL,
            
            -- Moving Averages
            sma_20 NUMERIC(18, 6),
            sma_50 NUMERIC(18, 6),
            sma_200 NUMERIC(18, 6),
            ema_12 NUMERIC(18, 6),
            ema_26 NUMERIC(18, 6),
            
            -- Momentum Indicators
            rsi_14 NUMERIC(10, 6),
            macd NUMERIC(18, 6),
            macd_signal NUMERIC(18, 6),
            macd_hist NUMERIC(18, 6),
            
            -- Volatility Indicators
            bb_upper NUMERIC(18, 6),
            bb_middle NUMERIC(18, 6),
            bb_lower NUMERIC(18, 6),
            atr_14 NUMERIC(18, 6),
            
            -- Volume Indicators
            obv BIGINT,
            
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            -- Composite unique constraint to prevent duplicates
            CONSTRAINT unique_intraday_analytics UNIQUE (symbol, timestamp, interval_minutes)
        );
        
        -- Create index for querying specific symbols and intervals
        CREATE INDEX IF NOT EXISTS idx_intraday_analytics_symbol_interval_timestamp 
        ON intraday_analytics (symbol, interval_minutes, timestamp DESC);
        
        -- Convert to hypertable for TimescaleDB
        SELECT create_hypertable('intraday_analytics', 'timestamp', 
                               if_not_exists => TRUE);
        """)
        conn.commit()
        logger.info("Created intraday_analytics table")
    except Exception as e:
        logger.error(f"Error creating intraday_analytics table: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()

def create_daily_analytics_table(conn):
    """Create table for storing daily technical indicators."""
    cur = conn.cursor()
    try:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_analytics (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,  -- This is the day
            price NUMERIC(18, 6) NOT NULL,  -- Closing price
            
            -- Moving Averages
            sma_20 NUMERIC(18, 6),
            sma_50 NUMERIC(18, 6),
            sma_200 NUMERIC(18, 6),
            ema_12 NUMERIC(18, 6),
            ema_26 NUMERIC(18, 6),
            
            -- Momentum Indicators
            rsi_14 NUMERIC(10, 6),
            macd NUMERIC(18, 6),
            macd_signal NUMERIC(18, 6),
            macd_hist NUMERIC(18, 6),
            
            -- Volatility Indicators
            bb_upper NUMERIC(18, 6),
            bb_middle NUMERIC(18, 6),
            bb_lower NUMERIC(18, 6),
            atr_14 NUMERIC(18, 6),
            
            -- Volume Indicators
            obv BIGINT,
            
            -- Trend Indicators
            adx_14 NUMERIC(10, 6),
            
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            -- Composite unique constraint to prevent duplicates
            CONSTRAINT unique_daily_analytics UNIQUE (symbol, timestamp)
        );
        
        -- Create index for querying specific symbols
        CREATE INDEX IF NOT EXISTS idx_daily_analytics_symbol_timestamp 
        ON daily_analytics (symbol, timestamp DESC);
        
        -- Convert to hypertable for TimescaleDB
        SELECT create_hypertable('daily_analytics', 'timestamp', 
                               if_not_exists => TRUE);
        """)
        conn.commit()
        logger.info("Created daily_analytics table")
    except Exception as e:
        logger.error(f"Error creating daily_analytics table: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()

def create_retention_policies(conn):
    """Create data retention policies for TimescaleDB hypertables."""
    cur = conn.cursor()
    try:
        # Add retention policy for realtime_prices (keep for 7 days)
        cur.execute("""
        SELECT add_retention_policy('realtime_prices', INTERVAL '7 days', if_not_exists => TRUE);
        """)
        
        # Add retention policy for intraday_ohlcv (keep for 90 days)
        cur.execute("""
        SELECT add_retention_policy('intraday_ohlcv', INTERVAL '90 days', if_not_exists => TRUE);
        """)
        
        # Add retention policy for intraday_analytics (keep for 90 days)
        cur.execute("""
        SELECT add_retention_policy('intraday_analytics', INTERVAL '90 days', if_not_exists => TRUE);
        """)
        
        conn.commit()
        logger.info("Created retention policies for TimescaleDB tables")
    except Exception as e:
        logger.error(f"Error creating retention policies: {e}")
        conn.rollback()
    finally:
        cur.close()

def run_migration(db_url):
    """Run the initial schema migration."""
    conn = psycopg2.connect(db_url)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    try:
        # Create extensions
        create_extensions(conn)
        
        # Create tables
        create_realtime_prices_table(conn)
        create_intraday_ohlcv_table(conn)
        create_historical_ohlcv_table(conn)
        create_intraday_analytics_table(conn)
        create_daily_analytics_table(conn)
        
        # Create retention policies
        create_retention_policies(conn)
        
        logger.info("Initial schema migration completed successfully")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        conn.close()