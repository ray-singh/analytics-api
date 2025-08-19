"""
Generate sample market data for testing and development
Usage: python -m scripts.seed_demo_data [--symbols AAPL,MSFT] [--days 10]
"""

import argparse
import os
import sys
import psycopg2
from psycopg2 import sql
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from dotenv import load_dotenv

load_dotenv()
DEFAULT_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/stockanalytics")
DEFAULT_SYMBOLS = ["AAPL"]
DEFAULT_DAYS = 30

def fetch_historical_data(symbols, days):
    """Fetch historical data from Yahoo Finance"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    print(f"Fetching historical data for {symbols} from {start_date.date()} to {end_date.date()}")
    
    # Daily data
    daily_data = yf.download(
        symbols,
        start=start_date,
        end=end_date,
        interval="1d",
        group_by="ticker",
        auto_adjust=True
    )
    
    # Get 1-hour data for the past 7 days (Yahoo limitation)
    lookback_days = min(days, 7)  # Yahoo only allows 7 days of intraday data
    intraday_start = end_date - timedelta(days=lookback_days)
    
    intraday_data = yf.download(
        symbols,
        start=intraday_start,
        end=end_date,
        interval="1h",
        group_by="ticker",
        auto_adjust=True
    )
    
    return {
        "daily": daily_data,
        "intraday": intraday_data
    }

def seed_database(db_url, symbols, days, clean=False):
    """Seed the database with historical data"""
    conn = None
    try:
        # Connect to database
        print(f"Connecting to database: {db_url}")
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        cur = conn.cursor()
        
        # Clean existing data if requested
        if clean:
            print("Cleaning existing data...")
            cur.execute("""
                DELETE FROM realtime_prices;
                DELETE FROM intraday_ohlcv;
                DELETE FROM historical_ohlcv;
                DELETE FROM intraday_analytics;
                DELETE FROM daily_analytics;
            """)
        
        # Fetch data
        data = fetch_historical_data(symbols, days)
        
        # Insert daily data
        print("Inserting daily OHLCV data...")
        for symbol in symbols:
            if symbol in data["daily"]:
                df = data["daily"][symbol]
                
                for index, row in df.iterrows():
                    # Check if data already exists
                    cur.execute("""
                        SELECT 1 FROM historical_ohlcv 
                        WHERE symbol = %s AND timestamp = %s
                    """, (symbol, index))
                    
                    if not cur.fetchone():
                        cur.execute("""
                            INSERT INTO historical_ohlcv 
                            (symbol, timestamp, open, high, low, close, volume, adjusted_close, source)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            symbol,
                            index,
                            float(row['Open']),
                            float(row['High']),
                            float(row['Low']),
                            float(row['Close']),
                            int(row['Volume']),
                            float(row['Close']),  
                            'yfinance'
                        ))
        
        # Insert intraday data
        print("Inserting intraday OHLCV data...")
        for symbol in symbols:
            if symbol in data["intraday"]:
                df = data["intraday"][symbol]
                
                for index, row in df.iterrows():
                    # Check if data already exists
                    cur.execute("""
                        SELECT 1 FROM intraday_ohlcv 
                        WHERE symbol = %s AND timestamp = %s AND interval_minutes = %s
                    """, (symbol, index, 60))  # 60 minutes = 1 hour
                    
                    if not cur.fetchone():
                        cur.execute("""
                            INSERT INTO intraday_ohlcv 
                            (symbol, timestamp, interval_minutes, open, high, low, close, volume, source)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            symbol,
                            index,
                            60,  # 60 minutes = 1 hour
                            float(row['Open']),
                            float(row['High']),
                            float(row['Low']),
                            float(row['Close']),
                            int(row['Volume']),
                            'yfinance'
                        ))
        
        # Generate simulated realtime prices (last day only)
        print("Generating simulated realtime tick data...")
        
        # Get the last day's intraday data for each symbol
        for symbol in symbols:
            if symbol not in data["intraday"]:
                continue
                
            df = data["intraday"][symbol]
            if df.empty:
                continue
                
            # Get last day's data
            last_day = df.index.max().date()
            last_day_data = df[df.index.date == last_day]
            
            if last_day_data.empty:
                continue
                
            # Generate simulated ticks between OHLC points
            for i in range(len(last_day_data) - 1):
                row = last_day_data.iloc[i]
                next_row = last_day_data.iloc[i + 1]
                
                start_time = last_day_data.index[i]
                end_time = last_day_data.index[i + 1]
                
                # Generate ~10 ticks between each hour
                num_ticks = 10
                
                # Generate timestamps
                tick_times = pd.date_range(start=start_time, end=end_time, periods=num_ticks)
                
                # Linear price movement with some noise
                start_price = row['Close']
                end_price = next_row['Open']
                price_diff = end_price - start_price
                
                for j in range(num_ticks):
                    # Linear interpolation with small random variations
                    progress = j / (num_ticks - 1) if num_ticks > 1 else 0
                    base_price = start_price + progress * price_diff
                    noise = np.random.normal(0, abs(price_diff) * 0.02)  # 2% noise
                    tick_price = max(0.01, base_price + noise)  # Ensure price > 0
                    
                    # Random volume for tick
                    tick_volume = int(np.random.poisson(row['Volume'] / num_ticks))
                    
                    # Insert simulated tick
                    cur.execute("""
                        INSERT INTO realtime_prices 
                        (symbol, price, volume, timestamp, source)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        symbol,
                        float(tick_price),
                        tick_volume,
                        tick_times[j],
                        'simulation'
                    ))
        
        # Commit all changes
        conn.commit()
        print(f"Successfully seeded database with {days} days of data for {len(symbols)} symbols")
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error seeding database: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

def main():
    parser = argparse.ArgumentParser(description='Seed database with sample market data')
    parser.add_argument('--db-url', default=DEFAULT_DB_URL,
                        help=f'Database connection URL (default: {DEFAULT_DB_URL})')
    parser.add_argument('--symbols', default=','.join(DEFAULT_SYMBOLS),
                        help=f'Comma-separated list of symbols (default: {",".join(DEFAULT_SYMBOLS)})')
    parser.add_argument('--days', type=int, default=DEFAULT_DAYS,
                        help=f'Number of days to backfill (default: {DEFAULT_DAYS})')
    parser.add_argument('--clean', action='store_true',
                        help='Clean existing data before seeding')
    
    args = parser.parse_args()
    symbols = args.symbols.split(',')
    
    seed_database(args.db_url, symbols, args.days, args.clean)

if __name__ == "__main__":
    main()