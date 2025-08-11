import asyncio
import json
import ssl
import certifi
import os
import pandas as pd
from twelvedata import TDClient
from aiokafka import AIOKafkaProducer
from db import insert_realtime_price, insert_intraday_ohlcv, insert_historical_daily
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
import requests
import io
import yfinance as yf

# Kafka configuration
KAFKA_TOPIC = "stock_prices"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

load_dotenv()
td = TDClient(apikey=os.getenv("TWELVEDATA_API_KEY"))
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

async def fetch_historic_intraday_data_chunked(symbol: str, interval: str = "5min", 
                                             start_date: str = "2024-01-01", 
                                             end_date: str = None,
                                             chunk_days: int = 60):
    """
    Fetch historic intraday data for a given symbol in chunks to manage API rate limits.
    
    Args:
        symbol: Stock symbol to fetch data for
        interval: Data interval (e.g., '1min', '5min', '15min')
        start_date: Start date for the data in 'YYYY-MM-DD' format
        end_date: End date for the data in 'YYYY-MM-DD' format (defaults to yesterday)
        chunk_days: Number of days to fetch in each API call (default: 60 days ~ 2 months)
        
    Returns:
        Dictionary of DataFrames with chunks of historical data
    """
    # Set end date to yesterday if not specified
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Convert dates to datetime objects for easier manipulation
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Initialize dictionary to store data chunks
    data_chunks = {}
    
    # Calculate total number of API calls needed
    total_days = (end_dt - start_dt).days
    num_chunks = (total_days // chunk_days) + (1 if total_days % chunk_days > 0 else 0)
    
    print(f"Fetching {interval} data for {symbol} from {start_date} to {end_date}")
    print(f"Will fetch data in {num_chunks} chunks of {chunk_days} days each")
    
    # Track API calls for rate limiting
    api_calls = 0
    
    # Process data in chunks
    current_start = start_dt
    chunk_num = 1
    
    while current_start < end_dt:
        # Calculate end date for current chunk
        current_end = min(current_start + timedelta(days=chunk_days), end_dt)
        
        # Format dates for API call
        chunk_start_str = current_start.strftime('%Y-%m-%d')
        chunk_end_str = current_end.strftime('%Y-%m-%d')
        
        print(f"Fetching chunk {chunk_num}/{num_chunks}: {chunk_start_str} to {chunk_end_str}")
        
        # Make API call
        try:
            ts = td.time_series(
                symbol=symbol,
                interval=interval,
                timezone="America/New_York",
                start_date=chunk_start_str,
                end_date=chunk_end_str,
                outputsize=5000  # Maximum allowed
            )
            chunk_data = ts.as_pandas()
            
            # Store data in dictionary
            key = f"{chunk_start_str}_{chunk_end_str}"
            data_chunks[key] = chunk_data
            
            print(f"Successfully fetched {len(chunk_data)} data points for {symbol} "
                  f"from {chunk_start_str} to {chunk_end_str}")
            
            # Insert data into database (if exists)
            if len(chunk_data) > 0:
                await store_historical_intraday_data(symbol, chunk_data, interval)
                
            # Rate limiting: 8 API calls per minute
            api_calls += 1
            if api_calls >= 8:
                print("Rate limit approaching. Pausing for 65 seconds...")
                await asyncio.sleep(65)  # Wait a bit more than a minute to be safe
                api_calls = 0
            else:
                # Small pause between requests anyway
                await asyncio.sleep(1)
                
        except Exception as e:
            print(f"Error fetching data for {chunk_start_str} to {chunk_end_str}: {e}")
            # Continue with next chunk
        
        # Move to next chunk
        current_start = current_end + timedelta(days=1)
        chunk_num += 1
    
    print(f"Completed fetching historical {interval} data for {symbol}")
    return data_chunks

async def store_historical_intraday_data(symbol, data_df, interval):
    """
    Store historical intraday data from DataFrame into the database.
    
    Args:
        symbol: Stock ticker symbol
        data_df: DataFrame containing OHLCV data
        interval: Data interval (e.g., '5min')
    """
    # Convert interval string to minutes
    interval_mins = {
        "1min": 1,
        "5min": 5,
        "15min": 15,
        "30min": 30,
        "1h": 60,
        "4h": 240
    }.get(interval, 5)  # Default to 5min if unknown interval
    
    # Convert to list of dictionaries for easier processing
    records = data_df.reset_index().to_dict('records')
    
    # Process each record
    count = 0
    for record in records:
        # Extract timestamp
        if 'datetime' in record:
            timestamp = int(pd.Timestamp(record['datetime']).timestamp())
        else:
            # Assume index is datetime
            timestamp = int(pd.Timestamp(record['index']).timestamp())
        
        # Insert into database
        insert_intraday_ohlcv(
            symbol=symbol,
            timestamp=timestamp,
            open_price=float(record['open']),
            high=float(record['high']),
            low=float(record['low']),
            close=float(record['close']),
            volume=int(record['volume']),
            interval_minutes=interval_mins,
            source="TwelveData"
        )
        
        count += 1
        
        # Print progress every 500 records
        if count % 500 == 0:
            print(f"Inserted {count}/{len(records)} records")
    
    print(f"Successfully inserted {count} historical records for {symbol}")

async def init_historical_data(symbols=["AAPL"], interval="5min", days_back=365):
    """
    Initialize historical intraday data for one or more symbols.
    
    Args:
        symbols: List of stock symbols to fetch data for
        interval: Data interval (e.g., '5min')
        days_back: How many days of historical data to fetch
    """
    start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    for symbol in symbols:
        print(f"Initializing historical {interval} data for {symbol}...")
        await fetch_historic_intraday_data_chunked(
            symbol=symbol,
            interval=interval,
            start_date=start_date,
            end_date=end_date
        )
        # Add a short delay between symbols
        await asyncio.sleep(5)

async def subscribe_and_produce(symbol: str, fetch_history=False, interval="5min"):
    """
    Subscribe to real-time stock price updates using yfinance WebSocket and produce them to Kafka.
    Optionally fetch historical data first using smart backfill.
    """
    # Smart backfill of historical data if requested
    if fetch_history:
        print(f"Performing smart backfill for {symbol} before starting real-time stream...")
        await smart_backfill(symbol, interval)
    
    # Start automatic consolidation task
    consolidation_task = asyncio.create_task(scheduled_consolidation(symbol))
    
    # Set up Kafka producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

    try:
        # Define the message handler for yfinance WebSocket
        def message_handler(message):
            """
            Handle incoming WebSocket messages from yfinance.
            """
            try:
                print(f"{message}")
                # Parse the message
                if message.get("id") == symbol:
                    # Extract relevant fields
                    price = float(message["price"])
                    timestamp = int(message["time"]) // 1000  # Convert milliseconds to seconds
                    volume = int(message.get("day_volume", 0))
                    
                    # Create the event
                    event = {
                        "symbol": symbol,
                        "price": price,
                        "timestamp": timestamp,
                        "volume": volume
                    }
                    
                    # Produce the event to Kafka
                    asyncio.run_coroutine_threadsafe(
                        handle_event(event, producer), asyncio.get_event_loop()
                    )
            except Exception as e:
                print(f"Error processing WebSocket message: {e}")

        # Start the yfinance WebSocket
        print(f"Starting yfinance WebSocket for {symbol}...")
        with yf.WebSocket() as ws:
            ws.subscribe([symbol])
            ws.listen(message_handler)

    except KeyboardInterrupt:
        print("Keyboard interrupt received, shutting down...")
        consolidation_task.cancel()
    except Exception as e:
        print(f"Error in WebSocket connection: {e}")
    finally:
        await producer.stop()

async def handle_event(event, producer):
    """
    Handle the event by producing it to Kafka and storing it in the database.
    """
    try:
        # Produce the event to Kafka
        await producer.send_and_wait(KAFKA_TOPIC, json.dumps(event).encode())
        print(f"Produced to Kafka: {event}")
        
        # Insert the event into the database
        insert_realtime_price(
            symbol=event["symbol"],
            price=event["price"],
            timestamp=event["timestamp"],
            volume=event.get("volume", None),
            source="YFinance-Realtime"
        )
    except Exception as e:
        print(f"Error handling event: {e}")

async def detect_data_gaps(symbol: str, interval: str = "5min"):
    """
    Detect gaps in historical intraday data and determine what data needs to be fetched.
    
    Args:
        symbol: Stock symbol to check
        interval: Data interval (e.g., '5min', '15min')
        
    Returns:
        dict: Information about data gaps and what to fetch
    """
    from db import get_conn
    
    # Convert interval string to minutes for database query
    interval_mins = {
        "1min": 1, "5min": 5, "15min": 15, "30min": 30, 
        "1h": 60, "4h": 240
    }.get(interval, 5)
    
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        print(f"Checking existing data for {symbol} at {interval} interval...")
        
        # Find the most recent data point
        cur.execute(
            "SELECT MAX(timestamp) FROM intraday_ohlcv WHERE symbol = %s AND interval_minutes = %s",
            (symbol, interval_mins)
        )
        latest_timestamp = cur.fetchone()[0]
        
        # Find the oldest data point
        cur.execute(
            "SELECT MIN(timestamp) FROM intraday_ohlcv WHERE symbol = %s AND interval_minutes = %s",
            (symbol, interval_mins)
        )
        oldest_timestamp = cur.fetchone()[0]
        
        # Count how many records we have
        cur.execute(
            "SELECT COUNT(*) FROM intraday_ohlcv WHERE symbol = %s AND interval_minutes = %s",
            (symbol, interval_mins)
        )
        record_count = cur.fetchone()[0]
        
        # Current time and yesterday (as we generally don't want to fetch current day data)
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        
        # Calculate what data we need to fetch
        if latest_timestamp is None:
            # No data exists - need to do a full backfill
            status = "full_backfill"
            # Default to 1 year of data
            start_date = (yesterday - timedelta(days=365)).strftime('%Y-%m-%d')
            end_date = yesterday.strftime('%Y-%m-%d')
            gap_days = 365
            
            print(f"No existing data found for {symbol} at {interval} interval.")
            print(f"Will perform full backfill from {start_date} to {end_date}")
            
        else:
            # Some data exists - need to check if it's up to date
            latest_date = latest_timestamp.date()
            yesterday_date = yesterday.date()
            
            if latest_date < yesterday_date:
                # Data needs updating - fill the gap
                status = "update"
                # Start from the day after our latest data
                start_date = (latest_timestamp + timedelta(days=1)).strftime('%Y-%m-%d')
                end_date = yesterday.strftime('%Y-%m-%d')
                gap_days = (yesterday_date - latest_date).days
                
                print(f"Data gap detected for {symbol}: from {start_date} to {end_date}")
                print(f"Missing approximately {gap_days} days of data")
                
            else:
                # Data is current
                status = "current"
                start_date = None
                end_date = None
                gap_days = 0
                
                print(f"Data for {symbol} at {interval} interval is up to date.")
                print(f"Latest data point: {latest_timestamp}")
        
        return {
            "symbol": symbol,
            "interval": interval,
            "status": status,
            "record_count": record_count,
            "latest_timestamp": latest_timestamp,
            "oldest_timestamp": oldest_timestamp,
            "start_date": start_date,
            "end_date": end_date,
            "gap_days": gap_days
        }
        
    finally:
        cur.close()
        conn.close()

async def smart_backfill(symbol: str, interval: str = "5min", max_days_back: int = 365):
    """
    Smart backfill of historical data - only fetch what's missing.
    
    Args:
        symbol: Stock symbol to backfill
        interval: Data interval (e.g., '5min', '15min')
        max_days_back: Maximum days to backfill if doing a full backfill
        
    Returns:
        dict: Information about the backfill operation
    """
    # First detect what data we have and what we need
    gap_info = await detect_data_gaps(symbol, interval)
    
    if gap_info["status"] == "current":
        print(f"Data for {symbol} at {interval} interval is already up to date. No backfill needed.")
        return gap_info
    
    # If we're doing a backfill, either full or update
    if gap_info["status"] in ["full_backfill", "update"]:
        start_date = gap_info["start_date"]
        end_date = gap_info["end_date"]
        
        print(f"Backfilling {symbol} {interval} data from {start_date} to {end_date}")
        
        # Fetch the missing data
        chunks = await fetch_historic_intraday_data_chunked(
            symbol=symbol,
            interval=interval,
            start_date=start_date,
            end_date=end_date
        )
        
        # Add information about the chunks we fetched to the gap info
        gap_info["chunks_fetched"] = len(chunks)
        gap_info["total_records_fetched"] = sum(len(df) for df in chunks.values())
        
        print(f"Backfill complete for {symbol} - fetched {gap_info['total_records_fetched']} records")
        
    return gap_info

async def scheduled_consolidation(symbol):
    """
    Periodically consolidate real-time data to intraday and intraday to historical.
    """
    from db import consolidate_realtime_to_intraday, consolidate_intraday_to_daily, clean_realtime_data
    
    try:
        print(f"Starting scheduled data consolidation for {symbol}")
        while True:
            # Every 15 minutes, consolidate real-time data older than 15 minutes
            consolidated = consolidate_realtime_to_intraday(symbol, older_than_minutes=15)
            if consolidated > 0:
                print(f"Consolidated {consolidated} intervals from real-time to intraday")
                
            # Every day at midnight, consolidate previous day's intraday data to historical
            now = datetime.now()
            if now.hour == 0 and now.minute < 15:  # Between 00:00 and 00:15
                consolidated = consolidate_intraday_to_daily(symbol, days_old=1)
                if consolidated > 0:
                    print(f"Consolidated {consolidated} days from intraday to historical")
            
            # Check if we should clean real-time data due to inactivity
            cleaned = clean_realtime_data(idle_minutes=60)
            
            # Wait before next check
            await asyncio.sleep(15 * 60)  # Check every 15 minutes
    except asyncio.CancelledError:
        print("Consolidation task cancelled")
    except Exception as e:
        print(f"Error in scheduled consolidation: {e}")

from db import get_conn

async def fetch_and_store_historical_ohlcv(symbol: str):
    """
    Fetch historical daily OHLCV data from Alpha Vantage and store it in the database.
    If the symbol already has data in the historical_ohlcv table, skip fetching.
    
    Args:
        symbol: Stock symbol (e.g., 'AAPL')
    """
    # Check if the symbol already has data in the historical_ohlcv table
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM historical_ohlcv WHERE symbol = %s", (symbol,))
        record_count = cur.fetchone()[0]
        
        if record_count > 0:
            print(f"Data for {symbol} already exists in the historical_ohlcv table. Skipping fetch.")
            return
    except Exception as e:
        print(f"Error checking existing data for {symbol}: {e}")
        return
    finally:
        cur.close()
        conn.close()

    # Fetch data from Alpha Vantage
    print(f"Fetching historical daily OHLCV data for {symbol}...")
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={ALPHA_VANTAGE_API_KEY}&datatype=csv"
    
    try:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error fetching data for {symbol}: {response.status_code} - {response.text}")
            return
        
        # Parse the CSV response into a DataFrame
        data = pd.read_csv(io.StringIO(response.text))
        if data.empty:
            print(f"No data returned for {symbol}.")
            return
        
        # Insert data into the database
        for _, row in data.iterrows():
            print(f"Inserting data for {symbol} on {row['timestamp']}")
            insert_historical_daily(
                symbol=symbol,
                timestamp=pd.Timestamp(row['timestamp']).timestamp(),
                open_price=row['open'],
                high=row['high'],
                low=row['low'],
                close=row['close'],
                volume=row['volume'],
                source="AlphaVantage"
            )
        
        print(f"Successfully inserted historical daily OHLCV data for {symbol}.")
    
    except Exception as e:
        print(f"Error fetching or storing data for {symbol}: {e}")

if __name__ == "__main__":
    from db import init_db
    init_db()
    asyncio.run(fetch_and_store_historical_ohlcv('AAPL'))
    asyncio.run(subscribe_and_produce(
        symbol="AAPL", 
        fetch_history=True,
        interval="5min",
    ))