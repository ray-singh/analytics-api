import asyncio
import json
import ssl
import certifi
import os
import pandas as pd
from twelvedata import TDClient
from aiokafka import AIOKafkaProducer
from db import insert_realtime_price, insert_intraday_ohlcv
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

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

async def subscribe_and_produce(symbol: str, fetch_history=False, interval="5min", days_back=365):
    """
    Subscribe to real-time stock price updates and produce them to Kafka.
    Optionally fetch historical data first.
    """
    # Fetch historical data if requested
    if fetch_history:
        print(f"Fetching historical data for {symbol} before starting real-time stream...")
        await init_historical_data([symbol], interval, days_back)
    
    # Set up real-time streaming
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    loop = asyncio.get_event_loop()

    def on_event(data):
        if data["event"] == "price":
            print(data)
            price = float(data["price"])
            timestamp = int(data["timestamp"])
            # Use get() with default value to handle missing volume
            volume = int(data.get("volume", 0))
            event = {
                "symbol": symbol,
                "price": price,
                "timestamp": timestamp,
                "volume": volume
            }

            # Schedule the Kafka producer task on the main event loop
            asyncio.run_coroutine_threadsafe(
                handle_event(event, producer), loop
            )

    async def handle_event(event, producer):
        """
        Handle the event by producing it to Kafka and storing it in the database.
        """
        await producer.send_and_wait(KAFKA_TOPIC, json.dumps(event).encode())
        print(f"Produced to Kafka: {event}")
        insert_realtime_price(
            symbol = event["symbol"], 
            price = event["price"], 
            timestamp=event["timestamp"], 
            volume=event.get("day_volume", None),
            source="TwelveData-RealTime"
        )
        
    ws = td.websocket(
        symbols=symbol,
        on_event=on_event,
        ssl_context=ssl.create_default_context(cafile=certifi.where())
    )
    ws.subscribe(symbol)
    ws.connect()

    try:
        while True:
            ws.heartbeat()  # Send a heartbeat to keep the connection alive
            await asyncio.sleep(10)
    except KeyboardInterrupt:
        ws.close()
    finally:
        await producer.stop()

if __name__ == "__main__":
    from db import init_db
    init_db()
    asyncio.run(subscribe_and_produce(
        symbol="AAPL", 
        fetch_history=True,
        interval="5min",
        days_back=365  # Fetch 1 year of data
    ))