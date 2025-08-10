import asyncio
import json
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from db import insert_intraday_analytics, insert_daily_analytics
from analytics_engine import TechnicalIndicators

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('analytics_service')

# Kafka configuration
KAFKA_TOPIC_IN = "stock_prices"
KAFKA_TOPIC_OUT = "analytics"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Initialize indicators engine
indicators_engine = TechnicalIndicators()

# Maintain price history and window sizes
symbol_prices = defaultdict(lambda: deque(maxlen=100))  # Increased for longer indicators
symbol_timestamps = defaultdict(list)
symbol_volumes = defaultdict(list)

# Define timeframes for calculation
INTRADAY_WINDOWS = [5, 10, 20, 50]  # Different window sizes for intraday calculations
DAILY_WINDOWS = [20, 50, 200]        # Standard windows for daily calculations

async def consume_and_analyze():
    """Main function to consume price events and calculate indicators"""
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="analytics-group",
        auto_offset_reset="latest"
    )
    
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    
    await consumer.start()
    await producer.start()
    
    # Start daily calculation task
    daily_task = asyncio.create_task(calculate_daily_indicators())
    
    try:
        async for msg in consumer:
            try:
                # Parse the incoming price event
                event = json.loads(msg.value)
                symbol = event["symbol"]
                price = event["price"]
                timestamp = event["timestamp"]
                volume = event.get("volume", 0)
                
                # Store in our local history
                symbol_prices[symbol].append(price)
                symbol_timestamps[symbol].append(timestamp)
                symbol_volumes[symbol].append(volume)
                
                # Add to the indicators engine
                indicators_engine.add_price(
                    symbol=symbol, 
                    price=price, 
                    volume=volume, 
                    timestamp=datetime.fromtimestamp(timestamp)
                )
                
                # Only calculate indicators when we have enough data points
                min_window = min(INTRADAY_WINDOWS)
                if len(symbol_prices[symbol]) >= min_window:
                    await calculate_intraday_indicators(symbol, timestamp)
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                
    except asyncio.CancelledError:
        logger.info("Consumer task cancelled")
    except Exception as e:
        logger.error(f"Unexpected error in consumer: {e}", exc_info=True)
    finally:
        daily_task.cancel()
        await consumer.stop()
        await producer.stop()

async def calculate_intraday_indicators(symbol, timestamp):
    """Calculate intraday technical indicators and store in database"""
    try:
        # Get timestamp as datetime
        dt_timestamp = datetime.fromtimestamp(timestamp)
        
        # Round timestamp to nearest 5-minute interval
        rounded_timestamp = dt_timestamp.replace(
            minute=(dt_timestamp.minute // 5) * 5,
            second=0, 
            microsecond=0
        )
        
        # Calculate indicators for various windows
        indicators = {}
        
        # Simple Moving Averages
        for window in INTRADAY_WINDOWS:
            sma = indicators_engine.simple_moving_average(symbol, window)
            if sma is not None:
                indicators[f'sma_{window}'] = sma
        
        # Exponential Moving Averages
        for window in INTRADAY_WINDOWS:
            ema = indicators_engine.exponential_moving_average(symbol, window)
            if ema is not None:
                indicators[f'ema_{window}'] = ema
        
        # RSI (14 is standard)
        rsi = indicators_engine.relative_strength_index(symbol, 14)
        if rsi is not None:
            indicators['rsi_14'] = rsi
            
        # MACD (standard parameters)
        macd_data = indicators_engine.macd(symbol)
        if macd_data:
            indicators['macd'] = macd_data['macd_line']
            indicators['macd_signal'] = macd_data['signal_line']
            indicators['macd_histogram'] = macd_data['histogram']
            
        # Bollinger Bands (standard 20-period)
        bb_data = indicators_engine.bollinger_bands(symbol)
        if bb_data:
            indicators['bollinger_upper'] = bb_data['upper_band']
            indicators['bollinger_lower'] = bb_data['lower_band']
            indicators['bollinger_middle'] = bb_data['middle_band']
        
        # Only store if we have at least some indicators calculated
        if indicators:
            # Store in database
            insert_intraday_analytics(
                symbol=symbol,
                timestamp=rounded_timestamp,
                interval_minutes=5,
                **indicators
            )
            
            # Log the calculation
            logger.info(f"Calculated intraday indicators for {symbol} at {rounded_timestamp}")
            
            # Create analytics event for Kafka
            analytics_event = {
                "symbol": symbol,
                "timestamp": timestamp,
                "interval": "5min",
                "indicators": indicators
            }
            
            # Publish to Kafka
            producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            await producer.start()
            await producer.send_and_wait(
                KAFKA_TOPIC_OUT, 
                json.dumps(analytics_event).encode()
            )
            await producer.stop()
            
    except Exception as e:
        logger.error(f"Error calculating intraday indicators for {symbol}: {e}", exc_info=True)

async def calculate_daily_indicators():
    """
    Periodically calculate daily indicators from consolidated daily data.
    This runs on a schedule, independent of the real-time data stream.
    """
    try:
        while True:
            # Run once per day, typically after market close
            now = datetime.now()
            # If it's after 4:30 PM ET (market close + 30 min)
            if now.hour >= 16 and now.minute >= 30:
                logger.info("Starting daily indicators calculation")
                
                # Get list of all tracked symbols
                symbols = list(symbol_prices.keys())
                
                for symbol in symbols:
                    try:
                        # We'll pull daily data directly from our historical_ohlcv table
                        from db import get_conn
                        from psycopg2.extras import RealDictCursor
                        
                        conn = get_conn()
                        cur = conn.cursor(cursor_factory=RealDictCursor)
                        
                        # Get last 200 days of data (enough for our longest MA)
                        cur.execute("""
                            SELECT * FROM historical_ohlcv
                            WHERE symbol = %s
                            ORDER BY timestamp DESC
                            LIMIT 200
                        """, (symbol,))
                        
                        daily_data = cur.fetchall()
                        cur.close()
                        conn.close()
                        
                        if not daily_data:
                            logger.warning(f"No daily data available for {symbol}")
                            continue
                            
                        # Process the data
                        daily_data.reverse()  # Oldest first for calculations
                        
                        # Extract close prices for indicators
                        close_prices = [float(d['close']) for d in daily_data]
                        
                        # Create a temporary indicators engine for daily data
                        daily_engine = TechnicalIndicators()
                        
                        # Add all prices to the engine
                        for day in daily_data:
                            daily_engine.add_price(
                                symbol=symbol,
                                price=float(day['close']),
                                volume=day['volume'],
                                timestamp=day['timestamp']
                            )
                        
                        # Calculate indicators
                        daily_indicators = {}
                        
                        # Moving Averages for each window
                        for window in DAILY_WINDOWS:
                            sma = daily_engine.simple_moving_average(symbol, window)
                            if sma is not None:
                                daily_indicators[f'sma_{window}'] = sma
                                
                            ema = daily_engine.exponential_moving_average(symbol, window)
                            if ema is not None:
                                daily_indicators[f'ema_{window}'] = ema
                        
                        # RSI
                        rsi = daily_engine.relative_strength_index(symbol, 14)
                        if rsi is not None:
                            daily_indicators['rsi_14'] = rsi
                            
                        # MACD
                        macd_data = daily_engine.macd(symbol)
                        if macd_data:
                            daily_indicators['macd'] = macd_data['macd_line']
                            daily_indicators['macd_signal'] = macd_data['signal_line']
                            daily_indicators['macd_histogram'] = macd_data['histogram']
                            
                        # Bollinger Bands
                        bb_data = daily_engine.bollinger_bands(symbol)
                        if bb_data:
                            daily_indicators['bollinger_upper'] = bb_data['upper_band']
                            daily_indicators['bollinger_lower'] = bb_data['lower_band']
                            daily_indicators['bollinger_middle'] = bb_data['middle_band']
                            
                        # ATR
                        atr = daily_engine.average_true_range(symbol)
                        if atr is not None:
                            daily_indicators['atr_14'] = atr
                        
                        # Store indicators in the database
                        if daily_indicators:
                            today = now.date()
                            insert_daily_analytics(
                                symbol=symbol,
                                date=today,
                                **daily_indicators
                            )
                            logger.info(f"Calculated daily indicators for {symbol} on {today}")
                    
                    except Exception as e:
                        logger.error(f"Error calculating daily indicators for {symbol}: {e}", exc_info=True)
                
                # Sleep until tomorrow
                tomorrow = (now + timedelta(days=1)).replace(hour=16, minute=30, second=0)
                sleep_seconds = (tomorrow - now).total_seconds()
                await asyncio.sleep(sleep_seconds)
            else:
                # Check again in an hour
                await asyncio.sleep(3600)
                
    except asyncio.CancelledError:
        logger.info("Daily calculation task cancelled")
    except Exception as e:
        logger.error(f"Error in daily calculation task: {e}", exc_info=True)

if __name__ == "__main__":
    from db import init_db
    init_db()
    logger.info("Starting analytics service")
    asyncio.run(consume_and_analyze())
