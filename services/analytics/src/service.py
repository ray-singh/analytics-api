import asyncio
import json
import os
import logging
from datetime import datetime
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import pandas as pd
import services.analytics.src.indicator.momentum as momentum
import services.analytics.src.indicator.volatility as volatility
from services.analytics.src.indicator.realtime import calculate_real_time_indicators
from prometheus_client import start_http_server, Counter, Histogram

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("analytics-service")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "analytics-service")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "market.prices.ohlcv")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "market.analytics")
MAX_HISTORY = int(os.getenv("MAX_HISTORY", "200"))  

start_http_server(8001)

# In-memory price histories for technical analysis
# Structure: {symbol: {interval: pd.DataFrame}}
price_history = {}

# Prometheus metrics
PROCESSING_TIME = Histogram("analytics_processing_time", "Time spent processing analytics events", ["symbol", "interval"])

kafka_messages_consumed_total = Counter(
    "kafka_messages_consumed_total",
    "Total Kafka messages consumed",
    ["job"]
)

ANALYTICS_ERRORS = Counter("analytics_errors_total", "Total analytics errors")

async def process_ohlcv_bar(producer, event):
    """Process an OHLCV bar event and calculate analytics"""
    symbol = event["symbol"]
    interval = event["interval_minutes"]
    
    bar_timestamp = event.get("bar_start_ts") or event.get("timestamp") or event.get("bar_end_ts")
    
    if bar_timestamp is None:
        logger.warning(f"Missing timestamp in event: {event}")
        return
    
    # Initialize nested dictionaries if needed
    if symbol not in price_history:
        price_history[symbol] = {}
    if interval not in price_history[symbol]:
        price_history[symbol][interval] = pd.DataFrame(
            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
        )
    
    # Add the new bar to history
    new_row = pd.DataFrame([{
        'timestamp': bar_timestamp,
        'open': event["open"],
        'high': event["high"],
        'low': event["low"],
        'close': event["close"],
        'volume': event["volume"]
    }])

    if price_history[symbol][interval].empty:
        df = new_row
    else:
        df = pd.concat([price_history[symbol][interval], new_row], ignore_index=True)
    
    df = df.sort_values(by='timestamp').reset_index(drop=True)
    
    # Trim to keep only recent history
    if len(df) > MAX_HISTORY:
        df = df.tail(MAX_HISTORY)
    
    price_history[symbol][interval] = df
    
    if len(df) < 30:
        logger.info(f"Not enough data for {symbol} {interval}min ({len(df)} bars). Need 30.")
        return
    
    # Calculate indicators
    indicators = {}
    
    # Calculate momentum indicators (RSI, MACD, etc.)
    momentum_indicators = momentum.calculate_momentum_indicators(df)
    indicators.update(momentum_indicators)
    
    # Calculate volatility indicators (Bollinger Bands, ATR, etc.)
    volatility_indicators = volatility.calculate_volatility_indicators(df)
    indicators.update(volatility_indicators)
    
    if indicators:
        # Create analytics event
        analytics_event = {
            "event_type": "analytics.technical",
            "version": "1.0",
            "symbol": symbol,
            "interval_minutes": interval,
            "timestamp": event.get("bar_end_ts", bar_timestamp),
            "price": event["close"],
            "indicators": indicators
        }
        
        # symbol + interval as key for partitioning
        key = f"{symbol}:{interval}".encode()
        await producer.send(OUTPUT_TOPIC, key=key, value=json.dumps(analytics_event).encode())
        
        logger.info(f"Published analytics for {symbol} {interval}min: RSI={indicators.get('rsi_14', 'N/A')}")

async def consume_ohlcv_bars():
    """Consume OHLCV bar events and produce analytics events"""
    logger.info(f"Starting analytics service. Consuming from {INPUT_TOPIC}")
    
    # Create consumer
    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode()),
        enable_auto_commit=True
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP
    )
    
    await producer.start()
    await consumer.start()
    
    try:
        # Process messages
        async for msg in consumer:
            try:
                event = msg.value
                
                # Log the event for debugging
                logger.debug(f"Received event: {event}")
                
                # Validate event structure (we need OHLC data and a timestamp field)
                required_fields = ["symbol", "interval_minutes", "open", "high", "low", "close"]
                timestamp_fields = ["timestamp", "bar_end_ts"]
                
                if (event.get("event_type") == "price.ohlcv" and 
                    all(k in event for k in required_fields) and
                    any(t in event for t in timestamp_fields)):
                    kafka_messages_consumed_total.labels(job="analytics").inc()
                    await process_ohlcv_bar(producer, event)
                else:
                    missing = [k for k in required_fields if k not in event]
                    if not any(t in event for t in timestamp_fields):
                        missing.append("timestamp field")
                    logger.warning(f"Invalid event structure: {event.get('event_type', 'unknown')}, missing: {missing}")
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                ANALYTICS_ERRORS.inc()
    finally:
        # Clean up
        await consumer.stop()
        await producer.stop()
        logger.info("Analytics service stopped")

async def consume_real_time_prices():
    """Consume real-time price ticks and produce analytics events"""
    logger.info(f"Starting real-time analytics service. Consuming from market.prices.raw")
    consumer = AIOKafkaConsumer(
        "market.prices.raw",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=f"{CONSUMER_GROUP}-realtime",
        auto_offset_reset="latest",  # Use latest for real-time data
        value_deserializer=lambda v: json.loads(v.decode()),
        enable_auto_commit=True
    )
    
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP
    )
    
    await producer.start()
    await consumer.start()
    
    # In-memory state for real-time analytics
    realtime_state = {}
    try:
        async for msg in consumer:
            try:
                event = msg.value
                await process_real_time_tick(producer, event, realtime_state)
                kafka_messages_consumed_total.labels(job="analytics").inc()
                
            except Exception as e:
                logger.error(f"Error processing real-time message: {e}", exc_info=True)
                ANALYTICS_ERRORS.inc()
    finally:
        await consumer.stop()
        await producer.stop()
        logger.info("Real-time analytics service stopped")

async def process_real_time_tick(producer, event, realtime_state):
    """Process a real-time price tick and calculate analytics"""
    # Extract fields
    symbol = event.get("symbol")
    price = event.get("price")
    timestamp = event.get("timestamp")
    volume = event.get("volume", 0)
    
    if not all([symbol, price, timestamp]):
        logger.warning(f"Missing required fields in real-time event: {event}")
        return
    
    # Initialize state for this symbol if needed
    if symbol not in realtime_state:
        realtime_state[symbol] = {
            "prices": [],
            "timestamps": [],
            "volumes": [],
            "last_analytics_time": None
        }
    
    # Add data to state
    state = realtime_state[symbol]
    state["prices"].append(float(price))
    state["timestamps"].append(timestamp)
    state["volumes"].append(int(volume) if volume else 0)
    
    # Keep a rolling window of recent ticks (last 200)
    max_ticks = 200
    if len(state["prices"]) > max_ticks:
        state["prices"] = state["prices"][-max_ticks:]
        state["timestamps"] = state["timestamps"][-max_ticks:]
        state["volumes"] = state["volumes"][-max_ticks:]
    
    if len(state["prices"]) < 30:
        return
    
    # Rate limit analytics calculations (max once per second per symbol)
    current_time = datetime.now()
    if (state["last_analytics_time"] and 
        (current_time - state["last_analytics_time"]).total_seconds() < 1):
        return
    
    # Calculate real-time indicators
    indicators = calculate_real_time_indicators(state)
    state["last_analytics_time"] = current_time
    
    # Create and publish analytics event
    analytics_event = {
        "event_type": "analytics.realtime",
        "version": "1.0",
        "symbol": symbol,
        "timestamp": timestamp,
        "price": price,
        "indicators": indicators
    }
    
    # Use symbol as key for partitioning
    key = f"{symbol}:realtime".encode()
    await producer.send(OUTPUT_TOPIC + ".realtime", key=key, 
                        value=json.dumps(analytics_event).encode())
    
    logger.debug(f"Published real-time analytics for {symbol}")

async def main():
    """Main entry point for the analytics service"""
    try:
        await asyncio.gather(
            consume_ohlcv_bars(),
            consume_real_time_prices()
        )
    except KeyboardInterrupt:
        logger.info("Service interrupted")
    except Exception as e:
        logger.error(f"Service error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())