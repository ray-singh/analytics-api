import asyncio
import json
import os
import logging
from datetime import datetime
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import start_http_server
from .aggregators.bar_aggregator import BarAggregator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("aggregation-service")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "market.prices.raw")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "market.prices.ohlcv")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "aggregation-service")

# Start metrics server on port 8001
start_http_server(8001)

async def run_service():
    """Main service function that processes raw price events into OHLCV bars"""
    logger.info(f"Starting aggregation service, consuming from {INPUT_TOPIC}")
    
    # Initialize the bar aggregators for different timeframes
    aggregator = BarAggregator()
    
    # Start Kafka consumer
    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode())
    )
    
    # Start Kafka producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP
    )
    
    # Start the consumer and producer
    await consumer.start()
    await producer.start()
    
    logger.info("Successfully connected to Kafka")
    
    # Health check task
    health_check_task = asyncio.create_task(periodic_health_check())
    
    try:
        # Process incoming messages
        async for msg in consumer:
            try:
                event = msg.value
                
                # Log the received event for debugging
                logger.debug(f"Received event: {event}")
                
                # Check if we have all required fields
                if "symbol" in event and "price" in event and "timestamp" in event:
                    symbol = event["symbol"]
                    price = float(event["price"])
                    timestamp = event["timestamp"]  
                    volume = event.get("volume", 0)
                    
                    # Update aggregators with new price
                    completed_bars = aggregator.update(symbol, timestamp, price, volume)
                    
                    # Publish completed bars
                    for interval, bar in completed_bars.items():
                        # Use symbol + interval as key for partitioning
                        key = f"{symbol}:{interval}".encode()
                        
                        # Calculate the actual end timestamp for this bar
                        window_start = bar["timestamp"]
                        window_end = window_start + (interval * 60)
                        
                        # Add required fields for downstream processing
                        bar["event_type"] = "price.ohlcv"
                        bar["interval_minutes"] = interval  # Add the interval_minutes field
                        bar["bar_start_ts"] = window_start  # Explicitly add start timestamp
                        bar["bar_end_ts"] = window_end  # Set end timestamp correctly
                        
                        await producer.send(
                            OUTPUT_TOPIC, 
                            key=key, 
                            value=json.dumps(bar).encode()
                        )
                        logger.info(f"Published {interval}min bar for {symbol}: {bar['close']}")
                else:
                    logger.warning(f"Skipping event, missing required fields: {event}")
            
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                
    except Exception as e:
        logger.error(f"Aggregation service error: {e}", exc_info=True)
    finally:
        # Clean up
        health_check_task.cancel()
        await consumer.stop()
        await producer.stop()
        logger.info("Aggregation service shut down")

async def periodic_health_check():
    """Periodically log health status"""
    while True:
        logger.info("Aggregation service health check: OK")
        await asyncio.sleep(60)  # Check every minute

def main():
    """Entry point for the service"""
    try:
        asyncio.run(run_service())
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1
    return 0

if __name__ == "__main__":
    exit(main())