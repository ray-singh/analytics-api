import asyncio
import json
import os
import logging
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv
from services.persistence.src.repositories.price_repository import PriceRepository
from services.persistence.src.repositories.ohlcv_repository import OHLCVRepository
from services.persistence.src.repositories.analytics_repository import AnalyticsRepository
from services.persistence.src.repositories.realtime_analytics_repository import RealTimeAnalyticsRepository

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("persistence-service")

load_dotenv()
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/stockanalytics")

# Initialize repositories
price_repo = PriceRepository(DATABASE_URL)
ohlcv_repo = OHLCVRepository(DATABASE_URL)
analytics_repo = AnalyticsRepository(DATABASE_URL)
realtime_analytics_repo = RealTimeAnalyticsRepository(DATABASE_URL)

async def consume_raw_prices():
    """Consume raw price events and store them in the database"""
    consumer = AIOKafkaConsumer(
        "market.prices.raw",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="persistence-service-raw",
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode())
    )
    await consumer.start()
    
    try:
        logger.info("Started consuming from market.prices.raw")
        async for msg in consumer:
            event = msg.value
            try:
                await price_repo.insert_realtime_price(
                    symbol=event["symbol"],
                    price=event["price"],
                    timestamp=event["timestamp"],
                    volume=event.get("volume", 0),
                    source=event.get("source", "unknown")
                    )
            except Exception as e:
                logger.error(f"Error processing raw price: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Consumer error: {e}", exc_info=True)
    finally:
        await consumer.stop()
        logger.info("Stopped consuming from market.prices.raw")

async def consume_ohlcv_bars():
    """Consume OHLCV bar events and store them in the database"""
    consumer = AIOKafkaConsumer(
        "market.prices.ohlcv",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="persistence-service-ohlcv",
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode())
    )
    await consumer.start()
    
    try:
        logger.info("Started consuming from market.prices.ohlcv")
        async for msg in consumer:
            bar = msg.value
            try:
                if bar["event_type"] == "price.ohlcv":
                    # Determine if this is intraday or daily data
                    if bar["interval_minutes"] < 1440:  # Less than a day
                        await ohlcv_repo.insert_intraday_ohlcv(
                            symbol=bar["symbol"],
                            timestamp=bar["bar_end_ts"],
                            open_price=bar["open"],
                            high=bar["high"],
                            low=bar["low"],
                            close=bar["close"],
                            volume=bar["volume"],
                            interval_minutes=bar["interval_minutes"],
                            source=bar.get("source", "aggregator")
                        )
                    else:  # Daily data
                        await ohlcv_repo.insert_historical_daily(
                            symbol=bar["symbol"],
                            timestamp=bar["bar_end_ts"],
                            open_price=bar["open"],
                            high=bar["high"],
                            low=bar["low"],
                            close=bar["close"],
                            volume=bar["volume"],
                            source=bar.get("source", "aggregator")
                        )
            except Exception as e:
                logger.error(f"Error processing OHLCV bar: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Consumer error: {e}", exc_info=True)
    finally:
        await consumer.stop()
        logger.info("Stopped consuming from market.prices.ohlcv")

async def consume_analytics():
    """Consume analytics events and store them in the database"""
    consumer = AIOKafkaConsumer(
        "market.analytics",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="persistence-service-analytics",
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode())
    )
    await consumer.start()
    
    try:
        logger.info("Started consuming from market.analytics")
        async for msg in consumer:
            event = msg.value
            try:
                # Determine if this is intraday or daily data
                if event["interval_minutes"] < 1440:  # Less than a day
                    await analytics_repo.insert_intraday_analytics(
                        symbol=event["symbol"],
                        timestamp=event["timestamp"],
                        price=event["price"],
                        interval_minutes=event["interval_minutes"],
                        indicators=event["indicators"]
                    )
                else:  # Daily data
                    await analytics_repo.insert_daily_analytics(
                        symbol=event["symbol"],
                        timestamp=event["timestamp"],
                        price=event["price"],
                        indicators=event["indicators"]
                    )
            except Exception as e:
                logger.error(f"Error processing analytics: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Consumer error: {e}", exc_info=True)
    finally:
        await consumer.stop()
        logger.info("Stopped consuming from market.analytics")

async def consume_realtime_analytics():
    """Consume real-time analytics events and store them in the database"""
    consumer = AIOKafkaConsumer(
        "market.analytics.realtime",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="persistence-service-realtime-analytics",
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode())
    )
    await consumer.start()
    
    try:
        logger.info("Started consuming from market.analytics.realtime")
        async for msg in consumer:
            event = msg.value
            try:
                if event.get("event_type") == "analytics.realtime":
                    symbol = event.get("symbol")
                    timestamp = event.get("timestamp")
                    price = event.get("price")
                    indicators = event.get("indicators", {})
                    
                    if symbol and timestamp and price and indicators:
                        try:
                            await realtime_analytics_repo.insert_realtime_analytics(
                                symbol, timestamp, price, indicators
                            )
                            logger.debug(f"Stored real-time analytics for {symbol}")
                        except Exception as e:
                            logger.error(f"Error storing real-time analytics: {e}")
                    else:
                        logger.warning(f"Incomplete real-time analytics event: {event}")
            except Exception as e:
                logger.error(f"Error processing real-time analytics: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Consumer error: {e}", exc_info=True)
    finally:
        await consumer.stop()
        logger.info("Stopped consuming from market.analytics.realtime")

async def initialize_database():
    """Initialize database schema if needed"""
    try:
        # Initialize database schema
        await price_repo.init_db()
        await ohlcv_repo.init_db()
        await analytics_repo.init_db()
        await realtime_analytics_repo.init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {e}", exc_info=True)
        raise

async def main():
    """Main entry point for the persistence service"""
    logger.info("Starting persistence service")
    
    # Initialize database
    await initialize_database()
    
    # Run consumers in parallel
    await asyncio.gather(
        consume_raw_prices(),
        consume_ohlcv_bars(),
        consume_analytics(),
        consume_realtime_analytics()
    )

if __name__ == "__main__":
    asyncio.run(main())