import asyncio
import os
import json
import asyncpg
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/stockanalytics")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "market.prices.ohlcv"

async def fetch_ohlcv_bars(table_name):
    """
    Fetch OHLCV bars from the specified table in the database.

    Args:
        table_name (str): Name of the table to fetch bars from ("intraday_ohlcv" or "historical_ohlcv").

    Returns:
        list: List of asyncpg.Record objects representing OHLCV bars.
    """
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        if table_name == "intraday_ohlcv":
            rows = await conn.fetch("""
                SELECT symbol, open, high, low, close, volume, interval_minutes, timestamp
                FROM intraday_ohlcv
                ORDER BY timestamp ASC
            """)
        elif table_name == "historical_ohlcv":
            rows = await conn.fetch("""
                SELECT symbol, open, high, low, close, volume, timestamp
                FROM historical_ohlcv
                ORDER BY timestamp ASC
            """)
        else:
            rows = []
            
    except Exception as e:
        logger.error(f"Error fetching from {table_name}: {e}")
        rows = []
    finally:
        await conn.close()
    
    return rows

async def publish_bars_to_kafka(bars, producer, interval_minutes=None):
    """
    Publish OHLCV bars to a Kafka topic.

    Args:
        bars (list): List of asyncpg.Record objects representing OHLCV bars.
        producer (AIOKafkaProducer): Kafka producer instance.
        interval_minutes (int, optional): Interval in minutes for the bars (used for daily bars).
    """
    for row in bars:
        try:
            if "interval_minutes" in row:
                interval = row["interval_minutes"]
            else:
                interval = interval_minutes
            
            if interval is None:
                logger.warning(f"No interval specified for {row.get('symbol', 'unknown')}")
                continue
                
            event = {
                "event_type": "price.ohlcv",
                "symbol": row["symbol"],
                "interval_minutes": interval,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row["volume"]),
                "timestamp": row["timestamp"].isoformat() if hasattr(row["timestamp"], 'isoformat') else str(row["timestamp"])
            }
            
            await producer.send_and_wait(TOPIC, value=json.dumps(event).encode())
            logger.info(f"Published OHLCV bar for {row.get('symbol', 'unknown')}")
        except Exception as e:
            logger.error(f"Error publishing bar for {row.get('symbol', 'unknown')}: {e}")
            continue

async def main():
    """
    Main entry point for the backfill script.
    Fetches OHLCV bars from the database and publishes them to Kafka for analytics backfill.
    """
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    
    try:
        # Process intraday bars
        logger.info("Fetching intraday OHLCV bars...")
        intraday_bars = await fetch_ohlcv_bars("intraday_ohlcv")
        logger.info(f"Publishing {len(intraday_bars)} intraday bars to Kafka...")
        await publish_bars_to_kafka(intraday_bars, producer)
        
        # Process daily bars
        logger.info("Fetching daily OHLCV bars...")
        daily_bars = await fetch_ohlcv_bars("historical_ohlcv")
        logger.info(f"Publishing {len(daily_bars)} daily bars to Kafka...")
        await publish_bars_to_kafka(daily_bars, producer, interval_minutes=1440)
        
        logger.info("Backfill complete.")
        
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        raise
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())