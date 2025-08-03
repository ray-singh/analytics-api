import asyncio
import json
import random
from aiokafka import AIOKafkaProducer
from datetime import datetime
from db import insert_price

KAFKA_TOPIC = "stock_prices"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SYMBOLS = ["AAPL", "MSFT", "TSLA"]

async def produce_stock_prices():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        while True:
            for symbol in SYMBOLS:
                price = round(random.uniform(100, 300), 2)
                timestamp = datetime.utcnow().timestamp()
                event = {
                    "symbol": symbol,
                    "price": price,
                    "timestamp": timestamp
                }
                await producer.send_and_wait(KAFKA_TOPIC, json.dumps(event).encode())
                insert_price(symbol, price, timestamp)
                print(f"Produced: {event}")
            await asyncio.sleep(1)
    finally:
        await producer.stop()

if __name__ == "__main__":
    from db import init_db
    init_db()
    asyncio.run(produce_stock_prices()) 