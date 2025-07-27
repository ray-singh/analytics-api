import asyncio
import json
import random
from aiokafka import AIOKafkaProducer

KAFKA_TOPIC = "stock_prices"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

async def produce_stock_prices():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        while True:
            event = {
                "symbol": "AAPL",
                "price": round(random.uniform(190, 200), 2),
                "timestamp": asyncio.get_event_loop().time()
            }
            await producer.send_and_wait(KAFKA_TOPIC, json.dumps(event).encode())
            print(f"Produced: {event}")
            await asyncio.sleep(1)
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(produce_stock_prices()) 