import asyncio
import json
from collections import defaultdict, deque
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from db import insert_analytics

KAFKA_TOPIC_IN = "stock_prices"
KAFKA_TOPIC_OUT = "analytics"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
WINDOW_SIZE = 5

symbol_prices = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))

async def consume_and_analyze():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="analytics-group"
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await consumer.start()
    await producer.start()
    try:
        async for msg in consumer:
            event = json.loads(msg.value)
            symbol = event["symbol"]
            price = event["price"]
            timestamp = event["timestamp"]
            symbol_prices[symbol].append(price)
            if len(symbol_prices[symbol]) == WINDOW_SIZE:
                moving_avg = sum(symbol_prices[symbol]) / WINDOW_SIZE
                analytics_event = {
                    "symbol": symbol,
                    "moving_average": moving_avg,
                    "timestamp": timestamp
                }
                await producer.send_and_wait(KAFKA_TOPIC_OUT, json.dumps(analytics_event).encode())
                insert_analytics(symbol, "moving_average", moving_avg, timestamp, window_size=WINDOW_SIZE)
                print(f"Published analytics: {analytics_event}")
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    from db import init_db
    init_db()
    asyncio.run(consume_and_analyze())
