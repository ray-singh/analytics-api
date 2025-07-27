import asyncio
import json
from collections import deque
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

KAFKA_TOPIC_IN = "stock_prices"
KAFKA_TOPIC_OUT = "analytics"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
WINDOW_SIZE = 5

prices = deque(maxlen=WINDOW_SIZE)

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
            prices.append(event["price"])
            if len(prices) == WINDOW_SIZE:
                moving_avg = sum(prices) / WINDOW_SIZE
                analytics_event = {
                    "symbol": event["symbol"],
                    "moving_average": moving_avg,
                    "timestamp": event["timestamp"]
                }
                await producer.send_and_wait(KAFKA_TOPIC_OUT, json.dumps(analytics_event).encode())
                print(f"Published analytics: {analytics_event}")
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(consume_and_analyze())
