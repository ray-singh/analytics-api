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

            # Compute moving average when enough data is available
            if len(symbol_prices[symbol]) == WINDOW_SIZE:
                moving_avg = sum(symbol_prices[symbol]) / WINDOW_SIZE
                analytics_event = {
                    "symbol": symbol,
                    "moving_average": moving_avg,
                    "timestamp": timestamp
                }

                # Produce analytics event to Kafka
                await producer.send_and_wait(KAFKA_TOPIC_OUT, json.dumps(analytics_event).encode())
                print(f"Published analytics: {analytics_event}")

                # Store analytics in the database
                insert_analytics(symbol, "moving_average", moving_avg, timestamp, window_size=WINDOW_SIZE)
    finally:
        await consumer.stop()
        await producer.stop()

def on_event(data):
    if data["event"] == "price":
        price = float(data["price"])
        timestamp = int(data["timestamp"])
        volume = int(data["volume"])
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
    insert_price(event["symbol"], event["price"], event["timestamp"], event["volume"])

if __name__ == "__main__":
    from db import init_db
    init_db()
    asyncio.run(consume_and_analyze())
