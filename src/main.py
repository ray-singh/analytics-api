from fastapi import FastAPI
from typing import Optional
import asyncio
import json
from aiokafka import AIOKafkaConsumer

app = FastAPI()
latest_analytics = {}

@app.get("/analytics/{symbol}")
def get_analytics(symbol: str):
    return latest_analytics.get(symbol, {"error": "No data"})

KAFKA_TOPIC = "analytics"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

async def consume_analytics():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="api-group"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            event = json.loads(msg.value)
            latest_analytics[event["symbol"]] = event
    finally:
        await consumer.stop()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(consume_analytics())