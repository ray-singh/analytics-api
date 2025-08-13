import asyncio
import json
import time
from aiokafka import AIOKafkaProducer

async def test_kafka():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        client_id='test-client'
    )
    
    try:
        await producer.start()
        print("Producer started")
        
        # Send a simple message
        message = {"test": "message", "time": time.time()}
        print(f"Sending: {message}")
        
        result = await producer.send_and_wait("test-topic", json.dumps(message).encode())
        print(f"Message sent to partition {result.partition}, offset {result.offset}")
        
    finally:
        await producer.stop()
        print("Producer stopped")

if __name__ == "__main__":
    asyncio.run(test_kafka())