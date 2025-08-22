from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict, List, Set, Any
import json
import asyncio
from datetime import datetime
import logging
from aiokafka import AIOKafkaConsumer
import os

logger = logging.getLogger(__name__)

class ConnectionManager:
    """
    WebSocket connection manager to handle client connections
    and broadcast messages to subscribers.
    """
    
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}
        self.subscriptions: Dict[WebSocket, Set[str]] = {}
        self.consumer_task = None
        self.consumers: Dict[str, AIOKafkaConsumer] = {}
        self.running = False
    
    async def connect(self, websocket: WebSocket, client_id: str = None):
        """
        Connect a WebSocket client
        """
        await websocket.accept()
        if client_id is None:
            client_id = str(id(websocket))
            
        if client_id not in self.active_connections:
            self.active_connections[client_id] = []
        
        self.active_connections[client_id].append(websocket)
        self.subscriptions[websocket] = set()
        
        # Start Kafka consumer if not already running
        if not self.running:
            self.running = True
            self.consumer_task = asyncio.create_task(self.run_consumer())
        
        return client_id
    
    def disconnect(self, websocket: WebSocket):
        """
        Disconnect a WebSocket client
        """
        # Find and remove from active connections
        for client_id, connections in list(self.active_connections.items()):
            if websocket in connections:
                connections.remove(websocket)
                if not connections:
                    del self.active_connections[client_id]
        
        # Remove subscriptions
        if websocket in self.subscriptions:
            del self.subscriptions[websocket]
            
        # Stop consumer if no more connections
        if not self.active_connections and self.consumer_task:
            self.running = False
    
    async def send_personal_message(self, message: str, websocket: WebSocket):
        """
        Send a message to a specific WebSocket
        """
        try:
            await websocket.send_text(message)
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            await self.disconnect(websocket)
    
    async def broadcast(self, message: str, client_id: str = None):
        """
        Broadcast a message to all connected clients or a specific client ID
        """
        if client_id:
            if client_id in self.active_connections:
                disconnected = []
                for connection in self.active_connections[client_id]:
                    try:
                        await connection.send_text(message)
                    except Exception as e:
                        logger.error(f"Error broadcasting: {e}")
                        disconnected.append(connection)
                
                # Disconnect any failed connections
                for connection in disconnected:
                    await self.disconnect(connection)
        else:
            # Broadcast to all clients
            for client_id in list(self.active_connections.keys()):
                await self.broadcast(message, client_id)
    
    async def subscribe(self, websocket: WebSocket, topic: str, symbol: str):
        """
        Subscribe a WebSocket to a specific topic and symbol
        """
        subscription_key = f"{topic}:{symbol}"
        self.subscriptions[websocket].add(subscription_key)
        
        # Initialize Kafka consumer for topic if needed
        if topic not in self.consumers:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                group_id=f"api-ws-{datetime.now().timestamp()}",
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode())
            )
            await consumer.start()
            self.consumers[topic] = consumer
        
        # Send confirmation
        await self.send_personal_message(
            json.dumps({
                "type": "subscription",
                "status": "success",
                "topic": topic,
                "symbol": symbol,
                "timestamp": datetime.now().isoformat()
            }),
            websocket
        )
    
    async def unsubscribe(self, websocket: WebSocket, topic: str, symbol: str):
        """
        Unsubscribe a WebSocket from a specific topic and symbol
        """
        subscription_key = f"{topic}:{symbol}"
        if websocket in self.subscriptions and subscription_key in self.subscriptions[websocket]:
            self.subscriptions[websocket].remove(subscription_key)
            
            # Send confirmation
            await self.send_personal_message(
                json.dumps({
                    "type": "subscription",
                    "status": "unsubscribed",
                    "topic": topic,
                    "symbol": symbol,
                    "timestamp": datetime.now().isoformat()
                }),
                websocket
            )
    
    async def run_consumer(self):
        """
        Run Kafka consumer loop to process messages from subscribed topics
        """
        try:
            while self.running:
                # Process messages from all active consumers
                for topic, consumer in list(self.consumers.items()):
                    # Only poll if we have active connections
                    if self.active_connections:
                        try:
                            # Non-blocking poll with timeout
                            messages = await asyncio.wait_for(
                                consumer.getmany(timeout_ms=100),
                                timeout=0.2
                            )
                            
                            for tp, msgs in messages.items():
                                for msg in msgs:
                                    # Extract symbol from message
                                    event = msg.value
                                    symbol = event.get("symbol", "unknown")
                                    subscription_key = f"{topic}:{symbol}"
                                    
                                    # Find subscribers for this topic+symbol
                                    for ws, subs in list(self.subscriptions.items()):
                                        if subscription_key in subs:
                                            try:
                                                await ws.send_text(json.dumps(event))
                                            except Exception as e:
                                                logger.error(f"Error sending to websocket: {e}")
                                                # Will be removed on next iteration
                        except asyncio.TimeoutError:
                            # This is fine, just continue
                            pass
                        except Exception as e:
                            logger.error(f"Error processing Kafka messages: {e}")
                
                # Prevent CPU spinning
                await asyncio.sleep(0.1)
                
        except Exception as e:
            logger.error(f"Consumer loop error: {e}")
        finally:
            # Close all consumers when loop exits
            for topic, consumer in self.consumers.items():
                await consumer.stop()
            self.consumers = {}
            self.running = False

    # Add a function to broadcast real-time analytics
    async def broadcast_realtime_analytics(self, symbol: str, data: dict):
        """Broadcast real-time analytics to subscribed clients"""
        if f"realtime-analytics:{symbol}" in self.active_connections:
            for connection in self.active_connections[f"realtime-analytics:{symbol}"]:
                await connection.send_json(data)

# Create a global connection manager instance
manager = ConnectionManager()

# Add WebSocket endpoints (to be imported by FastAPI app)
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query

router = APIRouter()

@router.websocket("/ws/prices/{symbol}")
async def websocket_prices(
    websocket: WebSocket,
    symbol: str,
):
    """WebSocket endpoint for real-time price updates"""
    client_id = await manager.connect(websocket)
    
    try:
        # Subscribe to market.prices.raw topic
        await manager.subscribe(websocket, "market.prices.raw", symbol)
        
        # Keep connection alive and handle client messages
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                # Handle client commands
                if message.get("action") == "unsubscribe":
                    await manager.unsubscribe(websocket, "market.prices.raw", symbol)
            except json.JSONDecodeError:
                # Ignore invalid messages
                pass
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@router.websocket("/ws/analytics/{symbol}")
async def websocket_analytics(
    websocket: WebSocket,
    symbol: str,
    interval: str = Query("5min"),
):
    """WebSocket endpoint for real-time analytics updates"""
    client_id = await manager.connect(websocket)
    
    try:
        # Subscribe to market.analytics topic
        await manager.subscribe(websocket, "market.analytics", symbol)
        
        # Keep connection alive and handle client messages
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                # Handle client commands
                if message.get("action") == "unsubscribe":
                    await manager.unsubscribe(websocket, "market.analytics", symbol)
            except json.JSONDecodeError:
                # Ignore invalid messages
                pass
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)