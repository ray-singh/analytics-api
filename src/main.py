from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from typing import Optional, List
import asyncio
from src.db import (
    get_latest_analytics, get_analytics_history, get_price_history, 
    get_data_quality_issues, get_pipeline_metrics, init_db
)
from aiokafka import AIOKafkaConsumer

app = FastAPI(title="Stock Analytics API", version="2.0.0")

# Store connected WebSocket clients
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                pass  # Handle broken connections gracefully

manager = ConnectionManager()

@app.on_event("startup")
async def startup_event():
    init_db()
    asyncio.create_task(kafka_to_websocket_broadcaster())

async def kafka_to_websocket_broadcaster():
    consumer = AIOKafkaConsumer(
        "analytics",  
        bootstrap_servers="localhost:9092",
        group_id="websocket-group",
        auto_offset_reset="latest"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            await manager.broadcast(msg.value.decode())
    finally:
        await consumer.stop()

@app.get("/")
def root():
    return {
        "message": "Stock Analytics API",
        "version": "2.0.0",
        "features": [
            "Multi-symbol support",
            "Enhanced technical indicators",
            "Data quality monitoring",
            "ETL pipeline metrics"
        ]
    }

@app.get("/analytics/{symbol}")
def get_analytics(symbol: str, analytics_type: Optional[str] = None):
    """Get latest analytics for a symbol"""
    result = get_latest_analytics(symbol, analytics_type)
    if result:
        return result
    return {"error": "No data found"}

@app.get("/analytics/{symbol}/history")
def get_analytics_history_api(
    symbol: str, 
    analytics_type: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000)
):
    """Get historical analytics for a symbol"""
    return get_analytics_history(symbol, analytics_type, limit)

@app.get("/prices/{symbol}/history")
def get_price_history_api(
    symbol: str,
    limit: int = Query(100, ge=1, le=1000)
):
    """Get historical prices for a symbol"""
    return get_price_history(symbol, limit)

@app.get("/data-quality/issues")
def get_data_quality_issues_api(
    symbol: Optional[str] = None,
    severity: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000)
):
    """Get data quality issues"""
    return get_data_quality_issues(symbol, severity, limit)

@app.get("/pipeline/metrics")
def get_pipeline_metrics_api():
    """Get ETL pipeline performance metrics"""
    return get_pipeline_metrics()

@app.get("/health")
def health_check():
    """Health check endpoint"""
    try:
        metrics = get_pipeline_metrics()
        return {
            "status": "healthy",
            "timestamp": "2024-01-15T10:30:00Z",
            "metrics": metrics
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": "2024-01-15T10:30:00Z"
        }

@app.websocket("/ws/analytics/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):
    """
    WebSocket endpoint for real-time analytics updates for a specific symbol.
    """
    await manager.connect(websocket)
    try:
        while True:
            # Simulate sending real-time data (replace with actual data source)
            latest_analytics = get_latest_analytics(symbol)
            if latest_analytics:
                await websocket.send_json(latest_analytics)
            await asyncio.sleep(1)  
    except WebSocketDisconnect:
        manager.disconnect(websocket)