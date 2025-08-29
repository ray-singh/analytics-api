from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os
from dotenv import load_dotenv
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from .routers import realtime, intraday, historical
from .websockets.connection_manager import router as websocket_router

load_dotenv()

app = FastAPI(
    title="Market Analytics API",
    description="API for real-time market data and analytics",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(realtime.router, prefix="/api/v2/realtime", tags=["realtime"])
app.include_router(intraday.router, prefix="/api/v2/intraday", tags=["intraday"])
app.include_router(historical.router, prefix="/api/v2/historical", tags=["historical"])
app.include_router(websocket_router, tags=["websockets"])

REQUEST_COUNT = Counter("api_requests_total", "Total API requests", ["endpoint", "method"])
ERROR_COUNT = Counter("api_errors_total", "Total API errors", ["endpoint", "method"])

@app.middleware("http")
async def prometheus_middleware(request, call_next):
    endpoint = request.url.path
    method = request.method
    REQUEST_COUNT.labels(endpoint=endpoint, method=method).inc()
    try:
        response = await call_next(request)
        return response
    except Exception:
        ERROR_COUNT.labels(endpoint=endpoint, method=method).inc()
        raise

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/", tags=["health"])
async def root():
    """Health check endpoint"""
    return {
        "status": "online",
        "service": "market-analytics-api",
        "version": "2.0.0",
        "endpoints": {
            "realtime": "/api/v2/realtime",
            "intraday": "/api/v2/intraday", 
            "historical": "/api/v2/historical",
            "websockets": "/ws"
        }
    }

@app.get("/health", tags=["health"])
async def health_check():
    """Detailed health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": "2024-01-01T00:00:00Z",
        "services": {
            "database": "connected",
            "kafka": "connected", 
            "websockets": "active"
        }
    }

if __name__ == "__main__":
    port = int(os.getenv("API_PORT", "8000"))
    uvicorn.run("services.api.src.main:app", host="0.0.0.0", port=port, reload=True)