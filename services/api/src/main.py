from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os
from dotenv import load_dotenv

# Import routers
from .routers import prices, analytics
from .websockets.connection_manager import router as websocket_router

# Load environment variables
load_dotenv()

# Create FastAPI application
app = FastAPI(
    title="Market Analytics API",
    description="API for real-time market data and analytics",
    version="1.0.0",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(prices.router, prefix="/api/v1", tags=["prices"])
app.include_router(analytics.router, prefix="/api/v1", tags=["analytics"])
app.include_router(websocket_router, tags=["websockets"])

@app.get("/", tags=["health"])
async def root():
    """Health check endpoint"""
    return {
        "status": "online",
        "service": "market-analytics-api",
        "version": "1.0.0"
    }

if __name__ == "__main__":
    port = int(os.getenv("API_PORT", "8000"))
    uvicorn.run("services.api.src.main:app", host="0.0.0.0", port=port, reload=True)