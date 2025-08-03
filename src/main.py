from fastapi import FastAPI, Query
from typing import Optional, List
from src.db import (
    get_latest_analytics, get_analytics_history, get_price_history, 
    get_data_quality_issues, get_pipeline_metrics, init_db
)

app = FastAPI(title="Stock Analytics API", version="2.0.0")

@app.on_event("startup")
def startup_event():
    init_db()

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