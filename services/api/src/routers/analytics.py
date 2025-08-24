from fastapi import APIRouter, Path, Query, HTTPException, Depends
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
import os
from ..websockets.connection_manager import manager
import logging
import asyncpg

router = APIRouter()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get a connection to the database"""
    return psycopg2.connect(
        os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/stockanalytics"),
        cursor_factory=psycopg2.extras.RealDictCursor
    )

@router.get("/analytics/available-indicators", summary="Get available indicators")
async def get_available_indicators():
    """
    Get list of all available technical indicators.
    """
    indicators = {
        "moving_averages": [
            {"id": "sma_5", "name": "Simple Moving Average (5)", "category": "trend"},
            {"id": "sma_10", "name": "Simple Moving Average (10)", "category": "trend"},
            {"id": "sma_20", "name": "Simple Moving Average (20)", "category": "trend"},
            {"id": "sma_50", "name": "Simple Moving Average (50)", "category": "trend"},
            {"id": "sma_100", "name": "Simple Moving Average (100)", "category": "trend"},
            {"id": "sma_200", "name": "Simple Moving Average (200)", "category": "trend"},
            {"id": "ema_9", "name": "Exponential Moving Average (9)", "category": "trend"},
            {"id": "ema_12", "name": "Exponential Moving Average (12)", "category": "trend"},
            {"id": "ema_26", "name": "Exponential Moving Average (26)", "category": "trend"},
            {"id": "ema_50", "name": "Exponential Moving Average (50)", "category": "trend"},
            {"id": "ema_200", "name": "Exponential Moving Average (200)", "category": "trend"}
        ],
        "oscillators": [
            {"id": "rsi_7", "name": "Relative Strength Index (7)", "category": "momentum"},
            {"id": "rsi_14", "name": "Relative Strength Index (14)", "category": "momentum"},
            {"id": "rsi_21", "name": "Relative Strength Index (21)", "category": "momentum"},
            {"id": "stoch_k", "name": "Stochastic %K", "category": "momentum"},
            {"id": "stoch_d", "name": "Stochastic %D", "category": "momentum"}
        ],
        "trend_indicators": [
            {"id": "macd", "name": "MACD Line", "category": "trend"},
            {"id": "macd_signal", "name": "MACD Signal Line", "category": "trend"},
            {"id": "macd_hist", "name": "MACD Histogram", "category": "trend"}
        ],
        "volatility_indicators": [
            {"id": "bb_upper", "name": "Bollinger Band Upper", "category": "volatility"},
            {"id": "bb_middle", "name": "Bollinger Band Middle", "category": "volatility"},
            {"id": "bb_lower", "name": "Bollinger Band Lower", "category": "volatility"},
            {"id": "atr_14", "name": "Average True Range (14)", "category": "volatility"}
        ]
    }
    
    # Flatten all indicators for easier lookup
    all_indicators = []
    for category, indicators_list in indicators.items():
        all_indicators.extend(indicators_list)
    
    return {
        "categories": indicators,
        "all": all_indicators
    }

@router.get("/analytics/{symbol}", summary="Get technical indicators")
async def get_analytics(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)"),
    interval: str = Query("5min", description="Data interval (5min, 15min, 1h, 1d)"),
    indicators: List[str] = Query(
        ["sma_20", "ema_12", "rsi_14", "macd"], 
        description="Technical indicators to include"
    ),
    limit: int = Query(100, description="Number of data points to return", ge=1, le=1000),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """
    Get technical indicators for a specific symbol.
    
    Available indicators include:
    - Moving averages (sma_X, ema_X where X is the period)
    - Oscillators (rsi_X, stoch_X)
    - Trend indicators (macd, adx)
    - Volatility indicators (bbands, atr)
    """
    # Convert interval string to minutes for database query
    interval_map = {
        "5min": 5, 
        "1d": 1440  # Daily data
    }
    
    # Determine which table to use based on interval
    if interval == "1d":
        table_name = "daily_analytics"
        interval_filter = ""
    else:
        table_name = "intraday_analytics"
        if interval not in interval_map:
            raise HTTPException(status_code=400, detail=f"Invalid interval: {interval}")
        interval_filter = f"AND interval_minutes = {interval_map[interval]}"
    
    # Check that the indicators are valid to prevent SQL injection
    allowed_indicators = [
        "sma_5", "sma_10", "sma_20", "sma_50", "sma_100", "sma_200",
        "ema_9", "ema_12", "ema_26", "ema_50", "ema_200",
        "rsi_7", "rsi_14", "rsi_21",
        "macd", "macd_signal", "macd_hist",
        "bb_upper", "bb_middle", "bb_lower",
        "atr_14", "stoch_k", "stoch_d",
        "price"
    ]
    
    for indicator in indicators:
        if indicator not in allowed_indicators:
            raise HTTPException(status_code=400, detail=f"Invalid indicator: {indicator}")
    
    # Always include timestamp and price
    if "price" not in indicators:
        indicators.append("price")
    
    # Build column list
    columns = "timestamp, " + ", ".join(indicators)
    
    # Build date filter if provided
    date_filter = ""
    if start_date:
        date_filter += f" AND timestamp >= '{start_date}'"
    if end_date:
        date_filter += f" AND timestamp <= '{end_date}'"
    
    # Build the query
    query = f"""
    SELECT {columns}
    FROM {table_name}
    WHERE symbol = %s
    {interval_filter}
    {date_filter}
    ORDER BY timestamp DESC
    LIMIT %s
    """
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, (symbol, limit))
            rows = cur.fetchall()
            
            # Convert to list and format timestamps
            result = []
            for row in rows:
                # Convert timestamp to ISO format
                row['timestamp'] = row['timestamp'].isoformat()
                result.append(row)
            
            return {
                "symbol": symbol,
                "interval": interval,
                "indicators": indicators,
                "count": len(result),
                "data": result
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@router.get("/realtime/{symbol}")
async def get_realtime_analytics(
    symbol: str,
    limit: int = Query(100, gt=0, le=1000),
    last_minutes: int = Query(30, gt=0, le=180)
):
    """Get real-time analytics for a symbol over the last N minutes"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Query real-time analytics
        query = """
            SELECT * FROM realtime_analytics
            WHERE symbol = %s
            AND timestamp > NOW() - interval '1 minute' * %s
            ORDER BY timestamp DESC
            LIMIT %s
        """
        
        cur.execute(query, (symbol, last_minutes, limit))
        records = cur.fetchall()
        
        # Convert to response model
        result = [dict(record) for record in records]
        conn.close()
        
        return result
    except Exception as e:
        logger.error(f"Error retrieving real-time analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))