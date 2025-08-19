from fastapi import APIRouter, Path, Query, HTTPException, Depends
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
import os
from ..websockets.connection_manager import manager

# Create router
router = APIRouter()

def get_db_connection():
    """Get a connection to the database"""
    return psycopg2.connect(
        os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/stockanalytics"),
        cursor_factory=psycopg2.extras.RealDictCursor
    )

@router.get("/prices/{symbol}", summary="Get historical price data")
async def get_prices(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)"),
    interval: str = Query("5min", description="Price interval (1min, 5min, 15min, 1h, 1d)"),
    limit: int = Query(100, description="Number of data points to return", ge=1, le=1000),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """
    Get historical price data for a specific symbol.
    
    Returns OHLCV data at the specified interval.
    """
    # Convert interval string to minutes for database query
    interval_map = {
        "1min": 1,
        "5min": 5, 
        "15min": 15,
        "30min": 30,
        "1h": 60,
        "4h": 240,
        "1d": 1440  # Daily data
    }
    
    # Determine which table to use based on interval
    if interval == "1d":
        table_name = "historical_ohlcv"
        interval_filter = ""
    else:
        table_name = "intraday_ohlcv"
        if interval not in interval_map:
            raise HTTPException(status_code=400, detail=f"Invalid interval: {interval}")
        interval_filter = f"AND interval_minutes = {interval_map[interval]}"
    
    # Build date filter if provided
    date_filter = ""
    if start_date:
        date_filter += f" AND timestamp >= '{start_date}'"
    if end_date:
        date_filter += f" AND timestamp <= '{end_date}'"
    
    # Build the query
    query = f"""
    SELECT 
        timestamp, open, high, low, close, volume
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
                "count": len(result),
                "data": result
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@router.get("/prices/{symbol}/latest", summary="Get latest price")
async def get_latest_price(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)")
):
    """
    Get the latest price for a specific symbol.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    price, timestamp, volume, source
                FROM realtime_prices
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (symbol,)
            )
            row = cur.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail=f"No data found for symbol {symbol}")
                
            # Format timestamp
            row['timestamp'] = row['timestamp'].isoformat()
            
            return {
                "symbol": symbol,
                "data": row
            }
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@router.get("/symbols", summary="Get available symbols")
async def get_symbols():
    """
    Get list of all available symbols in the database.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT symbol
                FROM realtime_prices
                ORDER BY symbol
                """
            )
            rows = cur.fetchall()
            
            symbols = [row['symbol'] for row in rows]
            
            return {
                "count": len(symbols),
                "symbols": symbols
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()