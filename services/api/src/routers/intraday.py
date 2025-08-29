from fastapi import APIRouter, Path, Query, HTTPException
from typing import List, Optional
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
import os
import logging
from .utils.timezone_utils import parse_est_datetime, parse_est_date_range, format_est_datetime

router = APIRouter()
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get a connection to the database"""
    return psycopg2.connect(
        os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/stockanalytics"),
        cursor_factory=psycopg2.extras.RealDictCursor
    )

# Mapping of interval strings to minutes
INTERVAL_MAPPING = {
    "1min": 1,
    "5min": 5,
    "15min": 15,
    "30min": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240
}

@router.get("/prices/{symbol}", summary="Get intraday OHLCV data")
async def get_intraday_prices(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)"),
    interval: str = Query("5min", description="Price interval (1min, 5min, 15min, 30min, 1h, 2h, 4h)"),
    limit: int = Query(100, description="Number of data points to return", ge=1, le=2000),
    date: Optional[str] = Query(None, description="Specific date (YYYY-MM-DD) in est timezone"),
    start_time: Optional[str] = Query(None, description="Start time (HH:MM:SS or HH:MM) in est timezone"),
    end_time: Optional[str] = Query(None, description="End time (HH:MM:SS or HH:MM) in est timezone"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD) in est timezone"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD) in est timezone"),
    last_hours: Optional[int] = Query(None, description="Get data from last N hours", ge=1, le=168)  # Max 1 week
):
    """
    Get intraday OHLCV bar data for a specific symbol and interval.
    
    Time filtering options (in order of precedence):
    1. last_hours: Get data from the last N hours
    2. date + start_time/end_time: Get data for specific date and time range
    3. start_date/end_date: Get data for date range
    4. No filters: Get latest data (limited by limit parameter)
    
    All date/time inputs are interpreted as est timezone (EST/EDT).
    """
    # Validate interval
    if interval not in INTERVAL_MAPPING:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid interval: {interval}. Valid intervals: {list(INTERVAL_MAPPING.keys())}"
        )
    
    interval_minutes = INTERVAL_MAPPING[interval]
    conn = get_db_connection()
    
    try:
        # Build the base query
        query = """
        SELECT 
            symbol, timestamp, open, high, low, close, volume, 
            interval_minutes, source
        FROM intraday_ohlcv
        WHERE symbol = %s AND interval_minutes = %s
        """
        params = [symbol, interval_minutes]
        
        # Add time filters
        if last_hours:
            query += " AND timestamp > NOW() - interval '%s hours'"
            params.append(last_hours)
        elif date:
            # Single date with optional time range
            if start_time or end_time:
                start_dt, end_dt = parse_est_date_range(
                    date, date, start_time, end_time
                )
            else:
                # Whole day
                start_dt, end_dt = parse_est_date_range(date, date)
            
            query += " AND timestamp >= %s AND timestamp <= %s"
            params.extend([start_dt, end_dt])
        elif start_date and end_date:
            # Date range
            start_dt, end_dt = parse_est_date_range(start_date, end_date)
            query += " AND timestamp >= %s AND timestamp <= %s"
            params.extend([start_dt, end_dt])
        elif start_date:
            # From start date to now
            start_dt = parse_est_datetime(start_date)
            query += " AND timestamp >= %s"
            params.append(start_dt)
        
        # Order and limit
        query += " ORDER BY timestamp DESC LIMIT %s"
        params.append(limit)
        
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            
            if not rows:
                return {
                    "symbol": symbol,
                    "interval": interval,
                    "count": 0,
                    "data": [],
                    "message": f"No intraday {interval} data found for {symbol} with the specified filters"
                }
            
            # Format timestamps with est timezone info
            result = []
            for row in rows:
                row_dict = dict(row)
                row_dict['timestamp_utc'] = row_dict['timestamp'].isoformat()
                row_dict['timestamp_est'] = format_est_datetime(row_dict['timestamp'])
                # Remove interval_minutes from response (it's redundant with the interval parameter)
                row_dict.pop('interval_minutes', None)
                result.append(row_dict)
            
            metadata = {
                "symbol": symbol,
                "interval": interval,
                "count": len(result),
                "data": result,
                "timezone_note": "timestamp_est is in America/New_York timezone (EST/EDT)"
            }

            return create_formatted_response(
                result, 
                format, 
                metadata, 
                f"intraday_prices_{symbol}_{interval}"
            )

    except Exception as e:
        logger.error(f"Error fetching intraday prices: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@router.get("/prices/{symbol}/latest", summary="Get latest intraday bar")
async def get_latest_intraday_price(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)"),
    interval: str = Query("5min", description="Price interval")
):
    """Get the most recent intraday OHLCV bar for a symbol and interval."""
    if interval not in INTERVAL_MAPPING:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid interval: {interval}. Valid intervals: {list(INTERVAL_MAPPING.keys())}"
        )
    
    interval_minutes = INTERVAL_MAPPING[interval]
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, timestamp, open, high, low, close, volume, source
                FROM intraday_ohlcv
                WHERE symbol = %s AND interval_minutes = %s
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (symbol, interval_minutes)
            )
            row = cur.fetchone()
            
            if not row:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No intraday {interval} data found for symbol {symbol}"
                )
            
            result = dict(row)
            result['timestamp_utc'] = result['timestamp'].isoformat()
            result['timestamp_est'] = format_est_datetime(result['timestamp'])
            
            return {
                "symbol": symbol,
                "interval": interval,
                "data": result,
                "timezone_note": "timestamp_est is in America/New_York timezone (EST/EDT)"
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching latest intraday price: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@router.get("/analytics/{symbol}", summary="Get intraday analytics")
async def get_intraday_analytics(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)"),
    interval: str = Query("5min", description="Analytics interval (5min, 15min, 30min, 1h, 4h)"),
    indicators: List[str] = Query(
        ["rsi_14", "macd", "bb_upper", "bb_lower"], 
        description="Technical indicators to include"
    ),
    limit: int = Query(100, description="Number of data points to return", ge=1, le=1000),
    date: Optional[str] = Query(None, description="Specific date (YYYY-MM-DD) in est timezone"),
    start_time: Optional[str] = Query(None, description="Start time (HH:MM:SS) in est timezone"),
    end_time: Optional[str] = Query(None, description="End time (HH:MM:SS) in est timezone"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD) in est timezone"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD) in est timezone"),
    last_hours: Optional[int] = Query(None, description="Get data from last N hours", ge=1, le=72)
):
    """
    Get intraday technical analytics for a symbol and interval.
    
    Available indicators include:
    - RSI variants: rsi_7, rsi_14, rsi_21
    - MACD: macd, macd_signal, macd_hist
    - Bollinger Bands: bb_upper, bb_middle, bb_lower, bb_bandwidth, bb_percent_b
    - Moving averages: sma_20, sma_50, ema_12, ema_26
    - Volatility: atr_14, stddev_20
    - Momentum: stoch_k, stoch_d, roc_10, momentum_10
    
    All date/time inputs are interpreted as est timezone.
    """
    # Validate interval
    if interval not in INTERVAL_MAPPING:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid interval: {interval}. Valid intervals: {list(INTERVAL_MAPPING.keys())}"
        )
    
    interval_minutes = INTERVAL_MAPPING[interval]
    
    # Validate indicators
    allowed_indicators = [
        "rsi_7", "rsi_14", "rsi_21", "macd", "macd_signal", "macd_hist",
        "bb_upper", "bb_middle", "bb_lower", "bb_bandwidth", "bb_percent_b",
        "sma_5", "sma_10", "sma_20", "sma_50", "sma_100", "sma_200",
        "ema_9", "ema_12", "ema_26", "ema_50", "ema_200",
        "atr_14", "stddev_20", "stoch_k", "stoch_d", "roc_10", "momentum_10",
        "willr_14", "price"
    ]
    
    for indicator in indicators:
        if indicator not in allowed_indicators:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid indicator: {indicator}. Valid indicators: {allowed_indicators}"
            )
    
    # Always include price and timestamp
    if "price" not in indicators:
        indicators.append("price")
    
    conn = get_db_connection()
    
    try:
        # Build dynamic column list
        indicator_columns = ", ".join([f"indicators->>'{ind}' as {ind}" for ind in indicators if ind != "price"])
        columns = f"symbol, timestamp, price"
        if indicator_columns:
            columns += f", {indicator_columns}"
        
        # Build the base query
        query = f"""
        SELECT {columns}
        FROM intraday_analytics
        WHERE symbol = %s AND interval_minutes = %s
        """
        params = [symbol, interval_minutes]
        
        # Add time filters
        if last_hours:
            query += " AND timestamp > NOW() - interval '%s hours'"
            params.append(last_hours)
        elif date:
            if start_time or end_time:
                start_dt, end_dt = parse_est_date_range(
                    date, date, start_time, end_time
                )
            else:
                start_dt, end_dt = parse_est_date_range(date, date)
            
            query += " AND timestamp >= %s AND timestamp <= %s"
            params.extend([start_dt, end_dt])
        elif start_date and end_date:
            start_dt, end_dt = parse_est_date_range(start_date, end_date)
            query += " AND timestamp >= %s AND timestamp <= %s"
            params.extend([start_dt, end_dt])
        elif start_date:
            start_dt = parse_est_datetime(start_date)
            query += " AND timestamp >= %s"
            params.append(start_dt)
        
        query += " ORDER BY timestamp DESC LIMIT %s"
        params.append(limit)
        
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            
            if not rows:
                return {
                    "symbol": symbol,
                    "interval": interval,
                    "indicators": indicators,
                    "count": 0,
                    "data": [],
                    "message": f"No intraday analytics found for {symbol} {interval}"
                }
            
            # Format the response
            result = []
            for row in rows:
                row_dict = dict(row)
                row_dict['timestamp_utc'] = row_dict['timestamp'].isoformat()
                row_dict['timestamp_est'] = format_est_datetime(row_dict['timestamp'])
                result.append(row_dict)
            
            return {
                "symbol": symbol,
                "interval": interval,
                "indicators": indicators,
                "count": len(result),
                "data": result,
                "timezone_note": "timestamp_est is in America/New_York timezone (EST/EDT)"
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching intraday analytics: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@router.get("/prices/{symbol}/last/{count}", summary="Get last N intraday bars")
async def get_last_intraday_bars(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)"),
    count: int = Path(..., description="Number of latest bars to return", ge=1, le=1000),
    interval: str = Query("5min", description="Price interval")
):
    """Get the last N intraday OHLCV bars for a symbol."""
    if interval not in INTERVAL_MAPPING:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid interval: {interval}. Valid intervals: {list(INTERVAL_MAPPING.keys())}"
        )
    
    interval_minutes = INTERVAL_MAPPING[interval]
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, timestamp, open, high, low, close, volume, source
                FROM intraday_ohlcv
                WHERE symbol = %s AND interval_minutes = %s
                ORDER BY timestamp DESC
                LIMIT %s
                """,
                (symbol, interval_minutes, count)
            )
            rows = cur.fetchall()
            
            if not rows:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No intraday {interval} data found for symbol {symbol}"
                )
            
            result = []
            for row in rows:
                row_dict = dict(row)
                row_dict['timestamp_utc'] = row_dict['timestamp'].isoformat()
                row_dict['timestamp_est'] = format_est_datetime(row_dict['timestamp'])
                result.append(row_dict)
            
            return {
                "symbol": symbol,
                "interval": interval,
                "count": len(result),
                "requested_count": count,
                "data": result,
                "timezone_note": "timestamp_est is in America/New_York timezone (EST/EDT)"
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching last intraday bars: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@router.get("/analytics/{symbol}/last/{count}", summary="Get last N intraday analytics")
async def get_last_intraday_analytics(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)"),
    count: int = Path(..., description="Number of latest analytics to return", ge=1, le=500),
    interval: str = Query("5min", description="Analytics interval"),
    indicators: List[str] = Query(
        ["rsi_14", "macd", "bb_upper", "bb_lower"], 
        description="Technical indicators to include"
    )
):
    """Get the last N intraday analytics records for a symbol."""
    if interval not in INTERVAL_MAPPING:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid interval: {interval}. Valid intervals: {list(INTERVAL_MAPPING.keys())}"
        )
    
    interval_minutes = INTERVAL_MAPPING[interval]
    
    # Validate indicators (reuse the same validation logic)
    allowed_indicators = [
        "rsi_7", "rsi_14", "rsi_21", "macd", "macd_signal", "macd_hist",
        "bb_upper", "bb_middle", "bb_lower", "bb_bandwidth", "bb_percent_b",
        "sma_5", "sma_10", "sma_20", "sma_50", "sma_100", "sma_200",
        "ema_9", "ema_12", "ema_26", "ema_50", "ema_200",
        "atr_14", "stddev_20", "stoch_k", "stoch_d", "roc_10", "momentum_10",
        "willr_14", "price"
    ]
    
    for indicator in indicators:
        if indicator not in allowed_indicators:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid indicator: {indicator}. Valid indicators: {allowed_indicators}"
            )
    
    if "price" not in indicators:
        indicators.append("price")
    
    conn = get_db_connection()
    
    try:
        # Build dynamic column list
        indicator_columns = ", ".join([f"indicators->>'{ind}' as {ind}" for ind in indicators if ind != "price"])
        columns = f"symbol, timestamp, price"
        if indicator_columns:
            columns += f", {indicator_columns}"
        
        query = f"""
        SELECT {columns}
        FROM intraday_analytics
        WHERE symbol = %s AND interval_minutes = %s
        ORDER BY timestamp DESC
        LIMIT %s
        """
        
        with conn.cursor() as cur:
            cur.execute(query, (symbol, interval_minutes, count))
            rows = cur.fetchall()
            
            if not rows:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No intraday analytics found for {symbol} {interval}"
                )
            
            result = []
            for row in rows:
                row_dict = dict(row)
                row_dict['timestamp_utc'] = row_dict['timestamp'].isoformat()
                row_dict['timestamp_est'] = format_est_datetime(row_dict['timestamp'])
                result.append(row_dict)
            
            return {
                "symbol": symbol,
                "interval": interval,
                "indicators": indicators,
                "count": len(result),
                "requested_count": count,
                "data": result,
                "timezone_note": "timestamp_est is in America/New_York timezone (EST/EDT)"
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching last intraday analytics: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()