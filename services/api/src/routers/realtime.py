from fastapi import APIRouter, Path, Query, HTTPException
from typing import List, Optional
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
import os
import logging
from .utils.timezone_utils import parse_est_datetime, parse_est_date_range, format_est_datetime
from .utils.format_utils import create_formatted_response

router = APIRouter()
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get a connection to the database"""
    return psycopg2.connect(
        os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/stockanalytics"),
        cursor_factory=psycopg2.extras.RealDictCursor
    )

@router.get("/prices/{symbol}", summary="Get real-time price data")
async def get_realtime_prices(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)"),
    limit: int = Query(100, description="Number of data points to return", ge=1, le=5000),
    date: Optional[str] = Query(None, description="Specific date (YYYY-MM-DD) in EST timezone"),
    start_time: Optional[str] = Query(None, description="Start time (HH:MM:SS or HH:MM) in EST timezone"),
    end_time: Optional[str] = Query(None, description="End time (HH:MM:SS or HH:MM) in EST timezone"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD) in EST timezone"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD) in EST timezone"),
    last_minutes: Optional[int] = Query(None, description="Get data from last N minutes", ge=1, le=1440),
    format: str = Query("json", description="Response format: json or csv")
):
    """
    Get real-time tick price data for a specific symbol.
    
    Time filtering options (in order of precedence):
    1. last_minutes: Get data from the last N minutes
    2. date + start_time/end_time: Get data for specific date and time range
    3. start_date/end_date: Get data for date range
    4. No filters: Get latest data (limited by limit parameter)
    
    All date/time inputs are interpreted as EST timezone (EST/EDT).
    """
    conn = get_db_connection()
    
    try:
        # Build the base query
        query = """
        SELECT 
            symbol, price, timestamp, volume, source
        FROM realtime_prices
        WHERE symbol = %s
        """
        params = [symbol]
        
        # Add time filters
        if last_minutes:
            query += " AND timestamp > NOW() - interval '%s minutes'"
            params.append(last_minutes)
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
                metadata = {
                    "symbol": symbol,
                    "count": 0,
                    "message": f"No real-time data found for {symbol} with the specified filters"
                }
                return create_formatted_response([], format, metadata, f"realtime_prices_{symbol}")
            
            # Format timestamps with EST timezone info
            result = []
            for row in rows:
                row_dict = dict(row)
                row_dict['timestamp_utc'] = row_dict['timestamp'].isoformat()
                row_dict['timestamp_est'] = format_est_datetime(row_dict['timestamp'])
                result.append(row_dict)
            
            # Create response metadata
            metadata = {
                "symbol": symbol,
                "count": len(result),
                "timezone_note": "timestamp_est is in America/New_York timezone (EST/EDT)"
            }
            
            return create_formatted_response(
                result, 
                format, 
                metadata, 
                f"realtime_prices_{symbol}"
            )
            
    except Exception as e:
        logger.error(f"Error fetching realtime prices: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@router.get("/prices/{symbol}/latest", summary="Get latest real-time price")
async def get_latest_realtime_price(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)")
):
    """Get the most recent real-time price for a symbol."""
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, price, timestamp, volume, source
                FROM realtime_prices
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (symbol,)
            )
            row = cur.fetchone()
            
            if not row:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No real-time data found for symbol {symbol}"
                )
            
            result = dict(row)
            result['timestamp_utc'] = result['timestamp'].isoformat()
            result['timestamp_est'] = format_est_datetime(result['timestamp'])
            
            return {
                "symbol": symbol,
                "data": result,
                "timezone_note": "timestamp_est is in America/New_York timezone (EST/EDT)"
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching latest price: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@router.get("/analytics/{symbol}", summary="Get real-time analytics")
async def get_realtime_analytics(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)"),
    limit: int = Query(100, description="Number of data points to return", ge=1, le=1000),
    last_minutes: Optional[int] = Query(30, description="Get data from last N minutes", ge=1, le=180),
    date: Optional[str] = Query(None, description="Specific date (YYYY-MM-DD) in EST timezone"),
    start_time: Optional[str] = Query(None, description="Start time (HH:MM:SS) in EST timezone"),
    end_time: Optional[str] = Query(None, description="End time (HH:MM:SS) in EST timezone"),
    indicators: List[str] = Query([], description="Real-time indicators to include (empty for all)"),
    format: str = Query("json", description="Response format: json or csv")
):
    """
    Get real-time technical analytics for a symbol.
    
    Real-time analytics include tick-based indicators like:
    - Price velocity
    - Tick momentum 
    - Real-time RSI approximation
    - Volume profile indicators
    
    If no indicators are specified, returns all available real-time indicators.
    All date/time inputs are interpreted as EST timezone.
    """
    # Get all available real-time indicators if none specified
    realtime_indicators = [
        "price_velocity_10", "price_velocity_50", "tick_ratio", "tick_vwap_20",
        "volume_momentum", "bb_bandwidth", "bb_position", "tick_rsi_14", "price"
    ]
    
    if not indicators:
        indicators = realtime_indicators
    else:
        # Validate indicators
        invalid_indicators = [ind for ind in indicators if ind not in realtime_indicators]
        if invalid_indicators:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid real-time indicators: {invalid_indicators}. Available indicators: {realtime_indicators}"
            )
    
    # Always include price
    if "price" not in indicators:
        indicators.append("price")
    
    conn = get_db_connection()
    
    try:
        # Build the query - for real-time analytics, we need to extract from JSONB
        indicator_columns = ", ".join([f"indicators->>'{ind}' as {ind}" for ind in indicators if ind != "price"])
        columns = f"symbol, timestamp, price"
        if indicator_columns:
            columns += f", {indicator_columns}"
        
        query = f"""
        SELECT {columns}
        FROM realtime_analytics
        WHERE symbol = %s
        """
        params = [symbol]
        
        # Add time filters
        if date:
            if start_time or end_time:
                start_dt, end_dt = parse_est_date_range(
                    date, date, start_time, end_time
                )
            else:
                start_dt, end_dt = parse_est_date_range(date, date)
            
            query += " AND timestamp >= %s AND timestamp <= %s"
            params.extend([start_dt, end_dt])
        else:
            # Default to last N minutes
            query += " AND timestamp > NOW() - interval '%s minutes'"
            params.append(last_minutes)
        
        query += " ORDER BY timestamp DESC LIMIT %s"
        params.append(limit)
        
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            
            if not rows:
                metadata = {
                    "symbol": symbol,
                    "indicators": indicators,
                    "count": 0,
                    "message": f"No real-time analytics found for {symbol}"
                }
                return create_formatted_response([], format, metadata, f"realtime_analytics_{symbol}")
            
            # Format the response
            result = []
            for row in rows:
                row_dict = dict(row)
                row_dict['timestamp_utc'] = row_dict['timestamp'].isoformat()
                row_dict['timestamp_est'] = format_est_datetime(row_dict['timestamp'])
                result.append(row_dict)
            
            metadata = {
                "symbol": symbol,
                "indicators": indicators,
                "count": len(result),
                "timezone_note": "timestamp_est is in America/New_York timezone (EST/EDT)"
            }
            
            return create_formatted_response(
                result, 
                format, 
                metadata, 
                f"realtime_analytics_{symbol}"
            )
            
    except Exception as e:
        logger.error(f"Error fetching realtime analytics: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@router.get("/symbols", summary="Get symbols with real-time data")
async def get_realtime_symbols():
    """Get list of all symbols that have real-time data available."""
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT symbol, 
                       COUNT(*) as tick_count,
                       MIN(timestamp) as first_seen,
                       MAX(timestamp) as last_seen
                FROM realtime_prices
                GROUP BY symbol
                ORDER BY symbol
                """
            )
            rows = cur.fetchall()
            
            symbols_info = []
            for row in rows:
                info = dict(row)
                info['first_seen_est'] = format_est_datetime(info['first_seen'])
                info['last_seen_est'] = format_est_datetime(info['last_seen'])
                symbols_info.append(info)
            
            return {
                "count": len(symbols_info),
                "symbols": symbols_info,
                "timezone_note": "All timestamps shown in America/New_York timezone"
            }
            
    except Exception as e:
        logger.error(f"Error fetching symbols: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()
    """Get list of all symbols that have real-time data available."""
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT symbol, 
                       COUNT(*) as tick_count,
                       MIN(timestamp) as first_seen,
                       MAX(timestamp) as last_seen
                FROM realtime_prices
                GROUP BY symbol
                ORDER BY symbol
                """
            )
            rows = cur.fetchall()
            
            symbols_info = []
            for row in rows:
                info = dict(row)
                info['first_seen_est'] = format_est_datetime(info['first_seen'])
                info['last_seen_est'] = format_est_datetime(info['last_seen'])
                symbols_info.append(info)
            
            return {
                "count": len(symbols_info),
                "symbols": symbols_info,
                "timezone_note": "All timestamps shown in America/New_York timezone"
            }
            
    except Exception as e:
        logger.error(f"Error fetching symbols: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()