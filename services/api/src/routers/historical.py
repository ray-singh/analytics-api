from fastapi import APIRouter, Path, Query, HTTPException
from typing import List, Optional
from datetime import datetime, timedelta, date
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

@router.get("/prices/{symbol}", summary="Get historical daily OHLCV data")
async def get_historical_prices(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)"),
    limit: int = Query(252, description="Number of data points to return (252 = 1 year)", ge=1, le=5000),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD) in EST timezone"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD) in EST timezone"),
    last_days: Optional[int] = Query(None, description="Get data from last N trading days", ge=1, le=2000),
    format: str = Query("json", description="Response format: json or csv")
):
    """
    Get historical daily OHLCV data for a specific symbol.
    
    Time filtering options (in order of precedence):
    1. last_days: Get data from the last N trading days
    2. start_date/end_date: Get data for date range
    3. start_date only: Get data from start date to present
    4. No filters: Get latest data (limited by limit parameter)
    
    All dates are interpreted as est timezone (EST/EDT).
    Note: Historical data represents end-of-day prices in market timezone.
    """
    conn = get_db_connection()
    
    try:
        query = """
        SELECT 
            symbol, timestamp, open, high, low, close, volume, source
        FROM historical_ohlcv
        WHERE symbol = %s
        """
        params = [symbol]
        
        # Add time filters
        if last_days:
            # Get last N trading days (approximately last_days * 1.4 to account for weekends)
            query += " AND timestamp > NOW() - interval '%s days'"
            params.append(int(last_days * 1.4))  # Buffer for weekends/holidays
        elif start_date and end_date:
            # Date range - for daily data, we only care about dates, not times
            start_dt = parse_est_datetime(start_date)
            end_dt = parse_est_datetime(end_date, "23:59:59")  # End of day
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
                    "count": 0,
                    "data": [],
                    "message": f"No historical daily data found for {symbol} with the specified filters"
                }
            
            # Format timestamps with est timezone info
            result = []
            for row in rows:
                row_dict = dict(row)
                ts = row_dict['timestamp']
                if isinstance(ts, datetime):
                    ts_dt = ts
                elif isinstance(ts, date):
                    ts_dt = datetime.combine(ts, datetime.min.time())
                else:
                    ts_dt = datetime.strptime(str(ts), "%Y-%m-%d %H:%M:%S")

                row_dict['timestamp_utc'] = ts_dt.isoformat()
                row_dict['date_est'] = format_est_datetime(ts_dt).split(' ')[0]  # Just the date part
                result.append(row_dict)
            
            metadata = {
                "symbol": symbol,
                "count": len(result),
                "data": result,
                "timezone_note": "Historical daily data represents end-of-day prices in America/New_York timezone"
            }
            return create_formatted_response(result, format, metadata, f"historical_prices_{symbol}")
            
    except Exception as e:
        logger.error(f"Error fetching historical prices: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@router.get("/prices/{symbol}/latest", summary="Get latest historical daily bar")
async def get_latest_historical_price(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)"),
    format: str = Query("json", description="Response format: json or csv")
):
    """Get the most recent historical daily OHLCV bar for a symbol."""
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, timestamp, open, high, low, close, volume, source
                FROM historical_ohlcv
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
                    detail=f"No historical daily data found for symbol {symbol}"
                )
            
            result = dict(row)
            ts = result['timestamp']
            if isinstance(ts, datetime):
                ts_dt = ts
            elif isinstance(ts, date):
                ts_dt = datetime.combine(ts, datetime.min.time())
            else:
                ts_dt = datetime.strptime(str(ts), "%Y-%m-%d %H:%M:%S")

            result['timestamp_utc'] = ts_dt.isoformat()
            result['date_est'] = format_est_datetime(ts_dt).split(' ')[0]
            
            metadata = {
                "symbol": symbol,
                "data": result,
                "timezone_note": "Historical daily data represents end-of-day prices in America/New_York timezone"
            }
            return create_formatted_response(result, format, metadata, f"historical_prices_{symbol}")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching latest historical price: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@router.get("/prices/{symbol}/last/{count}", summary="Get last N historical daily bars")
async def get_last_historical_bars(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)"),
    count: int = Path(..., description="Number of latest trading days to return", ge=1, le=2000),
    format: str = Query("json", description="Response format: json or csv")
):
    """Get the last N historical daily OHLCV bars for a symbol."""
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, timestamp, open, high, low, close, volume, source
                FROM historical_ohlcv
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT %s
                """,
                (symbol, count)
            )
            rows = cur.fetchall()
            
            if not rows:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No historical daily data found for symbol {symbol}"
                )
            
            result = []
            for row in rows:
                row_dict = dict(row)
                ts = row_dict['timestamp']
                if isinstance(ts, datetime):
                    ts_dt = ts
                elif isinstance(ts, date):
                    ts_dt = datetime.combine(ts, datetime.min.time())
                else:
                    ts_dt = datetime.strptime(str(ts), "%Y-%m-%d %H:%M:%S")

                row_dict['timestamp_utc'] = ts_dt.isoformat()
                row_dict['date_est'] = format_est_datetime(ts_dt).split(' ')[0]
                result.append(row_dict)

            metadata = {
                "symbol": symbol,
                "count": len(result),
                "requested_count": count,
                "data": result,
                "timezone_note": "Historical daily data represents end-of-day prices in America/New_York timezone"
            }
            return create_formatted_response(result, format, metadata, f"historical_prices_{symbol}")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching last historical bars: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@router.get("/analytics/{symbol}", summary="Get historical daily analytics")
async def get_historical_analytics(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)"),
    indicators: List[str] = Query(
        ["rsi_14", "macd", "sma_20", "sma_50"], 
        description="Technical indicators to include"
    ),
    limit: int = Query(252, description="Number of data points to return", ge=1, le=2000),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD) in est timezone"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD) in est timezone"),
    last_days: Optional[int] = Query(None, description="Get data from last N trading days", ge=1, le=1000),
    format: str = Query("json", description="Response format: json or csv")
):
    """
    Get historical daily technical analytics for a symbol.
    
    Available indicators for daily data include:
    - RSI variants: rsi_7, rsi_14, rsi_21
    - MACD: macd, macd_signal, macd_hist
    - Bollinger Bands: bb_upper, bb_middle, bb_lower, bb_bandwidth, bb_percent_b
    - Moving averages: sma_5, sma_10, sma_20, sma_50, sma_100, sma_200, ema_12, ema_26, ema_50, ema_200
    - Volatility: atr_14, stddev_20
    - Momentum: stoch_k, stoch_d, roc_10, momentum_10
    
    All dates are interpreted as est timezone.
    """
    # Validate indicators
    allowed_indicators = [
        "rsi_7", "rsi_14", "rsi_21", "macd", "macd_signal", "macd_hist",
        "bb_upper", "bb_middle", "bb_lower", "bb_bandwidth", "bb_percent_b",
        "sma_5", "sma_10", "sma_20", "sma_50", "sma_100", "sma_200",
        "ema_9", "ema_12", "ema_26", "ema_50", "ema_200",
        "atr_14", "stddev_20", "stoch_k", "stoch_d", "roc_10", "momentum_10",
        "willr_14", "hist_vol_20", "keltner_upper", "keltner_lower", "price"
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
        FROM daily_analytics
        WHERE symbol = %s
        """
        params = [symbol]
        
        # Add time filters
        if last_days:
            query += " AND timestamp > NOW() - interval '%s days'"
            params.append(int(last_days * 1.4))  # Buffer for weekends
        elif start_date and end_date:
            start_dt = parse_est_datetime(start_date)
            end_dt = parse_est_datetime(end_date, "23:59:59")
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
                    "indicators": indicators,
                    "count": 0,
                    "data": [],
                    "message": f"No historical daily analytics found for {symbol}"
                }
            
            # Format the response
            result = []
            for row in rows:
                row_dict = dict(row)
                ts = row_dict['timestamp']
                if isinstance(ts, datetime):
                    ts_dt = ts
                elif isinstance(ts, date):
                    ts_dt = datetime.combine(ts, datetime.min.time())
                else:
                    ts_dt = datetime.strptime(str(ts), "%Y-%m-%d %H:%M:%S")

                row_dict['timestamp_utc'] = ts_dt.isoformat()
                row_dict['date_est'] = format_est_datetime(ts_dt).split(' ')[0]
                result.append(row_dict)
            
            metadata = {
                "symbol": symbol,
                "indicators": indicators,
                "count": len(result),
                "data": result,
                "timezone_note": "Historical daily analytics represent end-of-day calculations in America/New_York timezone"
            }
            return create_formatted_response(result, format, metadata, f"historical_analytics_{symbol}")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching historical analytics: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@router.get("/analytics/{symbol}/last/{count}", summary="Get last N historical daily analytics")
async def get_last_historical_analytics(
    symbol: str = Path(..., description="Stock symbol (e.g., AAPL, MSFT)"),
    count: int = Path(..., description="Number of latest analytics to return", ge=1, le=1000),
    indicators: List[str] = Query(
        ["rsi_14", "macd", "sma_20", "sma_50"], 
        description="Technical indicators to include"
    ),
    format: str = Query("json", description="Response format: json or csv")
):
    """Get the last N historical daily analytics records for a symbol."""
    allowed_indicators = [
        "rsi_7", "rsi_14", "rsi_21", "macd", "macd_signal", "macd_hist",
        "bb_upper", "bb_middle", "bb_lower", "bb_bandwidth", "bb_percent_b",
        "sma_5", "sma_10", "sma_20", "sma_50", "sma_100", "sma_200",
        "ema_9", "ema_12", "ema_26", "ema_50", "ema_200",
        "atr_14", "stddev_20", "stoch_k", "stoch_d", "roc_10", "momentum_10",
        "willr_14", "hist_vol_20", "keltner_upper", "keltner_lower", "price"
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
        FROM daily_analytics
        WHERE symbol = %s
        ORDER BY timestamp DESC
        LIMIT %s
        """
        
        with conn.cursor() as cur:
            cur.execute(query, (symbol, count))
            rows = cur.fetchall()
            
            if not rows:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No historical daily analytics found for {symbol}"
                )
            
            result = []
            for row in rows:
                row_dict = dict(row)
                ts = row_dict['timestamp']
                if isinstance(ts, datetime):
                    ts_dt = ts
                elif isinstance(ts, date):
                    ts_dt = datetime.combine(ts, datetime.min.time())
                else:
                    ts_dt = datetime.strptime(str(ts), "%Y-%m-%d %H:%M:%S")

                row_dict['timestamp_utc'] = ts_dt.isoformat()
                row_dict['date_est'] = format_est_datetime(ts_dt).split(' ')[0]
                result.append(row_dict)
            
            metadata = {
                "symbol": symbol,
                "indicators": indicators,
                "count": len(result),
                "requested_count": count,
                "data": result,
                "timezone_note": "Historical daily analytics represent end-of-day calculations in America/New_York timezone"
            }
            return create_formatted_response(result, format, metadata, f"historical_analytics_{symbol}")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching last historical analytics: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()
