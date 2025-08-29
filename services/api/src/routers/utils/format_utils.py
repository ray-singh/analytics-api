import csv
import io
from typing import List, Dict, Any
from fastapi import Response
import json

def format_response_data(data: List[Dict[str, Any]], format_type: str, filename: str = "data") -> Response:
    """
    Format response data as JSON or CSV based on format_type.
    
    Args:
        data: List of dictionaries containing the data
        format_type: Either 'json' or 'csv'
        filename: Base filename for CSV downloads
        
    Returns:
        FastAPI Response object with appropriate content type and data
    """
    if format_type.lower() == 'csv':
        return convert_to_csv_response(data, filename)
    else:
        # Default to JSON
        return Response(
            content=json.dumps(data, indent=2, default=str),
            media_type="application/json"
        )

def convert_to_csv_response(data: List[Dict[str, Any]], filename: str = "data") -> Response:
    """
    Convert list of dictionaries to CSV format and return as FastAPI Response.
    
    Args:
        data: List of dictionaries to convert
        filename: Filename for the CSV download
        
    Returns:
        FastAPI Response with CSV content
    """
    if not data:
        return Response(
            content="No data available",
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}.csv"}
        )
    
    output = io.StringIO()
    
    fieldnames = set()
    for row in data:
        fieldnames.update(row.keys())
    
    fieldnames = sorted(list(fieldnames))
    
    # Write CSV
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in data:
        # Handle None values and convert complex types to strings
        clean_row = {}
        for key in fieldnames:
            value = row.get(key)
            if value is None:
                clean_row[key] = ""
            elif isinstance(value, (dict, list)):
                clean_row[key] = json.dumps(value)
            else:
                clean_row[key] = str(value)
        writer.writerow(clean_row)
    
    csv_content = output.getvalue()
    output.close()
    
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}.csv"}
    )

def get_all_available_indicators() -> List[str]:
    """
    Get list of all available technical indicators.
    
    Returns:
        List of all available indicator names
    """
    return [
        # RSI variants
        "rsi_7", "rsi_14", "rsi_21",
        
        # MACD
        "macd", "macd_signal", "macd_hist",
        
        # Bollinger Bands
        "bb_upper", "bb_middle", "bb_lower", "bb_bandwidth", "bb_percent_b",
        
        # Simple Moving Averages
        "sma_5", "sma_10", "sma_20", "sma_50", "sma_100", "sma_200",
        
        # Exponential Moving Averages
        "ema_9", "ema_12", "ema_26", "ema_50", "ema_200",
        
        # Volatility Indicators
        "atr_14", "stddev_20", "hist_vol_20",
        
        # Momentum Indicators
        "stoch_k", "stoch_d", "roc_10", "momentum_10", "willr_14",
        
        # Keltner Channels
        "keltner_upper", "keltner_lower",
        
        # Price (always included)
        "price"
    ]

def validate_and_get_indicators(requested_indicators: List[str]) -> List[str]:
    """
    Validate requested indicators and return the final list to use.
    If requested_indicators is empty, return all available indicators.
    
    Args:
        requested_indicators: List of requested indicator names
        
    Returns:
        List of validated indicator names
        
    Raises:
        ValueError: If any requested indicator is invalid
    """
    available_indicators = get_all_available_indicators()
    
    # If empty list, return all indicators
    if not requested_indicators:
        return available_indicators
    
    # Validate each requested indicator
    invalid_indicators = [ind for ind in requested_indicators if ind not in available_indicators]
    if invalid_indicators:
        raise ValueError(f"Invalid indicators: {invalid_indicators}. Available indicators: {available_indicators}")
    
    # Always include price if not already requested
    final_indicators = list(requested_indicators)
    if "price" not in final_indicators:
        final_indicators.append("price")
    
    return final_indicators

def create_formatted_response(
    data: List[Dict[str, Any]], 
    format_type: str,
    response_metadata: Dict[str, Any],
    filename_prefix: str = "market_data"
) -> Response:
    """
    Create a formatted response with metadata for JSON or just data for CSV.
    
    Args:
        data: The actual data to return
        format_type: 'json' or 'csv'
        response_metadata: Metadata to include in JSON responses
        filename_prefix: Prefix for CSV filename
        
    Returns:
        FastAPI Response object
    """
    if format_type.lower() == 'csv':
        return convert_to_csv_response(data, filename_prefix)
    else:
        full_response = {**response_metadata, "data": data}
        return Response(
            content=json.dumps(full_response, indent=2, default=str),
            media_type="application/json"
        )