from datetime import datetime
import pytz
from typing import Optional, Tuple

# est timezone (handles EST/EDT automatically)
est_TZ = pytz.timezone('America/New_York')
UTC_TZ = pytz.UTC

def parse_est_datetime(date_str: str, time_str: Optional[str] = None) -> datetime:
    """
    Parse date and optional time in est timezone and convert to UTC.
    
    Args:
        date_str: Date in YYYY-MM-DD format
        time_str: Optional time in HH:MM:SS or HH:MM format
        
    Returns:
        datetime: UTC datetime object
        
    Raises:
        ValueError: If date/time format is invalid
    """
    if time_str:
        # Combine date and time
        if len(time_str.split(':')) == 2:
            time_str += ':00'  # Add seconds if not provided
        datetime_str = f"{date_str} {time_str}"
        naive_dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
    else:
        # Date only - default to start of day (00:00:00)
        naive_dt = datetime.strptime(date_str, '%Y-%m-%d')
    
    # Localize to est timezone
    est_dt = est_TZ.localize(naive_dt)
    
    # Convert to UTC
    return est_dt.astimezone(UTC_TZ)

def parse_est_date_range(
    start_date: str, 
    end_date: str, 
    start_time: Optional[str] = None,
    end_time: Optional[str] = None
) -> Tuple[datetime, datetime]:
    """
    Parse date range in est timezone and convert to UTC.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format  
        start_time: Optional start time in HH:MM:SS or HH:MM format
        end_time: Optional end time in HH:MM:SS or HH:MM format
        
    Returns:
        Tuple[datetime, datetime]: UTC start and end datetimes
    """
    start_dt = parse_est_datetime(start_date, start_time)
    
    # For end date, if no time specified, default to end of day
    if end_time is None and start_time is None:
        end_time = "23:59:59"
    
    end_dt = parse_est_datetime(end_date, end_time)
    
    return start_dt, end_dt

def utc_to_est(utc_dt: datetime) -> datetime:
    """
    Convert UTC datetime to est timezone.
    
    Args:
        utc_dt: UTC datetime object
        
    Returns:
        datetime: est timezone datetime
    """
    if utc_dt.tzinfo is None:
        utc_dt = UTC_TZ.localize(utc_dt)
    
    return utc_dt.astimezone(est_TZ)

def format_est_datetime(utc_dt: datetime) -> str:
    """
    Format UTC datetime as est timezone string.
    
    Args:
        utc_dt: UTC datetime object
        
    Returns:
        str: Formatted est datetime string
    """
    est_dt = utc_to_est(utc_dt)
    return est_dt.strftime('%Y-%m-%d %H:%M:%S %Z')

def validate_market_hours(est_dt: datetime) -> bool:
    """
    Check if the given est datetime falls within typical market hours.
    
    Args:
        est_dt: est timezone datetime
        
    Returns:
        bool: True if within market hours (9:30 AM - 4:00 PM ET, weekdays)
    """
    # Check if it's a weekday (Monday = 0, Sunday = 6)
    if est_dt.weekday() > 4:  # Saturday or Sunday
        return False
        
    # Check market hours (9:30 AM - 4:00 PM ET)
    market_open = est_dt.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = est_dt.replace(hour=16, minute=0, second=0, microsecond=0)
    
    return market_open <= est_dt <= market_close