import numpy as np
import pandas as pd
import talib

def safe_float(value):
    """Convert to float or None if NaN/Infinity"""
    try:
        value = float(value)
        return None if np.isnan(value) or np.isinf(value) else value
    except:
        return None

def calculate_volatility_indicators(df):
    """
    Calculate volatility-based technical indicators
    
    Args:
        df: DataFrame with OHLCV data (must have columns: 'close', 'high', 'low')
        
    Returns:
        dict: Dictionary of calculated indicators
    """
    if len(df) < 30:
        return {}
    
    close_prices = df['close'].values
    indicators = {}
    
    # Bollinger Bands
    try:
        upper, middle, lower = talib.BBANDS(
            close_prices,
            timeperiod=20,
            nbdevup=2,
            nbdevdn=2,
            matype=0
        )
        
        indicators['bb_upper'] = safe_float(upper[-1])
        indicators['bb_middle'] = safe_float(middle[-1])
        indicators['bb_lower'] = safe_float(lower[-1])
        
        # Calculate bandwidth and %B
        bandwidth = (upper[-1] - lower[-1]) / middle[-1]
        percent_b = (close_prices[-1] - lower[-1]) / (upper[-1] - lower[-1]) if upper[-1] != lower[-1] else 0.5
        
        indicators['bb_bandwidth'] = safe_float(bandwidth)
        indicators['bb_percent_b'] = safe_float(percent_b)
    except Exception as e:
        print(f"Error calculating Bollinger Bands: {e}")
    
    # Average True Range (ATR)
    try:
        if all(col in df.columns for col in ['high', 'low']):
            atr = talib.ATR(
                df['high'].values,
                df['low'].values,
                close_prices,
                timeperiod=14
            )
            indicators['atr_14'] = safe_float(atr[-1])
            
            # ATR percent (ATR relative to price)
            indicators['atr_percent_14'] = safe_float(atr[-1] / close_prices[-1] * 100) if close_prices[-1] > 0 else 0
    except Exception as e:
        print(f"Error calculating ATR: {e}")
    
    # Standard Deviation
    try:
        stddev = talib.STDDEV(close_prices, timeperiod=20, nbdev=1)
        indicators['stddev_20'] = safe_float(stddev[-1])
    except Exception as e:
        print(f"Error calculating Standard Deviation: {e}")
    
    # Keltner Channels (simplified version using ATR)
    try:
        if 'atr_14' in indicators and 'bb_middle' in indicators:
            keltner_upper = indicators['bb_middle'] + (2 * indicators['atr_14'])
            keltner_lower = indicators['bb_middle'] - (2 * indicators['atr_14'])
            
            indicators['keltner_upper'] = safe_float(keltner_upper)
            indicators['keltner_lower'] = safe_float(keltner_lower)
    except Exception as e:
        print(f"Error calculating Keltner Channels: {e}")
    
    # Historical Volatility (20-day)
    try:
        # Calculate daily returns
        returns = np.diff(np.log(close_prices))
        if len(returns) >= 20:
            # Annualized volatility
            hist_vol = np.std(returns[-20:]) * np.sqrt(252) * 100
            indicators['hist_vol_20'] = safe_float(hist_vol)
    except Exception as e:
        print(f"Error calculating Historical Volatility: {e}")
    
    return indicators