import numpy as np
import pandas as pd
import talib

def calculate_momentum_indicators(df):
    """
    Calculate momentum-based technical indicators
    
    Args:
        df: DataFrame with OHLCV data (must have 'close' column)
        
    Returns:
        dict: Dictionary of calculated indicators
    """
    if len(df) < 30:
        return {}
    
    close_prices = df['close'].values
    indicators = {}

    # SMA (Simple Moving Average)
    try:
        indicators['sma_20'] = float(talib.SMA(close_prices, timeperiod=20)[-1])
        indicators['sma_50'] = float(talib.SMA(close_prices, timeperiod=50)[-1])
        indicators['sma_200'] = float(talib.SMA(close_prices, timeperiod=200)[-1])
    except Exception as e:
        print(f"Error calculating SMA: {e}")

    # EMA (Exponential Moving Average)
    try:
        indicators['ema_12'] = float(talib.EMA(close_prices, timeperiod=12)[-1])
        indicators['ema_26'] = float(talib.EMA(close_prices, timeperiod=26)[-1])
    except Exception as e:
        print(f"Error calculating EMA: {e}")
    
    # RSI (Relative Strength Index)
    try:
        rsi = talib.RSI(close_prices, timeperiod=14)
        indicators['rsi_14'] = float(rsi[-1])
    except Exception as e:
        print(f"Error calculating RSI: {e}")
    
    # MACD (Moving Average Convergence Divergence)
    try:
        macd, macd_signal, macd_hist = talib.MACD(
            close_prices, 
            fastperiod=12, 
            slowperiod=26, 
            signalperiod=9
        )
        indicators['macd'] = float(macd[-1])
        indicators['macd_signal'] = float(macd_signal[-1]) if not np.isnan(macd_signal[-1]) else None
        indicators['macd_hist'] = float(macd_hist[-1]) if not np.isnan(macd_hist[-1]) else None
    except Exception as e:
        print(f"Error calculating MACD: {e}")
    
    # Stochastic Oscillator
    try:
        if 'high' in df.columns and 'low' in df.columns:
            slowk, slowd = talib.STOCH(
                df['high'].values,
                df['low'].values,
                close_prices,
                fastk_period=14,
                slowk_period=3,
                slowd_period=3
            )
            indicators['stoch_k'] = float(slowk[-1])
            indicators['stoch_d'] = float(slowd[-1])
    except Exception as e:
        print(f"Error calculating Stochastic: {e}")
    
    # Rate of Change (ROC)
    try:
        roc = talib.ROC(close_prices, timeperiod=10)
        indicators['roc_10'] = float(roc[-1])
    except Exception as e:
        print(f"Error calculating ROC: {e}")
    
    # Momentum
    try:
        mom = talib.MOM(close_prices, timeperiod=10)
        indicators['momentum_10'] = float(mom[-1])
    except Exception as e:
        print(f"Error calculating Momentum: {e}")
    
    # Williams %R
    try:
        if 'high' in df.columns and 'low' in df.columns:
            willr = talib.WILLR(
                df['high'].values,
                df['low'].values,
                close_prices,
                timeperiod=14
            )
            indicators['willr_14'] = float(willr[-1])
    except Exception as e:
        print(f"Error calculating Williams %R: {e}")
    
    return indicators