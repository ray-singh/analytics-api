import numpy as np
import pandas as pd

def calculate_real_time_indicators(state):
    """Calculate indicators for real-time tick data"""
    indicators = {}
    prices = np.array(state["prices"])
    volumes = np.array(state["volumes"])
    
    # Price velocity (rate of change)
    if len(prices) >= 10:
        # Short-term velocity (last 10 ticks)
        short_velocity = (prices[-1] - prices[-10]) / prices[-10] * 100
        indicators["price_velocity_10"] = round(float(short_velocity), 4)
    
    if len(prices) >= 50:
        # Medium-term velocity (last 50 ticks)
        medium_velocity = (prices[-1] - prices[-50]) / prices[-50] * 100
        indicators["price_velocity_50"] = round(float(medium_velocity), 4)
    
    # Tick momentum
    if len(prices) >= 20:
        # Count recent up vs down ticks
        up_ticks = sum(1 for i in range(1, 20) if prices[-i] > prices[-i-1])
        down_ticks = sum(1 for i in range(1, 20) if prices[-i] < prices[-i-1])
        
        # Momentum ratio
        if down_ticks > 0:
            tick_ratio = up_ticks / down_ticks
        else:
            tick_ratio = 10.0  #
            
        indicators["tick_ratio"] = round(float(tick_ratio), 4)
    
    # Volume profile
    if len(volumes) >= 20 and sum(volumes[-20:]) > 0:
        # Volume weighted average price (VWAP)
        recent_volumes = volumes[-20:]
        recent_prices = prices[-20:]
        vwap = np.sum(recent_prices * recent_volumes) / np.sum(recent_volumes)
        indicators["tick_vwap_20"] = round(float(vwap), 4)
        
        # Volume momentum (increasing or decreasing)
        vol_momentum = sum(volumes[-10:]) / (sum(volumes[-20:-10]) + 1)
        indicators["volume_momentum"] = round(float(vol_momentum), 4)
    
    # Bollinger bandwidth calculation for volatility
    if len(prices) >= 20:
        # Calculate moving average and standard deviation
        ma20 = np.mean(prices[-20:])
        std20 = np.std(prices[-20:])
        
        # Bollinger Bands
        upper_band = ma20 + (2 * std20)
        lower_band = ma20 - (2 * std20)
        
        # Bandwidth = (Upper - Lower) / Middle
        bandwidth = (upper_band - lower_band) / ma20
        indicators["bb_bandwidth"] = round(float(bandwidth), 4)
        
        # Price relative to bands (0 = at lower, 1 = at upper)
        if upper_band > lower_band:
            band_position = (prices[-1] - lower_band) / (upper_band - lower_band)
            indicators["bb_position"] = round(float(band_position), 4)
    
    # Real-time RSI approximation
    if len(prices) >= 14:
        deltas = np.diff(prices[-15:])
        seed = deltas[:14]
        up = seed[seed >= 0].sum()
        down = -seed[seed < 0].sum()
        
        if down > 0:
            rs = up / down
        else:
            rs = 100.0
            
        rsi = 100.0 - (100.0 / (1.0 + rs))
        indicators["tick_rsi_14"] = round(float(rsi), 2)
    
    return indicators