import numpy as np
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import math

class TechnicalIndicators:
    def __init__(self):
        self.price_history = defaultdict(list)
        self.volume_history = defaultdict(list)
        
    def add_price(self, symbol: str, price: float, volume: Optional[int] = None, timestamp: Optional[datetime] = None):
        """Add a new price point to the history"""
        self.price_history[symbol].append({
            'price': price,
            'volume': volume,
            'timestamp': timestamp or datetime.now()
        })
        
        if volume:
            self.volume_history[symbol].append({
                'volume': volume,
                'timestamp': timestamp or datetime.now()
            })
    
    def get_prices(self, symbol: str, window: int = None) -> List[float]:
        """Get price history for a symbol"""
        prices = [p['price'] for p in self.price_history[symbol]]
        if window:
            return prices[-window:]
        return prices
    
    def simple_moving_average(self, symbol: str, window: int) -> Optional[float]:
        """Calculate Simple Moving Average"""
        prices = self.get_prices(symbol, window)
        if len(prices) >= window:
            return sum(prices) / window
        return None
    
    def exponential_moving_average(self, symbol: str, window: int) -> Optional[float]:
        """Calculate Exponential Moving Average"""
        prices = self.get_prices(symbol)
        if len(prices) < window:
            return None
            
        alpha = 2 / (window + 1)
        ema = prices[0]
        
        for price in prices[1:]:
            ema = alpha * price + (1 - alpha) * ema
            
        return ema
    
    def weighted_moving_average(self, symbol: str, window: int) -> Optional[float]:
        """Calculate Weighted Moving Average"""
        prices = self.get_prices(symbol, window)
        if len(prices) < window:
            return None
            
        weights = list(range(1, window + 1))
        weighted_sum = sum(p * w for p, w in zip(prices, weights))
        weight_sum = sum(weights)
        
        return weighted_sum / weight_sum
    
    def relative_strength_index(self, symbol: str, window: int = 14) -> Optional[float]:
        """Calculate Relative Strength Index"""
        prices = self.get_prices(symbol)
        if len(prices) < window + 1:
            return None
            
        deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        gains = [d if d > 0 else 0 for d in deltas]
        losses = [-d if d < 0 else 0 for d in deltas]
        
        avg_gain = sum(gains[-window:]) / window
        avg_loss = sum(losses[-window:]) / window
        
        if avg_loss == 0:
            return 100
            
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def macd(self, symbol: str, fast: int = 12, slow: int = 26, signal: int = 9) -> Optional[Dict[str, float]]:
        """Calculate MACD (Moving Average Convergence Divergence)"""
        prices = self.get_prices(symbol)
        if len(prices) < slow:
            return None
            
        ema_fast = self._calculate_ema(prices, fast)
        ema_slow = self._calculate_ema(prices, slow)
        
        if ema_fast is None or ema_slow is None:
            return None
            
        macd_line = ema_fast - ema_slow
        
        # Calculate signal line (EMA of MACD line)
        macd_values = []
        for i in range(len(prices) - slow + 1):
            fast_ema = self._calculate_ema(prices[i:i+slow], fast)
            slow_ema = self._calculate_ema(prices[i:i+slow], slow)
            if fast_ema and slow_ema:
                macd_values.append(fast_ema - slow_ema)
        
        if len(macd_values) < signal:
            return None
            
        signal_line = self._calculate_ema(macd_values, signal)
        histogram = macd_line - signal_line if signal_line else None
        
        return {
            'macd_line': macd_line,
            'signal_line': signal_line,
            'histogram': histogram
        }
    
    def bollinger_bands(self, symbol: str, window: int = 20, std_dev: float = 2) -> Optional[Dict[str, float]]:
        """Calculate Bollinger Bands"""
        prices = self.get_prices(symbol, window)
        if len(prices) < window:
            return None
            
        sma = sum(prices) / window
        variance = sum((p - sma) ** 2 for p in prices) / window
        std = math.sqrt(variance)
        
        upper_band = sma + (std_dev * std)
        lower_band = sma - (std_dev * std)
        
        return {
            'upper_band': upper_band,
            'middle_band': sma,
            'lower_band': lower_band,
            'bandwidth': (upper_band - lower_band) / sma,
            'percent_b': (prices[-1] - lower_band) / (upper_band - lower_band)
        }
    
    def average_true_range(self, symbol: str, window: int = 14) -> Optional[float]:
        """Calculate Average True Range"""
        prices = self.get_prices(symbol)
        if len(prices) < window + 1:
            return None
            
        true_ranges = []
        for i in range(1, len(prices)):
            high = prices[i]
            low = prices[i-1]
            prev_close = prices[i-1] if i > 1 else prices[0]
            
            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)
            
            true_range = max(tr1, tr2, tr3)
            true_ranges.append(true_range)
        
        return sum(true_ranges[-window:]) / window
    
    def volume_weighted_average_price(self, symbol: str, window: int = 20) -> Optional[float]:
        """Calculate Volume Weighted Average Price"""
        prices = self.get_prices(symbol, window)
        volumes = [p.get('volume', 1) for p in self.price_history[symbol][-window:]]
        
        if len(prices) < window or not volumes:
            return None
            
        total_volume = sum(volumes)
        if total_volume == 0:
            return None
            
        vwap = sum(p * v for p, v in zip(prices, volumes)) / total_volume
        return vwap
    
    def _calculate_ema(self, prices: List[float], window: int) -> Optional[float]:
        """Helper method to calculate EMA"""
        if len(prices) < window:
            return None
            
        alpha = 2 / (window + 1)
        ema = prices[0]
        
        for price in prices[1:]:
            ema = alpha * price + (1 - alpha) * ema
            
        return ema
    
    def get_all_indicators(self, symbol: str) -> Dict[str, any]:
        """Get all technical indicators for a symbol"""
        indicators = {}
        
        # Moving Averages
        indicators['sma_5'] = self.simple_moving_average(symbol, 5)
        indicators['sma_20'] = self.simple_moving_average(symbol, 20)
        indicators['ema_12'] = self.exponential_moving_average(symbol, 12)
        indicators['ema_26'] = self.exponential_moving_average(symbol, 26)
        indicators['wma_10'] = self.weighted_moving_average(symbol, 10)
        
        # Oscillators
        indicators['rsi'] = self.relative_strength_index(symbol, 14)
        
        # MACD
        macd_data = self.macd(symbol)
        if macd_data:
            indicators['macd'] = macd_data
        
        # Bollinger Bands
        bb_data = self.bollinger_bands(symbol)
        if bb_data:
            indicators['bollinger_bands'] = bb_data
        
        # Volatility
        indicators['atr'] = self.average_true_range(symbol, 14)
        
        # Volume
        indicators['vwap'] = self.volume_weighted_average_price(symbol, 20)
        
        return indicators 