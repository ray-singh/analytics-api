import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, Set, Callable, Optional

try:
    import yfinance as yf
except ImportError:
    raise ImportError("yfinance package is required. Install with 'pip install yfinance'.")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("market_data_service.yfinance")

class YFinanceClient:
    """
    Simple YFinance WebSocket client focused on real-time data streaming.
    Handles connections to Yahoo Finance WebSocket API with robust error handling.
    """
    
    def __init__(self):
        self._subscribers: Dict[str, Set[Callable]] = {}
        self._websockets: Dict[str, yf.AsyncWebSocket] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._stopping = False
        
    async def subscribe_to_price_updates(self, symbol: str, callback: Callable):
        """
        Subscribe to real-time price updates for a symbol
        
        Args:
            symbol: Stock ticker symbol
            callback: Async function that will be called with price updates
        """
        # Add callback to subscribers
        if symbol not in self._subscribers:
            self._subscribers[symbol] = set()
            # Start streaming for this symbol
            asyncio.create_task(self._start_symbol_stream(symbol))
        
        self._subscribers[symbol].add(callback)
        logger.info(f"Added subscriber for {symbol}, total subscribers: {len(self._subscribers[symbol])}")
    
    async def unsubscribe_from_price_updates(self, symbol: str, callback: Optional[Callable] = None):
        """
        Unsubscribe from price updates for a symbol
        
        Args:
            symbol: Stock ticker symbol
            callback: Specific callback to remove (or all if None)
        """
        if symbol not in self._subscribers:
            return
            
        if callback:
            # Remove specific callback
            self._subscribers[symbol].discard(callback)
            logger.info(f"Removed subscriber for {symbol}, remaining: {len(self._subscribers[symbol])}")
            
            # If no subscribers left, stop the WebSocket
            if not self._subscribers[symbol]:
                await self._stop_symbol_stream(symbol)
        else:
            # Remove all subscribers for this symbol
            self._subscribers[symbol].clear()
            await self._stop_symbol_stream(symbol)
    
    async def _start_symbol_stream(self, symbol: str):
        """Start streaming data for a symbol"""
        if symbol in self._tasks and not self._tasks[symbol].done():
            return  # Already streaming
        
        self._tasks[symbol] = asyncio.create_task(self._stream_symbol_data(symbol))
        logger.info(f"Started streaming task for {symbol}")
    
    async def _stop_symbol_stream(self, symbol: str):
        """Stop streaming data for a symbol"""
        if symbol in self._websockets:
            try:
                await self._websockets[symbol].close()
            except Exception as e:
                logger.warning(f"Error closing WebSocket for {symbol}: {e}")
            del self._websockets[symbol]
            
        if symbol in self._tasks:
            task = self._tasks[symbol]
            if not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=2)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            del self._tasks[symbol]
            
        logger.info(f"Stopped streaming for {symbol}")
    
    async def _stream_symbol_data(self, symbol: str):
        """Stream data for a symbol with automatic reconnection"""
        retries = 0
        max_retries = 10
        delay = 1
        
        while not self._stopping and retries < max_retries:
            try:
                # Create a WebSocket for this symbol
                ws = yf.AsyncWebSocket()
                self._websockets[symbol] = ws
                
                async with ws:
                    # Subscribe to the symbol
                    await ws.subscribe([symbol])
                    
                    # Process messages
                    async def message_handler(message):
                        # Check if message is for our symbol
                        msg_symbol = message.get('id', message.get('symbol'))
                        if msg_symbol != symbol:
                            return
                            
                        # Debug log the message
                        logger.debug(f"Received data for {symbol}: {message}")
                        
                        # Create a standardized price update
                        if 'price' in message:
                            try:
                                price_value = float(message['price'])
                                
                                price_data = {
                                    "symbol": symbol,
                                    "price": price_value,
                                    "timestamp": datetime.now().isoformat(),
                                    "volume": int(message.get("day_volume", 0) or 0),
                                    "source": "yfinance"
                                }
                                
                                # Log successful price update
                                logger.info(f"Price update for {symbol}: ${price_value}")
                                
                                # Notify subscribers
                                await self._notify_subscribers(symbol, price_data)
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Invalid price data for {symbol}: {e}")
                    
                    # Listen for messages
                    await ws.listen(message_handler)
                    
            except asyncio.CancelledError:
                logger.info(f"Streaming task cancelled for {symbol}")
                break
                
            except Exception as e:
                retries += 1
                logger.error(f"Error in {symbol} stream (attempt {retries}/{max_retries}): {e}")
                
                # Exponential backoff
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)  # Max 60 seconds delay
                
                # Try to clean up the websocket
                if symbol in self._websockets:
                    try:
                        await self._websockets[symbol].close()
                    except Exception:
                        pass
                    del self._websockets[symbol]
                    
        if retries >= max_retries:
            logger.error(f"Stopped streaming for {symbol} after {max_retries} failed attempts")
    
    async def _notify_subscribers(self, symbol: str, data: dict):
        """Notify all subscribers for a symbol"""
        if symbol not in self._subscribers:
            return
            
        # Make a copy to avoid modification during iteration
        subscribers = list(self._subscribers[symbol])
        
        for callback in subscribers:
            try:
                await callback(data)
            except Exception as e:
                logger.error(f"Error in subscriber callback for {symbol}: {e}")
    
    async def stream_prices(self, symbol: str):
        """
        Stream real-time prices for a symbol (generator function)
        
        Args:
            symbol: Stock ticker symbol
            
        Yields:
            dict: Price update data
        """
        queue = asyncio.Queue()
        
        async def price_callback(price_data: dict):
            # Only forward actual price updates
            if "price" in price_data and price_data["price"] > 0:
                await queue.put(price_data)
        
        # Subscribe to price updates
        await self.subscribe_to_price_updates(symbol, price_callback)
        
        try:
            while True:
                try:
                    # Get price update with timeout
                    price_data = await asyncio.wait_for(queue.get(), timeout=60)
                    yield price_data
                except asyncio.TimeoutError:
                    # Send a heartbeat if no data received for 60 seconds
                    yield {
                        "symbol": symbol,
                        "type": "heartbeat",
                        "timestamp": datetime.now().isoformat(),
                        "source": "yfinance"
                    }
        finally:
            # Clean up subscription
            await self.unsubscribe_from_price_updates(symbol, price_callback)
    
    async def close(self):
        """Close all WebSocket connections and clean up"""
        logger.info("Closing all YFinance WebSocket connections")
        self._stopping = True
        
        # Close all websockets
        for symbol in list(self._websockets.keys()):
            await self._stop_symbol_stream(symbol)
            
        # Clear subscribers
        self._subscribers.clear()
