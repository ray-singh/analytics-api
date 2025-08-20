import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Dict, Set, Callable, Any
from twelvedata import TDClient

logger = logging.getLogger("market_data_service.twelvedata_ws")

class TwelveDataWebSocketClient:
    """
    Client for TwelveData WebSocket API for real-time price streaming.
    """
    
    def __init__(self):
        """Initialize the TwelveData WebSocket client."""
        self.api_key = os.getenv("TWELVEDATA_API_KEY")
        if not self.api_key:
            raise ValueError("TWELVEDATA_API_KEY environment variable not set")
            
        self.td_client = TDClient(apikey=self.api_key)
        self.websocket = None
        self._subscribers = {}  # {symbol: set(callbacks)}
        self._running = False
        self._heartbeat_task = None
        self._ws_task = None
        
    async def connect(self):
        """Connect to the TwelveData WebSocket API."""
        if self._running:
            return
            
        self._running = True
        self._ws_task = asyncio.create_task(self._run_websocket())
        self._heartbeat_task = asyncio.create_task(self._send_heartbeats())
        logger.info("TwelveData WebSocket client started")
        
    async def _run_websocket(self):
        """Run the WebSocket connection and event handler."""
        # Get all symbols we need to subscribe to
        symbols = list(self._subscribers.keys())
        if not symbols:
            logger.warning("No symbols to subscribe to")
            return
            
        logger.info(f"Connecting WebSocket for symbols: {symbols}")
        
        try:
            self.websocket = self.td_client.websocket(on_event=self._handle_event)
            self.websocket.subscribe(symbols)
            
            # Connect to WebSocket
            self.websocket.connect()
            logger.info("WebSocket connected")
            
            # Keep the connection alive until stopped
            while self._running:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            if self.websocket:
                self.websocket.close()
                logger.info("WebSocket closed")
    
    async def _send_heartbeats(self):
        """Send periodic heartbeats to keep the connection alive."""
        try:
            while self._running and self.websocket:
                await asyncio.sleep(10)  # Send heartbeat every 10 seconds
                if self.websocket:
                    # The heartbeat method is synchronous
                    self.websocket.heartbeat()
                    logger.debug("Sent heartbeat")
        except asyncio.CancelledError:
            logger.info("Heartbeat task cancelled")
        except Exception as e:
            logger.error(f"Error in heartbeat task: {e}")
    
    def _handle_event(self, event):
        """Handle events from the WebSocket."""
        try:
            # Skip handling if we're not running
            if not self._running:
                return
                
            # Log the event type
            event_type = event.get("event")
            
            # Handle different event types
            if event_type == "heartbeat":
                logger.debug("Received heartbeat")
            elif event_type == "subscribe-status":
                success = event.get("success", [])
                fails = event.get("fails", [])
                if success:
                    logger.info(f"Successfully subscribed to {len(success)} symbols: {[s.get('symbol') for s in success]}")
                if fails:
                    logger.warning(f"Failed to subscribe to {len(fails)} symbols: {fails}")
            elif event_type == "price":
                symbol = event.get("symbol")
                price = event.get("price")
                
                # Log every price update at INFO level
                logger.info(f"📊 TD PRICE: {symbol} @ ${price} (vol: {event.get('day_volume', 'N/A')}, ts: {event.get('timestamp')})")
                
                if symbol and price and symbol in self._subscribers:
                    # Create price update data
                    price_data = {
                        "symbol": symbol,
                        "price": float(price),
                        "timestamp": datetime.fromtimestamp(event.get("timestamp", 0)).isoformat(),
                        "volume": int(event.get("day_volume", 0) or 0),
                        "source": "twelvedata"
                    }
                    
                    logger.debug(f"Notifying {len(self._subscribers.get(symbol, []))} subscribers for {symbol}")
                    
                    for callback in list(self._subscribers.get(symbol, set())):
                        try:
                            # Run the callback
                            callback(price_data)
                        except Exception as e:
                            logger.error(f"Error in subscriber callback: {e}")
        else:
            # Log any other event types we receive
            logger.info(f"Received {event_type} event: {event}")
                
        except Exception as e:
            logger.error(f"Error handling WebSocket event: {e}", exc_info=True)
    
    async def subscribe_to_price_updates(self, symbol: str, callback: Callable[[Dict[str, Any]], None]):
        """
        Subscribe to real-time price updates for a symbol.
        
        Args:
            symbol: The stock symbol to subscribe to
            callback: Function to call with price updates
        """
        symbol = symbol.upper()
        
        # Add to subscribers dict
        if symbol not in self._subscribers:
            self._subscribers[symbol] = set()
        self._subscribers[symbol].add(callback)
        
        logger.info(f"Added subscriber for {symbol}, total subscribers: {len(self._subscribers[symbol])}")
        
        # If we're already running, we need to reconnect with the new symbol
        if self._running and self.websocket:
            # Close current connection
            self._running = False
            if self._ws_task:
                self._ws_task.cancel()
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
            
            # Wait a moment
            await asyncio.sleep(1)
            
            # Restart
            self._running = True
            self._ws_task = asyncio.create_task(self._run_websocket())
            self._heartbeat_task = asyncio.create_task(self._send_heartbeats())
        elif not self._running:
            await self.connect()
    
    async def unsubscribe_from_price_updates(self, symbol: str, callback: Callable[[Dict[str, Any]], None]):
        """
        Unsubscribe from price updates for a symbol.
        
        Args:
            symbol: The stock symbol to unsubscribe from
            callback: The callback function to remove
        """
        symbol = symbol.upper()
        
        if symbol in self._subscribers and callback in self._subscribers[symbol]:
            self._subscribers[symbol].remove(callback)
            logger.info(f"Removed subscriber for {symbol}, remaining: {len(self._subscribers[symbol])}")
            
            # If no subscribers left for this symbol, clean up
            if not self._subscribers[symbol]:
                del self._subscribers[symbol]
                logger.info(f"No more subscribers for {symbol}, removed from subscription list")
                
                # If no symbols left, we can close the connection
                if not self._subscribers and self._running:
                    await self.close()
    
    async def close(self):
        """Close the WebSocket connection."""
        if self._running:
            self._running = False
            
            # Cancel tasks
            if self._ws_task:
                self._ws_task.cancel()
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
                
            # Close WebSocket
            if self.websocket:
                self.websocket.close()
                self.websocket = None
                
            logger.info("TwelveData WebSocket client stopped")