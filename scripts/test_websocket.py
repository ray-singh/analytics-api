#!/usr/bin/env python3
"""
Test script for viewing Yahoo Finance WebSocket data
Usage: python scripts/test_websocket.py AAPL MSFT GOOG
"""
import asyncio
import sys
import os
import logging
from datetime import datetime
import colorama
from colorama import Fore, Style

# Add project root to Python path so we can import the client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the YFinanceClient
from services.market_data.src.clients.yfinance_client import YFinanceClient

# Initialize colorama
colorama.init()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Keep price history for display
price_history = {}

async def display_price_stream(symbols, duration=None):
    """Display real-time price updates for symbols"""
    client = YFinanceClient()
    
    # Set up price tracking
    last_prices = {symbol: None for symbol in symbols}
    
    print(f"{Fore.CYAN}Starting real-time price stream for: {', '.join(symbols)}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Press Ctrl+C to exit{Style.RESET_ALL}")
    print("-" * 60)
    
    # Header row
    print(f"{Fore.WHITE}{'Symbol':<8} {'Price':<10} {'Change':<10} {'Time':<12} {'Volume'}{Style.RESET_ALL}")
    print("-" * 60)
    
    async def price_callback(data):
        symbol = data.get('symbol')
        price = data.get('price')
        
        if price is not None and symbol in last_prices:
            # Calculate price change
            prev_price = last_prices[symbol]
            if prev_price is not None:
                change = price - prev_price
                change_pct = (change / prev_price) * 100 if prev_price > 0 else 0
                
                # Color based on price change
                if change > 0:
                    color = Fore.GREEN
                    change_str = f"+{change:.2f} ({change_pct:.2f}%)"
                elif change < 0:
                    color = Fore.RED
                    change_str = f"{change:.2f} ({change_pct:.2f}%)"
                else:
                    color = Fore.WHITE
                    change_str = "0.00 (0.00%)"
            else:
                color = Fore.WHITE
                change_str = "N/A"
                
            # Update last price
            last_prices[symbol] = price
            
            # Format timestamp
            timestamp = datetime.now().strftime("%H:%M:%S")
            
            # Print price update
            print(f"{color}{symbol:<8} {price:<10.2f} {change_str:<10} {timestamp} {data.get('volume', 'N/A')}{Style.RESET_ALL}")
    
    # Subscribe to price updates for each symbol
    for symbol in symbols:
        await client.subscribe_to_price_updates(symbol, price_callback)
    
    try:
        # Run until duration or indefinitely if duration is None
        if duration:
            await asyncio.sleep(duration)
        else:
            while True:
                await asyncio.sleep(3600)  # Just keep running
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Stream interrupted by user{Style.RESET_ALL}")
    finally:
        # Clean up
        for symbol in symbols:
            await client.unsubscribe_from_price_updates(symbol)
        await client.close()

def main():
    # Get symbols from command line arguments
    if len(sys.argv) < 2:
        print("Usage: python scripts/test_websocket.py SYMBOL1 [SYMBOL2 ...]")
        print("Example: python scripts/test_websocket.py AAPL MSFT GOOG")
        sys.exit(1)
        
    symbols = sys.argv[1:]
    
    try:
        asyncio.run(display_price_stream(symbols))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()