import asyncio
import json
import os
import logging
from datetime import datetime
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv
from services.market_data.src.clients.twelvedata_client import TwelveDataClient
from services.market_data.src.clients.alphavantage_client import AlphaVantageClient
from services.market_data.src.clients.yfinance_client import YFinanceClient
from services.market_data.src.backfill.backfill_manager import BackfillManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("market-data-service")

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PRICE_TOPIC = os.getenv("PRICE_TOPIC", "market.prices.raw")
BACKFILL_REQUEST_TOPIC = os.getenv("BACKFILL_REQUEST_TOPIC", "command.backfill.request")
BACKFILL_STATUS_TOPIC = os.getenv("BACKFILL_STATUS_TOPIC", "command.backfill.status")
SYMBOLS = os.getenv("STREAM_SYMBOLS", "AAPL").split(",")
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY")
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

class MarketDataService:
    def __init__(self):
        self.producer = None
        self.yf_client = YFinanceClient()
        self.td_client = TwelveDataClient()
        self.av_client = AlphaVantageClient()
        self.backfill_manager = BackfillManager()
        self._running = False
        self._tasks = []
        
    async def start(self):
        """Start the market data service"""
        if self._running:
            return
            
        self._running = True
        
        # Initialize Kafka producer
        logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}")
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            retry_backoff_ms=500,
            request_timeout_ms=30000
        )
        await self.producer.start()
        logger.info("Connected to Kafka")
        
        logger.info(f"Starting market data streams for symbols: {SYMBOLS}")
        
        # Start backfill request listener
        self._tasks.append(asyncio.create_task(self.listen_for_backfill_requests()))
        
        # Set up price streams for all symbols
        for symbol in SYMBOLS:
            logger.info(f"Setting up price stream for {symbol}")
            self._tasks.append(asyncio.create_task(self.setup_price_stream(symbol)))
        
        # Health check task
        self._tasks.append(asyncio.create_task(self._health_check()))
    
    async def setup_price_stream(self, symbol):
        """Set up real-time price streaming for a symbol"""
        async def on_price_update(price_data):
            """Process price updates"""
            try:
                # Skip heartbeats
                if price_data.get("type") == "heartbeat":
                    return
                    
                # Log the data for debugging
                logger.debug(f"Received price update for {symbol}: {price_data}")
                    
                # Send to Kafka
                await self.producer.send_and_wait(
                    PRICE_TOPIC,
                    json.dumps(price_data).encode("utf-8")
                )
                
            except Exception as e:
                logger.error(f"Error publishing price update for {symbol}: {e}")
        
        await self.yf_client.subscribe_to_price_updates(symbol, on_price_update)
        try:
            while self._running:
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            logger.info(f"Price stream for {symbol} cancelled")
            await self.yf_client.unsubscribe_from_price_updates(symbol, on_price_update)
            raise
    
    async def _health_check(self):
        """Periodic health check"""
        try:
            while self._running:
                await asyncio.sleep(300)  # Check every 5 minutes
                logger.info("Market data service health check - running")
                
                # Check Kafka connection
                try:
                    cluster_metadata = await self.producer.client.fetch_all_metadata()
                    topic_count = len(cluster_metadata.topics)
                    logger.info(f"Kafka connection OK - {topic_count} topics available")
                except Exception as e:
                    logger.error(f"Kafka connection issue: {e}")
                    
        except asyncio.CancelledError:
            logger.info("Health check cancelled")
            raise
    
    async def listen_for_backfill_requests(self):
        """Listen for backfill requests on Kafka"""
        from aiokafka import AIOKafkaConsumer
        
        logger.info(f"Starting backfill request listener on {BACKFILL_REQUEST_TOPIC}")
        
        consumer = AIOKafkaConsumer(
            BACKFILL_REQUEST_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="market-data-service-backfill"
        )
        
        await consumer.start()
        
        try:
            async for msg in consumer:
                try:
                    request = json.loads(msg.value.decode("utf-8"))
                    logger.info(f"Received backfill request: {request}")
                    
                    # Process backfill request in a separate task
                    asyncio.create_task(self.process_backfill_request(request))
                    
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON in backfill request: {msg.value}")
                except Exception as e:
                    logger.error(f"Error processing backfill request: {e}")
        except asyncio.CancelledError:
            logger.info("Backfill request listener cancelled")
        finally:
            await consumer.stop()
    
    async def process_backfill_request(self, request):
        """Process a backfill request"""
        request_id = request.get("request_id", "unknown")
        symbol = request.get("symbol")
        interval = request.get("interval")
        start_date = request.get("start_date")
        end_date = request.get("end_date")
        
        if not all([symbol, interval]):
            error_msg = "Missing required parameters in backfill request"
            logger.error(error_msg)
            await self.send_backfill_status(request_id, "error", error_msg)
            return
        
        try:
            # Send status update
            await self.send_backfill_status(request_id, "started", 
                                          f"Starting backfill for {symbol} ({interval})")
            
            # Choose appropriate data source based on interval
            if interval in ['1day', '1d', 'daily']:
                # Use AlphaVantage for historical daily data
                data = await self.backfill_manager.backfill_daily_data(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date
                )
            else:
                # Use TwelveData for intraday data
                data = await self.backfill_manager.backfill_intraday_data(
                    symbol=symbol,
                    interval=interval,
                    start_date=start_date,
                    end_date=end_date
                )
            
            if data.empty:
                await self.send_backfill_status(request_id, "error", 
                                              f"No data found for {symbol} ({interval})")
                return
            
            # Convert data to OHLCV format and send to Kafka
            # Implementation depends on how you handle historical data
            
            # Send success status
            count = len(data)
            await self.send_backfill_status(request_id, "completed", 
                                          f"Backfill completed for {symbol} ({interval}): {count} bars")
            
        except Exception as e:
            logger.error(f"Error in backfill for {symbol}: {e}")
            await self.send_backfill_status(request_id, "error", str(e))
    
    async def send_backfill_status(self, request_id, status, message):
        """Send backfill status update to Kafka"""
        status_msg = {
            "request_id": request_id,
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "message": message
        }
        
        await self.producer.send_and_wait(
            BACKFILL_STATUS_TOPIC,
            json.dumps(status_msg).encode("utf-8")
        )
        
    async def stop(self):
        """Stop the market data service"""
        if not self._running:
            return
            
        self._running = False
        logger.info("Stopping market data service")
        
        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
            
        # Wait for all tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()
            
        # Close WebSocket connections
        await self.yf_client.close()
        
        # Stop Kafka producer
        if self.producer:
            await self.producer.stop()
            
        logger.info("Market data service stopped")

async def main():
    """Main entry point for the market data service"""
    service = MarketDataService()
    
    # Set up signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    
    # Start the service
    try:
        await service.start()
        
        # Keep running until interrupted
        while True:
            await asyncio.sleep(3600)
            
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Service interrupted, shutting down gracefully")
    finally:
        await service.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Exiting market data service")