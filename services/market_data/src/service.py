import asyncio
import json
import os
import logging
from datetime import datetime, timedelta
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv
from services.market_data.src.clients.twelvedata_client import TwelveDataClient
from services.market_data.src.clients.alphavantage_client import AlphaVantageClient
from services.market_data.src.clients.yfinance_client import YFinanceClient
from services.market_data.src.backfill.backfill_manager import BackfillManager
from services.market_data.src.repositories.ohlcv_repository import OHLCVRepository
from datetime import timezone as tz
from prometheus_client import start_http_server, Counter

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

start_http_server(8001)
PRICE_UPDATES_PUBLISHED = Counter("price_updates_published_total", "Total price updates published", ["symbol"])
MARKET_DATA_ERRORS = Counter("market_data_errors_total", "Total market data errors")

class MarketDataService:
    def __init__(self):
        self.producer = None
        self.yf_client = YFinanceClient()
        self.backfill_manager = BackfillManager()
        self._running = False
        self._tasks = []
        self.db_url = os.getenv("DATABASE_URL", "postgresql://postgres:password@timescaledb:5432/stockanalytics")
        self.repo = OHLCVRepository(self.db_url)

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
        # Set up price streams for all symbols
        for symbol in SYMBOLS:
            logger.info(f"Setting up price stream for {symbol}")
            self._tasks.append(asyncio.create_task(self.setup_price_stream(symbol)))
        
        # Health check task
        self._tasks.append(asyncio.create_task(self._health_check()))
        
        # Schedule periodic data checks
        self._tasks.append(asyncio.create_task(self._schedule_data_checks()))
    
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
                PRICE_UPDATES_PUBLISHED.labels(symbol=symbol).inc()
                
            except Exception as e:
                logger.error(f"Error publishing price update for {symbol}: {e}")
                MARKET_DATA_ERRORS.inc()
        
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
        
    async def check_and_update_intraday_data(self):
        """
        Check if intraday data needs updating and fetch if necessary
        Should run periodically to ensure 5-minute data is up to date
        """        
        for symbol in SYMBOLS:
            try:
                latest = await self.repo.get_latest_intraday(symbol, 5)  
                need_update = False
                
                if not latest:
                    logger.info(f"No intraday data for {symbol}, fetching initial data")
                    need_update = True
                    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
                    end_date = datetime.now().strftime('%Y-%m-%d')
                else:
                    try:
                        if isinstance(latest['timestamp'], str):
                            try:
                                latest_dt = datetime.fromisoformat(latest['timestamp'])
                            except ValueError:
                                import dateutil.parser
                                latest_dt = dateutil.parser.parse(latest['timestamp'])
                        else:
                            latest_dt = latest['timestamp']
                        
                        # Ensure both datetimes are timezone-aware or both are naive
                        current_time = datetime.now()
                        
                        # If latest_dt is timezone-aware but current_time is not
                        if latest_dt.tzinfo is not None and current_time.tzinfo is None:
                            current_time = current_time.replace(tzinfo=tz.utc)
                        # If current_time is timezone-aware but latest_dt is not
                        elif latest_dt.tzinfo is None and current_time.tzinfo is not None:
                            latest_dt = latest_dt.replace(tzinfo=tz.utc)
                        
                        if (current_time - latest_dt).total_seconds() > 300:  # 5 minutes in seconds
                            logger.info(f"Intraday data for {symbol} outdated, fetching updates")
                            need_update = True
                            start_date = latest_dt.strftime('%Y-%m-%d')
                            end_date = datetime.now().strftime('%Y-%m-%d')
                    except Exception as e:
                        logger.error(f"Error processing timestamp for {symbol}: {e}")
                        need_update = True
                        start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                        end_date = datetime.now().strftime('%Y-%m-%d')
                
                if need_update:
                    await self.backfill_manager.fetch_intraday_data(
                        symbol=symbol,
                        interval="5min",
                        start_date=start_date,
                        end_date=end_date
                    )
                    logger.info(f"Updated intraday data for {symbol}")
                
            except Exception as e:
                logger.error(f"Error updating intraday data for {symbol}: {e}", exc_info=True)

    async def check_and_update_daily_data(self):
        """
        Check if daily data needs updating and fetch if necessary
        Should run once per day to ensure daily OHLCV is up to date
        """        
        for symbol in SYMBOLS:
            try:
                latest = await self.repo.get_latest_daily(symbol)
                need_update = False
                
                if not latest:
                    logger.info(f"No daily data for {symbol}, fetching initial data")
                    need_update = True
                    start_date = None
                    end_date = None
                else:
                    try:
                        # Handle timestamp with timezone awareness
                        if isinstance(latest['timestamp'], str):
                            try:
                                latest_dt = datetime.fromisoformat(latest['timestamp'])
                            except ValueError:
                                import dateutil.parser
                                latest_dt = dateutil.parser.parse(latest['timestamp'])
                        else:
                            latest_dt = latest['timestamp']
                        
                        current_time = datetime.now()
                        
                        # If latest_dt is timezone-aware but current_time is not
                        if latest_dt.tzinfo is not None and current_time.tzinfo is None:
                            current_time = current_time.replace(tzinfo=tz.utc)
                        # If current_time is timezone-aware but latest_dt is not
                        elif latest_dt.tzinfo is None and current_time.tzinfo is not None:
                            latest_dt = latest_dt.replace(tzinfo=tz.utc)
                        
                        if (current_time - latest_dt).total_seconds() > 86400:  # 24 hours in seconds
                            logger.info(f"Daily data for {symbol} outdated, fetching updates")
                            need_update = True
                            # Start fetching from the last entry
                            start_date = latest_dt.strftime('%Y-%m-%d')
                            end_date = datetime.now().strftime('%Y-%m-%d')
                    except Exception as e:
                        logger.error(f"Error processing timestamp for {symbol}: {e}")
                        # If we can't process the timestamp, assume we need an update
                        need_update = True
                        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                        end_date = datetime.now().strftime('%Y-%m-%d')
            
                if need_update:
                    # Use the backfill manager to fetch data
                    await self.backfill_manager.fetch_daily_data(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date
                    )
                    logger.info(f"Updated daily data for {symbol}")
                
            except Exception as e:
                logger.error(f"Error updating daily data for {symbol}: {e}", exc_info=True)
    
    async def _schedule_data_checks(self):
        """Schedule periodic data checks and updates"""
        try:
            # Initial data check at startup
            await self.check_and_update_intraday_data()
            await self.check_and_update_daily_data()
            
            while self._running:
                # Check intraday data every 5 minutes
                await asyncio.sleep(300)
                if self._running:
                    await self.check_and_update_intraday_data()
                
                # Check daily data once per day (after market close)
                now = datetime.now()
                if now.hour == 16 and now.minute < 5:  # Around 4:00 PM
                    await self.check_and_update_daily_data()
                    
        except asyncio.CancelledError:
            logger.info("Data check scheduler cancelled")
            raise
    
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