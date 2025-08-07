import asyncio
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from collections import defaultdict, deque
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from src.schemas import StockPriceEvent, AnalyticsEvent, DataQualityEvent, EventType
from src.analytics_engine import TechnicalIndicators
from src.db import insert_price, insert_analytics

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self):
        self.kafka_bootstrap_servers = "localhost:9092"
        self.price_topic = "stock_prices"
        self.analytics_topic = "analytics"
        self.dlq_topic = "dead_letter_queue"
        self.data_quality_topic = "data_quality_issues"
        
        self.analytics_engine = TechnicalIndicators()
        self.data_quality_metrics = defaultdict(lambda: {
            'total_records': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'last_processed': None,
            'errors': deque(maxlen=100)
        })
        
        # Data quality thresholds
        self.price_change_threshold = 0.5  # 50% price change is suspicious
        self.volume_threshold = 1000000  # Unusually high volume
        self.frequency_threshold = 0.1  # 100ms between updates is too fast
        
    async def start_pipeline(self):
        """Start the ETL pipeline"""
        logger.info("Starting ETL Pipeline...")
        
        # Start consumers and producers
        self.price_consumer = AIOKafkaConsumer(
            self.price_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            group_id="etl-pipeline-group",
            auto_offset_reset="earliest"
        )
        
        self.analytics_producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers
        )
        
        self.dlq_producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers
        )
        
        self.dq_producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers
        )
        
        await self.price_consumer.start()
        await self.analytics_producer.start()
        await self.dlq_producer.start()
        await self.dq_producer.start()
        
        try:
            async for message in self.price_consumer:
                await self.process_price_event(message)
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
        finally:
            await self.shutdown()
    
    async def process_price_event(self, message):
        """Process a price event through the ETL pipeline"""
        try:
            # Extract and validate event
            event_data = json.loads(message.value.decode())
            price_event = StockPriceEvent(**event_data)
            
            # Update metrics
            symbol = price_event.symbol
            self.data_quality_metrics[symbol]['total_records'] += 1
            self.data_quality_metrics[symbol]['last_processed'] = datetime.now()
            
            # Data quality checks
            quality_issues = await self.perform_data_quality_checks(price_event)
            
            if quality_issues:
                # Send to dead letter queue if critical issues
                if any(issue['severity'] == 'CRITICAL' for issue in quality_issues):
                    await self.send_to_dlq(price_event, quality_issues)
                    self.data_quality_metrics[symbol]['invalid_records'] += 1
                    return
                
                # Send data quality events
                for issue in quality_issues:
                    await self.send_data_quality_event(issue)
            
            # Transform data
            transformed_data = await self.transform_price_data(price_event)
            
            # Load data
            await self.load_price_data(price_event)
            
            # Generate analytics
            analytics_events = await self.generate_analytics(price_event)
            
            # Send analytics events
            for analytics_event in analytics_events:
                await self.send_analytics_event(analytics_event)
                await self.load_analytics_data(analytics_event)
            
            self.data_quality_metrics[symbol]['valid_records'] += 1
            
        except Exception as e:
            logger.error(f"Error processing price event: {e}")
            await self.send_to_dlq(message.value, [{'error': str(e), 'severity': 'CRITICAL'}])
    
    async def perform_data_quality_checks(self, price_event: StockPriceEvent) -> List[Dict]:
        """Perform comprehensive data quality checks"""
        issues = []
        symbol = price_event.symbol
        
        # Check for extreme price changes
        prices = self.analytics_engine.get_prices(symbol, 2)
        if len(prices) >= 2:
            price_change = abs(price_event.price - prices[-1]) / prices[-1]
            if price_change > self.price_change_threshold:
                issues.append({
                    'symbol': symbol,
                    'issue_type': 'EXTREME_PRICE_CHANGE',
                    'description': f'Price change of {price_change:.2%} exceeds threshold',
                    'severity': 'HIGH',
                    'value': price_change,
                    'threshold': self.price_change_threshold
                })
        
        # Check for unusual volume
        if price_event.volume and price_event.volume > self.volume_threshold:
            issues.append({
                'symbol': symbol,
                'issue_type': 'HIGH_VOLUME',
                'description': f'Volume {price_event.volume} exceeds threshold',
                'severity': 'MEDIUM',
                'value': price_event.volume,
                'threshold': self.volume_threshold
            })
        
        # Check for data freshness
        if price_event.timestamp:
            age = datetime.now() - price_event.timestamp
            if age > timedelta(minutes=5):
                issues.append({
                    'symbol': symbol,
                    'issue_type': 'STALE_DATA',
                    'description': f'Data is {age.total_seconds():.0f} seconds old',
                    'severity': 'MEDIUM',
                    'value': age.total_seconds(),
                    'threshold': 300
                })
        
        # Check for duplicate data
        if len(prices) > 0 and abs(price_event.price - prices[-1]) < 0.01:
            issues.append({
                'symbol': symbol,
                'issue_type': 'DUPLICATE_PRICE',
                'description': 'Price unchanged from previous value',
                'severity': 'LOW',
                'value': price_event.price,
                'previous_value': prices[-1]
            })
        
        return issues
    
    async def transform_price_data(self, price_event: StockPriceEvent) -> Dict:
        """Transform price data for analytics"""
        return {
            'symbol': price_event.symbol,
            'price': price_event.price,
            'volume': price_event.volume,
            'timestamp': price_event.timestamp,
            'source': price_event.source,
            'processed_at': datetime.now()
        }
    
    async def load_price_data(self, price_event: StockPriceEvent):
        """Load price data into database"""
        try:
            insert_price(
                price_event.symbol,
                price_event.price,
                price_event.timestamp.timestamp()
            )
            logger.debug(f"Loaded price data for {price_event.symbol}")
        except Exception as e:
            logger.error(f"Error loading price data: {e}")
            raise
    
    async def generate_analytics(self, price_event: StockPriceEvent) -> List[AnalyticsEvent]:
        """Generate analytics events from price data"""
        analytics_events = []
        symbol = price_event.symbol
        
        # Add price to analytics engine
        self.analytics_engine.add_price(
            symbol, 
            price_event.price, 
            price_event.volume, 
            price_event.timestamp
        )
        
        # Get all indicators
        indicators = self.analytics_engine.get_all_indicators(symbol)
        
        # Create analytics events for each indicator
        for indicator_name, value in indicators.items():
            if value is not None:
                if isinstance(value, dict):
                    # Handle complex indicators like MACD
                    for sub_name, sub_value in value.items():
                        if sub_value is not None:
                            analytics_event = AnalyticsEvent(
                                symbol=symbol,
                                analytics_type=f"{indicator_name}_{sub_name}",
                                value=sub_value,
                                timestamp=price_event.timestamp,
                                metadata={'indicator': indicator_name, 'sub_indicator': sub_name}
                            )
                            analytics_events.append(analytics_event)
                else:
                    # Handle simple indicators
                    analytics_event = AnalyticsEvent(
                        symbol=symbol,
                        analytics_type=indicator_name,
                        value=value,
                        timestamp=price_event.timestamp,
                        metadata={'indicator': indicator_name}
                    )
                    analytics_events.append(analytics_event)
        
        return analytics_events
    
    async def load_analytics_data(self, analytics_event: AnalyticsEvent):
        """Load analytics data into database"""
        try:
            insert_analytics(
                analytics_event.symbol,
                analytics_event.analytics_type,
                analytics_event.value,
                analytics_event.timestamp.timestamp(),
                metadata=analytics_event.metadata
            )
            logger.debug(f"Loaded analytics data for {analytics_event.symbol}")
        except Exception as e:
            logger.error(f"Error loading analytics data: {e}")
            raise
    
    async def send_analytics_event(self, analytics_event: AnalyticsEvent):
        """Send analytics event to Kafka"""
        try:
            event_json = analytics_event.json()
            await self.analytics_producer.send_and_wait(
                self.analytics_topic,
                event_json.encode()
            )
            logger.debug(f"Sent analytics event: {analytics_event.analytics_type}")
        except Exception as e:
            logger.error(f"Error sending analytics event: {e}")
            raise
    
    async def send_data_quality_event(self, issue: Dict):
        """Send data quality event to Kafka"""
        try:
            dq_event = DataQualityEvent(
                symbol=issue.get('symbol'),
                issue_type=issue['issue_type'],
                description=issue['description'],
                severity=issue['severity'],
                timestamp=datetime.now(),
                metadata=issue
            )
            
            event_json = dq_event.json()
            await self.dq_producer.send_and_wait(
                self.data_quality_topic,
                event_json.encode()
            )
            logger.info(f"Sent data quality event: {issue['issue_type']}")
        except Exception as e:
            logger.error(f"Error sending data quality event: {e}")
    
    async def send_to_dlq(self, data: Any, issues: List[Dict]):
        """Send failed data to dead letter queue"""
        try:
            dlq_message = {
                'original_data': data,
                'issues': issues,
                'timestamp': datetime.now().isoformat()
            }
            
            await self.dlq_producer.send_and_wait(
                self.dlq_topic,
                json.dumps(dlq_message).encode()
            )
            logger.warning(f"Sent to DLQ: {len(issues)} issues")
        except Exception as e:
            logger.error(f"Error sending to DLQ: {e}")
    
    def get_pipeline_metrics(self) -> Dict:
        """Get pipeline performance metrics"""
        return {
            'data_quality_metrics': dict(self.data_quality_metrics),
            'total_processed': sum(m['total_records'] for m in self.data_quality_metrics.values()),
            'total_valid': sum(m['valid_records'] for m in self.data_quality_metrics.values()),
            'total_invalid': sum(m['invalid_records'] for m in self.data_quality_metrics.values()),
            'pipeline_status': 'running'
        }
    
    async def shutdown(self):
        """Shutdown the pipeline gracefully"""
        logger.info("Shutting down ETL Pipeline...")
        await self.price_consumer.stop()
        await self.analytics_producer.stop()
        await self.dlq_producer.stop()
        await self.dq_producer.stop()

if __name__ == "__main__":
    pipeline = ETLPipeline()
    asyncio.run(pipeline.start_pipeline()) 