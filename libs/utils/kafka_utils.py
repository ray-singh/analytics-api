"""Utilities for working with Kafka."""

import json
import asyncio
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union, cast
import aiokafka
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError

from libs.config.settings import settings
from libs.utils.logging import get_logger

logger = get_logger(__name__)
MessageHandler = Callable[[aiokafka.ConsumerRecord], None]
AsyncMessageHandler = Callable[[aiokafka.ConsumerRecord], asyncio.coroutine]

# JSON serialization/deserialization for Kafka messages
def json_serializer(obj: Any) -> bytes:
    """Serialize object to JSON bytes.
    
    Args:
        obj: Object to serialize
        
    Returns:
        JSON bytes
    """
    return json.dumps(obj).encode("utf-8")


def json_deserializer(data: bytes) -> Any:
    """Deserialize JSON bytes to object.
    
    Args:
        data: JSON bytes
        
    Returns:
        Deserialized object
    """
    if data is None:
        return None
    return json.loads(data.decode("utf-8"))


async def ensure_topics(
    bootstrap_servers: Optional[str] = None,
    topics: Optional[Dict[str, Dict[str, Any]]] = None
) -> None:
    """Ensure Kafka topics exist with the specified configuration.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers
        topics: Dictionary of topics with their configuration
    """
    bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
    topics = topics or settings.KAFKA_TOPICS
    
    admin_client = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    
    try:
        await admin_client.start()
        
        # Get existing topics
        existing_topics = await admin_client.list_topics()
        
        # Create new topics if they don't exist
        new_topics = []
        for topic_name, config in topics.items():
            if topic_name not in existing_topics:
                logger.info(f"Creating topic: {topic_name}")
                new_topic = NewTopic(
                    name=topic_name,
                    num_partitions=config.get("partitions", 1),
                    replication_factor=settings.KAFKA_REPLICATION_FACTOR,
                    topic_configs={
                        "retention.ms": str(config.get("retention_ms", 604800000)),  # Default 7 days
                        "cleanup.policy": config.get("cleanup_policy", "delete")
                    }
                )
                new_topics.append(new_topic)
        
        if new_topics:
            await admin_client.create_topics(new_topics)
            logger.info(f"Created {len(new_topics)} topics")
    
    except KafkaError as e:
        logger.error(f"Error ensuring Kafka topics: {str(e)}")
        raise
    finally:
        await admin_client.close()


class KafkaConsumerManager:
    """Manager for Kafka consumers with error handling and graceful shutdown."""
    
    def __init__(
        self,
        topics: List[str],
        group_id: str,
        bootstrap_servers: Optional[str] = None,
        auto_offset_reset: str = "latest",
        enable_auto_commit: bool = True,
        max_poll_interval_ms: int = 300000,  # 5 minutes
        auto_commit_interval_ms: int = 5000,  # 5 seconds
    ) -> None:
        """Initialize the Kafka consumer manager.
        
        Args:
            topics: List of topics to consume
            group_id: Consumer group ID
            bootstrap_servers: Kafka bootstrap servers
            auto_offset_reset: Auto offset reset strategy
            enable_auto_commit: Enable auto commit
            max_poll_interval_ms: Maximum time between polls
            auto_commit_interval_ms: Auto commit interval
        """
        self.topics = topics
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.max_poll_interval_ms = max_poll_interval_ms
        self.auto_commit_interval_ms = auto_commit_interval_ms
        self.consumer: Optional[aiokafka.AIOKafkaConsumer] = None
        self.running = False
        self.logger = get_logger(__name__, {"component": "KafkaConsumerManager"})
    
    async def start(self) -> aiokafka.AIOKafkaConsumer:
        """Start the Kafka consumer.
        
        Returns:
            The Kafka consumer instance
        """
        self.consumer = aiokafka.AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=self.enable_auto_commit,
            value_deserializer=json_deserializer,
            max_poll_interval_ms=self.max_poll_interval_ms,
            auto_commit_interval_ms=self.auto_commit_interval_ms,
        )
        
        await self.consumer.start()
        self.running = True
        self.logger.info(f"Started Kafka consumer for topics: {', '.join(self.topics)}")
        return self.consumer
    
    async def stop(self) -> None:
        """Stop the Kafka consumer."""
        self.running = False
        if self.consumer:
            await self.consumer.stop()
            self.logger.info("Stopped Kafka consumer")
    
    async def consume(self, handler: Union[MessageHandler, AsyncMessageHandler]) -> None:
        """Consume messages and process them with the handler.
        
        Args:
            handler: Function to handle messages
        """
        if not self.consumer:
            await self.start()
        
        assert self.consumer is not None
        
        self.logger.info("Starting message consumption loop")
        try:
            async for message in self.consumer:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(message)
                    else:
                        handler(message)
                except Exception as e:
                    self.logger.error(f"Error processing message: {str(e)}", exc_info=True)
                
                # Break the loop if we're no longer running
                if not self.running:
                    break
        except Exception as e:
            self.logger.error(f"Error consuming messages: {str(e)}", exc_info=True)
            raise
        finally:
            await self.stop()


class KafkaProducerManager:
    """Manager for Kafka producers with error handling and graceful shutdown."""
    
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        acks: str = "all",
        compression_type: str = "gzip",
        max_batch_size: int = 16384,
        linger_ms: int = 5,
    ) -> None:
        """Initialize the Kafka producer manager.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            acks: Producer acknowledgment strategy
            compression_type: Compression type
            max_batch_size: Maximum batch size
            linger_ms: Linger time
        """
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.acks = acks
        self.compression_type = compression_type
        self.max_batch_size = max_batch_size
        self.linger_ms = linger_ms
        self.producer: Optional[aiokafka.AIOKafkaProducer] = None
        self.logger = get_logger(__name__, {"component": "KafkaProducerManager"})
    
    async def start(self) -> aiokafka.AIOKafkaProducer:
        """Start the Kafka producer.
        
        Returns:
            The Kafka producer instance
        """
        self.producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=json_serializer,
            acks=self.acks,
            compression_type=self.compression_type,
            max_batch_size=self.max_batch_size,
            linger_ms=self.linger_ms,
        )
        
        await self.producer.start()
        self.logger.info("Started Kafka producer")
        return self.producer
    
    async def stop(self) -> None:
        """Stop the Kafka producer."""
        if self.producer:
            await self.producer.stop()
            self.logger.info("Stopped Kafka producer")
    
    async def send(
        self, 
        topic: str, 
        value: Any, 
        key: Optional[bytes] = None, 
        partition: Optional[int] = None,
        headers: Optional[List[tuple]] = None
    ) -> None:
        """Send a message to Kafka.
        
        Args:
            topic: Topic to send to
            value: Value to send
            key: Message key
            partition: Specific partition
            headers: Message headers
        """
        if not self.producer:
            await self.start()
        
        assert self.producer is not None
        
        try:
            await self.producer.send_and_wait(
                topic=topic,
                value=value,
                key=key,
                partition=partition,
                headers=headers
            )
        except KafkaError as e:
            self.logger.error(f"Error sending message to {topic}: {str(e)}")
            raise