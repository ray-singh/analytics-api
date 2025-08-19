#!/usr/bin/env python3
"""
Create required Kafka topics with appropriate configurations
Usage: python -m scripts.create_topics [--delete-existing]
"""

import argparse
import os
import sys
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Default configuration
DEFAULT_KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_PARTITIONS = 3
DEFAULT_REPLICATION = 1  # Use 3 in production with multi-broker setup
DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000  # 7 days in milliseconds
PRICE_RAW_RETENTION_MS = 24 * 60 * 60 * 1000  # 1 day for raw prices (high volume)

def create_topics(bootstrap_servers, delete_existing=False):
    """Create all required Kafka topics with proper configuration"""
    # Connect to Kafka admin
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    
    # Check existing topics
    metadata = admin_client.list_topics(timeout=10)
    existing_topics = metadata.topics
    
    # Define topic configurations
    topics_to_create = [
        # Market data topics
        NewTopic(
            "market.prices.raw", 
            num_partitions=DEFAULT_PARTITIONS,
            replication_factor=DEFAULT_REPLICATION,
            config={
                'retention.ms': str(PRICE_RAW_RETENTION_MS),
                'cleanup.policy': 'delete',
                'compression.type': 'lz4'
            }
        ),
        NewTopic(
            "market.prices.ohlcv", 
            num_partitions=DEFAULT_PARTITIONS,
            replication_factor=DEFAULT_REPLICATION,
            config={
                'retention.ms': str(DEFAULT_RETENTION_MS),
                'cleanup.policy': 'compact',
                'compression.type': 'lz4'
            }
        ),
        
        # Analytics topics
        NewTopic(
            "market.analytics", 
            num_partitions=DEFAULT_PARTITIONS,
            replication_factor=DEFAULT_REPLICATION,
            config={
                'retention.ms': str(DEFAULT_RETENTION_MS),
                'cleanup.policy': 'compact',
                'compression.type': 'lz4'
            }
        ),
        
        # Command topics
        NewTopic(
            "command.backfill.request", 
            num_partitions=1,  # Single partition for ordered processing
            replication_factor=DEFAULT_REPLICATION,
            config={
                'retention.ms': str(DEFAULT_RETENTION_MS),
                'cleanup.policy': 'compact'
            }
        ),
        NewTopic(
            "command.backfill.status", 
            num_partitions=DEFAULT_PARTITIONS,
            replication_factor=DEFAULT_REPLICATION,
            config={
                'retention.ms': str(DEFAULT_RETENTION_MS),
                'cleanup.policy': 'compact'
            }
        ),
        
        # Dead-letter-queue topics
        NewTopic(
            "error.dlq", 
            num_partitions=1,
            replication_factor=DEFAULT_REPLICATION,
            config={
                'retention.ms': str(30 * 24 * 60 * 60 * 1000),  # 30 days
                'cleanup.policy': 'delete'
            }
        )
    ]
    
    # Delete existing topics if specified
    if delete_existing:
        topics_to_delete = [topic.topic for topic in topics_to_create 
                          if topic.topic in existing_topics]
        
        if topics_to_delete:
            print(f"Deleting existing topics: {', '.join(topics_to_delete)}")
            futures = admin_client.delete_topics(topics_to_delete)
            
            # Wait for deletion
            for topic, future in futures.items():
                try:
                    future.result()
                    print(f"Topic {topic} deleted successfully")
                except Exception as e:
                    print(f"Failed to delete topic {topic}: {e}")
    
    # Filter out topics that already exist
    topics_to_create_filtered = [topic for topic in topics_to_create 
                               if delete_existing or topic.topic not in existing_topics]
    
    if not topics_to_create_filtered:
        print("All topics already exist. No new topics created.")
        return
    
    # Create topics
    print(f"Creating topics: {', '.join(t.topic for t in topics_to_create_filtered)}")
    futures = admin_client.create_topics(topics_to_create_filtered)
    
    # Wait for creation
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic {topic} created successfully")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

def main():
    parser = argparse.ArgumentParser(description='Create Kafka topics for analytics-api')
    parser.add_argument('--bootstrap-servers', default=DEFAULT_KAFKA_BOOTSTRAP,
                        help=f'Kafka bootstrap servers (default: {DEFAULT_KAFKA_BOOTSTRAP})')
    parser.add_argument('--delete-existing', action='store_true',
                        help='Delete existing topics before creating new ones')
    
    args = parser.parse_args()
    create_topics(args.bootstrap_servers, args.delete_existing)

if __name__ == "__main__":
    main()