"""Central configuration management for all microservices."""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()
BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings:
    """Centralized settings management for all microservices.
    
    Each microservice can access these settings, and they will be
    consistently loaded from environment variables or default values.
    """
    APP_NAME: str = os.getenv("APP_NAME", "analytics-api")
    APP_VERSION: str = os.getenv("APP_VERSION", "0.1.0")
    DEBUG: bool = os.getenv("DEBUG", "False").lower() in ("true", "1", "t")
    
    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_AUTO_CREATE_TOPICS: bool = os.getenv("KAFKA_AUTO_CREATE_TOPICS", "True").lower() in ("true", "1", "t")
    KAFKA_REPLICATION_FACTOR: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
    
    # Topics with default configurations
    KAFKA_TOPICS: Dict[str, Dict[str, Any]] = {
        "market.prices.raw": {
            "partitions": 3,
            "retention_ms": 86400000,  # 24 hours
            "cleanup_policy": "delete"
        },
        "market.prices.ohlcv": {
            "partitions": 3,
            "retention_ms": 604800000,  # 7 days
            "cleanup_policy": "delete"
        },
        "market.analytics": {
            "partitions": 3, 
            "retention_ms": 604800000,  # 7 days
            "cleanup_policy": "delete"
        },
        "market.commands": {
            "partitions": 1,
            "retention_ms": 86400000,  # 24 hours
            "cleanup_policy": "delete"
        }
    }
    
    # Database settings
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "stockanalytics")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "password")
    
    @property
    def database_url(self) -> str:
        """Get the database URL for SQLAlchemy or other database clients."""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    # API settings
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    API_WORKERS: int = int(os.getenv("API_WORKERS", "1"))
    
    TWELVEDATA_API_KEY: Optional[str] = os.getenv("TWELVEDATA_API_KEY")
    ALPHA_VANTAGE_API_KEY: Optional[str] = os.getenv("ALPHA_VANTAGE_API_KEY")
    
    # Symbols to stream (comma-separated)
    STREAM_SYMBOLS: List[str] = os.getenv("STREAM_SYMBOLS", "AAPL").split(",")
    
    # Logging settings
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = os.getenv("LOG_FORMAT", "json") 
    
    def validate(self) -> None:
        """Validate the settings and raise exceptions for any problems."""
        # Validate required API keys
        if not self.TWELVEDATA_API_KEY and not self.ALPHA_VANTAGE_API_KEY:
            raise ValueError("At least one API key (TWELVEDATA_API_KEY or ALPHA_VANTAGE_API_KEY) must be provided")
        
        # Validate topics
        for topic, config in self.KAFKA_TOPICS.items():
            if "partitions" not in config:
                raise ValueError(f"Topic {topic} missing partitions configuration")

settings = Settings()