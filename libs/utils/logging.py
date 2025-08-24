"""Centralized logging configuration for all microservices."""
import json
import logging
import os
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, Optional
from libs.config.settings import settings


class JsonFormatter(logging.Formatter):
    """Custom formatter that outputs logs as JSON objects."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a JSON object."""
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": os.environ.get("SERVICE_NAME", settings.APP_NAME),
        }
        
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info)
            }
        
        # Add extra attributes if available
        if hasattr(record, "extra"):
            log_data.update(record.extra)
        
        return json.dumps(log_data)


class TextFormatter(logging.Formatter):
    """Standard text formatter for console output."""
    
    def __init__(self) -> None:
        super().__init__(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )


def setup_logging(service_name: Optional[str] = None, log_level: Optional[str] = None) -> logging.Logger:
    """Configure logging for the application.
    
    Args:
        service_name: Name of the service (defaults to APP_NAME from settings)
        log_level: Log level (defaults to LOG_LEVEL from settings)
        
    Returns:
        Logger instance configured for the service
    """
    service_name = service_name or settings.APP_NAME
    log_level_str = log_level or settings.LOG_LEVEL
    log_level_num = getattr(logging, log_level_str.upper(), logging.INFO)
    
    # Set service name in environment for child processes
    os.environ["SERVICE_NAME"] = service_name
    
    # Create logger
    logger = logging.getLogger(service_name)
    logger.setLevel(log_level_num)
    logger.handlers = []  
    
    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level_num)
    
    # Set formatter based on settings
    if settings.LOG_FORMAT.lower() == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(TextFormatter())
    
    # Add handler to logger
    logger.addHandler(handler)
    
    return logger


class LoggerAdapter(logging.LoggerAdapter):
    """Adapter to add context to log messages."""
    
    def __init__(self, logger: logging.Logger, extra: Dict[str, Any]) -> None:
        super().__init__(logger, extra)
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """Process the log message, adding extra context."""
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        kwargs['extra'].update(self.extra)
        return msg, kwargs


def get_logger(name: str, context: Optional[Dict[str, Any]] = None) -> logging.LoggerAdapter:
    """Get a logger with optional context.
    
    Args:
        name: Name for the logger
        context: Additional context to include with log messages
        
    Returns:
        LoggerAdapter instance with the specified context
    """
    logger = logging.getLogger(name)
    return LoggerAdapter(logger, context or {})