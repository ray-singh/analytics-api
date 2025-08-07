"""
Error handling and resilience utilities for the stock analytics system.
"""

import asyncio
import logging
from typing import Callable, Any, Optional, Type
from functools import wraps
from datetime import datetime, timedelta
from enum import Enum
import time

logger = logging.getLogger(__name__)

class ErrorSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class AnalyticsError(Exception):
    """Base exception for analytics system errors"""
    def __init__(self, message: str, severity: ErrorSeverity = ErrorSeverity.MEDIUM, 
                 retryable: bool = True, context: dict = None):
        super().__init__(message)
        self.severity = severity
        self.retryable = retryable
        self.context = context or {}
        self.timestamp = datetime.now()

class DatabaseError(AnalyticsError):
    """Database-related errors"""
    pass

class KafkaError(AnalyticsError):
    """Kafka-related errors"""
    pass

class DataQualityError(AnalyticsError):
    """Data quality validation errors"""
    pass

class CircuitBreaker:
    """Circuit breaker pattern implementation"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60, 
                 expected_exception: Type[Exception] = Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection"""
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
            else:
                raise AnalyticsError("Circuit breaker is OPEN", 
                                   severity=ErrorSeverity.HIGH, retryable=False)
        
        try:
            result = func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
            return result
        except self.expected_exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                logger.error(f"Circuit breaker opened after {self.failure_count} failures")
            
            raise

def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0, 
                      max_delay: float = 60.0, exponential_base: float = 2.0,
                      exceptions: tuple = (Exception,)):
    """
    Retry decorator with exponential backoff
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential backoff calculation
        exceptions: Tuple of exceptions to catch and retry
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries: {e}")
                        raise
                    
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    logger.warning(f"Function {func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}), "
                                 f"retrying in {delay:.2f}s: {e}")
                    await asyncio.sleep(delay)
            
            raise last_exception
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries: {e}")
                        raise
                    
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    logger.warning(f"Function {func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}), "
                                 f"retrying in {delay:.2f}s: {e}")
                    time.sleep(delay)
            
            raise last_exception
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator

def handle_errors(func: Callable) -> Callable:
    """Generic error handling decorator"""
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
            raise AnalyticsError(f"Error in {func.__name__}: {str(e)}", 
                               severity=ErrorSeverity.MEDIUM, context={'function': func.__name__})
    
    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
            raise AnalyticsError(f"Error in {func.__name__}: {str(e)}", 
                               severity=ErrorSeverity.MEDIUM, context={'function': func.__name__})
    
    return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

class ErrorTracker:
    """Track and analyze errors for monitoring and alerting"""
    
    def __init__(self, max_errors: int = 1000):
        self.errors = []
        self.max_errors = max_errors
        self.error_counts = {}
        
    def record_error(self, error: Exception, context: dict = None):
        """Record an error for analysis"""
        error_info = {
            'timestamp': datetime.now(),
            'error_type': type(error).__name__,
            'message': str(error),
            'context': context or {}
        }
        
        self.errors.append(error_info)
        if len(self.errors) > self.max_errors:
            self.errors.pop(0)
        
        # Update error counts
        error_type = type(error).__name__
        self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1
        
    def get_error_summary(self) -> dict:
        """Get summary of recent errors"""
        return {
            'total_errors': len(self.errors),
            'error_counts': self.error_counts,
            'recent_errors': self.errors[-10:] if self.errors else []
        }
    
    def get_error_rate(self, window_minutes: int = 5) -> float:
        """Calculate error rate in the specified time window"""
        if not self.errors:
            return 0.0
        
        cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
        recent_errors = [e for e in self.errors if e['timestamp'] > cutoff_time]
        return len(recent_errors) / window_minutes  # errors per minute

# Global error tracker instance
error_tracker = ErrorTracker() 