"""Utilities for monitoring and metrics collection."""

import time
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, TypeVar, cast

# Type variable for generic function
F = TypeVar('F', bound=Callable[..., Any])

# In-memory metrics store for simple metrics collection
# TO DO: replace with Prometheus
_metrics_store: Dict[str, Dict[str, float]] = {
    "counters": {},
    "gauges": {},
    "histograms": {}
}


def increment_counter(name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
    """Increment a counter metric.
    
    Args:
        name: Name of the counter
        value: Value to increment by (default: 1.0)
        labels: Additional labels for the metric
    """
    key = name
    if labels:
        key = f"{name}:{','.join(f'{k}={v}' for k, v in sorted(labels.items()))}"
    
    if key not in _metrics_store["counters"]:
        _metrics_store["counters"][key] = 0.0
    
    _metrics_store["counters"][key] += value


def set_gauge(name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
    """Set a gauge metric.
    
    Args:
        name: Name of the gauge
        value: Value to set
        labels: Additional labels for the metric
    """
    key = name
    if labels:
        key = f"{name}:{','.join(f'{k}={v}' for k, v in sorted(labels.items()))}"
    
    _metrics_store["gauges"][key] = value


def observe_histogram(name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
    """Observe a value for a histogram metric.
    
    Args:
        name: Name of the histogram
        value: Value to observe
        labels: Additional labels for the metric
    """
    key = name
    if labels:
        key = f"{name}:{','.join(f'{k}={v}' for k, v in sorted(labels.items()))}"
    
    if key not in _metrics_store["histograms"]:
        _metrics_store["histograms"][key] = []
    
    _metrics_store["histograms"][key].append(value)


def timer(name: str, labels: Optional[Dict[str, str]] = None) -> Callable[[F], F]:
    """Decorator to time a function and record the duration as a histogram.
    
    Args:
        name: Name of the timer metric
        labels: Additional labels for the metric
        
    Returns:
        Decorated function that records timing
    """
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start_time = time.time()
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            observe_histogram(name, duration, labels)
            return result
        return cast(F, wrapper)
    return decorator


def get_metrics() -> Dict[str, Dict[str, Any]]:
    """Get all collected metrics.
    
    Returns:
        Dictionary of metrics by type
    """
    result = {
        "counters": _metrics_store["counters"].copy(),
        "gauges": _metrics_store["gauges"].copy(),
        "histograms": {}
    }
    
    # Calculate histogram statistics
    for name, values in _metrics_store["histograms"].items():
        if not values:
            continue
        
        # Sort values for percentiles
        sorted_values = sorted(values)
        count = len(sorted_values)
        
        result["histograms"][name] = {
            "count": count,
            "sum": sum(sorted_values),
            "min": min(sorted_values),
            "max": max(sorted_values),
            "mean": sum(sorted_values) / count if count > 0 else 0,
            "p50": sorted_values[int(count * 0.5)] if count > 0 else 0,
            "p90": sorted_values[int(count * 0.9)] if count > 0 else 0,
            "p95": sorted_values[int(count * 0.95)] if count > 0 else 0,
            "p99": sorted_values[int(count * 0.99)] if count > 0 else 0,
        }
    
    return result


def reset_metrics() -> None:
    """Reset all metrics to initial state."""
    _metrics_store["counters"] = {}
    _metrics_store["gauges"] = {}
    _metrics_store["histograms"] = {}