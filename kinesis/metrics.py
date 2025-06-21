"""
Metrics collection for async-kinesis.

This module provides optional metrics collection for monitoring producer and consumer
performance. Metrics are disabled by default and have zero overhead when not enabled.
"""

import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Optional, Union


class MetricType(Enum):
    """Types of metrics collected."""

    # Producer metrics
    PRODUCER_RECORDS_SENT = "producer_records_sent_total"
    PRODUCER_BYTES_SENT = "producer_bytes_sent_total"
    PRODUCER_ERRORS = "producer_errors_total"
    PRODUCER_THROTTLES = "producer_throttles_total"
    PRODUCER_BATCH_SIZE = "producer_batch_size"
    PRODUCER_QUEUE_SIZE = "producer_queue_size"
    PRODUCER_FLUSH_DURATION = "producer_flush_duration_seconds"

    # Consumer metrics
    CONSUMER_RECORDS_RECEIVED = "consumer_records_received_total"
    CONSUMER_BYTES_RECEIVED = "consumer_bytes_received_total"
    CONSUMER_ERRORS = "consumer_errors_total"
    CONSUMER_CHECKPOINT_SUCCESS = "consumer_checkpoint_success_total"
    CONSUMER_CHECKPOINT_FAILURE = "consumer_checkpoint_failure_total"
    CONSUMER_LAG = "consumer_lag_records"
    CONSUMER_PROCESSING_TIME = "consumer_processing_time_seconds"
    CONSUMER_ITERATOR_AGE = "consumer_iterator_age_milliseconds"

    # Stream metrics
    STREAM_SHARDS_ACTIVE = "stream_shards_active"
    STREAM_SHARDS_CLOSED = "stream_shards_closed"
    STREAM_RESHARDING_EVENTS = "stream_resharding_events_total"


class MetricsCollector(ABC):
    """Abstract base class for metrics collectors."""

    @abstractmethod
    def increment(self, metric: MetricType, value: float = 1, labels: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter metric."""
        pass

    @abstractmethod
    def gauge(self, metric: MetricType, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge metric."""
        pass

    @abstractmethod
    def histogram(self, metric: MetricType, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Record a histogram metric."""
        pass

    @abstractmethod
    def timer(self, metric: MetricType, labels: Optional[Dict[str, str]] = None):
        """Context manager for timing operations."""
        pass


class Timer:
    """Context manager for timing operations."""

    def __init__(self, callback, metric: MetricType, labels: Optional[Dict[str, str]] = None):
        self.callback = callback
        self.metric = metric
        self.labels = labels
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        self.callback(self.metric, duration, self.labels)


class NoOpMetricsCollector(MetricsCollector):
    """No-op metrics collector that does nothing. Used when metrics are disabled."""

    def increment(self, metric: MetricType, value: float = 1, labels: Optional[Dict[str, str]] = None) -> None:
        """No-op increment."""
        pass

    def gauge(self, metric: MetricType, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """No-op gauge."""
        pass

    def histogram(self, metric: MetricType, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """No-op histogram."""
        pass

    def timer(self, metric: MetricType, labels: Optional[Dict[str, str]] = None):
        """No-op timer."""
        return Timer(lambda *args: None, metric, labels)


class InMemoryMetricsCollector(MetricsCollector):
    """Simple in-memory metrics collector for testing and debugging."""

    def __init__(self):
        self.counters: Dict[str, float] = {}
        self.gauges: Dict[str, float] = {}
        self.histograms: Dict[str, list] = {}

    def _make_key(self, metric: MetricType, labels: Optional[Dict[str, str]] = None) -> str:
        """Create a unique key for a metric with labels."""
        key = metric.value
        if labels:
            label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            key = f"{key}{{{label_str}}}"
        return key

    def increment(self, metric: MetricType, value: float = 1, labels: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter metric."""
        key = self._make_key(metric, labels)
        self.counters[key] = self.counters.get(key, 0) + value

    def gauge(self, metric: MetricType, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge metric."""
        key = self._make_key(metric, labels)
        self.gauges[key] = value

    def histogram(self, metric: MetricType, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Record a histogram metric."""
        key = self._make_key(metric, labels)
        if key not in self.histograms:
            self.histograms[key] = []
        self.histograms[key].append(value)

    def timer(self, metric: MetricType, labels: Optional[Dict[str, str]] = None):
        """Context manager for timing operations."""
        return Timer(self.histogram, metric, labels)

    def get_metrics(self) -> Dict[str, Union[float, list]]:
        """Get all collected metrics."""
        metrics = {}
        metrics.update(self.counters)
        metrics.update(self.gauges)
        metrics.update(self.histograms)
        return metrics


# Global default collector - NoOp by default
_default_collector = NoOpMetricsCollector()


def set_metrics_collector(collector: MetricsCollector) -> None:
    """Set the global metrics collector."""
    global _default_collector
    _default_collector = collector


def get_metrics_collector() -> MetricsCollector:
    """Get the current metrics collector."""
    return _default_collector


def reset_metrics_collector() -> None:
    """Reset to the default no-op collector."""
    global _default_collector
    _default_collector = NoOpMetricsCollector()
