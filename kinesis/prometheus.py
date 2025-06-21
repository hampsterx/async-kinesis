"""
Prometheus metrics collector for async-kinesis.

This module is optional and only imported if prometheus_client is installed.
"""

from typing import Dict, Optional

try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    Counter = Gauge = Histogram = CollectorRegistry = None

from .metrics import MetricsCollector, MetricType, Timer


class PrometheusMetricsCollector(MetricsCollector):
    """Prometheus metrics collector implementation."""

    def __init__(self, namespace: str = "async_kinesis", registry: Optional["CollectorRegistry"] = None):
        """
        Initialize Prometheus metrics collector.

        Args:
            namespace: Prometheus metric namespace
            registry: Optional prometheus CollectorRegistry. If not provided,
                     uses the default global registry.
        """
        if not PROMETHEUS_AVAILABLE:
            raise ImportError(
                "prometheus_client is not installed. " "Install with: pip install async-kinesis[prometheus]"
            )

        self.namespace = namespace
        self.registry = registry
        self._metrics = {}

        # Initialize all metrics
        self._init_metrics()

    def _init_metrics(self):
        """Initialize all Prometheus metrics."""
        # Producer metrics
        self._metrics[MetricType.PRODUCER_RECORDS_SENT] = Counter(
            name="producer_records_sent_total",
            documentation="Total number of records sent to Kinesis",
            namespace=self.namespace,
            labelnames=["stream_name"],
            registry=self.registry,
        )

        self._metrics[MetricType.PRODUCER_BYTES_SENT] = Counter(
            name="producer_bytes_sent_total",
            documentation="Total bytes sent to Kinesis",
            namespace=self.namespace,
            labelnames=["stream_name"],
            registry=self.registry,
        )

        self._metrics[MetricType.PRODUCER_ERRORS] = Counter(
            name="producer_errors_total",
            documentation="Total number of producer errors",
            namespace=self.namespace,
            labelnames=["stream_name", "error_type"],
            registry=self.registry,
        )

        self._metrics[MetricType.PRODUCER_THROTTLES] = Counter(
            name="producer_throttles_total",
            documentation="Total number of throttling errors",
            namespace=self.namespace,
            labelnames=["stream_name"],
            registry=self.registry,
        )

        self._metrics[MetricType.PRODUCER_BATCH_SIZE] = Histogram(
            name="producer_batch_size",
            documentation="Size of batches sent to Kinesis",
            namespace=self.namespace,
            labelnames=["stream_name"],
            buckets=(1, 5, 10, 25, 50, 100, 250, 500),
            registry=self.registry,
        )

        self._metrics[MetricType.PRODUCER_QUEUE_SIZE] = Gauge(
            name="producer_queue_size",
            documentation="Current size of the producer queue",
            namespace=self.namespace,
            labelnames=["stream_name"],
            registry=self.registry,
        )

        self._metrics[MetricType.PRODUCER_FLUSH_DURATION] = Histogram(
            name="producer_flush_duration_seconds",
            documentation="Time taken to flush records to Kinesis",
            namespace=self.namespace,
            labelnames=["stream_name"],
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
            registry=self.registry,
        )

        # Consumer metrics
        self._metrics[MetricType.CONSUMER_RECORDS_RECEIVED] = Counter(
            name="consumer_records_received_total",
            documentation="Total number of records received from Kinesis",
            namespace=self.namespace,
            labelnames=["stream_name", "shard_id"],
            registry=self.registry,
        )

        self._metrics[MetricType.CONSUMER_BYTES_RECEIVED] = Counter(
            name="consumer_bytes_received_total",
            documentation="Total bytes received from Kinesis",
            namespace=self.namespace,
            labelnames=["stream_name", "shard_id"],
            registry=self.registry,
        )

        self._metrics[MetricType.CONSUMER_ERRORS] = Counter(
            name="consumer_errors_total",
            documentation="Total number of consumer errors",
            namespace=self.namespace,
            labelnames=["stream_name", "shard_id", "error_type"],
            registry=self.registry,
        )

        self._metrics[MetricType.CONSUMER_CHECKPOINT_SUCCESS] = Counter(
            name="consumer_checkpoint_success_total",
            documentation="Total number of successful checkpoints",
            namespace=self.namespace,
            labelnames=["stream_name", "shard_id"],
            registry=self.registry,
        )

        self._metrics[MetricType.CONSUMER_CHECKPOINT_FAILURE] = Counter(
            name="consumer_checkpoint_failure_total",
            documentation="Total number of failed checkpoints",
            namespace=self.namespace,
            labelnames=["stream_name", "shard_id"],
            registry=self.registry,
        )

        self._metrics[MetricType.CONSUMER_LAG] = Gauge(
            name="consumer_lag_records",
            documentation="Consumer lag in number of records",
            namespace=self.namespace,
            labelnames=["stream_name", "shard_id"],
            registry=self.registry,
        )

        self._metrics[MetricType.CONSUMER_PROCESSING_TIME] = Histogram(
            name="consumer_processing_time_seconds",
            documentation="Time taken to process records",
            namespace=self.namespace,
            labelnames=["stream_name", "shard_id"],
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
            registry=self.registry,
        )

        self._metrics[MetricType.CONSUMER_ITERATOR_AGE] = Gauge(
            name="consumer_iterator_age_milliseconds",
            documentation="Age of the iterator in milliseconds",
            namespace=self.namespace,
            labelnames=["stream_name", "shard_id"],
            registry=self.registry,
        )

        # Stream metrics
        self._metrics[MetricType.STREAM_SHARDS_ACTIVE] = Gauge(
            name="stream_shards_active",
            documentation="Number of active shards",
            namespace=self.namespace,
            labelnames=["stream_name"],
            registry=self.registry,
        )

        self._metrics[MetricType.STREAM_SHARDS_CLOSED] = Gauge(
            name="stream_shards_closed",
            documentation="Number of closed shards",
            namespace=self.namespace,
            labelnames=["stream_name"],
            registry=self.registry,
        )

        self._metrics[MetricType.STREAM_RESHARDING_EVENTS] = Counter(
            name="stream_resharding_events_total",
            documentation="Total number of resharding events detected",
            namespace=self.namespace,
            labelnames=["stream_name"],
            registry=self.registry,
        )

    def increment(self, metric: MetricType, value: float = 1, labels: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter metric."""
        if metric not in self._metrics:
            return

        prometheus_metric = self._metrics[metric]
        if labels:
            prometheus_metric.labels(**labels).inc(value)
        else:
            prometheus_metric.inc(value)

    def gauge(self, metric: MetricType, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge metric."""
        if metric not in self._metrics:
            return

        prometheus_metric = self._metrics[metric]
        if labels:
            prometheus_metric.labels(**labels).set(value)
        else:
            prometheus_metric.set(value)

    def histogram(self, metric: MetricType, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Record a histogram metric."""
        if metric not in self._metrics:
            return

        prometheus_metric = self._metrics[metric]
        if labels:
            prometheus_metric.labels(**labels).observe(value)
        else:
            prometheus_metric.observe(value)

    def timer(self, metric: MetricType, labels: Optional[Dict[str, str]] = None):
        """Context manager for timing operations."""
        return Timer(self.histogram, metric, labels)
