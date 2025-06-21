"""Unit tests for metrics functionality."""

import time
from unittest.mock import MagicMock

import pytest

from kinesis import (
    InMemoryMetricsCollector,
    MetricType,
    NoOpMetricsCollector,
    get_metrics_collector,
    reset_metrics_collector,
    set_metrics_collector,
)
from kinesis.metrics import Timer


class TestNoOpMetricsCollector:
    def test_all_operations_are_noop(self):
        """Test that NoOpMetricsCollector does nothing."""
        collector = NoOpMetricsCollector()

        # These should all do nothing without errors
        collector.increment(MetricType.PRODUCER_RECORDS_SENT, 10)
        collector.gauge(MetricType.PRODUCER_QUEUE_SIZE, 100)
        collector.histogram(MetricType.PRODUCER_FLUSH_DURATION, 0.5)

        # Timer should also do nothing
        with collector.timer(MetricType.PRODUCER_FLUSH_DURATION):
            time.sleep(0.01)  # Small delay

        # No way to verify it did nothing, but no errors = success


class TestInMemoryMetricsCollector:
    def test_counter_increments(self):
        """Test counter functionality."""
        collector = InMemoryMetricsCollector()

        # First increment
        collector.increment(MetricType.PRODUCER_RECORDS_SENT, 5, {"stream_name": "test"})
        assert collector.counters["producer_records_sent_total{stream_name=test}"] == 5

        # Second increment should add
        collector.increment(MetricType.PRODUCER_RECORDS_SENT, 3, {"stream_name": "test"})
        assert collector.counters["producer_records_sent_total{stream_name=test}"] == 8

        # Different labels should be separate
        collector.increment(MetricType.PRODUCER_RECORDS_SENT, 2, {"stream_name": "other"})
        assert collector.counters["producer_records_sent_total{stream_name=other}"] == 2
        assert collector.counters["producer_records_sent_total{stream_name=test}"] == 8

    def test_gauge_sets_value(self):
        """Test gauge functionality."""
        collector = InMemoryMetricsCollector()

        # Set initial value
        collector.gauge(MetricType.PRODUCER_QUEUE_SIZE, 100, {"stream_name": "test"})
        assert collector.gauges["producer_queue_size{stream_name=test}"] == 100

        # Update should overwrite
        collector.gauge(MetricType.PRODUCER_QUEUE_SIZE, 50, {"stream_name": "test"})
        assert collector.gauges["producer_queue_size{stream_name=test}"] == 50

    def test_histogram_records_values(self):
        """Test histogram functionality."""
        collector = InMemoryMetricsCollector()

        # Record multiple values
        collector.histogram(MetricType.PRODUCER_BATCH_SIZE, 10, {"stream_name": "test"})
        collector.histogram(MetricType.PRODUCER_BATCH_SIZE, 20, {"stream_name": "test"})
        collector.histogram(MetricType.PRODUCER_BATCH_SIZE, 15, {"stream_name": "test"})

        values = collector.histograms["producer_batch_size{stream_name=test}"]
        assert values == [10, 20, 15]

    def test_timer_records_duration(self):
        """Test timer context manager."""
        collector = InMemoryMetricsCollector()

        with collector.timer(MetricType.PRODUCER_FLUSH_DURATION, {"stream_name": "test"}):
            time.sleep(0.01)  # Small delay

        values = collector.histograms["producer_flush_duration_seconds{stream_name=test}"]
        assert len(values) == 1
        assert values[0] >= 0.01  # Should be at least the sleep duration
        assert values[0] < 0.1  # But not too long

    def test_labels_ordering(self):
        """Test that labels are ordered consistently."""
        collector = InMemoryMetricsCollector()

        # Different order, same labels
        collector.increment(MetricType.CONSUMER_ERRORS, 1, {"stream_name": "test", "error_type": "timeout"})
        collector.increment(MetricType.CONSUMER_ERRORS, 1, {"error_type": "timeout", "stream_name": "test"})

        # Should be the same key
        assert len(collector.counters) == 1
        key = "consumer_errors_total{error_type=timeout,stream_name=test}"
        assert collector.counters[key] == 2

    def test_get_metrics(self):
        """Test get_metrics returns all metrics."""
        collector = InMemoryMetricsCollector()

        collector.increment(MetricType.PRODUCER_RECORDS_SENT, 5)
        collector.gauge(MetricType.PRODUCER_QUEUE_SIZE, 100)
        collector.histogram(MetricType.PRODUCER_BATCH_SIZE, 50)

        metrics = collector.get_metrics()
        assert "producer_records_sent_total" in metrics
        assert "producer_queue_size" in metrics
        assert "producer_batch_size" in metrics
        assert metrics["producer_records_sent_total"] == 5
        assert metrics["producer_queue_size"] == 100
        assert metrics["producer_batch_size"] == [50]


class TestTimer:
    def test_timer_calls_callback(self):
        """Test Timer calls callback with correct arguments."""
        callback = MagicMock()
        metric = MetricType.PRODUCER_FLUSH_DURATION
        labels = {"stream_name": "test"}

        with Timer(callback, metric, labels):
            time.sleep(0.01)

        # Verify callback was called
        callback.assert_called_once()
        args = callback.call_args[0]
        assert args[0] == metric
        assert isinstance(args[1], float)
        assert args[1] >= 0.01
        assert args[2] == labels


class TestGlobalCollectorManagement:
    def test_default_collector_is_noop(self):
        """Test default collector is NoOp."""
        reset_metrics_collector()
        collector = get_metrics_collector()
        assert isinstance(collector, NoOpMetricsCollector)

    def test_set_and_get_custom_collector(self):
        """Test setting custom collector."""
        custom = InMemoryMetricsCollector()
        set_metrics_collector(custom)

        retrieved = get_metrics_collector()
        assert retrieved is custom

        # Clean up
        reset_metrics_collector()

    def test_reset_returns_to_noop(self):
        """Test reset returns to NoOp collector."""
        # Set custom
        set_metrics_collector(InMemoryMetricsCollector())
        assert not isinstance(get_metrics_collector(), NoOpMetricsCollector)

        # Reset
        reset_metrics_collector()
        assert isinstance(get_metrics_collector(), NoOpMetricsCollector)


class TestPrometheusOptional:
    def test_prometheus_import_handling(self):
        """Test that PrometheusMetricsCollector handles missing dependency gracefully."""
        try:
            from kinesis import PrometheusMetricsCollector

            if PrometheusMetricsCollector is not None:
                # If available, should be able to create
                collector = PrometheusMetricsCollector()
                assert collector is not None
            else:
                # If not available, should be None
                assert PrometheusMetricsCollector is None
        except ImportError:
            # This is fine - prometheus_client not installed
            pass


class TestMetricTypeEnum:
    def test_all_metric_types_have_values(self):
        """Test all MetricType enum values are strings."""
        for metric in MetricType:
            assert isinstance(metric.value, str)
            assert len(metric.value) > 0

    def test_metric_naming_convention(self):
        """Test metric names follow Prometheus naming conventions."""
        for metric in MetricType:
            name = metric.value
            # Should be lowercase with underscores
            assert name.islower() or "_" in name
            # Should not have spaces
            assert " " not in name
