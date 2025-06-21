# Metrics & Observability

async-kinesis provides optional metrics collection for monitoring producer and consumer performance. Metrics are disabled by default and have zero overhead when not enabled.

## Table of Contents
- [Quick Start](#quick-start)
- [Available Metrics](#available-metrics)
- [Metrics Collectors](#metrics-collectors)
- [Prometheus Integration](#prometheus-integration)
- [Custom Collectors](#custom-collectors)
- [Best Practices](#best-practices)

## Quick Start

### Basic Usage (In-Memory Metrics)

```python
from kinesis import Producer, InMemoryMetricsCollector

# Create a metrics collector
metrics = InMemoryMetricsCollector()

# Use it with a producer
async with Producer(
    stream_name="my-stream",
    metrics_collector=metrics
) as producer:
    await producer.put({"message": "hello"})

# Check collected metrics
print(metrics.get_metrics())
# {'producer_records_sent_total{stream_name=my-stream}': 1.0, ...}
```

### Prometheus Integration

```bash
# Install with prometheus support
pip install async-kinesis[prometheus]
```

```python
from kinesis import Producer, PrometheusMetricsCollector
from prometheus_client import start_http_server

# Start Prometheus metrics server
start_http_server(8000)  # Metrics available at http://localhost:8000

# Create Prometheus collector
metrics = PrometheusMetricsCollector(namespace="my_app")

# Use with producer/consumer
async with Producer(
    stream_name="my-stream",
    metrics_collector=metrics
) as producer:
    await producer.put({"message": "hello"})
```

### Global Metrics Configuration

```python
from kinesis import set_metrics_collector, PrometheusMetricsCollector

# Set globally for all producers/consumers
set_metrics_collector(PrometheusMetricsCollector())

# Now all instances will use this collector by default
async with Producer(stream_name="my-stream") as producer:
    # Automatically uses the global collector
    await producer.put({"message": "hello"})
```

## Available Metrics

### Producer Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|---------|
| `producer_records_sent_total` | Counter | Total records successfully sent | `stream_name` |
| `producer_bytes_sent_total` | Counter | Total bytes successfully sent | `stream_name` |
| `producer_errors_total` | Counter | Total producer errors | `stream_name`, `error_type` |
| `producer_throttles_total` | Counter | Total throttling errors | `stream_name` |
| `producer_batch_size` | Histogram | Size of batches sent | `stream_name` |
| `producer_queue_size` | Gauge | Current producer queue size | `stream_name` |
| `producer_flush_duration_seconds` | Histogram | Time to flush records | `stream_name` |

### Consumer Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|---------|
| `consumer_records_received_total` | Counter | Total records received | `stream_name`, `shard_id` |
| `consumer_bytes_received_total` | Counter | Total bytes received | `stream_name`, `shard_id` |
| `consumer_errors_total` | Counter | Total consumer errors | `stream_name`, `shard_id`, `error_type` |
| `consumer_checkpoint_success_total` | Counter | Successful checkpoints | `stream_name`, `shard_id` |
| `consumer_checkpoint_failure_total` | Counter | Failed checkpoints | `stream_name`, `shard_id` |
| `consumer_lag_records` | Gauge | Consumer lag in records | `stream_name`, `shard_id` |
| `consumer_processing_time_seconds` | Histogram | Record processing time | `stream_name`, `shard_id` |
| `consumer_iterator_age_milliseconds` | Gauge | Iterator age (lag indicator) | `stream_name`, `shard_id` |

### Stream Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|---------|
| `stream_shards_active` | Gauge | Number of active shards | `stream_name` |
| `stream_shards_closed` | Gauge | Number of closed shards | `stream_name` |
| `stream_resharding_events_total` | Counter | Resharding events detected | `stream_name` |

## Metrics Collectors

### NoOpMetricsCollector (Default)

The default collector that does nothing. Zero overhead when metrics are not needed.

```python
from kinesis import NoOpMetricsCollector

# Explicitly use no-op (this is the default)
async with Producer(
    stream_name="my-stream",
    metrics_collector=NoOpMetricsCollector()
) as producer:
    # No metrics collected
    pass
```

### InMemoryMetricsCollector

Simple collector for testing and debugging. Stores metrics in memory.

```python
from kinesis import InMemoryMetricsCollector

metrics = InMemoryMetricsCollector()

# Use with producer
async with Producer(
    stream_name="my-stream",
    metrics_collector=metrics
) as producer:
    await producer.put({"test": "data"})

# Access metrics
print(f"Records sent: {metrics.counters}")
print(f"Queue sizes: {metrics.gauges}")
print(f"Flush times: {metrics.histograms}")
```

### PrometheusMetricsCollector

Production-ready Prometheus integration.

```python
from kinesis import PrometheusMetricsCollector
from prometheus_client import CollectorRegistry, start_http_server

# Custom registry (optional)
registry = CollectorRegistry()
metrics = PrometheusMetricsCollector(
    namespace="kinesis_app",
    registry=registry
)

# Start metrics server
start_http_server(8000, registry=registry)
```

## Custom Collectors

Create your own metrics collector by implementing the `MetricsCollector` interface:

```python
from kinesis import MetricsCollector, MetricType
from typing import Dict, Optional

class DatadogMetricsCollector(MetricsCollector):
    def __init__(self, datadog_client):
        self.client = datadog_client

    def increment(
        self,
        metric: MetricType,
        value: float = 1,
        labels: Optional[Dict[str, str]] = None
    ) -> None:
        tags = [f"{k}:{v}" for k, v in (labels or {}).items()]
        self.client.increment(metric.value, value, tags=tags)

    def gauge(
        self,
        metric: MetricType,
        value: float,
        labels: Optional[Dict[str, str]] = None
    ) -> None:
        tags = [f"{k}:{v}" for k, v in (labels or {}).items()]
        self.client.gauge(metric.value, value, tags=tags)

    def histogram(
        self,
        metric: MetricType,
        value: float,
        labels: Optional[Dict[str, str]] = None
    ) -> None:
        tags = [f"{k}:{v}" for k, v in (labels or {}).items()]
        self.client.histogram(metric.value, value, tags=tags)

    def timer(self, metric: MetricType, labels: Optional[Dict[str, str]] = None):
        # Return a context manager that times the operation
        from kinesis.metrics import Timer
        return Timer(self.histogram, metric, labels)
```

## Best Practices

### 1. Production Setup

```python
import logging
from kinesis import PrometheusMetricsCollector, set_metrics_collector
from prometheus_client import start_http_server

# Configure logging
logging.basicConfig(level=logging.INFO)

# Start Prometheus endpoint
start_http_server(8000)

# Set global metrics collector
set_metrics_collector(PrometheusMetricsCollector(
    namespace="my_service"
))

# Now all producers/consumers will emit metrics
```

### 2. Monitoring Dashboards

Example Prometheus queries for Grafana:

```promql
# Producer throughput
rate(my_service_producer_records_sent_total[5m])

# Producer error rate
rate(my_service_producer_errors_total[5m])

# Consumer lag
my_service_consumer_lag_records

# Resharding events
increase(my_service_stream_resharding_events_total[1h])
```

### 3. Alerting Rules

```yaml
groups:
  - name: kinesis
    rules:
      - alert: HighProducerErrorRate
        expr: rate(my_service_producer_errors_total[5m]) > 0.1
        for: 5m
        annotations:
          summary: "High error rate in Kinesis producer"

      - alert: HighConsumerLag
        expr: my_service_consumer_lag_records > 10000
        for: 10m
        annotations:
          summary: "Consumer lag exceeds 10k records"

      - alert: ReshardingDetected
        expr: increase(my_service_stream_resharding_events_total[5m]) > 0
        annotations:
          summary: "Kinesis stream resharding detected"
```

### 4. Testing with Metrics

```python
import pytest
from kinesis import Producer, InMemoryMetricsCollector

@pytest.mark.asyncio
async def test_producer_metrics():
    metrics = InMemoryMetricsCollector()

    async with Producer(
        stream_name="test-stream",
        metrics_collector=metrics
    ) as producer:
        await producer.put({"test": "data"})
        await producer.flush()

    # Verify metrics
    assert metrics.counters.get(
        "producer_records_sent_total{stream_name=test-stream}"
    ) > 0
```

### 5. Performance Considerations

- Metrics collection adds minimal overhead (~1-2%)
- NoOpMetricsCollector has zero overhead
- Prometheus collectors are thread-safe
- Histograms use configurable buckets for efficiency

### 6. Multi-Stream Applications

```python
# Different collectors per stream
metrics_orders = PrometheusMetricsCollector(namespace="orders")
metrics_events = PrometheusMetricsCollector(namespace="events")

async with Producer(
    stream_name="orders-stream",
    metrics_collector=metrics_orders
) as orders_producer:
    # Orders metrics
    pass

async with Producer(
    stream_name="events-stream",
    metrics_collector=metrics_events
) as events_producer:
    # Events metrics
    pass
```

## Troubleshooting

### Metrics not appearing in Prometheus

1. Check the metrics endpoint is accessible:
```bash
curl http://localhost:8000/metrics
```

2. Verify collector is set:
```python
from kinesis import get_metrics_collector
print(type(get_metrics_collector()))  # Should not be NoOpMetricsCollector
```

3. Ensure records are being processed:
```python
# Force a flush to trigger metrics
await producer.flush()
```

### High memory usage with InMemoryMetricsCollector

The InMemoryMetricsCollector stores all histogram values. For production, use PrometheusMetricsCollector which uses efficient buckets.

### Custom labels not working

Ensure your labels are strings and don't contain invalid characters:
```python
# Good
labels = {"stream_name": "my-stream", "env": "prod"}

# Bad
labels = {"stream.name": "my-stream"}  # Dots may cause issues
```
