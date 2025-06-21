# Common Patterns & Use Cases

This guide covers common patterns and real-world use cases for async-kinesis.

## Table of Contents
- [Log Aggregation](#log-aggregation)
- [Event Streaming](#event-streaming)
- [IoT Data Collection](#iot-data-collection)
- [Error Handling](#error-handling)
- [Multi-Consumer Processing](#multi-consumer-processing)
- [Graceful Shutdown](#graceful-shutdown)
- [Production Monitoring](#production-monitoring)

## Log Aggregation

Aggregate logs from multiple services and process them centrally:

```python
import asyncio
import json
import logging
from datetime import datetime
from kinesis import Producer, Consumer, JsonLineProcessor

# Set up logging
logger = logging.getLogger(__name__)

# Producer: Collect logs from multiple sources
async def log_producer():
    async with Producer(
        stream_name="application-logs",
        processor=JsonLineProcessor(),  # Efficient for line-delimited logs
        buffer_time=1.0,  # Buffer for 1 second to batch logs
        batch_size=500
    ) as producer:

        # Simulate logs from different services
        services = ["api", "worker", "database"]
        levels = ["INFO", "WARN", "ERROR"]

        for i in range(100):
            service = services[i % len(services)]
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "service": service,
                "level": levels[i % len(levels)],
                "message": f"Log message {i} from {service}",
                "request_id": f"req-{i//10}"  # Group related logs
            }

            # Use service name as partition key for grouping
            await producer.put(log_entry, partition_key=service)

# Consumer: Process and route logs
async def log_processor():
    error_count = {}

    async with Consumer(
        stream_name="application-logs",
        processor=JsonLineProcessor()
    ) as consumer:

        async for log_entry in consumer:
            service = log_entry["service"]
            level = log_entry["level"]

            # Track error rates
            if level == "ERROR":
                error_count[service] = error_count.get(service, 0) + 1

                # Alert on high error rates
                if error_count[service] > 10:
                    logger.warning(f"High error rate for {service}: {error_count[service]} errors")

            # Route logs based on level
            if level in ["ERROR", "WARN"]:
                # Send to alerting system
                logger.error(f"Alert: {service} - {level} - {log_entry['message']}")

            # Could also write to S3, ElasticSearch, etc.
```

## Event Streaming

Build event-driven architectures with multiple event types:

```python
import logging
import uuid
from enum import Enum
from typing import Dict, Any
from datetime import datetime
from kinesis import Producer, Consumer, MsgpackProcessor

# Set up logging
logger = logging.getLogger(__name__)

class EventType(Enum):
    USER_CREATED = "user.created"
    ORDER_PLACED = "order.placed"
    PAYMENT_PROCESSED = "payment.processed"
    SHIPMENT_SENT = "shipment.sent"

# Producer: Emit business events
async def emit_event(producer: Producer, event_type: EventType, data: Dict[str, Any]):
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type.value,
        "timestamp": datetime.now().isoformat(),
        "data": data
    }

    # Use event type as partition key for ordering
    await producer.put(event, partition_key=event_type.value)

async def business_logic():
    async with Producer(
        stream_name="business-events",
        processor=MsgpackProcessor()  # Efficient binary format
    ) as producer:

        # User registration flow
        user_id = "user-123"
        await emit_event(producer, EventType.USER_CREATED, {
            "user_id": user_id,
            "email": "user@example.com"
        })

        # Order flow
        order_id = "order-456"
        await emit_event(producer, EventType.ORDER_PLACED, {
            "order_id": order_id,
            "user_id": user_id,
            "total": 99.99
        })

        await emit_event(producer, EventType.PAYMENT_PROCESSED, {
            "order_id": order_id,
            "payment_method": "credit_card"
        })

# Consumer: Handle events with different processors
async def event_processor():
    handlers = {
        EventType.USER_CREATED: handle_user_created,
        EventType.ORDER_PLACED: handle_order_placed,
        EventType.PAYMENT_PROCESSED: handle_payment_processed,
    }

    async with Consumer(
        stream_name="business-events",
        processor=MsgpackProcessor()
    ) as consumer:

        async for event in consumer:
            event_type = EventType(event["event_type"])
            handler = handlers.get(event_type)

            if handler:
                try:
                    await handler(event["data"])
                except Exception as e:
                    logger.error(f"Error handling {event_type}: {e}")
                    # Could implement retry logic or dead letter queue

async def handle_user_created(data):
    logger.info(f"Sending welcome email to {data['email']}")

async def handle_order_placed(data):
    logger.info(f"Reserving inventory for order {data['order_id']}")

async def handle_payment_processed(data):
    logger.info(f"Updating order status for {data['order_id']}")
```

## IoT Data Collection

Collect and process high-volume IoT sensor data:

```python
import asyncio
import logging
import random
from datetime import datetime
from kinesis import Producer, Consumer, KPLJsonProcessor

# Set up logging
logger = logging.getLogger(__name__)

# Producer: Simulate IoT sensors
async def iot_sensors():
    async with Producer(
        stream_name="iot-sensor-data",
        processor=KPLJsonProcessor(),  # KPL aggregation for high volume
        put_rate_limit_per_shard=500,  # Conservative rate to avoid throttling
        create_stream=True,
        create_stream_shards=4  # Multiple shards for parallel processing
    ) as producer:

        # Simulate 100 sensors
        sensor_ids = [f"sensor-{i:03d}" for i in range(100)]

        while True:
            tasks = []
            for sensor_id in sensor_ids:
                reading = {
                    "sensor_id": sensor_id,
                    "timestamp": datetime.now().isoformat(),
                    "temperature": round(20 + random.uniform(-5, 5), 2),
                    "humidity": round(50 + random.uniform(-10, 10), 2),
                    "pressure": round(1013 + random.uniform(-20, 20), 2)
                }

                # Use sensor_id as partition key for consistent routing
                tasks.append(producer.put(reading, partition_key=sensor_id))

            # Send all readings concurrently
            await asyncio.gather(*tasks)
            await asyncio.sleep(1)  # Readings every second

# Consumer: Process and aggregate sensor data
async def iot_processor():
    sensor_stats = {}
    alert_thresholds = {
        "temperature": {"min": 10, "max": 30},
        "humidity": {"min": 30, "max": 70}
    }

    async with Consumer(
        stream_name="iot-sensor-data",
        processor=KPLJsonProcessor(),
        max_shard_consumers=4  # Process all shards in parallel
    ) as consumer:

        async for reading in consumer:
            sensor_id = reading["sensor_id"]

            # Update rolling statistics
            if sensor_id not in sensor_stats:
                sensor_stats[sensor_id] = {
                    "readings": 0,
                    "temperature_sum": 0,
                    "last_reading": None
                }

            stats = sensor_stats[sensor_id]
            stats["readings"] += 1
            stats["temperature_sum"] += reading["temperature"]
            stats["last_reading"] = reading

            # Check thresholds
            for metric, thresholds in alert_thresholds.items():
                value = reading[metric]
                if value < thresholds["min"] or value > thresholds["max"]:
                    logger.warning(f"Alert: {sensor_id} {metric}={value} outside range")

            # Periodic aggregation (every 100 readings)
            if stats["readings"] % 100 == 0:
                avg_temp = stats["temperature_sum"] / stats["readings"]
                logger.info(f"{sensor_id}: {stats['readings']} readings, avg temp: {avg_temp:.2f}°C")
```

## Error Handling

Robust error handling patterns for production:

```python
import asyncio
import logging
import random
from typing import Optional
from datetime import datetime
from kinesis import Producer, Consumer, JsonProcessor

# Set up logging
logger = logging.getLogger(__name__)

class RetryableError(Exception):
    """Errors that should be retried"""
    pass

class FatalError(Exception):
    """Errors that should not be retried"""
    pass

# Producer with retry logic
async def reliable_producer():
    async def send_with_retry(producer: Producer, data: dict, max_retries: int = 3):
        for attempt in range(max_retries):
            try:
                await producer.put(data)
                return  # Success

            except Exception as e:
                if "ProvisionedThroughputExceededException" in str(e):
                    # Exponential backoff for rate limiting
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"Rate limited, waiting {wait_time:.2f}s...")
                    await asyncio.sleep(wait_time)
                else:
                    # Non-retryable error
                    raise FatalError(f"Failed to send: {e}")

        raise RetryableError(f"Failed after {max_retries} attempts")

    async with Producer(
        stream_name="reliable-stream",
        retry_limit=10,  # Built-in retries
        expo_backoff=True,
        expo_backoff_limit=30
    ) as producer:

        # Send critical data with custom retry logic
        important_data = {"transaction_id": "123", "amount": 1000}
        await send_with_retry(producer, important_data)

# Consumer with error recovery
async def resilient_consumer():
    async def process_record(record: dict) -> bool:
        """Process a record, return True if successful"""
        try:
            # Simulate processing that might fail
            if random.random() < 0.1:  # 10% failure rate
                raise Exception("Processing failed")

            logger.info(f"Processed: {record}")
            return True

        except Exception as e:
            logger.error(f"Error processing record: {e}")
            return False

    # Dead letter queue for failed records
    failed_records = []

    async with Consumer(
        stream_name="reliable-stream",
        iterator_type="LATEST",
        retry_limit=5,
        expo_backoff=True
    ) as consumer:

        async for record in consumer:
            success = await process_record(record)

            if not success:
                failed_records.append({
                    "record": record,
                    "timestamp": datetime.now().isoformat(),
                    "attempts": 1
                })

                # Periodic retry of failed records
                if len(failed_records) >= 10:
                    logger.info(f"Retrying {len(failed_records)} failed records...")

                    still_failed = []
                    for failed in failed_records:
                        if await process_record(failed["record"]):
                            logger.info(f"Retry successful for record")
                        else:
                            failed["attempts"] += 1
                            if failed["attempts"] < 3:
                                still_failed.append(failed)
                            else:
                                logger.error(f"Record failed permanently: {failed['record']}")

                    failed_records = still_failed
```

## Multi-Consumer Processing

Coordinate multiple consumers for parallel processing:

```python
import asyncio
import json
import logging
from kinesis import Consumer, RedisCheckPointer

# Set up logging
logger = logging.getLogger(__name__)

# Different consumer groups for different processing needs
async def analytics_consumer():
    """Real-time analytics consumer"""
    async with Consumer(
        stream_name="multi-tenant-stream",
        checkpointer=RedisCheckPointer(
            name="analytics-group",
            session_timeout=60,
            heartbeat_frequency=15
        ),
        max_shard_consumers=2  # Limit for this consumer group
    ) as consumer:

        metrics = {"events": 0, "bytes": 0}

        async for event in consumer:
            metrics["events"] += 1
            metrics["bytes"] += len(json.dumps(event))

            # Emit metrics every 100 events
            if metrics["events"] % 100 == 0:
                logger.info(f"Analytics: {metrics}")

async def archival_consumer():
    """Batch archival consumer"""
    async with Consumer(
        stream_name="multi-tenant-stream",
        checkpointer=RedisCheckPointer(
            name="archival-group",
            session_timeout=300  # Longer timeout for batch processing
        ),
        record_limit=1000  # Larger batches
    ) as consumer:

        batch = []

        async for event in consumer:
            batch.append(event)

            # Archive when batch is full
            if len(batch) >= 100:
                # Simulate S3 upload
                logger.info(f"Archiving batch of {len(batch)} events to S3")
                batch = []

async def alerting_consumer():
    """Low-latency alerting consumer"""
    async with Consumer(
        stream_name="multi-tenant-stream",
        checkpointer=RedisCheckPointer(
            name="alerting-group",
            heartbeat_frequency=5  # Frequent heartbeats
        ),
        iterator_type="LATEST",  # Only new events
        sleep_time_no_records=0.5  # Minimal latency
    ) as consumer:

        async for event in consumer:
            if event.get("severity") == "critical":
                logger.error(f"CRITICAL ALERT: {event}")

# Run all consumer groups concurrently
async def run_consumer_groups():
    await asyncio.gather(
        analytics_consumer(),
        archival_consumer(),
        alerting_consumer()
    )
```

## Graceful Shutdown

Handle shutdown gracefully to avoid data loss:

```python
import signal
import asyncio
import logging
from kinesis import Producer, Consumer

# Set up logging
logger = logging.getLogger(__name__)

class GracefulShutdown:
    def __init__(self):
        self.shutdown_event = asyncio.Event()

        # Register signal handlers
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_event.set()

    async def run_producer(self):
        async with Producer(
            stream_name="graceful-stream",
            buffer_time=0.5
        ) as producer:

            i = 0
            while not self.shutdown_event.is_set():
                await producer.put({"message": f"Event {i}"})
                i += 1

                # Check for shutdown more frequently
                try:
                    await asyncio.wait_for(
                        self.shutdown_event.wait(),
                        timeout=0.1
                    )
                    break
                except asyncio.TimeoutError:
                    continue

            logger.info("Producer shutting down, flushing remaining records...")
            # Producer automatically flushes on exit

    async def run_consumer(self):
        async with Consumer(
            stream_name="graceful-stream"
        ) as consumer:

            async for message in consumer:
                if self.shutdown_event.is_set():
                    logger.info("Consumer shutting down after current batch...")
                    break

                # Process message
                logger.info(f"Processed: {message}")

    async def run(self):
        try:
            await asyncio.gather(
                self.run_producer(),
                self.run_consumer()
            )
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
        finally:
            logger.info("Shutdown complete")

# Usage
async def main():
    shutdown_handler = GracefulShutdown()
    await shutdown_handler.run()

if __name__ == "__main__":
    asyncio.run(main())
```

## Production Monitoring

Monitor your Kinesis streams in production:

```python
import asyncio
import logging
import time
from datetime import datetime, timedelta
from kinesis import Consumer, Producer

# Set up logging
logger = logging.getLogger(__name__)

class StreamMonitor:
    def __init__(self, stream_name: str):
        self.stream_name = stream_name
        self.metrics = {
            "records_processed": 0,
            "bytes_processed": 0,
            "errors": 0,
            "checkpoints": 0,
            "start_time": time.time()
        }

    async def monitor_consumer(self):
        async with Consumer(
            stream_name=self.stream_name,
            iterator_type="LATEST"
        ) as consumer:

            last_report = time.time()

            while True:
                # Get shard status
                status = consumer.get_shard_status()

                # Log shard health
                logger.info(f"Stream Status Report - {datetime.now()}")
                logger.info(f"Total shards: {status['total_shards']}")
                logger.info(f"Active shards: {status['active_shards']}")
                logger.info(f"Closed shards: {status['closed_shards']}")
                logger.info(f"Behind shards: {status.get('behind_shards', 0)}")

                # Check for resharding
                if status['parent_shards'] > 0:
                    logger.warning(f"Resharding detected! Parent shards: {status['parent_shards']}")

                # Monitor consumer lag
                for shard in status['shard_details']:
                    if shard.get('behind_latest'):
                        lag = shard.get('records_behind', 'unknown')
                        logger.warning(f"Shard {shard['shard_id']} is behind by {lag} records")

                # Performance metrics
                elapsed = time.time() - self.metrics["start_time"]
                if elapsed > 0:
                    rps = self.metrics["records_processed"] / elapsed
                    bps = self.metrics["bytes_processed"] / elapsed
                    logger.info(f"Performance Metrics:")
                    logger.info(f"Records/sec: {rps:.2f}")
                    logger.info(f"Bytes/sec: {bps:.2f}")
                    logger.info(f"Total errors: {self.metrics['errors']}")

                # Check error rate
                if self.metrics["errors"] > 100:
                    logger.error(f"High error rate detected: {self.metrics['errors']} errors")

                # Wait before next check
                await asyncio.sleep(30)

    async def health_check_producer(self):
        """Send periodic health check records"""
        async with Producer(
            stream_name=self.stream_name,
            buffer_time=5.0
        ) as producer:

            while True:
                health_record = {
                    "type": "health_check",
                    "timestamp": datetime.now().isoformat(),
                    "producer_id": "monitor-001",
                    "status": "healthy"
                }

                try:
                    await producer.put(health_record, partition_key="health")
                    logger.info(f"Health check sent at {datetime.now()}")
                except Exception as e:
                    logger.error(f"Health check failed: {e}")

                await asyncio.sleep(60)  # Every minute

# Usage
async def run_monitoring():
    monitor = StreamMonitor("production-stream")

    await asyncio.gather(
        monitor.monitor_consumer(),
        monitor.health_check_producer()
    )
```

## Best Practices Summary

1. **Partition Keys**: Always use meaningful partition keys for related data
2. **Error Handling**: Implement retry logic with exponential backoff
3. **Monitoring**: Track shard status and consumer lag in production
4. **Graceful Shutdown**: Handle signals properly to avoid data loss
5. **Rate Limiting**: Configure conservative limits to avoid throttling
6. **Checkpointing**: Use Redis checkpointer for multi-consumer scenarios
7. **Processor Selection**: Choose the right processor for your data format and volume

## Next Steps

- Review the [Performance Tuning Guide](./performance-tuning.md) for optimization tips
- See [Troubleshooting Guide](./troubleshooting.md) for common issues
- Check [Architecture Details](./DESIGN.md) for deep technical understanding
