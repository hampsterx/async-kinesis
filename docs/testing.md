# Testing Guide

In-memory stream mocks and test helpers for async-kinesis. No Docker, no LocalStack, no network calls.

## Installation

The testing utilities ship with the main package — no extra dependencies required:

```bash
pip install async-kinesis
```

## Quick Start

```python
import asyncio
from kinesis.testing import (
    MockKinesisBackend, MockProducer, MockConsumer, collect_records,
)

async def main():
    MockKinesisBackend.create_stream("my-stream")

    async with MockProducer(stream_name="my-stream") as producer:
        await producer.put({"user": "alice", "action": "login"})
        await producer.put({"user": "bob", "action": "signup"})

    MockKinesisBackend.get_stream("my-stream").seal()

    async with MockConsumer(stream_name="my-stream") as consumer:
        records = await collect_records(consumer)

    print(records)
    # [{'user': 'alice', 'action': 'login'}, {'user': 'bob', 'action': 'signup'}]

asyncio.run(main())
```

## Pytest Fixtures

Fixtures are auto-discovered when async-kinesis is installed (via `pytest11` entry point). No imports or `conftest.py` changes needed.

### Available Fixtures

| Fixture | Scope | Provides |
|---------|-------|----------|
| `kinesis_backend` | function | `MockKinesisBackend` (reset before and after each test) |
| `kinesis_stream` | function | Single-shard `MemoryStream` named `test-stream` |
| `kinesis_producer` | function | `MockProducer` connected to `kinesis_stream` |
| `kinesis_consumer` | function | `MockConsumer` connected to `kinesis_stream` |

### Basic Fixture Usage

```python
import pytest
from kinesis.testing import collect_records

@pytest.mark.asyncio
async def test_roundtrip(kinesis_stream, kinesis_producer, kinesis_consumer):
    await kinesis_producer.put({"msg": "hello"})
    await kinesis_producer.flush()
    kinesis_stream.seal()

    records = await collect_records(kinesis_consumer)
    assert records == [{"msg": "hello"}]
```

### Custom Stream Configuration

```python
import pytest
from kinesis.testing import MockKinesisBackend, MockProducer, MockConsumer, collect_records

@pytest.mark.asyncio
async def test_multi_shard(kinesis_backend):
    stream = kinesis_backend.create_stream("orders", shard_count=4)

    async with MockProducer(stream_name="orders") as producer:
        for i in range(100):
            await producer.put({"order_id": i}, partition_key=f"customer-{i % 10}")

    stream.seal()

    async with MockConsumer(stream_name="orders") as consumer:
        records = await collect_records(consumer)

    assert len(records) == 100
```

### Using with Different Processors

```python
from kinesis import StringProcessor
from kinesis.testing import MockProducer, MockConsumer, MockKinesisBackend, collect_records

@pytest.mark.asyncio
async def test_string_processor(kinesis_backend):
    kinesis_backend.create_stream("logs")

    async with MockProducer(stream_name="logs", processor=StringProcessor()) as producer:
        await producer.put("2024-01-15 ERROR: Connection failed")
        await producer.put("2024-01-15 INFO: Retry succeeded")

    MockKinesisBackend.get_stream("logs").seal()

    async with MockConsumer(stream_name="logs", processor=StringProcessor()) as consumer:
        records = await collect_records(consumer)

    assert len(records) == 2
    assert "ERROR" in records[0]
```

## Drop-in Replacement with `patch()`

The mock classes accept the same constructor arguments as the real classes (ignoring AWS-specific ones), so they work as drop-in replacements:

### Application Code

```python
# myapp/pipeline.py
from kinesis import Producer, Consumer

async def process_events(stream_name: str, events: list):
    async with Producer(stream_name=stream_name, region_name="us-east-1") as producer:
        for event in events:
            await producer.put(event)

async def consume_events(stream_name: str):
    results = []
    async with Consumer(stream_name=stream_name, region_name="us-east-1") as consumer:
        async for record in consumer:
            results.append(record)
    return results
```

### Test Code

```python
from unittest.mock import patch
from kinesis.testing import MockProducer, MockConsumer, MockKinesisBackend

@pytest.mark.asyncio
async def test_pipeline():
    MockKinesisBackend.reset()
    stream = MockKinesisBackend.create_stream("events")

    with patch("myapp.pipeline.Producer", MockProducer), \
         patch("myapp.pipeline.Consumer", MockConsumer):
        await process_events("events", [{"type": "click"}, {"type": "view"}])

        stream.seal()
        results = await consume_events("events")

    assert len(results) == 2
    MockKinesisBackend.reset()
```

## Test Helpers

### `collect_records(consumer, count=None, timeout=5.0)`

Collect records from a consumer with optional count limit and timeout.

```python
# Collect all records (until stream ends or timeout)
records = await collect_records(consumer)

# Collect exactly 10 records
records = await collect_records(consumer, count=10)

# With custom timeout
records = await collect_records(consumer, timeout=30.0)
```

### `assert_records_delivered(producer, consumer, records, ...)`

End-to-end verification: produce, seal, consume, assert.

```python
from kinesis.testing import assert_records_delivered

@pytest.mark.asyncio
async def test_delivery(kinesis_stream, kinesis_producer, kinesis_consumer):
    test_data = [{"id": i} for i in range(50)]
    await assert_records_delivered(kinesis_producer, kinesis_consumer, test_data)
```

With a shared partition key (guarantees ordering):

```python
await assert_records_delivered(
    kinesis_producer, kinesis_consumer,
    [{"seq": 1}, {"seq": 2}, {"seq": 3}],
    partition_key="same-shard",
)
```

### `assert_shard_ordering(records, key_func, order_func)`

Verify that records with the same partition key maintain their ordering.

Groups records by `key_func` and asserts that `order_func` values are monotonically non-decreasing within each group.

```python
from kinesis.testing import assert_shard_ordering

records = await collect_records(consumer)

# Verify ordering by partition key, using a sequence field
assert_shard_ordering(
    records,
    key_func=lambda r: r["user_id"],
    order_func=lambda r: r["timestamp"],
)
```

## Non-Pytest Usage

Use the `memory_stream` context manager for frameworks other than pytest:

```python
from kinesis.testing import memory_stream, MockProducer, MockConsumer, collect_records

async def test_with_unittest():
    with memory_stream("my-stream", shard_count=2) as stream:
        async with MockProducer(stream_name="my-stream") as producer:
            await producer.put({"key": "value"})
            await producer.flush()

        stream.seal()

        async with MockConsumer(stream_name="my-stream") as consumer:
            records = await collect_records(consumer)

        assert records == [{"key": "value"}]
```

## API Reference

### MemoryStream

| Property / Method | Description |
|-------------------|-------------|
| `name` | Stream name |
| `shard_count` | Number of shards |
| `shards` | List of `MemoryShard` instances |
| `sealed` | Whether `seal()` has been called |
| `record_count` | Total records across all shards |
| `put(data, partition_key)` | Write bytes to the appropriate shard |
| `seal()` | Signal end-of-stream (injects sentinel per shard) |

### MockKinesisBackend

| Method | Description |
|--------|-------------|
| `create_stream(name, shard_count=1)` | Register a new in-memory stream |
| `get_stream(name)` | Look up a stream (raises `KeyError` if missing) |
| `stream_exists(name)` | Check if a stream exists |
| `reset()` | Clear all streams |

### MockProducer

Same constructor as `kinesis.Producer`. AWS-specific arguments are accepted but ignored.

| Method | Description |
|--------|-------------|
| `put(data, partition_key=None)` | Serialise and write to stream |
| `flush()` | Flush aggregator buffer |
| `close()` | Flush and stop |
| `stream` | The backing `MemoryStream` (property) |

### MockConsumer

Same constructor as `kinesis.Consumer`. AWS-specific arguments are accepted but ignored.

Extra parameter: `poll_delay` (default `0`) — seconds between poll cycles when no records are available.

| Method / Property | Description |
|-------------------|-------------|
| `async for record in consumer` | Iterate records until sealed + drained |
| `close()` | Close checkpointer |
| `stream` | The backing `MemoryStream` (property) |
| `checkpointer` | The checkpointer instance |

### Iterator Types

| Type | Behaviour |
|------|-----------|
| `TRIM_HORIZON` (default) | Read from the beginning of each shard |
| `LATEST` | Only see records produced after the consumer starts |

## Migrating from Docker/LocalStack

Replace infrastructure-dependent test setup with in-memory mocks:

**Before (Docker + kinesalite):**

```python
@pytest_asyncio.fixture
async def producer(random_stream_name):
    async with Producer(
        stream_name=random_stream_name,
        endpoint_url="http://localhost:4567",
        region_name="ap-southeast-2",
        create_stream=True,
    ) as prod:
        yield prod
```

**After (in-memory):**

```python
from kinesis.testing import MockProducer

@pytest_asyncio.fixture
async def producer(kinesis_stream):
    async with MockProducer(stream_name=kinesis_stream.name) as prod:
        yield prod
```

Key differences:
- No `endpoint_url`, `region_name`, or AWS credentials needed
- No Docker containers to start/stop
- Tests run in milliseconds instead of seconds
- Deterministic behaviour (no network flakiness)
- Call `stream.seal()` to signal end-of-stream for consumer termination
