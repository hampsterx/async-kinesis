# async-kinesis

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black) [![PyPI version](https://badge.fury.io/py/async-kinesis.svg)](https://badge.fury.io/py/async-kinesis) [![Python 3.9](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/downloads/release/python-390/) [![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3100/) [![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/) [![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)

High-performance async Python library for AWS Kinesis with production-ready resharding support.

## Quick Start

**Producer:**
```python
from kinesis import Producer

async with Producer(stream_name="my-stream") as producer:
    await producer.put({"message": "hello world"})
```

**Consumer:**
```python
from kinesis import Consumer

async with Consumer(stream_name="my-stream") as consumer:
    async for message in consumer:
        print(message)
```

📚 **New to async-kinesis?** Check out our [comprehensive Getting Started guide](./docs/getting-started.md) for step-by-step tutorials and examples.

## Table of Contents

- [Key Features](#key-features)
- [Installation](#installation)
- [Basic Usage](#basic-usage)
  - [Producer](#producer)
  - [Consumer](#consumer)
- [Configuration](#configuration)
  - [Producer Options](#producer-options)
  - [Consumer](#consumer)
- [Rate Limiting & API Optimization](#rate-limiting--api-optimization)
- [Shard Management](#shard-management)
- [Architecture Comparison](#architecture-comparison)
- [Checkpointers](#checkpointers)
- [Processors](#processors-aggregator--serializer)
- [Benchmark/Example](#benchmarkexample)
- [Development & Testing](#development--testing)
- [Documentation](#documentation)

## Key Features

- **Production-Ready Resharding**: Automatic shard discovery and topology management
- **Async/Await Native**: Built for modern Python async patterns
- **High Performance**: Queue-based architecture with configurable batching
- **AWS Best Practices**: Parent-child shard ordering and proper error handling
- **Rate Limit Optimization**: Skip DescribeStream or use ListShards for better API limits
- **Stream Addressing**: Support for both stream names and ARNs
- **Custom Partition Keys**: Control record distribution and ordering across shards
- **Multi-Consumer Support**: Redis-based checkpointing with heartbeats
- **Flexible Processing**: Pluggable serialization (JSON, MessagePack, KPL)
- **Operational Visibility**: Rich monitoring APIs for production debugging
- **Metrics & Observability**: Optional Prometheus/CloudWatch metrics with zero overhead when disabled

### Resharding Support Highlights

Unlike basic Kinesis libraries, async-kinesis provides enterprise-grade resharding capabilities:

- **Automatic discovery** of new shards during resharding operations
- **Parent-child ordering** enforcement following AWS best practices
- **Graceful handling** of closed shards and iterator expiration
- **Real-time monitoring** with detailed topology and status reporting
- **Seamless coordination** between multiple consumer instances

📖 **[Architecture Details](./docs/DESIGN.md)** | **[Why Another Library?](docs/YETANOTHER.md)**

## Installation

```bash
pip install async-kinesis

# With optional dependencies
pip install async-kinesis[prometheus]  # For Prometheus metrics
pip install async-kinesis[redis]       # For Redis checkpointing
pip install async-kinesis[dynamodb]    # For DynamoDB checkpointing
```

## Basic Usage

### Environment Variables

As required by boto3

```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
```

### Stream Addressing

The `stream_name` argument to consumer and producer accepts either the [StreamName]
like `test`, or a full [StreamARN] like `arn:aws:kinesis:eu-central-1:842404475894:stream/test`.

### Producer

```python
from kinesis import Producer

async with Producer(stream_name="test") as producer:
    # Put item onto queue to be flushed via put_records()
    await producer.put({'my': 'data'})

    # Use custom partition key for record distribution control
    await producer.put({'user_id': '123', 'action': 'login'}, partition_key='user-123')
```

**With Metrics:**
```python
from kinesis import Producer, PrometheusMetricsCollector

# Enable Prometheus metrics (optional)
metrics = PrometheusMetricsCollector(namespace="my_app")

async with Producer(stream_name="test", metrics_collector=metrics) as producer:
    await producer.put({'my': 'data'})
    # Metrics are automatically collected: throughput, errors, batch sizes, etc.
```

## Configuration

### Producer Options

### Custom Partition Keys

Control shard distribution and maintain record ordering by specifying custom partition keys:

```python
async with Producer(stream_name="my-stream") as producer:
    # Group related records with same partition key
    await producer.put({'user_id': '123', 'action': 'login'}, partition_key='user-123')
    await producer.put({'user_id': '123', 'action': 'view_page'}, partition_key='user-123')

    # Different partition key for different user
    await producer.put({'user_id': '456', 'action': 'login'}, partition_key='user-456')

    # Use default generated partition key
    await producer.put({'system': 'health_check'})  # Auto-generated key
```

**Key Benefits:**
- **Ordered processing**: Records with same partition key go to same shard, maintaining order
- **Load distribution**: Spread records across shards using different partition keys
- **Grouping**: Keep related records together (e.g., all events for a user)
- **Backward compatible**: Existing code without partition keys continues to work

**Validation:**
- Maximum 256 bytes when UTF-8 encoded
- Supports Unicode characters
- Cannot be empty string
- Must be string type

**Aggregation Behavior:**
- **SimpleAggregator**: Records with different partition keys are batched separately
- **KPL Aggregators**: Custom partition keys not supported (raises `ValueError`)
- **Other Aggregators**: Automatically group records by partition key within batches

### Rate Limiting & API Optimization

AWS Kinesis APIs have strict rate limits that can impact performance in high-throughput or multi-consumer scenarios. async-kinesis provides optimization options to work around these constraints:

#### API Rate Limits
- **DescribeStream**: 10 operations/second **account-wide** (shared across all applications)
- **ListShards**: 100 operations/second **per stream** (much higher limit)

### Optimization Options

**Skip DescribeStream entirely** (best for pre-provisioned streams):
```python
# Assumes stream exists and is active - no API calls during startup
async with Consumer(stream_name="my-stream", skip_describe_stream=True) as consumer:
    async for message in consumer:
        print(message)
```

**Use ListShards API** for better rate limits:
```python
# Uses ListShards (100 ops/s) instead of DescribeStream (10 ops/s)
async with Consumer(stream_name="my-stream", use_list_shards=True) as consumer:
    async for message in consumer:
        print(message)
```

**Combined approach** for maximum optimization:
```python
# Skip startup calls + use ListShards for any later shard discovery
async with Consumer(
    stream_name="my-stream",
    skip_describe_stream=True,  # Skip startup API calls
    use_list_shards=True       # Use ListShards if shard refresh needed
) as consumer:
    async for message in consumer:
        print(message)
```

### When to Use

| Scenario | Recommendation | Reason |
| --- | --- | --- |
| **Single consumer, known stream** | `skip_describe_stream=True` | Eliminates rate limit issues entirely |
| **Multiple consumers** | `use_list_shards=True` | 10x better rate limits than DescribeStream |
| **Short-lived consumers** | `skip_describe_stream=True` | Fastest startup, no API overhead |
| **Stream discovery needed** | `use_list_shards=True` | Need shard info but want better limits |
| **Production deployment** | Both options | Maximum resilience to rate limiting |

**⚠️ Important**: `skip_describe_stream=True` assumes the stream exists and is active. Use only when you're certain the stream is available.

| Parameter | Default | Description |
| --- | --- | --- |
| **session** | None | AioSession (to use non default profile etc) |
| **region_name** | None | AWS Region |
| **buffer_time** | 0.5 | Buffer time in seconds before auto flushing records |
| **put_rate_limit_per_shard** | 1000 | "A single shard can ingest up to 1 MiB of data per second (including partition keys) or 1,000 records per second for writes" |
| **put_bandwidth_limit_per_shard** | 1024 | Kb per sec. max is 1024 per shard (ie 1 MiB). Keep below to minimize ProvisionedThroughputExceeded" errors * |
| **batch_size** | 500 | "Each PutRecords request can support up to 500 records" |
| **max_queue_size** | 10000 | put() method will block when queue is at max |
| **after_flush_fun** | None | async function to call after doing a flush (err put_records()) call |
| **processor** | JsonProcessor() | Record aggregator/serializer. Default is JSON without aggregation. Note this is highly inefficient as each record can be up to 1Mib |
| **retry_limit** | None | How many connection attempts should be made before raising a exception |
| **expo_backoff** | None | Exponential Backoff when connection attempt fails |
| **expo_backoff_limit** | 120 | Max amount of seconds Exponential Backoff can grow |
| **skip_describe_stream** | False | Skip DescribeStream API calls for better rate limits (assumes stream exists and is active) |
| **use_list_shards** | False | Use ListShards API instead of DescribeStream for better rate limits (100 ops/s vs 10 ops/s) |
| **create_stream** | False | Creates a Kinesis Stream based on the `stream_name` keyword argument. Note if stream already existing it will ignore |
| **create_stream_shards** | 1 | Sets the amount of shard you want for your new stream. Note if stream already existing it will ignore  |

* Throughput exceeded. The docs (for Java/KPL see: https://docs.aws.amazon.com/streams/latest/dev/kinesis-producer-adv-retries-rate-limiting.html) state:

> You can lower this limit to reduce spamming due to excessive retries. However, the best practice is for each producer is to retry for maximum throughput aggressively and to handle any resulting throttling determined as excessive by expanding the capacity of the stream and implementing an appropriate partition key strategy.

Even though our default here is to limit at this threshold (1024kb) in reality the threshold seems lower (~80%).
If you wish to avoid excessive throttling or have multiple producers on a stream you will want to set this quite a bit lower.


### Consumer

```python
from kinesis import Consumer

async with Consumer(stream_name="test") as consumer:
    async for item in consumer:
        print(item)
    # Consumer continues to wait for new messages after catching up
```


Options:

(comments in quotes are Kinesis Limits as per AWS Docs)


| Arg | Default | Description |
| --- | --- | --- |
| session | None | AioSession (to use non default profile etc) |
| region_name | None | AWS Region |
| max_queue_size | 10000 | the fetch() task shard will block when queue is at max |
| max_shard_consumers | None | Max number of shards to use. None = all |
| record_limit | 10000 | Number of records to fetch with get_records() |
| sleep_time_no_records | 2 | No of seconds to sleep when caught up |
| iterator_type | TRIM_HORIZON | Default shard iterator type for new/unknown shards (ie start from start of stream). Alternatives are "LATEST" (ie end of stream), "AT_TIMESTAMP" (ie particular point in time, requires defining `timestamp` arg) |
| shard_fetch_rate | 1 | No of fetches per second (max = 5). 1 is recommended as allows having multiple consumers without hitting the max limit. |
| checkpointer | MemoryCheckPointer() | Checkpointer to use |
| processor | JsonProcessor() |  Record aggregator/serializer. Must Match processor used by Producer() |
| retry_limit | None | How many connection attempts should be made before raising a exception |
| expo_backoff | None | Exponential Backoff when connection attempt fails |
| expo_backoff_limit | 120 | Max amount of seconds Exponential Backoff can grow |
| skip_describe_stream | False | Skip DescribeStream API calls for better rate limits (assumes stream exists and is active) |
| use_list_shards | False | Use ListShards API instead of DescribeStream for better rate limits (100 ops/s vs 10 ops/s) |
| create_stream | False | Creates a Kinesis Stream based on the `stream_name` keyword argument. Note if stream already existing it will ignore |
| create_stream_shards | 1 | Sets the amount of shard you want for your new stream. Note if stream already existing it will ignore  |
| timestamp | None | Timestamp to start reading stream from. Used with iterator type "AT_TIMESTAMP"

## Shard Management

The consumer includes sophisticated shard management capabilities for handling Kinesis stream resharding. Our approach combines automatic topology management with lightweight coordination, providing production-ready resharding support without the overhead of external databases or complex lease management systems.

### Automatic Shard Discovery
- **Dynamic detection**: Automatically discovers new shards during resharding operations
- **Configurable refresh**: Checks for new shards every 60 seconds (configurable via `_shard_refresh_interval`)
- **State preservation**: Maintains existing shard iterators and statistics when discovering new shards
- **Resharding events**: Proactively detects and logs resharding events when child shards appear

### Parent-Child Shard Ordering (AWS Best Practice)
- **Topology mapping**: Automatically builds parent-child relationship maps from shard metadata
- **Sequential consumption**: Enforces reading parent shards before child shards to maintain data order
- **Exhaustion tracking**: Tracks when parent shards are fully consumed to enable child consumption
- **Smart allocation**: Only allocates child shards after their parents are exhausted or closed

### Closed Shard Handling
- **Graceful closure**: Properly handles shards that reach end-of-life (`NextShardIterator` returns `null`)
- **Resource cleanup**: Automatically deallocates closed shards from checkpointers
- **Parent transitioning**: Marks exhausted parent shards to enable their children for consumption
- **Coordination**: Enables other consumers to take over child shards immediately

### Error Recovery
- **Iterator expiration**: Automatically recreates expired shard iterators from last known position
- **Missing shards**: Handles cases where shards are deleted or become unavailable
- **Connection recovery**: Robust retry logic with exponential backoff
- **Topology resilience**: Maintains parent-child relationships even during connection failures

### Monitoring & Operational Visibility
Use `consumer.get_shard_status()` to get comprehensive operational visibility:

```python
status = consumer.get_shard_status()

# Overall shard metrics
print(f"Total shards: {status['total_shards']}")
print(f"Active shards: {status['active_shards']}")
print(f"Closed shards: {status['closed_shards']}")
print(f"Allocated shards: {status['allocated_shards']}")

# Topology information (resharding awareness)
print(f"Parent shards: {status['parent_shards']}")
print(f"Child shards: {status['child_shards']}")
print(f"Exhausted parents: {status['exhausted_parents']}")

# Parent-child relationships
for parent, children in status['topology']['parent_child_map'].items():
    print(f"Parent {parent} → Children {children}")

# Per-shard details with topology info
for shard in status['shard_details']:
    print(f"Shard {shard['shard_id']}: "
          f"allocated={shard['is_allocated']}, "
          f"parent={shard['is_parent']}, "
          f"child={shard['is_child']}, "
          f"can_allocate={shard['can_allocate']}")
```

This provides detailed information about shard allocation, closure status, parent-child relationships, and iterator health for production monitoring and debugging resharding scenarios.

## Architecture Comparison

### Traditional Approaches

**External Database Coordination**: Some enterprise solutions rely on external databases (like DynamoDB) for lease management and shard coordination. While sophisticated, this approach requires additional infrastructure, increases operational complexity, and adds latency to shard allocation decisions.

**Manual Thread-per-Shard**: Basic implementations use simple iterator-based loops with one thread per shard. These require manual resharding detection, lack parent-child ordering enforcement, and provide limited coordination between multiple consumers.

**Reactive Closed Shard Detection**: Most basic approaches only detect closed shards when `NextShardIterator` returns null, without proactive resharding awareness or topology management.

### Our Approach

**Lightweight Async Coordination**: Uses Redis-based checkpointing with heartbeat mechanisms for multi-consumer coordination without requiring external database infrastructure.

**Proactive Topology Management**: Automatically builds and maintains parent-child shard relationships from AWS metadata, enforcing proper consumption ordering according to AWS best practices.

**Intelligent Allocation**: Only allocates child shards after their parents are exhausted, preventing data ordering issues during resharding events.

**Production-Ready Error Recovery**: Comprehensive handling of iterator expiration, connection failures, and shard state transitions with exponential backoff and automatic recovery.

### Benefits

- **Operational Simplicity**: No additional database infrastructure required beyond optional Redis for multi-consumer scenarios
- **AWS Best Practices**: Automatic compliance with parent-child consumption ordering
- **Async Performance**: Built for modern async/await patterns with non-blocking operations
- **Resource Efficiency**: Intelligent shard allocation reduces unnecessary resource consumption
- **Production Monitoring**: Rich operational visibility for debugging and monitoring resharding scenarios


## Checkpointers

Checkpointers manage shard allocation and progress tracking across multiple consumer instances.

### MemoryCheckPointer

The default checkpointer (but only useful for single-consumer testing):

```python
MemoryCheckPointer()
```

### RedisCheckPointer

Redis-based checkpointer for distributed consumers:

```python
RedisCheckPointer(
    name="consumer-group",
    session_timeout=60,
    heartbeat_frequency=15,
    is_cluster=False
)
```

**Requirements:**
- Install: `pip install async-kinesis[redis]`
- Environment: `REDIS_HOST` (and optionally `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_DB`)

### DynamoDBCheckPointer

DynamoDB-based checkpointer for serverless deployments:

```python
DynamoDBCheckPointer(
    name="consumer-group",
    table_name=None,  # Optional: defaults to kinesis-checkpoints-{name}
    session_timeout=60,
    heartbeat_frequency=15,
    create_table=True,  # Auto-create table if needed
    ttl_hours=24  # Automatic cleanup of old records
)
```

**Requirements:**
- Install: `pip install async-kinesis[dynamodb]`
- AWS credentials with DynamoDB permissions

**Benefits over Redis:**
- No infrastructure to manage
- Pay-per-request pricing (no idle costs)
- Automatic scaling
- Built-in backup and recovery

📖 **[DynamoDB Checkpointing Guide](./docs/dynamodb-checkpointing.md)** - Detailed setup and configuration


## Processors (Aggregator + Serializer)


Aggregation enable batching up multiple records to more efficiently use the stream.
Refer https://aws.amazon.com/blogs/big-data/implementing-efficient-and-reliable-producers-with-the-amazon-kinesis-producer-library/


| Class | Aggregator | Serializer | Description |
| --- | --- | --- | --- |
| StringProcessor | SimpleAggregator | StringSerializer | Single String record |
| JsonProcessor | SimpleAggregator | JsonSerializer | Single JSON record |
| JsonLineProcessor | NewlineAggregator | JsonSerializer | Multiple JSON record separated by new line char |
| JsonListProcessor | ListAggregator | JsonSerializer | Multiple JSON record returned by list |
| MsgpackProcessor | NetstringAggregator | MsgpackSerializer | Multiple Msgpack record framed with Netstring Protocol (https://en.wikipedia.org/wiki/Netstring) |
| KPLJsonProcessor | KPLAggregator | JsonSerializer | Multiple JSON record in a KPL Aggregated Record (https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md) |
| KPLStringProcessor | KPLAggregator | StringSerializer | Multiple String record in a KPL Aggregated Record (https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md) |

Note you can define your own processor easily as it's simply a class inheriting the Aggregator + Serializer.

```
class MsgpackProcessor(Processor, NetstringAggregator, MsgpackSerializer):
    pass
```

Just define a new Serializer class with serialize() and deserialize() methods.

Note:

* Json will use `pip install ujson` if installed
* Msgpack requires `pip install msgpack` to be installed
* KPL requires `pip install aws-kinesis-agg` to be installed

## Benchmark/Example

See [benchmark.py](./benchmark.py) for code

The benchmark tool allows you to test the performance of different processors with AWS Kinesis streams.

### Usage

```bash
# Run with default settings (50k records)
python benchmark.py

# Dry run without creating AWS resources
python benchmark.py --dry-run

# Custom parameters
python benchmark.py --records 10000 --shards 2 --processors json msgpack

# Generate markdown output
python benchmark.py --markdown

# Run multiple iterations
python benchmark.py --iterations 3
```

### Example Results

50k items of approx 1k (python) in size, using single shard:

| Processor | Iteration | Python Bytes | Kinesis Bytes | Time (s) | Records/s | Python MB/s | Kinesis MB/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| StringProcessor | 1 | 2.7 MB | 50.0 MB | 51.2 | 977 | 53.9 kB | 1000.0 kB |
| JsonProcessor | 1 | 2.7 MB | 50.0 MB | 52.1 | 960 | 52.9 kB | 982.5 kB |
| JsonLineProcessor | 1 | 2.7 MB | 41.5 MB | 43.5 | 1149 | 63.4 kB | 976.7 kB |
| JsonListProcessor | 1 | 2.7 MB | 2.6 MB | 2.8 | 17857 | 982.1 kB | 946.4 kB |
| MsgpackProcessor | 1 | 2.7 MB | 33.9 MB | 35.8 | 1397 | 77.0 kB | 969.8 kB |

*Note: MsgpackProcessor performance varies significantly with record size. While it appears slower here with small records due to netstring framing overhead (~9%) and msgpack serialization costs, it can be faster than JSON processors with larger, complex data structures where msgpack's binary efficiency provides benefits.*

### Processor Recommendations

Choose the optimal processor based on your use case:

| Use Case | Recommended Processor | Reason |
| --- | --- | --- |
| **High-frequency small messages** (<500 bytes) | JsonLineProcessor | Minimal aggregation overhead, simple parsing |
| **Individual JSON messages** | JsonProcessor | Maximum compatibility, no aggregation complexity |
| **Batch processing arrays** | JsonListProcessor | Highest throughput for compatible consumers |
| **Large complex data** (>1KB) | MsgpackProcessor | Binary efficiency outweighs overhead |
| **Raw text/logs** | StringProcessor | Minimal processing overhead |
| **Binary data or deeply nested objects** | MsgpackProcessor | Compact binary representation |
| **Real-time streaming** | JsonLineProcessor | Simple, fast parsing with good compression |
| **Bandwidth-constrained environments** | MsgpackProcessor | Smaller payload sizes |

**Performance Testing:** Use the benchmark tool with different `--record-size-kb` and `--processors` options to determine the best processor for your specific data patterns.


## Development & Testing

### Local Testing

Uses LocalStack for integration testing:

```bash
# Run full test suite via Docker
docker-compose up --abort-on-container-exit --exit-code-from test

# Local development setup
docker-compose up kinesis redis
pip install -r test-requirements.txt
pytest
```

### Code Quality

This project uses automated code formatting and linting:

```bash
# Install development tools
pip install -r test-requirements.txt

# Run formatting and linting
black .
isort .
flake8 .

# Or use pre-commit hooks
pre-commit install
pre-commit run --all-files
```

### AWS Integration Tests

Some tests require actual AWS Kinesis. Create `.env` file:

```
TESTING_USE_AWS_KINESIS=1
```

### Resharding Tests

Comprehensive resharding test suite available:

```bash
# Unit tests (no AWS required)
python tests/resharding/test_resharding_simple.py

# Integration tests (requires LocalStack)
python tests/resharding/test_resharding_integration.py

# Production testing (requires AWS)
python tests/resharding/resharding_test.py --scenario scale-up-small
```


## Documentation

- **[Getting Started Guide](./docs/getting-started.md)** - Step-by-step tutorials for beginners
- **[Common Patterns](./docs/common-patterns.md)** - Real-world use cases and examples
- **[Metrics & Observability](./docs/metrics.md)** - Prometheus integration and monitoring
- **[DynamoDB Checkpointing](./docs/dynamodb-checkpointing.md)** - DynamoDB checkpointer setup and best practices
- **[Troubleshooting Guide](./docs/troubleshooting.md)** - Solutions for common issues
- **[Architecture Details](./docs/DESIGN.md)** - Technical deep dive into the implementation
- **[Why Another Library?](./docs/YETANOTHER.md)** - Comparison with other Kinesis libraries

[ARN]: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html
[StreamARN]: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StreamDescription.html#Streams-Type-StreamDescription-StreamARN
[StreamName]: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StreamDescription.html#Streams-Type-StreamDescription-StreamName
