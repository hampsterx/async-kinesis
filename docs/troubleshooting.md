# Troubleshooting Guide

This guide helps you diagnose and fix common issues with async-kinesis.

## Table of Contents
- [Connection Issues](#connection-issues)
- [Authentication Errors](#authentication-errors)
- [Rate Limiting & Throttling](#rate-limiting--throttling)
- [Consumer Not Receiving Messages](#consumer-not-receiving-messages)
- [Producer Performance Issues](#producer-performance-issues)
- [Resharding Problems](#resharding-problems)
- [Memory Issues](#memory-issues)
- [Error Messages Reference](#error-messages-reference)

## Connection Issues

### Problem: Cannot connect to Kinesis
```text
botocore.exceptions.EndpointConnectionError: Could not connect to the endpoint URL
```

**Solutions:**

1. **Check AWS credentials:**
```bash
# Verify credentials are set
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY
echo $AWS_DEFAULT_REGION
```

2. **Test with AWS CLI:**
```bash
aws kinesis list-streams --region your-region
```

3. **Check network connectivity:**
```python
# For LocalStack/development
async with Producer(
    stream_name="test",
    session=AioSession(),
    endpoint_url="http://localhost:4566"  # LocalStack endpoint
) as producer:
    await producer.put({"test": "data"})
```

4. **Behind a proxy?** Configure boto3 session:
```python
from aiobotocore.session import AioSession

session = AioSession()
session.set_config_variable('proxy', 'http://proxy.company.com:8080')

async with Producer(stream_name="test", session=session) as producer:
    # ...
```

## Authentication Errors

### Problem: NoCredentialsError
```text
botocore.exceptions.NoCredentialsError: Unable to locate credentials
```

**Solutions:**

1. **Environment variables (recommended for development):**
```bash
export AWS_ACCESS_KEY_ID="your-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
```

2. **AWS profile:**
```python
from aiobotocore.session import AioSession

session = AioSession(profile='my-profile')
async with Producer(stream_name="test", session=session) as producer:
    # ...
```

3. **IAM role (for EC2/ECS/Lambda):**
```python
# No credentials needed - uses instance metadata
async with Producer(stream_name="test") as producer:
    # ...
```

### Problem: AccessDeniedException
```text
botocore.exceptions.ClientError: An error occurred (AccessDeniedException)
```

**Required IAM permissions:**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kinesis:PutRecords",
                "kinesis:PutRecord",
                "kinesis:DescribeStream",
                "kinesis:ListShards",
                "kinesis:GetShardIterator",
                "kinesis:GetRecords",
                "kinesis:CreateStream",
                "kinesis:DeleteStream"
            ],
            "Resource": "arn:aws:kinesis:*:*:stream/your-stream-name"
        }
    ]
}
```

## Rate Limiting & Throttling

### Problem: ProvisionedThroughputExceededException
```text
botocore.exceptions.ClientError: An error occurred (ProvisionedThroughputExceededException)
```

**Solutions:**

1. **Reduce producer rate limits:**
```python
async with Producer(
    stream_name="high-volume-stream",
    put_rate_limit_per_shard=500,     # Half the default
    put_bandwidth_limit_per_shard=512, # Half the default
    buffer_time=2.0                    # Longer buffering
) as producer:
    # ...
```

2. **Add more shards:**
```bash
aws kinesis update-shard-count \
    --stream-name your-stream \
    --target-shard-count 4
```

3. **Implement backoff in your code:**
```python
async def put_with_backoff(producer, data, max_retries=5):
    for attempt in range(max_retries):
        try:
            await producer.put(data)
            return
        except Exception as e:
            if "ProvisionedThroughputExceededException" in str(e):
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                await asyncio.sleep(wait_time)
            else:
                raise
```

4. **Use multiple streams for very high volume:**
```python
# Distribute load across streams
stream_names = ["stream-1", "stream-2", "stream-3"]
producers = []

for stream in stream_names:
    producer = Producer(stream_name=stream)
    producers.append(producer)

# Round-robin distribution
for i, data in enumerate(data_items):
    producer = producers[i % len(producers)]
    await producer.put(data)
```

## Consumer Not Receiving Messages

### Problem: Consumer appears stuck, not processing new records

**Diagnostic steps:**

1. **Check iterator type:**
```python
# This only reads NEW messages
async with Consumer(
    stream_name="test",
    iterator_type="LATEST"  # Change to "TRIM_HORIZON" to read from beginning
) as consumer:
    # ...
```

2. **Verify stream has data:**
```bash
# Check if stream exists and has data
aws kinesis describe-stream --stream-name your-stream
```

3. **Enable debug logging:**
```python
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('kinesis')
logger.setLevel(logging.DEBUG)
```

4. **Check shard status:**
```python
async with Consumer(stream_name="test") as consumer:
    # Let it initialize
    await asyncio.sleep(2)

    # Check shard status
    status = consumer.get_shard_status()
    print(f"Active shards: {status['active_shards']}")
    print(f"Allocated shards: {status['allocated_shards']}")

    for shard in status['shard_details']:
        print(f"Shard {shard['shard_id']}: allocated={shard['is_allocated']}")
```

5. **Check checkpointer conflicts:**
```python
# If using Redis checkpointer, ensure unique names
async with Consumer(
    stream_name="test",
    checkpointer=RedisCheckPointer(
        name="consumer-group-1",  # Must be unique per consumer group
        session_timeout=60
    )
) as consumer:
    # ...
```

## Producer Performance Issues

### Problem: Slow message sending or high latency

**Solutions:**

1. **Optimize batching:**
```python
async with Producer(
    stream_name="test",
    batch_size=500,      # Maximum batch size
    buffer_time=0.1,     # Minimal buffer time for low latency
    max_queue_size=50000 # Larger queue for bursts
) as producer:
    # ...
```

2. **Use appropriate processor:**
```python
# For high-volume small messages
from kinesis import JsonLineProcessor

async with Producer(
    stream_name="test",
    processor=JsonLineProcessor()  # More efficient for small records
) as producer:
    # ...
```

3. **Parallelize puts:**
```python
async def parallel_puts(producer, records):
    tasks = []
    for record in records:
        # Create tasks without awaiting
        task = producer.put(record)
        tasks.append(task)

    # Wait for all to complete
    await asyncio.gather(*tasks)
```

4. **Monitor queue size:**
```python
# Check if queue is full (blocking puts)
if producer._queue.qsize() > producer._max_queue_size * 0.8:
    print("Warning: Queue nearly full, consider increasing max_queue_size")
```

## Resharding Problems

### Problem: Consumer not processing new shards after resharding

**Solutions:**

1. **Enable shard refresh:**
```python
# Consumer automatically refreshes every 60 seconds by default
# You can check the current topology:
async with Consumer(stream_name="test") as consumer:
    await asyncio.sleep(5)  # Let it initialize

    status = consumer.get_shard_status()
    print(f"Parent shards: {status['parent_shards']}")
    print(f"Child shards: {status['child_shards']}")

    # Check parent-child relationships
    for parent, children in status['topology']['parent_child_map'].items():
        print(f"Parent {parent} has children: {children}")
```

2. **Force shard refresh:**
```python
# Manually trigger shard discovery (usually not needed)
consumer._resharding_manager._last_refresh = 0
await consumer._resharding_manager.refresh_shards()
```

3. **Monitor resharding events:**
```python
# Log when resharding is detected
async for record in consumer:
    status = consumer.get_shard_status()
    if status['parent_shards'] > 0:
        print(f"Resharding in progress: {status['parent_shards']} parent shards")
```

## Memory Issues

### Problem: High memory usage or OOM errors

**Solutions:**

1. **Limit queue sizes:**
```python
# Producer
async with Producer(
    stream_name="test",
    max_queue_size=1000,  # Smaller queue
    batch_size=100        # Smaller batches
) as producer:
    # ...

# Consumer
async with Consumer(
    stream_name="test",
    max_queue_size=1000,  # Smaller queue
    record_limit=100      # Fewer records per fetch
) as consumer:
    # ...
```

2. **Process records immediately:**
```python
# Don't accumulate records in memory
async for record in consumer:
    # Process immediately
    await process_record(record)

    # Avoid this pattern:
    # records.append(record)  # Don't accumulate!
```

3. **Use streaming processors:**
```python
# For large records, process as streams
async def process_large_record(record):
    # Process in chunks rather than loading entire record
    if 'large_data' in record:
        data = record['large_data']
        for chunk in chunks(data, 1000):
            await process_chunk(chunk)
```

## Error Messages Reference

### Common Kinesis Errors

| Error | Meaning | Solution |
|-------|---------|----------|
| `ProvisionedThroughputExceededException` | Rate limit exceeded | Reduce rate, add shards, or implement backoff |
| `ResourceNotFoundException` | Stream doesn't exist | Create stream or check name/region |
| `ResourceInUseException` | Stream being modified | Wait for stream to be active |
| `ExpiredIteratorException` | Shard iterator expired | Consumer will auto-retry, check for long processing |
| `InvalidArgumentException` | Invalid parameter | Check record size (<1MB), partition key (<256 bytes) |
| `AccessDeniedException` | IAM permissions issue | Update IAM policy |
| `LimitExceededException` | AWS account limit | Request limit increase from AWS |

### async-kinesis Specific Issues

| Issue | Symptom | Solution |
|-------|---------|----------|
| Queue full | `put()` blocks | Increase `max_queue_size` or reduce input rate |
| No shards allocated | Consumer idle | Check `max_shard_consumers` setting |
| Checkpointer conflict | Missing messages | Use unique checkpointer names |
| High latency | Slow message delivery | Reduce `buffer_time`, optimize processor |

## Debug Techniques

### Enable verbose logging:
```python
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Set specific loggers
logging.getLogger('kinesis').setLevel(logging.DEBUG)
logging.getLogger('botocore').setLevel(logging.INFO)
```

### Monitor metrics:
```python
# Producer metrics
async def monitor_producer(producer):
    while True:
        queue_size = producer._queue.qsize()
        print(f"Producer queue size: {queue_size}")
        await asyncio.sleep(10)

# Consumer metrics
async def monitor_consumer(consumer):
    while True:
        status = consumer.get_shard_status()
        print(f"Consumer status: {status}")
        await asyncio.sleep(30)
```

### Test connectivity:
```python
async def test_kinesis_connection():
    try:
        async with Producer(
            stream_name="test-connection",
            create_stream=True,
            create_stream_shards=1
        ) as producer:
            await producer.put({"test": "connection"})
            print("✓ Successfully connected to Kinesis")
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        raise
```
