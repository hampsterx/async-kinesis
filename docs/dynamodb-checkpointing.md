# DynamoDB Checkpointing Guide

This guide covers how to use DynamoDB for checkpointing in distributed Kinesis consumer applications.

## Table of Contents
- [Overview](#overview)
- [Installation](#installation)
- [Basic Usage](#basic-usage)
- [Configuration Options](#configuration-options)
- [Table Management](#table-management)
- [Monitoring & Debugging](#monitoring--debugging)
- [Cost Optimization](#cost-optimization)
- [Migration from Redis](#migration-from-redis)

## Overview

The DynamoDB checkpointer provides a serverless, fully-managed solution for coordinating multiple Kinesis consumers. It offers:

- **Automatic table creation** with proper schema and TTL configuration
- **Atomic operations** for safe concurrent access across consumers
- **Pay-per-request pricing** - no idle costs
- **Built-in TTL** for automatic cleanup of old records
- **No infrastructure** to manage compared to Redis

## Installation

```bash
pip install async-kinesis[dynamodb]
```

This installs the required `aioboto3` dependency for async DynamoDB operations.

## Basic Usage

### Simple Setup

```python
from kinesis import Consumer, DynamoDBCheckPointer

async with Consumer(
    stream_name="my-stream",
    checkpointer=DynamoDBCheckPointer(
        name="my-app"  # Used to generate table name
    )
) as consumer:
    async for record in consumer:
        print(record)
```

### Multiple Consumer Groups

```python
# Analytics consumer group
analytics_consumer = Consumer(
    stream_name="events",
    checkpointer=DynamoDBCheckPointer(
        name="analytics",
        table_name="kinesis-checkpoints"  # Share table across groups
    )
)

# Archival consumer group
archival_consumer = Consumer(
    stream_name="events",
    checkpointer=DynamoDBCheckPointer(
        name="archival",
        table_name="kinesis-checkpoints"  # Same table, different app name
    )
)
```

## Configuration Options

### Complete Configuration Example

```python
from kinesis import Consumer, DynamoDBCheckPointer

checkpointer = DynamoDBCheckPointer(
    name="my-app",                    # Application name (required)
    table_name="custom-table",        # Override default table name
    session_timeout=60,               # Seconds before shard is considered abandoned
    heartbeat_frequency=15,           # Seconds between heartbeat updates
    auto_checkpoint=True,             # Checkpoint automatically vs manually
    create_table=True,                # Create table if it doesn't exist
    endpoint_url=None,                # Custom endpoint (for LocalStack testing)
    region_name="us-east-1",          # AWS region
    ttl_hours=24,                     # Hours to retain old records
)
```

### Configuration Parameters

| Parameter | Default | Description |
| --- | --- | --- |
| **name** | Required | Application name used for table naming and consumer identification |
| **table_name** | `kinesis-checkpoints-{name}` | DynamoDB table name |
| **session_timeout** | 60 | Seconds before a shard lease expires |
| **heartbeat_frequency** | 15 | Seconds between heartbeat updates |
| **auto_checkpoint** | True | Whether to checkpoint automatically |
| **create_table** | True | Create table if it doesn't exist |
| **endpoint_url** | None | Custom DynamoDB endpoint (for testing) |
| **region_name** | AWS_DEFAULT_REGION or us-east-1 | AWS region |
| **ttl_hours** | 24 | Hours to retain checkpoint records after expiry |

## Table Management

### Automatic Table Creation

By default, the checkpointer creates a DynamoDB table with:
- **On-demand billing** - Pay only for what you use
- **TTL enabled** - Automatic cleanup of old records
- **Simple schema** - Just shard_id as the partition key

### Manual Table Creation

If you prefer to create the table manually (e.g., for specific tags or encryption):

```python
# Disable auto-creation
checkpointer = DynamoDBCheckPointer(
    name="my-app",
    create_table=False
)
```

Create the table with this schema:

```yaml
TableName: kinesis-checkpoints-my-app
KeySchema:
  - AttributeName: shard_id
    KeyType: HASH
AttributeDefinitions:
  - AttributeName: shard_id
    AttributeType: S
BillingMode: PAY_PER_REQUEST
TimeToLiveSpecification:
  Enabled: true
  AttributeName: ttl
```

### Table Structure

Each record in the table contains:

| Field | Type | Description |
| --- | --- | --- |
| **shard_id** | String | Kinesis shard ID (partition key) |
| **ref** | String | Consumer instance reference |
| **ts** | Number | Last update timestamp |
| **sequence** | String | Last checkpointed sequence number |
| **ttl** | Number | TTL timestamp for automatic deletion |

## Monitoring & Debugging

### View Table Contents

```python
import boto3

# Scan the table to see all checkpoints
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('kinesis-checkpoints-my-app')

response = table.scan()
for item in response['Items']:
    print(f"Shard: {item['shard_id']}")
    print(f"  Owner: {item.get('ref', 'None')}")
    print(f"  Sequence: {item.get('sequence', 'None')}")
    print(f"  Last Update: {item.get('ts', 0)}")
```

### CloudWatch Metrics

Monitor your checkpointer performance with CloudWatch:

- **ConsumedReadCapacityUnits** - Read usage
- **ConsumedWriteCapacityUnits** - Write usage
- **SuccessfulRequestLatency** - Operation latency
- **SystemErrors** - DynamoDB errors

### Common Issues

**1. Conditional Check Failed**
```
ConditionalCheckFailedException
```
This is normal - it means another consumer owns the shard. The checkpointer handles this automatically.

**2. Table Not Found**
```
ResourceNotFoundException
```
Set `create_table=True` or create the table manually.

**3. Throttling**
```
ProvisionedThroughputExceededException
```
This shouldn't happen with on-demand billing. If it does, check your AWS limits.

## Cost Optimization

### Understanding Costs

With on-demand pricing, you pay per request:
- **Write requests**: $1.25 per million
- **Read requests**: $0.25 per million

### Typical Usage Pattern

For a stream with 10 shards and 5 consumers:
- Heartbeats: 10 shards × 4/min × 60 min × 24 hours = ~58K writes/day
- Checkpoints: Depends on throughput, typically much less than heartbeats
- Total daily cost: ~$0.07/day

### Optimization Tips

1. **Increase heartbeat frequency** only if you need faster failover:
   ```python
   # Slower heartbeat = lower cost
   checkpointer = DynamoDBCheckPointer(
       name="my-app",
       heartbeat_frequency=30,  # Every 30 seconds instead of 15
       session_timeout=120      # 2 minutes timeout
   )
   ```

2. **Use manual checkpointing** for batch processing:
   ```python
   checkpointer = DynamoDBCheckPointer(
       name="batch-processor",
       auto_checkpoint=False
   )

   # Process batch then checkpoint once
   batch = []
   async for record in consumer:
       batch.append(record)
       if len(batch) >= 1000:
           process_batch(batch)
           await checkpointer.manual_checkpoint()
           batch = []
   ```

3. **Share tables** across related applications to reduce operational overhead

## Migration from Redis

### Comparison

| Feature | Redis | DynamoDB |
| --- | --- | --- |
| **Setup** | Requires Redis cluster | No infrastructure |
| **Cost** | Fixed (instance hours) | Pay-per-request |
| **Maintenance** | Updates, backups, scaling | Fully managed |
| **Availability** | Depends on setup | 99.999% SLA |
| **Performance** | Sub-millisecond | Single-digit milliseconds |

### Migration Steps

1. **Deploy with DynamoDB** alongside Redis:
   ```python
   # Temporary: Run both checkpointers
   if use_dynamodb:
       checkpointer = DynamoDBCheckPointer(name="my-app")
   else:
       checkpointer = RedisCheckPointer(name="my-app")
   ```

2. **Test with a subset** of consumers first

3. **Monitor performance** and costs for a few days

4. **Cut over** remaining consumers

5. **Decommission Redis** once stable

### Feature Comparison

Both checkpointers support:
- Distributed coordination
- Heartbeat-based leases
- Manual checkpointing
- Session timeouts

DynamoDB advantages:
- No infrastructure management
- Automatic scaling
- Built-in backup and recovery
- Global tables for multi-region

Redis advantages:
- Lower latency (sub-millisecond)
- Fixed costs if already using Redis
- More familiar for some teams

## Production Best Practices

1. **Use consistent naming** for table organization:
   ```python
   # Good: Environment-specific tables
   table_name = f"kinesis-checkpoints-{environment}-{app_name}"
   ```

2. **Set appropriate timeouts** based on processing time:
   ```python
   # For fast processing
   fast_processor = DynamoDBCheckPointer(
       name="realtime",
       session_timeout=30,
       heartbeat_frequency=10
   )

   # For batch processing
   batch_processor = DynamoDBCheckPointer(
       name="batch",
       session_timeout=300,  # 5 minutes
       heartbeat_frequency=60
   )
   ```

3. **Monitor table metrics** in CloudWatch for anomalies

4. **Use tags** for cost allocation:
   ```python
   # If creating table manually, add tags
   table.tag_resource(
       ResourceArn=table_arn,
       Tags=[
           {'Key': 'Application', 'Value': 'my-app'},
           {'Key': 'Environment', 'Value': 'production'},
       ]
   )
   ```

5. **Enable point-in-time recovery** for critical applications

## Testing with LocalStack

For local development and testing:

```python
checkpointer = DynamoDBCheckPointer(
    name="test-app",
    endpoint_url="http://localhost:4566",  # LocalStack endpoint
    region_name="us-east-1"
)
```

## Next Steps

- Review [Consumer Configuration](../README.md#consumer) for more options
- See [Multi-Consumer Patterns](./common-patterns.md#multi-consumer-processing)
- Check [Monitoring Guide](./common-patterns.md#production-monitoring)
