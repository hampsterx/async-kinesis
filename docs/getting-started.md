# Getting Started with async-kinesis

This guide will walk you through setting up and using async-kinesis for the first time. We'll start with basic concepts and progressively build up to more advanced usage.

## Table of Contents
- [Prerequisites](#prerequisites)
- [What is AWS Kinesis?](#what-is-aws-kinesis)
- [Installation](#installation)
- [Setting Up AWS](#setting-up-aws)
- [Your First Producer](#your-first-producer)
- [Your First Consumer](#your-first-consumer)
- [Complete Working Example](#complete-working-example)
- [Next Steps](#next-steps)

## Prerequisites

Before you begin, you should have:
- Python 3.10 or higher installed (matches `setup.py` `python_requires`)
- Basic understanding of Python's async/await
- An AWS account (free tier is sufficient for testing)
- AWS credentials configured (we'll show you how)

## What is AWS Kinesis?

AWS Kinesis is a real-time data streaming service. Think of it as a highly scalable message queue where:
- **Producers** put data records into the stream
- **Consumers** read and process those records
- **Streams** are divided into **shards** (parallel lanes of data)
- Each record has a **partition key** that determines which shard it goes to

## Installation

```bash
pip install async-kinesis
```

## Setting Up AWS

### 1. Configure AWS Credentials

First, you need to set up your AWS credentials. The easiest way is using environment variables:

```bash
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"
export AWS_DEFAULT_REGION="us-east-1"  # or your preferred region
```

### 2. Create a Test Stream

You can create a stream using the AWS CLI:

```bash
aws kinesis create-stream --stream-name my-first-stream --shard-count 1
```

Or let async-kinesis create it for you (see examples below).

## Your First Producer

Let's start with a simple producer that sends messages to Kinesis:

```python
import asyncio
from kinesis import Producer

async def produce_messages():
    # Create a producer that will auto-create the stream if needed
    async with Producer(
        stream_name="my-first-stream",
        create_stream=True,  # Creates stream if it doesn't exist
        create_stream_shards=1  # Number of shards for new stream
    ) as producer:

        # Send a simple message
        await producer.put({"message": "Hello Kinesis!"})
        print("Sent first message!")

        # Send multiple messages
        for i in range(10):
            await producer.put({
                "message": f"Message number {i}",
                "timestamp": i
            })
            print(f"Sent message {i}")

        # Messages are automatically flushed when the producer closes

if __name__ == "__main__":
    asyncio.run(produce_messages())
```

## Your First Consumer

Now let's create a consumer to read those messages:

```python
import asyncio
from kinesis import Consumer

async def consume_messages():
    async with Consumer(
        stream_name="my-first-stream",
        # Start from the beginning of the stream
        iterator_type="TRIM_HORIZON"
    ) as consumer:

        print("Waiting for messages...")

        # Read messages as they arrive
        async for message in consumer:
            print(f"Received: {message}")

            # Consumer will wait for new messages after catching up

if __name__ == "__main__":
    asyncio.run(consume_messages())
```

## Complete Working Example

Here's a complete example that demonstrates producer and consumer working together.

Two ordering rules that the example below makes explicit:

1. **Create the stream before the consumer starts.** If the consumer reaches
   `async with Consumer(...)` before the stream exists, it raises. The producer's
   `create_stream=True` only takes effect on enter, so launching both with
   `asyncio.gather` is racy.
2. **With `iterator_type="LATEST"`, wait for the consumer to be ready before
   producing.** `LATEST` reads from records arriving *after* the iterator is
   obtained. If the producer publishes first, those events are silently missed.
   `Consumer.wait_ready()` blocks until shard iterators are in place.

```python
import asyncio
from datetime import datetime
from kinesis import Producer, Consumer

STREAM_NAME = "user-events"


async def setup_stream():
    """Pre-create the stream so the consumer doesn't race against creation."""
    async with Producer(
        stream_name=STREAM_NAME,
        create_stream=True,
        create_stream_shards=2,  # 2 shards for parallel processing
    ):
        pass  # entering the context creates the stream; exit is a no-op here


async def produce_events(ready: asyncio.Event):
    """Wait until the consumer is ready, then publish."""
    await ready.wait()

    async with Producer(stream_name=STREAM_NAME) as producer:
        users = ["alice", "bob", "charlie", "diana"]
        actions = ["login", "purchase", "view_item", "logout"]

        for i in range(20):
            user = users[i % len(users)]
            event = {
                "user_id": user,
                "action": actions[i % len(actions)],
                "timestamp": datetime.now().isoformat(),
                "value": i * 10,
            }
            # Use user_id as partition key so all of a user's events
            # land on the same shard (maintaining order).
            await producer.put(event, partition_key=user)
            print(f"Sent event: {event['user_id']} - {event['action']}")
            await asyncio.sleep(0.5)


async def process_events(ready: asyncio.Event):
    """Start the consumer and signal readiness once shard iterators are in place."""
    async with Consumer(
        stream_name=STREAM_NAME,
        iterator_type="LATEST",  # Only events arriving after we're ready
    ) as consumer:
        await consumer.wait_ready()  # safe to produce after this
        ready.set()
        print("Consumer ready, waiting for events...")

        async for event in consumer:
            user = event["user_id"]
            action = event["action"]
            value = event.get("value", 0)
            print(f"Processing: {user} performed {action} (value: ${value})")

            if action == "purchase":
                print(f"  Recording purchase of ${value} for {user}")
            elif action == "login":
                print(f"  User {user} logged in")


async def main():
    await setup_stream()  # create stream first
    ready = asyncio.Event()
    await asyncio.gather(produce_events(ready), process_events(ready))


if __name__ == "__main__":
    try:
        asyncio.run(asyncio.wait_for(main(), timeout=30))
    except asyncio.TimeoutError:
        print("\nDemo completed!")
```

## Common Issues and Solutions

### 1. Credentials Not Found
If you see `NoCredentialsError`, make sure your AWS credentials are set:
```python
# Option 1: Environment variables (recommended)
# Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY

# Option 2: Pass session explicitly
from aiobotocore.session import AioSession
session = AioSession(profile='my-profile')
async with Producer(stream_name="test", session=session) as producer:
    # ...
```

### 2. Stream Doesn't Exist
Either create the stream manually or use `create_stream=True`:
```python
async with Producer(
    stream_name="my-stream",
    create_stream=True,
    create_stream_shards=1
) as producer:
    # Stream will be created if it doesn't exist
```

### 3. Not Receiving Messages
Check your iterator type:
- `TRIM_HORIZON`: Start from the beginning
- `LATEST`: Start from new messages only
- `AT_TIMESTAMP`: Start from a specific time

### 4. Rate Limit Errors
If you see `ProvisionedThroughputExceededException`, you're sending too fast:
```python
async with Producer(
    stream_name="my-stream",
    # Reduce limits to avoid throttling
    put_rate_limit_per_shard=500,  # Default is 1000
    put_bandwidth_limit_per_shard=512  # Default is 1024 KB/s
) as producer:
    # ...
```

## Next Steps

Now that you understand the basics:

1. **Learn about Advanced Features:**
   - [Rate Limiting & Optimization](../README.md#rate-limiting--api-optimization)
   - [Custom Partition Keys](../README.md#custom-partition-keys)
   - [Different Processors](../README.md#processors-aggregator--serializer)

2. **Explore Production Topics:**
   - [Multi-Consumer Setup](../README.md#checkpointers)
   - [Resharding Support](../README.md#shard-management)
   - [Monitoring & Debugging](../README.md#monitoring--operational-visibility)

3. **Check out Examples:**
   - [Benchmark Script](../benchmark.py) - Performance testing
   - [Integration Tests](../tests/) - More usage patterns

## Getting Help

- Review the [Main Documentation](../README.md)
- Check [Architecture Details](./DESIGN.md)
- Report issues on [GitHub](https://github.com/yourrepo/async-kinesis)

Happy streaming! 🚀
