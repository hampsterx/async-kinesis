# async-kinesis

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black) [![PyPI version](https://badge.fury.io/py/async-kinesis.svg)](https://badge.fury.io/py/async-kinesis) [![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3100/) [![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/) [![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)

```
pip install async-kinesis
```

## Features

- uses queues for both producer and consumer
  - producer flushes with put_records() if has enough to flush or after "buffer_time" reached
  - consumer iterates over msg queue independent of shard readers
- Configurable to handle Sharding limits but will throttle/retry if required
  - ie multiple independent clients are saturating the Shards
- Checkpointing with heartbeats
  - deadlock + reallocation of shards if checkpoint fails to heartbeat within "session_timeout"
- processors (aggregator + serializer)
    - json line delimited, msgpack
- Address Kinesis streams by name or [ARN]

See [docs/design](./docs/DESIGN.md) for more details.
See [docs/yetanother](docs/YETANOTHER.md) as to why reinvent the wheel.

## Environment Variables

As required by boto3

```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
```

## Stream addressing

The `stream_name` argument to consumer and producer accepts either the [StreamName]
like `test`, or a full [StreamARN] like `arn:aws:kinesis:eu-central-1:842404475894:stream/test`.

## Producer

    from kinesis import Producer

    async with Producer(stream_name="test") as producer:
        # Put item onto queue to be flushed via put_records()
        await producer.put({'my': 'data'})


Options:

(comments in quotes are Kinesis Limits as per AWS Docs)

| Arg | Default | Description |
| --- | --- | --- |
| session | None | AioSession (to use non default profile etc) |
| region_name | None | AWS Region |
| buffer_time | 0.5 | Buffer time in seconds before auto flushing records |
| put_rate_limit_per_shard | 1000 | "A single shard can ingest up to 1 MiB of data per second (including partition keys) or 1,000 records per second for writes" |
| put_bandwidth_limit_per_shard | 1024 | Kb per sec. max is 1024 per shard (ie 1 MiB). Keep below to minimize ProvisionedThroughputExceeded" errors * |
| batch_size | 500 | "Each PutRecords request can support up to 500 records" |
| max_queue_size | 10000 | put() method will block when queue is at max |
| after_flush_fun | None | async function to call after doing a flush (err put_records()) call |
| processor | JsonProcessor() | Record aggregator/serializer. Default is JSON without aggregation. Note this is highly inefficient as each record can be up to 1Mib |
| retry_limit | None | How many connection attempts should be made before raising a exception |
| expo_backoff | None | Exponential Backoff when connection attempt fails |
| expo_backoff_limit | 120 | Max amount of seconds Exponential Backoff can grow |
| create_stream | False | Creates a Kinesis Stream based on the `stream_name` keyword argument. Note if stream already existing it will ignore |
| create_stream_shards | 1 | Sets the amount of shard you want for your new stream. Note if stream already existing it will ignore  |

* Throughput exceeded. The docs (for Java/KPL see: https://docs.aws.amazon.com/streams/latest/dev/kinesis-producer-adv-retries-rate-limiting.html) state:

> You can lower this limit to reduce spamming due to excessive retries. However, the best practice is for each producer is to retry for maximum throughput aggressively and to handle any resulting throttling determined as excessive by expanding the capacity of the stream and implementing an appropriate partition key strategy.

Even though our default here is to limit at this threshold (1024kb) in reality the threshold seems lower (~80%).
If you wish to avoid excessive throttling or have multiple producers on a stream you will want to set this quite a bit lower.


## Consumer

    from kinesis import Consumer

    async with Consumer(stream_name="test") as consumer:
        while True:
            async for item in consumer:
                print(item)
            # caught up.. take a breather~


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
| create_stream | False | Creates a Kinesis Stream based on the `stream_name` keyword argument. Note if stream already existing it will ignore |
| create_stream_shards | 1 | Sets the amount of shard you want for your new stream. Note if stream already existing it will ignore  |
| timestamp | None | Timestamp to start reading stream from. Used with iterator type "AT_TIMESTAMP"


## Checkpointers

- memory (the default but kinda pointless)

```
    MemoryCheckPointer()
```

- redis

```
    RedisCheckPointer(name, session_timeout=60, heartbeat_frequency=15, is_cluster=False)
```

Requires ENV:

```
    REDIS_HOST
```

Requires `pip install aredis`


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


## Unit Testing

Uses https://github.com/mhart/kinesalite for local testing.

Run tests via docker

```
docker-compose up --abort-on-container-exit --exit-code-from test
```

For local testing use

```
docker-compose up kinesis redis
```

then within your virtualenv

```
nosetests

# or run individual test
nosetests tests.py:KinesisTests.test_create_stream_shard_limit_exceeded
```

Note there are a few test cases using the *actual* AWS Kinesis (AWSKinesisTests)
These require setting an env in order to run

Create an ".env" file with

```
TESTING_USE_AWS_KINESIS=1
```

Note you can ignore these tests if submitting PR unless core batching/processing behaviour is being changed.


[ARN]: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html
[StreamARN]: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StreamDescription.html#Streams-Type-StreamDescription-StreamARN
[StreamName]: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StreamDescription.html#Streams-Type-StreamDescription-StreamName
