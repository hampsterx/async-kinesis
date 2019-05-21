# async-kinesis

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black) [![PyPI version](https://badge.fury.io/py/async-kinesis.svg)](https://badge.fury.io/py/async-kinesis)

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
- aggregators
    - json line delimited, msgpack


See [docs/design](./docs/DESIGN.md) for more details.
See [docs/yetanother](docs/YETANOTHER.md) as to why reinvent the wheel.

## Environment Variables

As required by boto3

```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
```

## Producer
    
    async with Producer(stream_name="test") as producer:
        # Put item onto queue to be flushed via put_records()
        await producer.put({'my': 'data'})


Options:

(comments in quotes are Kinesis Limits as per AWS Docs)

| Arg | Default | Description |
| --- | --- | --- |
| region_name | None | AWS Region |
| buffer_time | 0.5 | Buffer time in seconds before auto flushing records |
| put_rate_limit_per_shard | 1000 | "A single shard can ingest up to 1 MiB of data per second (including partition keys) or 1,000 records per second for writes" |
| batch_size | 500 | "Each PutRecords request can support up to 500 records" |
| max_queue_size | 10000 | put() method will block when queue is at max |
| after_flush_fun | None | async function to call after doing a flush (err put_records()) call |
| aggregator | JsonWithoutAggregation() | Record aggregator. Default is JSON without aggregation. Note this is highly inefficient as each record can be up to 1Mib |


## Consumer

    async with Consumer(stream_name="test") as consumer:
        while True:
            async for item in consumer:
                print(item)
            # caught up.. take a breather~


Options:

(comments in quotes are Kinesis Limits as per AWS Docs)


| Arg | Default | Description |
| --- | --- | --- |
| region_name | None | AWS Region |
| max_queue_size | 1000 | the fetch() task shard will block when queue is at max |
| max_shard_consumers | None | Max number of shards to use. None = all |
| record_limit | 10000 | Number of records to fetch with get_records() |
| sleep_time_no_records | 2 | No of seconds to sleep when caught up |
| iterator_type | TRIM_HORIZON | Default shard iterator type for new/unknown shards (ie start from start of stream). Alternative is "LATEST" (ie end of stream) |
| shard_fetch_rate | 1 | No of fetches per second (max = 5). 1 is recommended as allows having multiple consumers without hitting the max limit. |
| checkpointer | MemoryCheckPointer() | Checkpointer to use |
| aggregator | JsonWithoutAggregation() |  Record aggregator. Must Match aggregator used by Producer() |


## Checkpointers

- memory (the default but kinda pointless)

```
    MemoryCheckPointer()
```

- redis

```
    RedisCheckPointer(name, session_timeout=60, heartbeat_frequency=15)
```

Requires ENV:

```
    REDIS_HOST
```

## Aggregators


Aggregators enable batching up multiple records to more efficiently use the stream.
Refer https://aws.amazon.com/blogs/big-data/implementing-efficient-and-reliable-producers-with-the-amazon-kinesis-producer-library/


| Class | Args | Description |
| --- | --- | --- |
| StringWithoutAggregation | None | Single String record |
| JsonWithoutAggregation | None | Single JSON record |
| JsonLineAggregation | None | Multiple JSON record separated by new line
| MsgPackAggregation | None | Multiple Msgpack record separated by 4 byte (size) header 


## Benchmark/Example

See [examples/benchmark.py](./examples/benchmark.py) for code

todo

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

// or run individual test
nosetests tests.py:KinesisTests.test_create_stream_shard_limit_exceeded
```

Note there are a few test cases using the *actual* AWS Kinesis (AWSKinesisTests)
These require setting an env in order to run

Create an ".env" file with

```
TESTING_USE_AWS_KINESIS=1
```

Note you can ignore these tests if submitting PR (unlikely to be affected)


