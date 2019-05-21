# async-kinesis

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black)

[![PyPI version](https://badge.fury.io/py/async-kinesis.svg)](https://badge.fury.io/py/async-kinesis)


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

## Consumer Design

(Bears some explanation, kinda complex~)

- fetch() gets called periodically (0.2 sec (ie max 5x per second as is the limit on shard get_records()))
  - iterate over the list of shards (set on startup, does not currently detect resharding)
    - assign shard if not in use and not at "max_shard_consumers" limit otherwise ignore/continue
    - ignore/continue if this shard is still fetching
    - process records if shard is done fetching
        - put records on queue
        - add checkpoint record to queue
        - assign NextShardIterator
    - create (get_records()) task again

Note that get_records() is throttled via "shard_fetch_rate=5" (ie the same 0.2 sec/ 5x limit)

This pattern seemed like the easiest way to maintain a pool of consumers without needing to think too hard about starting it's next job or handling new shards etc.


## Not Implemented

- resharding
- client rebalancing (ie share the shards between consumers)

See also

https://aws.amazon.com/blogs/big-data/implementing-efficient-and-reliable-producers-with-the-amazon-kinesis-producer-library/


## Producer
    
    async with Producer(stream_name="test") as producer:
        # Put item onto queue to be flushed via put_records()
        await producer.put({'my': 'data'})


Options:

(comments in quotes are Kinesis Limits as per AWS Docs)

* region_name
    > AWS Region

* buffer_time=0.5
    > Buffer time in seconds before auto flushing records
    
* put_rate_limit_per_shard=1000
    > "A single shard can ingest up to 1 MiB of data per second (including partition keys) or 1,000 records per second for writes

* batch_size=500
    > "Each PutRecords request can support up to 500 records"    

* max_queue_size=10000
   > put() method will block when queue is at max 
   
* after_flush_fun
   > async function to call after doing a flush (err put_records()) call

* aggregator=JsonWithoutAggregation()
   > Record aggregator. Default is JSON without aggregation
   > Note this is highly inefficient as each record can be up to 1Mib

## Consumer

    async with Consumer(stream_name="test") as consumer:
        while True:
            async for item in consumer:
                print(item)
            # caught up.. take a breather~


Options:

(comments in quotes are Kinesis Limits as per AWS Docs)

* region_name
    > AWS Region

* max_queue_size=1000
    > the fetch() task shard 

* max_shard_consumers=None
    > Max number of shards to use. None = all
    
* record_limit=10000
    > Number of records to fetch with get_records()

* sleep_time_no_records=2
    > No of seconds to sleep when caught up
    
* iterator_type="TRIM_HORIZON"
    > Default shard iterator type for new/unknown shards (ie start from start of stream)
    > Alternative is "LATEST" (ie end of stream)

* shard_fetch_rate=5
    > No of fetches per second (max = 5)           

* checkpointer=MemoryCheckPointer()
    > Checkpointer to use

* aggregator=JsonWithoutAggregation()
   > Record aggregator. Must Match aggregator used by Producer()


## Checkpointers

- memory
- redis


## Yet another Python Kinesis Library?

Sadly I had issues with every other library I could find :(

* https://github.com/NerdWalletOSS/kinesis-python
    * pro:
        * kinda works
    * con
        * threaded
        * Outstanding PR to fix some issues
        * checkpoints on every record on main thread

* https://github.com/ungikim/kinsumer
    * pro:
        * handles shard changes
        * no producer
        * no redis checkpointer/heartbeat
        * threaded/seems kinda complicated~
    * con
        * consumer only
         
* https://github.com/bufferapp/kiner
    * pro:
        * Batching
    * con
        * Producer only

* https://github.com/niklio/aiokinesis
    * pro:
        * asyncio
        * no checkpointing
    * con
        * limited to 1 shard / too simplistic

* https://github.com/ticketea/pynesis
    * pro:
        * checkpoints
    * con
        * hasn't been updated for 1 year
        * doesnt use put_records()
        * single threaded / round robin reads shards

* https://github.com/whale2/async-kinesis-client
    * pro:
        * checkpoints
        * asyncio
    * con
        * ?

(Actually I only found this one recently, might be ok alternative?)




