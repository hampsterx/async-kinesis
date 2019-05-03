# async-kinesis

Features:

- uses queues for both producer and consumer
  - producer flushes with put_records() after "buffer_time" reached
  - consumer iterates over msg queue independent of shard readers
- Configurable to handle Sharding limits but will throttle/retry if required
  - ie multiple independent clients are saturating the Shards
- Checkpointing with heartbeats
  - deadlock + reallocation of shards if checkpoint fails to heartbeat within "session_timeout"

See https://aws.amazon.com/blogs/big-data/implementing-efficient-and-reliable-producers-with-the-amazon-kinesis-producer-library/


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
   
* after_flush_fun=None,
   > async function to call after doing a flush (err put_records()) call
                 

## Consumer

    async with Consumer(stream_name="test") as consumer:
        while True:
            async for item in consumer:
                print(item)
            # caught up.. take a breather~


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




