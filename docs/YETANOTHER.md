
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
