

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


See also

https://aws.amazon.com/blogs/big-data/implementing-efficient-and-reliable-producers-with-the-amazon-kinesis-producer-library/

