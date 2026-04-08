

# async-kinesis Design

## Consumer Architecture

The consumer uses an async task-based design optimized for handling multiple shards efficiently.

### Core Loop Design

- **fetch()** gets called periodically (0.2 sec intervals, respecting the 5 req/sec limit per shard)
  - Iterates over all known shards in the stream
  - **Dynamic shard discovery**: Refreshes shard list every 60 seconds to detect resharding
  - **Smart allocation**: Only assigns shards when under `max_shard_consumers` limit and following parent-child ordering
  - **Async processing**: Each shard uses independent async tasks for `get_records()` calls
  - **Queue-based**: Records flow through asyncio queues for non-blocking iteration

### Shard Management

- **Topology tracking**: Maintains parent-child relationships from AWS shard metadata
- **Exhaustion handling**: Tracks when parent shards are fully consumed to enable child consumption
- **Expired parent detection**: Automatically recognises parent shards that have been removed from the shard list by AWS (retention expired) and allows their children to be allocated immediately
- **Closed shard detection**: Gracefully handles shards that reach end-of-life during resharding
- **Error recovery**: Automatic retry with exponential backoff for connection failures and expired iterators

### Resharding Support

Unlike earlier versions, the consumer now fully supports dynamic resharding:

- **Proactive discovery**: Detects new child shards appearing during resharding operations
- **AWS best practices**: Enforces parent-before-child consumption ordering
- **Seamless transitions**: Maintains consumption continuity during shard splits and merges
- **Operational visibility**: Provides detailed status reporting for monitoring resharding events

### Throttling & Rate Limiting

- **Per-shard throttling**: Respects AWS limits (5 requests/sec per shard) via throttler objects
- **Backoff strategies**: Exponential backoff for throughput exceeded exceptions
- **Queue flow control**: Configurable queue sizes prevent memory exhaustion

## Producer Architecture

- **Batching strategy**: Accumulates records up to `batch_size` (max 500) or `buffer_time` timeout
- **Efficient flushing**: Uses `put_records()` for batch uploads to maximize throughput
- **Rate limiting**: Configurable per-shard bandwidth and record rate limits
- **Async buffering**: Non-blocking `put()` operations with configurable queue sizes

### Checkpoint Safety

Checkpoints use a **deferred execution** model to prevent data loss:

1. **Deferred commit**: When a `__CHECKPOINT__` sentinel is dequeued from the internal queue, it is stored as pending but not committed. The checkpoint only fires at the start of the *next* `__anext__()` call, proving the user's code survived processing the preceding records. If the consumer crashes between receiving a record and calling `__anext__()` again, the checkpoint is never committed and records replay on restart (at-least-once).

2. **Queue put timeout**: If enqueueing a parsed record times out (bounded queue full for 30s), `LastSequenceNumber` only advances to the last fully-enqueued record. The remaining rows in the Kinesis batch are abandoned to prevent a non-contiguous sequence gap that would skip records on restart.

3. **Shard deallocation ordering**: When a shard iterator is exhausted (`NextShardIterator=None`), all pending checkpoints for that shard are flushed *before* `deallocate()` releases ownership. No checkpoint sentinel is enqueued for the terminal batch (it would race with deallocation); instead, those records replay on restart. Checkpoint sentinels that were already queued before deallocation are silently skipped via a `_deallocated_shards` set.

4. **`checkpoint_interval` debouncing**: When set, checkpoint writes are buffered in `_pending_checkpoints` and flushed by a background task every N seconds, reducing backend write pressure. The flusher uses compare-and-delete to avoid dropping a newer sequence that arrives during the `await` on the checkpoint backend. On `close()`, deferred checkpoints are committed, the flusher is cancelled (triggering a final flush), and any remaining buffered checkpoints are flushed before the checkpointer is closed.

These guarantees hold under single-process asyncio concurrency. For multi-process coordination, the `CheckPointer` implementation (e.g. Redis with locking) must handle ownership contention.

## Integration Points

- **Checkpointing**: Pluggable checkpointer interface (Memory, Redis) for multi-consumer coordination
- **Processing**: Configurable aggregation and serialization via processor classes
- **Monitoring**: Rich status APIs for operational visibility and debugging

See also:
- [AWS Kinesis Producer Library Best Practices](https://aws.amazon.com/blogs/big-data/implementing-efficient-and-reliable-producers-with-the-amazon-kinesis-producer-library/)
- [AWS Kinesis Resharding Documentation](https://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-resharding.html)
