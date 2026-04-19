# Changelog

## Unreleased

### Fixed
- `RedisCheckPointer.manual_checkpoint()` and `DynamoDBCheckPointer.manual_checkpoint()`
  no longer drop buffered checkpoints on a mid-loop exception. Previously, both
  cleared `_manual_checkpoints` before iterating, so if the backend raised for
  shard N, the entries for shards N+1..end were evicted and never retried.
  Shards are now popped individually after a successful backend write, so a
  raise leaves the unflushed remainder buffered for the next flush. The shard
  that tripped also stays buffered (it was not successfully persisted), so
  callers that retry will re-attempt it alongside the rest. Fixes
  [#76](https://github.com/hampsterx/async-kinesis/issues/76).
- `consumer_checkpoint_success_total` / `_failure_total` now count actual backend
  persist operations. Previously, under `auto_checkpoint=False` (Redis / DynamoDB
  manual mode), the consumer-side wrapper incremented success on every
  `checkpoint()` call even though the backend write was deferred to
  `manual_checkpoint()`. The counters now track durable writes, which means they
  stay flat between flushes in manual mode and spike when the user flushes. Metric
  label set unchanged (`{stream_name, shard_id}`); checkpointers used without a
  Consumer appear under a new sentinel `stream_name="<standalone>"`.

### Backwards compatibility
- **Shared checkpointer across Consumers** (`BaseCheckPointer` subclasses only):
  reusing a single `MemoryCheckPointer` / `RedisCheckPointer` / `DynamoDBCheckPointer`
  instance across multiple `Consumer`s with different `stream_name` or different
  `metrics_collector` now raises `RuntimeError` at the second Consumer's construction
  (from `bind_metrics`). Idempotent rebind with matching args is still a no-op.
  Custom checkpointers that conform to the `CheckPointer` Protocol without
  implementing `bind_metrics` are unaffected (they silently opt out of checkpoint
  metrics, same as before). If you relied on sharing a built-in checkpointer,
  give each Consumer its own instance.
- **Metric cadence in manual mode**: `consumer_checkpoint_success_total` no
  longer ticks on every per-shard `checkpoint()` call under `auto_checkpoint=False`.
  It stays flat between `manual_checkpoint()` flushes and spikes in bursts on
  flush. Dashboards using `rate(consumer_checkpoint_success_total[5m])` will
  show quiet periods followed by spikes, which is the correct signal (durable
  writes actually happen in bursts).
- **New sentinel label value** (`BaseCheckPointer` subclasses only): built-in
  checkpointers constructed without a Consumer now default to
  `stream_name="<standalone>"` until `bind_metrics` is called. Dashboards
  filtering on specific stream names are unaffected; wildcard queries will see
  the new value. Custom Protocol-only checkpointers emit whatever labels they
  always did.

### Internal
- Removed `Consumer._checkpoint_with_metrics` (private, added in v2.5.0).
  Callers invoke `checkpointer.checkpoint()` directly; emission now lives
  on `BaseCheckPointer` via `_emit_checkpoint_success` / `_emit_checkpoint_failure`.
- CI and local docker-compose now run Floci 1.5.4 (was: Floci 1.4.0 in CI,
  kinesalite in docker-compose). Local dev port is 4566 (was 4567 for kinesalite).
  Re-enables the `test_producer_consumer_with_stream_arn` integration test
  (Floci 1.5.2+ resolves stream names from `StreamARN`) and the
  `consumer_iterator_age_milliseconds` assertion in
  `test_consumer_metrics_round_trip` (Floci 1.5.4 returns time-based
  `MillisBehindLatest`). No runtime behaviour change for library consumers.
- Added `BaseCheckPointer.bind_metrics(collector, labels)` so Consumer can inject
  its `stream_name` label into the checkpointer's emissions at construction time.
  Intentionally not added to the `CheckPointer` Protocol to avoid breaking static
  type-checking for exotic user checkpointers; Consumer uses `hasattr()` to wire
  metrics only on implementations that provide it.
