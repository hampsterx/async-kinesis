# Changelog

## Unreleased

### Fixed
- `consumer_checkpoint_success_total` / `_failure_total` now count actual backend
  persist operations. Previously, under `auto_checkpoint=False` (Redis / DynamoDB
  manual mode), the consumer-side wrapper incremented success on every
  `checkpoint()` call even though the backend write was deferred to
  `manual_checkpoint()`. The counters now track durable writes, which means they
  stay flat between flushes in manual mode and spike when the user flushes. Metric
  label set unchanged (`{stream_name, shard_id}`); checkpointers used without a
  Consumer appear under a new sentinel `stream_name="<standalone>"`.

### Backwards compatibility
- **Shared checkpointer across Consumers**: reusing a single `CheckPointer`
  instance across multiple `Consumer`s with different `stream_name` or different
  `metrics_collector` now raises `RuntimeError` at the second Consumer's
  construction (from `BaseCheckPointer.bind_metrics`). Idempotent rebind with
  matching args is still a no-op. If you relied on sharing a checkpointer, give
  each Consumer its own instance.
- **Metric cadence in manual mode**: `consumer_checkpoint_success_total` no
  longer ticks on every per-shard `checkpoint()` call under `auto_checkpoint=False`.
  It stays flat between `manual_checkpoint()` flushes and spikes in bursts on
  flush. Dashboards using `rate(consumer_checkpoint_success_total[5m])` will
  show quiet periods followed by spikes, which is the correct signal (durable
  writes actually happen in bursts).
- **New sentinel label value**: checkpointers constructed without a Consumer
  default to `stream_name="<standalone>"` until `bind_metrics` is called.
  Dashboards filtering on specific stream names are unaffected; wildcard
  queries will see the new value.

### Internal
- Removed `Consumer._checkpoint_with_metrics` (private, added in v2.5.0).
  Callers invoke `checkpointer.checkpoint()` directly; emission now lives
  on `BaseCheckPointer` via `_emit_checkpoint_success` / `_emit_checkpoint_failure`.
- Added `BaseCheckPointer.bind_metrics(collector, labels)` so Consumer can inject
  its `stream_name` label into the checkpointer's emissions at construction time.
  Intentionally not added to the `CheckPointer` Protocol to avoid breaking static
  type-checking for exotic user checkpointers; Consumer uses `hasattr()` to wire
  metrics only on implementations that provide it.
