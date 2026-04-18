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

### Internal
- Removed `Consumer._checkpoint_with_metrics` (private, added in v2.5.0 four days
  prior). Callers invoke `checkpointer.checkpoint()` directly; emission now lives
  on `BaseCheckPointer` via `_emit_checkpoint_success` / `_emit_checkpoint_failure`.
- Added `BaseCheckPointer.bind_metrics(collector, labels)` so Consumer can inject
  its `stream_name` label into the checkpointer's emissions at construction time.
  Intentionally not added to the `CheckPointer` Protocol to avoid breaking static
  type-checking for exotic user checkpointers; Consumer uses `hasattr()` to wire
  metrics only on implementations that provide it.
