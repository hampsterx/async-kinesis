# Dogfood Soak Tool

`scripts/kinesis_dogfood.py` is a maintainer-only soak tool for exercising `async-kinesis` against a temporary Kinesis stream. It is not part of the public package API and it is not a pytest replacement.

Use it when you want a short pre-release smoke run, or when you need a repeatable harness for lifecycle issues that only show up with a live backend.

## Current Scope

The current implementation supports:

- Temporary stream creation and deletion
- `memory` and `dynamodb` checkpointers
- One or more producers and consumers in a single process
- In-process `InMemoryMetricsCollector`
- Warning and error capture from `kinesis*` loggers
- JSON and Markdown reports under `dogfood-reports/`
- `SIGINT` and `SIGTERM` cleanup before report writing
- AWS control-plane create, delete, and `UpdateShardCount`
- DynamoDB checkpoint table creation through `DynamoDBCheckPointer`
- DynamoDB checkpoint table deletion during cleanup
- Manual checkpoint mode for DynamoDB with flush error counts
- `--dry-run` plan reporting without creating resources
- `--cleanup-orphans` for old `async-kinesis-dogfood-*` streams and tables

Redis checkpointer support and issue scenario presets are not yet implemented. Passing unsupported options records an unsupported-option finding instead of silently pretending the path was exercised.

The default checkpointer is `memory` for `--backend floci` and `dynamodb` for `--backend aws`. Pass `--checkpointer` explicitly when you want the other path.

## Floci Smoke Run

Start Floci and Redis with the local development compose stack:

```bash
docker compose up -d kinesis redis
```

Then run:

```bash
ENDPOINT_URL=http://localhost:4566 \
AWS_DEFAULT_REGION=ap-southeast-2 \
AWS_ACCESS_KEY_ID=test \
AWS_SECRET_ACCESS_KEY=test \
python scripts/kinesis_dogfood.py \
  --backend floci \
  --duration-minutes 1 \
  --initial-shards 1 \
  --producers 1 \
  --consumers 1 \
  --checkpointer memory \
  --yes
```

The command writes:

```text
dogfood-reports/<run-id>.json
dogfood-reports/<run-id>.md
```

JSON is the source of truth. Markdown is a readable summary for quick review.

## AWS Soak Run

Use AWS mode for lifecycle and resharding behaviour that Floci cannot represent:

```bash
python scripts/kinesis_dogfood.py \
  --backend aws \
  --region us-east-1 \
  --duration-minutes 5 \
  --initial-shards 1 \
  --reshard-plan 2,1 \
  --producers 2 \
  --consumers 2 \
  --checkpointer dynamodb \
  --yes
```

AWS mode uses the standard credential chain. It creates a temporary stream named `async-kinesis-dogfood-<run-id>`. With `--checkpointer dynamodb`, it also uses a temporary table with the same name. Both are deleted during cleanup unless `--keep-stream` or `--keep-checkpoint-table` is passed.

Kinesis rejects direct shard-count jumps that more than double or more than halve the current open shard count. The tool keeps the requested `--reshard-plan` in `resharding_summary.requested_plan` and expands it into legal intermediate counts in `resharding_summary.execution_plan`. For example, `--initial-shards 1 --reshard-plan 3,1` executes `2,3,2,1`.

Run a plan without creating resources:

```bash
python scripts/kinesis_dogfood.py \
  --backend aws \
  --region us-east-1 \
  --duration-minutes 5 \
  --reshard-plan 2,1 \
  --checkpointer memory \
  --dry-run \
  --yes
```

Sweep old dogfood resources:

```bash
python scripts/kinesis_dogfood.py \
  --backend aws \
  --region us-east-1 \
  --cleanup-orphans \
  --orphan-min-age-minutes 60 \
  --yes
```

## Confirmation and Cleanup

The tool blocks unless `--yes` is passed. Without `--yes`, it prints the planned backend, region, stream name, checkpoint table name, duration, estimated cost, and cleanup contract, then exits without creating resources.

By default, created streams and checkpoint tables are deleted during cleanup. Pass `--keep-stream` or `--keep-checkpoint-table` only when you need to inspect resources after a failed run.

Interrupting a running soak with `Ctrl-C` sets `success=false`, adds a low-severity interruption finding, runs cleanup, and still writes both reports.

## Report Fields

Each JSON report includes:

- Run metadata: `run_id`, backend, region, stream, start and finish timestamps
- Resource cleanup state
- CLI config
- Producer and consumer summaries
- Checkpointer and resharding summaries
- In-memory metrics
- Captured log findings
- Harness findings with `critical`, `high`, `medium`, or `low` severity

A successful run should have `success=true` and no `critical` or `high` findings. A failed workload still writes partial producer, consumer, checkpointer, resharding, phase, and cleanup state when enough context exists.

## Floci Limitations

Green Floci means the harness can drive the library and clean up local resources. It does not prove AWS behaviour.

| Behaviour | Floci status | Report handling |
| --- | --- | --- |
| Temporary stream create/delete | Supported | Exercised by the Floci smoke run |
| Producer and consumer round trip | Supported | Exercised by the Floci smoke run |
| `UpdateShardCount` resharding | Unsupported in local smoke | Reported as a high run failure if forced through `--backend aws --endpoint-url` |
| DynamoDB checkpointer lifecycle | Not a substitute for AWS DynamoDB | Use AWS mode to validate the DynamoDB checkpointer |
| Real AWS parent shard expiry timing | Not represented | AWS-only behaviour |
| Real Kinesis connection idle drops | Not represented | AWS-only behaviour |
| Orphan stream/table sweep | Kinesis stream sweep works only for available local APIs | AWS mode is the intended target |

## Interpreting Failures

- `critical`: cleanup failed or the harness found data-loss evidence.
- `high`: the run failed, a required phase was unsupported, or consumers could not make progress.
- `medium`: sustained backend issues that deserve investigation.
- `low`: unsupported local behaviour, expected interruption, or other non-blocking observations.

When a soak finds a library bug, add a focused pytest regression where practical. The soak report should help reproduce and diagnose the issue, not stand in for deterministic coverage.
