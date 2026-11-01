# Resharding Tests

Comprehensive test suite for Kinesis stream resharding scenarios.

## Quick Start

```bash
# Run core tests (no dependencies)
python tests/resharding/test_resharding_simple.py
python tests/resharding/test_resharding_detection.py

# Run with Floci
docker-compose up kinesis
export AWS_DEFAULT_REGION=ap-southeast-2 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
pytest tests/resharding/test_split_shard.py
python tests/resharding/test_resharding_integration.py

# Test with real AWS, in a shell without the Floci exports (dry run first!)
python tests/resharding/resharding_test.py --dry-run --scenario scale-up-small
```

## Test Files

| File | Type | Dependencies | Description |
|------|------|--------------|-------------|
| `test_resharding_simple.py` | Unit | None | Core shard topology logic |
| `test_resharding_detection.py` | Unit | None | Resharding detection algorithms |
| `test_split_shard.py` | Integration | Floci | Real `SplitShard`, read back through `Consumer` |
| `test_resharding_integration.py` | Integration | Floci | E2E with real streams |
| `test_resharding_in_progress.py` | Integration | Floci | Status handling and shard discovery. Runs no real reshard, so the stream stays `ACTIVE` |
| `test_start_during_resharding.py` | Integration | Floci | Startup with simulated topology. Runs no real reshard, so the stream stays `ACTIVE` |
| `resharding_test.py` ⭐ | Production | AWS | **Real AWS resharding tool** |

## Key Scenarios Tested

✅ **Core Functionality**
- Parent-child shard topology management
- AWS best practice: consume parents before children
- Dynamic shard discovery and closed shard handling

✅ **Edge Cases**
- Consumer starting during active resharding
- Producer resilience during topology changes
- Stream status transitions (UPDATING, CREATING, etc.)
- Rapid start/stop cycles and concurrent operations

✅ **Performance**
- ~730 msg/s throughput maintained during resharding
- Sub-second topology detection
- Zero resource leaks

## Production AWS Tool

The `resharding_test.py` tool performs real resharding operations:

```bash
# Safe dry run
python tests/resharding/resharding_test.py --dry-run

# Real resharding (creates AWS resources)
python tests/resharding/resharding_test.py --scenario scale-up-small  # 1→3 shards
python tests/resharding/resharding_test.py --scenario scale-down      # 3→1 shards
python tests/resharding/resharding_test.py --all-scenarios
```

⚠️ **Use with caution** - creates and modifies real AWS Kinesis streams.
