"""Tests for the in-memory testing utilities (kinesis.testing)."""

import asyncio

import pytest

from kinesis import StringProcessor
from kinesis.processors import JsonLineProcessor
from kinesis.testing import (
    MemoryShard,
    MemoryStream,
    MockConsumer,
    MockKinesisBackend,
    MockProducer,
    assert_records_delivered,
    assert_shard_ordering,
    collect_records,
    memory_stream,
)


@pytest.fixture(autouse=True)
def _reset_backend():
    """Reset the mock backend before and after every test."""
    MockKinesisBackend.reset()
    yield
    MockKinesisBackend.reset()


# ---------------------------------------------------------------------------
# MemoryShard
# ---------------------------------------------------------------------------


class TestMemoryShard:
    def test_put_returns_sequence_numbers(self):
        shard = MemoryShard("shard-0")
        seq1 = shard.put(b"hello", "pk1")
        seq2 = shard.put(b"world", "pk2")
        assert seq1 < seq2
        assert shard.record_count == 2

    def test_sequence_numbers_are_zero_padded(self):
        shard = MemoryShard("shard-0")
        seq = shard.put(b"data", "pk")
        assert len(seq) == 20
        assert seq == "00000000000000000001"

    def test_seal_adds_sentinel(self):
        shard = MemoryShard("shard-0")
        shard.put(b"data", "pk")
        shard.seal()
        assert len(shard.records) == 2
        assert shard.record_count == 1  # sentinel not counted


# ---------------------------------------------------------------------------
# MemoryStream
# ---------------------------------------------------------------------------


class TestMemoryStream:
    def test_single_shard(self):
        stream = MemoryStream("test", shard_count=1)
        stream.put(b"a", "pk1")
        stream.put(b"b", "pk2")
        assert stream.record_count == 2
        assert stream.shards[0].record_count == 2

    def test_multi_shard_routing(self):
        stream = MemoryStream("test", shard_count=4)
        # Put many records with different keys — should distribute across shards
        for i in range(100):
            stream.put(f"record-{i}".encode(), f"key-{i}")
        assert stream.record_count == 100
        # With 100 records and 4 shards, each shard should have some records
        counts = [s.record_count for s in stream.shards]
        assert all(c > 0 for c in counts), f"Shard distribution: {counts}"

    def test_same_partition_key_same_shard(self):
        stream = MemoryStream("test", shard_count=4)
        stream.put(b"a", "same-key")
        stream.put(b"b", "same-key")
        stream.put(b"c", "same-key")
        # All should be on the same shard
        non_empty = [s for s in stream.shards if s.record_count > 0]
        assert len(non_empty) == 1
        assert non_empty[0].record_count == 3

    def test_seal(self):
        stream = MemoryStream("test")
        stream.put(b"data", "pk")
        assert not stream.sealed
        stream.seal()
        assert stream.sealed

    def test_put_after_seal_raises(self):
        stream = MemoryStream("test")
        stream.seal()
        with pytest.raises(RuntimeError, match="sealed"):
            stream.put(b"data", "pk")

    def test_seal_is_idempotent(self):
        stream = MemoryStream("test")
        stream.seal()
        stream.seal()  # Should not raise or double-add sentinels
        assert len(stream.shards[0].records) == 1  # Just one sentinel


# ---------------------------------------------------------------------------
# MockKinesisBackend
# ---------------------------------------------------------------------------


class TestMockKinesisBackend:
    def test_create_and_get(self):
        stream = MockKinesisBackend.create_stream("my-stream", shard_count=2)
        assert MockKinesisBackend.get_stream("my-stream") is stream
        assert stream.shard_count == 2

    def test_get_nonexistent_raises(self):
        with pytest.raises(KeyError, match="does not exist"):
            MockKinesisBackend.get_stream("nope")

    def test_reset_clears_all(self):
        MockKinesisBackend.create_stream("s1")
        MockKinesisBackend.create_stream("s2")
        MockKinesisBackend.reset()
        assert not MockKinesisBackend.stream_exists("s1")
        assert not MockKinesisBackend.stream_exists("s2")

    def test_stream_exists(self):
        assert not MockKinesisBackend.stream_exists("x")
        MockKinesisBackend.create_stream("x")
        assert MockKinesisBackend.stream_exists("x")


# ---------------------------------------------------------------------------
# MockProducer
# ---------------------------------------------------------------------------


class TestMockProducer:
    @pytest.mark.asyncio
    async def test_put_writes_to_stream(self):
        stream = MockKinesisBackend.create_stream("test")
        async with MockProducer(stream_name="test") as producer:
            await producer.put({"msg": "hello"})
        assert stream.record_count == 1

    @pytest.mark.asyncio
    async def test_create_stream_on_enter(self):
        async with MockProducer(stream_name="auto-created", create_stream=True, create_stream_shards=3) as producer:
            assert MockKinesisBackend.stream_exists("auto-created")
            assert producer.stream.shard_count == 3

    @pytest.mark.asyncio
    async def test_custom_partition_key(self):
        stream = MockKinesisBackend.create_stream("test", shard_count=4)
        async with MockProducer(stream_name="test") as producer:
            await producer.put({"a": 1}, partition_key="my-key")
            await producer.put({"b": 2}, partition_key="my-key")
        # Both should be on the same shard
        non_empty = [s for s in stream.shards if s.record_count > 0]
        assert len(non_empty) == 1

    @pytest.mark.asyncio
    async def test_flush_on_exit(self):
        """Verify the context manager flushes on exit."""
        stream = MockKinesisBackend.create_stream("test")
        async with MockProducer(stream_name="test") as producer:
            await producer.put({"data": True})
        # After exiting, the record should be in the stream
        assert stream.record_count == 1

    @pytest.mark.asyncio
    async def test_string_processor(self):
        stream = MockKinesisBackend.create_stream("test")
        async with MockProducer(stream_name="test", processor=StringProcessor()) as producer:
            await producer.put("hello world")
        assert stream.record_count == 1

    @pytest.mark.asyncio
    async def test_ignores_aws_args(self):
        """AWS-specific constructor args should not raise."""
        MockKinesisBackend.create_stream("test")
        async with MockProducer(
            stream_name="test",
            endpoint_url="http://localhost:4567",
            region_name="us-east-1",
            buffer_time=1.0,
            put_rate_limit_per_shard=500,
            batch_size=100,
        ) as producer:
            await producer.put({"ok": True})


# ---------------------------------------------------------------------------
# MockConsumer
# ---------------------------------------------------------------------------


class TestMockConsumer:
    @pytest.mark.asyncio
    async def test_consume_records(self):
        stream = MockKinesisBackend.create_stream("test")
        async with MockProducer(stream_name="test") as producer:
            await producer.put({"n": 1})
            await producer.put({"n": 2})
            await producer.put({"n": 3})
        stream.seal()

        async with MockConsumer(stream_name="test") as consumer:
            records = await collect_records(consumer)

        assert len(records) == 3
        assert sorted(records, key=lambda r: r["n"]) == [{"n": 1}, {"n": 2}, {"n": 3}]

    @pytest.mark.asyncio
    async def test_empty_sealed_stream(self):
        stream = MockKinesisBackend.create_stream("test")
        stream.seal()

        async with MockConsumer(stream_name="test") as consumer:
            records = await collect_records(consumer, timeout=1.0)

        assert records == []

    @pytest.mark.asyncio
    async def test_latest_iterator_type(self):
        stream = MockKinesisBackend.create_stream("test")

        # Produce some records BEFORE consumer starts
        async with MockProducer(stream_name="test") as producer:
            await producer.put({"before": True})

        # Consumer with LATEST should not see the earlier record
        async with MockConsumer(stream_name="test", iterator_type="LATEST") as consumer:
            # Produce after consumer started
            async with MockProducer(stream_name="test") as producer:
                await producer.put({"after": True})

            stream.seal()
            records = await collect_records(consumer)

        assert records == [{"after": True}]

    @pytest.mark.asyncio
    async def test_trim_horizon_iterator_type(self):
        stream = MockKinesisBackend.create_stream("test")
        async with MockProducer(stream_name="test") as producer:
            await producer.put({"first": True})
        # Consumer with TRIM_HORIZON sees records from the beginning
        async with MockConsumer(stream_name="test", iterator_type="TRIM_HORIZON") as consumer:
            stream.seal()
            records = await collect_records(consumer)
        assert records == [{"first": True}]

    @pytest.mark.asyncio
    async def test_checkpointer_receives_updates(self):
        stream = MockKinesisBackend.create_stream("test")
        async with MockProducer(stream_name="test") as producer:
            await producer.put({"x": 1})
            await producer.put({"x": 2})
        stream.seal()

        async with MockConsumer(stream_name="test") as consumer:
            await collect_records(consumer)
            checkpoints = consumer.checkpointer.get_all_checkpoints()

        # Should have checkpointed the shard
        assert len(checkpoints) == 1
        shard_id = list(checkpoints.keys())[0]
        assert shard_id.startswith("shardId-")

    @pytest.mark.asyncio
    async def test_multi_shard_consumption(self):
        stream = MockKinesisBackend.create_stream("test", shard_count=4)
        async with MockProducer(stream_name="test") as producer:
            for i in range(50):
                await producer.put({"i": i}, partition_key=f"key-{i}")
        stream.seal()

        async with MockConsumer(stream_name="test") as consumer:
            records = await collect_records(consumer)

        assert len(records) == 50
        assert sorted(records, key=lambda r: r["i"]) == [{"i": i} for i in range(50)]

    @pytest.mark.asyncio
    async def test_string_processor_roundtrip(self):
        stream = MockKinesisBackend.create_stream("test")
        async with MockProducer(stream_name="test", processor=StringProcessor()) as producer:
            await producer.put("hello")
            await producer.put("world")
        stream.seal()

        async with MockConsumer(stream_name="test", processor=StringProcessor()) as consumer:
            records = await collect_records(consumer)

        assert sorted(records) == ["hello", "world"]

    @pytest.mark.asyncio
    async def test_create_stream_on_enter(self):
        async with MockConsumer(stream_name="auto", create_stream=True) as consumer:
            assert MockKinesisBackend.stream_exists("auto")

    @pytest.mark.asyncio
    async def test_ignores_aws_args(self):
        MockKinesisBackend.create_stream("test")
        stream = MockKinesisBackend.get_stream("test")
        stream.seal()
        async with MockConsumer(
            stream_name="test",
            endpoint_url="http://localhost:4567",
            region_name="us-east-1",
            max_queue_size=5000,
            sleep_time_no_records=5,
        ) as consumer:
            await collect_records(consumer, timeout=1.0)


# ---------------------------------------------------------------------------
# collect_records
# ---------------------------------------------------------------------------


class TestCollectRecords:
    @pytest.mark.asyncio
    async def test_count_limit(self):
        stream = MockKinesisBackend.create_stream("test")
        async with MockProducer(stream_name="test") as producer:
            for i in range(10):
                await producer.put({"i": i})
        stream.seal()

        async with MockConsumer(stream_name="test") as consumer:
            records = await collect_records(consumer, count=3)

        assert len(records) == 3

    @pytest.mark.asyncio
    async def test_timeout_returns_partial(self):
        MockKinesisBackend.create_stream("test")
        # Don't seal — consumer will block waiting for more records
        async with MockProducer(stream_name="test") as producer:
            await producer.put({"x": 1})

        async with MockConsumer(stream_name="test") as consumer:
            records = await collect_records(consumer, timeout=0.5)

        assert len(records) == 1


# ---------------------------------------------------------------------------
# assert_records_delivered
# ---------------------------------------------------------------------------


class TestAssertRecordsDelivered:
    @pytest.mark.asyncio
    async def test_success(self):
        MockKinesisBackend.create_stream("test")
        async with MockProducer(stream_name="test") as producer:
            async with MockConsumer(stream_name="test") as consumer:
                received = await assert_records_delivered(
                    producer, consumer, [{"a": 1}, {"b": 2}, {"c": 3}]
                )
        assert len(received) == 3

    @pytest.mark.asyncio
    async def test_with_partition_key(self):
        MockKinesisBackend.create_stream("test", shard_count=4)
        async with MockProducer(stream_name="test") as producer:
            async with MockConsumer(stream_name="test") as consumer:
                await assert_records_delivered(
                    producer, consumer,
                    [{"seq": i} for i in range(10)],
                    partition_key="same-shard",
                )


# ---------------------------------------------------------------------------
# assert_shard_ordering
# ---------------------------------------------------------------------------


class TestAssertShardOrdering:
    def test_ordered_records_pass(self):
        records = [
            {"key": "a", "ts": 1},
            {"key": "b", "ts": 1},
            {"key": "a", "ts": 2},
            {"key": "b", "ts": 2},
            {"key": "a", "ts": 3},
        ]
        assert_shard_ordering(records, key_func=lambda r: r["key"], order_func=lambda r: r["ts"])

    def test_unordered_records_fail(self):
        records = [
            {"key": "a", "ts": 3},
            {"key": "a", "ts": 1},  # Out of order
        ]
        with pytest.raises(AssertionError, match="not in order"):
            assert_shard_ordering(records, key_func=lambda r: r["key"], order_func=lambda r: r["ts"])

    def test_multiple_keys_all_ordered(self):
        records = [
            {"key": "a", "ts": 1},
            {"key": "b", "ts": 10},
            {"key": "a", "ts": 2},
            {"key": "b", "ts": 20},
        ]
        assert_shard_ordering(records, key_func=lambda r: r["key"], order_func=lambda r: r["ts"])


# ---------------------------------------------------------------------------
# memory_stream context manager
# ---------------------------------------------------------------------------


class TestMemoryStreamContextManager:
    def test_creates_and_cleans_up(self):
        with memory_stream("ctx-test", shard_count=2) as stream:
            assert stream.name == "ctx-test"
            assert stream.shard_count == 2
            assert MockKinesisBackend.stream_exists("ctx-test")

        # After exit, backend should be reset
        assert not MockKinesisBackend.stream_exists("ctx-test")


# ---------------------------------------------------------------------------
# Pytest fixtures (test that they work)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fixture_roundtrip(kinesis_stream, kinesis_producer, kinesis_consumer):
    """End-to-end test using the auto-discovered pytest fixtures."""
    await kinesis_producer.put({"fixture": "test"})
    await kinesis_producer.flush()
    kinesis_stream.seal()

    records = await collect_records(kinesis_consumer)
    assert records == [{"fixture": "test"}]


@pytest.mark.asyncio
async def test_fixture_backend_isolation(kinesis_backend):
    """Verify kinesis_backend fixture provides a clean slate."""
    assert not MockKinesisBackend.stream_exists("test-stream")
    kinesis_backend.create_stream("isolated")
    assert MockKinesisBackend.stream_exists("isolated")


# ---------------------------------------------------------------------------
# Aggregating processor round-trip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_jsonline_processor_roundtrip():
    """Test with JsonLineProcessor which aggregates multiple records."""
    stream = MockKinesisBackend.create_stream("agg-test")

    async with MockProducer(stream_name="agg-test", processor=JsonLineProcessor()) as producer:
        for i in range(5):
            await producer.put({"i": i})

    stream.seal()

    async with MockConsumer(stream_name="agg-test", processor=JsonLineProcessor()) as consumer:
        records = await collect_records(consumer)

    assert sorted(records, key=lambda r: r["i"]) == [{"i": i} for i in range(5)]


# ---------------------------------------------------------------------------
# Concurrent produce/consume
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_produce_consume():
    """Consumer started before all records are produced."""
    stream = MockKinesisBackend.create_stream("concurrent")

    async with MockConsumer(stream_name="concurrent") as consumer:

        async def produce():
            async with MockProducer(stream_name="concurrent") as producer:
                for i in range(10):
                    await producer.put({"i": i})
                    await asyncio.sleep(0)  # Yield control
            stream.seal()

        produce_task = asyncio.create_task(produce())
        records = await collect_records(consumer, timeout=5.0)
        await produce_task

    assert len(records) == 10
