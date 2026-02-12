"""End-to-end integration tests using in-memory mocks. No Docker required.

These tests mirror the Docker-based integration tests in test_integration.py,
validating the same produce → consume workflows without infrastructure.
Tests for mock primitives (MemoryShard, MemoryStream, etc.) live in test_testing.py.
"""

from kinesis.checkpointers import MemoryCheckPointer
from kinesis.processors import JsonListProcessor
from kinesis.testing import (
    MockConsumer,
    MockProducer,
    assert_shard_ordering,
    collect_records,
)


class TestMockIntegration:
    """Produce → consume roundtrip tests using in-memory mocks."""

    async def test_basic_roundtrip(self, kinesis_stream, kinesis_producer, kinesis_consumer):
        """Multi-message produce → consume roundtrip with content verification."""
        test_messages = [
            {"id": 1, "message": "first"},
            {"id": 2, "message": "second"},
            {"id": 3, "message": "third"},
        ]

        for msg in test_messages:
            await kinesis_producer.put(msg)
        await kinesis_producer.flush()

        kinesis_stream.seal()

        records = await collect_records(kinesis_consumer)

        assert len(records) == 3
        assert sorted(records, key=lambda r: r["id"]) == test_messages

    async def test_jsonlist_processor_roundtrip(self, kinesis_backend):
        """Roundtrip with JsonListProcessor (aggregating, returns list-of-lists)."""
        stream = kinesis_backend.create_stream("jsonlist-test")
        processor = JsonListProcessor()

        async with MockProducer(stream_name="jsonlist-test", processor=processor) as producer:
            await producer.put({"item": 1})
            await producer.put({"item": 2})
            await producer.flush()

        stream.seal()

        async with MockConsumer(stream_name="jsonlist-test", processor=processor) as consumer:
            records = await collect_records(consumer)

        assert len(records) == 1
        assert records[0] == [{"item": 1}, {"item": 2}]

    async def test_checkpointing_resume(self, kinesis_backend):
        """Produce 5 records, consume 3 with checkpoint, resume to get remaining 2."""
        stream = kinesis_backend.create_stream("checkpoint-test")
        checkpointer = MemoryCheckPointer(name="resume-test")

        async with MockProducer(stream_name="checkpoint-test") as producer:
            for i in range(5):
                await producer.put({"id": i})
            await producer.flush()

        stream.seal()

        # First consumer: read 3 records then stop
        first_batch = []
        async with MockConsumer(
            stream_name="checkpoint-test", checkpointer=checkpointer
        ) as consumer:
            async for record in consumer:
                first_batch.append(record)
                if len(first_batch) >= 3:
                    break

        assert len(first_batch) == 3

        # Second consumer with same checkpointer: should resume after checkpoint
        async with MockConsumer(
            stream_name="checkpoint-test", checkpointer=checkpointer
        ) as consumer:
            second_batch = await collect_records(consumer)

        assert len(second_batch) == 2

        total_ids = sorted([r["id"] for r in first_batch + second_batch])
        assert total_ids == [0, 1, 2, 3, 4]

    async def test_large_message_integrity(self, kinesis_stream, kinesis_producer, kinesis_consumer):
        """Verify large payloads survive the roundtrip unchanged."""
        large_payload = "x" * 100_000
        messages = [{"id": i, "data": large_payload} for i in range(3)]

        for msg in messages:
            await kinesis_producer.put(msg)
        await kinesis_producer.flush()

        kinesis_stream.seal()

        records = await collect_records(kinesis_consumer)

        assert len(records) == 3
        for record in records:
            assert len(record["data"]) == 100_000

    async def test_high_throughput(self, kinesis_stream, kinesis_producer, kinesis_consumer):
        """100 records produced and consumed."""
        for i in range(100):
            await kinesis_producer.put({"id": i})
        await kinesis_producer.flush()

        kinesis_stream.seal()

        records = await collect_records(kinesis_consumer)

        assert len(records) == 100
        assert sorted([r["id"] for r in records]) == list(range(100))

    async def test_multiple_shards_distribution(self, kinesis_backend):
        """Records distribute across shards and are all consumed."""
        stream = kinesis_backend.create_stream("multi-shard", shard_count=4)

        async with MockProducer(stream_name="multi-shard") as producer:
            for i in range(20):
                await producer.put({"id": i}, partition_key=f"key-{i}")
            await producer.flush()

        stream.seal()

        async with MockConsumer(stream_name="multi-shard") as consumer:
            records = await collect_records(consumer)

        assert len(records) == 20
        non_empty = sum(1 for shard in stream.shards if shard.record_count > 0)
        assert non_empty > 1, "Records should distribute across multiple shards"

    async def test_multiple_consumers_same_stream(self, kinesis_backend):
        """Two independent consumers can both read all records."""
        stream = kinesis_backend.create_stream("shared-stream")

        async with MockProducer(stream_name="shared-stream") as producer:
            for i in range(5):
                await producer.put({"id": i})
            await producer.flush()

        stream.seal()

        async with MockConsumer(stream_name="shared-stream") as consumer_a:
            records_a = await collect_records(consumer_a)

        async with MockConsumer(stream_name="shared-stream") as consumer_b:
            records_b = await collect_records(consumer_b)

        assert len(records_a) == 5
        assert len(records_b) == 5

    async def test_shard_ordering(self, kinesis_backend):
        """Per-partition ordering is preserved across shards."""
        stream = kinesis_backend.create_stream("ordering-test", shard_count=2)

        async with MockProducer(stream_name="ordering-test") as producer:
            for i in range(10):
                await producer.put(
                    {"key": f"pk-{i % 3}", "seq": i},
                    partition_key=f"pk-{i % 3}",
                )
            await producer.flush()

        stream.seal()

        async with MockConsumer(stream_name="ordering-test") as consumer:
            records = await collect_records(consumer)

        assert_shard_ordering(
            records,
            key_func=lambda r: r["key"],
            order_func=lambda r: r["seq"],
        )
