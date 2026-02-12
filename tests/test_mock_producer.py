"""Producer tests using in-memory mocks. No Docker required.

Tests for MockProducer behaviour not already covered in test_testing.py.
Focuses on aggregating processor lifecycle, multi-record patterns, and error cases.
"""

import pytest

from kinesis.processors import JsonLineProcessor
from kinesis.testing import MockKinesisBackend, MockProducer


@pytest.mark.asyncio
class TestMockProducer:
    """MockProducer behaviour tests."""

    async def test_put_multiple_records(self, kinesis_stream):
        """Multiple records are written to the stream."""
        async with MockProducer(stream_name=kinesis_stream.name) as producer:
            for i in range(10):
                await producer.put({"id": i})
            await producer.flush()

        assert kinesis_stream.record_count == 10

    async def test_flush_aggregating_processor(self, kinesis_backend):
        """Aggregating processor buffers until flush."""
        stream = kinesis_backend.create_stream("agg-test")
        processor = JsonLineProcessor()

        async with MockProducer(stream_name="agg-test", processor=processor) as producer:
            await producer.put({"a": 1})
            await producer.put({"b": 2})

            # Not flushed yet — aggregating processor holds items
            assert stream.record_count == 0

            await producer.flush()

            # Now flushed — single aggregated record
            assert stream.record_count == 1

    async def test_close_flushes_pending(self, kinesis_backend):
        """close() flushes any pending aggregated items."""
        stream = kinesis_backend.create_stream("close-test")
        processor = JsonLineProcessor()

        producer = MockProducer(stream_name="close-test", processor=processor)
        await producer.__aenter__()

        await producer.put({"data": "pending"})
        assert stream.record_count == 0

        await producer.close()
        assert stream.record_count == 1

    async def test_exit_flushes_aggregated(self, kinesis_backend):
        """__aexit__ flushes pending aggregated items."""
        stream = kinesis_backend.create_stream("exit-test")
        processor = JsonLineProcessor()

        async with MockProducer(stream_name="exit-test", processor=processor) as producer:
            await producer.put({"data": "pending"})
            assert stream.record_count == 0

        assert stream.record_count == 1

    async def test_auto_partition_key(self, kinesis_stream):
        """Without explicit key, auto-generated keys are used."""
        async with MockProducer(stream_name=kinesis_stream.name) as producer:
            await producer.put({"msg": "a"})
            await producer.put({"msg": "b"})
            await producer.flush()

        assert kinesis_stream.record_count == 2

    async def test_sealed_stream_error(self, kinesis_stream):
        """Writing to a sealed stream raises RuntimeError."""
        kinesis_stream.seal()

        async with MockProducer(stream_name=kinesis_stream.name) as producer:
            with pytest.raises(RuntimeError, match="sealed"):
                await producer.put({"msg": "should fail"})

    async def test_partition_key_routing_consistency(self, kinesis_backend):
        """Same partition key always routes to the same shard."""
        stream = kinesis_backend.create_stream("routing-test", shard_count=4)

        async with MockProducer(stream_name="routing-test") as producer:
            for _ in range(5):
                await producer.put({"data": "same-key"}, partition_key="consistent-key")
            await producer.flush()

        non_empty_shards = [s for s in stream.shards if s.record_count > 0]
        assert len(non_empty_shards) == 1
        assert non_empty_shards[0].record_count == 5
