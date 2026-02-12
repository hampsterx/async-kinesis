"""Consumer tests using in-memory mocks. No Docker required.

Tests for MockConsumer behaviour not already covered in test_testing.py.
Focuses on direct iteration, timeout handling, and explicit close().
"""

from kinesis.testing import (
    MockConsumer,
    MockProducer,
    collect_records,
)


class TestMockConsumer:
    """MockConsumer behaviour tests."""

    async def test_iteration_all_records(self, kinesis_stream):
        """Consumer iterates over all records via async for until sealed."""
        async with MockProducer(stream_name=kinesis_stream.name) as producer:
            for i in range(5):
                await producer.put({"id": i})
            await producer.flush()

        kinesis_stream.seal()

        records = []
        async with MockConsumer(stream_name=kinesis_stream.name) as consumer:
            async for record in consumer:
                records.append(record)

        assert len(records) == 5

    async def test_collect_records_timeout(self, kinesis_backend):
        """collect_records returns after timeout on unsealed empty stream."""
        kinesis_backend.create_stream("timeout-test")

        async with MockConsumer(stream_name="timeout-test") as consumer:
            records = await collect_records(consumer, timeout=0.1)

        assert records == []

    async def test_close(self, kinesis_stream):
        """close() can be called explicitly outside context manager."""
        kinesis_stream.seal()

        consumer = MockConsumer(stream_name=kinesis_stream.name)
        await consumer.__aenter__()
        await consumer.close()
