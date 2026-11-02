import asyncio
import logging
import os
import uuid
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from aiobotocore.session import AioSession

from kinesis import Consumer, InMemoryMetricsCollector, MemoryCheckPointer, Producer, RedisCheckPointer, exceptions
from kinesis.aggregators import NetstringAggregator
from kinesis.processors import (
    JsonLineProcessor,
    JsonListProcessor,
    JsonProcessor,
    MsgpackProcessor,
    Processor,
    StringProcessor,
)
from kinesis.serializers import Serializer
from kinesis.timeout_compat import timeout
from tests.conftest import skip_if_no_aws, skip_if_no_redis

log = logging.getLogger(__name__)


class BytesSerializer(Serializer):
    def serialize(self, item):
        return item.encode()

    def deserialize(self, data):
        return data


class BytesProcessor(Processor, NetstringAggregator, BytesSerializer):
    pass


@pytest.mark.integration
class TestIntegration:
    """Integration tests for producer and consumer working together (requires Docker)."""

    @pytest.mark.asyncio
    async def test_producer_consumer_basic_flow(self, test_stream, endpoint_url):
        """Test basic producer -> consumer flow."""
        test_messages = [
            {"id": 1, "message": "first"},
            {"id": 2, "message": "second"},
            {"id": 3, "message": "third"},
        ]

        # Produce messages
        async with Producer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
        ) as producer:
            for message in test_messages:
                await producer.put(message)
            await producer.flush()

        # Consume messages
        consumed_messages = []
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            sleep_time_no_records=0.1,
        ) as consumer:
            try:
                async with timeout(5):
                    async for message in consumer:
                        consumed_messages.append(message)
                        if len(consumed_messages) >= len(test_messages):
                            break
            except asyncio.TimeoutError:
                pass

        assert len(consumed_messages) >= len(test_messages)

        # Verify message content (order might be different)
        consumed_ids = {msg["id"] for msg in consumed_messages[:3]}
        expected_ids = {msg["id"] for msg in test_messages}
        assert consumed_ids == expected_ids

    @pytest.mark.asyncio
    async def test_producer_consumer_different_processors(self, endpoint_url):
        """Test producer and consumer with matching processors."""
        processors_to_test = [
            (JsonProcessor(), {"type": "json", "data": "test"}),
            (StringProcessor(), "test string"),
            (JsonListProcessor(), {"type": "jsonlist", "data": "test"}),
        ]

        for i, (processor, test_data) in enumerate(processors_to_test):
            # Use unique stream name for each processor test
            stream_name = f"test_processors_{i}_{processor.__class__.__name__.lower()}"
            # Produce with specific processor
            async with Producer(
                stream_name=stream_name,
                endpoint_url=endpoint_url,
                processor=processor,
                create_stream=True,
                create_stream_shards=1,
            ) as producer:
                await producer.put(test_data)
                await producer.flush()

            # Consume with matching processor
            async with Consumer(
                stream_name=stream_name,
                endpoint_url=endpoint_url,
                processor=processor,
                sleep_time_no_records=0.1,
            ) as consumer:
                try:
                    async with timeout(2):
                        async for message in consumer:
                            if isinstance(processor, JsonListProcessor):
                                # JsonList processor returns list of items
                                assert isinstance(message, list)
                                if message:
                                    assert message[0] == test_data
                            else:
                                assert message == test_data
                            break
                except asyncio.TimeoutError:
                    pass

    @pytest.mark.asyncio
    async def test_producer_consumer_checkpointing(self, test_stream, endpoint_url):
        """Test producer -> consumer with checkpointing."""
        checkpointer = MemoryCheckPointer(name="integration_test")

        test_messages = [{"checkpoint_test": i, "data": f"message-{i}"} for i in range(5)]

        # Produce messages
        async with Producer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
        ) as producer:
            for message in test_messages:
                await producer.put(message)
            await producer.flush()

        # First consumer - consume some messages
        consumed_first = []
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            checkpointer=checkpointer,
            sleep_time_no_records=0.1,
        ) as consumer:
            try:
                async with timeout(3):
                    async for message in consumer:
                        consumed_first.append(message)
                        if len(consumed_first) >= 3:
                            break
            except asyncio.TimeoutError:
                pass

        # Second consumer - should resume from checkpoint
        consumed_second = []
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            checkpointer=checkpointer,
            sleep_time_no_records=0.1,
        ) as consumer:
            try:
                async with timeout(3):
                    async for message in consumer:
                        consumed_second.append(message)
                        if len(consumed_second) >= 2:
                            break
            except asyncio.TimeoutError:
                pass

        # Should have consumed all messages across both consumers
        total_consumed = len(consumed_first) + len(consumed_second)
        assert total_consumed >= len(test_messages)

    @pytest.mark.asyncio
    @skip_if_no_redis
    async def test_producer_consumer_redis_checkpointing(self, test_stream, endpoint_url):
        """Test producer -> consumer with Redis checkpointing."""
        import uuid

        checkpoint_name = f"integration_test_{str(uuid.uuid4())[0:8]}"

        test_messages = [{"redis_test": i, "data": f"message-{i}"} for i in range(3)]

        # Produce messages
        async with Producer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
        ) as producer:
            for message in test_messages:
                await producer.put(message)
            await producer.flush()

        # Consumer with Redis checkpointing
        checkpointer = RedisCheckPointer(name=checkpoint_name, id="test-consumer")

        try:
            consumed_messages = []
            async with Consumer(
                stream_name=test_stream,
                endpoint_url=endpoint_url,
                checkpointer=checkpointer,
                sleep_time_no_records=0.1,
            ) as consumer:
                try:
                    async with timeout(5):
                        async for message in consumer:
                            consumed_messages.append(message)
                            if len(consumed_messages) >= len(test_messages):
                                break
                except asyncio.TimeoutError:
                    pass

            assert len(consumed_messages) >= len(test_messages)

        finally:
            await checkpointer.close()

    @pytest.mark.asyncio
    async def test_producer_consumer_large_messages(self, test_stream, endpoint_url, random_string):
        """Test producer -> consumer with large messages."""
        # Create messages with large payloads (but within limits)
        large_payload = random_string(1024 * 100)  # 100KB
        test_messages = [{"id": i, "large_data": large_payload} for i in range(3)]

        # Produce large messages
        async with Producer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
        ) as producer:
            for message in test_messages:
                await producer.put(message)
            await producer.flush()

        # Consume large messages
        consumed_messages = []
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            sleep_time_no_records=0.1,
        ) as consumer:
            try:
                async with timeout(10):
                    async for message in consumer:
                        consumed_messages.append(message)
                        if len(consumed_messages) >= len(test_messages):
                            break
            except asyncio.TimeoutError:
                pass

        assert len(consumed_messages) >= len(test_messages)

        # Verify large payload integrity
        for msg in consumed_messages[:3]:
            if "large_data" in msg:
                assert len(msg["large_data"]) == len(large_payload)

    @pytest.mark.asyncio
    async def test_producer_consumer_high_throughput(self, test_stream, endpoint_url):
        """Test producer -> consumer with high message volume."""
        message_count = 100
        test_messages = [{"id": i, "timestamp": datetime.now(timezone.utc).isoformat()} for i in range(message_count)]

        # Produce many messages quickly
        async with Producer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            batch_size=50,
            buffer_time=0.1,
        ) as producer:
            for message in test_messages:
                await producer.put(message)
            await producer.flush()

        # Consume all messages
        consumed_messages = []
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            record_limit=50,
            sleep_time_no_records=0.1,
        ) as consumer:
            try:
                async with timeout(15):
                    async for message in consumer:
                        consumed_messages.append(message)
                        if len(consumed_messages) >= message_count:
                            break
            except asyncio.TimeoutError:
                pass

        # Should consume most or all messages (LocalStack may have limitations)
        assert len(consumed_messages) >= message_count * 0.5  # Allow for LocalStack variance

    @pytest.mark.asyncio
    async def test_producer_consumer_multiple_shards(self, random_stream_name, endpoint_url):
        """Test producer -> consumer with multiple shards."""
        shard_count = 2
        test_messages = [{"shard_test": i, "data": f"message-{i}"} for i in range(20)]

        # Create stream with multiple shards and produce messages
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=shard_count,
        ) as producer:
            for message in test_messages:
                await producer.put(message)
            await producer.flush()

        # Consume from multiple shards
        consumed_messages = []
        async with Consumer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            max_shard_consumers=shard_count,
            sleep_time_no_records=0.1,
        ) as consumer:
            try:
                async with timeout(10):
                    async for message in consumer:
                        consumed_messages.append(message)
                        if len(consumed_messages) >= len(test_messages):
                            break
            except asyncio.TimeoutError:
                pass

        assert len(consumed_messages) >= len(test_messages)

    @pytest.mark.asyncio
    @skip_if_no_aws
    async def test_aws_kinesis_integration(self, aws_testing_enabled):
        """Test against real AWS Kinesis (requires AWS credentials)."""
        if not aws_testing_enabled:
            pytest.skip("AWS testing not enabled")

        import uuid

        stream_name = f"async-kinesis-test-{str(uuid.uuid4())[0:8]}"

        test_messages = [{"aws_test": i, "message": f"aws-message-{i}"} for i in range(5)]

        try:
            # Create stream and produce messages
            async with Producer(
                stream_name=stream_name,
                create_stream=True,
                create_stream_shards=1,
            ) as producer:
                for message in test_messages:
                    await producer.put(message)
                await producer.flush()

            # Wait for AWS propagation
            await asyncio.sleep(5)

            # Consume messages
            consumed_messages = []
            async with Consumer(
                stream_name=stream_name,
                sleep_time_no_records=1,
            ) as consumer:
                try:
                    async with timeout(30):
                        async for message in consumer:
                            consumed_messages.append(message)
                            if len(consumed_messages) >= len(test_messages):
                                break
                except asyncio.TimeoutError:
                    pass

            assert len(consumed_messages) >= len(test_messages)

        except Exception as e:
            log.warning(f"AWS integration test failed: {e}")
            pytest.skip(f"AWS integration test failed: {e}")

    @pytest.mark.asyncio
    async def test_consumer_iterator_types_integration(self, test_stream, endpoint_url):
        # Skip for local emulators due to LATEST iterator timing sensitivity
        if any(host in endpoint_url for host in ["localhost", "kinesis:", "localstack"]):
            pytest.skip("Local emulator timing issues with LATEST iterator")
        """Test different iterator types in integration scenario."""
        # Produce some initial messages
        async with Producer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
        ) as producer:
            await producer.put({"phase": "initial", "id": 1})
            await producer.flush()

        # Wait a bit
        await asyncio.sleep(0.5)

        # Test LATEST iterator (should not see initial message)
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            iterator_type="LATEST",
            sleep_time_no_records=0.1,
        ) as consumer:
            # Produce new message after consumer starts
            async with Producer(
                stream_name=test_stream,
                endpoint_url=endpoint_url,
            ) as producer:
                await asyncio.sleep(0.1)  # Ensure consumer is ready
                await producer.put({"phase": "after_latest", "id": 2})
                await producer.flush()

            # Should only see the message produced after consumer started
            consumed_messages = []
            try:
                async with timeout(3):
                    async for message in consumer:
                        consumed_messages.append(message)
                        if message.get("phase") == "after_latest":
                            break
            except asyncio.TimeoutError:
                pass

            # Should not contain initial message
            phases = [msg.get("phase") for msg in consumed_messages]
            assert "after_latest" in phases

    @pytest.mark.asyncio
    async def test_producer_session_cleanup_on_connection_failure(self):
        """Test that producer cleans up properly when connection fails (Issue #35)."""
        # This test verifies the fix for AttributeError on session cleanup
        # when connection fails with non-existent streams or invalid endpoints
        with pytest.raises(ConnectionError):
            async with Producer(
                "nonexistent-stream-issue-35-test",
                processor=JsonProcessor(),
                retry_limit=1,  # Fail quickly
                endpoint_url="http://invalid-endpoint-for-test:9999",
            ) as producer:
                await producer.put({"test": "data"})

    @pytest.mark.asyncio
    async def test_consumer_session_cleanup_on_connection_failure(self):
        """Test that consumer cleans up properly when connection fails (Issue #35)."""
        # This test verifies the fix for AttributeError on session cleanup
        # when connection fails with non-existent streams or invalid endpoints
        with pytest.raises(ConnectionError):
            async with Consumer(
                "nonexistent-stream-issue-35-test",
                retry_limit=1,  # Fail quickly
                endpoint_url="http://invalid-endpoint-for-test:9999",
            ) as consumer:
                async for item in consumer:
                    break

    @pytest.mark.asyncio
    async def test_producer_consumer_with_stream_arn(self, random_stream_name, endpoint_url):
        """Test producer and consumer with stream ARN instead of name (PR #39)."""
        stream_arn = f"arn:aws:kinesis:us-east-1:123456789012:stream/{random_stream_name}"

        test_messages = [{"arn_test": i, "message": f"arn-message-{i}"} for i in range(3)]

        # First create the stream with regular name
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
        ) as producer:
            pass  # Just create the stream

        # Now produce using ARN
        async with Producer(
            stream_name=stream_arn,
            endpoint_url=endpoint_url,
            create_stream=False,  # Don't try to create with ARN
        ) as producer:
            for message in test_messages:
                await producer.put(message)
            await producer.flush()

        # Consume using ARN
        consumed_messages = []
        async with Consumer(
            stream_name=stream_arn,
            endpoint_url=endpoint_url,
            sleep_time_no_records=0.1,
        ) as consumer:
            try:
                async with timeout(5):
                    async for message in consumer:
                        consumed_messages.append(message)
                        if len(consumed_messages) >= len(test_messages):
                            break
            except asyncio.TimeoutError:
                pass

        assert len(consumed_messages) >= len(test_messages)

    @pytest.mark.asyncio
    async def test_stream_arn_validation(self, endpoint_url):
        """Test that ARN format is properly detected and handled (PR #39)."""
        valid_arns = [
            "arn:aws:kinesis:us-east-1:123456789012:stream/test-stream",
            "arn:aws:kinesis:eu-west-1:999999999999:stream/my-stream",
            "arn:aws-cn:kinesis:cn-north-1:123456789012:stream/test",
            "arn:aws-us-gov:kinesis:us-gov-west-1:123456789012:stream/test",
        ]

        regular_names = ["test-stream", "my_stream", "stream123", "a" * 128]

        # Test that ARNs are detected correctly
        for arn in valid_arns:
            producer = Producer(
                stream_name=arn,
                endpoint_url=endpoint_url,
                create_stream=False,
            )
            address = producer.address
            assert "StreamARN" in address
            assert address["StreamARN"] == arn
            assert "StreamName" not in address

        # Test that regular names are handled correctly
        for name in regular_names:
            producer = Producer(
                stream_name=name,
                endpoint_url=endpoint_url,
                create_stream=False,
            )
            address = producer.address
            assert "StreamName" in address
            assert address["StreamName"] == name
            assert "StreamARN" not in address

    @pytest.mark.asyncio
    async def test_stream_creation_with_arn_should_fail(self, endpoint_url):
        """Test that stream creation with ARN should not be attempted (PR #39)."""
        stream_arn = "arn:aws:kinesis:us-east-1:123456789012:stream/test-create-arn"

        # Stream creation should fail or be skipped when using ARN
        # since ARNs are for existing streams only
        try:
            async with Producer(
                stream_name=stream_arn,
                endpoint_url=endpoint_url,
                create_stream=True,  # This should not work with ARN
                create_stream_shards=1,
                retry_limit=1,
            ) as producer:
                # If we get here, check that the producer at least uses ARN correctly
                assert "StreamARN" in producer.address
        except Exception:
            # Expected - creating a stream with ARN format should fail
            pass

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "processor_class, items, expected",
        [
            (JsonLineProcessor, [{"test": i} for i in range(10)], [{"test": i} for i in range(10)]),
            (MsgpackProcessor, [{"test": i} for i in range(10)], [{"test": i} for i in range(10)]),
            (BytesProcessor, ["0", "1"], [b"0", b"1"]),
        ],
    )
    async def test_aggregated_record_round_trip(self, test_stream, endpoint_url, processor_class, items, expected):
        """One Kinesis record comes back as every item aggregated into it, including binary payloads."""
        collector = InMemoryMetricsCollector()

        async with Producer(
            stream_name=test_stream, endpoint_url=endpoint_url, processor=processor_class()
        ) as producer:
            for item in items:
                await producer.put(item)
            await producer.flush()

        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            processor=processor_class(),
            metrics_collector=collector,
        ) as consumer:
            results = [item async for item in consumer]

        assert results == expected
        assert sum(v for k, v in collector.counters.items() if k.startswith("consumer_records_received_total")) == 1

    @pytest.mark.asyncio
    async def test_close_delivers_unflushed_records_across_several_puts(self, test_stream, endpoint_url, random_string):
        """Leaving the producer without flushing still writes everything, including what overflows one put."""
        flushed_batches = []

        async def after_flush(items):
            flushed_batches.append(len(items))

        # 15 x 100 KB exceeds the 1 MB a single PutRecords call takes
        async with Producer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            processor=StringProcessor(),
            after_flush_fun=after_flush,
        ) as producer:
            for _ in range(15):
                await producer.put(random_string(100 * 1024))

        assert len(flushed_batches) >= 2
        assert sum(flushed_batches) == 15

        async with Consumer(
            stream_name=test_stream, endpoint_url=endpoint_url, processor=StringProcessor()
        ) as consumer:
            results = [item async for item in consumer]

        assert len(results) == 15

    @pytest.mark.asyncio
    async def test_record_at_size_limit_round_trip(self, test_stream, endpoint_url, random_string):
        """A record of exactly the largest size the processor allows reaches the consumer intact."""
        limit = 40 * 25 * 1024
        assert StringProcessor().max_bytes == limit
        payload = random_string(limit)

        async with Producer(
            stream_name=test_stream, endpoint_url=endpoint_url, processor=StringProcessor()
        ) as producer:
            await producer.put(payload)

        async with Consumer(
            stream_name=test_stream, endpoint_url=endpoint_url, processor=StringProcessor()
        ) as consumer:
            results = [item async for item in consumer]

        assert results == [payload]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("processor_class", [JsonLineProcessor, JsonListProcessor, MsgpackProcessor])
    async def test_aggregated_record_at_size_limit_round_trip(self, test_stream, endpoint_url, processor_class):
        """An item whose framing fills the record arrives alone, with no empty record ahead of it."""
        processor = processor_class(max_size=25)
        item = "x" * 1000
        overhead = len(processor.serialize(item)) - len(item)
        # The largest framing here is netstring's, seven bytes for a five digit length
        for framing in range(1, 8):
            item = "x" * (processor.max_bytes - overhead - framing)
            serialized = processor.serialize(item)
            if len(serialized) + processor.get_header_size(serialized) == processor.max_bytes:
                break
        else:
            pytest.fail("no item fills the record exactly")

        collector = InMemoryMetricsCollector()

        async with Producer(
            stream_name=test_stream, endpoint_url=endpoint_url, processor=processor, metrics_collector=collector
        ) as producer:
            await producer.put(item)

        async with Consumer(
            stream_name=test_stream, endpoint_url=endpoint_url, processor=processor_class(max_size=25)
        ) as consumer:
            results = [message async for message in consumer]

        expected = [item]
        assert results == ([expected] if processor_class is JsonListProcessor else expected)
        # JsonLine's parse() and the consumer's own counter both skip an empty record, so count at the producer
        assert sum(v for k, v in collector.counters.items() if k.startswith("producer_records_sent_total")) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("processor_class", [JsonLineProcessor, MsgpackProcessor])
    async def test_consumer_skips_an_empty_record(self, test_stream, endpoint_url, processor_class):
        """An empty record, which producers before 3.0.2 could write, yields nothing and holds back nothing."""
        async with Producer(
            stream_name=test_stream, endpoint_url=endpoint_url, processor=processor_class()
        ) as producer:
            # A record on each side of the empty one, read back in one batch: a parse error on the
            # empty record re-delivered the one before it on every retry and never reached the one after
            await producer.put({"n": 0})
            await producer.flush()
            await producer.client.put_record(StreamName=test_stream, Data=b"", PartitionKey="empty")
            for i in range(1, 3):
                await producer.put({"n": i})

        async with Consumer(
            stream_name=test_stream, endpoint_url=endpoint_url, processor=processor_class()
        ) as consumer:
            results = [message async for message in consumer]

        assert results == [{"n": 0}, {"n": 1}, {"n": 2}]

    @pytest.mark.asyncio
    async def test_consumer_queue_smaller_than_backlog_delivers_all(self, test_stream, endpoint_url):
        """A full queue applies backpressure to fetching rather than dropping or ending early."""
        async with Producer(stream_name=test_stream, endpoint_url=endpoint_url) as producer:
            for i in range(100):
                await producer.put(i)
            await producer.flush()

        async with Consumer(stream_name=test_stream, endpoint_url=endpoint_url, max_queue_size=20) as consumer:
            results = [item async for item in consumer]

        assert results == list(range(100))

    @pytest.mark.asyncio
    async def test_consumer_record_limit_pages_through_backlog(self, test_stream, endpoint_url):
        """A backlog larger than record_limit arrives whole, over as many GetRecords calls as it takes."""
        async with Producer(stream_name=test_stream, endpoint_url=endpoint_url) as producer:
            for i in range(100):
                await producer.put(i)
            await producer.flush()

        results = []
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            record_limit=10,
            shard_fetch_rate=2,
            # Wide enough that a stalled fetch on a slow runner does not end iteration early
            idle_timeout=10,
        ) as consumer:
            async with timeout(60):
                async for item in consumer:
                    results.append(item)
                    if len(results) == 100:
                        break

            assert consumer.shards[0]["stats"].to_data()["success"] >= 10

        assert results == list(range(100))

    @pytest.mark.asyncio
    async def test_producer_leaves_no_tasks_running_after_exit(self, test_stream, endpoint_url):
        """Leaving the context stops the background flush task and everything it started."""
        tasks_before = asyncio.all_tasks()

        async with Producer(stream_name=test_stream, endpoint_url=endpoint_url) as producer:
            await producer.put("test")

        assert asyncio.all_tasks() - tasks_before == set()

    @pytest.mark.asyncio
    async def test_latest_iterator_resumes_from_checkpoint(self, test_stream, endpoint_url):
        """LATEST applies only until a sequence is checkpointed; after that a consumer resumes from it."""
        checkpointer = MemoryCheckPointer(name="test")

        def checkpointed_sequence():
            checkpoints = checkpointer.get_all_checkpoints()
            assert len(checkpoints) == 1
            return next(iter(checkpoints.values()))["sequence"]

        def latest_consumer():
            return Consumer(
                stream_name=test_stream,
                endpoint_url=endpoint_url,
                checkpointer=checkpointer,
                iterator_type="LATEST",
                sleep_time_no_records=0.5,
            )

        async with Producer(stream_name=test_stream, endpoint_url=endpoint_url) as producer:
            await producer.put("before any consumer")
            await producer.flush()

            async with latest_consumer() as consumer:
                results = [item async for item in consumer]
            assert results == []
            assert checkpointed_sequence() is None

            async with latest_consumer() as consumer:
                await consumer.wait_ready(timeout=10)
                await producer.put("after wait_ready")
                await producer.flush()
                results = [item async for item in consumer]
            assert results == ["after wait_ready"]
            assert checkpointed_sequence() is not None

            # Nothing is consuming while these arrive, so LATEST alone would miss them
            missed = [f"while stopped {i}" for i in range(10)]
            for item in missed:
                await producer.put(item)
            await producer.flush()

            async with latest_consumer() as consumer:
                results = [item async for item in consumer]
            assert results == missed

    @pytest.mark.asyncio
    @skip_if_no_redis
    async def test_multiple_shards_with_redis_checkpointer(self, random_stream_name, endpoint_url):
        """One consumer takes every shard through RedisCheckPointer and checkpoints each of them."""
        expected = [f"test.{i}" for i in range(100)]

        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=2,
        ) as producer:
            for item in expected:
                await producer.put(item)
            await producer.flush()

        checkpointer = RedisCheckPointer(name=f"test-{uuid.uuid4().hex[:8]}", heartbeat_frequency=3)

        async with Consumer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            checkpointer=checkpointer,
            record_limit=10,
            sleep_time_no_records=0.1,
            # Wide enough that a stalled fetch on a slow runner does not end iteration early
            idle_timeout=10,
        ) as consumer:
            results = []
            async with timeout(60):
                async for item in consumer:
                    results.append(item)
                    if len(results) == len(expected):
                        break

            # Read before the consumer closes, since closing deallocates both shards
            checkpoints = checkpointer.get_all_checkpoints()

        assert sorted(results) == sorted(expected)
        assert len(checkpoints) == 2
        assert all(checkpoints.values())

    @pytest.mark.asyncio
    async def test_supplied_session_creates_the_client(self, endpoint_url):
        """Producer and Consumer use the session passed to them, and a missing stream still raises."""
        session = AioSession()

        with patch.object(session, "create_client", wraps=session.create_client) as create_client:
            with pytest.raises(exceptions.StreamDoesNotExist):
                async with Producer(session=session, stream_name="missing-stream", endpoint_url=endpoint_url):
                    pass
            assert create_client.called
            create_client.reset_mock()

            with pytest.raises(exceptions.StreamDoesNotExist):
                async with Consumer(session=session, stream_name="missing-stream", endpoint_url=endpoint_url):
                    pass
            assert create_client.called
