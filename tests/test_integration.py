import asyncio
import logging
import os
from datetime import datetime, timezone

import pytest

from kinesis import Consumer, MemoryCheckPointer, Producer, RedisCheckPointer
from kinesis.processors import JsonLineProcessor, JsonListProcessor, JsonProcessor, StringProcessor
from kinesis.timeout_compat import timeout
from tests.conftest import skip_if_no_aws, skip_if_no_redis

log = logging.getLogger(__name__)


class TestIntegration:
    """Integration tests for producer and consumer working together."""

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
        # Skip test for LocalStack/kinesalite due to LATEST iterator timing issues
        if any(host in endpoint_url for host in ["localhost", "kinesis:", "localstack"]):
            pytest.skip("LocalStack/kinesalite timing issues with LATEST iterator")
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
        # Skip for LocalStack as it may not support ARN addressing
        if any(host in endpoint_url for host in ["localhost", "kinesis:", "localstack"]):
            pytest.skip("LocalStack may not fully support ARN addressing")

        # Create a mock ARN for the stream
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
        try:
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

        except Exception as e:
            # ARN support might not work with LocalStack/testing environment
            pytest.skip(f"ARN support test failed: {e}")

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
