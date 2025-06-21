import asyncio
import logging
from unittest.mock import AsyncMock, patch

import pytest

from kinesis import Producer, exceptions
from kinesis.processors import JsonLineProcessor, JsonProcessor, StringProcessor

log = logging.getLogger(__name__)


class TestProducer:
    """Test producer functionality."""

    @pytest.mark.asyncio
    async def test_producer_context_manager(self, random_stream_name, endpoint_url):
        """Test producer as async context manager."""
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            region_name="ap-southeast-2",
            create_stream=True,
            create_stream_shards=1,
        ) as producer:
            assert producer is not None
            await producer.put({"test": "data"})

    @pytest.mark.asyncio
    async def test_producer_put_single_record(self, producer):
        """Test putting a single record."""
        await producer.put({"message": "test"})

        # Force flush to ensure record is sent
        await producer.flush()

    @pytest.mark.asyncio
    async def test_producer_put_multiple_records(self, producer):
        """Test putting multiple records."""
        for i in range(10):
            await producer.put({"message": f"test-{i}"})

        await producer.flush()

    @pytest.mark.asyncio
    async def test_producer_auto_flush(self, random_stream_name, endpoint_url):
        """Test automatic flushing based on buffer time."""
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
            buffer_time=0.1,  # Very short buffer time
        ) as producer:
            await producer.put({"message": "test"})

            # Wait for auto flush
            await asyncio.sleep(0.2)

    @pytest.mark.asyncio
    async def test_producer_batch_size_flush(self, random_stream_name, endpoint_url):
        """Test flushing when batch size is reached."""
        batch_size = 5
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
            batch_size=batch_size,
            buffer_time=10,  # Long buffer time to test batch size trigger
        ) as producer:
            # Add records up to batch size
            for i in range(batch_size):
                await producer.put({"message": f"test-{i}"})

            # Should have triggered a flush
            await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    async def test_producer_different_processors(self, random_stream_name, endpoint_url):
        """Test producer with different processors."""
        processors = [
            JsonProcessor(),
            StringProcessor(),
            JsonLineProcessor(),
        ]

        for processor in processors:
            async with Producer(
                stream_name=random_stream_name,
                endpoint_url=endpoint_url,
                create_stream=True,
                create_stream_shards=1,
                processor=processor,
            ) as producer:
                if isinstance(processor, StringProcessor):
                    await producer.put("test string")
                else:
                    await producer.put({"test": "data"})

                await producer.flush()

    @pytest.mark.asyncio
    async def test_producer_queue_full_blocking(self, random_stream_name, endpoint_url):
        """Test producer queue management and blocking behavior."""
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
            max_queue_size=5,
            buffer_time=0.1,  # Short buffer time for quick testing
        ) as producer:
            # Add several items
            for i in range(3):
                await producer.put({"message": f"test-{i}"})

            # Queue should have items
            assert producer.queue.qsize() > 0

            # Force a flush to clear the queue
            await producer.flush()

            # After flush, queue should be cleared or reduced
            await asyncio.sleep(0.1)  # Allow time for flush to complete

    @pytest.mark.asyncio
    async def test_producer_after_flush_callback(self, random_stream_name, endpoint_url):
        """Test after flush callback functionality."""
        callback_called = False

        async def after_flush_callback(items):
            nonlocal callback_called
            callback_called = True

        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
            after_flush_fun=after_flush_callback,
        ) as producer:
            await producer.put({"message": "test"})
            await producer.flush()

            # Give callback time to execute
            await asyncio.sleep(0.1)
            assert callback_called

    @pytest.mark.asyncio
    async def test_producer_rate_limiting(self, random_stream_name, endpoint_url):
        """Test producer rate limiting."""
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
            put_rate_limit_per_shard=10,  # Very low rate limit
            put_bandwidth_limit_per_shard=1,  # Very low bandwidth limit
        ) as producer:
            # Add several records quickly
            for i in range(5):
                await producer.put({"message": f"test-{i}"})

            await producer.flush()

    @pytest.mark.asyncio
    async def test_producer_error_handling(self, endpoint_url):
        """Test producer error handling with invalid stream."""
        with pytest.raises(exceptions.StreamDoesNotExist):
            async with Producer(
                stream_name="nonexistent-stream",
                endpoint_url=endpoint_url,
                create_stream=False,
            ) as producer:
                await producer.put({"message": "test"})
                await producer.flush()

    @pytest.mark.asyncio
    async def test_producer_stream_creation(self, random_stream_name, endpoint_url):
        """Test automatic stream creation."""
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=2,
        ) as producer:
            await producer.put({"message": "test"})
            await producer.flush()

    @pytest.mark.asyncio
    async def test_producer_retry_logic(self, random_stream_name, endpoint_url):
        """Test producer retry logic."""
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
            retry_limit=3,
            expo_backoff=0.1,
        ) as producer:
            await producer.put({"message": "test"})
            await producer.flush()

    @pytest.mark.asyncio
    async def test_producer_large_record(self, producer, random_string):
        """Test producer with large record."""
        # Create a large but valid record (less than 1MB)
        large_data = random_string(1024 * 500)  # 500KB

        await producer.put({"large_data": large_data})
        await producer.flush()

    @pytest.mark.asyncio
    async def test_producer_very_large_record(self, producer, random_string):
        """Test producer with record exceeding limits."""
        # Create a record larger than 1MB
        very_large_data = random_string(1024 * 1024 + 1)  # > 1MB

        with pytest.raises(exceptions.ExceededPutLimit):
            await producer.put({"very_large_data": very_large_data})

    @pytest.mark.asyncio
    async def test_producer_graceful_shutdown(self, random_stream_name, endpoint_url):
        """Test producer graceful shutdown."""
        producer = Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
        )

        await producer.start()

        # Add some data
        await producer.put({"message": "test"})

        # Should flush pending data on close
        await producer.close()

    @pytest.mark.asyncio
    async def test_producer_partition_key(self, producer):
        """Test producer with custom partition key."""
        # Note: This would require updating the producer to support partition keys
        # For now, just test that the basic functionality works
        await producer.put({"message": "test", "user_id": "12345"})
        await producer.flush()

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
    async def test_producer_session_cleanup_with_auth_failure(self):
        """Test producer cleanup with authentication failures (Issue #35)."""
        # Test scenario that mimics the original issue report
        with pytest.raises(ConnectionError):
            async with Producer("auth-fail-test-stream", processor=JsonProcessor(), retry_limit=1) as producer:
                await producer.put({"test": "data"})

    @pytest.mark.asyncio
    async def test_producer_with_stream_arn(self, endpoint_url):
        """Test producer with stream ARN instead of name (PR #39)."""
        # Create a mock ARN
        stream_arn = "arn:aws:kinesis:us-east-1:123456789012:stream/test-producer-arn"

        producer = Producer(
            stream_name=stream_arn,
            endpoint_url=endpoint_url,
            create_stream=False,
        )

        # Verify that the address property returns StreamARN
        address = producer.address
        assert "StreamARN" in address
        assert address["StreamARN"] == stream_arn
        assert "StreamName" not in address

    @pytest.mark.asyncio
    async def test_producer_arn_format_validation(self, endpoint_url):
        """Test producer correctly handles various ARN formats (PR #39)."""
        test_cases = [
            # (stream_name, should_be_arn)
            ("arn:aws:kinesis:us-east-1:123456789012:stream/test", True),
            ("arn:aws:kinesis:eu-west-1:999999999999:stream/my-stream", True),
            ("arn:aws-cn:kinesis:cn-north-1:123456789012:stream/test", True),
            ("arn:aws-us-gov:kinesis:us-gov-west-1:123456789012:stream/test", True),
            ("test-stream", False),
            ("my_stream", False),
            ("stream-123", False),
            (
                "arnot:aws:kinesis:us-east-1:123456789012:stream/test",
                False,
            ),  # Invalid prefix
            ("arn-test-stream", False),  # Starts with arn but not a valid ARN
        ]

        for stream_name, should_be_arn in test_cases:
            producer = Producer(
                stream_name=stream_name,
                endpoint_url=endpoint_url,
                create_stream=False,
            )
            address = producer.address

            if should_be_arn:
                assert "StreamARN" in address
                assert address["StreamARN"] == stream_name
                assert "StreamName" not in address
            else:
                assert "StreamName" in address
                assert address["StreamName"] == stream_name
                assert "StreamARN" not in address
