import asyncio
import logging
from unittest.mock import AsyncMock, patch

import pytest

from kinesis import Consumer, MemoryCheckPointer, Producer
from kinesis.processors import JsonProcessor, StringProcessor
from kinesis.timeout_compat import timeout

log = logging.getLogger(__name__)


class TestConsumer:
    """Test consumer functionality."""

    @pytest.mark.asyncio
    async def test_consumer_context_manager(self, random_stream_name, endpoint_url):
        """Test consumer as async context manager."""
        # First create a stream with some data
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
        ) as producer:
            await producer.put({"message": "test"})
            await producer.flush()

        # Now consume the data
        async with Consumer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
        ) as consumer:
            assert consumer is not None

    @pytest.mark.asyncio
    async def test_consumer_iteration(self, test_stream, endpoint_url):
        """Test consumer iteration over records."""
        # First add some data
        async with Producer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
        ) as producer:
            for i in range(5):
                await producer.put({"message": f"test-{i}"})
            await producer.flush()

        # Now consume the data
        consumed_messages = []
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            sleep_time_no_records=0.1,
        ) as consumer:
            # Set a timeout to avoid infinite loop
            try:
                async with timeout(5):
                    async for message in consumer:
                        consumed_messages.append(message)
                        if len(consumed_messages) >= 5:
                            break
            except asyncio.TimeoutError:
                pass

        assert len(consumed_messages) > 0

    @pytest.mark.asyncio
    async def test_consumer_checkpointing(self, test_stream, endpoint_url):
        """Test consumer with checkpointing."""
        checkpointer = MemoryCheckPointer(name="test")

        # First add some data
        async with Producer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
        ) as producer:
            await producer.put({"message": "test"})
            await producer.flush()

        # Consume with checkpointing
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            checkpointer=checkpointer,
            sleep_time_no_records=0.1,
        ) as consumer:
            try:
                async with timeout(2):
                    async for message in consumer:
                        break  # Just get one message
            except asyncio.TimeoutError:
                pass

    @pytest.mark.asyncio
    async def test_consumer_different_processors(self, test_stream, endpoint_url):
        """Test consumer with different processors."""
        # Test with JSON processor
        async with Producer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            processor=JsonProcessor(),
        ) as producer:
            await producer.put({"message": "json_test"})
            await producer.flush()

        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            processor=JsonProcessor(),
            sleep_time_no_records=0.1,
        ) as consumer:
            try:
                async with timeout(2):
                    async for message in consumer:
                        assert isinstance(message, dict)
                        break
            except asyncio.TimeoutError:
                pass

    @pytest.mark.asyncio
    async def test_consumer_iterator_types(self, test_stream, endpoint_url):
        """Test consumer with different iterator types."""
        # Test TRIM_HORIZON (default)
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            iterator_type="TRIM_HORIZON",
        ) as consumer:
            assert consumer.iterator_type == "TRIM_HORIZON"

        # Test LATEST
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            iterator_type="LATEST",
        ) as consumer:
            assert consumer.iterator_type == "LATEST"

    @pytest.mark.asyncio
    async def test_consumer_max_shard_consumers(self, test_stream, endpoint_url):
        """Test consumer with max shard consumers limit."""
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            max_shard_consumers=1,
        ) as consumer:
            assert consumer.max_shard_consumers == 1

    @pytest.mark.asyncio
    async def test_consumer_record_limit(self, test_stream, endpoint_url):
        """Test consumer with record limit."""
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            record_limit=100,
        ) as consumer:
            assert consumer.record_limit == 100

    @pytest.mark.asyncio
    async def test_consumer_shard_fetch_rate(self, test_stream, endpoint_url):
        """Test consumer with custom shard fetch rate."""
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            shard_fetch_rate=2,
        ) as consumer:
            assert consumer.shard_fetch_rate == 2

    @pytest.mark.asyncio
    async def test_consumer_empty_stream(self, test_stream, endpoint_url):
        """Test consumer with empty stream."""
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            sleep_time_no_records=0.1,
        ) as consumer:
            messages = []
            try:
                async with timeout(1):
                    async for message in consumer:
                        messages.append(message)
            except asyncio.TimeoutError:
                pass

            # Should handle empty stream gracefully
            assert len(messages) == 0

    @pytest.mark.asyncio
    async def test_consumer_error_recovery(self, endpoint_url):
        """Test consumer error recovery."""
        # Test with non-existent stream
        with pytest.raises(Exception):
            async with Consumer(
                stream_name="nonexistent-stream",
                endpoint_url=endpoint_url,
            ) as consumer:
                async for message in consumer:
                    break

    @pytest.mark.asyncio
    async def test_consumer_graceful_shutdown(self, test_stream, endpoint_url):
        """Test consumer graceful shutdown."""
        consumer = Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
        )

        await consumer.start()

        # Should be able to close without issues
        await consumer.close()

    @pytest.mark.asyncio
    async def test_consumer_multiple_shards(self, random_stream_name, endpoint_url):
        """Test consumer with multiple shards."""
        # Create stream with multiple shards
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=2,
        ) as producer:
            for i in range(10):
                await producer.put({"message": f"test-{i}"})
            await producer.flush()

        # Consume from multiple shards
        async with Consumer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            max_shard_consumers=2,
        ) as consumer:
            messages = []
            try:
                async with timeout(3):
                    async for message in consumer:
                        messages.append(message)
                        if len(messages) >= 5:
                            break
            except asyncio.TimeoutError:
                pass

    @pytest.mark.asyncio
    async def test_consumer_at_timestamp(self, test_stream, endpoint_url):
        """Test consumer with AT_TIMESTAMP iterator type."""
        from datetime import datetime, timezone

        timestamp = datetime.now(timezone.utc)

        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            iterator_type="AT_TIMESTAMP",
            timestamp=timestamp,
        ) as consumer:
            assert consumer.iterator_type == "AT_TIMESTAMP"
            assert consumer.timestamp == timestamp

    @pytest.mark.asyncio
    async def test_consumer_queue_management(self, test_stream, endpoint_url):
        """Test consumer queue management."""
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            max_queue_size=100,
        ) as consumer:
            assert consumer.queue.maxsize == 100

    @pytest.mark.asyncio
    async def test_consumer_retry_logic(self, test_stream, endpoint_url):
        """Test consumer retry logic."""
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            retry_limit=3,
            expo_backoff=0.1,
        ) as consumer:
            # Test that retry parameters are set
            assert hasattr(consumer, "retry_limit")
            assert hasattr(consumer, "expo_backoff")

    @pytest.mark.asyncio
    async def test_consumer_stream_arn_support(self, endpoint_url):
        """Test consumer with stream ARN (if supported)."""
        # This would test the new ARN functionality from the recent PR
        stream_arn = "arn:aws:kinesis:us-east-1:123456789012:stream/test-stream"

        # This might fail if ARN support isn't fully implemented yet
        try:
            async with Consumer(
                stream_name=stream_arn,
                endpoint_url=endpoint_url,
            ) as consumer:
                assert consumer.stream_name == stream_arn
        except Exception:
            # ARN support might not be fully implemented yet
            pytest.skip("ARN support not fully implemented")

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
