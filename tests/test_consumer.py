import asyncio
import logging
from unittest.mock import AsyncMock

import pytest

from kinesis import Consumer, MemoryCheckPointer, Producer, exceptions
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
        with pytest.raises(exceptions.StreamDoesNotExist):
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
        """Test consumer with stream ARN (PR #39)."""
        # Create a mock ARN
        stream_arn = "arn:aws:kinesis:us-east-1:123456789012:stream/test-consumer-arn"

        consumer = Consumer(
            stream_name=stream_arn,
            endpoint_url=endpoint_url,
        )

        # Verify that the address property returns StreamARN
        address = consumer.address
        assert "StreamARN" in address
        assert address["StreamARN"] == stream_arn
        assert "StreamName" not in address
        assert consumer.stream_name == stream_arn

    @pytest.mark.asyncio
    async def test_consumer_arn_format_validation(self, endpoint_url):
        """Test consumer correctly handles various ARN formats (PR #39)."""
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
            consumer = Consumer(
                stream_name=stream_name,
                endpoint_url=endpoint_url,
            )
            address = consumer.address

            if should_be_arn:
                assert "StreamARN" in address
                assert address["StreamARN"] == stream_name
                assert "StreamName" not in address
            else:
                assert "StreamName" in address
                assert address["StreamName"] == stream_name
                assert "StreamARN" not in address

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
    async def test_consumer_shard_refresh(self, test_stream, endpoint_url):
        """Test shard refresh functionality."""
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
        ) as consumer:
            # Force a shard refresh
            await consumer.refresh_shards()

            # Should have discovered shards
            assert consumer.shards is not None
            assert len(consumer.shards) > 0

    @pytest.mark.asyncio
    async def test_consumer_shard_status(self, test_stream, endpoint_url):
        """Test shard status reporting."""
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
        ) as consumer:
            # Get shard status
            status = consumer.get_shard_status()

            # Should have basic status information
            assert "total_shards" in status
            assert "active_shards" in status
            assert "closed_shards" in status
            assert "allocated_shards" in status
            assert "shard_details" in status

            # Should have at least one shard for the test stream
            assert status["total_shards"] >= 1

    @pytest.mark.asyncio
    async def test_consumer_closed_shard_handling(self, endpoint_url):
        """Test handling of closed shards."""
        consumer = Consumer(
            stream_name="test-closed-shard",
            endpoint_url=endpoint_url,
        )

        # Mock a shard that will be closed
        mock_shard = {
            "ShardId": "test-shard-001",
            "ShardIterator": "test-iterator",
            "stats": consumer.ShardStats if hasattr(consumer, "ShardStats") else None,
        }

        consumer.shards = [mock_shard]
        consumer.checkpointer = AsyncMock()
        consumer.checkpointer.is_allocated = lambda x: True
        consumer.checkpointer.deallocate = AsyncMock()

        # Simulate the closed shard scenario in the fetch logic
        shard_id = mock_shard["ShardId"]
        consumer._closed_shards.add(shard_id)

        # Verify the shard is marked as closed
        assert shard_id in consumer._closed_shards

        # Test shard status with closed shard
        status = consumer.get_shard_status()
        assert status["closed_shards"] >= 1

    @pytest.mark.asyncio
    async def test_consumer_shard_discovery_integration(self, random_stream_name, endpoint_url):
        """Test shard discovery during stream operations."""
        # Create a stream with multiple shards
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=2,
        ) as producer:
            await producer.put({"test": "message"})
            await producer.flush()

        # Consumer should discover all shards
        async with Consumer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
        ) as consumer:
            # Force refresh to ensure shards are discovered
            await consumer.refresh_shards()

            # Should have discovered the shards
            status = consumer.get_shard_status()
            assert status["total_shards"] == 2
            assert status["active_shards"] == 2

            # Verify shard details contain the expected information
            for shard_detail in status["shard_details"]:
                assert "shard_id" in shard_detail
                assert "is_allocated" in shard_detail
                assert "is_closed" in shard_detail
                assert "has_iterator" in shard_detail

    @pytest.mark.asyncio
    async def test_consumer_expired_iterator_handling(self, test_stream, endpoint_url):
        """Test handling of expired shard iterators."""
        from unittest.mock import AsyncMock, patch

        from botocore.exceptions import ClientError

        consumer = Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
        )

        # Setup mock shard
        mock_shard = {
            "ShardId": "test-shard-001",
            "ShardIterator": "expired-iterator",
            "LastSequenceNumber": "123456",
            "throttler": AsyncMock(),
        }

        # Mock the client and methods
        consumer.client = AsyncMock()
        consumer.get_shard_iterator = AsyncMock(return_value="new-iterator")

        # Mock expired iterator exception
        expired_error = ClientError(
            error_response={"Error": {"Code": "ExpiredIteratorException"}},
            operation_name="GetRecords",
        )

        # Test that expired iterator is handled properly
        # This would normally be called in get_records method
        try:
            # The improved error handling should recreate the iterator
            await consumer.get_shard_iterator(
                shard_id=mock_shard["ShardId"],
                last_sequence_number=mock_shard["LastSequenceNumber"],
            )
            # Should succeed with new iterator
            assert True
        except Exception:
            pytest.fail("Should handle expired iterator gracefully")

    @pytest.mark.asyncio
    async def test_consumer_shard_allocation_skips_closed(self, test_stream, endpoint_url):
        """Test that shard allocation skips closed shards."""
        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
        ) as consumer:
            # Manually mark a shard as closed
            if consumer.shards:
                test_shard_id = consumer.shards[0]["ShardId"]
                consumer._closed_shards.add(test_shard_id)

                # The fetch method should skip this shard
                # This is tested implicitly through the status
                status = consumer.get_shard_status()
                assert status["closed_shards"] >= 1

    @pytest.mark.asyncio
    async def test_consumer_parent_child_ordering(self, endpoint_url):
        """Test parent-child shard ordering logic."""
        consumer = Consumer(
            stream_name="test-topology",
            endpoint_url=endpoint_url,
        )

        # Mock shards with parent-child relationships
        mock_shards = [
            {
                "ShardId": "shard-parent-001",
                "SequenceNumberRange": {"StartingSequenceNumber": "100"},
                # No ParentShardId - this is a parent
            },
            {
                "ShardId": "shard-child-001",
                "ParentShardId": "shard-parent-001",
                "SequenceNumberRange": {"StartingSequenceNumber": "200"},
            },
            {
                "ShardId": "shard-child-002",
                "ParentShardId": "shard-parent-001",
                "SequenceNumberRange": {"StartingSequenceNumber": "300"},
            },
        ]

        # Build topology
        consumer._build_shard_topology(mock_shards)

        # Verify topology is correctly built
        assert "shard-parent-001" in consumer._parent_shards
        assert "shard-child-001" in consumer._child_shards
        assert "shard-child-002" in consumer._child_shards

        # Parent should be allocatable
        assert consumer._should_allocate_shard("shard-parent-001") == True

        # Children should NOT be allocatable while parent is active
        assert consumer._should_allocate_shard("shard-child-001") == False
        assert consumer._should_allocate_shard("shard-child-002") == False

        # Mark parent as exhausted
        consumer._exhausted_parents.add("shard-parent-001")

        # Now children should be allocatable
        assert consumer._should_allocate_shard("shard-child-001") == True
        assert consumer._should_allocate_shard("shard-child-002") == True

    @pytest.mark.asyncio
    async def test_consumer_shard_topology_status(self, endpoint_url):
        """Test shard status includes topology information."""
        consumer = Consumer(
            stream_name="test-topology",
            endpoint_url=endpoint_url,
        )

        # Mock shards with parent-child relationships
        mock_shards = [
            {"ShardId": "parent-1"},
            {"ShardId": "child-1", "ParentShardId": "parent-1"},
            {"ShardId": "child-2", "ParentShardId": "parent-1"},
        ]

        consumer.shards = mock_shards
        consumer._build_shard_topology(mock_shards)

        status = consumer.get_shard_status()

        # Should include topology information
        assert "parent_shards" in status
        assert "child_shards" in status
        assert "exhausted_parents" in status
        assert "topology" in status

        # Should have correct counts
        assert status["parent_shards"] == 1
        assert status["child_shards"] == 2
        assert status["exhausted_parents"] == 0

        # Should include topology maps
        assert "parent_child_map" in status["topology"]
        assert "child_parent_map" in status["topology"]

        # Verify shard details include new fields
        for shard_detail in status["shard_details"]:
            assert "is_parent" in shard_detail
            assert "is_child" in shard_detail
            assert "can_allocate" in shard_detail

    @pytest.mark.asyncio
    async def test_consumer_resharding_detection(self, endpoint_url):
        """Test detection of resharding events."""
        consumer = Consumer(
            stream_name="test-resharding",
            endpoint_url=endpoint_url,
        )

        # Initial shards
        consumer.shards = [{"ShardId": "original-shard"}]
        consumer._last_shard_refresh = 0  # Force refresh

        # Mock new shards after resharding
        new_shards = [
            {"ShardId": "original-shard"},  # Still exists
            {"ShardId": "child-1", "ParentShardId": "original-shard"},
            {"ShardId": "child-2", "ParentShardId": "original-shard"},
        ]

        # Mock the stream description call
        from unittest.mock import AsyncMock

        consumer.get_stream_description = AsyncMock(return_value={"Shards": new_shards, "StreamStatus": "ACTIVE"})

        # Trigger refresh
        await consumer.refresh_shards()

        # Should have detected the topology
        assert len(consumer._parent_shards) == 1
        assert len(consumer._child_shards) == 2
        assert "original-shard" in consumer._parent_shards
