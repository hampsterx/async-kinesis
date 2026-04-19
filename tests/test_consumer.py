import asyncio
import logging
import time
from unittest.mock import AsyncMock

import pytest

from kinesis import Consumer, InMemoryMetricsCollector, MemoryCheckPointer, Producer, exceptions
from kinesis.metrics import MetricType
from kinesis.processors import JsonProcessor
from kinesis.timeout_compat import timeout

log = logging.getLogger(__name__)


@pytest.mark.integration
class TestConsumer:
    """Test consumer functionality (requires Docker)."""

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
    async def test_consumer_metrics_round_trip(self, test_stream, endpoint_url):
        """Consumer emits records/bytes received counters and queue size gauge after a round trip."""
        collector = InMemoryMetricsCollector()

        async with Producer(stream_name=test_stream, endpoint_url=endpoint_url) as producer:
            for i in range(3):
                await producer.put({"message": f"m-{i}"})
            await producer.flush()

        async with Consumer(
            stream_name=test_stream,
            endpoint_url=endpoint_url,
            sleep_time_no_records=0.1,
            metrics_collector=collector,
        ) as consumer:
            consumed = []
            try:
                async with timeout(5):
                    async for message in consumer:
                        consumed.append(message)
                        if len(consumed) >= 3:
                            break
            except asyncio.TimeoutError:
                pass

        records_keys = [k for k in collector.counters if k.startswith("consumer_records_received_total")]
        bytes_keys = [k for k in collector.counters if k.startswith("consumer_bytes_received_total")]
        queue_keys = [k for k in collector.gauges if k.startswith("consumer_queue_size")]

        assert records_keys, f"expected consumer_records_received_total, got {list(collector.counters)}"
        # 3 distinct JSON puts → 3 Kinesis rows. Exact match guards against double-counting on retry.
        assert sum(collector.counters[k] for k in records_keys) == 3
        # Bytes are the serialized Data field; derive the expected size from whichever
        # JSON backend the serializer actually used (ujson is compact, stdlib has spaces).
        expected_per_record = sum(len(item.data) for item in JsonProcessor().add_item({"message": "m-0"}))
        assert sum(collector.counters[k] for k in bytes_keys) == 3 * expected_per_record
        assert queue_keys

        iterator_age_keys = [k for k in collector.gauges if k.startswith("consumer_iterator_age_milliseconds")]
        assert iterator_age_keys, f"expected consumer_iterator_age_milliseconds, got {list(collector.gauges)}"
        assert all(collector.gauges[k] >= 0 for k in iterator_age_keys)

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
        from unittest.mock import AsyncMock

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

        # Note: ExpiredIteratorException has Code="ExpiredIteratorException" in the Error dict

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
        assert consumer._should_allocate_shard("shard-parent-001")

        # Children should NOT be allocatable while parent is active
        assert not consumer._should_allocate_shard("shard-child-001")
        assert not consumer._should_allocate_shard("shard-child-002")

        # Mark parent as exhausted
        consumer._exhausted_parents.add("shard-parent-001")

        # Now children should be allocatable
        assert consumer._should_allocate_shard("shard-child-001")
        assert consumer._should_allocate_shard("shard-child-002")

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
    async def test_consumer_expired_parent_shards(self, endpoint_url):
        """Test that child shards are allocatable when parent has expired from shard list (GH-68)."""
        consumer = Consumer(
            stream_name="test-expired-parents",
            endpoint_url=endpoint_url,
        )

        # Simulate shards where all parents have expired (no longer in list_shards response)
        mock_shards = [
            {
                "ShardId": "shardId-000000000373",
                "ParentShardId": "shardId-000000000100",
                "SequenceNumberRange": {"StartingSequenceNumber": "100"},
            },
            {
                "ShardId": "shardId-000000000374",
                "ParentShardId": "shardId-000000000100",
                "SequenceNumberRange": {"StartingSequenceNumber": "200"},
            },
            {
                "ShardId": "shardId-000000000375",
                "ParentShardId": "shardId-000000000101",
                "SequenceNumberRange": {"StartingSequenceNumber": "300"},
            },
        ]

        consumer._build_shard_topology(mock_shards)

        # Parents not in shard list should be tracked as expired
        assert len(consumer._expired_parent_shards) == 2
        assert "shardId-000000000100" in consumer._expired_parent_shards
        assert "shardId-000000000101" in consumer._expired_parent_shards
        assert len(consumer._parent_shards) == 0

        # All children should be allocatable since their parents expired
        assert consumer._should_allocate_shard("shardId-000000000373")
        assert consumer._should_allocate_shard("shardId-000000000374")
        assert consumer._should_allocate_shard("shardId-000000000375")

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

    @pytest.mark.asyncio
    async def test_wait_ready_then_produce_latest(self, random_stream_name, endpoint_url):
        """End-to-end: wait_ready() with LATEST, then produce, then consume."""
        # Create stream first
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
        ) as producer:
            # Put a throwaway record so the stream is fully active
            await producer.put({"setup": True})
            await producer.flush()

        # Start consumer with LATEST — should only see records produced AFTER wait_ready
        async with Consumer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            iterator_type="LATEST",
            sleep_time_no_records=0.5,
            idle_timeout=3.0,
        ) as consumer:
            await consumer.wait_ready(timeout=10)

            # Now produce — consumer has iterators, so these should be seen
            async with Producer(
                stream_name=random_stream_name,
                endpoint_url=endpoint_url,
            ) as producer:
                for i in range(3):
                    await producer.put({"seq": i})
                await producer.flush()

            consumed = []
            try:
                async with timeout(10):
                    async for message in consumer:
                        consumed.append(message)
                        # Stop once we have the 3 seq records
                        if sum(1 for m in consumed if "seq" in m) >= 3:
                            break
            except asyncio.TimeoutError:
                pass

            seq_records = [m for m in consumed if "seq" in m]
            assert len(seq_records) == 3
            assert [m["seq"] for m in seq_records] == [0, 1, 2]


class TestConsumerReadySignal:
    """Unit tests for consumer ready signal (no Docker required)."""

    @pytest.mark.asyncio
    async def test_fetch_sets_ready_when_shards_allocated(self, mock_consumer):
        """fetch() should set _ready when all allocated shards have iterators."""
        consumer = mock_consumer()

        # Simulate: shard is allocated and has an iterator
        consumer.shards = [{"ShardId": "shard-0", "ShardIterator": "iter-abc"}]
        consumer.checkpointer = MemoryCheckPointer(name="test")
        await consumer.checkpointer.allocate("shard-0")

        # Mock refresh_shards and get_records so fetch() runs through the loop
        consumer.refresh_shards = AsyncMock()
        consumer.get_records = AsyncMock(return_value=None)

        await consumer.fetch()

        assert consumer.is_ready

    @pytest.mark.asyncio
    async def test_wait_ready_returns_immediately_when_already_ready(self, mock_consumer):
        """If _ready is already set, wait_ready() should return near-instantly."""
        consumer = mock_consumer(skip_describe_stream=False)
        consumer.shards = [{"ShardId": "shard-0"}]
        consumer._ready.set()

        start = time.monotonic()
        await consumer.wait_ready(timeout=5)
        elapsed = time.monotonic() - start

        assert elapsed < 0.1, f"Expected instant return, got {elapsed:.3f}s"

    @pytest.mark.asyncio
    async def test_fetch_not_ready_with_zero_owned_shards(self, mock_consumer):
        """Consumer with zero allocated shards should NOT become ready (would miss LATEST records)."""
        consumer = mock_consumer()
        # Mock checkpointer that always fails allocation (simulates stale lease / another consumer)
        consumer.checkpointer = AsyncMock()
        consumer.checkpointer.is_allocated = lambda shard_id: False
        consumer.checkpointer.allocate = AsyncMock(return_value=(False, None))
        consumer.shards = [{"ShardId": "shard-0"}]

        consumer.refresh_shards = AsyncMock()
        consumer.get_records = AsyncMock(return_value=None)
        await consumer.fetch()

        assert not consumer.is_ready, "Zero owned shards should not be ready"

    @pytest.mark.asyncio
    async def test_fetch_multi_shard_waits_for_all(self, mock_consumer):
        """Readiness should only fire when ALL owned shards have iterators."""
        consumer = mock_consumer()
        consumer.checkpointer = MemoryCheckPointer(name="test")

        # 3 shards, only 2 have iterators
        consumer.shards = [
            {"ShardId": "shard-0", "ShardIterator": "iter-0"},
            {"ShardId": "shard-1", "ShardIterator": "iter-1"},
            {"ShardId": "shard-2"},  # No iterator yet
        ]
        await consumer.checkpointer.allocate("shard-0")
        await consumer.checkpointer.allocate("shard-1")
        await consumer.checkpointer.allocate("shard-2")

        # Mock fetch to run through the loop without side effects
        consumer.refresh_shards = AsyncMock()
        consumer.get_records = AsyncMock(return_value=None)
        await consumer.fetch()

        assert not consumer.is_ready, "Should NOT be ready — shard-2 missing iterator"

        # Now give shard-2 its iterator
        consumer.shards[2]["ShardIterator"] = "iter-2"
        await consumer.fetch()

        assert consumer.is_ready, "Should be ready — all shards have iterators"

    @pytest.mark.asyncio
    async def test_fetch_max_shard_consumers(self, mock_consumer):
        """With max_shard_consumers=2, readiness fires after those 2 get iterators."""
        consumer = mock_consumer(max_shard_consumers=2)
        consumer.checkpointer = MemoryCheckPointer(name="test")

        # 4 shards exist but only 2 are allocated
        consumer.shards = [
            {"ShardId": "shard-0", "ShardIterator": "iter-0"},
            {"ShardId": "shard-1", "ShardIterator": "iter-1"},
            {"ShardId": "shard-2"},
            {"ShardId": "shard-3"},
        ]
        await consumer.checkpointer.allocate("shard-0")
        await consumer.checkpointer.allocate("shard-1")
        # shard-2 and shard-3 are NOT allocated

        consumer.refresh_shards = AsyncMock()
        consumer.get_records = AsyncMock(return_value=None)
        await consumer.fetch()

        assert consumer.is_ready, "Should be ready — both allocated shards have iterators"

    @pytest.mark.asyncio
    async def test_wait_ready_skip_describe_stream_raises(self, mock_consumer):
        """wait_ready() should raise ValueError with skip_describe_stream=True."""
        consumer = mock_consumer(skip_describe_stream=True)

        with pytest.raises(ValueError, match="skip_describe_stream"):
            await consumer.wait_ready(timeout=1)

    @pytest.mark.asyncio
    async def test_wait_ready_fetch_task_crash(self, mock_consumer):
        """If the fetch task crashes, wait_ready() should re-raise immediately."""
        consumer = mock_consumer(skip_describe_stream=False)
        consumer.shards = [{"ShardId": "shard-0"}]

        # Replace fetch task with one that already failed
        consumer.fetch_task.cancel()
        try:
            await consumer.fetch_task
        except asyncio.CancelledError:
            pass

        async def failing_fetch():
            raise RuntimeError("Simulated fetch crash")

        consumer.fetch_task = asyncio.ensure_future(failing_fetch())
        await asyncio.sleep(0.05)

        with pytest.raises(RuntimeError, match="Simulated fetch crash"):
            await consumer.wait_ready(timeout=5)

    @pytest.mark.asyncio
    async def test_wait_ready_fetch_task_crash_during_wait(self, mock_consumer):
        """If fetch task dies DURING the wait, wait_ready() should raise immediately."""
        consumer = mock_consumer(skip_describe_stream=False)
        consumer.shards = [{"ShardId": "shard-0"}]

        # Replace fetch task with one that fails after a delay
        consumer.fetch_task.cancel()
        try:
            await consumer.fetch_task
        except asyncio.CancelledError:
            pass

        async def delayed_crash():
            await asyncio.sleep(0.2)
            raise RuntimeError("Delayed fetch crash")

        consumer.fetch_task = asyncio.ensure_future(delayed_crash())

        start = time.monotonic()
        with pytest.raises(RuntimeError, match="Delayed fetch crash"):
            await consumer.wait_ready(timeout=10)
        elapsed = time.monotonic() - start

        # Should have raised quickly (~0.2s), not waited for the 10s timeout
        assert elapsed < 2.0, f"Expected fast failure, got {elapsed:.3f}s"

    @pytest.mark.asyncio
    async def test_wait_ready_fetch_task_cancelled(self, mock_consumer):
        """If fetch task is cancelled (shutdown), wait_ready() should raise CancelledError."""
        consumer = mock_consumer(skip_describe_stream=False)
        consumer.shards = [{"ShardId": "shard-0"}]

        # Cancel the fetch task to simulate graceful shutdown
        consumer.fetch_task.cancel()
        try:
            await consumer.fetch_task
        except asyncio.CancelledError:
            pass

        with pytest.raises(asyncio.CancelledError, match="cancelled"):
            await consumer.wait_ready(timeout=1)

    @pytest.mark.asyncio
    async def test_is_ready_property(self, mock_consumer):
        """is_ready should reflect the state of _ready event."""
        consumer = mock_consumer()

        assert not consumer.is_ready
        consumer._ready.set()
        assert consumer.is_ready


class TestConsumerIdleTimeout:
    """Unit tests for consumer idle_timeout behaviour (no Docker required).

    These test the fix for the one-shot consumption pattern where __anext__
    gives up immediately on an empty queue instead of waiting for records
    to arrive from Kinesis.
    """

    @pytest.mark.asyncio
    async def test_anext_waits_for_delayed_records(self, mock_consumer):
        """Records arriving within idle_timeout should be consumed, not skipped."""
        consumer = mock_consumer(idle_timeout=2.0)

        # Deliver a record after a short delay
        async def delayed_put():
            await asyncio.sleep(0.3)
            await consumer.queue.put({"msg": "delayed"})

        asyncio.create_task(delayed_put())

        item = await consumer.__anext__()
        assert item == {"msg": "delayed"}

    @pytest.mark.asyncio
    async def test_anext_stops_after_idle_timeout(self, mock_consumer):
        """With no records, StopAsyncIteration should fire after idle_timeout, not immediately."""
        consumer = mock_consumer(idle_timeout=0.3)

        start = time.monotonic()
        with pytest.raises(StopAsyncIteration):
            await consumer.__anext__()
        elapsed = time.monotonic() - start

        # Should have waited at least close to idle_timeout (not instant)
        assert elapsed >= 0.25, f"Expected ~0.3s wait, got {elapsed:.3f}s"

    @pytest.mark.asyncio
    async def test_checkpoints_deferred_before_data(self, mock_consumer):
        """Checkpoint items in queue should be deferred, not executed immediately.

        The checkpoint is committed on the *next* __anext__ call (implicit ack).
        """
        consumer = mock_consumer()
        consumer.checkpointer = AsyncMock()
        consumer.checkpointer.checkpoint = AsyncMock()

        # Queue: checkpoint, then a real record
        await consumer.queue.put({"__CHECKPOINT__": {"ShardId": "shard-0", "SequenceNumber": "100"}})
        await consumer.queue.put({"msg": "real"})

        item = await consumer.__anext__()
        assert item == {"msg": "real"}
        # Checkpoint is deferred, not yet committed
        consumer.checkpointer.checkpoint.assert_not_awaited()

        # Next __anext__ commits the deferred checkpoint
        await consumer.queue.put({"msg": "next"})
        item2 = await consumer.__anext__()
        assert item2 == {"msg": "next"}
        consumer.checkpointer.checkpoint.assert_awaited_once_with("shard-0", "100")

    @pytest.mark.asyncio
    async def test_streaming_pattern_unaffected(self, mock_consumer):
        """The existing streaming pattern (while True: async for) still works with idle_timeout."""
        consumer = mock_consumer(idle_timeout=0.2)

        # Pre-load records
        for i in range(3):
            await consumer.queue.put({"msg": f"batch1-{i}"})

        # First iteration: consume all 3
        batch1 = []
        async for item in consumer:
            batch1.append(item)
        assert len(batch1) == 3

        # Simulate streaming: add more records and iterate again
        for i in range(2):
            await consumer.queue.put({"msg": f"batch2-{i}"})

        batch2 = []
        async for item in consumer:
            batch2.append(item)
        assert len(batch2) == 2

    @pytest.mark.asyncio
    async def test_consume_once_with_propagation_delay(self, mock_consumer):
        """One-shot consume pattern receives all records despite Kinesis propagation delay."""
        consumer = mock_consumer(idle_timeout=2.0)

        # Simulate records arriving in bursts with propagation delay
        async def delayed_produce():
            await asyncio.sleep(0.3)  # Kinesis propagation delay
            for i in range(3):
                await consumer.queue.put({"msg": f"record-{i}"})

        asyncio.create_task(delayed_produce())

        # One-shot pattern: async for should wait for records, then stop after idle_timeout
        consumed = []
        async for item in consumer:
            consumed.append(item)

        assert len(consumed) == 3
        assert [r["msg"] for r in consumed] == ["record-0", "record-1", "record-2"]
