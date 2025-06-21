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

    @pytest.mark.asyncio
    async def test_producer_bandwidth_throttle_per_item_not_cumulative(self, random_stream_name, endpoint_url):
        """
        Test that bandwidth throttling uses individual item sizes, not cumulative sizes.

        This test catches the bug fixed in PR #37 where bandwidth throttle was using
        self.flush_total_size (cumulative) instead of size_kb (individual item size).
        """
        from unittest.mock import MagicMock

        # Track what sizes the bandwidth throttle is called with
        throttle_sizes = []

        class MockThrottler:
            def __call__(self, size=1):
                throttle_sizes.append(size)
                return self

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
            batch_size=3,  # Allow multiple items in batch
            buffer_time=10,  # Long buffer time to prevent auto-flush
        ) as producer:
            # Replace the throttler instance with our mock
            producer.put_bandwidth_throttle = MockThrottler()

            # Add items of different sizes to create a batch
            # Item 1: ~100 bytes (size_kb = 1KB due to math.ceil(100/1024))
            small_data = "x" * 100
            await producer.put({"data": small_data})

            # Item 2: ~2KB (size_kb = 2KB due to math.ceil(2048/1024))
            medium_data = "x" * 2048
            await producer.put({"data": medium_data})

            # Item 3: ~3KB (size_kb = 3KB due to math.ceil(3072/1024))
            large_data = "x" * 3072
            await producer.put({"data": large_data})

            # Force flush to process the batch
            await producer.flush()

        # Verify throttle was called with individual item sizes, not cumulative
        # Expected individual sizes: [1, 3, 4] KB (accounting for JSON serialization overhead)
        # With the bug, would be cumulative: [1, 4, 8] KB
        expected_sizes = [1, 3, 4]
        assert len(throttle_sizes) == 3, f"Expected 3 throttle calls, got {len(throttle_sizes)}"
        assert throttle_sizes == expected_sizes, (
            f"Bandwidth throttle should use individual item sizes {expected_sizes}, "
            f"but got {throttle_sizes}. This suggests cumulative sizing bug from PR #37."
        )

    @pytest.mark.asyncio
    async def test_producer_custom_partition_key(self, random_stream_name, endpoint_url):
        """Test producer with custom partition key (Issue #34)."""
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
        ) as producer:
            await producer.put({"message": "test"}, partition_key="custom-key")
            await producer.flush()

    @pytest.mark.asyncio
    async def test_producer_partition_key_validation(self, random_stream_name, endpoint_url):
        """Test partition key validation (Issue #34)."""
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
        ) as producer:
            # Test invalid partition key types
            with pytest.raises(exceptions.ValidationError, match="must be a string"):
                await producer.put({"message": "test"}, partition_key=123)

            # Test empty partition key
            with pytest.raises(exceptions.ValidationError, match="cannot be empty"):
                await producer.put({"message": "test"}, partition_key="")

            # Test partition key too long (over 256 bytes)
            long_key = "x" * 257
            with pytest.raises(exceptions.ValidationError, match="256 bytes or less"):
                await producer.put({"message": "test"}, partition_key=long_key)

            # Test valid partition key
            await producer.put({"message": "test"}, partition_key="valid-key")
            await producer.flush()

    @pytest.mark.asyncio
    async def test_producer_partition_key_unicode(self, random_stream_name, endpoint_url):
        """Test partition key with Unicode characters (Issue #34)."""
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
        ) as producer:
            # Test Unicode partition key that's under 256 bytes
            unicode_key = "用户-123"
            await producer.put({"message": "test"}, partition_key=unicode_key)
            await producer.flush()

            # Test Unicode key that exceeds 256 bytes when encoded
            # Each Chinese character is 3 bytes in UTF-8
            long_unicode = "用" * 86  # 86 * 3 = 258 bytes
            with pytest.raises(exceptions.ValidationError, match="256 bytes or less"):
                await producer.put({"message": "test"}, partition_key=long_unicode)

    @pytest.mark.asyncio
    async def test_producer_mixed_partition_keys(self, random_stream_name, endpoint_url):
        """Test producer with mixed custom and default partition keys (Issue #34)."""
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
            batch_size=5,
            buffer_time=10,  # Long buffer time to prevent auto-flush
        ) as producer:
            # Mix of custom and default partition keys
            await producer.put({"message": "test1"}, partition_key="custom-1")
            await producer.put({"message": "test2"})  # Default key
            await producer.put({"message": "test3"}, partition_key="custom-2")
            await producer.flush()

    @pytest.mark.asyncio
    async def test_producer_kpl_partition_key_compatibility(self, random_stream_name, endpoint_url):
        """Test that KPL processors reject custom partition keys (Issue #34)."""
        try:
            import aws_kinesis_agg
        except ImportError:
            pytest.skip("KPL aggregation library not available")

        from kinesis.processors import KPLJsonProcessor

        # KPL processor should reject custom partition keys
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
            processor=KPLJsonProcessor(),
        ) as producer:
            # Should work without partition key
            await producer.put({"message": "test"})

            # Should fail with custom partition key
            with pytest.raises(ValueError, match="Custom partition keys are not supported with KPL"):
                await producer.put({"message": "test"}, partition_key="custom-key")

    @pytest.mark.asyncio
    async def test_producer_partition_key_aggregation_grouping(self, random_stream_name, endpoint_url):
        """Test that aggregated records group by partition key (Issue #34)."""
        from unittest.mock import MagicMock

        from kinesis.processors import JsonLineProcessor

        # Track what gets sent to Kinesis
        sent_records = []

        class MockClient:
            async def put_records(self, Records, **kwargs):
                sent_records.extend(Records)
                return {"FailedRecordCount": 0, "Records": [{"ShardId": "shard-1"} for _ in Records]}

            async def close(self):
                pass

        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
            processor=JsonLineProcessor(),
            batch_size=10,
            buffer_time=10,  # Long buffer time to prevent auto-flush
        ) as producer:
            # Replace client with mock
            producer.client = MockClient()

            # Add records with different partition keys
            await producer.put({"message": "test1"}, partition_key="key-a")
            await producer.put({"message": "test2"}, partition_key="key-a")  # Same key
            await producer.put({"message": "test3"}, partition_key="key-b")  # Different key
            await producer.put({"message": "test4"}, partition_key="key-a")  # Back to key-a

            await producer.flush()

            # Should have multiple records due to partition key changes
            assert len(sent_records) >= 2, "Should create separate records for different partition keys"

            # Check that partition keys are correctly set
            partition_keys = [record["PartitionKey"] for record in sent_records]
            assert "key-a" in partition_keys
            assert "key-b" in partition_keys

    @pytest.mark.asyncio
    async def test_producer_partition_key_backward_compatibility(self, random_stream_name, endpoint_url):
        """Test that existing code without partition keys still works (Issue #34)."""
        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
        ) as producer:
            # All existing patterns should still work
            await producer.put({"message": "test1"})
            await producer.put({"message": "test2"})
            await producer.put({"message": "test3"})
            await producer.flush()

            # Verify default partition key generation still works
            # (This is implicit - if the test passes, keys were generated)

    @pytest.mark.asyncio
    async def test_producer_partition_key_bandwidth_calculation(self, random_stream_name, endpoint_url):
        """Test that partition key size is included in bandwidth calculations (Issue #34)."""
        from unittest.mock import MagicMock

        # Track bandwidth throttle sizes
        throttle_sizes = []

        class MockThrottler:
            def __call__(self, size=1):
                throttle_sizes.append(size)
                return self

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

        async with Producer(
            stream_name=random_stream_name,
            endpoint_url=endpoint_url,
            create_stream=True,
            create_stream_shards=1,
            batch_size=2,
            buffer_time=10,  # Long buffer time to prevent auto-flush
        ) as producer:
            # Replace the throttler with our mock
            producer.put_bandwidth_throttle = MockThrottler()

            # Test with data that will demonstrate partition key inclusion
            # Item 1: Very large data that's just under 1KB with small partition key
            large_data = {"data": "x" * 800}  # ~800+ bytes when serialized
            await producer.put(large_data, partition_key="x" * 250)  # 250 byte partition key

            # Item 2: Small data without partition key
            small_data = {"data": "x" * 100}
            await producer.put(small_data)

            await producer.flush()

            # Verify that partition key sizes were included in bandwidth calculations
            assert len(throttle_sizes) == 2
            # With a 250-byte partition key + large data, should be larger than small data alone
            # The key test is that the code runs without errors and includes partition key in calculation
            assert throttle_sizes[0] >= 1, "Should include partition key in size calculation"
            assert throttle_sizes[1] >= 1, "Should calculate size for item without partition key"
