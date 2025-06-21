import asyncio
import logging
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from kinesis import Consumer, MemoryCheckPointer, RedisCheckPointer
from tests.conftest import skip_if_no_redis

log = logging.getLogger(__name__)


class TestCheckpointers:
    """Test checkpointer functionality."""

    @staticmethod
    def patch_consumer_fetch(consumer):
        """Helper to patch consumer methods for testing."""

        async def get_shard_iterator(shard_id, last_sequence_number=None):
            log.info(f"getting shard iterator for {shard_id} @ {last_sequence_number}")
            return True

        consumer.get_shard_iterator = get_shard_iterator

        async def get_records(shard):
            log.info(f"get records shard={shard['ShardId']}")
            return {}

        consumer.get_records = get_records
        consumer.is_fetching = True

    @pytest.mark.asyncio
    async def test_memory_checkpoint(self, endpoint_url):
        """Test memory checkpointer with multiple consumers."""
        # first consumer
        checkpointer = MemoryCheckPointer(name="test")

        consumer_a = Consumer(
            stream_name=None,
            checkpointer=checkpointer,
            max_shard_consumers=1,
            endpoint_url=endpoint_url,
        )

        self.patch_consumer_fetch(consumer_a)

        consumer_a.shards = [{"ShardId": "test-1"}, {"ShardId": "test-2"}]

        await consumer_a.fetch()

        shards = [s["ShardId"] for s in consumer_a.shards if s.get("stats")]

        # Expect only one shard assigned as max = 1
        assert shards == ["test-1"]

        # second consumer (note: max_shard_consumers needs to be 2 as uses checkpointer to get allocated shards)
        consumer_b = Consumer(
            stream_name=None,
            checkpointer=checkpointer,
            max_shard_consumers=2,
            endpoint_url=endpoint_url,
        )

        self.patch_consumer_fetch(consumer_b)

        consumer_b.shards = [{"ShardId": "test-1"}, {"ShardId": "test-2"}]

        await consumer_b.fetch()

        shards = [s["ShardId"] for s in consumer_b.shards if s.get("stats")]

        # Expect only one shard assigned as max = 1
        assert shards == ["test-2"]

    @pytest.mark.asyncio
    @skip_if_no_redis
    async def test_redis_checkpoint_locking(self):
        """Test Redis checkpointer shard locking."""
        name = f"test-{str(uuid.uuid4())[0:8]}"

        # first consumer
        checkpointer_a = RedisCheckPointer(name=name, id="proc-1")

        # second consumer
        checkpointer_b = RedisCheckPointer(name=name, id="proc-2")

        # try to allocate the same shard
        result = await asyncio.gather(*[checkpointer_a.allocate("test"), checkpointer_b.allocate("test")])

        result = list(sorted([x[0] for x in result]))

        # Expect only one to have succeeded
        assert result == [False, True]

        await checkpointer_a.close()
        await checkpointer_b.close()

    @pytest.mark.asyncio
    @skip_if_no_redis
    async def test_redis_checkpoint_reallocate(self):
        """Test Redis checkpointer reallocation after deallocation."""
        name = f"test-{str(uuid.uuid4())[0:8]}"

        # first consumer
        checkpointer_a = RedisCheckPointer(name=name, id="proc-1")

        await checkpointer_a.allocate("test")

        # checkpoint
        await checkpointer_a.checkpoint("test", "123")

        # stop on this shard
        await checkpointer_a.deallocate("test")

        # second consumer
        checkpointer_b = RedisCheckPointer(name=name, id="proc-2")

        success, sequence = await checkpointer_b.allocate("test")

        assert success is True
        assert sequence == "123"

        await checkpointer_b.close()

        assert checkpointer_b.get_all_checkpoints() == {}

        await checkpointer_a.close()

    @pytest.mark.asyncio
    @skip_if_no_redis
    async def test_redis_checkpoint_heartbeat(self):
        """Test Redis checkpointer heartbeat functionality."""
        name = f"test-{str(uuid.uuid4())[0:8]}"

        checkpointer = RedisCheckPointer(name=name, heartbeat_frequency=0.5)

        await checkpointer.allocate("test")
        await checkpointer.checkpoint("test", "123")

        await asyncio.sleep(1)

        await checkpointer.close()

    @pytest.mark.asyncio
    async def test_memory_checkpointer_basic_operations(self):
        """Test basic memory checkpointer operations."""
        checkpointer = MemoryCheckPointer(name="test")

        # Test allocation
        success, sequence = await checkpointer.allocate("shard-1")
        assert success is True
        assert sequence is None

        # Test checkpointing
        await checkpointer.checkpoint("shard-1", "seq-123")

        # Test deallocation and reallocation
        await checkpointer.deallocate("shard-1")
        success, sequence = await checkpointer.allocate("shard-1")
        assert success is True
        assert sequence == "seq-123"

        # Test get_all_checkpoints
        checkpoints = checkpointer.get_all_checkpoints()
        assert "shard-1" in checkpoints
        assert checkpoints["shard-1"]["sequence"] == "seq-123"

    @pytest.mark.asyncio
    @skip_if_no_redis
    async def test_redis_checkpointer_basic_operations(self):
        """Test basic Redis checkpointer operations."""
        name = f"test-{str(uuid.uuid4())[0:8]}"
        checkpointer = RedisCheckPointer(name=name, id="test-proc")

        try:
            # Test allocation
            success, sequence = await checkpointer.allocate("shard-1")
            assert success is True
            assert sequence is None

            # Test checkpointing
            await checkpointer.checkpoint("shard-1", "seq-123")

            # Test deallocation and reallocation
            await checkpointer.deallocate("shard-1")
            success, sequence = await checkpointer.allocate("shard-1")
            assert success is True
            assert sequence == "seq-123"

        finally:
            await checkpointer.close()

    @pytest.mark.asyncio
    async def test_memory_checkpointer_concurrent_allocation(self):
        """Test memory checkpointer with concurrent allocation attempts."""
        checkpointer = MemoryCheckPointer(name="test")

        # Test concurrent allocation of the same shard
        results = await asyncio.gather(
            checkpointer.allocate("shard-1"),
            checkpointer.allocate("shard-1"),
            checkpointer.allocate("shard-1"),
        )

        # Only one should succeed
        successes = [result[0] for result in results]
        assert successes.count(True) == 1
        assert successes.count(False) == 2
