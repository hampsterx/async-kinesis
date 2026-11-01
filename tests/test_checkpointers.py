import asyncio
import logging
import os
import uuid
import warnings
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
    @pytest.mark.integration
    async def test_memory_checkpoint(self, endpoint_url):
        """Test memory checkpointer with multiple consumers (requires Docker)."""
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


@pytest.mark.asyncio
async def test_redis_cluster_checkpointer_constructs():
    """RedisCheckPointer(is_cluster=True) builds a cluster client on every supported redis.

    redis>=4.3.0 is the first to ship redis.asyncio.cluster, and 4.4.0 onward rejects
    unknown constructor arguments. Construction does not connect, so no cluster is needed.
    """
    # Skip only when redis itself is absent: an older redis must still fail the import below
    pytest.importorskip("redis")
    from redis.asyncio.cluster import RedisCluster

    checkpointer = RedisCheckPointer(name="test", is_cluster=True)
    try:
        assert isinstance(checkpointer.client, RedisCluster)
        nodes_manager = checkpointer.client.nodes_manager
        # redis 4.3.0 stores it privately; later releases made it public and default it to True
        require_full_coverage = getattr(
            nodes_manager, "require_full_coverage", getattr(nodes_manager, "_require_full_coverage", None)
        )
        assert require_full_coverage is False
    finally:
        await checkpointer.close()


def _spy_on_client_close(checkpointer, on_close=None):
    """Wrap the method that closes this client on the installed redis, which still closes it for real."""
    client = checkpointer.client
    name = "aclose" if hasattr(client, "aclose") else "close"
    real_close = getattr(client, name)

    async def close_client(*args, **kwargs):
        if on_close:
            on_close()
        return await real_close(*args, **kwargs)

    return patch.object(client, name, AsyncMock(side_effect=close_client))


@pytest.mark.parametrize("is_cluster", [False, True], ids=["standalone", "cluster"])
class TestRedisCheckPointerClosesClient:
    """close() closes the client it built, on every supported redis.

    redis 4.3.0 to 5.0.0 only have close(); 5.0.1 added aclose() and deprecated close(). Both
    client types are built without connecting, so none of these need a server.
    """

    @pytest.fixture(autouse=True)
    def _needs_redis(self):
        pytest.importorskip("redis")

    async def test_close_closes_client_without_the_deprecated_method(self, is_cluster):
        checkpointer = RedisCheckPointer(name="test", is_cluster=is_cluster)

        with _spy_on_client_close(checkpointer) as close_client, warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            await checkpointer.close()

        close_client.assert_awaited_once_with()

    async def test_close_closes_client_when_deallocation_raises(self, is_cluster):
        checkpointer = RedisCheckPointer(name="test", is_cluster=is_cluster)
        checkpointer._items["shard-1"] = "seq-1"

        with (
            _spy_on_client_close(checkpointer) as close_client,
            patch.object(checkpointer.client, "set", AsyncMock(side_effect=RuntimeError("deallocate failed"))),
        ):
            with pytest.raises(RuntimeError, match="deallocate failed"):
                await checkpointer.close()

        close_client.assert_awaited_once_with()

    async def test_client_close_error_chains_the_deallocation_error(self, is_cluster):
        checkpointer = RedisCheckPointer(name="test", is_cluster=is_cluster)
        checkpointer._items["shard-1"] = "seq-1"
        close_name = "aclose" if hasattr(checkpointer.client, "aclose") else "close"

        with (
            patch.object(checkpointer.client, close_name, AsyncMock(side_effect=RuntimeError("client close failed"))),
            patch.object(checkpointer.client, "set", AsyncMock(side_effect=RuntimeError("deallocate failed"))),
        ):
            with pytest.raises(RuntimeError, match="client close failed") as excinfo:
                await checkpointer.close()

        assert str(excinfo.value.__context__) == "deallocate failed"

    async def test_close_closes_client_when_cancelled_during_deallocation(self, is_cluster):
        checkpointer = RedisCheckPointer(name="test", is_cluster=is_cluster)
        checkpointer._items["shard-1"] = "seq-1"
        deallocating = asyncio.Event()

        async def set_key(key, value):
            deallocating.set()
            await asyncio.Event().wait()

        with (
            _spy_on_client_close(checkpointer) as close_client,
            patch.object(checkpointer.client, "set", AsyncMock(side_effect=set_key)),
        ):
            close_task = asyncio.create_task(checkpointer.close())
            await deallocating.wait()
            close_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await close_task

        close_client.assert_awaited_once_with()

    async def test_close_closes_client_after_every_deallocation_settled(self, is_cluster):
        """A client closed under an in-flight write loses that shard's lease release on some redis
        versions and reopens a connection on others."""
        checkpointer = RedisCheckPointer(name="test", is_cluster=is_cluster)
        checkpointer._items.update({"failing": "seq-1", "slow": "seq-2"})
        events = []

        async def set_key(key, value):
            if key == checkpointer.get_key("failing"):
                raise RuntimeError("deallocate failed")
            await asyncio.sleep(0.01)
            events.append("slow shard released")

        with (
            _spy_on_client_close(checkpointer, lambda: events.append("client closed")),
            patch.object(checkpointer.client, "set", AsyncMock(side_effect=set_key)),
        ):
            with pytest.raises(RuntimeError, match="deallocate failed"):
                await checkpointer.close()

        assert events == ["slow shard released", "client closed"]


@pytest.mark.asyncio
@skip_if_no_redis
async def test_redis_close_disconnects_from_server():
    """The connection is gone from the server's own client list, and a second close() is harmless."""
    from redis.asyncio import Redis

    observer = Redis(host=os.environ["REDIS_HOST"], port=int(os.environ.get("REDIS_PORT", "6379")))
    checkpointer = RedisCheckPointer(name=f"test-{str(uuid.uuid4())[0:8]}")

    async def server_has_connection(client_id):
        return any(int(client["id"]) == client_id for client in await observer.client_list())

    try:
        await checkpointer.allocate("shard-1")
        client_id = await checkpointer.client.client_id()
        assert await server_has_connection(client_id)

        await checkpointer.close()
        await checkpointer.close()

        # The server notices the disconnect on its next event loop pass, not synchronously.
        for _ in range(50):
            if not await server_has_connection(client_id):
                break
            await asyncio.sleep(0.01)
        assert not await server_has_connection(client_id)
    finally:
        await (observer.aclose() if hasattr(observer, "aclose") else observer.close())
