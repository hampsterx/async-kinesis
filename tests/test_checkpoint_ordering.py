"""Tests for checkpoint ordering safety.

Proves the checkpoint-before-processing bug exists (regression tests),
then validates the deferred checkpoint fix, shard deallocation ordering,
and checkpoint_interval debouncing.
"""

import asyncio
from unittest.mock import AsyncMock, call

import pytest

from kinesis import Consumer
from kinesis.checkpointers import MemoryCheckPointer
from kinesis.consumer import ShardStats
from kinesis.utils import Throttler


def _make_mock_checkpointer(**overrides):
    """AsyncMock checkpointer with standard method stubs.

    All methods are AsyncMock except is_allocated (sync, returns True).
    Pass keyword overrides to replace individual attributes.
    """
    cp = AsyncMock()
    cp.is_allocated = lambda sid: True
    for k, v in overrides.items():
        setattr(cp, k, v)
    return cp


# ---------------------------------------------------------------------------
# Deferred checkpoint execution (Change 1)
# ---------------------------------------------------------------------------


class TestDeferredCheckpointExecution:
    """Core correctness tests for deferred checkpoint."""

    @pytest.mark.asyncio
    async def test_deferred_checkpoint_commits_on_next_anext(self, mock_consumer):
        """Checkpoint sentinel is deferred, then committed on next __anext__ call."""
        consumer = mock_consumer()
        consumer.checkpointer = _make_mock_checkpointer()

        await consumer.queue.put({"msg": "A1"})
        await consumer.queue.put({"__CHECKPOINT__": {"ShardId": "shard-0", "SequenceNumber": "100"}})
        await consumer.queue.put({"msg": "A2"})

        item1 = await consumer.__anext__()
        assert item1 == {"msg": "A1"}
        consumer.checkpointer.checkpoint.assert_not_awaited()

        # Dequeues checkpoint (defers it), yields A2
        item2 = await consumer.__anext__()
        assert item2 == {"msg": "A2"}
        consumer.checkpointer.checkpoint.assert_not_awaited()

        # Commits the deferred checkpoint at entry, yields A3
        await consumer.queue.put({"msg": "A3"})
        item3 = await consumer.__anext__()
        assert item3 == {"msg": "A3"}
        consumer.checkpointer.checkpoint.assert_awaited_once_with("shard-0", "100")

    @pytest.mark.asyncio
    async def test_crash_after_sentinel_no_checkpoint(self, mock_consumer):
        """If user stops iterating after the checkpoint sentinel has been deferred,
        the checkpoint is pending but NOT committed."""
        consumer = mock_consumer()
        consumer.checkpointer = _make_mock_checkpointer()

        await consumer.queue.put({"msg": "A1"})
        await consumer.queue.put({"__CHECKPOINT__": {"ShardId": "shard-0", "SequenceNumber": "100"}})
        await consumer.queue.put({"msg": "A2"})

        await consumer.__anext__()  # A1
        await consumer.__anext__()  # defers checkpoint, yields A2

        # Checkpoint is deferred but not committed — simulates user crash here
        assert consumer._deferred_checkpoints == {"shard-0": "100"}
        consumer.checkpointer.checkpoint.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_multiple_checkpoints_last_wins(self, mock_consumer):
        """When consecutive checkpoint sentinels queue up, the last sequence wins."""
        consumer = mock_consumer()
        consumer.checkpointer = _make_mock_checkpointer()

        await consumer.queue.put({"msg": "A1"})
        await consumer.queue.put({"__CHECKPOINT__": {"ShardId": "shard-0", "SequenceNumber": "100"}})
        await consumer.queue.put({"__CHECKPOINT__": {"ShardId": "shard-0", "SequenceNumber": "200"}})
        await consumer.queue.put({"msg": "A2"})

        await consumer.__anext__()  # A1
        await consumer.__anext__()  # defers ckpt(100), then ckpt(200) overwrites, yields A2

        await consumer.queue.put({"msg": "A3"})
        await consumer.__anext__()  # commits 200, yields A3

        consumer.checkpointer.checkpoint.assert_awaited_once_with("shard-0", "200")

    @pytest.mark.asyncio
    async def test_close_flushes_pending_checkpoint(self, mock_consumer):
        """close() commits deferred checkpoint before deallocating shards."""
        consumer = mock_consumer()
        checkpointer = MemoryCheckPointer(name="test")
        await checkpointer.allocate("shard-0")
        consumer.checkpointer = checkpointer

        await consumer.queue.put({"msg": "A1"})
        await consumer.queue.put({"__CHECKPOINT__": {"ShardId": "shard-0", "SequenceNumber": "100"}})
        await consumer.queue.put({"msg": "A2"})

        await consumer.__anext__()  # A1
        await consumer.__anext__()  # defers checkpoint, yields A2

        assert checkpointer.get_checkpoint("shard-0") == {"sequence": None, "active": True}

        await consumer.close()

        assert checkpointer.get_checkpoint("shard-0")["sequence"] == "100"

    @pytest.mark.asyncio
    async def test_no_checkpointer_skips_checkpoint_processing(self, mock_consumer):
        """Without a checkpointer, checkpoint sentinels are silently consumed."""
        consumer = mock_consumer()
        consumer.checkpointer = None

        await consumer.queue.put({"__CHECKPOINT__": {"ShardId": "shard-0", "SequenceNumber": "100"}})
        await consumer.queue.put({"msg": "data"})

        item = await consumer.__anext__()
        assert item == {"msg": "data"}


# ---------------------------------------------------------------------------
# Queue put timeout fix
# ---------------------------------------------------------------------------


class TestQueuePutTimeoutFix:
    """Verify LastSequenceNumber only advances to last successfully enqueued record."""

    @pytest.mark.asyncio
    async def test_queue_put_timeout_no_sequence_advance(self, mock_consumer):
        """When queue.put() times out mid-batch, LastSequenceNumber tracks only
        the last successfully enqueued record, not the last fetched."""
        consumer = mock_consumer(sleep_time_no_records=0)
        consumer.checkpointer = MemoryCheckPointer(name="test")
        await consumer.checkpointer.allocate("shard-0")
        consumer.refresh_shards = AsyncMock()
        consumer.get_records = AsyncMock(return_value=None)

        # Make queue.put fail after the first successful put
        put_count = 0
        original_put = consumer.queue.put

        async def failing_put(item):
            nonlocal put_count
            put_count += 1
            if put_count > 1:
                raise asyncio.TimeoutError("simulated queue full")
            await original_put(item)

        consumer.queue.put = failing_put

        # Set up shard with a completed fetch returning 3 records
        shard = consumer.shards[0]
        shard["ShardIterator"] = "iter-0"
        shard["stats"] = ShardStats()
        shard["throttler"] = Throttler(rate_limit=1, period=1)

        fetch_result = {
            "Records": [
                {"SequenceNumber": "100", "Data": b'{"msg": "r1"}'},
                {"SequenceNumber": "200", "Data": b'{"msg": "r2"}'},
                {"SequenceNumber": "300", "Data": b'{"msg": "r3"}'},
            ],
            "NextShardIterator": "iter-next",
        }
        fut = asyncio.get_running_loop().create_future()
        fut.set_result(fetch_result)
        shard["fetch"] = fut

        await consumer.fetch()

        # Only the first record was enqueued; sequence must not advance to 300
        assert shard.get("LastSequenceNumber") == "100"


# ---------------------------------------------------------------------------
# Shard deallocation ordering (Change 3)
# ---------------------------------------------------------------------------


class TestShardDeallocationOrdering:
    """Verify checkpoints flush before shard deallocation."""

    @pytest.mark.asyncio
    async def test_shard_exhaustion_flushes_checkpoint_before_deallocate(self, mock_consumer):
        """When a shard iterator is exhausted, pending checkpoint flushes
        before deallocate via the actual fetch() code path."""
        consumer = mock_consumer(sleep_time_no_records=0)
        checkpointer = _make_mock_checkpointer()
        consumer.checkpointer = checkpointer
        consumer.refresh_shards = AsyncMock()
        consumer.get_records = AsyncMock(return_value=None)

        # Pending deferred checkpoint for shard-0
        consumer._deferred_checkpoints["shard-0"] = "100"

        # Set up shard with completed fetch returning exhausted iterator
        shard = consumer.shards[0]
        shard["ShardIterator"] = "iter-0"
        shard["stats"] = ShardStats()
        shard["throttler"] = Throttler(rate_limit=1, period=1)

        fetch_result = {
            "Records": [],
            "NextShardIterator": None,  # Shard exhausted
        }
        fut = asyncio.get_running_loop().create_future()
        fut.set_result(fetch_result)
        shard["fetch"] = fut

        await consumer.fetch()

        # Checkpoint must have been called before deallocate
        checkpointer.assert_has_calls([
            call.checkpoint("shard-0", "100"),
            call.deallocate("shard-0"),
        ])

    @pytest.mark.asyncio
    async def test_shard_exhaustion_with_records_no_sentinel_enqueued(self, mock_consumer):
        """When a shard exhausts with a final batch of records, no checkpoint
        sentinel is enqueued (would race with deallocate). Records are in the
        queue for processing; the terminal batch replays on restart (at-least-once)."""
        consumer = mock_consumer(sleep_time_no_records=0)
        checkpointer = _make_mock_checkpointer()
        consumer.checkpointer = checkpointer
        consumer.refresh_shards = AsyncMock()
        consumer.get_records = AsyncMock(return_value=None)

        shard = consumer.shards[0]
        shard["ShardIterator"] = "iter-0"
        shard["stats"] = ShardStats()
        shard["throttler"] = Throttler(rate_limit=1, period=1)

        fetch_result = {
            "Records": [
                {"SequenceNumber": "100", "Data": b'{"msg": "r1"}'},
                {"SequenceNumber": "200", "Data": b'{"msg": "r2"}'},
            ],
            "NextShardIterator": None,  # Shard exhausted with records
        }
        fut = asyncio.get_running_loop().create_future()
        fut.set_result(fetch_result)
        shard["fetch"] = fut

        await consumer.fetch()

        # Deallocate should be called (no checkpoint for terminal batch)
        checkpointer.deallocate.assert_awaited_once_with("shard-0")

        # Records should be in the queue, but no checkpoint sentinel
        items = []
        while not consumer.queue.empty():
            items.append(consumer.queue.get_nowait())
        data_items = [i for i in items if isinstance(i, dict) and "msg" in i]
        checkpoint_items = [i for i in items if isinstance(i, dict) and "__CHECKPOINT__" in i]
        assert len(data_items) == 2
        assert checkpoint_items == [], "No sentinel for terminal batch (would race with deallocate)"

    @pytest.mark.asyncio
    async def test_close_flushes_then_deallocates(self, mock_consumer):
        """close() flushes all pending checkpoints before deallocating shards."""
        consumer = mock_consumer()
        checkpointer = MemoryCheckPointer(name="test")
        await checkpointer.allocate("shard-0")
        consumer.checkpointer = checkpointer

        consumer._deferred_checkpoints["shard-0"] = "500"

        await consumer.close()

        cp = checkpointer._items.get("shard-0")
        assert cp is not None
        assert cp["sequence"] == "500"


# ---------------------------------------------------------------------------
# Checkpoint interval (Change 2)
# ---------------------------------------------------------------------------


class TestCheckpointInterval:
    """Tests for consumer-level checkpoint debouncing."""

    @pytest.mark.asyncio
    async def test_checkpoint_interval_debounces(self, mock_consumer):
        """Checkpoint backend called at most once per interval, not on every batch."""
        consumer = mock_consumer(checkpoint_interval=0.5)
        checkpointer = _make_mock_checkpointer()
        consumer.checkpointer = checkpointer

        for i in range(5):
            await consumer.queue.put({"msg": f"r{i}"})
            await consumer.queue.put(
                {"__CHECKPOINT__": {"ShardId": "shard-0", "SequenceNumber": str((i + 1) * 100)}}
            )

        for _ in range(5):
            await consumer.__anext__()

        await asyncio.sleep(0.7)

        assert checkpointer.checkpoint.await_count <= 2

        await consumer.close()

    @pytest.mark.asyncio
    async def test_checkpoint_interval_none_immediate(self, mock_consumer):
        """checkpoint_interval=None preserves every-batch checkpoint behaviour."""
        consumer = mock_consumer(checkpoint_interval=None)
        checkpointer = _make_mock_checkpointer()
        consumer.checkpointer = checkpointer

        await consumer.queue.put({"msg": "A1"})
        await consumer.queue.put({"__CHECKPOINT__": {"ShardId": "shard-0", "SequenceNumber": "100"}})
        await consumer.queue.put({"msg": "A2"})
        await consumer.queue.put({"msg": "A3"})

        await consumer.__anext__()  # A1
        await consumer.__anext__()  # defers checkpoint, yields A2
        await consumer.__anext__()  # commits checkpoint immediately (no interval), yields A3

        checkpointer.checkpoint.assert_awaited_once_with("shard-0", "100")

    @pytest.mark.asyncio
    async def test_checkpoint_interval_flush_on_close(self, mock_consumer):
        """Pending interval-buffered checkpoints are flushed on close()."""
        consumer = mock_consumer(checkpoint_interval=60.0)  # Long interval, won't fire naturally
        checkpointer = _make_mock_checkpointer()
        consumer.checkpointer = checkpointer

        await consumer.queue.put({"msg": "A1"})
        await consumer.queue.put({"__CHECKPOINT__": {"ShardId": "shard-0", "SequenceNumber": "100"}})
        await consumer.queue.put({"msg": "A2"})
        await consumer.queue.put({"msg": "A3"})

        await consumer.__anext__()  # A1
        await consumer.__anext__()  # defers checkpoint, yields A2
        await consumer.__anext__()  # buffers checkpoint (interval mode), yields A3

        checkpointer.checkpoint.assert_not_awaited()

        await consumer.close()
        checkpointer.checkpoint.assert_awaited_once_with("shard-0", "100")

    @pytest.mark.asyncio
    async def test_checkpoint_interval_quiet_stream(self, mock_consumer):
        """Background flusher fires even when no new records arrive."""
        consumer = mock_consumer(checkpoint_interval=0.2)
        checkpointer = _make_mock_checkpointer()
        consumer.checkpointer = checkpointer

        await consumer.queue.put({"msg": "A1"})
        await consumer.queue.put({"__CHECKPOINT__": {"ShardId": "shard-0", "SequenceNumber": "100"}})
        await consumer.queue.put({"msg": "A2"})

        await consumer.__anext__()  # A1
        await consumer.__anext__()  # defers checkpoint, yields A2

        # Trigger deferred→buffered by calling __anext__ again
        await consumer.queue.put({"msg": "A3"})
        await consumer.__anext__()  # commits deferred into buffer, yields A3

        # Wait for background flusher
        await asyncio.sleep(0.4)

        checkpointer.checkpoint.assert_awaited_once_with("shard-0", "100")

        await consumer.close()

    @pytest.mark.asyncio
    async def test_checkpoint_interval_with_auto_checkpoint_false_raises(self):
        """checkpoint_interval + auto_checkpoint=False raises ValueError."""
        with pytest.raises(ValueError, match="mutually exclusive"):
            Consumer(
                stream_name="test-stream",
                endpoint_url="http://localhost:4567",
                skip_describe_stream=True,
                checkpoint_interval=5.0,
                checkpointer=AsyncMock(auto_checkpoint=False),
            )

    @pytest.mark.asyncio
    async def test_checkpoint_backend_raises_during_flush(self, mock_consumer):
        """Exception from checkpoint backend during flush propagates from close()."""
        consumer = mock_consumer(checkpoint_interval=0.2)
        checkpointer = _make_mock_checkpointer(
            checkpoint=AsyncMock(side_effect=RuntimeError("DB connection lost")),
        )
        consumer.checkpointer = checkpointer

        await consumer.queue.put({"msg": "A1"})
        await consumer.queue.put({"__CHECKPOINT__": {"ShardId": "shard-0", "SequenceNumber": "100"}})
        await consumer.queue.put({"msg": "A2"})
        await consumer.queue.put({"msg": "A3"})

        await consumer.__anext__()  # A1
        await consumer.__anext__()  # defers checkpoint, yields A2
        await consumer.__anext__()  # buffers checkpoint, yields A3

        with pytest.raises(RuntimeError, match="DB connection lost"):
            await consumer.close()
