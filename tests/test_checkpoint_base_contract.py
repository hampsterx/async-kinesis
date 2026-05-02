"""Base-class contract tests for BaseHeartbeatCheckPointer.

Proves that checkpoint() / manual_checkpoint() behave identically regardless
of backend. Each test drives a minimal fake subclass whose _checkpoint is a
controllable in-memory write with an opt-in failure map. If both concrete
backends (Redis, DynamoDB) inherit these methods unchanged, the contract
guaranteed by these tests is what they provide.

Covers the five cases from PLAN_ISSUE_82:
  1. auto_checkpoint=False buffers without writing.
  2. Successful flush pops only entries whose buffered sequence still matches.
  3. Failed flush leaves the offending shard in _manual_checkpoints.
  4. Mixed success/failure raises CheckpointFlushError listing every failure,
     __cause__ = first failure.
  5. Concurrent checkpoint() buffering a newer sequence mid-flush is retained.
"""

import asyncio
from typing import Dict, Optional, Set

import pytest

from kinesis.checkpointers import BaseHeartbeatCheckPointer, CheckpointFlushError


class FakeHeartbeatCheckPointer(BaseHeartbeatCheckPointer):
    """Minimal BaseHeartbeatCheckPointer subclass for contract tests.

    _checkpoint writes to an in-memory dict. A per-shard failure set forces
    _checkpoint to raise for those shards, simulating a backend write error.
    Heartbeat is effectively disabled (interval well beyond any test
    duration); the task is cancelled in stop().

    `mid_flush_hook` is an optional async callable invoked inside each
    _checkpoint call before the write lands, used by the concurrency test
    to inject a newer buffered sequence mid-flush.
    """

    def __init__(self, auto_checkpoint: bool = True):
        super().__init__(
            name="fake",
            id="test",
            heartbeat_frequency=3600,
            auto_checkpoint=auto_checkpoint,
        )
        self.writes: Dict[str, str] = {}
        self.failures: Set[str] = set()
        self.mid_flush_hook = None

    async def stop(self) -> None:
        self.heartbeat_task.cancel()
        try:
            await self.heartbeat_task
        except asyncio.CancelledError:
            pass

    def get_key(self, shard_id: str) -> str:
        return shard_id

    def get_ts(self) -> int:
        return 0

    async def do_heartbeat(self, key, value) -> None:
        return None

    async def allocate(self, shard_id: str):
        self._items[shard_id] = None
        return True, None

    async def deallocate(self, shard_id: str) -> None:
        self._items.pop(shard_id, None)

    async def _checkpoint(self, shard_id: str, sequence: str) -> None:
        try:
            if self.mid_flush_hook is not None:
                await self.mid_flush_hook(shard_id, sequence)
            if shard_id in self.failures:
                raise RuntimeError(f"forced failure for {shard_id}")
            self.writes[shard_id] = sequence
            self._items[shard_id] = sequence
        except Exception:
            self._emit_checkpoint_failure(shard_id)
            raise
        self._emit_checkpoint_success(shard_id)


@pytest.fixture
async def cp():
    checkpointer = FakeHeartbeatCheckPointer(auto_checkpoint=False)
    try:
        yield checkpointer
    finally:
        await checkpointer.stop()


@pytest.fixture
async def auto_cp():
    checkpointer = FakeHeartbeatCheckPointer(auto_checkpoint=True)
    try:
        yield checkpointer
    finally:
        await checkpointer.stop()


class TestBaseCheckpointContract:
    @pytest.mark.asyncio
    async def test_auto_checkpoint_false_buffers_without_writing(self, cp):
        """Case 1: buffered checkpoints never hit _checkpoint."""
        await cp.checkpoint("shard-0", "seq-1")
        await cp.checkpoint("shard-1", "seq-2")

        assert cp.writes == {}
        assert cp._manual_checkpoints == {"shard-0": "seq-1", "shard-1": "seq-2"}

    @pytest.mark.asyncio
    async def test_auto_checkpoint_true_writes_immediately(self, auto_cp):
        """Contract companion to case 1: auto path drives _checkpoint."""
        await auto_cp.checkpoint("shard-0", "seq-1")

        assert auto_cp.writes == {"shard-0": "seq-1"}
        assert auto_cp._manual_checkpoints == {}

    @pytest.mark.asyncio
    async def test_successful_flush_pops_all(self, cp):
        """Case 2: a clean flush leaves no buffered entries."""
        await cp.checkpoint("shard-0", "seq-1")
        await cp.checkpoint("shard-1", "seq-2")

        await cp.manual_checkpoint()

        assert cp.writes == {"shard-0": "seq-1", "shard-1": "seq-2"}
        assert cp._manual_checkpoints == {}

    @pytest.mark.asyncio
    async def test_failed_flush_retains_only_failing_shard(self, cp):
        """Case 3: the offending shard stays buffered; peers are popped."""
        await cp.checkpoint("shard-0", "seq-1")
        await cp.checkpoint("shard-1", "seq-2")
        await cp.checkpoint("shard-2", "seq-3")
        cp.failures.add("shard-1")

        with pytest.raises(CheckpointFlushError) as exc_info:
            await cp.manual_checkpoint()

        assert [sid for sid, _ in exc_info.value.errors] == ["shard-1"]
        assert cp._manual_checkpoints == {"shard-1": "seq-2"}
        assert cp.writes == {"shard-0": "seq-1", "shard-2": "seq-3"}

    @pytest.mark.asyncio
    async def test_mixed_failures_raise_compound_error(self, cp):
        """Case 4: every failing shard listed in .errors, __cause__ = first."""
        await cp.checkpoint("shard-0", "seq-1")
        await cp.checkpoint("shard-1", "seq-2")
        await cp.checkpoint("shard-2", "seq-3")
        cp.failures.update({"shard-0", "shard-2"})

        with pytest.raises(CheckpointFlushError) as exc_info:
            await cp.manual_checkpoint()

        failed_shards = [sid for sid, _ in exc_info.value.errors]
        assert failed_shards == ["shard-0", "shard-2"]
        assert exc_info.value.__cause__ is exc_info.value.errors[0][1]
        assert cp._manual_checkpoints == {"shard-0": "seq-1", "shard-2": "seq-3"}
        assert cp.writes == {"shard-1": "seq-2"}

    @pytest.mark.asyncio
    async def test_concurrent_newer_sequence_not_dropped(self, cp):
        """Case 5: a newer sequence buffered mid-flush survives the pop.

        checkpoint() running while manual_checkpoint() is awaiting _checkpoint
        overwrites _manual_checkpoints[shard] with a newer sequence. The
        value-match guard on the success-path pop must leave that newer
        sequence in the buffer rather than dropping it.
        """

        async def inject_newer(shard_id: str, sequence: str) -> None:
            if shard_id == "shard-0" and sequence == "seq-1":
                cp._manual_checkpoints["shard-0"] = "seq-2"

        await cp.checkpoint("shard-0", "seq-1")
        cp.mid_flush_hook = inject_newer

        await cp.manual_checkpoint()

        assert cp._manual_checkpoints == {"shard-0": "seq-2"}
        assert cp.writes == {"shard-0": "seq-1"}


class TestManualCheckpointConcurrency:
    """Issue #80 hazard 2: two tasks calling ``manual_checkpoint()`` must not
    double-flush the same buffered shard. The base-class lock around the
    flush body serialises them; only the first flusher writes any given
    shard's buffered sequence, the second observes a drained buffer."""

    @pytest.mark.asyncio
    async def test_concurrent_flushes_serialise_per_shard(self, cp):
        """Two ``manual_checkpoint()`` coroutines see each shard written
        exactly once across the pair, in a deterministic order pinned by an
        ``asyncio.Event`` injected mid-flush.

        Without the lock both flushers iterate the same buffer snapshot and
        each calls ``_checkpoint(shard, seq)`` once, producing two writes
        per shard. With the lock the second flusher waits, sees the
        shard already popped, and writes nothing.
        """

        await cp.checkpoint("shard-0", "seq-1")
        await cp.checkpoint("shard-1", "seq-2")

        write_log: list = []
        gate = asyncio.Event()
        first_in_flight = asyncio.Event()

        async def gated_checkpoint(shard_id: str, sequence: str) -> None:
            write_log.append((shard_id, sequence))
            cp.writes[shard_id] = sequence
            cp._items[shard_id] = sequence
            if not first_in_flight.is_set():
                first_in_flight.set()
                await gate.wait()
            cp._emit_checkpoint_success(shard_id)

        cp._checkpoint = gated_checkpoint  # type: ignore[method-assign]

        flush_a = asyncio.create_task(cp.manual_checkpoint())
        await first_in_flight.wait()
        flush_b = asyncio.create_task(cp.manual_checkpoint())
        # Yield enough times for flush_b to reach the lock and block on it.
        for _ in range(5):
            await asyncio.sleep(0)
        gate.set()
        await asyncio.gather(flush_a, flush_b)

        assert sorted(write_log) == [("shard-0", "seq-1"), ("shard-1", "seq-2")]
        assert cp._manual_checkpoints == {}
        assert cp.writes == {"shard-0": "seq-1", "shard-1": "seq-2"}

    @pytest.mark.asyncio
    async def test_second_flusher_sees_drained_buffer(self, cp):
        """A second ``manual_checkpoint()`` arriving after the first drains
        the buffer must return cleanly: empty buffer is a no-op, not an
        error."""

        await cp.checkpoint("shard-0", "seq-1")

        await cp.manual_checkpoint()
        # Second call against an already-drained buffer.
        await cp.manual_checkpoint()

        assert cp.writes == {"shard-0": "seq-1"}
        assert cp._manual_checkpoints == {}


class TestHeartbeatRobustness:
    """Issue #81: heartbeat iteration and close() must survive concurrent
    mutation / task death so shards don't leak."""

    @pytest.mark.asyncio
    async def test_heartbeat_survives_concurrent_items_mutation(self):
        """Without list(self._items.items()), a concurrent deallocate()-style
        pop during the awaited do_heartbeat raises
        RuntimeError: dictionary changed size during iteration.

        Doesn't claim to stop stale writes on de-allocated shards: that's a
        separate ownership-race problem (see PLAN_ISSUE_80). Only guards the
        iteration itself.
        """
        cp = FakeHeartbeatCheckPointer()
        # Drain the auto-started heartbeat task; we drive our own below.
        cp.heartbeat_task.cancel()
        try:
            await cp.heartbeat_task
        except asyncio.CancelledError:
            pass

        cp._items = {"shard-0": "seq-0", "shard-1": "seq-1"}
        cp.heartbeat_frequency = 0
        both_seen = asyncio.Event()
        seen = []

        async def mutating_heartbeat(key, value):
            seen.append(key)
            # Mid-tick: simulate concurrent deallocate() popping another shard.
            cp._items.pop("shard-1", None)
            if {"shard-0", "shard-1"}.issubset(seen):
                both_seen.set()

        cp.do_heartbeat = mutating_heartbeat

        task = asyncio.create_task(cp.heartbeat())
        try:
            # Without the list() snapshot, the iterator raises after the first
            # tick body pops shard-1 and both_seen never fires.
            await asyncio.wait_for(both_seen.wait(), timeout=1.0)
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Extra shard-0 observations from subsequent ticks before cancel are
        # fine; we only care that the first tick processed both snapshot
        # entries instead of raising RuntimeError mid-iteration.
        assert {"shard-0", "shard-1"}.issubset(seen), f"iteration aborted early (missing fix?): {seen}"

    @pytest.mark.asyncio
    async def test_close_deallocates_when_heartbeat_crashed(self):
        """If the heartbeat task died with a non-Cancel exception before
        close() was called, close() must still deallocate every shard.
        Guards against the PR #83 shape that awaited the task but only
        swallowed CancelledError.
        """

        class CrashingHeartbeatCheckPointer(FakeHeartbeatCheckPointer):
            async def heartbeat(self):
                raise RuntimeError("heartbeat dead")

        cp = CrashingHeartbeatCheckPointer()
        await cp.allocate("shard-0")
        await cp.allocate("shard-1")
        assert cp._items == {"shard-0": None, "shard-1": None}

        # Let the task run and die.
        for _ in range(20):
            await asyncio.sleep(0)
            if cp.heartbeat_task.done():
                break
        assert cp.heartbeat_task.done()

        # Without the broad `except Exception` in close(), awaiting the crashed
        # task re-raises RuntimeError and super().close() never runs.
        await cp.close()

        assert cp._items == {}
