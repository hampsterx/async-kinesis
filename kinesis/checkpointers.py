import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Protocol, Tuple, Union

from .metrics import MetricsCollector, MetricType, get_metrics_collector

log = logging.getLogger(__name__)

STANDALONE_STREAM_LABEL = "<standalone>"


class CheckPointer(Protocol):
    """Protocol for checkpointer implementations.

    Checkpointers track processing progress per shard so that a consumer
    can resume from the correct position after a restart. They also provide
    shard-level locking so that multiple consumers don't process the same
    shard concurrently.

    Metrics opt-in (optional): implementations that want to participate in
    ``consumer_checkpoint_success_total`` / ``consumer_checkpoint_failure_total``
    should also implement ``bind_metrics(collector, labels)`` and emit on their
    real backend-write path (see ``BaseCheckPointer`` for the reference pattern).
    The Consumer detects the method via ``hasattr`` and wires it at construction;
    implementations without ``bind_metrics`` still work but produce no checkpoint
    metrics.
    """

    async def allocate(self, shard_id: str) -> Tuple[bool, Optional[str]]:
        """Allocate a shard for processing.

        Returns (True, sequence) if allocation succeeded. The sequence is the
        last checkpointed position (None if no prior checkpoint exists).
        Returns (False, None) if the shard is owned by another consumer.

        Implementations must be safe to call multiple times for the same shard
        (idempotent if already allocated by this consumer).
        """
        ...

    async def deallocate(self, shard_id: str) -> None:
        """Release a shard (e.g., on shard closure or consumer shutdown).

        Must preserve the last checkpoint sequence for future consumers.
        Called when a shard's iterator is exhausted (resharding) or on
        consumer close(). The consumer guarantees all pending checkpoints
        for this shard are flushed before deallocate() is called.
        """
        ...

    async def checkpoint(self, shard_id: str, sequence_number: str) -> None:
        """Record processing progress for a shard.

        Called after the consumer has yielded all records up to sequence_number
        to the user and the user has returned control (called __anext__ again).
        At-least-once semantics: the last batch's records may be reprocessed
        on restart if close() was not called after the final iteration.

        Implementations should be idempotent and handle out-of-order calls
        gracefully (e.g., ignore a sequence older than the current checkpoint).

        If this method raises, the consumer will propagate the exception. The
        checkpoint is considered not persisted; the same records may be
        re-delivered on restart.
        """
        ...

    def get_all_checkpoints(self) -> Dict[str, str]:
        """Return all known checkpoints as {shard_id: sequence}.

        Used for monitoring and status reporting. May return stale data
        for eventually-consistent backends.
        """
        ...

    async def close(self) -> None:
        """Clean up resources and deallocate all owned shards.

        Implementations should flush any pending/buffered checkpoints
        before deallocating.
        """
        ...

    # NOTE: bind_metrics is intentionally NOT part of the Protocol. Adding it
    # would break static type-checking for exotic user checkpointers. Consumer
    # uses hasattr() to wire metrics only on implementations that provide it
    # (which BaseCheckPointer and its subclasses do). Exotic checkpointers opt
    # out of checkpoint-success/failure metrics; they can add bind_metrics if
    # they want to participate.


class BaseCheckPointer:
    def __init__(self, name: str = "", id: Optional[Union[str, int]] = None) -> None:
        self._id: Union[str, int] = id if id else os.getpid()
        self._name: str = name
        self._items: Dict[str, Any] = {}
        self.metrics: MetricsCollector = get_metrics_collector()
        self._metrics_labels: Dict[str, str] = {"stream_name": STANDALONE_STREAM_LABEL}
        self._metrics_bound: bool = False

    def bind_metrics(self, collector: MetricsCollector, labels: Dict[str, str]) -> None:
        # Fail fast: checkpoint metrics register labelnames=["stream_name", "shard_id"]
        # (see PrometheusMetricsCollector). shard_id is added at emit time, so the
        # caller must supply stream_name here. Without this check, a malformed bind
        # would only surface later when Prometheus rejects the labelset at first emit,
        # far from the wiring site.
        if "stream_name" not in labels:
            raise ValueError("bind_metrics requires a 'stream_name' label " f"(got keys: {sorted(labels.keys())!r})")
        if self._metrics_bound:
            if self.metrics is collector and self._metrics_labels == labels:
                return
            raise RuntimeError(
                "Checkpointer already bound to a different metrics collector or labels. "
                "Sharing a checkpointer across consumers is not supported."
            )
        self.metrics = collector
        self._metrics_labels = dict(labels)
        self._metrics_bound = True

    def _emit_checkpoint_success(self, shard_id: str) -> None:
        labels = {**self._metrics_labels, "shard_id": shard_id}
        self.metrics.increment(MetricType.CONSUMER_CHECKPOINT_SUCCESS, 1, labels)

    def _emit_checkpoint_failure(self, shard_id: str) -> None:
        labels = {**self._metrics_labels, "shard_id": shard_id}
        self.metrics.increment(MetricType.CONSUMER_CHECKPOINT_FAILURE, 1, labels)

    def get_id(self) -> Union[str, int]:
        return self._id

    def get_ref(self) -> str:
        return "{}/{}".format(self._name, self._id)

    def get_all_checkpoints(self):
        return self._items.copy()

    def get_checkpoint(self, shard_id):
        return self._items.get(shard_id)

    async def close(self):
        log.info("{} stopping..".format(self.get_ref()))
        await asyncio.gather(*[self.deallocate(shard_id) for shard_id in self._items.keys()])

    def is_allocated(self, shard_id):
        return shard_id in self._items


class BaseHeartbeatCheckPointer(BaseCheckPointer):
    def __init__(
        self,
        name,
        id=None,
        session_timeout=60,
        heartbeat_frequency=15,
        auto_checkpoint=True,
    ):
        super().__init__(name=name, id=id)

        self.session_timeout = session_timeout
        self.heartbeat_frequency = heartbeat_frequency
        self.auto_checkpoint = auto_checkpoint
        self._manual_checkpoints = {}

        self.heartbeat_task = asyncio.Task(self.heartbeat())

    async def close(self):
        log.debug("Cancelling heartbeat task..")
        self.heartbeat_task.cancel()

        await super().close()

    async def heartbeat(self):
        while True:
            await asyncio.sleep(self.heartbeat_frequency)

            # todo: don't heartbeat if checkpoint already updated it recently
            for shard_id, sequence in self._items.items():
                key = self.get_key(shard_id)
                val = {"ref": self.get_ref(), "ts": self.get_ts(), "sequence": sequence}
                log.debug("Heartbeating {}@{}".format(shard_id, sequence))
                await self.do_heartbeat(key, val)


class MemoryCheckPointer(BaseCheckPointer):
    async def deallocate(self, shard_id):
        log.info("{} deallocated on {}@{}".format(self.get_ref(), shard_id, self._items[shard_id]))
        self._items[shard_id]["active"] = False

    def is_allocated(self, shard_id):
        return shard_id in self._items and self._items[shard_id]["active"]

    async def allocate(self, shard_id):
        if self.is_allocated(shard_id):
            return False, None

        if shard_id not in self._items:
            self._items[shard_id] = {"sequence": None}

        self._items[shard_id]["active"] = True

        return True, self._items[shard_id]["sequence"]

    async def checkpoint(self, shard_id, sequence):
        try:
            log.debug("{} checkpointed on {} @ {}".format(self.get_ref(), shard_id, sequence))
            self._items[shard_id]["sequence"] = sequence
        except Exception:
            self._emit_checkpoint_failure(shard_id)
            raise
        self._emit_checkpoint_success(shard_id)


class RedisCheckPointer(BaseHeartbeatCheckPointer):
    def __init__(
        self,
        name,
        id=None,
        session_timeout=60,
        heartbeat_frequency=15,
        is_cluster=False,
        auto_checkpoint=True,
    ):
        super().__init__(
            name=name,
            id=id,
            session_timeout=session_timeout,
            heartbeat_frequency=heartbeat_frequency,
            auto_checkpoint=auto_checkpoint,
        )

        if is_cluster:
            from redis.asyncio.cluster import RedisCluster as Redis
        else:
            from redis.asyncio import Redis

        params = {
            "host": os.environ.get("REDIS_HOST", "localhost"),
            "port": int(os.environ.get("REDIS_PORT", "6379")),
            "password": os.environ.get("REDIS_PASSWORD"),
        }

        if not is_cluster:
            db = int(os.environ.get("REDIS_DB", 0))
            if db > 0:
                params["db"] = db
        else:
            params["skip_full_coverage_check"] = True

        self.client = Redis(**params)

    async def do_heartbeat(self, key, value):
        await self.client.set(key, json.dumps(value))

    def get_key(self, shard_id):
        return "pyredis-{}-{}".format(self._name, shard_id)

    def get_ts(self):
        return round(int(datetime.now(tz=timezone.utc).timestamp()))

    async def checkpoint(self, shard_id, sequence):

        if not self.auto_checkpoint:
            log.debug("{} updated manual checkpoint {}@{}".format(self.get_ref(), shard_id, sequence))
            self._manual_checkpoints[shard_id] = sequence
            return

        await self._checkpoint(shard_id, sequence)

    async def manual_checkpoint(self):
        # Pop per-shard on success so a mid-loop raise leaves the remaining
        # shards buffered for retry on the next manual_checkpoint() call.
        for shard_id in list(self._manual_checkpoints):
            sequence = self._manual_checkpoints[shard_id]
            await self._checkpoint(shard_id, sequence)
            self._manual_checkpoints.pop(shard_id, None)

    async def _checkpoint(self, shard_id, sequence):
        try:
            key = self.get_key(shard_id)

            val = {"ref": self.get_ref(), "ts": self.get_ts(), "sequence": sequence}

            previous_val = await self.client.getset(key, json.dumps(val))
            previous_val = json.loads(previous_val) if previous_val else None

            if not previous_val:
                raise NotImplementedError(
                    "{} checkpointed on {} but key did not exist?".format(self.get_ref(), shard_id)
                )

            if previous_val["ref"] != self.get_ref():
                raise NotImplementedError(
                    "{} checkpointed on {} but ref is different {}".format(self.get_ref(), shard_id, val["ref"])
                )

            log.debug("{} checkpointed on {}@{}".format(self.get_ref(), shard_id, sequence))
            self._items[shard_id] = sequence
        except Exception:
            self._emit_checkpoint_failure(shard_id)
            raise
        self._emit_checkpoint_success(shard_id)

    async def deallocate(self, shard_id):

        key = self.get_key(shard_id)

        val = {"ref": None, "ts": None, "sequence": self._items[shard_id]}

        await self.client.set(key, json.dumps(val))

        log.info("{} deallocated on {}@{}".format(self.get_ref(), shard_id, self._items[shard_id]))

        self._items.pop(shard_id)

    async def allocate(self, shard_id):

        key = self.get_key(shard_id)

        ts = self.get_ts()

        # try to set lock
        success = await self.client.set(
            key,
            json.dumps({"ref": self.get_ref(), "ts": ts, "sequence": None}),
            nx=True,
        )

        val = await self.client.get(key)
        val = json.loads(val) if val else None

        original_ts = val["ts"]

        if success:
            log.info("{} allocated {} (new checkpoint)".format(self.get_ref(), shard_id))
            self._items[shard_id] = None
            return True, None

        if val["ts"]:

            log.info("{} could not allocate {}, still in use by {}".format(self.get_ref(), shard_id, val["ref"]))

            # Wait a bit before carrying on to avoid spamming ourselves
            await asyncio.sleep(1)

            age = ts - original_ts

            # still alive?
            if age < self.session_timeout:
                return False, None

            log.info(
                "Attempting to take lock as {} is {} seconds over due..".format(val["ref"], age - self.session_timeout)
            )

        val["ref"] = self.get_ref()
        val["ts"] = ts

        previous_val = await self.client.getset(key, json.dumps(val))
        previous_val = json.loads(previous_val) if previous_val else None

        if previous_val["ts"] != original_ts:
            log.info("{} beat me to the lock..".format(previous_val["ref"]))
            return False, None

        log.info("{} allocating {}@{}".format(self.get_ref(), shard_id, val["sequence"]))

        self._items[shard_id] = val["sequence"]

        return True, val["sequence"]
