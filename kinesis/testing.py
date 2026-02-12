"""
In-memory testing utilities for async-kinesis.

Provides mock implementations of Producer and Consumer that use in-memory
streams instead of AWS Kinesis. No Docker, no LocalStack, no network calls.

Usage::

    from kinesis.testing import MockProducer, MockConsumer, MockKinesisBackend

    stream = MockKinesisBackend.create_stream("my-stream", shard_count=2)

    async with MockProducer(stream_name="my-stream") as producer:
        await producer.put({"key": "value"})
        await producer.flush()

    stream.seal()

    async with MockConsumer(stream_name="my-stream") as consumer:
        async for record in consumer:
            print(record)

Or with pytest fixtures (auto-discovered via pytest11 entry point)::

    async def test_roundtrip(kinesis_stream, kinesis_producer, kinesis_consumer):
        await kinesis_producer.put({"hello": "world"})
        await kinesis_producer.flush()
        kinesis_stream.seal()
        records = await collect_records(kinesis_consumer)
        assert records == [{"hello": "world"}]
"""

import asyncio
import hashlib
import logging
from collections import defaultdict, deque
from contextlib import contextmanager
from typing import Any, AsyncIterator, Callable, Dict, List, Optional

from .checkpointers import MemoryCheckPointer
from .processors import JsonProcessor, Processor

log = logging.getLogger(__name__)

__all__ = [
    "MemoryShard",
    "MemoryStream",
    "MockKinesisBackend",
    "MockProducer",
    "MockConsumer",
    "collect_records",
    "assert_records_delivered",
    "assert_shard_ordering",
    "memory_stream",
]

_SENTINEL = object()


class MemoryShard:
    """Single shard within a MemoryStream. Stores records in insertion order with sequence numbers."""

    def __init__(self, shard_id: str) -> None:
        self.shard_id = shard_id
        self.records: list = []
        self._seq = 0

    def put(self, data: bytes, partition_key: str) -> str:
        """Append a record and return its sequence number."""
        self._seq += 1
        seq = str(self._seq).zfill(20)
        self.records.append((seq, data, partition_key))
        return seq

    def seal(self) -> None:
        """Inject an end-of-shard sentinel."""
        self.records.append(_SENTINEL)

    @property
    def record_count(self) -> int:
        return sum(1 for r in self.records if r is not _SENTINEL)


class MemoryStream:
    """
    In-memory Kinesis stream with shard-aware storage.

    Records are routed to shards via MD5 hash of the partition key,
    matching real Kinesis behaviour. Each shard maintains insertion-order
    guarantees and monotonic sequence numbers.
    """

    def __init__(self, name: str, shard_count: int = 1) -> None:
        self.name = name
        self.shard_count = shard_count
        self.shards = [MemoryShard(f"shardId-{str(i).zfill(12)}") for i in range(shard_count)]
        self._sealed = False

    def _route_to_shard(self, partition_key: str) -> int:
        """Route a partition key to a shard index using MD5 (same algorithm as Kinesis)."""
        hash_bytes = hashlib.md5(partition_key.encode("utf-8")).digest()
        hash_int = int.from_bytes(hash_bytes, byteorder="big")
        return hash_int % self.shard_count

    def put(self, data: bytes, partition_key: str) -> str:
        """Write a record to the appropriate shard. Returns the sequence number."""
        if self._sealed:
            raise RuntimeError(f"Stream '{self.name}' is sealed")
        shard_idx = self._route_to_shard(partition_key)
        return self.shards[shard_idx].put(data, partition_key)

    def seal(self) -> None:
        """Seal the stream. Injects a sentinel per shard so consumers drain all records before stopping."""
        if self._sealed:
            return
        self._sealed = True
        for shard in self.shards:
            shard.seal()

    @property
    def sealed(self) -> bool:
        return self._sealed

    @property
    def record_count(self) -> int:
        """Total records across all shards (excludes sentinels)."""
        return sum(shard.record_count for shard in self.shards)


class MockKinesisBackend:
    """
    Singleton registry mapping stream names to MemoryStream instances.

    Allows MockProducer/MockConsumer to accept ``stream_name`` (same as real
    classes) and look up the backing stream internally. Enables
    ``unittest.mock.patch('myapp.Producer', MockProducer)`` without changing
    application code.
    """

    _streams: Dict[str, MemoryStream] = {}

    @classmethod
    def create_stream(cls, name: str, shard_count: int = 1) -> MemoryStream:
        """Register a new in-memory stream."""
        stream = MemoryStream(name=name, shard_count=shard_count)
        cls._streams[name] = stream
        return stream

    @classmethod
    def get_stream(cls, name: str) -> MemoryStream:
        """Look up a stream by name. Raises KeyError if it doesn't exist."""
        if name not in cls._streams:
            raise KeyError(f"Stream '{name}' does not exist. Call MockKinesisBackend.create_stream() first.")
        return cls._streams[name]

    @classmethod
    def reset(cls) -> None:
        """Clear all streams. Call between tests to prevent state bleeding."""
        cls._streams.clear()

    @classmethod
    def stream_exists(cls, name: str) -> bool:
        return name in cls._streams


def _ensure_stream(stream_name: str, create: bool, shard_count: int) -> None:
    """Create the stream if requested and it doesn't already exist."""
    if create and not MockKinesisBackend.stream_exists(stream_name):
        MockKinesisBackend.create_stream(stream_name, shard_count)


# ---------------------------------------------------------------------------
# Mock Producer
# ---------------------------------------------------------------------------


class MockProducer:
    """
    In-memory replacement for :class:`kinesis.Producer`.

    Mirrors the real Producer's constructor signature (ignoring AWS-specific
    arguments) so it works as a drop-in replacement via ``unittest.mock.patch``.

    Records are serialised through the processor pipeline and written directly
    to a :class:`MemoryStream`.
    """

    def __init__(
        self,
        stream_name: str,
        processor: Optional[Processor] = None,
        # Accepted for signature compatibility — ignored
        session=None,
        endpoint_url=None,
        region_name=None,
        buffer_time=None,
        put_rate_limit_per_shard=None,
        put_bandwidth_limit_per_shard=None,
        after_flush_fun=None,
        batch_size=None,
        max_queue_size=None,
        skip_describe_stream=None,
        use_list_shards=None,
        retry_limit=None,
        expo_backoff=None,
        expo_backoff_limit=None,
        create_stream: bool = False,
        create_stream_shards: int = 1,
        metrics_collector=None,
        **kwargs: Any,
    ) -> None:
        self.stream_name = stream_name
        self.processor = processor if processor else JsonProcessor()
        self._create_stream = create_stream
        self._create_stream_shards = create_stream_shards
        self._pk_counter = 0

    @property
    def stream(self) -> MemoryStream:
        return MockKinesisBackend.get_stream(self.stream_name)

    async def __aenter__(self) -> "MockProducer":
        _ensure_stream(self.stream_name, self._create_stream, self._create_stream_shards)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.flush()

    async def put(self, data: Any, partition_key: Optional[str] = None) -> None:
        """Serialise *data* through the processor and write to the memory stream."""
        for output_item in self.processor.add_item(data, partition_key):
            self._write_to_stream(output_item)

    async def flush(self) -> None:
        """Flush any records buffered by an aggregating processor."""
        if self.processor.has_items():
            for output_item in self.processor.get_items():
                self._write_to_stream(output_item)

    async def close(self) -> None:
        await self.flush()

    def _write_to_stream(self, output_item: Any) -> None:
        pk = output_item.partition_key
        if pk is None:
            self._pk_counter += 1
            pk = f"auto-{self._pk_counter}"
        self.stream.put(output_item.data, pk)


# ---------------------------------------------------------------------------
# Mock Consumer
# ---------------------------------------------------------------------------


class MockConsumer:
    """
    In-memory replacement for :class:`kinesis.Consumer`.

    Reads records from a :class:`MemoryStream`, deserialising them through the
    processor pipeline. Iteration terminates only when the stream is
    :meth:`~MemoryStream.seal`-ed and all shards are drained.

    The consumer respects ``iterator_type``:

    * ``TRIM_HORIZON`` — reads from the beginning of each shard.
    * ``LATEST`` — reads only records produced after the consumer starts.
    """

    def __init__(
        self,
        stream_name: str,
        processor: Optional[Processor] = None,
        checkpointer=None,
        iterator_type: str = "TRIM_HORIZON",
        # Accepted for signature compatibility — ignored
        session=None,
        endpoint_url=None,
        region_name=None,
        max_queue_size=None,
        max_shard_consumers=None,
        record_limit=None,
        sleep_time_no_records=None,
        shard_fetch_rate=None,
        retry_limit=None,
        expo_backoff=None,
        expo_backoff_limit=None,
        skip_describe_stream=None,
        use_list_shards=None,
        create_stream: bool = False,
        create_stream_shards: int = 1,
        timestamp=None,
        poll_delay: float = 0,
        **kwargs: Any,
    ) -> None:
        self.stream_name = stream_name
        self.processor = processor if processor else JsonProcessor()
        self.checkpointer = checkpointer if checkpointer else MemoryCheckPointer()
        self.iterator_type = iterator_type
        self._create_stream = create_stream
        self._create_stream_shards = create_stream_shards
        self._poll_delay = poll_delay
        self._positions: Dict[int, int] = {}
        self._exhausted: set = set()
        self._buffer: deque = deque()

    @property
    def stream(self) -> MemoryStream:
        return MockKinesisBackend.get_stream(self.stream_name)

    async def __aenter__(self) -> "MockConsumer":
        _ensure_stream(self.stream_name, self._create_stream, self._create_stream_shards)

        stream = self.stream
        for i, shard in enumerate(stream.shards):
            # Allocate shard on checkpointer (returns last checkpointed sequence)
            success, last_seq = await self.checkpointer.allocate(shard.shard_id)
            if self.iterator_type == "LATEST":
                self._positions[i] = len(shard.records)
            elif last_seq is not None:
                # Resume from checkpoint: skip past the last checkpointed sequence
                pos = 0
                for j, record in enumerate(shard.records):
                    if record is _SENTINEL:
                        continue
                    seq, data, pk = record
                    if seq == last_seq:
                        pos = j + 1
                        break
                self._positions[i] = pos
            else:
                self._positions[i] = 0
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.checkpointer.close()

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    async def __anext__(self) -> Any:
        # Drain buffer from a previous multi-item parse (aggregated records)
        if self._buffer:
            return self._buffer.popleft()

        stream = self.stream

        while True:
            found_record = False

            for shard_idx, shard in enumerate(stream.shards):
                if shard_idx in self._exhausted:
                    continue

                pos = self._positions.get(shard_idx, 0)

                if pos < len(shard.records):
                    record = shard.records[pos]
                    self._positions[shard_idx] = pos + 1

                    if record is _SENTINEL:
                        self._exhausted.add(shard_idx)
                        continue

                    seq, data, partition_key = record
                    await self.checkpointer.checkpoint(shard.shard_id, seq)

                    items = list(self.processor.parse(data))
                    if items:
                        self._buffer.extend(items[1:])
                        return items[0]

                    found_record = True  # Parsed but yielded nothing — keep scanning

            if len(self._exhausted) == len(stream.shards):
                raise StopAsyncIteration

            if not found_record:
                await asyncio.sleep(self._poll_delay if self._poll_delay > 0 else 0)

    async def close(self) -> None:
        await self.checkpointer.close()


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


async def collect_records(
    consumer: Any,
    count: Optional[int] = None,
    timeout: float = 5.0,
) -> List[Any]:
    """
    Collect records from a consumer with optional count limit and timeout.

    Works with both real :class:`~kinesis.Consumer` and :class:`MockConsumer`.
    The consumer must already be entered as a context manager.

    Args:
        consumer: A Consumer or MockConsumer instance.
        count: Stop after collecting this many records.  ``None`` = collect
            until the stream ends or timeout.
        timeout: Maximum seconds to wait before returning whatever was collected.

    Returns:
        List of collected records.
    """
    records: List[Any] = []

    async def _collect() -> None:
        async for record in consumer:
            records.append(record)
            if count is not None and len(records) >= count:
                break

    try:
        await asyncio.wait_for(_collect(), timeout=timeout)
    except (asyncio.TimeoutError, TimeoutError):
        pass

    return records


async def assert_records_delivered(
    producer: MockProducer,
    consumer: MockConsumer,
    records: List[Any],
    partition_key: Optional[str] = None,
    timeout: float = 5.0,
) -> List[Any]:
    """
    Produce records, seal the stream, consume, and verify all records arrived.

    Both *producer* and *consumer* must already be entered as context managers.

    Returns the received records list.

    Raises:
        AssertionError: If the wrong number of records or mismatched content.
    """
    for record in records:
        await producer.put(record, partition_key=partition_key)
    await producer.flush()

    stream = MockKinesisBackend.get_stream(producer.stream_name)
    stream.seal()

    received = await collect_records(consumer, timeout=timeout)

    assert len(received) == len(records), f"Expected {len(records)} records, got {len(received)}"

    assert sorted(received, key=repr) == sorted(records, key=repr), (
        f"Record mismatch.\nExpected: {sorted(records, key=repr)}\nReceived: {sorted(received, key=repr)}"
    )

    return received


def assert_shard_ordering(
    records: List[Any],
    key_func: Callable[[Any], Any],
    order_func: Callable[[Any], Any],
) -> None:
    """
    Verify per-shard (partition-key) ordering is preserved.

    Groups records by ``key_func(record)`` (typically the partition key) and
    verifies that within each group the values extracted by ``order_func``
    are monotonically non-decreasing.

    Args:
        records: Consumed records in the order they were received.
        key_func: Extracts the grouping key (e.g. partition key) from a record.
        order_func: Extracts an orderable value from a record.
    """
    groups: Dict[Any, List] = defaultdict(list)
    for record in records:
        key = key_func(record)
        groups[key].append(record)

    for key, group in groups.items():
        values = [order_func(r) for r in group]
        assert values == sorted(values), f"Records for key '{key}' are not in order: {values}"


# ---------------------------------------------------------------------------
# Convenience context manager (non-pytest)
# ---------------------------------------------------------------------------


@contextmanager
def memory_stream(name: str = "test-stream", shard_count: int = 1):
    """
    Context manager that creates a fresh :class:`MemoryStream` and resets
    the backend on exit.  For use outside of pytest::

        from kinesis.testing import memory_stream, MockProducer, MockConsumer

        with memory_stream("my-stream", shard_count=2) as stream:
            async with MockProducer(stream_name="my-stream") as producer:
                await producer.put({"hello": "world"})
    """
    MockKinesisBackend.reset()
    stream = MockKinesisBackend.create_stream(name, shard_count=shard_count)
    try:
        yield stream
    finally:
        MockKinesisBackend.reset()


# ---------------------------------------------------------------------------
# Pytest fixtures (auto-discovered via pytest11 entry point in setup.py)
# ---------------------------------------------------------------------------

try:
    import pytest
    import pytest_asyncio

    @pytest.fixture
    def kinesis_backend():
        """Reset and yield the MockKinesisBackend. Resets again on teardown."""
        MockKinesisBackend.reset()
        yield MockKinesisBackend
        MockKinesisBackend.reset()

    @pytest.fixture
    def kinesis_stream(kinesis_backend):
        """Create a single-shard MemoryStream named ``test-stream``."""
        return kinesis_backend.create_stream("test-stream", shard_count=1)

    @pytest_asyncio.fixture
    async def kinesis_producer(kinesis_stream):
        """MockProducer connected to the ``kinesis_stream`` fixture."""
        async with MockProducer(stream_name=kinesis_stream.name) as producer:
            yield producer

    @pytest_asyncio.fixture
    async def kinesis_consumer(kinesis_stream):
        """MockConsumer connected to the ``kinesis_stream`` fixture."""
        async with MockConsumer(stream_name=kinesis_stream.name) as consumer:
            yield consumer

except ImportError:
    pass
