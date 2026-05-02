"""Unit tests for metrics functionality."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp import ClientConnectionError
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    EndpointConnectionError,
    ReadTimeoutError,
)

from kinesis import (
    CheckpointFlushError,
    CheckpointOwnershipConflict,
    InMemoryMetricsCollector,
    MemoryCheckPointer,
    MetricType,
    NoOpMetricsCollector,
    get_metrics_collector,
    reset_metrics_collector,
    set_metrics_collector,
)
from kinesis.consumer import ShardStats
from kinesis.metrics import Timer
from kinesis.utils import Throttler


@pytest.fixture
def fast_asyncio_sleep(monkeypatch):
    """Replace asyncio.sleep with a zero-delay awaitable for retry-path tests."""
    orig_sleep = asyncio.sleep

    async def fast_sleep(_):
        await orig_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    return fast_sleep


class TestNoOpMetricsCollector:
    def test_all_operations_are_noop(self):
        """Test that NoOpMetricsCollector does nothing."""
        collector = NoOpMetricsCollector()

        # These should all do nothing without errors
        collector.increment(MetricType.PRODUCER_RECORDS_SENT, 10)
        collector.gauge(MetricType.PRODUCER_QUEUE_SIZE, 100)
        collector.histogram(MetricType.PRODUCER_FLUSH_DURATION, 0.5)

        # Timer should also do nothing
        with collector.timer(MetricType.PRODUCER_FLUSH_DURATION):
            time.sleep(0.01)  # Small delay

        # No way to verify it did nothing, but no errors = success


class TestInMemoryMetricsCollector:
    def test_counter_increments(self):
        """Test counter functionality."""
        collector = InMemoryMetricsCollector()

        # First increment
        collector.increment(MetricType.PRODUCER_RECORDS_SENT, 5, {"stream_name": "test"})
        assert collector.counters["producer_records_sent_total{stream_name=test}"] == 5

        # Second increment should add
        collector.increment(MetricType.PRODUCER_RECORDS_SENT, 3, {"stream_name": "test"})
        assert collector.counters["producer_records_sent_total{stream_name=test}"] == 8

        # Different labels should be separate
        collector.increment(MetricType.PRODUCER_RECORDS_SENT, 2, {"stream_name": "other"})
        assert collector.counters["producer_records_sent_total{stream_name=other}"] == 2
        assert collector.counters["producer_records_sent_total{stream_name=test}"] == 8

    def test_gauge_sets_value(self):
        """Test gauge functionality."""
        collector = InMemoryMetricsCollector()

        # Set initial value
        collector.gauge(MetricType.PRODUCER_QUEUE_SIZE, 100, {"stream_name": "test"})
        assert collector.gauges["producer_queue_size{stream_name=test}"] == 100

        # Update should overwrite
        collector.gauge(MetricType.PRODUCER_QUEUE_SIZE, 50, {"stream_name": "test"})
        assert collector.gauges["producer_queue_size{stream_name=test}"] == 50

    def test_histogram_records_values(self):
        """Test histogram functionality."""
        collector = InMemoryMetricsCollector()

        # Record multiple values
        collector.histogram(MetricType.PRODUCER_BATCH_SIZE, 10, {"stream_name": "test"})
        collector.histogram(MetricType.PRODUCER_BATCH_SIZE, 20, {"stream_name": "test"})
        collector.histogram(MetricType.PRODUCER_BATCH_SIZE, 15, {"stream_name": "test"})

        values = collector.histograms["producer_batch_size{stream_name=test}"]
        assert values == [10, 20, 15]

    def test_timer_records_duration(self):
        """Test timer context manager."""
        collector = InMemoryMetricsCollector()

        with collector.timer(MetricType.PRODUCER_FLUSH_DURATION, {"stream_name": "test"}):
            time.sleep(0.01)  # Small delay

        values = collector.histograms["producer_flush_duration_seconds{stream_name=test}"]
        assert len(values) == 1
        assert values[0] >= 0.01  # Should be at least the sleep duration
        assert values[0] < 0.1  # But not too long

    def test_labels_ordering(self):
        """Test that labels are ordered consistently."""
        collector = InMemoryMetricsCollector()

        # Different order, same labels
        collector.increment(MetricType.CONSUMER_ERRORS, 1, {"stream_name": "test", "error_type": "timeout"})
        collector.increment(MetricType.CONSUMER_ERRORS, 1, {"error_type": "timeout", "stream_name": "test"})

        # Should be the same key
        assert len(collector.counters) == 1
        key = "consumer_errors_total{error_type=timeout,stream_name=test}"
        assert collector.counters[key] == 2

    def test_get_metrics(self):
        """Test get_metrics returns all metrics."""
        collector = InMemoryMetricsCollector()

        collector.increment(MetricType.PRODUCER_RECORDS_SENT, 5)
        collector.gauge(MetricType.PRODUCER_QUEUE_SIZE, 100)
        collector.histogram(MetricType.PRODUCER_BATCH_SIZE, 50)

        metrics = collector.get_metrics()
        assert "producer_records_sent_total" in metrics
        assert "producer_queue_size" in metrics
        assert "producer_batch_size" in metrics
        assert metrics["producer_records_sent_total"] == 5
        assert metrics["producer_queue_size"] == 100
        assert metrics["producer_batch_size"] == [50]


class TestTimer:
    def test_timer_calls_callback(self):
        """Test Timer calls callback with correct arguments."""
        callback = MagicMock()
        metric = MetricType.PRODUCER_FLUSH_DURATION
        labels = {"stream_name": "test"}

        with Timer(callback, metric, labels):
            time.sleep(0.01)

        # Verify callback was called
        callback.assert_called_once()
        args = callback.call_args[0]
        assert args[0] == metric
        assert isinstance(args[1], float)
        assert args[1] >= 0.01
        assert args[2] == labels


class TestGlobalCollectorManagement:
    def test_default_collector_is_noop(self):
        """Test default collector is NoOp."""
        reset_metrics_collector()
        collector = get_metrics_collector()
        assert isinstance(collector, NoOpMetricsCollector)

    def test_set_and_get_custom_collector(self):
        """Test setting custom collector."""
        custom = InMemoryMetricsCollector()
        set_metrics_collector(custom)

        retrieved = get_metrics_collector()
        assert retrieved is custom

        # Clean up
        reset_metrics_collector()

    def test_reset_returns_to_noop(self):
        """Test reset returns to NoOp collector."""
        # Set custom
        set_metrics_collector(InMemoryMetricsCollector())
        assert not isinstance(get_metrics_collector(), NoOpMetricsCollector)

        # Reset
        reset_metrics_collector()
        assert isinstance(get_metrics_collector(), NoOpMetricsCollector)


class TestPrometheusOptional:
    def test_prometheus_import_handling(self):
        """Test that PrometheusMetricsCollector handles missing dependency gracefully."""
        try:
            from kinesis import PrometheusMetricsCollector

            if PrometheusMetricsCollector is not None:
                # If available, should be able to create
                collector = PrometheusMetricsCollector()
                assert collector is not None
            else:
                # If not available, should be None
                assert PrometheusMetricsCollector is None
        except ImportError:
            # This is fine - prometheus_client not installed
            pass


class TestMetricTypeEnum:
    def test_all_metric_types_have_values(self):
        """Test all MetricType enum values are strings."""
        for metric in MetricType:
            assert isinstance(metric.value, str)
            assert len(metric.value) > 0

    def test_metric_naming_convention(self):
        """Test metric names follow Prometheus naming conventions."""
        for metric in MetricType:
            name = metric.value
            # Should be lowercase with underscores
            assert name.islower() or "_" in name
            # Should not have spaces
            assert " " not in name


async def _drain_pending_shard_fetch(consumer):
    """Cancel any in-flight shard fetch task left behind by fetch()."""
    for shard in consumer.shards or []:
        task = shard.get("fetch")
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            shard["fetch"] = None


def _shard_labels(shard_id, stream_name="test-stream"):
    return f"shard_id={shard_id},stream_name={stream_name}"


class TestConsumerMetricsWiring:
    """Verify Consumer accepts metrics_collector and wires it correctly."""

    @pytest.mark.asyncio
    async def test_consumer_accepts_metrics_collector(self, mock_consumer):
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        assert consumer.metrics is collector

    @pytest.mark.asyncio
    async def test_consumer_defaults_to_global_collector(self, mock_consumer):
        reset_metrics_collector()
        consumer = mock_consumer()
        assert isinstance(consumer.metrics, NoOpMetricsCollector)


class TestConsumerFetchEmits:
    """Exercise fetch() emit sites with a synthetic result dict."""

    @staticmethod
    def _prime_shard_with_result(consumer, result, shard_id="shard-0"):
        fetch_future = asyncio.Future()
        fetch_future.set_result(result)
        consumer.shards = [
            {
                "ShardId": shard_id,
                "ShardIterator": "curr-iter",
                "fetch": fetch_future,
                "throttler": Throttler(rate_limit=1000, period=1),
                "stats": ShardStats(),
            }
        ]

    @pytest.mark.asyncio
    async def test_fetch_increments_records_and_bytes_per_enqueued_row(self, mock_consumer):
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer.checkpointer = MemoryCheckPointer(name="test")
        await consumer.checkpointer.allocate("shard-0")
        consumer.refresh_shards = AsyncMock()
        consumer.get_records = AsyncMock(return_value=None)

        records = [
            {"SequenceNumber": "1", "Data": b'{"id": 1}'},  # 9 bytes
            {"SequenceNumber": "2", "Data": b'{"id": 22}'},  # 10 bytes
        ]
        self._prime_shard_with_result(
            consumer,
            {"Records": records, "NextShardIterator": "next-iter"},
        )

        await consumer.fetch()

        key_base = _shard_labels("shard-0")
        assert collector.counters[f"consumer_records_received_total{{{key_base}}}"] == 2
        assert collector.counters[f"consumer_bytes_received_total{{{key_base}}}"] == 19

        # Queue size gauge emitted once records were enqueued
        assert "consumer_queue_size{stream_name=test-stream}" in collector.gauges

        await _drain_pending_shard_fetch(consumer)

    @pytest.mark.asyncio
    async def test_fetch_emits_iterator_age_when_millis_behind_present(self, mock_consumer):
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer.checkpointer = MemoryCheckPointer(name="test")
        await consumer.checkpointer.allocate("shard-0")
        consumer.refresh_shards = AsyncMock()
        consumer.get_records = AsyncMock(return_value=None)

        self._prime_shard_with_result(
            consumer,
            {
                "Records": [],
                "NextShardIterator": "next-iter",
                "MillisBehindLatest": 54321,
            },
        )

        await consumer.fetch()

        key = f"consumer_iterator_age_milliseconds{{{_shard_labels('shard-0')}}}"
        assert collector.gauges[key] == 54321

        await _drain_pending_shard_fetch(consumer)

    @pytest.mark.asyncio
    async def test_fetch_skips_iterator_age_when_field_absent(self, mock_consumer):
        """Backends that omit MillisBehindLatest must no-op (no iterator_age emission)."""
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer.checkpointer = MemoryCheckPointer(name="test")
        await consumer.checkpointer.allocate("shard-0")
        consumer.refresh_shards = AsyncMock()
        consumer.get_records = AsyncMock(return_value=None)

        self._prime_shard_with_result(
            consumer,
            {"Records": [], "NextShardIterator": "next-iter"},
        )

        await consumer.fetch()

        iterator_age_keys = [k for k in collector.gauges if k.startswith("consumer_iterator_age_milliseconds")]
        assert iterator_age_keys == []

        await _drain_pending_shard_fetch(consumer)

    @pytest.mark.asyncio
    async def test_fetch_reemits_last_iterator_age_on_error_branch(self, mock_consumer):
        """After a successful fetch caches MillisBehindLatest, a subsequent failed
        fetch (get_records returned None) must re-emit the cached value so the
        gauge stays observable during error spikes."""
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer.checkpointer = MemoryCheckPointer(name="test")
        await consumer.checkpointer.allocate("shard-0")
        consumer.refresh_shards = AsyncMock()
        consumer.get_records = AsyncMock(return_value=None)

        # 1) First fetch succeeds and caches MillisBehindLatest on the shard.
        self._prime_shard_with_result(
            consumer,
            {"Records": [], "NextShardIterator": "next-iter-1", "MillisBehindLatest": 12345},
        )
        await consumer.fetch()

        key = f"consumer_iterator_age_milliseconds{{{_shard_labels('shard-0')}}}"
        assert collector.gauges[key] == 12345

        await _drain_pending_shard_fetch(consumer)

        # 2) Second fetch errors out (result is None via the same mechanism as a
        # connection/timeout failure). The gauge must still be emitted with the
        # cached value so Prometheus doesn't treat it as stale.
        collector.gauges.clear()  # isolate the error-branch emission
        fetch_future = asyncio.Future()
        fetch_future.set_result(None)
        consumer.shards[0]["fetch"] = fetch_future
        consumer.shards[0]["ShardIterator"] = "curr-iter"

        await consumer.fetch()

        assert collector.gauges[key] == 12345  # cached value re-emitted unchanged

        await _drain_pending_shard_fetch(consumer)

    @pytest.mark.asyncio
    async def test_fetch_invalidates_cached_iterator_age_when_field_disappears(self, mock_consumer):
        """If a later successful fetch omits MillisBehindLatest (backend stopped
        supplying it), the cache must be cleared so the error branch no longer
        re-emits the old value."""
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer.checkpointer = MemoryCheckPointer(name="test")
        await consumer.checkpointer.allocate("shard-0")
        consumer.refresh_shards = AsyncMock()
        consumer.get_records = AsyncMock(return_value=None)

        # 1) Prime cache with a successful fetch carrying MillisBehindLatest.
        self._prime_shard_with_result(
            consumer,
            {"Records": [], "NextShardIterator": "iter-1", "MillisBehindLatest": 777},
        )
        await consumer.fetch()
        assert consumer.shards[0].get("LastMillisBehindLatest") == 777

        await _drain_pending_shard_fetch(consumer)

        # 2) Successful fetch without the field should clear the cache.
        fetch_future = asyncio.Future()
        fetch_future.set_result({"Records": [], "NextShardIterator": "iter-2"})
        consumer.shards[0]["fetch"] = fetch_future
        consumer.shards[0]["ShardIterator"] = "iter-1"
        await consumer.fetch()
        assert "LastMillisBehindLatest" not in consumer.shards[0]

        await _drain_pending_shard_fetch(consumer)

        # 3) Error branch must not re-emit the now-invalidated value.
        collector.gauges.clear()
        fetch_future = asyncio.Future()
        fetch_future.set_result(None)
        consumer.shards[0]["fetch"] = fetch_future
        consumer.shards[0]["ShardIterator"] = "iter-2"
        await consumer.fetch()

        iterator_age_keys = [k for k in collector.gauges if k.startswith("consumer_iterator_age_milliseconds")]
        assert iterator_age_keys == []

        await _drain_pending_shard_fetch(consumer)

    @pytest.mark.asyncio
    async def test_fetch_error_branch_skips_iterator_age_when_no_cache(self, mock_consumer):
        """Error branch must not emit iterator_age with a fabricated zero when we
        never had a successful fetch (avoids misleading ``MillisBehindLatest=0``
        during startup connection failures)."""
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer.checkpointer = MemoryCheckPointer(name="test")
        await consumer.checkpointer.allocate("shard-0")
        consumer.refresh_shards = AsyncMock()
        consumer.get_records = AsyncMock(return_value=None)

        self._prime_shard_with_result(consumer, None)  # error branch

        await consumer.fetch()

        iterator_age_keys = [k for k in collector.gauges if k.startswith("consumer_iterator_age_milliseconds")]
        assert iterator_age_keys == []

        await _drain_pending_shard_fetch(consumer)

    @pytest.mark.asyncio
    async def test_fetch_counts_only_successfully_enqueued_rows(self, mock_consumer):
        """Rows that time out mid-batch must not be counted (avoid double-count on retry)."""
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer.checkpointer = MemoryCheckPointer(name="test")
        await consumer.checkpointer.allocate("shard-0")
        consumer.refresh_shards = AsyncMock()
        consumer.get_records = AsyncMock(return_value=None)

        records = [
            {"SequenceNumber": "1", "Data": b'{"id": 1}'},
            {"SequenceNumber": "2", "Data": b'{"id": 2}'},
            {"SequenceNumber": "3", "Data": b'{"id": 3}'},
        ]
        self._prime_shard_with_result(
            consumer,
            {"Records": records, "NextShardIterator": "next-iter"},
        )

        # Patch the queue's put directly so first put succeeds, subsequent puts
        # raise asyncio.TimeoutError (matches what asyncio.wait_for would surface
        # on a real timeout). Patching put rather than asyncio.wait_for keeps the
        # test stable if other wait_for sites are added inside fetch().
        real_put = consumer.queue.put
        put_calls = {"n": 0}

        async def flaky_put(item):
            put_calls["n"] += 1
            if put_calls["n"] >= 2:
                raise asyncio.TimeoutError()
            await real_put(item)

        consumer.queue.put = flaky_put

        await consumer.fetch()

        key_base = _shard_labels("shard-0")
        # Only the first row got enqueued before the timeout broke the loop.
        assert collector.counters.get(f"consumer_records_received_total{{{key_base}}}") == 1
        assert collector.counters.get(f"consumer_bytes_received_total{{{key_base}}}") == 9

        await _drain_pending_shard_fetch(consumer)


class TestConsumerGetRecordsErrors:
    """CONSUMER_ERRORS emits from get_records error paths with correct error_type."""

    @staticmethod
    def _shard(shard_id="shard-0"):
        return {
            "ShardId": shard_id,
            "ShardIterator": "curr-iter",
            "throttler": Throttler(rate_limit=1000, period=1),
            "stats": ShardStats(),
        }

    @pytest.mark.asyncio
    async def test_connection_error_emits_connection_label(self, mock_consumer, fast_asyncio_sleep):
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer.client = MagicMock()
        consumer.client.get_records = AsyncMock(side_effect=ClientConnectionError("boom"))
        consumer.get_conn = AsyncMock()

        await consumer.get_records(self._shard())

        key = f"consumer_errors_total{{error_type=connection,{_shard_labels('shard-0')}}}"
        assert collector.counters[key] == 1
        consumer.get_conn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_botocore_connection_closed_emits_connection_label(self, mock_consumer, fast_asyncio_sleep):
        """Real AWS commonly raises botocore ConnectionClosedError when an idle
        keep-alive connection is dropped mid-response. Prior to #73 fix this fell
        through to the catch-all and got mislabeled as error_type=unknown."""
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer.client = MagicMock()
        consumer.client.get_records = AsyncMock(
            side_effect=ConnectionClosedError(endpoint_url="https://kinesis.us-east-1.amazonaws.com/")
        )
        consumer.get_conn = AsyncMock()

        await consumer.get_records(self._shard())

        key = f"consumer_errors_total{{error_type=connection,{_shard_labels('shard-0')}}}"
        assert collector.counters[key] == 1
        consumer.get_conn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_botocore_endpoint_connection_error_emits_connection_label(self, mock_consumer, fast_asyncio_sleep):
        """botocore.EndpointConnectionError (DNS/TCP failure before request is sent)
        should be counted as a connection error and return without rebuilding."""
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer.client = MagicMock()
        consumer.client.get_records = AsyncMock(
            side_effect=EndpointConnectionError(endpoint_url="https://kinesis.us-east-1.amazonaws.com/")
        )
        consumer.get_conn = AsyncMock()

        await consumer.get_records(self._shard())

        key = f"consumer_errors_total{{error_type=connection,{_shard_labels('shard-0')}}}"
        assert collector.counters[key] == 1
        consumer.get_conn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_botocore_read_timeout_emits_timeout_label(self, mock_consumer, fast_asyncio_sleep):
        """botocore.ReadTimeoutError lands in the timeout bucket, not unknown."""
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer.client = MagicMock()
        consumer.client.get_records = AsyncMock(
            side_effect=ReadTimeoutError(endpoint_url="https://kinesis.us-east-1.amazonaws.com/")
        )

        await consumer.get_records(self._shard())

        key = f"consumer_errors_total{{error_type=timeout,{_shard_labels('shard-0')}}}"
        assert collector.counters[key] == 1

    @pytest.mark.asyncio
    async def test_timeout_emits_timeout_label(self, mock_consumer, fast_asyncio_sleep):
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer.client = MagicMock()
        consumer.client.get_records = AsyncMock(side_effect=asyncio.TimeoutError())

        await consumer.get_records(self._shard())

        key = f"consumer_errors_total{{error_type=timeout,{_shard_labels('shard-0')}}}"
        assert collector.counters[key] == 1

    @pytest.mark.asyncio
    async def test_client_error_emits_raw_code_label(self, mock_consumer, fast_asyncio_sleep):
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        err = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "throttled"}},
            "GetRecords",
        )
        consumer.client = MagicMock()
        consumer.client.get_records = AsyncMock(side_effect=err)

        await consumer.get_records(self._shard())

        key = (
            "consumer_errors_total{" f"error_type=ProvisionedThroughputExceededException,{_shard_labels('shard-0')}" "}"
        )
        assert collector.counters[key] == 1

    @pytest.mark.asyncio
    async def test_internal_failure_no_longer_rebuilds(self, mock_consumer, fast_asyncio_sleep):
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        err = ClientError(
            {"Error": {"Code": "InternalFailure", "Message": "internal failure"}},
            "GetRecords",
        )
        consumer.client = MagicMock()
        consumer.client.get_records = AsyncMock(side_effect=err)
        consumer.get_conn = AsyncMock()

        await consumer.get_records(self._shard())

        key = f"consumer_errors_total{{error_type=InternalFailure,{_shard_labels('shard-0')}}}"
        assert collector.counters[key] == 1
        consumer.get_conn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unknown_exception_emits_unknown_label(self, mock_consumer, fast_asyncio_sleep):
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer.client = MagicMock()
        consumer.client.get_records = AsyncMock(side_effect=RuntimeError("surprise"))

        await consumer.get_records(self._shard())

        key = f"consumer_errors_total{{error_type=unknown,{_shard_labels('shard-0')}}}"
        assert collector.counters[key] == 1

    @pytest.mark.asyncio
    async def test_none_client_skips_without_raising_or_counting_error(self, mock_consumer):
        """When _get_reconn_helper has set self.client = None on one shard's
        connection error, concurrent get_records dispatches from fetch() must
        return None quietly rather than hit the catch-all with
        AttributeError("'NoneType' object has no attribute 'get_records'") and
        pollute error_type=unknown. Regression from 2.5.2 reported on #73."""
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer.client = None  # simulate mid-reconnect state

        result = await consumer.get_records(self._shard())

        assert result is None
        # No error metric should be emitted for the skipped-because-rebuilding
        # case; the triggering connection error was already counted on the
        # shard that initiated the reconnect.
        assert not any(k.startswith("consumer_errors_total") for k in collector.counters), collector.counters

    @pytest.mark.asyncio
    async def test_client_nulled_during_throttler_await_does_not_raise(self, mock_consumer):
        """Tighter race: self.client is valid on function entry but flips to
        None while the throttler's __aenter__ is awaiting (e.g. another shard
        triggered _get_reconn_helper in the meantime). The snapshot-and-check
        inside the throttler block must catch this; without it, dereferencing
        self.client later in the same method would AttributeError and pollute
        error_type=unknown."""
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer.client = MagicMock()
        consumer.client.get_records = AsyncMock(return_value={"Records": [], "NextShardIterator": "next"})

        shard = self._shard()

        # Wrap the throttler so that during its acquire (an await point),
        # consumer.client flips to None. Simulates _get_reconn_helper running
        # on another task between function entry and the get_records call.
        real_throttler = shard["throttler"]

        class RaceThrottler:
            async def __aenter__(self):
                await real_throttler.__aenter__()
                consumer.client = None
                return self

            async def __aexit__(self, *exc):
                return await real_throttler.__aexit__(*exc)

        shard["throttler"] = RaceThrottler()

        result = await consumer.get_records(shard)

        assert result is None
        assert not any(k.startswith("consumer_errors_total") for k in collector.counters), collector.counters


async def _make_redis_cp(auto_checkpoint=True):
    """Construct a RedisCheckPointer with a mocked client and a cancelled heartbeat task.

    Returns the checkpointer with `self.client` replaced by an AsyncMock. Callers
    configure the mock's return/side-effect as needed and must `await cp.close()`
    (or rely on the try/finally) to cancel the heartbeat.
    """
    from kinesis.checkpointers import RedisCheckPointer

    cp = RedisCheckPointer("test", auto_checkpoint=auto_checkpoint, heartbeat_frequency=3600)
    cp.heartbeat_task.cancel()
    try:
        await cp.heartbeat_task
    except asyncio.CancelledError:
        pass
    cp.client = AsyncMock()
    return cp


class TestConsumerCheckpointMetrics:
    """Emission now happens in the checkpointer's backend-write path. These tests
    exercise the real write paths of MemoryCheckPointer (success) and
    RedisCheckPointer (failure), not the removed Consumer wrapper."""

    @pytest.mark.asyncio
    async def test_maybe_checkpoint_success(self, mock_consumer):
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        # Default MemoryCheckPointer is bound to the consumer's collector via __init__.
        await consumer.checkpointer.allocate("shard-0")

        await consumer._maybe_checkpoint("shard-0", "seq-1")

        key = f"consumer_checkpoint_success_total{{{_shard_labels('shard-0')}}}"
        assert collector.counters[key] == 1
        assert f"consumer_checkpoint_failure_total{{{_shard_labels('shard-0')}}}" not in collector.counters

    @pytest.mark.asyncio
    async def test_checkpoint_failure_emits_on_backend_raise(self):
        """Exercise RedisCheckPointer._checkpoint try/except with a raising client."""
        collector = InMemoryMetricsCollector()
        cp = await _make_redis_cp()
        cp.bind_metrics(collector, {"stream_name": "test-stream"})
        cp.client.getset = AsyncMock(side_effect=RuntimeError("backend down"))

        try:
            with pytest.raises(RuntimeError):
                await cp._checkpoint("shard-0", "seq-1")
        finally:
            await cp.close()

        key = f"consumer_checkpoint_failure_total{{{_shard_labels('shard-0')}}}"
        assert collector.counters[key] == 1
        assert f"consumer_checkpoint_success_total{{{_shard_labels('shard-0')}}}" not in collector.counters

    @pytest.mark.asyncio
    async def test_flush_pending_checkpoints_success(self, mock_consumer):
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        await consumer.checkpointer.allocate("shard-0")
        await consumer.checkpointer.allocate("shard-1")
        consumer._pending_checkpoints = {"shard-0": "seq-1", "shard-1": "seq-9"}

        await consumer._flush_pending_checkpoints()

        assert collector.counters[f"consumer_checkpoint_success_total{{{_shard_labels('shard-0')}}}"] == 1
        assert collector.counters[f"consumer_checkpoint_success_total{{{_shard_labels('shard-1')}}}"] == 1

    @pytest.mark.asyncio
    async def test_checkpoint_metrics_skipped_in_manual_mode(self):
        """Under auto_checkpoint=False, Redis checkpoint() buffers without emitting."""
        collector = InMemoryMetricsCollector()
        cp = await _make_redis_cp(auto_checkpoint=False)
        cp.bind_metrics(collector, {"stream_name": "test-stream"})

        try:
            await cp.checkpoint("shard-0", "seq-1")
        finally:
            await cp.close()

        # No counters at all: neither success nor failure
        assert not any(key.startswith("consumer_checkpoint_") for key in collector.counters)
        assert cp._manual_checkpoints == {"shard-0": "seq-1"}

    @pytest.mark.asyncio
    async def test_manual_checkpoint_flush_retains_buffer_on_failure(self):
        """When every attempted shard raises, all stay buffered for retry and each
        emits its own failure metric."""
        collector = InMemoryMetricsCollector()
        cp = await _make_redis_cp(auto_checkpoint=False)
        cp.bind_metrics(collector, {"stream_name": "test-stream"})
        cp.client.getset = AsyncMock(return_value=None)  # triggers CheckpointOwnershipConflict

        try:
            await cp.checkpoint("shard-0", "seq-1")
            await cp.checkpoint("shard-1", "seq-9")
            assert not any(key.startswith("consumer_checkpoint_") for key in collector.counters)

            with pytest.raises(CheckpointFlushError) as exc_info:
                await cp.manual_checkpoint()

            # Best-effort flush: every shard was attempted, every shard raised.
            assert [shard_id for shard_id, _ in exc_info.value.errors] == ["shard-0", "shard-1"]
            assert all(isinstance(exc, CheckpointOwnershipConflict) for _, exc in exc_info.value.errors)

            # Both failed, so both remain buffered.
            assert cp._manual_checkpoints == {"shard-0": "seq-1", "shard-1": "seq-9"}
        finally:
            await cp.close()

        # Both shards emitted their own failure counter from inside _checkpoint.
        assert collector.counters[f"consumer_checkpoint_failure_total{{{_shard_labels('shard-0')}}}"] == 1
        assert collector.counters[f"consumer_checkpoint_failure_total{{{_shard_labels('shard-1')}}}"] == 1

    @pytest.mark.asyncio
    async def test_manual_checkpoint_flush_partial_failure_retains_failed_shard(self):
        """When the first shard succeeds and the second raises, only the failed
        shard stays buffered; the successful shard is popped."""
        collector = InMemoryMetricsCollector()
        cp = await _make_redis_cp(auto_checkpoint=False)
        cp.bind_metrics(collector, {"stream_name": "test-stream"})

        # First call succeeds (returns previous val matching our ref), second raises.
        previous_val = '{"ref": "' + cp.get_ref() + '", "ts": 0, "sequence": "seq-prev"}'
        cp.client.getset = AsyncMock(side_effect=[previous_val, RuntimeError("flake")])

        try:
            await cp.checkpoint("shard-0", "seq-1")
            await cp.checkpoint("shard-1", "seq-9")

            with pytest.raises(CheckpointFlushError) as exc_info:
                await cp.manual_checkpoint()

            assert [shard_id for shard_id, _ in exc_info.value.errors] == ["shard-1"]
            assert isinstance(exc_info.value.errors[0][1], RuntimeError)

            # shard-0 was flushed and popped; shard-1 raised before pop, stays buffered.
            assert cp._manual_checkpoints == {"shard-1": "seq-9"}
        finally:
            await cp.close()

        assert collector.counters[f"consumer_checkpoint_success_total{{{_shard_labels('shard-0')}}}"] == 1
        assert collector.counters[f"consumer_checkpoint_failure_total{{{_shard_labels('shard-1')}}}"] == 1

    @pytest.mark.asyncio
    async def test_manual_checkpoint_continues_past_head_failure(self):
        """Issue #79: a shard that repeatedly fails at the head of the buffer must
        not block healthy shards behind it from being flushed. Poison-pill fix."""
        collector = InMemoryMetricsCollector()
        cp = await _make_redis_cp(auto_checkpoint=False)
        cp.bind_metrics(collector, {"stream_name": "test-stream"})

        # shard-0 always raises; shard-1 always succeeds.
        previous_val = '{"ref": "' + cp.get_ref() + '", "ts": 0, "sequence": "seq-prev"}'
        cp.client.getset = AsyncMock(side_effect=[RuntimeError("poison"), previous_val])

        try:
            await cp.checkpoint("shard-0", "seq-1")
            await cp.checkpoint("shard-1", "seq-9")

            with pytest.raises(CheckpointFlushError) as exc_info:
                await cp.manual_checkpoint()

            # Only shard-0 is in the error list; shard-1 was attempted after it.
            assert [shard_id for shard_id, _ in exc_info.value.errors] == ["shard-0"]

            # shard-0 stays buffered, shard-1 was flushed and popped. Without the
            # continue-on-error fix, shard-1 would still be buffered here.
            assert cp._manual_checkpoints == {"shard-0": "seq-1"}
        finally:
            await cp.close()

        assert collector.counters[f"consumer_checkpoint_failure_total{{{_shard_labels('shard-0')}}}"] == 1
        assert collector.counters[f"consumer_checkpoint_success_total{{{_shard_labels('shard-1')}}}"] == 1

    @pytest.mark.asyncio
    async def test_checkpoint_flush_error_message_and_cause(self):
        """CheckpointFlushError lists every failing shard in its message, and
        ``__cause__`` points at the first underlying exception for traceback."""
        cp = await _make_redis_cp(auto_checkpoint=False)
        cp.client.getset = AsyncMock(return_value=None)  # triggers CheckpointOwnershipConflict

        try:
            await cp.checkpoint("shard-0", "seq-1")
            await cp.checkpoint("shard-1", "seq-9")

            with pytest.raises(CheckpointFlushError) as exc_info:
                await cp.manual_checkpoint()
        finally:
            await cp.close()

        exc = exc_info.value
        assert "2 shard(s) failed" in str(exc)
        assert "shard-0" in str(exc) and "shard-1" in str(exc)
        # __cause__ is the first collected exception (insertion order).
        assert exc.__cause__ is exc.errors[0][1]
        assert isinstance(exc.__cause__, CheckpointOwnershipConflict)

    @pytest.mark.asyncio
    async def test_checkpoint_ref_mismatch_raises_with_conflicting_owner(self):
        """Issue #81: ref-mismatch branch of _checkpoint must raise
        CheckpointOwnershipConflict and name the *conflicting* owner (the ref
        that currently holds the lease), not this consumer's own ref. The
        original code printed val["ref"] (self), which was useless for
        diagnosing who stole the lease.
        """
        cp = await _make_redis_cp()
        other_owner = "other-consumer/12345"
        previous_val = '{"ref": "' + other_owner + '", "ts": 0, "sequence": "seq-prev"}'
        cp.client.getset = AsyncMock(return_value=previous_val)

        try:
            with pytest.raises(CheckpointOwnershipConflict) as exc_info:
                await cp._checkpoint("shard-0", "seq-1")
        finally:
            await cp.close()

        msg = str(exc_info.value)
        assert other_owner in msg, f"expected conflicting owner in message: {msg!r}"

    @pytest.mark.asyncio
    async def test_manual_checkpoint_no_raise_on_all_success(self):
        """Regression guard: when every flush succeeds, manual_checkpoint() must
        not raise and must drain the buffer."""
        cp = await _make_redis_cp(auto_checkpoint=False)

        previous_val = '{"ref": "' + cp.get_ref() + '", "ts": 0, "sequence": "seq-prev"}'
        cp.client.getset = AsyncMock(return_value=previous_val)

        try:
            await cp.checkpoint("shard-0", "seq-1")
            await cp.checkpoint("shard-1", "seq-9")

            await cp.manual_checkpoint()  # must not raise

            assert cp._manual_checkpoints == {}
        finally:
            await cp.close()

    @pytest.mark.asyncio
    async def test_manual_checkpoint_flush_retries_remaining_on_next_call(self):
        """After a flush leaves a failing shard buffered, a subsequent
        manual_checkpoint() call re-attempts it."""
        collector = InMemoryMetricsCollector()
        cp = await _make_redis_cp(auto_checkpoint=False)
        cp.bind_metrics(collector, {"stream_name": "test-stream"})

        previous_val = '{"ref": "' + cp.get_ref() + '", "ts": 0, "sequence": "seq-prev"}'
        # First flush: shard-0 raises, shard-1 succeeds. Second flush: shard-0 succeeds.
        cp.client.getset = AsyncMock(side_effect=[RuntimeError("flake"), previous_val, previous_val])

        try:
            await cp.checkpoint("shard-0", "seq-1")
            await cp.checkpoint("shard-1", "seq-9")

            with pytest.raises(CheckpointFlushError):
                await cp.manual_checkpoint()
            # shard-1 was flushed in the same call (continue-on-error), only the
            # failing shard stays buffered for retry.
            assert set(cp._manual_checkpoints) == {"shard-0"}

            # Second flush drains the buffer.
            await cp.manual_checkpoint()
            assert cp._manual_checkpoints == {}
        finally:
            await cp.close()

    @pytest.mark.asyncio
    async def test_manual_checkpoint_flush_preserves_concurrent_newer_sequence(self):
        """If checkpoint(shard, newer_seq) runs while manual_checkpoint is awaiting
        the backend write for shard@older_seq, the newer buffered sequence must not
        be popped (otherwise we'd silently lose it and process records twice)."""
        cp = await _make_redis_cp(auto_checkpoint=False)

        original_checkpoint = cp._checkpoint

        async def interposing_checkpoint(shard_id, sequence):
            # Simulate a concurrent checkpoint() landing during the in-flight flush:
            # it overwrites the buffered entry with a newer sequence.
            if shard_id == "shard-0" and sequence == "seq-1":
                cp._manual_checkpoints["shard-0"] = "seq-2"
            return await original_checkpoint(shard_id, sequence)

        previous_val = '{"ref": "' + cp.get_ref() + '", "ts": 0, "sequence": "seq-prev"}'
        cp.client.getset = AsyncMock(return_value=previous_val)
        cp._checkpoint = interposing_checkpoint

        try:
            await cp.checkpoint("shard-0", "seq-1")
            await cp.manual_checkpoint()

            # seq-2 was buffered during the await; the conditional pop must have
            # spared it. A subsequent flush will drain it on the next call.
            assert cp._manual_checkpoints == {"shard-0": "seq-2"}
        finally:
            await cp.close()

    @pytest.mark.asyncio
    async def test_manual_checkpoint_flush_success_path_emits_per_shard(self):
        """Happy-path manual flush on MemoryCheckPointer-style emission (direct exercise)."""
        collector = InMemoryMetricsCollector()
        cp = MemoryCheckPointer("test")
        cp.bind_metrics(collector, {"stream_name": "test-stream"})
        await cp.allocate("shard-0")
        await cp.allocate("shard-1")

        await cp.checkpoint("shard-0", "seq-1")
        await cp.checkpoint("shard-1", "seq-9")

        assert collector.counters[f"consumer_checkpoint_success_total{{{_shard_labels('shard-0')}}}"] == 1
        assert collector.counters[f"consumer_checkpoint_success_total{{{_shard_labels('shard-1')}}}"] == 1

    @pytest.mark.asyncio
    async def test_checkpointer_without_consumer_uses_standalone_label(self):
        """Checkpointer used directly (no Consumer) defaults to stream_name=<standalone>."""
        collector = InMemoryMetricsCollector()
        from kinesis.metrics import get_metrics_collector as _get

        # Temporarily make <collector> the global so default __init__ picks it up.
        previous = _get()
        set_metrics_collector(collector)
        try:
            cp = MemoryCheckPointer("test")
            await cp.allocate("shard-0")
            await cp.checkpoint("shard-0", "seq-1")
        finally:
            set_metrics_collector(previous)

        key = "consumer_checkpoint_success_total{shard_id=shard-0,stream_name=<standalone>}"
        assert collector.counters[key] == 1

    @pytest.mark.asyncio
    async def test_heartbeat_does_not_emit_checkpoint_counters(self):
        """Regression guard: heartbeat must not route through _checkpoint."""
        collector = InMemoryMetricsCollector()
        cp = await _make_redis_cp()
        cp.bind_metrics(collector, {"stream_name": "test-stream"})
        cp._items = {"shard-0": "seq-1"}
        cp.client.set = AsyncMock(return_value=True)

        # Simulate one heartbeat tick synchronously (without the sleep loop)
        key = cp.get_key("shard-0")
        val = {"ref": cp.get_ref(), "ts": cp.get_ts(), "sequence": "seq-1"}
        await cp.do_heartbeat(key, val)

        try:
            pass
        finally:
            await cp.close()

        assert not any(k.startswith("consumer_checkpoint_") for k in collector.counters)

    @pytest.mark.asyncio
    async def test_bind_metrics_rebind_with_same_args_is_noop(self):
        cp = MemoryCheckPointer("test")
        collector = InMemoryMetricsCollector()
        labels = {"stream_name": "test-stream"}
        cp.bind_metrics(collector, labels)
        cp.bind_metrics(collector, labels)  # idempotent: must not raise

    @pytest.mark.asyncio
    async def test_bind_metrics_rebind_with_different_collector_raises(self):
        cp = MemoryCheckPointer("test")
        cp.bind_metrics(InMemoryMetricsCollector(), {"stream_name": "a"})
        with pytest.raises(RuntimeError, match="already bound"):
            cp.bind_metrics(InMemoryMetricsCollector(), {"stream_name": "a"})
        with pytest.raises(RuntimeError, match="already bound"):
            cp.bind_metrics(InMemoryMetricsCollector(), {"stream_name": "b"})

    @pytest.mark.asyncio
    async def test_bind_metrics_without_stream_name_raises(self):
        cp = MemoryCheckPointer("test")
        with pytest.raises(ValueError, match="stream_name"):
            cp.bind_metrics(InMemoryMetricsCollector(), {})
        with pytest.raises(ValueError, match="stream_name"):
            cp.bind_metrics(InMemoryMetricsCollector(), {"shard_id": "shard-0"})

    @pytest.mark.asyncio
    async def test_bind_metrics_missing_stream_name_raises_before_idempotent_rebind(self):
        """Regression guard: the stream_name check must run before the
        _metrics_bound short-circuit, so a malformed rebind can't slip past
        the guard via the idempotent-same-args path."""
        cp = MemoryCheckPointer("test")
        collector = InMemoryMetricsCollector()
        cp.bind_metrics(collector, {"stream_name": "test-stream"})  # first bind ok
        with pytest.raises(ValueError, match="stream_name"):
            cp.bind_metrics(collector, {})
        with pytest.raises(ValueError, match="stream_name"):
            cp.bind_metrics(collector, {"shard_id": "shard-0"})

    @pytest.mark.asyncio
    async def test_prometheus_checkpoint_label_roundtrip(self):
        """End-to-end label-shape check: bind → emit → PrometheusCounter.labels()
        must succeed for both a real stream_name and the standalone sentinel.

        Regression guard: Prometheus registers labelnames=["stream_name","shard_id"]
        (see kinesis/prometheus.py). If the checkpointer ever omits a label or adds
        an unexpected one at emit time, prometheus_client raises and the test fails.
        """
        prometheus_client = pytest.importorskip("prometheus_client")
        from kinesis import PrometheusMetricsCollector

        registry = prometheus_client.CollectorRegistry()
        collector = PrometheusMetricsCollector(registry=registry)

        # Case 1: Consumer-wired stream label
        cp1 = MemoryCheckPointer("test-wired")
        cp1.bind_metrics(collector, {"stream_name": "my-stream"})
        await cp1.allocate("shard-0")
        await cp1.checkpoint("shard-0", "seq-1")

        # Case 2: Standalone sentinel (no bind)
        from kinesis.checkpointers import STANDALONE_STREAM_LABEL

        previous = get_metrics_collector()
        set_metrics_collector(collector)
        try:
            cp2 = MemoryCheckPointer("test-standalone")
            await cp2.allocate("shard-1")
            await cp2.checkpoint("shard-1", "seq-2")
        finally:
            set_metrics_collector(previous)

        # Both label-shapes must be retrievable from the registry — proves
        # prometheus_client accepted them without label-mismatch errors.
        wired = registry.get_sample_value(
            "async_kinesis_consumer_checkpoint_success_total",
            {"stream_name": "my-stream", "shard_id": "shard-0"},
        )
        standalone = registry.get_sample_value(
            "async_kinesis_consumer_checkpoint_success_total",
            {"stream_name": STANDALONE_STREAM_LABEL, "shard_id": "shard-1"},
        )
        assert wired == 1.0
        assert standalone == 1.0


class TestStreamMetrics:
    @pytest.mark.asyncio
    async def test_refresh_shards_emits_active_and_closed_gauges(self, mock_consumer):
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer._last_shard_refresh = 0  # Force refresh
        consumer.skip_describe_stream = False
        consumer.use_list_shards = False
        consumer._closed_shards = {"shard-closed"}

        consumer.get_stream_description = AsyncMock(
            return_value={
                "StreamStatus": consumer.ACTIVE,
                "Shards": [
                    {"ShardId": "shard-0"},
                    {"ShardId": "shard-1"},
                    {"ShardId": "shard-closed"},
                ],
            }
        )

        await consumer.refresh_shards()

        assert collector.gauges["stream_shards_active{stream_name=test-stream}"] == 2
        assert collector.gauges["stream_shards_closed{stream_name=test-stream}"] == 1

    @pytest.mark.asyncio
    async def test_resharding_detected_emits_counter(self, mock_consumer):
        collector = InMemoryMetricsCollector()
        consumer = mock_consumer(metrics_collector=collector)
        consumer._last_shard_refresh = 0
        consumer.skip_describe_stream = False
        consumer.use_list_shards = False

        # Existing parent, new child appears → resharding event.
        consumer.shards = [
            {"ShardId": "parent-0", "SequenceNumberRange": {"StartingSequenceNumber": "1"}},
        ]
        consumer.get_stream_description = AsyncMock(
            return_value={
                "StreamStatus": consumer.ACTIVE,
                "Shards": [
                    {
                        "ShardId": "parent-0",
                        "SequenceNumberRange": {"StartingSequenceNumber": "1"},
                    },
                    {
                        "ShardId": "child-0",
                        "ParentShardId": "parent-0",
                        "SequenceNumberRange": {"StartingSequenceNumber": "100"},
                    },
                ],
            }
        )

        await consumer.refresh_shards()

        assert collector.counters["stream_resharding_events_total{stream_name=test-stream}"] == 1
