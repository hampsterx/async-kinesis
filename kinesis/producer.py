import asyncio
import logging
import math
import time
from asyncio.queues import QueueEmpty
from typing import Any, Awaitable, Callable, Optional

from aiobotocore.session import AioSession
from aiohttp import ClientConnectionError
from botocore.exceptions import ClientError

from . import exceptions
from .base import Base
from .metrics import MetricsCollector, MetricType, get_metrics_collector
from .processors import JsonProcessor, Processor
from .utils import Throttler

log = logging.getLogger(__name__)


class Producer(Base):
    def __init__(
        self,
        stream_name: str,
        session: Optional[AioSession] = None,
        endpoint_url: Optional[str] = None,
        region_name: Optional[str] = None,
        buffer_time: float = 0.5,
        put_rate_limit_per_shard: int = 1000,
        put_bandwidth_limit_per_shard: int = 1024,
        after_flush_fun: Optional[Callable[[], Awaitable[None]]] = None,
        batch_size: int = 500,
        max_queue_size: int = 10000,
        processor: Optional[Processor] = None,
        skip_describe_stream: bool = False,
        use_list_shards: bool = False,
        retry_limit: Optional[int] = None,
        expo_backoff: Optional[float] = None,
        expo_backoff_limit: int = 120,
        create_stream: bool = False,
        create_stream_shards: int = 1,
        metrics_collector: Optional[MetricsCollector] = None,
    ) -> None:

        super(Producer, self).__init__(
            stream_name,
            session=session,
            endpoint_url=endpoint_url,
            region_name=region_name,
            retry_limit=retry_limit,
            expo_backoff=expo_backoff,
            expo_backoff_limit=expo_backoff_limit,
            skip_describe_stream=skip_describe_stream,
            use_list_shards=use_list_shards,
            create_stream=create_stream,
            create_stream_shards=create_stream_shards,
        )

        self.buffer_time = buffer_time

        self.processor = processor if processor else JsonProcessor()

        self.queue = asyncio.Queue(maxsize=max_queue_size)

        self.batch_size = batch_size

        # A single shard can ingest up to 1 MiB of data per second (including partition keys)
        # or 1,000 records per second for writes
        self.put_rate_limit_per_shard = put_rate_limit_per_shard
        self.put_rate_throttle = None
        self.put_bandwidth_limit_per_shard = put_bandwidth_limit_per_shard
        self.put_bandwidth_throttle = None

        if put_bandwidth_limit_per_shard > 1024:
            log.warning(
                ("Put bandwidth {}kb exceeds 1024kb. Expect throughput errors..".format(put_bandwidth_limit_per_shard))
            )
        self.set_put_rate_throttle()

        self.flush_task = asyncio.create_task(self._flush())
        self.is_flushing = False
        self.after_flush_fun = after_flush_fun
        self._stop_flush = False

        # keep track of these (used by unit test only)
        self.throughput_exceeded_count = 0

        # overflow buffer
        self.overflow = []

        self.flush_total_records = 0
        self.flush_total_size = 0

        # Metrics collector
        self.metrics = metrics_collector if metrics_collector else get_metrics_collector()

    def set_put_rate_throttle(self):
        self.put_rate_throttle = Throttler(
            rate_limit=self.put_rate_limit_per_shard * (len(self.shards) if self.shards else 1),
            period=1,
        )
        self.put_bandwidth_throttle = Throttler(
            # kb per second. Go below a bit to avoid hitting the threshold
            size_limit=self.put_bandwidth_limit_per_shard * (len(self.shards) if self.shards else 1),
            period=1,
        )

    def _validate_partition_key(self, partition_key: str) -> None:
        """Validate partition key according to AWS Kinesis requirements."""
        if not isinstance(partition_key, str):
            raise exceptions.ValidationError("Partition key must be a string")

        if not partition_key:
            raise exceptions.ValidationError("Partition key cannot be empty")

        # Check byte length (UTF-8 encoded)
        try:
            encoded_key = partition_key.encode("utf-8")
        except UnicodeEncodeError as e:
            raise exceptions.ValidationError("Partition key must be valid UTF-8") from e

        if len(encoded_key) > 256:
            raise exceptions.ValidationError(
                f"Partition key must be 256 bytes or less when UTF-8 encoded, got {len(encoded_key)} bytes"
            )

    async def put(self, data: Any, partition_key: Optional[str] = None) -> None:

        # Validate partition key if provided
        if partition_key is not None:
            self._validate_partition_key(partition_key)

        # Raise exception from Flush Task to main task otherwise raise exception inside
        # Flush Task will fail silently
        if self.flush_task.done():
            raise self.flush_task.exception()

        if not self.stream_status == self.ACTIVE:
            await self.get_conn()

        elif self.queue.qsize() >= self.batch_size:
            await self.flush()

        for output in self.processor.add_item(data, partition_key):
            await self.queue.put(output)

        # Update queue size metric
        self.metrics.gauge(MetricType.PRODUCER_QUEUE_SIZE, self.queue.qsize(), {"stream_name": self.stream_name})

    async def close(self):
        log.debug(f"Closing Connection.. (stream status:{self.stream_status})")
        if not self.stream_status == self.RECONNECT:
            # Stop the flush task gracefully first
            self._stop_flush = True

            # Cancel Flush Task if still running
            if self.flush_task and not self.flush_task.done():
                self.flush_task.cancel()
                try:
                    # Give the task a moment to handle cancellation properly
                    await asyncio.wait_for(self.flush_task, timeout=1.0)
                except asyncio.CancelledError:
                    pass
                except asyncio.TimeoutError:
                    log.debug("Flush task cancellation timed out")
                except Exception as e:
                    log.debug(f"Error during flush task cleanup: {e}")
            # final flush (probably not required but no harm)
            await self.flush()

        if self.client is not None:
            await self.client.close()

    async def _flush(self):
        try:
            while not self._stop_flush:
                if self.stream_status == self.ACTIVE:
                    if not self.is_flushing:
                        await self.flush()

                # Sleep in small increments to check stop flag more frequently
                sleep_remaining = self.buffer_time
                while sleep_remaining > 0 and not self._stop_flush:
                    sleep_time = min(0.1, sleep_remaining)  # Check every 100ms
                    await asyncio.sleep(sleep_time)
                    sleep_remaining -= sleep_time

        except asyncio.CancelledError:
            log.debug("Auto-flush task cancelled")
            raise

    async def flush(self):

        if self.is_flushing:
            log.debug("Flush already in progress, ignoring..")
            return

        self.is_flushing = True

        if self.processor.has_items():
            for output in self.processor.get_items():
                await self.queue.put(output)

        while True:

            self.flush_total_records = 0
            self.flush_total_size = 0

            if self.queue.qsize() > 0 or len(self.overflow) > 0:
                log.debug("flush queue={} overflow={}".format(self.queue.qsize(), len(self.overflow)))

            items = await self.get_batch()

            if not items:
                break

            else:
                # Measure flush duration
                with self.metrics.timer(MetricType.PRODUCER_FLUSH_DURATION, {"stream_name": self.stream_name}):
                    result = await self._push_kinesis(items)
                    await self.process_result(result, items)

                # Record batch size
                self.metrics.histogram(MetricType.PRODUCER_BATCH_SIZE, len(items), {"stream_name": self.stream_name})

        self.is_flushing = False

    async def process_result(self, result, items):
        if not result:
            log.warning("Received empty result from Kinesis, assuming success")
            if self.after_flush_fun:
                await self.after_flush_fun(items)
            return

        if result["FailedRecordCount"]:

            errors = list(set([r.get("ErrorCode") for r in result["Records"] if r.get("ErrorCode")]))

            if not errors:
                raise exceptions.UnknownException("Failed to put records but no errorCodes return in results")

            if "ProvisionedThroughputExceededException" in errors:
                log.warning(
                    "Throughput exceeded ({} records failed, added back..), pausing for 0.25s..".format(
                        result["FailedRecordCount"]
                    )
                )

                self.throughput_exceeded_count += 1

                # Track throttling metrics
                self.metrics.increment(
                    MetricType.PRODUCER_THROTTLES, result["FailedRecordCount"], {"stream_name": self.stream_name}
                )
                self.metrics.increment(
                    MetricType.PRODUCER_ERRORS,
                    result["FailedRecordCount"],
                    {"stream_name": self.stream_name, "error_type": "ProvisionedThroughputExceededException"},
                )

                for i, record in enumerate(result["Records"]):
                    if "ErrorCode" in record:
                        self.overflow.append(items[i])

                # log.debug("items={} overflow={}".format(len(items), len(overflow)))

                await asyncio.sleep(0.25)

            elif "InternalFailure" in errors:
                log.warning("Received InternalFailure from Kinesis")
                await self.get_conn()

                # Track internal failure metrics
                self.metrics.increment(
                    MetricType.PRODUCER_ERRORS,
                    result["FailedRecordCount"],
                    {"stream_name": self.stream_name, "error_type": "InternalFailure"},
                )

                for i, record in enumerate(result["Records"]):
                    if "ErrorCode" in record:
                        self.overflow.append(items[i])

            else:
                raise exceptions.UnknownException("Failed to put records due to: {}".format(", ".join(errors)))

        else:
            # Track successful records
            successful_count = len(items) - result.get("FailedRecordCount", 0)
            if successful_count > 0:
                # Calculate total bytes sent (approximate)
                total_bytes = sum(len(item.data) for item in items[:successful_count])

                self.metrics.increment(
                    MetricType.PRODUCER_RECORDS_SENT, successful_count, {"stream_name": self.stream_name}
                )
                self.metrics.increment(MetricType.PRODUCER_BYTES_SENT, total_bytes, {"stream_name": self.stream_name})

            if self.after_flush_fun:
                await self.after_flush_fun(items)

    async def get_batch(self):
        items = []
        flush_max_size = 0

        for num in range(self.queue.qsize() + len(self.overflow)):
            async with self.put_rate_throttle:

                if self.overflow:
                    item = self.overflow.pop()

                else:
                    try:
                        item = self.queue.get_nowait()
                    except QueueEmpty:
                        break

                # Calculate total size including partition key
                data_size = item[0]  # OutputItem.size (data payload)
                partition_key_size = len(item.partition_key.encode("utf-8")) if item.partition_key else 0
                total_size = data_size + partition_key_size
                size_kb = math.ceil(total_size / 1024)

                flush_max_size += size_kb

                if flush_max_size > 1024:
                    self.overflow.append(item)

                elif num <= self.batch_size:
                    async with self.put_bandwidth_throttle(size=size_kb):
                        items.append(item)
                        self.flush_total_size += size_kb
                        self.flush_total_records += item[1]
                else:
                    self.overflow.append(item)

        return items

    async def _push_kinesis(self, items):

        log.debug(
            "doing flush with {} record ({} items) @ {} kb".format(
                len(items), self.flush_total_records, self.flush_total_size
            )
        )

        while True:

            try:

                # Use custom partition key if provided, otherwise generate one
                results = await self.client.put_records(
                    Records=[
                        {
                            "Data": item.data,
                            "PartitionKey": (
                                item.partition_key
                                if item.partition_key is not None
                                else "{0}{1}".format(time.perf_counter(), time.time())
                            ),
                        }
                        for item in items
                    ],
                    **self.address,
                )

                log.info(
                    "flush complete with {} record ({} items) @ {} kb".format(
                        len(items), self.flush_total_records, self.flush_total_size
                    )
                )
                return results

            except ClientError as err:

                code = err.response["Error"]["Code"]

                if code == "ValidationException":
                    if "must have length less than or equal" in err.response["Error"]["Message"]:
                        log.warning("Batch size {} exceeded the limit. retrying with less".format(len(items)))

                        existing_batch_size = self.batch_size
                        self.batch_size -= round(self.batch_size / 10)

                        # Must be small batch of big items, take at least one out..
                        if existing_batch_size == self.batch_size:
                            self.batch_size -= 1

                        self.overflow.extend(items)

                        self.flush_total_records = 0
                        self.flush_max_size = 0
                        self.flush_total_size = 0

                        items = await self.get_batch()

                    else:
                        log.warning(f'Unknown ValidationException error code {err.response["Error"]["Code"]}')
                        log.exception(err)
                        await self.get_conn()
                        # raise err
                elif code == "ResourceNotFoundException":
                    raise exceptions.StreamDoesNotExist("Stream '{}' does not exist".format(self.stream_name)) from None
                else:
                    log.warning(f'Unknown Client error code {err.response["Error"]["Code"]}')
                    log.exception(err)
                    await self.get_conn()
                    # raise err
            except ClientConnectionError:
                await self.get_conn()
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception(e)
                log.critical("Unknown Exception Caught")
                await self.get_conn()
