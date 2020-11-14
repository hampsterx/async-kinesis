import asyncio
import logging
import time
import math
from aiohttp import ClientConnectionError

from asyncio.queues import QueueEmpty

from .utils import Throttler
from botocore.exceptions import ClientError

from .base import Base
from . import exceptions
from .processors import JsonProcessor

log = logging.getLogger(__name__)


class Producer(Base):
    def __init__(
            self,
            stream_name,
            endpoint_url=None,
            region_name=None,
            buffer_time=0.5,
            put_rate_limit_per_shard=1000,
            put_bandwidth_limit_per_shard=1024,
            after_flush_fun=None,
            batch_size=500,
            max_queue_size=10000,
            processor=None,
            skip_describe_stream=False,
            conn_error_retry_limit=None,
            conn_error_expo_backoff=None
    ):

        super(Producer, self).__init__(
            stream_name, endpoint_url=endpoint_url, region_name=region_name,
            conn_error_retry_limit=conn_error_retry_limit,
            conn_error_expo_backoff=conn_error_expo_backoff
        )

        self.buffer_time = buffer_time

        self.processor = processor if processor else JsonProcessor()

        self.queue = asyncio.Queue(maxsize=max_queue_size)

        self.batch_size = batch_size

        # Short Lived producer might want to skip describing stream on startup
        self.skip_describe_stream = skip_describe_stream

        # A single shard can ingest up to 1 MiB of data per second (including partition keys)
        # or 1,000 records per second for writes
        self.put_rate_limit_per_shard = put_rate_limit_per_shard
        self.put_rate_throttle = None
        self.put_bandwidth_limit_per_shard = put_bandwidth_limit_per_shard
        self.put_bandwidth_throttle = None

        if put_bandwidth_limit_per_shard > 1024:
            log.warning(
                (
                    "Put bandwidth {}kb exceeds 1024kb. Expect throughput errors..".format(
                        put_bandwidth_limit_per_shard
                    )
                )
            )

        self.flush_task = asyncio.Task(self._flush())
        self.is_flushing = False
        self.after_flush_fun = after_flush_fun

        # Keep flush task looping while active
        self.active = True

        # keep track of these (used by unit test only)
        self.throughput_exceeded_count = 0

        # overflow buffer
        self.overflow = []

        self.flush_total_records = 0
        self.flush_total_size = 0

    async def create_stream(self, shards=1, ignore_exists=True):

        log.debug(
            "Creating (or ignoring) stream {} with {} shards".format(
                self.stream_name, shards
            )
        )

        if shards < 1:
            raise Exception("Min shard count is one")

        try:
            await self.client.create_stream(
                StreamName=self.stream_name, ShardCount=shards
            )
        except ClientError as err:
            code = err.response["Error"]["Code"]

            if code == "ResourceInUseException":
                if not ignore_exists:
                    raise exceptions.StreamExists(
                        "Stream '{}' exists, cannot create it".format(self.stream_name)
                    ) from None
            elif code == "LimitExceededException":
                raise exceptions.StreamShardLimit(
                    "Stream '{}' exceeded shard limit".format(self.stream_name)
                )
            else:
                raise

    def set_put_rate_throttle(self):
        self.put_rate_throttle = Throttler(
            rate_limit=self.put_rate_limit_per_shard
                       * (len(self.shards) if self.shards else 1),
            period=1,
        )
        self.put_bandwidth_throttle = Throttler(
            # kb per second. Go below a bit to avoid hitting the threshold
            size_limit=self.put_bandwidth_limit_per_shard
                       * (len(self.shards) if self.shards else 1),
            period=1,
        )

    async def put(self, data):

        if not self.stream_status == "ACTIVE":
            await self.start(skip_describe_stream=self.skip_describe_stream)
            self.set_put_rate_throttle()

        if self.queue.qsize() >= self.batch_size:
            await self.flush()

        for output in self.processor.add_item(data):
            await self.queue.put(output)

    async def close(self):
        self.active = False
        # Wait till task completes
        await self.flush_task

        # final flush (probably not required but no harm)
        await asyncio.shield(self.flush())

        await self.client.close()

    async def _flush(self):
        while self.active:
            await asyncio.sleep(self.buffer_time)
            if not self.is_flushing:
                await asyncio.shield(self.flush())

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
                log.debug(
                    "flush queue={} overflow={}".format(
                        self.queue.qsize(), len(self.overflow)
                    )
                )

            items = await self.get_batch()

            if not items:
                break

            else:
                result = await self._push_kinesis(items)
                await self.process_result(result, items)

        self.is_flushing = False

    async def process_result(self, result, items):
        if result["FailedRecordCount"]:

            errors = list(
                set(
                    [
                        r.get("ErrorCode")
                        for r in result["Records"]
                        if r.get("ErrorCode")
                    ]
                )
            )

            if not errors:
                raise exceptions.UnknownException(
                    "Failed to put records but no errorCodes return in results"
                )

            if "ProvisionedThroughputExceededException" in errors:
                log.warning(
                    "Throughput exceeded ({} records failed, added back..), pausing for 0.25s..".format(
                        result["FailedRecordCount"]
                    )
                )

                self.throughput_exceeded_count += 1

                for i, record in enumerate(result["Records"]):
                    if "ErrorCode" in record:
                        self.overflow.append(items[i])

                # log.debug("items={} overflow={}".format(len(items), len(overflow)))

                await asyncio.sleep(0.25)

            else:
                raise exceptions.UnknownException(
                    "Failed to put records due to: {}".format(", ".join(errors))
                )

        else:

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

                size_kb = math.ceil(item[0] / 1024)

                flush_max_size += size_kb

                if flush_max_size > 1024:
                    self.overflow.append(item)

                elif num <= self.batch_size:
                    async with self.put_bandwidth_throttle(size=self.flush_total_size):
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
        conn_error_retry_limit = self.conn_error_retry_limit
        conn_error_expo_backoff = self.conn_error_expo_backoff

        while True:

            try:

                # todo: custom partition key
                results = await self.client.put_records(
                    Records=[
                        {
                            "Data": item.data,
                            "PartitionKey": "{0}{1}".format(
                                time.perf_counter(), time.time()
                            ),
                        }
                        for item in items
                    ],
                    StreamName=self.stream_name,
                )

                log.debug(
                    "flush complete with {} record ({} items) @ {} kb".format(
                        len(items), self.flush_total_records, self.flush_total_size
                    )
                )
                return results

            except ClientError as err:

                code = err.response["Error"]["Code"]

                if code == "ValidationException":
                    if (
                            "must have length less than or equal"
                            in err.response["Error"]["Message"]
                    ):
                        log.warning(
                            "Batch size {} exceeded the limit. retrying with less".format(
                                len(items)
                            )
                        )

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
                        raise err
                elif code == "ResourceNotFoundException":
                    raise exceptions.StreamDoesNotExist(
                        "Stream '{}' does not exist".format(self.stream_name)
                    ) from None
                else:
                    raise err
            except ClientConnectionError:
                log.warning("Connection error. sleeping..")
                conn_error_expo_backoff = self.get_conn_error_expo_backoff(conn_error_expo_backoff)
                await asyncio.sleep(conn_error_expo_backoff)
                conn_error_retry_limit = await self.get_conn_error_retry(conn_error_retry_limit)

            except Exception as e:
                raise e

