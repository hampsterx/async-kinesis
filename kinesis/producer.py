import asyncio
import logging
import time
from aiohttp import ClientConnectionError

from asyncio.queues import QueueEmpty

from .utils import Throttler
from botocore.exceptions import ClientError

from .base import Base
from . import exceptions
from .aggregators import JsonWithoutAggregation

log = logging.getLogger(__name__)


class Producer(Base):
    async def _flush(self):
        while True:
            await asyncio.sleep(self.buffer_time, loop=self.loop)
            if not self.is_flushing:
                await self.flush()

    def __init__(
        self,
        stream_name,
        loop=None,
        endpoint_url=None,
        region_name=None,
        buffer_time=0.5,
        put_rate_limit_per_shard=1000,
        after_flush_fun=None,
        batch_size=500,
        max_queue_size=10000,
        aggregator=None,
    ):

        super(Producer, self).__init__(
            stream_name, loop=loop, endpoint_url=endpoint_url, region_name=region_name
        )

        self.buffer_time = buffer_time

        self.aggregator = aggregator if aggregator else JsonWithoutAggregation()

        self.queue = asyncio.Queue(maxsize=max_queue_size, loop=self.loop)

        self.batch_size = batch_size

        # A single shard can ingest up to 1 MiB of data per second (including partition keys)
        # or 1,000 records per second for writes
        self.put_rate_limit_per_shard = put_rate_limit_per_shard
        self.put_rate_throttle = None

        self.flush_task = asyncio.Task(self._flush(), loop=self.loop)
        self.is_flushing = False
        self.after_flush_fun = after_flush_fun

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
            rate_limit=self.put_rate_limit_per_shard * len(self.shards),
            period=1,
            loop=self.loop,
        )

    async def put(self, data):

        if not self.stream_status == "ACTIVE":
            await self.start()
            self.set_put_rate_throttle()

        if self.queue.qsize() >= self.batch_size:
            await self.flush()

        for output in self.aggregator.add_item(data):
            await self.queue.put(output)

    async def close(self):
        self.flush_task.cancel()
        await self.client.close()

    async def flush(self):

        self.is_flushing = True

        if self.aggregator.has_items():
            for output in self.aggregator.get_items():
                await self.queue.put(output)

        # Overflow in case we run into trouble with a batch
        overflow = []

        while True:

            items = []

            num = min(self.batch_size, self.queue.qsize() + len(overflow))

            for _ in range(num):
                async with self.put_rate_throttle:

                    if overflow:
                        items.append(overflow.pop())
                        continue

                    try:
                        items.append(self.queue.get_nowait())
                    except QueueEmpty:
                        break

            if not items:
                break

            log.debug("doing flush with {} items".format(len(items)))

            try:
                # todo: custom partition key
                result = await self.client.put_records(
                    Records=[
                        {
                            "Data": item,
                            "PartitionKey": "{0}{1}".format(time.clock(), time.time()),
                        }
                        for item in items
                    ],
                    StreamName=self.stream_name,
                )
            except ClientError as err:
                code = err.response["Error"]["Code"]

                if code == "ValidationException":
                    if (
                        "must have length less than or equal"
                        in err.response["Error"]["Message"]
                    ):
                        log.warning(
                            "Batch size {} exceeded the limit. retrying with 10% less".format(
                                len(items)
                            )
                        )
                        overflow = items
                        self.batch_size -= round(self.batch_size / 10)
                        continue
                    else:
                        raise
                elif code == "ResourceNotFoundException":
                    raise exceptions.StreamDoesNotExist(
                        "Stream '{}' does not exist".format(self.stream_name)
                    ) from None
                else:
                    raise
            except ClientConnectionError:
                log.warning("Connection error. sleeping..")
                overflow = items
                await asyncio.sleep(3, loop=self.loop)
                continue
            else:
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
                            "Throughput exceeding, slowing down the rate by 10%"
                        )
                        overflow = items
                        self.put_rate_limit_per_shard -= round(
                            self.put_rate_limit_per_shard / 10
                        )
                        self.set_put_rate_throttle()
                        # wait a bit
                        await asyncio.sleep(0.25, loop=self.loop)
                        continue

                    else:
                        raise exceptions.UnknownException(
                            "Failed to put records but not due to throughput exceeded: {}".format(
                                ", ".join(errors)
                            )
                        )

                else:
                    if self.after_flush_fun:
                        await self.after_flush_fun(items)

        self.is_flushing = False
