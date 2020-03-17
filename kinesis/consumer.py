import asyncio
import logging
from datetime import datetime, timezone
from aiohttp import ClientConnectionError
from asyncio import TimeoutError
from asyncio.queues import QueueEmpty
from botocore.exceptions import ClientError
from .utils import Throttler
from .base import Base
from .checkpointers import MemoryCheckPointer
from .processors import JsonProcessor

log = logging.getLogger(__name__)


class ShardStats:
    def __init__(self):
        self._throttled = 0
        self._success = 0

    def succeded(self):
        self._success += 1

    def throttled(self):
        self._throttled += 1

    def to_data(self):
        return {"throttled": self._throttled, "success": self._success}


class Consumer(Base):
    def __init__(
        self,
        stream_name,
        endpoint_url=None,
        region_name=None,
        max_queue_size=10000,
        max_shard_consumers=None,
        record_limit=10000,
        sleep_time_no_records=2,
        iterator_type="TRIM_HORIZON",
        shard_fetch_rate=1,
        checkpointer=None,
        processor=None,
    ):

        super(Consumer, self).__init__(
            stream_name, endpoint_url=endpoint_url, region_name=region_name
        )

        self.queue = asyncio.Queue(maxsize=max_queue_size)

        self.sleep_time_no_records = sleep_time_no_records

        self.max_shard_consumers = max_shard_consumers

        self.record_limit = record_limit

        self.is_fetching = True

        self.checkpointer = checkpointer if checkpointer else MemoryCheckPointer()

        self.processor = processor if processor else JsonProcessor()

        self.iterator_type = iterator_type

        self.fetch_task = None

        self.shard_fetch_rate = shard_fetch_rate

    def __aiter__(self):
        return self

    async def close(self):
        log.debug("Closing..")

        await self.flush()

        if self.fetch_task:
            self.fetch_task.cancel()
            self.fetch_task = None

        if self.checkpointer:
            await self.checkpointer.close()

        await self.client.close()

    async def flush(self):

        self.is_fetching = False

        if not self.shards:
            return

        # Wait for shard fetches to finish
        # todo: use gather
        for shard in self.shards:
            if shard.get("fetch"):
                if not shard["fetch"].done():
                    await shard["fetch"]

    async def _fetch(self):
        while self.is_fetching:
            # Ensure fetch is performed at most 5 times per second (the limit per shard)
            await asyncio.sleep(0.2)
            try:
                await self.fetch()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log.exception(e)

    async def fetch(self):

        if not self.is_fetching:
            return

        # todo: check for/handle new shards

        shards_in_use = [
            s for s in self.shards if self.checkpointer.is_allocated(s["ShardId"])
        ]

        # log.debug("shards in use: {}".format([s["ShardId"] for s in shards_in_use]))

        for shard in self.shards:

            if not self.is_fetching:
                break

            if not self.checkpointer.is_allocated(shard["ShardId"]):
                if (
                    self.max_shard_consumers
                    and len(shards_in_use) >= self.max_shard_consumers
                ):
                    continue

                if self.checkpointer is None:
                    log.debug("Marking shard in use {}".format(shard["ShardId"]))
                    shard["ShardIterator"] = await self.get_shard_iterator(
                        shard_id=shard["ShardId"]
                    )

                else:
                    success, checkpoint = await self.checkpointer.allocate(
                        shard["ShardId"]
                    )

                    if not success:
                        log.debug(
                            "Shard in use. Could not assign shard {} to checkpointer[{}]".format(
                                shard["ShardId"], self.checkpointer.get_ref()
                            )
                        )
                        continue

                    log.debug(
                        "Marking shard in use {} by checkpointer[{}] @ {}".format(
                            shard["ShardId"], self.checkpointer.get_ref(), checkpoint
                        )
                    )

                    shard["ShardIterator"] = await self.get_shard_iterator(
                        shard_id=shard["ShardId"], last_sequence_number=checkpoint
                    )

                if "ShardIterator" in shard:
                    shard["stats"] = ShardStats()
                    shard["throttler"] = Throttler(
                        rate_limit=self.shard_fetch_rate, period=1
                    )
                    shards_in_use.append(shard)

                    log.debug("Shard count now at {}".format(len(shards_in_use)))

            if shard.get("fetch"):
                if shard["fetch"].done():
                    result = shard["fetch"].result()

                    if not result:
                        shard["fetch"] = None
                        continue

                    records = result["Records"]

                    if records:
                        log.debug(
                            "Shard {} got {} records".format(
                                shard["ShardId"], len(records)
                            )
                        )

                        total_items = 0
                        for row in result["Records"]:
                            for n, output in enumerate(
                                self.processor.parse(row["Data"])
                            ):
                                await self.queue.put(output)
                            total_items += n + 1

                        # Get approx minutes behind..
                        last_arrival = records[-1].get("ApproximateArrivalTimestamp")
                        if last_arrival:
                            last_arrival = round(
                                (
                                    (
                                        datetime.now(timezone.utc) - last_arrival
                                    ).total_seconds()
                                    / 60
                                )
                            )

                            log.debug(
                                "Shard {} added {} items from {} records. Consumer is {}m behind".format(
                                    shard["ShardId"],
                                    total_items,
                                    len(records),
                                    last_arrival,
                                ),
                                extra={"consumer_behind_m": last_arrival},
                            )

                        else:
                            # ApproximateArrivalTimestamp not available in kinesis-lite
                            log.debug(
                                "Shard {} added {} items from {} records".format(
                                    shard["ShardId"], total_items, len(records)
                                )
                            )

                        # Add checkpoint record
                        last_record = result["Records"][-1]
                        await self.queue.put(
                            {
                                "__CHECKPOINT__": {
                                    "ShardId": shard["ShardId"],
                                    "SequenceNumber": last_record["SequenceNumber"],
                                }
                            }
                        )

                        shard["LastSequenceNumber"] = last_record["SequenceNumber"]

                    else:
                        log.debug(
                            "Shard {} caught up, sleeping {}s".format(
                                shard["ShardId"], self.sleep_time_no_records
                            )
                        )
                        await asyncio.sleep(self.sleep_time_no_records)

                    if not result["NextShardIterator"]:
                        raise NotImplementedError("Shard is closed?")

                    shard["ShardIterator"] = result["NextShardIterator"]

                    shard["fetch"] = None

                else:
                    # log.debug("shard {} fetch in progress..".format(shard['ShardId']))
                    continue

            if "ShardIterator" in shard and shard["ShardIterator"] is not None:
                shard["fetch"] = asyncio.create_task(self.get_records(shard=shard))

    async def get_records(self, shard):

        # Note: "This operation has a limit of five transactions per second per account."

        async with shard["throttler"]:
            # log.debug("get_records shard={}".format(shard['ShardId']))

            try:

                result = await self.client.get_records(
                    ShardIterator=shard["ShardIterator"], Limit=self.record_limit
                )

                shard["stats"].succeded()
                return result

            except ClientConnectionError as e:
                log.warning("Connection error {}. sleeping..".format(e))
                await asyncio.sleep(3)
            except TimeoutError as e:
                log.warning("Timeout {}. sleeping..".format(e))
                await asyncio.sleep(3)

            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code == "ProvisionedThroughputExceededException":
                    log.warning(
                        "{} hit ProvisionedThroughputExceededException".format(
                            shard["ShardId"]
                        )
                    )
                    shard["stats"].throttled()
                    # todo: control the throttle ?
                    await asyncio.sleep(0.25)

                elif code == "ExpiredIteratorException":
                    log.warning(
                        "{} hit ExpiredIteratorException".format(shard["ShardId"])
                    )

                    shard["ShardIterator"] = await self.get_shard_iterator(
                        shard_id=shard["ShardId"],
                        last_sequence_number=shard.get("LastSequenceNumber"),
                    )

                else:
                    log.warning("ClientError {}. sleeping..".format(code))
                    await asyncio.sleep(3)

            except Exception as e:
                log.warning("Unknown error {}. sleeping..".format(e))
                await asyncio.sleep(3)

        # Connection or other issue
        return None

    async def get_shard_iterator(self, shard_id, last_sequence_number=None):

        log.debug(
            "getting shard iterator for {} @ {}".format(
                shard_id,
                last_sequence_number if last_sequence_number else self.iterator_type,
            )
        )

        params = {
            "StreamName": self.stream_name,
            "ShardId": shard_id,
            "ShardIteratorType": "AFTER_SEQUENCE_NUMBER"
            if last_sequence_number
            else self.iterator_type,
        }

        if last_sequence_number:
            params["StartingSequenceNumber"] = last_sequence_number

        response = await self.client.get_shard_iterator(**params)
        return response["ShardIterator"]

    async def start_consumer(self, wait_iterations=10, wait_sleep=0.25):

        await self.start()

        # Start task to fetch periodically
        self.fetch_task = asyncio.Task(self._fetch())

        # Wait a while until we have some results
        for i in range(0, wait_iterations):
            if self.fetch_task and self.queue.qsize() == 0:
                await asyncio.sleep(wait_sleep)

        log.debug("start_consumer completed.. queue size={}".format(self.queue.qsize()))

    async def __anext__(self):

        if not self.shards:
            await self.start_consumer()

        while True:
            try:
                item = self.queue.get_nowait()

                if item and "__CHECKPOINT__" in item:
                    if self.checkpointer:
                        await self.checkpointer.checkpoint(
                            item["__CHECKPOINT__"]["ShardId"],
                            item["__CHECKPOINT__"]["SequenceNumber"],
                        )
                        continue
                    else:
                        continue

                return item

            except QueueEmpty:
                log.debug("Queue empty..")
                await asyncio.sleep(self.sleep_time_no_records)
                raise StopAsyncIteration
