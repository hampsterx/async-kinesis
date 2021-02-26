import asyncio
import logging
import time
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
            retry_limit=None,
            expo_backoff=None,
            expo_backoff_limit=120,
            skip_describe_stream=False,
            create_stream=False,
            create_stream_shards=1,
            shard_refresh_timer=(60 * 15)
    ):

        super(Consumer, self).__init__(
            stream_name,
            endpoint_url=endpoint_url,
            region_name=region_name,
            retry_limit=retry_limit,
            expo_backoff=expo_backoff,
            expo_backoff_limit=expo_backoff_limit,
            skip_describe_stream=skip_describe_stream,
            create_stream=create_stream,
            create_stream_shards=create_stream_shards,
            shard_refresh_timer=shard_refresh_timer
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
        log.debug("Closing Connection..")
        if not self.stream_status == self.RECONNECT:

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
                await self.sync_shards()
                await self.fetch()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log.exception(e)

    async def fetch(self):

        if not self.is_fetching:
            return

        for shard in self.shards:

            if not self.is_fetching:
                break

            if shard.get("fetch"):
                await self.get_fetch(shard)
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
                await self.get_conn()
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

                elif code == "InternalFailure":
                    log.warning(
                        "Received InternalFailure from Kinesis, rebuilding connection.. "
                    )
                    await self.get_conn()

                else:
                    log.warning("ClientError {}. sleeping..".format(code))
                    await asyncio.sleep(3)

            except Exception as e:
                log.warning("Unknown error {}. sleeping..".format(e))
                await asyncio.sleep(3)

            # Connection or other issue
            return None

    async def get_shard_iterator(self, shard_id, last_sequence_number=None):

        # Issue with get_iterator when new shards are created.
        # https://github.com/mhart/kinesalite/pull/103#issuecomment-782765923

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

        # Start task to fetch periodically

        self.fetch_task = asyncio.create_task(self._fetch())

        # Wait a while until we have some results
        for i in range(0, wait_iterations):
            if self.fetch_task and self.queue.qsize() == 0:
                await asyncio.sleep(wait_sleep)

        log.debug("start_consumer completed.. queue size={}".format(self.queue.qsize()))

    @staticmethod
    def _arrival_time(records, shard, total_items):
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

    async def _add_checkpoint_record(self, result, shard):

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

        return last_record["SequenceNumber"]

    async def get_fetch(self, shard):
        # actions done to this shard is pointed back to the same shard in self.shards

        if shard["fetch"].done():
            result = shard["fetch"].result()

            if not result:
                shard["fetch"] = None
                return

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

                if not result.get("NextShardIterator") or result.get("NextShardIterator") == 'null':
                    shard["fetch"] = None
                    await self.checkpointer.deallocate(shard["ShardId"])

                    self.shards = [x for x in self.shards
                                   if x['ShardId'] != shard['ShardId']
                                   ]
                    return

                self._arrival_time(records, shard, total_items)

                shard["LastSequenceNumber"] = await self._add_checkpoint_record(result, shard)

            else:
                log.debug(
                    "Shard {} caught up, sleeping {}s".format(
                        shard["ShardId"], self.sleep_time_no_records
                    )
                )
                await asyncio.sleep(self.sleep_time_no_records)
                shard["fetch"] = None
                return

            shard["ShardIterator"] = result["NextShardIterator"]

            shard["fetch"] = None

    async def _spilt_shards(self, shards):
        new_shards = []
        for shard in shards:

            if type(self.max_shard_consumers) == int and self.max_shard_consumers <= len(new_shards):
                continue

            if shard["SequenceNumberRange"].get("EndingSequenceNumber"):
                existing_shard = any(x for x in self.shards if x['ShardId'] == shard["ShardId"])
                if existing_shard:
                    ## TODO: change check point after reshard
                    ## NotImplementedError: test-b64c5c96/100942 checkpointed on shardId-000000000000 but ref is different test-b64c5c96/100942
                    await self.checkpointer.deallocate(shard["ShardId"])
                continue

            success, checkpoint = await self.checkpointer.allocate(shard["ShardId"])
            if not success and self.checkpointer.is_allocated(shard["ShardId"]):
                continue
            else:
                shard["ShardIterator"] = await self.get_shard_iterator(
                    shard_id=shard["ShardId"], last_sequence_number=checkpoint
                )
                shard["stats"] = ShardStats()
                shard["throttler"] = Throttler(
                    rate_limit=self.shard_fetch_rate, period=1
                )
            new_shards.append(shard)

        self.shards = new_shards
        log.info(
            " Stream {} has added shards ids {}".format(
                self.stream_name, ", ".join([x['ShardId'] for x in new_shards])
            )
        )

    async def _merge_shards(self, shards):
        new_shards = []
        old_shards = []
        for shard in shards:
            if shard["SequenceNumberRange"].get("EndingSequenceNumber"):
                existing_shard = any(x for x in self.shards if x['ShardId'] == shard["ShardId"])
                if existing_shard:
                    await self.checkpointer.deallocate(shard["ShardId"])
                    old_shards.append(shard)
                continue
            new_shards.append(shard)
        self.shards = new_shards
        log.info(
            " Stream {} has removed stale shards ids {}".format(
                self.stream_name, ", ".join([x['ShardId'] for x in old_shards])
            )
        )

    async def __anext__(self):

        if not self.shards:
            await self.get_conn()

        if not self.fetch_task:
            await self.start_consumer()

        # Raise exception from Fetch Task to main task otherwise raise exception inside
        # Fetch Task will fail silently
        if self.fetch_task.done():
            raise self.fetch_task.exception()

        while True:
            try:
                item = self.queue.get_nowait()

                if item and isinstance(item, dict) and "__CHECKPOINT__" in item:
                    if self.checkpointer:
                        await self.checkpointer.checkpoint(
                            item["__CHECKPOINT__"]["ShardId"],
                            item["__CHECKPOINT__"]["SequenceNumber"],
                        )
                    continue

                return item

            except QueueEmpty:
                log.debug("Queue empty..")
                await asyncio.sleep(self.sleep_time_no_records)
                raise StopAsyncIteration
