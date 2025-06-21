import asyncio
import logging
from asyncio import TimeoutError
from asyncio.queues import QueueEmpty
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, Optional

from aiobotocore.session import AioSession
from aiohttp import ClientConnectionError
from botocore.exceptions import ClientError

from .base import Base
from .checkpointers import CheckPointer, MemoryCheckPointer
from .processors import JsonProcessor, Processor
from .utils import Throttler

log = logging.getLogger(__name__)


class ShardStats:
    def __init__(self) -> None:
        self._throttled: int = 0
        self._success: int = 0

    def succeded(self) -> None:
        self._success += 1

    def throttled(self) -> None:
        self._throttled += 1

    def to_data(self) -> Dict[str, int]:
        return {"throttled": self._throttled, "success": self._success}


class Consumer(Base):
    def __init__(
        self,
        stream_name: str,
        session: Optional[AioSession] = None,
        endpoint_url: Optional[str] = None,
        region_name: Optional[str] = None,
        max_queue_size: int = 10000,
        max_shard_consumers: Optional[int] = None,
        record_limit: int = 10000,
        sleep_time_no_records: float = 2,
        iterator_type: str = "TRIM_HORIZON",
        shard_fetch_rate: int = 1,
        checkpointer: Optional[CheckPointer] = None,
        processor: Optional[Processor] = None,
        retry_limit: Optional[int] = None,
        expo_backoff: Optional[float] = None,
        expo_backoff_limit: int = 120,
        skip_describe_stream: bool = False,
        use_list_shards: bool = False,
        create_stream: bool = False,
        create_stream_shards: int = 1,
        timestamp: Optional[datetime] = None,
    ) -> None:

        super(Consumer, self).__init__(
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

        self.timestamp = timestamp

        # Shard management
        self._last_shard_refresh = 0
        self._shard_refresh_interval = 60  # Refresh shards every 60 seconds
        self._closed_shards = set()  # Track shards that have been closed
        self._shard_topology = {}  # Track parent-child relationships
        self._parent_shards = set()  # Track shards that are parents
        self._child_shards = set()  # Track shards that are children
        self._exhausted_parents = set()  # Track parent shards that are fully consumed

    def __aiter__(self) -> AsyncIterator[Any]:
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
        if self.client is not None:
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
        error_count = 0
        max_errors = 10

        while self.is_fetching:
            # Ensure fetch is performed at most 5 times per second (the limit per shard)
            await asyncio.sleep(0.2)
            try:
                await self.fetch()
                error_count = 0  # Reset error count on successful fetch
            except asyncio.CancelledError:
                log.debug("Fetch task cancelled")
                self.is_fetching = False
                break
            except Exception as e:
                log.exception(e)
                error_count += 1
                if error_count >= max_errors:
                    log.error(f"Too many fetch errors ({max_errors}), stopping fetch task")
                    self.is_fetching = False
                    break
                await asyncio.sleep(min(2**error_count, 30))  # Exponential backoff

    async def fetch(self):

        if not self.is_fetching:
            return

        # Refresh shards to discover new ones and handle closed ones
        await self.refresh_shards()

        shards_in_use = [s for s in self.shards if self.checkpointer.is_allocated(s["ShardId"])]

        # log.debug("shards in use: {}".format([s["ShardId"] for s in shards_in_use]))

        for shard in self.shards:

            if not self.is_fetching:
                break

            # Skip shards that are known to be closed
            if shard["ShardId"] in self._closed_shards:
                continue

            if not self.checkpointer.is_allocated(shard["ShardId"]):
                # Check parent-child ordering before allocation
                if not self._should_allocate_shard(shard["ShardId"]):
                    log.debug(f"Skipping child shard {shard['ShardId']} - parent not exhausted")
                    continue

                if self.max_shard_consumers and len(shards_in_use) >= self.max_shard_consumers:
                    continue

                if self.checkpointer is None:
                    log.debug("Marking shard in use {}".format(shard["ShardId"]))
                    shard["ShardIterator"] = await self.get_shard_iterator(shard_id=shard["ShardId"])

                else:
                    success, checkpoint = await self.checkpointer.allocate(shard["ShardId"])

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
                    shard["throttler"] = Throttler(rate_limit=self.shard_fetch_rate, period=1)
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
                        log.debug("Shard {} got {} records".format(shard["ShardId"], len(records)))

                        total_items = 0
                        for row in result["Records"]:
                            for n, output in enumerate(self.processor.parse(row["Data"])):
                                try:
                                    await asyncio.wait_for(self.queue.put(output), timeout=30.0)
                                except asyncio.TimeoutError:
                                    log.warning("Queue put timed out, skipping record")
                                    continue
                            total_items += n + 1

                        # Get approx minutes behind..
                        last_arrival = records[-1].get("ApproximateArrivalTimestamp")
                        if last_arrival:
                            last_arrival = round(((datetime.now(timezone.utc) - last_arrival).total_seconds() / 60))

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
                        try:
                            await asyncio.wait_for(
                                self.queue.put(
                                    {
                                        "__CHECKPOINT__": {
                                            "ShardId": shard["ShardId"],
                                            "SequenceNumber": last_record["SequenceNumber"],
                                        }
                                    }
                                ),
                                timeout=30.0,
                            )
                        except asyncio.TimeoutError:
                            log.warning("Checkpoint queue put timed out")
                            # Continue without checkpoint - not critical

                        shard["LastSequenceNumber"] = last_record["SequenceNumber"]

                    else:
                        log.debug(
                            "Shard {} caught up, sleeping {}s".format(shard["ShardId"], self.sleep_time_no_records)
                        )
                        await asyncio.sleep(self.sleep_time_no_records)

                    if not result["NextShardIterator"]:
                        # Shard is closed - this is normal during resharding
                        shard_id = shard["ShardId"]
                        log.info(f"Shard {shard_id} is closed (NextShardIterator is null)")
                        self._closed_shards.add(shard_id)

                        # If this is a parent shard, mark it as exhausted to allow child consumption
                        if shard_id in self._parent_shards:
                            self._exhausted_parents.add(shard_id)
                            children = self._shard_topology.get(shard_id, {}).get("children", set())
                            if children:
                                log.info(f"Parent shard {shard_id} exhausted, enabling child shards: {children}")

                        # Deallocate the shard so other consumers can take over child shards
                        if self.checkpointer:
                            await self.checkpointer.deallocate(shard_id)

                        # Remove shard iterator to stop fetching from this shard
                        shard.pop("ShardIterator", None)
                        shard["fetch"] = None
                        continue

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

                result = await self.client.get_records(ShardIterator=shard["ShardIterator"], Limit=self.record_limit)

                shard["stats"].succeded()
                return result

            except ClientConnectionError:
                await self.get_conn()
            except TimeoutError as e:
                log.warning("Timeout {}. sleeping..".format(e))
                await asyncio.sleep(3)

            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code == "ProvisionedThroughputExceededException":
                    log.warning("{} hit ProvisionedThroughputExceededException".format(shard["ShardId"]))
                    shard["stats"].throttled()
                    # todo: control the throttle ?
                    await asyncio.sleep(0.25)

                elif code == "ExpiredIteratorException":
                    log.warning("{} hit ExpiredIteratorException, recreating iterator".format(shard["ShardId"]))

                    try:
                        # Try to get a new iterator from the last known sequence number
                        shard["ShardIterator"] = await self.get_shard_iterator(
                            shard_id=shard["ShardId"],
                            last_sequence_number=shard.get("LastSequenceNumber"),
                        )
                        log.debug(f"Successfully recreated iterator for shard {shard['ShardId']}")
                    except ClientError as iterator_error:
                        iterator_code = iterator_error.response["Error"]["Code"]
                        if iterator_code == "ResourceNotFoundException":
                            log.warning(f"Shard {shard['ShardId']} no longer exists, marking as closed")
                            self._closed_shards.add(shard["ShardId"])
                            if self.checkpointer:
                                await self.checkpointer.deallocate(shard["ShardId"])
                            shard.pop("ShardIterator", None)
                        else:
                            log.error(f"Failed to recreate iterator for shard {shard['ShardId']}: {iterator_code}")
                            # For other errors, remove the iterator to avoid infinite loops
                            shard.pop("ShardIterator", None)
                    except Exception as iterator_error:
                        log.error(
                            f"Unexpected error recreating iterator for shard {shard['ShardId']}: {iterator_error}"
                        )
                        shard.pop("ShardIterator", None)

                elif code == "InternalFailure":
                    log.warning("Received InternalFailure from Kinesis, rebuilding connection.. ")
                    await self.get_conn()

                else:
                    log.warning("ClientError {}. sleeping..".format(code))
                    await asyncio.sleep(3)

            except Exception as e:
                log.warning("Unknown error {}. sleeping..".format(e))
                await asyncio.sleep(3)

            # Connection or other issue
            return None

    async def refresh_shards(self):
        """
        Refresh the shard list to discover new shards and handle closed ones.
        This is important for handling Kinesis stream resharding events.
        """
        import time

        current_time = time.time()
        if current_time - self._last_shard_refresh < self._shard_refresh_interval:
            return  # Too soon to refresh

        # Skip shard refresh if skip_describe_stream is enabled
        if self.skip_describe_stream:
            log.debug("Skipping shard refresh due to skip_describe_stream setting")
            self._last_shard_refresh = current_time
            return

        try:
            log.debug("Refreshing shard list to check for new/closed shards")

            if self.use_list_shards:
                # Use ListShards API for better rate limits
                log.debug("Using ListShards API for shard refresh")
                try:
                    new_shards = await self.list_shards()
                    stream_status = self.ACTIVE  # Assume active if ListShards succeeds
                except Exception as e:
                    log.warning(f"ListShards failed ({e}), falling back to DescribeStream for refresh")
                    self.use_list_shards = False

            if not self.use_list_shards:
                # Use DescribeStream API
                stream_info = await self.get_stream_description()
                stream_status = stream_info.get("StreamStatus", "UNKNOWN")

                # Handle stream that might be updating due to resharding
                if stream_status == self.UPDATING:
                    log.info("Stream is currently UPDATING (possibly due to resharding)")
                    # Don't refresh shards during updating status to avoid inconsistent state
                    return
                elif stream_status != self.ACTIVE:
                    log.warning(f"Stream is in unexpected status: {stream_status}")
                    return

                new_shards = stream_info["Shards"]

            # Get current shard IDs
            current_shard_ids = {s["ShardId"] for s in self.shards} if self.shards else set()
            new_shard_ids = {s["ShardId"] for s in new_shards}

            # Build shard topology map for parent-child relationships
            self._build_shard_topology(new_shards)

            # Find newly discovered shards
            discovered_shards = new_shard_ids - current_shard_ids
            if discovered_shards:
                log.info(f"Discovered new shards: {discovered_shards}")
                # Check if this might be a resharding event
                new_child_shards = discovered_shards & self._child_shards
                if new_child_shards:
                    log.info(f"Resharding detected: new child shards {new_child_shards}")

            # Find shards that no longer exist (though this is rare)
            missing_shards = current_shard_ids - new_shard_ids
            if missing_shards:
                log.info(f"Shards no longer in stream description: {missing_shards}")

            # Update the shards list
            # Preserve existing shard state (iterators, stats, etc.) for shards that still exist
            preserved_shards = {}
            if self.shards:
                for shard in self.shards:
                    if shard["ShardId"] in new_shard_ids:
                        preserved_shards[shard["ShardId"]] = shard

            # Build new shards list, preserving state where possible
            updated_shards = []
            for new_shard in new_shards:
                shard_id = new_shard["ShardId"]
                if shard_id in preserved_shards:
                    # Keep existing shard with its state
                    existing_shard = preserved_shards[shard_id]
                    # Update shard metadata from AWS
                    for key in ["SequenceNumberRange", "ParentShardId", "HashKeyRange"]:
                        if key in new_shard:
                            existing_shard[key] = new_shard[key]
                    updated_shards.append(existing_shard)
                else:
                    # New shard discovered
                    updated_shards.append(new_shard.copy())

            self.shards = updated_shards
            self._last_shard_refresh = current_time

            log.debug(f"Shard refresh complete. Total shards: {len(self.shards)}")

        except Exception as e:
            log.warning(f"Failed to refresh shards: {e}")

    def is_resharding_likely_in_progress(self) -> bool:
        """
        Detect if resharding is likely in progress based on shard topology.
        This helps consumers make informed decisions during potential resharding.
        """
        # If we have parent-child relationships, resharding has occurred
        if self._parent_shards and self._child_shards:
            # Check if we have active parents with children (mid-resharding)
            active_parents_with_children = 0
            for parent_id in self._parent_shards:
                if parent_id not in self._closed_shards and parent_id not in self._exhausted_parents:
                    if parent_id in self._shard_topology and self._shard_topology[parent_id].get("children"):
                        active_parents_with_children += 1

            if active_parents_with_children > 0:
                return True

        # If we have many closed shards relative to active ones, might be post-resharding
        # But only flag as "in progress" if we don't have clear parent-child completion
        if len(self._closed_shards) > 0 and self.shards:
            closed_ratio = len(self._closed_shards) / len(self.shards)
            if closed_ratio > 0.3:  # More than 30% of shards are closed
                # Only consider it "in progress" if we don't have a clear completed resharding pattern
                # (i.e., all parents are exhausted and we have active children)
                if self._parent_shards and self._child_shards:
                    # If all parents are exhausted, resharding is complete, not in progress
                    all_parents_exhausted = all(p in self._exhausted_parents for p in self._parent_shards)
                    if all_parents_exhausted:
                        return False

                return True

        return False

    def _build_shard_topology(self, shards):
        """
        Build parent-child relationship topology from shard metadata.
        Follows AWS best practice: consume parent shards before child shards.
        """
        self._shard_topology.clear()
        self._parent_shards.clear()
        self._child_shards.clear()

        # First pass: identify all parent shards
        all_shard_ids = {s["ShardId"] for s in shards}
        parent_shard_ids = set()

        for shard in shards:
            shard_id = shard["ShardId"]
            parent_shard_id = shard.get("ParentShardId")

            if parent_shard_id:
                # This is a child shard
                self._child_shards.add(shard_id)
                parent_shard_ids.add(parent_shard_id)

                # Build parent -> children mapping
                if parent_shard_id not in self._shard_topology:
                    self._shard_topology[parent_shard_id] = {
                        "children": set(),
                        "parent": None,
                    }
                self._shard_topology[parent_shard_id]["children"].add(shard_id)

                # Build child -> parent mapping
                if shard_id not in self._shard_topology:
                    self._shard_topology[shard_id] = {"children": set(), "parent": None}
                self._shard_topology[shard_id]["parent"] = parent_shard_id

        # Parent shards are those that have children OR are referenced as parents
        # but might not be in the current shard list (if they're already closed)
        self._parent_shards = parent_shard_ids & all_shard_ids

        log.debug(f"Shard topology: {len(self._parent_shards)} parents, {len(self._child_shards)} children")
        if self._parent_shards:
            log.debug(f"Parent shards: {self._parent_shards}")
        if self._child_shards:
            log.debug(f"Child shards: {self._child_shards}")

    def _should_allocate_shard(self, shard_id):
        """
        Determine if a shard should be allocated based on parent-child ordering rules.
        AWS Best Practice: Only allocate child shards after their parents are exhausted.
        """
        # Always allow parent shards
        if shard_id in self._parent_shards:
            return True

        # For child shards, check if parent is exhausted
        if shard_id in self._child_shards:
            parent_id = self._shard_topology.get(shard_id, {}).get("parent")
            if parent_id:
                # Parent must be exhausted (closed) before we can consume child
                return parent_id in self._exhausted_parents or parent_id in self._closed_shards

        # If not in any topology (independent shard), always allow
        return True

    async def get_shard_iterator(self, shard_id, last_sequence_number=None):

        log.debug(
            "getting shard iterator for {} @ {}".format(
                shard_id,
                last_sequence_number if last_sequence_number else self.iterator_type,
            )
        )

        params = {
            "ShardId": shard_id,
            "ShardIteratorType": ("AFTER_SEQUENCE_NUMBER" if last_sequence_number else self.iterator_type),
        }
        params.update(self.address)

        if last_sequence_number:
            params["StartingSequenceNumber"] = last_sequence_number

        if self.iterator_type == "AT_TIMESTAMP" and self.timestamp:
            params["Timestamp"] = self.timestamp

        response = await self.client.get_shard_iterator(**params)
        return response["ShardIterator"]

    def get_shard_status(self):
        """
        Get current status of all shards being consumed.
        Returns information about active, closed, and allocated shards.
        """
        if not self.shards:
            return {
                "total_shards": 0,
                "active_shards": 0,
                "closed_shards": 0,
                "allocated_shards": 0,
                "shard_details": [],
            }

        # Generate comprehensive shard details list
        shard_details = [
            {
                "shard_id": shard["ShardId"],
                "is_allocated": self.checkpointer.is_allocated(shard["ShardId"]),
                "is_closed": shard["ShardId"] in self._closed_shards,
                "has_iterator": "ShardIterator" in shard and shard["ShardIterator"] is not None,
                "sequence_range": shard.get("SequenceNumberRange", {}),
                "parent_shard_id": shard.get("ParentShardId"),
                "is_parent": shard["ShardId"] in self._parent_shards,
                "is_child": shard["ShardId"] in self._child_shards,
                "can_allocate": self._should_allocate_shard(shard["ShardId"]),
                "stats": (shard.get("stats").to_data() if shard.get("stats") else None),
            }
            for shard in self.shards
        ]

        # Calculate counts from shard_details
        active_shards_count = len([s for s in shard_details if not s["is_closed"]])
        allocated_shards_count = len([s for s in shard_details if s["is_allocated"]])

        return {
            "total_shards": len(self.shards),
            "active_shards": active_shards_count,
            "closed_shards": len(self._closed_shards),
            "allocated_shards": allocated_shards_count,
            "parent_shards": len(self._parent_shards),
            "child_shards": len(self._child_shards),
            "exhausted_parents": len(self._exhausted_parents),
            "resharding_in_progress": self.is_resharding_likely_in_progress(),
            "topology": {
                "parent_child_map": {k: list(v["children"]) for k, v in self._shard_topology.items() if v["children"]},
                "child_parent_map": {k: v["parent"] for k, v in self._shard_topology.items() if v["parent"]},
            },
            "shard_details": shard_details,
        }

    async def start_consumer(self, wait_iterations=10, wait_sleep=0.25):

        # Start task to fetch periodically

        self.fetch_task = asyncio.create_task(self._fetch())

        # Wait a while until we have some results
        for i in range(0, wait_iterations):
            if self.fetch_task and self.queue.qsize() == 0:
                await asyncio.sleep(wait_sleep)

        log.debug("start_consumer completed.. queue size={}".format(self.queue.qsize()))

    async def __anext__(self):

        if not self.shards:
            await self.get_conn()

        if not self.fetch_task:
            await self.start_consumer()

        # Raise exception from Fetch Task to main task otherwise raise exception inside
        # Fetch Task will fail silently
        if self.fetch_task.done():
            exception = self.fetch_task.exception()
            if exception:
                raise exception

        checkpoint_count = 0
        max_checkpoints = 100  # Prevent infinite checkpoint processing

        while True:
            try:
                item = self.queue.get_nowait()

                if item and isinstance(item, dict) and "__CHECKPOINT__" in item:
                    if self.checkpointer:
                        await self.checkpointer.checkpoint(
                            item["__CHECKPOINT__"]["ShardId"],
                            item["__CHECKPOINT__"]["SequenceNumber"],
                        )
                    checkpoint_count += 1
                    if checkpoint_count >= max_checkpoints:
                        log.warning(f"Processed {max_checkpoints} checkpoints, stopping iteration")
                        raise StopAsyncIteration
                    continue

                return item

            except QueueEmpty:
                log.debug("Queue empty..")
                await asyncio.sleep(self.sleep_time_no_records)
                raise StopAsyncIteration
