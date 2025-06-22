"""DynamoDB checkpointer implementation for async-kinesis.

This module provides a DynamoDB-based checkpointer for managing shard allocation
and checkpointing in distributed Kinesis consumer applications.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, Union

from .checkpointers import BaseHeartbeatCheckPointer

log = logging.getLogger(__name__)

try:
    import aioboto3
    from botocore.exceptions import ClientError

    HAS_DYNAMODB = True
except ImportError:
    HAS_DYNAMODB = False
    aioboto3 = None
    ClientError = None


class DynamoDBCheckPointer(BaseHeartbeatCheckPointer):
    """DynamoDB-based checkpointer for distributed Kinesis consumers.

    This checkpointer uses AWS DynamoDB to manage shard allocation and
    checkpointing across multiple consumer instances. It provides:

    - Automatic table creation
    - TTL-based cleanup of stale records
    - Heartbeat mechanism for lease management
    - Atomic operations for safe concurrent access

    Args:
        name: Application name used for table naming
        table_name: Override default table name (optional)
        id: Consumer instance ID (defaults to PID)
        session_timeout: Timeout in seconds before a shard is considered abandoned
        heartbeat_frequency: Frequency in seconds to update heartbeat
        auto_checkpoint: Whether to checkpoint automatically
        create_table: Whether to create table if it doesn't exist
        endpoint_url: Custom DynamoDB endpoint (for testing)
        region_name: AWS region name
        ttl_hours: Hours to retain old checkpoint records
    """

    def __init__(
        self,
        name: str,
        table_name: Optional[str] = None,
        id: Optional[Union[str, int]] = None,
        session_timeout: int = 60,
        heartbeat_frequency: int = 15,
        auto_checkpoint: bool = True,
        create_table: bool = True,
        endpoint_url: Optional[str] = None,
        region_name: Optional[str] = None,
        ttl_hours: int = 24,
    ):
        if not HAS_DYNAMODB:
            raise ImportError(
                "DynamoDB support requires aioboto3 to be installed. "
                "Install with: pip install async-kinesis[dynamodb]"
            )

        super().__init__(
            name=name,
            id=id,
            session_timeout=session_timeout,
            heartbeat_frequency=heartbeat_frequency,
            auto_checkpoint=auto_checkpoint,
        )

        self.table_name = table_name or f"kinesis-checkpoints-{name}"
        self.create_table = create_table
        self.endpoint_url = endpoint_url
        self.region_name = region_name or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        self.ttl_hours = ttl_hours

        self._table = None
        self._session = None
        self._initialized = False
        self._init_lock = asyncio.Lock()

    async def _initialize(self):
        """Initialize DynamoDB connection and ensure table exists."""
        async with self._init_lock:
            if self._initialized:
                return

            self._session = aioboto3.Session()

            # Create DynamoDB resource
            async with self._session.resource(
                "dynamodb",
                endpoint_url=self.endpoint_url,
                region_name=self.region_name,
            ) as dynamodb:

                # Try to get existing table
                try:
                    self._table = await dynamodb.Table(self.table_name)
                    await self._table.load()
                    log.info(f"Using existing DynamoDB table: {self.table_name}")
                except ClientError as e:
                    if e.response["Error"]["Code"] == "ResourceNotFoundException":
                        if self.create_table:
                            await self._create_table(dynamodb)
                        else:
                            raise Exception(f"DynamoDB table {self.table_name} does not exist") from e
                    else:
                        raise

            self._initialized = True

    async def _create_table(self, dynamodb):
        """Create the DynamoDB table if it doesn't exist."""
        log.info(f"Creating DynamoDB table: {self.table_name}")

        table = await dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[
                {"AttributeName": "shard_id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "shard_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",  # On-demand pricing
        )

        # Wait for table to be active
        await table.wait_until_exists()

        # Enable TTL on the ttl field
        try:
            async with self._session.client("dynamodb", region_name=self.region_name) as client:
                await client.update_time_to_live(
                    TableName=self.table_name,
                    TimeToLiveSpecification={
                        "Enabled": True,
                        "AttributeName": "ttl",
                    },
                )
        except ClientError as e:
            # TTL might already be enabled
            log.warning(f"Could not enable TTL: {e}")

        self._table = table
        log.info(f"Created DynamoDB table: {self.table_name}")

    def get_key(self, shard_id: str) -> str:
        """Get the DynamoDB key for a shard."""
        return shard_id

    def get_ts(self) -> int:
        """Get current timestamp in seconds."""
        return int(datetime.now(tz=timezone.utc).timestamp())

    def get_ttl(self) -> int:
        """Get TTL timestamp for record expiration."""
        return self.get_ts() + (self.ttl_hours * 3600)

    async def do_heartbeat(self, key: str, value: Dict[str, Any]) -> None:
        """Update heartbeat for a shard."""
        if not self._initialized:
            await self._initialize()

        value["ttl"] = self.get_ttl()

        async with self._session.resource("dynamodb", region_name=self.region_name) as dynamodb:
            table = await dynamodb.Table(self.table_name)

            try:
                # Update only if we still own the shard
                await table.update_item(
                    Key={"shard_id": key},
                    UpdateExpression="SET #ref = :ref, #ts = :ts, #seq = :seq, #ttl = :ttl",
                    ExpressionAttributeNames={
                        "#ref": "ref",
                        "#ts": "ts",
                        "#seq": "sequence",
                        "#ttl": "ttl",
                    },
                    ExpressionAttributeValues={
                        ":ref": value["ref"],
                        ":ts": value["ts"],
                        ":seq": value["sequence"],
                        ":ttl": value["ttl"],
                        ":expected_ref": self.get_ref(),
                    },
                    ConditionExpression="#ref = :expected_ref",
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    log.warning(f"Lost ownership of shard {key} during heartbeat")
                else:
                    raise

    async def checkpoint(self, shard_id: str, sequence: str) -> None:
        """Checkpoint progress for a shard."""
        if not self.auto_checkpoint:
            log.debug(f"{self.get_ref()} updated manual checkpoint {shard_id}@{sequence}")
            self._manual_checkpoints[shard_id] = sequence
            return

        await self._checkpoint(shard_id, sequence)

    async def manual_checkpoint(self) -> None:
        """Flush all manual checkpoints to DynamoDB."""
        items = [(k, v) for k, v in self._manual_checkpoints.items()]

        self._manual_checkpoints = {}

        for shard_id, sequence in items:
            await self._checkpoint(shard_id, sequence)

    async def _checkpoint(self, shard_id: str, sequence: str) -> None:
        """Internal checkpoint implementation."""
        if not self._initialized:
            await self._initialize()

        key = self.get_key(shard_id)

        async with self._session.resource("dynamodb", region_name=self.region_name) as dynamodb:
            table = await dynamodb.Table(self.table_name)

            try:
                # Update sequence number only if we own the shard
                await table.update_item(
                    Key={"shard_id": key},
                    UpdateExpression="SET #seq = :seq, #ts = :ts, #ttl = :ttl",
                    ExpressionAttributeNames={
                        "#seq": "sequence",
                        "#ts": "ts",
                        "#ttl": "ttl",
                        "#ref": "ref",
                    },
                    ExpressionAttributeValues={
                        ":seq": sequence,
                        ":ts": self.get_ts(),
                        ":ttl": self.get_ttl(),
                        ":expected_ref": self.get_ref(),
                    },
                    ConditionExpression="#ref = :expected_ref",
                    ReturnValues="ALL_NEW",
                )

                log.debug(f"{self.get_ref()} checkpointed on {shard_id}@{sequence}")
                self._items[shard_id] = sequence

            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    raise Exception(f"{self.get_ref()} tried to checkpoint {shard_id} but does not own it") from e
                else:
                    raise

    async def deallocate(self, shard_id: str) -> None:
        """Deallocate a shard."""
        if not self._initialized:
            await self._initialize()

        key = self.get_key(shard_id)
        sequence = self._items.get(shard_id)

        async with self._session.resource("dynamodb", region_name=self.region_name) as dynamodb:
            table = await dynamodb.Table(self.table_name)

            # Clear ownership but keep sequence number
            await table.update_item(
                Key={"shard_id": key},
                UpdateExpression="SET #ref = :null, #ts = :null, #seq = :seq, #ttl = :ttl",
                ExpressionAttributeNames={
                    "#ref": "ref",
                    "#ts": "ts",
                    "#seq": "sequence",
                    "#ttl": "ttl",
                },
                ExpressionAttributeValues={
                    ":null": None,
                    ":seq": sequence,
                    ":ttl": self.get_ttl(),
                },
            )

        log.info(f"{self.get_ref()} deallocated on {shard_id}@{sequence}")
        self._items.pop(shard_id, None)

    async def allocate(self, shard_id: str) -> Tuple[bool, Optional[str]]:
        """Try to allocate a shard for processing."""
        if not self._initialized:
            await self._initialize()

        key = self.get_key(shard_id)
        max_retries = 3

        for retry in range(max_retries):
            ts = self.get_ts()

            async with self._session.resource("dynamodb", region_name=self.region_name) as dynamodb:
                table = await dynamodb.Table(self.table_name)

                # First, try to create a new record
                try:
                    await table.put_item(
                        Item={
                            "shard_id": key,
                            "ref": self.get_ref(),
                            "ts": ts,
                            "sequence": None,
                            "ttl": self.get_ttl(),
                        },
                        ConditionExpression="attribute_not_exists(shard_id)",
                    )

                    log.info(f"{self.get_ref()} allocated {shard_id} (new checkpoint)")
                    self._items[shard_id] = None
                    return True, None

                except ClientError as e:
                    if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                        raise

                # Record exists, check if we can take it
                response = await table.get_item(Key={"shard_id": key})

                if "Item" not in response:
                    # Race condition - someone deleted it, retry
                    if retry < max_retries - 1:
                        log.debug(f"Race condition detected for {shard_id}, retrying...")
                        await asyncio.sleep(0.1)  # Small delay before retry
                        continue
                    else:
                        log.warning(f"Failed to allocate {shard_id} after {max_retries} retries due to race conditions")
                        return False, None

                item = response["Item"]

                if item.get("ts") and item.get("ref"):
                    # Check if current owner is still alive
                    age = ts - item["ts"]

                    if age < self.session_timeout:
                        log.info(f"{self.get_ref()} could not allocate {shard_id}, still in use by {item['ref']}")
                        await asyncio.sleep(1)  # Avoid spamming
                        return False, None

                    log.info(
                        f"Attempting to take lock as {item['ref']} is {age - self.session_timeout} seconds overdue"
                    )

                # Try to take ownership
                try:
                    await table.update_item(
                        Key={"shard_id": key},
                        UpdateExpression="SET #ref = :ref, #ts = :ts, #ttl = :ttl",
                        ExpressionAttributeNames={
                            "#ref": "ref",
                            "#ts": "ts",
                            "#ttl": "ttl",
                        },
                        ExpressionAttributeValues={
                            ":ref": self.get_ref(),
                            ":ts": ts,
                            ":ttl": self.get_ttl(),
                            ":old_ts": item.get("ts"),
                        },
                        ConditionExpression="#ts = :old_ts OR attribute_not_exists(#ts)",
                        ReturnValues="ALL_NEW",
                    )

                    sequence = item.get("sequence")
                    log.info(f"{self.get_ref()} allocated {shard_id}@{sequence}")
                    self._items[shard_id] = sequence
                    return True, sequence

                except ClientError as e:
                    if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                        log.info(f"Someone else beat us to allocating {shard_id}")
                        return False, None
                    else:
                        raise

        # Should not reach here, but just in case
        log.warning(f"Failed to allocate {shard_id} after {max_retries} retries")
        return False, None

    async def close(self) -> None:
        """Clean up resources."""
        await super().close()

        if self._session:
            await self._session.close()
            self._session = None
