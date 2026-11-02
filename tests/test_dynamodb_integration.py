"""Exercise DynamoDB lease and checkpoint writes against the integration backend."""

from contextlib import AsyncExitStack
from datetime import datetime, timezone
from uuid import uuid4

import pytest
import pytest_asyncio

from kinesis.dynamodb import DynamoDBCheckPointer

pytestmark = [pytest.mark.integration, pytest.mark.dynamodb]


@pytest_asyncio.fixture
async def dynamodb_lease(endpoint_url, monkeypatch):
    aioboto3 = pytest.importorskip("aioboto3", reason="requires the dynamodb extra")
    name = f"test-checkpoints-{uuid4().hex}"
    clock = {"now": int(datetime.now(timezone.utc).timestamp())}

    async with aioboto3.Session().resource(
        "dynamodb", endpoint_url=endpoint_url, region_name="ap-southeast-2"
    ) as dynamodb:
        async with AsyncExitStack() as cleanup:
            # Delete only this test's table, after both owners have stopped their heartbeats.
            table = await dynamodb.Table(name)

            async def delete_table():
                try:
                    await table.delete()
                except dynamodb.meta.client.exceptions.ResourceNotFoundException:
                    pass

            cleanup.push_async_callback(delete_table)
            owners = []
            for owner_id in ("first", "second"):
                owner = DynamoDBCheckPointer(
                    name,
                    table_name=name,
                    id=owner_id,
                    endpoint_url=endpoint_url,
                    region_name="ap-southeast-2",
                    session_timeout=60,
                    heartbeat_frequency=3600,
                )
                cleanup.push_async_callback(owner.close)
                monkeypatch.setattr(owner, "get_ts", lambda: clock["now"])
                owners.append(owner)
            yield owners[0], owners[1], table, clock


async def test_dynamodb_lease_lifecycle(dynamodb_lease):
    first, second, table, clock = dynamodb_lease
    shard_id = "shard-001"

    assert await first.allocate(shard_id) == (True, None)
    ttl = await table.meta.client.describe_time_to_live(TableName=table.name)
    assert ttl["TimeToLiveDescription"] == {"TimeToLiveStatus": "ENABLED", "AttributeName": "ttl"}

    await first.checkpoint(shard_id, "100")
    item = (await table.get_item(Key={"shard_id": shard_id}, ConsistentRead=True))["Item"]
    assert item["ref"] == first.get_ref()
    assert item["sequence"] == "100"
    assert item["ttl"] == clock["now"] + first.ttl_hours * 3600

    assert await second.allocate(shard_id) == (False, None)
    assert not second.is_allocated(shard_id)

    # Advance the lease clock, keeping real DynamoDB reads and conditional writes.
    clock["now"] += second.session_timeout
    assert await second.allocate(shard_id) == (True, "100")
    item = (await table.get_item(Key={"shard_id": shard_id}, ConsistentRead=True))["Item"]
    assert item["ref"] == second.get_ref()
    assert item["ts"] == clock["now"]

    await second.checkpoint(shard_id, "200")
    await second.deallocate(shard_id)
    assert not second.is_allocated(shard_id)
    item = (await table.get_item(Key={"shard_id": shard_id}, ConsistentRead=True))["Item"]
    assert item["ref"] is None
    assert item["ts"] is None
    assert item["sequence"] == "200"
    assert await first.allocate(shard_id) == (True, "200")


async def test_dynamodb_takeover_loses_to_renewed_lease(dynamodb_lease):
    first, second, table, clock = dynamodb_lease
    shard_id = "shard-001"
    assert await first.allocate(shard_id) == (True, None)
    await first.checkpoint(shard_id, "100")
    await second._initialize()
    clock["now"] += second.session_timeout

    async def renew_before_takeover(**kwargs):
        # Interleave a real renewal after the contender's read and before its conditional write.
        second._session.events.unregister("before-call.dynamodb.UpdateItem", renew_before_takeover)
        clock["now"] += 1
        await first.checkpoint(shard_id, "101")

    second._session.events.register("before-call.dynamodb.UpdateItem", renew_before_takeover)
    assert await second.allocate(shard_id) == (False, None)
    assert not second.is_allocated(shard_id)
    item = (await table.get_item(Key={"shard_id": shard_id}, ConsistentRead=True))["Item"]
    assert item["ref"] == first.get_ref()
    assert item["sequence"] == "101"
    assert item["ts"] == clock["now"]


@pytest.mark.parametrize("operation", ["checkpoint", "heartbeat"])
async def test_dynamodb_stale_owner_cannot_write_after_takeover(dynamodb_lease, operation):
    first, second, table, clock = dynamodb_lease
    shard_id = "shard-001"
    assert await first.allocate(shard_id) == (True, None)
    await first.checkpoint(shard_id, "100")
    clock["now"] += second.session_timeout
    assert await second.allocate(shard_id) == (True, "100")
    await second.checkpoint(shard_id, "200")
    expected = (await table.get_item(Key={"shard_id": shard_id}, ConsistentRead=True))["Item"]

    clock["now"] += 1
    if operation == "checkpoint":
        with pytest.raises(Exception, match="does not own it"):
            await first.checkpoint(shard_id, "stale")
    else:
        await first.do_heartbeat(shard_id, {"ref": first.get_ref(), "ts": clock["now"], "sequence": "stale"})

    item = (await table.get_item(Key={"shard_id": shard_id}, ConsistentRead=True))["Item"]
    assert item == expected, "a stale owner must not overwrite the new owner's lease or checkpoint"
