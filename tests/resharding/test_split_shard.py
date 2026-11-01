"""
A real SplitShard against the emulator, read back through Consumer.

The rest of this directory simulates topology or targets real AWS. These tests call the API against
the emulator, so they depend on it ending a closed shard the way Kinesis does: GetRecords returns a
null NextShardIterator once the parent is drained, which is the signal Consumer advances from parent
to child on.
"""

import asyncio
import time
import uuid

import pytest
import pytest_asyncio
from aiobotocore.session import get_session

from kinesis import Consumer, MemoryCheckPointer
from kinesis.processors import JsonProcessor

REGION = "ap-southeast-2"
READ_TIMEOUT = 30


async def wait_for_active(client, stream_name):
    deadline = time.monotonic() + READ_TIMEOUT
    while time.monotonic() < deadline:
        summary = await client.describe_stream_summary(StreamName=stream_name)
        if summary["StreamDescriptionSummary"]["StreamStatus"] == "ACTIVE":
            return
        await asyncio.sleep(0.2)
    pytest.fail(f"{stream_name} did not become ACTIVE within {READ_TIMEOUT}s")


async def put(client, stream_name, label, hash_key):
    """Write one record, returning the shard it landed in and the label Consumer should read back."""
    response = await client.put_record(
        StreamName=stream_name,
        PartitionKey=label,
        ExplicitHashKey=str(hash_key),
        Data=('{"label": "%s"}' % label).encode(),
    )
    return response["ShardId"], label


async def split_and_write_to_children(client, stream_name, parent):
    """Split parent at the midpoint and write to each child, returning the labels written."""
    low = int(parent["HashKeyRange"]["StartingHashKey"])
    high = int(parent["HashKeyRange"]["EndingHashKey"])
    middle = (low + high) // 2 + 1

    await client.split_shard(
        StreamName=stream_name,
        ShardToSplit=parent["ShardId"],
        NewStartingHashKey=str(middle),
    )
    await wait_for_active(client, stream_name)

    shards = (await client.list_shards(StreamName=stream_name))["Shards"]
    children = {s["ShardId"] for s in shards if s.get("ParentShardId") == parent["ShardId"]}
    assert len(children) == 2, f"expected two children of {parent['ShardId']}, listed {shards}"

    written = {}
    for i in range(3):
        for name, hash_key in (("low", low), ("high", high)):
            shard_id, label = await put(client, stream_name, f"child-{name}-{i}", hash_key)
            written.setdefault(shard_id, []).append(label)
    assert set(written) == children, f"expected records in both of {children}, wrote {written}"
    return [label for labels in written.values() for label in labels]


async def read_labels(consumer, expected_count):
    """Read until expected_count arrive. Consumer stops iterating when idle, so iteration is re-entered."""
    labels = []
    deadline = time.monotonic() + READ_TIMEOUT
    while len(labels) < expected_count and time.monotonic() < deadline:
        async for item in consumer:
            labels.append(item["label"])
            if len(labels) >= expected_count:
                break
    return labels


def make_consumer(stream_name, endpoint_url, **kwargs):
    # One record per fetch keeps the parent open across several fetch rounds, so a child read too
    # early lands among the parent's records instead of after them by luck of task scheduling.
    return Consumer(
        stream_name,
        endpoint_url=endpoint_url,
        region_name=REGION,
        iterator_type="TRIM_HORIZON",
        processor=JsonProcessor(),
        checkpointer=MemoryCheckPointer(name=f"split-{uuid.uuid4().hex[:8]}"),
        record_limit=1,
        shard_fetch_rate=5,
        sleep_time_no_records=0.1,
        idle_timeout=1.0,
        **kwargs,
    )


def assert_read_parent_then_children(labels, parent_labels, child_labels):
    assert sorted(labels) == sorted(parent_labels + child_labels)
    assert labels[: len(parent_labels)] == parent_labels, "parent records must be read, in order, before any child's"


def assert_parent_released(status, parent):
    """
    Check the parent is counted as exhausted and no longer allocated.

    The running-through test leaves this out: exhausted_parents only counts a parent already in the
    topology when it closes, and that consumer usually sees the close before its next shard refresh.
    """
    assert status["exhausted_parents"] == 1
    details = {shard["shard_id"]: shard for shard in status["shard_details"]}
    assert not details[parent["ShardId"]]["is_allocated"]


@pytest_asyncio.fixture
async def one_shard_stream(random_stream_name, endpoint_url):
    async with get_session().create_client("kinesis", region_name=REGION, endpoint_url=endpoint_url) as client:
        await client.create_stream(StreamName=random_stream_name, ShardCount=1)
        await wait_for_active(client, random_stream_name)
        parent = (await client.list_shards(StreamName=random_stream_name))["Shards"][0]
        yield client, random_stream_name, parent


async def write_to_parent(client, stream_name, parent, count=3):
    low = int(parent["HashKeyRange"]["StartingHashKey"])
    labels = []
    for i in range(count):
        shard_id, label = await put(client, stream_name, f"parent-{i}", low)
        assert shard_id == parent["ShardId"]
        labels.append(label)
    return labels


@pytest.mark.integration
async def test_consumer_started_after_split_reads_parent_then_both_children(one_shard_stream, endpoint_url):
    client, stream_name, parent = one_shard_stream
    parent_labels = await write_to_parent(client, stream_name, parent)
    child_labels = await split_and_write_to_children(client, stream_name, parent)

    async with make_consumer(stream_name, endpoint_url) as consumer:
        labels = await read_labels(consumer, len(parent_labels) + len(child_labels))
        status = consumer.get_shard_status()

    assert_read_parent_then_children(labels, parent_labels, child_labels)
    assert_parent_released(status, parent)


@pytest.mark.integration
async def test_consumer_running_through_split_reads_parent_then_both_children(one_shard_stream, endpoint_url):
    client, stream_name, parent = one_shard_stream
    parent_labels = await write_to_parent(client, stream_name, parent)

    async with make_consumer(stream_name, endpoint_url) as consumer:
        # Consumer rediscovers shards every 60s, too slow to wait for here.
        consumer._shard_refresh_interval = 0.5
        labels = await read_labels(consumer, len(parent_labels))
        assert labels == parent_labels

        child_labels = await split_and_write_to_children(client, stream_name, parent)
        read_from_children = await read_labels(consumer, len(child_labels))

    assert sorted(read_from_children) == sorted(child_labels)


@pytest.mark.integration
async def test_split_landing_while_consumer_is_behind_on_parent_reads_parent_first(one_shard_stream, endpoint_url):
    client, stream_name, parent = one_shard_stream
    # A one-slot queue blocks the fetch loop until this test reads, so the parent cannot drain during the
    # split and the child writes (up to Consumer's 30s put timeout), and rediscovery sees it still live.
    parent_labels = await write_to_parent(client, stream_name, parent, count=10)

    async with make_consumer(stream_name, endpoint_url, max_queue_size=1) as consumer:
        consumer._shard_refresh_interval = 0.5
        labels = await read_labels(consumer, 1)

        child_labels = await split_and_write_to_children(client, stream_name, parent)
        labels += await read_labels(consumer, len(parent_labels) + len(child_labels) - 1)
        status = consumer.get_shard_status()

    assert_read_parent_then_children(labels, parent_labels, child_labels)
    assert_parent_released(status, parent)
