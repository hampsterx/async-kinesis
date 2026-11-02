"""
A real MergeShards against the emulator, read back through Consumer.

A merged child names one parent in ParentShardId and the other in AdjacentParentShardId. Both parents
cover part of the child's hash range, so for per-key ordering the child must wait for both to drain,
not only for the one in ParentShardId.
"""

import pytest
import pytest_asyncio
from aiobotocore.session import get_session

from kinesis import Consumer
from tests.resharding.test_split_shard import REGION, make_consumer, put, read_labels, wait_for_active, wait_for_closed


async def write_labels(client, stream_name, shard, prefix, count):
    low = int(shard["HashKeyRange"]["StartingHashKey"])
    labels = []
    for i in range(count):
        shard_id, label = await put(client, stream_name, f"{prefix}-{i}", low)
        assert shard_id == shard["ShardId"]
        labels.append(label)
    return labels


async def merge(client, stream_name, parent, adjacent):
    await client.merge_shards(
        StreamName=stream_name,
        ShardToMerge=parent["ShardId"],
        AdjacentShardToMerge=adjacent["ShardId"],
    )
    await wait_for_active(client, stream_name)

    shards = (await client.list_shards(StreamName=stream_name))["Shards"]
    children = [s for s in shards if s.get("ParentShardId") == parent["ShardId"]]
    assert len(children) == 1, f"expected one child of {parent['ShardId']}, listed {shards}"
    child = children[0]
    assert (
        child.get("AdjacentParentShardId") == adjacent["ShardId"]
    ), f"child does not name the adjacent parent: {child}"
    return child


def assert_parents_released(status, parent, adjacent):
    assert status["exhausted_parents"] == 2
    assert status["resharding_in_progress"] is False
    details = {shard["shard_id"]: shard for shard in status["shard_details"]}
    assert not details[parent["ShardId"]]["is_allocated"]
    assert not details[adjacent["ShardId"]]["is_allocated"]


def test_merged_child_is_allocated_once_both_parents_are_exhausted_or_expired():
    consumer = Consumer(stream_name="merge-topology", endpoint_url="http://mock:4566", create_stream=False)
    merged = {"ShardId": "child", "ParentShardId": "parent", "AdjacentParentShardId": "adjacent"}

    consumer._build_shard_topology([{"ShardId": "parent"}, {"ShardId": "adjacent"}, merged])
    assert consumer._parent_shards == {"parent", "adjacent"}
    assert consumer._shard_topology["adjacent"]["children"] == {"child"}
    assert not consumer._should_allocate_shard("child")

    consumer._exhausted_parents.add("parent")
    assert not consumer._should_allocate_shard("child"), "the adjacent parent still holds records"

    consumer._closed_shards.add("adjacent")
    assert consumer._should_allocate_shard("child")

    # Once the adjacent parent ages out of the shard list, only ParentShardId is left to wait for.
    consumer._exhausted_parents.clear()
    consumer._closed_shards.clear()
    consumer._build_shard_topology([{"ShardId": "parent"}, merged])
    assert consumer._expired_parent_shards == {"adjacent"}
    assert not consumer._should_allocate_shard("child")
    consumer._closed_shards.add("parent")
    assert consumer._should_allocate_shard("child")


def test_merge_parent_that_is_itself_a_split_child_waits_for_its_own_parent():
    # grandparent splits into parent and adjacent, which then merge into child. Both middle shards are
    # parents of child and children of grandparent; being a parent must not let either skip grandparent.
    consumer = Consumer(stream_name="merge-topology", endpoint_url="http://mock:4566", create_stream=False)
    consumer._build_shard_topology(
        [
            {"ShardId": "grandparent"},
            {"ShardId": "parent", "ParentShardId": "grandparent"},
            {"ShardId": "adjacent", "ParentShardId": "grandparent"},
            {"ShardId": "child", "ParentShardId": "parent", "AdjacentParentShardId": "adjacent"},
        ]
    )
    assert consumer._should_allocate_shard("grandparent")
    assert not consumer._should_allocate_shard("parent")
    assert not consumer._should_allocate_shard("adjacent")
    assert not consumer._should_allocate_shard("child")

    consumer._closed_shards.add("grandparent")
    assert consumer._should_allocate_shard("parent")
    assert consumer._should_allocate_shard("adjacent")
    assert not consumer._should_allocate_shard("child")


def test_split_child_that_is_split_again_waits_for_its_own_parent():
    consumer = Consumer(stream_name="split-chain", endpoint_url="http://mock:4566", create_stream=False)
    consumer._build_shard_topology(
        [
            {"ShardId": "original"},
            {"ShardId": "middle", "ParentShardId": "original"},
            {"ShardId": "sibling", "ParentShardId": "original"},
            {"ShardId": "low", "ParentShardId": "middle"},
            {"ShardId": "high", "ParentShardId": "middle"},
        ]
    )
    assert not consumer._should_allocate_shard("middle"), "a split child must not be read before its parent drains"
    consumer._closed_shards.add("original")
    assert consumer._should_allocate_shard("middle")
    assert not consumer._should_allocate_shard("low")


@pytest_asyncio.fixture
async def two_shard_stream(random_stream_name, endpoint_url):
    async with get_session().create_client("kinesis", region_name=REGION, endpoint_url=endpoint_url) as client:
        await client.create_stream(StreamName=random_stream_name, ShardCount=2)
        await wait_for_active(client, random_stream_name)
        shards = (await client.list_shards(StreamName=random_stream_name))["Shards"]
        assert len(shards) == 2
        yield client, random_stream_name, shards[0], shards[1]


@pytest.mark.integration
async def test_child_of_merge_waits_for_the_adjacent_parent(two_shard_stream, endpoint_url):
    client, stream_name, parent, adjacent = two_shard_stream
    # ParentShardId drains in one fetch, the adjacent parent takes several, so a child that waits only on
    # ParentShardId is read while the adjacent parent still holds records for part of its hash range.
    parent_labels = await write_labels(client, stream_name, parent, "parent", 1)
    adjacent_labels = await write_labels(client, stream_name, adjacent, "adjacent", 6)
    child = await merge(client, stream_name, parent, adjacent)
    child_labels = await write_labels(client, stream_name, child, "child", 3)

    async with make_consumer(stream_name, endpoint_url) as consumer:
        labels = await read_labels(consumer, len(parent_labels) + len(adjacent_labels) + len(child_labels))
        status = consumer.get_shard_status()

    assert sorted(labels) == sorted(parent_labels + adjacent_labels + child_labels)
    parents_read = len(parent_labels) + len(adjacent_labels)
    assert sorted(labels[:parents_read]) == sorted(
        parent_labels + adjacent_labels
    ), "both parents' records must be read before any of the child's"
    assert [label for label in labels if label.startswith("adjacent-")] == adjacent_labels
    assert labels[parents_read:] == child_labels
    assert_parents_released(status, parent, adjacent)
    details = {shard["shard_id"]: shard for shard in status["shard_details"]}
    assert details[child["ShardId"]]["parent_shard_id"] == parent["ShardId"]
    assert details[child["ShardId"]]["adjacent_parent_shard_id"] == adjacent["ShardId"]


@pytest.mark.integration
async def test_consumer_running_through_merge_counts_both_parents_exhausted(two_shard_stream, endpoint_url):
    client, stream_name, parent, adjacent = two_shard_stream
    parent_labels = await write_labels(client, stream_name, parent, "parent", 2)
    adjacent_labels = await write_labels(client, stream_name, adjacent, "adjacent", 2)

    async with make_consumer(stream_name, endpoint_url) as consumer:
        labels = await read_labels(consumer, len(parent_labels) + len(adjacent_labels))
        assert sorted(labels) == sorted(parent_labels + adjacent_labels)

        # Both parents drain and close before the next 60s refresh lists either as a parent, so each must
        # be counted exhausted when that refresh comes, the adjacent parent included.
        child = await merge(client, stream_name, parent, adjacent)
        child_labels = await write_labels(client, stream_name, child, "child", 3)
        await wait_for_closed(consumer, parent["ShardId"])
        await wait_for_closed(consumer, adjacent["ShardId"])
        consumer._shard_refresh_interval = 0.5
        read_from_child = await read_labels(consumer, len(child_labels))
        status = consumer.get_shard_status()

    assert read_from_child == child_labels
    assert_parents_released(status, parent, adjacent)
