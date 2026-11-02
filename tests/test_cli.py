"""Tests for the async-kinesis CLI `list` command."""

import asyncio
import os
import uuid

import pytest
from click.testing import CliRunner

from kinesis import Producer
from kinesis.cli import main
from kinesis.cli import stream as cli_stream

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


class FakeKinesisClient:
    """Stands in for the aiobotocore client, recording each describe call."""

    def __init__(self, list_response, shard_counts=None, failing=(), shards=None):
        self.list_response = list_response
        self.shard_counts = shard_counts or {}
        self.failing = set(failing)
        self.shards = shards or {}
        self.summary_calls = []

    async def list_streams(self, Limit):
        return self.list_response

    async def describe_stream_summary(self, StreamName):
        self.summary_calls.append((StreamName, asyncio.get_running_loop().time()))
        if StreamName in self.failing:
            raise RuntimeError("LimitExceededException")
        return {"StreamDescriptionSummary": {"StreamName": StreamName, "OpenShardCount": self.shard_counts[StreamName]}}

    async def describe_stream(self, StreamName):
        if StreamName in self.failing:
            raise RuntimeError("boom")
        return {
            "StreamDescription": {
                "StreamName": StreamName,
                "StreamStatus": "ACTIVE",
                "Shards": [self._shard(i, closed) for i, closed in enumerate(self.shards[StreamName])],
            }
        }

    @staticmethod
    def _shard(index, closed):
        seq_range = {"StartingSequenceNumber": "1"}
        if closed:
            seq_range["EndingSequenceNumber"] = "2"
        return {"ShardId": f"shardId-{index}", "SequenceNumberRange": seq_range}


@pytest.fixture
def fake_client(monkeypatch):
    """Patch the CLI's client helper so `list` talks to a FakeKinesisClient."""
    holder = {}

    class FakeHelper:
        def __init__(self, *args, **kwargs):
            self.client = holder["client"]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(cli_stream, "_ClientHelper", FakeHelper)
    monkeypatch.setattr(cli_stream, "DESCRIBE_SUMMARY_INTERVAL", 0)

    def install(client):
        holder["client"] = client
        return client

    return install


def _summary(name, mode="PROVISIONED"):
    # The ListStreams StreamSummary shape: no shard count member.
    return {
        "StreamName": name,
        "StreamARN": f"arn:aws:kinesis:ap-southeast-2:123456789012:stream/{name}",
        "StreamStatus": "ACTIVE",
        "StreamModeDetails": {"StreamMode": mode},
    }


def _rows(output):
    """Split a format_table rendering into header + data rows of cells."""
    lines = output.strip().splitlines()
    return [line.split() for line in lines[:1] + lines[2:]]


def test_list_summaries_reads_open_shard_count_from_describe_stream_summary(fake_client):
    client = fake_client(
        FakeKinesisClient(
            {"StreamNames": ["a", "b"], "StreamSummaries": [_summary("a"), _summary("b", "ON_DEMAND")]},
            shard_counts={"a": 2, "b": 4},
        )
    )

    result = CliRunner().invoke(main, ["list"])

    assert result.exit_code == 0, result.output
    assert _rows(result.output) == [
        ["Name", "Status", "Shards", "Mode"],
        ["a", "ACTIVE", "2", "PROVISIONED"],
        ["b", "ACTIVE", "4", "ON_DEMAND"],
    ]
    assert [name for name, _ in client.summary_calls] == ["a", "b"]


def test_list_summaries_degrades_one_failed_lookup_to_question_mark(fake_client):
    fake_client(
        FakeKinesisClient(
            {"StreamNames": ["a", "b", "c"], "StreamSummaries": [_summary("a"), _summary("b"), _summary("c")]},
            shard_counts={"a": 1, "c": 3},
            failing={"b"},
        )
    )

    result = CliRunner().invoke(main, ["list"])

    assert result.exit_code == 0, result.output
    assert _rows(result.output)[1:] == [
        ["a", "ACTIVE", "1", "PROVISIONED"],
        ["b", "ACTIVE", "?", "PROVISIONED"],
        ["c", "ACTIVE", "3", "PROVISIONED"],
    ]


def test_list_summaries_empty(fake_client):
    client = fake_client(FakeKinesisClient({"StreamNames": [], "StreamSummaries": []}))

    result = CliRunner().invoke(main, ["list"])

    assert result.exit_code == 0, result.output
    assert result.output.strip() == "No streams found."
    assert client.summary_calls == []


def test_list_paces_describe_stream_summary_calls(fake_client, monkeypatch):
    interval = 0.05
    monkeypatch.setattr(cli_stream, "DESCRIBE_SUMMARY_INTERVAL", interval)
    names = ["a", "b", "c", "d"]
    client = fake_client(
        FakeKinesisClient(
            {"StreamNames": names, "StreamSummaries": [_summary(n) for n in names]},
            shard_counts={n: 1 for n in names},
        )
    )

    result = CliRunner().invoke(main, ["list"])

    assert result.exit_code == 0, result.output
    starts = sorted(t for _, t in client.summary_calls)
    gaps = [later - earlier for earlier, later in zip(starts, starts[1:])]
    assert len(gaps) == 3
    # Calls start one interval apart, not all at once.
    assert all(gap >= interval * 0.8 for gap in gaps), gaps


def test_list_names_only_fallback_uses_describe_stream(fake_client):
    client = fake_client(
        FakeKinesisClient(
            # Stream a was split: one closed parent, two open children.
            {"StreamNames": ["a", "b"]},
            shards={"a": [True, False, False]},
            failing={"b"},
        )
    )

    result = CliRunner().invoke(main, ["list"])

    assert result.exit_code == 0, result.output
    assert _rows(result.output) == [
        ["Name", "Status", "Shards"],
        ["a", "ACTIVE", "2"],
        ["b", "?", "?"],
    ]
    assert client.summary_calls == []


def test_list_names_only_fallback_empty(fake_client):
    fake_client(FakeKinesisClient({"StreamNames": []}))

    result = CliRunner().invoke(main, ["list"])

    assert result.exit_code == 0, result.output
    assert result.output.strip() == "No streams found."


@pytest.mark.integration
async def test_open_shard_counts_against_floci():
    stream_name = f"test_{str(uuid.uuid4())[0:8]}"
    missing = f"test_missing_{str(uuid.uuid4())[0:8]}"
    async with Producer(
        stream_name=stream_name,
        endpoint_url=ENDPOINT_URL,
        region_name="ap-southeast-2",
        create_stream=True,
        create_stream_shards=2,
    ):
        pass

    async with cli_stream._ClientHelper(endpoint_url=ENDPOINT_URL, region_name="ap-southeast-2") as helper:
        counts = await cli_stream._open_shard_counts(helper.client, [stream_name, missing])

    # A stream that does not exist raises ResourceNotFoundException, which degrades to "?".
    assert counts == {stream_name: "2", missing: "?"}
