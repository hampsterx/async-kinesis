import argparse
import asyncio
import json
import logging

import pytest

from scripts import kinesis_dogfood


def test_parse_reshard_plan():
    assert kinesis_dogfood.parse_reshard_plan(None) == []
    assert kinesis_dogfood.parse_reshard_plan("3, 1") == [3, 1]


def test_expand_reshard_plan_uses_legal_intermediate_counts():
    assert kinesis_dogfood.expand_reshard_plan(1, [3, 1]) == [2, 3, 2, 1]
    assert kinesis_dogfood.expand_reshard_plan(2, [8, 1]) == [4, 8, 4, 2, 1]


def test_validate_floci_defaults_endpoint_url(monkeypatch):
    monkeypatch.delenv("ENDPOINT_URL", raising=False)
    args = kinesis_dogfood.build_parser().parse_args(["--backend", "floci"])

    kinesis_dogfood.validate_args(args)

    assert args.endpoint_url == kinesis_dogfood.DEFAULT_FLOCI_ENDPOINT
    assert args.checkpointer == "memory"


def test_validate_aws_defaults_dynamodb_checkpointer(monkeypatch):
    monkeypatch.delenv("ENDPOINT_URL", raising=False)
    args = kinesis_dogfood.build_parser().parse_args(["--backend", "aws"])

    kinesis_dogfood.validate_args(args)

    assert args.checkpointer == "dynamodb"


@pytest.mark.parametrize(
    "extra_args, message_fragment",
    [
        (["--checkpoint-interval", "0"], "checkpoint-interval"),
        (["--checkpoint-interval", "-1"], "checkpoint-interval"),
        (["--orphan-min-age-minutes", "-1"], "orphan-min-age-minutes"),
        (["--reshard-plan", "0,2"], "reshard-plan"),
        (["--reshard-plan", "-1"], "reshard-plan"),
        (["--reshard-timeout-seconds", "0"], "reshard-timeout-seconds"),
    ],
)
def test_validate_args_rejects_bad_values(monkeypatch, extra_args, message_fragment):
    monkeypatch.delenv("ENDPOINT_URL", raising=False)
    args = kinesis_dogfood.build_parser().parse_args(["--backend", "aws", *extra_args])

    with pytest.raises(SystemExit) as exc_info:
        kinesis_dogfood.validate_args(args)

    assert message_fragment in str(exc_info.value)


@pytest.mark.parametrize(
    "extra_args, expected_code",
    [
        (["--checkpointer", "redis"], "unsupported_checkpointer"),
        (["--scenario", "issue-73-reconnect"], "unsupported_scenario"),
        (["--backend", "floci", "--reshard-plan", "2,1"], "unsupported_reshard"),
    ],
)
def test_preflight_rejects_unsupported_modes(monkeypatch, extra_args, expected_code):
    monkeypatch.delenv("ENDPOINT_URL", raising=False)
    args = kinesis_dogfood.build_parser().parse_args(extra_args)
    kinesis_dogfood.validate_args(args)
    report = kinesis_dogfood.build_report(args, "run-id", "stream-name")

    rejected = kinesis_dogfood.preflight_unsupported(args, report)

    assert rejected is True
    assert report["findings"][-1]["code"] == expected_code
    assert report["findings"][-1]["severity"] == "high"


def test_log_capture_records_child_logger_once_via_propagation():
    logger = logging.getLogger("kinesis.consumer")

    with kinesis_dogfood.LogCapture() as handler:
        logger.warning("captured once")

    matches = [record for record in handler.records if record["message"] == "captured once"]
    assert len(matches) == 1
    assert matches[0]["logger"] == "kinesis.consumer"


def test_log_errors_create_blocking_finding():
    args = kinesis_dogfood.build_parser().parse_args(["--backend", "aws"])
    report = kinesis_dogfood.build_report(args, "run-id", "stream-name")
    report["log_findings"] = [
        {"logger": "kinesis.consumer", "level": "ERROR", "message": "'NextShardIterator'"},
        {"logger": "kinesis.consumer", "level": "ERROR", "message": "'NextShardIterator'"},
    ]

    kinesis_dogfood.add_log_error_findings(report)

    assert kinesis_dogfood.has_blocking_findings(report)
    assert report["findings"][0]["code"] == "captured_error_logs"
    assert report["findings"][0]["details"]["logs"] == [
        {
            "logger": "kinesis.consumer",
            "level": "ERROR",
            "message": "'NextShardIterator'",
            "count": 2,
        }
    ]


@pytest.mark.asyncio
async def test_wait_consumer_ready_or_idle_allows_zero_shard_consumer():
    class IdleConsumer:
        async def wait_ready(self, timeout):
            raise asyncio.TimeoutError(f"Consumer did not become ready within {timeout}s")

        def get_shard_status(self):
            return {"total_shards": 1, "allocated_shards": 0}

    state, elapsed = await kinesis_dogfood.wait_consumer_ready_or_idle(IdleConsumer(), timeout=30)

    assert state == "idle"
    assert elapsed >= 0


@pytest.mark.asyncio
async def test_wait_consumer_ready_or_idle_fails_allocated_consumer():
    class StuckConsumer:
        async def wait_ready(self, timeout):
            raise asyncio.TimeoutError(f"Consumer did not become ready within {timeout}s")

        def get_shard_status(self):
            return {"total_shards": 1, "allocated_shards": 1}

    with pytest.raises(asyncio.TimeoutError):
        await kinesis_dogfood.wait_consumer_ready_or_idle(StuckConsumer(), timeout=30)


def test_write_reports_outputs_json_and_markdown(tmp_path):
    args = argparse.Namespace(
        backend="floci",
        endpoint_url="http://localhost:4566",
        region="ap-southeast-2",
        stream_name=None,
        duration_minutes=1,
        initial_shards=1,
        reshard_plan=None,
        reshard_timeout_seconds=300.0,
        producers=1,
        consumers=1,
        records_per_second=10,
        record_size_bytes=128,
        partition_mode="mixed",
        checkpointer="memory",
        manual_checkpoint=False,
        checkpoint_interval=None,
        throttle_info_rate=5,
        scenario=None,
        seed=None,
        keep_stream=False,
        keep_checkpoint_table=False,
        cleanup_orphans=False,
        orphan_min_age_minutes=60,
        dry_run=False,
    )
    run_id = "20260502-000000-test"
    report = kinesis_dogfood.build_report(args, run_id, "async-kinesis-dogfood-test")
    report["finished_at"] = report["started_at"]
    report["success"] = True

    json_path, md_path = kinesis_dogfood.write_reports(report, tmp_path)

    data = json.loads(json_path.read_text())
    assert data["run_id"] == run_id
    assert data["success"] is True
    assert "# async-kinesis Dogfood Report" in md_path.read_text()


def test_dynamodb_checkpoint_table_name_uses_run_id(capsys):
    args = kinesis_dogfood.build_parser().parse_args(["--backend", "aws", "--checkpointer", "dynamodb"])
    run_id = "20260504-000000-stable"
    stream_name = kinesis_dogfood.make_stream_name(args, run_id)

    report = kinesis_dogfood.build_report(args, run_id, stream_name)
    table_name = report["resources"]["checkpoint_table"]["name"]
    kinesis_dogfood.print_confirmation_banner(args, stream_name, table_name)

    assert table_name == f"{kinesis_dogfood.STREAM_PREFIX}-{run_id}"
    assert f"checkpoint_table: {table_name}" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_run_writes_cleanup_state_after_cancelled_workload(monkeypatch):
    args = kinesis_dogfood.build_parser().parse_args(["--backend", "floci", "--yes"])
    kinesis_dogfood.validate_args(args)

    async def fake_create_stream(_args, _stream_name, report):
        report["resources"]["stream"]["created"] = True

    async def fake_run_workload(_args, _run_id, _stream_name, _report, _stop_event):
        raise asyncio.CancelledError

    async def fake_delete_stream(_args, _stream_name, report):
        report["resources"]["stream"]["deleted"] = True

    monkeypatch.setattr(kinesis_dogfood, "build_run_id", lambda: "20260502-000000-cancel")
    monkeypatch.setattr(kinesis_dogfood, "install_signal_handlers", lambda _stop_event, _report: None)
    monkeypatch.setattr(kinesis_dogfood, "create_stream", fake_create_stream)
    monkeypatch.setattr(kinesis_dogfood, "run_workload", fake_run_workload)
    monkeypatch.setattr(kinesis_dogfood, "delete_stream", fake_delete_stream)

    report = await kinesis_dogfood.run(args)

    assert report["success"] is False
    assert report["interrupted"] is True
    assert report["resources"]["stream"]["deleted"] is True
    assert [finding["code"] for finding in report["findings"]] == ["interrupted"]


@pytest.mark.asyncio
async def test_dry_run_does_not_create_clients(monkeypatch):
    args = kinesis_dogfood.build_parser().parse_args(
        [
            "--backend",
            "aws",
            "--checkpointer",
            "dynamodb",
            "--reshard-plan",
            "2,1",
            "--dry-run",
            "--yes",
        ]
    )

    def fail_client(_service, _args):
        raise AssertionError("dry-run should not create boto3 clients")

    monkeypatch.setattr(kinesis_dogfood, "boto3_client", fail_client)
    monkeypatch.setattr(kinesis_dogfood, "install_signal_handlers", lambda _stop_event, _report: None)

    report = await kinesis_dogfood.run(args)

    assert report["success"] is True
    assert report["resources"]["stream"]["created"] is False
    assert report["resources"]["checkpoint_table"]["created"] is False
    assert report["resharding_summary"]["status"] == "dry_run"
    assert report["resharding_summary"]["requested_plan"] == [2, 1]
    assert report["resharding_summary"]["execution_plan"] == [2, 1]


@pytest.mark.asyncio
async def test_apply_reshard_updates_and_summarises_shards(monkeypatch):
    calls = []

    class FakeClient:
        def update_shard_count(self, **kwargs):
            calls.append(("update_shard_count", kwargs))

    args = kinesis_dogfood.build_parser().parse_args(["--backend", "aws"])
    client = FakeClient()

    async def inline_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(kinesis_dogfood.asyncio, "to_thread", inline_to_thread)
    monkeypatch.setattr(kinesis_dogfood, "boto3_client", lambda _service, _args: client)
    monkeypatch.setattr(kinesis_dogfood, "list_shard_ids", lambda _client, _stream_name: ["before", "after"])
    monkeypatch.setattr(kinesis_dogfood, "wait_stream_active", lambda _client, _stream_name, timeout_seconds=None: None)

    result = await kinesis_dogfood.apply_reshard(args, "stream-name", 2)

    assert result["status"] == "ok"
    assert result["target_shards"] == 2
    assert calls == [
        (
            "update_shard_count",
            {"StreamName": "stream-name", "TargetShardCount": 2, "ScalingType": "UNIFORM_SCALING"},
        )
    ]


def test_cleanup_orphans_requires_yes(tmp_path, monkeypatch):
    def fail_client(_service, _args):
        raise AssertionError("confirmation path should not create boto3 clients")

    monkeypatch.setattr(kinesis_dogfood, "boto3_client", fail_client)

    exit_code = kinesis_dogfood.main(
        [
            "--backend",
            "aws",
            "--cleanup-orphans",
            "--report-dir",
            str(tmp_path),
        ]
    )

    assert exit_code == 2


def test_list_stream_names_paginates():
    class FakeClient:
        def __init__(self):
            self.calls = []

        def list_streams(self, **kwargs):
            self.calls.append(kwargs)
            if not kwargs:
                return {"StreamNames": ["a", "b"], "HasMoreStreams": True}
            return {"StreamNames": ["c"], "HasMoreStreams": False}

    client = FakeClient()

    assert kinesis_dogfood.list_stream_names(client) == ["a", "b", "c"]
    assert client.calls == [{}, {"ExclusiveStartStreamName": "b"}]


def test_list_table_names_paginates():
    class FakeClient:
        def __init__(self):
            self.calls = []

        def list_tables(self, **kwargs):
            self.calls.append(kwargs)
            if not kwargs:
                return {"TableNames": ["a"], "LastEvaluatedTableName": "a"}
            return {"TableNames": ["b"]}

    client = FakeClient()

    assert kinesis_dogfood.list_table_names(client) == ["a", "b"]
    assert client.calls == [{}, {"ExclusiveStartTableName": "a"}]


@pytest.mark.asyncio
async def test_create_stream_records_failed_phase(monkeypatch):
    args = kinesis_dogfood.build_parser().parse_args(["--backend", "aws"])
    report = kinesis_dogfood.build_report(args, "run-id", "stream-name")

    async def inline_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    class FakeClient:
        def create_stream(self, **_kwargs):
            raise RuntimeError("no permission")

    monkeypatch.setattr(kinesis_dogfood.asyncio, "to_thread", inline_to_thread)
    monkeypatch.setattr(kinesis_dogfood, "boto3_client", lambda _service, _args: FakeClient())

    with pytest.raises(RuntimeError, match="no permission"):
        await kinesis_dogfood.create_stream(args, "stream-name", report)

    assert report["resources"]["stream"]["created"] is False
    assert report["phases"] == [
        {
            "name": "create_stream",
            "status": "failed",
            "duration_seconds": report["phases"][0]["duration_seconds"],
            "error": "RuntimeError: no permission",
        }
    ]
