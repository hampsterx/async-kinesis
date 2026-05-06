#!/usr/bin/env python3
"""Maintainer soak tool for exercising async-kinesis against a real backend."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import gc
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from kinesis import (
    CheckpointFlushError,
    Consumer,
    DynamoDBCheckPointer,
    InMemoryMetricsCollector,
    MemoryCheckPointer,
    Producer,
)

LOG_NAMES = ("kinesis",)
STREAM_PREFIX = "async-kinesis-dogfood"
DEFAULT_FLOCI_ENDPOINT = "http://localhost:4566"
LOGGER = logging.getLogger("kinesis.dogfood")

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None
    ClientError = None


@dataclass
class WorkloadStats:
    records_attempted: int = 0
    records_accepted: int = 0
    producer_errors: int = 0
    records_consumed: int = 0
    consumer_errors: int = 0
    checkpoint_flushes: int = 0
    checkpoint_flush_errors: int = 0
    checkpoint_flush_error_shards: int = 0
    idle_consumers: int = 0
    readiness_seconds: List[float] = field(default_factory=list)
    shard_status: List[Dict[str, Any]] = field(default_factory=list)


class DogfoodLogHandler(logging.Handler):
    """Capture warnings and errors from async-kinesis namespaces for the report."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.records: List[Dict[str, Any]] = []

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = record.getMessage()
        except Exception:
            message = str(record.msg)
        self.records.append(
            {
                "at": utc_now(),
                "logger": record.name,
                "level": record.levelname,
                "message": message,
            }
        )


class LogCapture:
    def __init__(self, logger_names: Iterable[str] = LOG_NAMES) -> None:
        self.logger_names = tuple(logger_names)
        self.handler = DogfoodLogHandler()

    def __enter__(self) -> DogfoodLogHandler:
        for name in self.logger_names:
            logger = logging.getLogger(name)
            logger.addHandler(self.handler)
        return self.handler

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        for name in self.logger_names:
            logger = logging.getLogger(name)
            logger.removeHandler(self.handler)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_run_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"


def parse_reshard_plan(value: Optional[str]) -> List[int]:
    if not value:
        return []
    plan = []
    for item in value.split(","):
        item = item.strip()
        if item:
            target = int(item)
            if target <= 0:
                raise argparse.ArgumentTypeError("--reshard-plan values must be positive")
            plan.append(target)
    return plan


def expand_reshard_plan(initial_shards: int, requested_plan: List[int]) -> List[int]:
    expanded = []
    current = initial_shards
    for target in requested_plan:
        while current < target:
            current = min(target, current * 2)
            expanded.append(current)
        while current > target:
            current = max(target, (current + 1) // 2)
            expanded.append(current)
    return expanded


def estimate_cost_usd(args: argparse.Namespace) -> float:
    if args.backend != "aws":
        return 0.0
    shard_counts = [args.initial_shards] + parse_reshard_plan(args.reshard_plan)
    max_shards = max(shard_counts or [args.initial_shards])
    hours = args.duration_minutes / 60.0
    kinesis_cost = max_shards * 0.015 * hours
    dynamodb_cost = 0.0
    if args.checkpointer == "dynamodb":
        estimated_records = args.records_per_second * args.duration_minutes * 60.0
        # On-demand writes are tiny at dogfood scale; include them so the banner
        # does not imply DynamoDB is free.
        dynamodb_cost = (estimated_records / 1_000_000.0) * 1.25
    return round(kinesis_cost + dynamodb_cost, 4)


def make_stream_name(args: argparse.Namespace, run_id: str) -> str:
    return args.stream_name or f"{STREAM_PREFIX}-{run_id}"


def make_checkpoint_table_name(args: argparse.Namespace, run_id: str) -> Optional[str]:
    if args.checkpointer != "dynamodb":
        return None
    return f"{STREAM_PREFIX}-{run_id}"


def build_config(args: argparse.Namespace) -> Dict[str, Any]:
    return {
        "backend": args.backend,
        "endpoint_url": args.endpoint_url,
        "region": args.region,
        "duration_minutes": args.duration_minutes,
        "initial_shards": args.initial_shards,
        "reshard_plan": parse_reshard_plan(args.reshard_plan),
        "reshard_timeout_seconds": args.reshard_timeout_seconds,
        "producers": args.producers,
        "consumers": args.consumers,
        "records_per_second": args.records_per_second,
        "record_size_bytes": args.record_size_bytes,
        "partition_mode": args.partition_mode,
        "checkpointer": args.checkpointer,
        "manual_checkpoint": args.manual_checkpoint,
        "checkpoint_interval": args.checkpoint_interval,
        "throttle_info_rate": args.throttle_info_rate,
        "scenario": args.scenario,
        "seed": args.seed,
        "keep_stream": args.keep_stream,
        "keep_checkpoint_table": args.keep_checkpoint_table,
        "cleanup_orphans": args.cleanup_orphans,
        "orphan_min_age_minutes": args.orphan_min_age_minutes,
        "dry_run": args.dry_run,
    }


def build_report(args: argparse.Namespace, run_id: str, stream_name: str) -> Dict[str, Any]:
    checkpoint_table_name = make_checkpoint_table_name(args, run_id)
    return {
        "run_id": run_id,
        "backend": args.backend,
        "region": args.region,
        "stream_name": stream_name,
        "started_at": utc_now(),
        "finished_at": None,
        "interrupted": False,
        "success": False,
        "estimated_cost_usd": estimate_cost_usd(args),
        "resources": {
            "stream": {"name": stream_name, "created": False, "deleted": False, "kept": args.keep_stream},
            "checkpoint_table": (
                {
                    "name": checkpoint_table_name,
                    "created": False,
                    "deleted": False,
                    "kept": args.keep_checkpoint_table,
                }
                if checkpoint_table_name
                else None
            ),
        },
        "config": build_config(args),
        "scenario": args.scenario,
        "phases": [],
        "producer_summary": {},
        "consumer_summary": {},
        "checkpointer_summary": {},
        "resharding_summary": {},
        "metrics": {},
        "log_findings": [],
        "findings": [],
    }


def add_finding(report: Dict[str, Any], severity: str, code: str, message: str, **details: Any) -> None:
    finding = {"severity": severity, "code": code, "message": message}
    if details:
        finding["details"] = details
    report["findings"].append(finding)


def mark_interrupted(report: Dict[str, Any], message: str) -> None:
    report["interrupted"] = True
    if not any(item.get("code") == "interrupted" for item in report["findings"]):
        add_finding(report, "low", "interrupted", message)


def has_blocking_findings(report: Dict[str, Any]) -> bool:
    return any(item.get("severity") in {"critical", "high"} for item in report["findings"])


def summarize_histogram(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"count": 0}
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": ordered[0],
        "p50": ordered[int((len(ordered) - 1) * 0.50)],
        "p95": ordered[int((len(ordered) - 1) * 0.95)],
        "max": ordered[-1],
        "avg": sum(ordered) / len(ordered),
    }


def serialize_metrics(metrics: InMemoryMetricsCollector) -> Dict[str, Any]:
    return {
        "counters": dict(sorted(metrics.counters.items())),
        "gauges": dict(sorted(metrics.gauges.items())),
        "histograms": {key: summarize_histogram(values) for key, values in sorted(metrics.histograms.items())},
    }


def print_confirmation_banner(args: argparse.Namespace, stream_name: str, checkpoint_table: Optional[str]) -> None:
    if args.cleanup_orphans:
        print("async-kinesis dogfood orphan cleanup will inspect and delete temporary resources:")
    else:
        print("async-kinesis dogfood soak will create temporary resources:")
    checkpoint_table_label = checkpoint_table or "<none>"
    print(f"  backend: {args.backend}")
    print(f"  region: {args.region}")
    print(f"  endpoint_url: {args.endpoint_url or '<aws default>'}")
    print(f"  stream_name: {stream_name}")
    print(f"  checkpoint_table: {checkpoint_table_label}")
    print(f"  initial_shards: {args.initial_shards}")
    requested_reshard_plan = parse_reshard_plan(args.reshard_plan)
    reshard_plan_label = (
        ",".join(str(target) for target in requested_reshard_plan) if requested_reshard_plan else "<none>"
    )
    print(f"  reshard_plan: {reshard_plan_label}")
    print(f"  duration_minutes: {args.duration_minutes}")
    print(f"  estimated_cost_usd: {estimate_cost_usd(args):.4f}")
    if args.cleanup_orphans:
        print(f"  cleanup: matching resources older than {args.orphan_min_age_minutes} minutes are deleted")
        print("Re-run with --yes to start orphan cleanup.")
    else:
        print("  cleanup: stream is deleted unless --keep-stream is set")
        print("Re-run with --yes to start the soak.")


def configure_backend_environment(args: argparse.Namespace) -> None:
    if args.backend == "floci":
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
        os.environ.setdefault("AWS_DEFAULT_REGION", args.region)


def boto3_client(service: str, args: argparse.Namespace) -> Any:
    if boto3 is None:
        raise RuntimeError("AWS control-plane support requires boto3 to be installed")
    kwargs = {"region_name": args.region}
    if args.endpoint_url:
        kwargs["endpoint_url"] = args.endpoint_url
    return boto3.client(service, **kwargs)


@contextlib.contextmanager
def boto3_client_ctx(service: str, args: argparse.Namespace) -> Iterator[Any]:
    client = boto3_client(service, args)
    try:
        yield client
    finally:
        try:
            client.close()
        except Exception:
            LOGGER.debug("boto3 %s client close failed", service, exc_info=True)


def client_error_code(exc: BaseException) -> Optional[str]:
    if ClientError is not None and isinstance(exc, ClientError):
        return exc.response.get("Error", {}).get("Code")
    return None


def list_shard_ids(client: Any, stream_name: str) -> List[str]:
    shard_ids = []
    kwargs = {"StreamName": stream_name}
    while True:
        response = client.list_shards(**kwargs)
        shard_ids.extend(shard["ShardId"] for shard in response.get("Shards", []))
        next_token = response.get("NextToken")
        if not next_token:
            return shard_ids
        kwargs = {"NextToken": next_token}


def wait_stream_active(client: Any, stream_name: str, timeout_seconds: float = 300.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while True:
        response = client.describe_stream_summary(StreamName=stream_name)
        status = response["StreamDescriptionSummary"]["StreamStatus"]
        if status == "ACTIVE":
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Stream {stream_name} did not become ACTIVE within {timeout_seconds} seconds")
        time.sleep(2)


def wait_stream_deleted(client: Any, stream_name: str, timeout_seconds: float = 300.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            client.describe_stream_summary(StreamName=stream_name)
        except Exception as exc:
            if client_error_code(exc) == "ResourceNotFoundException":
                return
            raise
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Stream {stream_name} was not deleted within {timeout_seconds} seconds")
        time.sleep(2)


def wait_table_deleted(client: Any, table_name: str, timeout_seconds: float = 300.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            client.describe_table(TableName=table_name)
        except Exception as exc:
            if client_error_code(exc) == "ResourceNotFoundException":
                return
            raise
        if time.monotonic() >= deadline:
            raise TimeoutError(f"DynamoDB table {table_name} was not deleted within {timeout_seconds} seconds")
        time.sleep(2)


def list_stream_names(client: Any) -> List[str]:
    stream_names = []
    kwargs: Dict[str, Any] = {}
    while True:
        response = client.list_streams(**kwargs)
        stream_names.extend(response.get("StreamNames", []))
        if not response.get("HasMoreStreams"):
            return stream_names
        if not response.get("StreamNames"):
            return stream_names
        kwargs = {"ExclusiveStartStreamName": response["StreamNames"][-1]}


def list_table_names(client: Any) -> List[str]:
    table_names = []
    kwargs: Dict[str, Any] = {}
    while True:
        response = client.list_tables(**kwargs)
        table_names.extend(response.get("TableNames", []))
        last_evaluated = response.get("LastEvaluatedTableName")
        if not last_evaluated:
            return table_names
        kwargs = {"ExclusiveStartTableName": last_evaluated}


async def create_stream(args: argparse.Namespace, stream_name: str, report: Dict[str, Any]) -> None:
    if args.dry_run:
        report["resources"]["stream"]["created"] = False
        report["phases"].append({"name": "create_stream", "status": "dry_run", "at": utc_now()})
        return

    started = time.monotonic()
    try:
        if args.backend == "aws":
            with boto3_client_ctx("kinesis", args) as client:

                def create_and_wait() -> None:
                    client.create_stream(StreamName=stream_name, ShardCount=args.initial_shards)
                    # Mark the stream as created before waiting for ACTIVE so cleanup runs
                    # even if the wait times out and AWS leaves the stream in CREATING state.
                    report["resources"]["stream"]["created"] = True
                    wait_stream_active(client, stream_name, timeout_seconds=args.reshard_timeout_seconds)

                await asyncio.to_thread(create_and_wait)
            report["phases"].append(
                {"name": "create_stream", "status": "ok", "duration_seconds": round(time.monotonic() - started, 3)}
            )
            return

        async with Producer(
            stream_name=stream_name,
            endpoint_url=args.endpoint_url,
            region_name=args.region,
            create_stream=True,
            create_stream_shards=args.initial_shards,
            describe_timeout=args.reshard_timeout_seconds,
        ):
            # Mark created on entry so cleanup runs even if Producer.__aexit__ raises.
            report["resources"]["stream"]["created"] = True
        report["phases"].append(
            {"name": "create_stream", "status": "ok", "duration_seconds": round(time.monotonic() - started, 3)}
        )
    except Exception as exc:
        report["phases"].append(
            {
                "name": "create_stream",
                "status": "failed",
                "duration_seconds": round(time.monotonic() - started, 3),
                "error": f"{exc.__class__.__name__}: {exc}",
            }
        )
        raise


async def delete_stream(args: argparse.Namespace, stream_name: str, report: Dict[str, Any]) -> None:
    if args.keep_stream or args.dry_run or not report["resources"]["stream"]["created"]:
        return

    started = time.monotonic()
    try:
        if args.backend == "aws":
            with boto3_client_ctx("kinesis", args) as client:

                def delete_and_wait() -> None:
                    try:
                        client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
                    except Exception as exc:
                        if client_error_code(exc) != "ResourceNotFoundException":
                            raise
                    wait_stream_deleted(client, stream_name, timeout_seconds=args.reshard_timeout_seconds)

                await asyncio.to_thread(delete_and_wait)
            report["resources"]["stream"]["deleted"] = True
            report["phases"].append(
                {"name": "delete_stream", "status": "ok", "duration_seconds": round(time.monotonic() - started, 3)}
            )
            return

        async with Producer(
            stream_name=stream_name,
            endpoint_url=args.endpoint_url,
            region_name=args.region,
            describe_timeout=args.reshard_timeout_seconds,
        ) as producer:
            await producer.client.delete_stream(StreamName=stream_name)
        report["resources"]["stream"]["deleted"] = True
        report["phases"].append(
            {"name": "delete_stream", "status": "ok", "duration_seconds": round(time.monotonic() - started, 3)}
        )
    except Exception as exc:
        add_finding(report, "critical", "cleanup_failed", f"Failed to delete stream {stream_name}: {exc}")
        report["phases"].append(
            {
                "name": "delete_stream",
                "status": "failed",
                "duration_seconds": round(time.monotonic() - started, 3),
                "error": str(exc),
            }
        )


async def delete_checkpoint_table(args: argparse.Namespace, report: Dict[str, Any]) -> None:
    table = report["resources"].get("checkpoint_table")
    if not table or args.keep_checkpoint_table or args.dry_run or not table["created"]:
        return

    started = time.monotonic()
    table_name = table["name"]
    try:
        with boto3_client_ctx("dynamodb", args) as client:

            def delete_and_wait() -> None:
                try:
                    client.delete_table(TableName=table_name)
                except Exception as exc:
                    if client_error_code(exc) != "ResourceNotFoundException":
                        raise
                wait_table_deleted(client, table_name, timeout_seconds=args.reshard_timeout_seconds)

            await asyncio.to_thread(delete_and_wait)
        table["deleted"] = True
        report["phases"].append(
            {
                "name": "delete_checkpoint_table",
                "status": "ok",
                "duration_seconds": round(time.monotonic() - started, 3),
            }
        )
    except Exception as exc:
        add_finding(report, "critical", "cleanup_failed", f"Failed to delete DynamoDB table {table_name}: {exc}")
        report["phases"].append(
            {
                "name": "delete_checkpoint_table",
                "status": "failed",
                "duration_seconds": round(time.monotonic() - started, 3),
                "error": str(exc),
            }
        )


async def apply_reshard(args: argparse.Namespace, stream_name: str, target_shards: int) -> Dict[str, Any]:
    if args.dry_run:
        return {"target_shards": target_shards, "status": "dry_run", "at": utc_now()}

    if args.backend != "aws":
        raise NotImplementedError("resharding requires --backend aws")

    started = time.monotonic()
    with boto3_client_ctx("kinesis", args) as client:

        def update_and_wait() -> Tuple[List[str], List[str]]:
            before = list_shard_ids(client, stream_name)
            client.update_shard_count(
                StreamName=stream_name,
                TargetShardCount=target_shards,
                ScalingType="UNIFORM_SCALING",
            )
            wait_stream_active(client, stream_name, timeout_seconds=args.reshard_timeout_seconds)
            after = list_shard_ids(client, stream_name)
            return before, after

        before_shards, after_shards = await asyncio.to_thread(update_and_wait)
    return {
        "target_shards": target_shards,
        "status": "ok",
        "duration_seconds": round(time.monotonic() - started, 3),
        "before_shards": before_shards,
        "after_shards": after_shards,
    }


def make_payload(args: argparse.Namespace, run_id: str, producer_id: int, sequence: int) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "producer_id": producer_id,
        "sequence": sequence,
        "created_at": utc_now(),
    }
    encoded_size = len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    padding_size = max(0, args.record_size_bytes - encoded_size - 16)
    if padding_size:
        payload["padding"] = "x" * padding_size
    return payload


def partition_key(args: argparse.Namespace, rng: random.Random, producer_id: int, sequence: int) -> str:
    mode = args.partition_mode
    if mode == "mixed":
        mode = ("hot", "random", "ordered")[sequence % 3]
    if mode == "hot":
        return "hot-key"
    if mode == "ordered":
        return f"user-{producer_id}-{sequence % 64}"
    return f"key-{rng.getrandbits(64):016x}"


async def producer_worker(
    args: argparse.Namespace,
    run_id: str,
    producer_id: int,
    producer: Producer,
    stop_event: asyncio.Event,
    stats: WorkloadStats,
) -> None:
    rng = random.Random((args.seed or 0) + producer_id)
    sequence = 0
    interval = args.producers / args.records_per_second if args.records_per_second > 0 else 0
    while not stop_event.is_set():
        started = time.monotonic()
        payload = make_payload(args, run_id, producer_id, sequence)
        key = partition_key(args, rng, producer_id, sequence)
        stats.records_attempted += 1
        try:
            await producer.put(payload, partition_key=key)
            stats.records_accepted += 1
        except Exception:
            stats.producer_errors += 1
            LOGGER.exception("producer %s failed to put record", producer_id)
            await asyncio.sleep(0.5)
        sequence += 1
        sleep_for = interval - (time.monotonic() - started)
        if sleep_for > 0:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=sleep_for)
            except asyncio.TimeoutError:
                pass


async def consumer_worker(
    consumer_id: int, consumer: Consumer, stop_event: asyncio.Event, stats: WorkloadStats
) -> None:
    while not stop_event.is_set():
        try:
            await consumer.__anext__()
            stats.records_consumed += 1
        except StopAsyncIteration:
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            raise
        except Exception:
            stats.consumer_errors += 1
            LOGGER.exception("consumer %s failed while reading", consumer_id)
            await asyncio.sleep(0.5)


def build_checkpointer(
    args: argparse.Namespace,
    run_id: str,
    consumer_id: int,
    checkpoint_table_name: Optional[str],
) -> Any:
    checkpointer_name = f"dogfood-{run_id}"
    checkpointer_id = f"consumer-{consumer_id}"
    if args.checkpointer == "memory":
        if args.manual_checkpoint:
            raise NotImplementedError("manual checkpointing is only supported with --checkpointer dynamodb")
        return MemoryCheckPointer(name=checkpointer_name, id=checkpointer_id)
    if args.checkpointer == "dynamodb":
        if DynamoDBCheckPointer is None:
            raise ImportError("DynamoDB support requires async-kinesis[dynamodb] to be installed")
        return DynamoDBCheckPointer(
            name=checkpointer_name,
            table_name=checkpoint_table_name,
            id=checkpointer_id,
            auto_checkpoint=not args.manual_checkpoint,
            create_table=True,
            endpoint_url=args.endpoint_url,
            region_name=args.region,
        )
    raise NotImplementedError("only --checkpointer memory and dynamodb are supported")


async def manual_checkpoint_worker(
    checkpointers: List[Any],
    interval: float,
    stop_event: asyncio.Event,
    stats: WorkloadStats,
) -> None:
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass
        await flush_manual_checkpoints(checkpointers, stats)


async def flush_manual_checkpoints(checkpointers: List[Any], stats: WorkloadStats) -> None:
    for checkpointer in checkpointers:
        try:
            await checkpointer.manual_checkpoint()
            stats.checkpoint_flushes += 1
        except CheckpointFlushError as exc:
            stats.checkpoint_flushes += 1
            stats.checkpoint_flush_errors += 1
            stats.checkpoint_flush_error_shards += len(exc.errors)
            LOGGER.warning("manual checkpoint flush failed for %s shard(s)", len(exc.errors), exc_info=True)


async def wait_consumer_ready_or_idle(consumer: Consumer, timeout: float) -> Tuple[str, float]:
    started = time.monotonic()
    try:
        await consumer.wait_ready(timeout=timeout)
        return "ready", round(time.monotonic() - started, 3)
    except asyncio.TimeoutError:
        status = consumer.get_shard_status()
        if status.get("total_shards", 0) > 0 and status.get("allocated_shards", 0) == 0:
            LOGGER.info("consumer has no allocated shards after readiness timeout; treating it as idle")
            return "idle", round(time.monotonic() - started, 3)
        raise


def unexpected_results(task_results: Iterable[Any]) -> List[BaseException]:
    """Filter ``asyncio.gather(..., return_exceptions=True)`` output for failures
    that were not the expected cancellation."""
    return [
        result
        for result in task_results
        if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError)
    ]


async def run_workload(
    args: argparse.Namespace,
    run_id: str,
    stream_name: str,
    report: Dict[str, Any],
    stop_event: asyncio.Event,
) -> None:
    requested_reshard_plan = parse_reshard_plan(args.reshard_plan)
    reshard_plan = expand_reshard_plan(args.initial_shards, requested_reshard_plan)
    max_shard_consumers = max(args.initial_shards, max(reshard_plan, default=0))

    metrics = InMemoryMetricsCollector()
    stats = WorkloadStats()
    producers: List[Producer] = []
    consumers: List[Consumer] = []
    checkpointers: List[Any] = []
    tasks: List[asyncio.Task] = []
    checkpoint_table = report["resources"].get("checkpoint_table")
    checkpoint_table_name = checkpoint_table["name"] if checkpoint_table else None
    started = time.monotonic()
    workload_status = "ok"
    workload_error: Optional[BaseException] = None

    try:
        for consumer_id in range(args.consumers):
            checkpointer = build_checkpointer(args, run_id, consumer_id, checkpoint_table_name)
            consumer = Consumer(
                stream_name=stream_name,
                endpoint_url=args.endpoint_url,
                region_name=args.region,
                iterator_type="LATEST",
                sleep_time_no_records=0.1,
                idle_timeout=0.2,
                max_shard_consumers=max_shard_consumers,
                checkpointer=checkpointer,
                metrics_collector=metrics,
                use_list_shards=True,
            )
            await consumer.__aenter__()
            checkpointers.append(checkpointer)
            consumers.append(consumer)
            if checkpoint_table is not None:
                checkpoint_table["created"] = True
            readiness_state, readiness_seconds = await wait_consumer_ready_or_idle(consumer, timeout=30)
            if readiness_state == "ready":
                stats.readiness_seconds.append(readiness_seconds)
            else:
                stats.idle_consumers += 1
            tasks.append(asyncio.create_task(consumer_worker(consumer_id, consumer, stop_event, stats)))

        for producer_id in range(args.producers):
            producer = Producer(
                stream_name=stream_name,
                endpoint_url=args.endpoint_url,
                region_name=args.region,
                buffer_time=0.2,
                metrics_collector=metrics,
                use_list_shards=True,
            )
            await producer.__aenter__()
            producers.append(producer)
            tasks.append(asyncio.create_task(producer_worker(args, run_id, producer_id, producer, stop_event, stats)))

        if args.manual_checkpoint:
            interval = args.checkpoint_interval or 5.0
            tasks.append(asyncio.create_task(manual_checkpoint_worker(checkpointers, interval, stop_event, stats)))

        duration_seconds = args.duration_minutes * 60.0
        if reshard_plan:
            report["resharding_summary"] = {
                "requested_plan": requested_reshard_plan,
                "execution_plan": reshard_plan,
                "status": "running",
                "transitions": [],
                "expired_parents": [],
            }
            phase_seconds = duration_seconds / (len(reshard_plan) + 1)
            for target_shards in reshard_plan:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=phase_seconds)
                    break
                except asyncio.TimeoutError:
                    pass
                transition = await apply_reshard(args, stream_name, target_shards)
                report["resharding_summary"]["transitions"].append(transition)
            remaining_seconds = max(0.0, duration_seconds - (phase_seconds * len(reshard_plan)))
            if not stop_event.is_set() and remaining_seconds:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=remaining_seconds)
                except asyncio.TimeoutError:
                    pass
            report["resharding_summary"]["status"] = "interrupted" if stop_event.is_set() else "ok"
        else:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=duration_seconds)
            except asyncio.TimeoutError:
                pass
    except BaseException as exc:
        workload_status = "failed"
        workload_error = exc
        raise
    finally:
        stop_event.set()
        if tasks:
            for task in tasks:
                task.cancel()
            task_results = await asyncio.gather(*tasks, return_exceptions=True)
            unexpected_task_errors = unexpected_results(task_results)
            for result in unexpected_task_errors:
                LOGGER.error(
                    "background workload task failed: %s",
                    result,
                    exc_info=(type(result), result, result.__traceback__),
                )
            if unexpected_task_errors and workload_error is None:
                workload_status = "failed"
                workload_error = unexpected_task_errors[0]

        for producer in producers:
            try:
                await producer.flush()
            except Exception:
                stats.producer_errors += 1
                LOGGER.exception("producer flush failed during shutdown")

        if args.manual_checkpoint:
            await flush_manual_checkpoints(checkpointers, stats)

        for consumer in consumers:
            try:
                stats.shard_status.append(consumer.get_shard_status())
            except Exception:
                stats.consumer_errors += 1
                LOGGER.exception("consumer status snapshot failed during shutdown")

        for consumer in reversed(consumers):
            try:
                await consumer.__aexit__(None, None, None)
            except Exception:
                stats.consumer_errors += 1
                LOGGER.exception("consumer close failed during shutdown")

        for producer in reversed(producers):
            try:
                await producer.__aexit__(None, None, None)
            except Exception:
                stats.producer_errors += 1
                LOGGER.exception("producer close failed during shutdown")

        phase = {
            "name": "workload",
            "status": workload_status,
            "duration_seconds": round(time.monotonic() - started, 3),
        }
        if workload_error is not None:
            phase["error"] = f"{workload_error.__class__.__name__}: {workload_error}"
        report["phases"].append(phase)
        report["producer_summary"] = {
            "records_attempted": stats.records_attempted,
            "records_accepted": stats.records_accepted,
            "errors": stats.producer_errors,
            "producer_count": args.producers,
        }
        report["consumer_summary"] = {
            "records_consumed": stats.records_consumed,
            "errors": stats.consumer_errors,
            "consumer_count": args.consumers,
            "idle_consumers": stats.idle_consumers,
            "readiness_seconds": stats.readiness_seconds,
            "shard_status": stats.shard_status,
        }
        report["checkpointer_summary"] = {
            "type": args.checkpointer,
            "manual": args.manual_checkpoint,
            "flushes": stats.checkpoint_flushes,
            "flush_errors": stats.checkpoint_flush_errors,
            "flush_error_shards": stats.checkpoint_flush_error_shards,
        }
        if not report["resharding_summary"]:
            report["resharding_summary"] = {
                "requested_plan": requested_reshard_plan,
                "execution_plan": reshard_plan,
                "status": "not_requested",
                "expired_parents": [],
            }
        report["metrics"] = serialize_metrics(metrics)


def collect_warning_findings(caught_warnings: Iterable[warnings.WarningMessage]) -> List[Dict[str, Any]]:
    findings = []
    for warning in caught_warnings:
        findings.append(
            {
                "at": utc_now(),
                "logger": "warnings",
                "level": warning.category.__name__,
                "message": str(warning.message),
                "filename": warning.filename,
                "line": warning.lineno,
            }
        )
    return findings


def summarize_error_log_findings(log_findings: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    summaries: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for finding in log_findings:
        level = finding.get("level")
        if level not in {"ERROR", "CRITICAL"}:
            continue
        key = (
            str(finding.get("logger", "")),
            str(level),
            str(finding.get("message", "")),
        )
        summary = summaries.setdefault(
            key,
            {
                "logger": key[0],
                "level": key[1],
                "message": key[2],
                "count": 0,
            },
        )
        summary["count"] += 1
    return sorted(summaries.values(), key=lambda item: (-item["count"], item["logger"], item["message"]))


def add_log_error_findings(report: Dict[str, Any]) -> None:
    summaries = summarize_error_log_findings(report.get("log_findings", []))
    if not summaries:
        return
    if any(finding.get("code") == "captured_error_logs" for finding in report["findings"]):
        return

    total = sum(item["count"] for item in summaries)
    severity = "critical" if any(item["level"] == "CRITICAL" for item in summaries) else "high"
    add_finding(
        report,
        severity,
        "captured_error_logs",
        f"Captured {total} ERROR/CRITICAL log record(s) from async-kinesis during the run",
        logs=summaries[:20],
        truncated=max(0, len(summaries) - 20),
    )


def parse_aws_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


async def cleanup_orphans(args: argparse.Namespace, report: Dict[str, Any]) -> None:
    if args.dry_run:
        report["phases"].append({"name": "cleanup_orphans", "status": "dry_run", "at": utc_now()})
        report["success"] = True
        return

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=args.orphan_min_age_minutes)
    deleted_streams = []
    deleted_tables = []
    skipped = []
    failures = []
    started = time.monotonic()

    with boto3_client_ctx("kinesis", args) as kinesis_client, boto3_client_ctx("dynamodb", args) as dynamodb_client:

        def sweep() -> None:
            for stream_name in list_stream_names(kinesis_client):
                if not stream_name.startswith(f"{STREAM_PREFIX}-"):
                    continue
                try:
                    response = kinesis_client.describe_stream_summary(StreamName=stream_name)
                    created_at = parse_aws_datetime(response["StreamDescriptionSummary"]["StreamCreationTimestamp"])
                    if created_at > cutoff:
                        skipped.append({"type": "stream", "name": stream_name, "reason": "too_new"})
                        continue
                    kinesis_client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
                    wait_stream_deleted(kinesis_client, stream_name, timeout_seconds=args.reshard_timeout_seconds)
                    deleted_streams.append(stream_name)
                except Exception as exc:
                    failures.append({"type": "stream", "name": stream_name, "reason": str(exc)})

            for table_name in list_table_names(dynamodb_client):
                if not table_name.startswith(f"{STREAM_PREFIX}-"):
                    continue
                try:
                    response = dynamodb_client.describe_table(TableName=table_name)
                    created_at = parse_aws_datetime(response["Table"]["CreationDateTime"])
                    if created_at > cutoff:
                        skipped.append({"type": "table", "name": table_name, "reason": "too_new"})
                        continue
                    dynamodb_client.delete_table(TableName=table_name)
                    wait_table_deleted(dynamodb_client, table_name, timeout_seconds=args.reshard_timeout_seconds)
                    deleted_tables.append(table_name)
                except Exception as exc:
                    failures.append({"type": "table", "name": table_name, "reason": str(exc)})

        await asyncio.to_thread(sweep)
    report["orphan_cleanup"] = {
        "deleted_streams": deleted_streams,
        "deleted_tables": deleted_tables,
        "skipped": skipped,
        "failures": failures,
    }
    report["phases"].append(
        {
            "name": "cleanup_orphans",
            "status": "failed" if failures else "ok",
            "duration_seconds": round(time.monotonic() - started, 3),
            "failure_count": len(failures),
        }
    )
    report["success"] = not failures


def add_dry_run_report(args: argparse.Namespace, report: Dict[str, Any]) -> None:
    requested_reshard_plan = parse_reshard_plan(args.reshard_plan)
    reshard_plan = expand_reshard_plan(args.initial_shards, requested_reshard_plan)
    report["phases"].extend(
        [
            {"name": "create_stream", "status": "dry_run", "at": utc_now()},
            {"name": "workload", "status": "dry_run", "at": utc_now()},
            {"name": "delete_stream", "status": "dry_run", "at": utc_now()},
        ]
    )
    report["producer_summary"] = {"producer_count": args.producers, "records_attempted": 0, "records_accepted": 0}
    report["consumer_summary"] = {"consumer_count": args.consumers, "records_consumed": 0}
    report["checkpointer_summary"] = {"type": args.checkpointer, "manual": args.manual_checkpoint}
    report["resharding_summary"] = {
        "requested_plan": requested_reshard_plan,
        "execution_plan": reshard_plan,
        "status": "dry_run" if reshard_plan else "not_requested",
        "expired_parents": [],
    }
    report["success"] = True


def write_reports(report: Dict[str, Any], report_dir: Path) -> Tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / f"{report['run_id']}.json"
    md_path = report_dir / f"{report['run_id']}.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    md_path.write_text(render_markdown_report(report))
    return json_path, md_path


def render_markdown_report(report: Dict[str, Any]) -> str:
    lines = [
        f"# async-kinesis Dogfood Report: {report['run_id']}",
        "",
        f"- Backend: `{report['backend']}`",
        f"- Region: `{report['region']}`",
        f"- Stream: `{report['stream_name']}`",
        f"- Started: `{report['started_at']}`",
        f"- Finished: `{report['finished_at']}`",
        f"- Success: `{report['success']}`",
        f"- Interrupted: `{report['interrupted']}`",
        f"- Estimated cost USD: `{report['estimated_cost_usd']}`",
        "",
        "## Summary",
        "",
        f"- Producers: `{report.get('producer_summary', {}).get('producer_count', 0)}`",
        f"- Records attempted: `{report.get('producer_summary', {}).get('records_attempted', 0)}`",
        f"- Records accepted: `{report.get('producer_summary', {}).get('records_accepted', 0)}`",
        f"- Consumers: `{report.get('consumer_summary', {}).get('consumer_count', 0)}`",
        f"- Records consumed: `{report.get('consumer_summary', {}).get('records_consumed', 0)}`",
        "",
        "## Findings",
        "",
    ]
    if report["findings"]:
        for finding in report["findings"]:
            lines.append(f"- `{finding['severity']}` `{finding['code']}`: {finding['message']}")
    else:
        lines.append("- None")
    lines.extend(["", "## Log Findings", ""])
    if report["log_findings"]:
        for finding in report["log_findings"][:50]:
            lines.append(f"- `{finding.get('level')}` `{finding.get('logger')}`: {finding.get('message', '')}".rstrip())
        if len(report["log_findings"]) > 50:
            lines.append(f"- Truncated {len(report['log_findings']) - 50} additional log findings")
    else:
        lines.append("- None")
    lines.extend(["", "## Phases", ""])
    for phase in report["phases"]:
        lines.append(f"- `{phase['name']}`: `{phase['status']}`")
    return "\n".join(lines) + "\n"


def install_signal_handlers(stop_event: asyncio.Event, report: Dict[str, Any]) -> None:
    loop = asyncio.get_running_loop()

    def request_stop(signum: int) -> None:
        mark_interrupted(report, f"Received signal {signal.Signals(signum).name}")
        stop_event.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, request_stop, signum)
        except NotImplementedError:
            signal.signal(signum, lambda _signum, _frame: request_stop(_signum))


def preflight_unsupported(args: argparse.Namespace, report: Dict[str, Any]) -> bool:
    """Return True if the run cannot proceed because of an unsupported option.

    Checks every option that would otherwise let the harness create temporary
    AWS resources before failing. Adds a high-severity finding so the report
    reflects the rejection without a stray cleanup phase.
    """
    if args.checkpointer == "redis":
        add_finding(report, "high", "unsupported_checkpointer", "redis checkpointer is not implemented")
        return True
    if args.scenario is not None:
        add_finding(report, "high", "unsupported_scenario", f"scenario {args.scenario!r} is not implemented")
        return True
    requested_reshard_plan = parse_reshard_plan(args.reshard_plan)
    if requested_reshard_plan and args.backend != "aws":
        add_finding(report, "high", "unsupported_reshard", "resharding is only supported with --backend aws")
        return True
    return False


async def run(args: argparse.Namespace) -> Dict[str, Any]:
    configure_backend_environment(args)
    run_id = build_run_id()
    stream_name = make_stream_name(args, run_id)
    report = build_report(args, run_id, stream_name)
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event, report)
    checkpoint_table = report["resources"].get("checkpoint_table")
    checkpoint_table_name = checkpoint_table["name"] if checkpoint_table else None

    if not args.yes:
        print_confirmation_banner(args, stream_name, checkpoint_table_name)
        report["finished_at"] = utc_now()
        return report

    if args.cleanup_orphans:
        try:
            await cleanup_orphans(args, report)
        except Exception as exc:
            add_finding(report, "high", "cleanup_orphans_failed", f"{exc.__class__.__name__}: {exc}")
            LOGGER.exception("dogfood orphan cleanup failed")
        report["finished_at"] = utc_now()
        return report

    if preflight_unsupported(args, report):
        report["finished_at"] = utc_now()
        return report

    if args.dry_run:
        add_dry_run_report(args, report)
        report["finished_at"] = utc_now()
        return report

    with LogCapture() as log_handler, warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter("always")
        try:
            await create_stream(args, stream_name, report)
            if not stop_event.is_set():
                await run_workload(args, run_id, stream_name, report, stop_event)
        except (asyncio.CancelledError, KeyboardInterrupt):
            mark_interrupted(report, "Run cancelled by signal")
            stop_event.set()
        except NotImplementedError as exc:
            add_finding(report, "high", "unsupported_option", str(exc))
        except Exception as exc:
            add_finding(report, "high", "run_failed", f"{exc.__class__.__name__}: {exc}")
            LOGGER.exception("dogfood run failed")
        finally:
            await delete_checkpoint_table(args, report)
            await delete_stream(args, stream_name, report)
            gc.collect()
            report["log_findings"] = log_handler.records + collect_warning_findings(caught_warnings)
            add_log_error_findings(report)

    report["finished_at"] = utc_now()
    report["success"] = not report["interrupted"] and not has_blocking_findings(report)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run an async-kinesis dogfood soak against a temporary stream.")
    parser.add_argument("--backend", choices=("aws", "floci"), default="aws")
    parser.add_argument("--endpoint-url", default=os.environ.get("ENDPOINT_URL"))
    parser.add_argument("--region", default=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    parser.add_argument("--stream-name")
    parser.add_argument("--duration-minutes", type=float, default=30)
    parser.add_argument("--initial-shards", type=int, default=1)
    parser.add_argument("--reshard-plan")
    parser.add_argument(
        "--reshard-timeout-seconds",
        type=float,
        default=300.0,
        help="seconds to wait for stream/table state transitions during reshard and cleanup",
    )
    parser.add_argument("--producers", type=int, default=2)
    parser.add_argument("--consumers", type=int, default=3)
    parser.add_argument("--records-per-second", type=float, default=200)
    parser.add_argument("--record-size-bytes", type=int, default=256)
    parser.add_argument("--partition-mode", choices=("hot", "random", "ordered", "mixed"), default="mixed")
    parser.add_argument(
        "--checkpointer",
        choices=("memory", "redis", "dynamodb"),
        help="checkpointer backend; default is dynamodb for AWS and memory for Floci",
    )
    parser.add_argument("--manual-checkpoint", action="store_true")
    parser.add_argument("--checkpoint-interval", type=float)
    parser.add_argument("--throttle-info-rate", type=float, default=5)
    parser.add_argument(
        "--scenario",
        choices=("issue-73-reconnect", "issue-80-concurrent-flush", "issue-81-papercuts", "issue-82-dedupe"),
    )
    parser.add_argument("--seed", type=int)
    parser.add_argument("--report-dir", type=Path, default=Path("dogfood-reports"))
    parser.add_argument("--keep-stream", action="store_true")
    parser.add_argument("--keep-checkpoint-table", action="store_true")
    parser.add_argument("--cleanup-orphans", action="store_true")
    parser.add_argument("--orphan-min-age-minutes", type=float, default=60)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.backend == "floci" and args.endpoint_url is None:
        args.endpoint_url = DEFAULT_FLOCI_ENDPOINT
    if args.checkpointer is None:
        args.checkpointer = "memory" if args.backend == "floci" else "dynamodb"
    if args.duration_minutes <= 0:
        raise SystemExit("--duration-minutes must be positive")
    if args.initial_shards <= 0:
        raise SystemExit("--initial-shards must be positive")
    if args.producers <= 0:
        raise SystemExit("--producers must be positive")
    if args.consumers <= 0:
        raise SystemExit("--consumers must be positive")
    if args.records_per_second <= 0:
        raise SystemExit("--records-per-second must be positive")
    if args.record_size_bytes <= 0:
        raise SystemExit("--record-size-bytes must be positive")
    if args.reshard_timeout_seconds <= 0:
        raise SystemExit("--reshard-timeout-seconds must be positive")
    if args.checkpoint_interval is not None and args.checkpoint_interval <= 0:
        raise SystemExit("--checkpoint-interval must be positive")
    if args.orphan_min_age_minutes <= 0:
        raise SystemExit("--orphan-min-age-minutes must be positive")
    try:
        parse_reshard_plan(args.reshard_plan)
    except argparse.ArgumentTypeError as exc:
        raise SystemExit(str(exc))


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_args(args)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    if not args.verbose:
        for logger_name in ("aiobotocore", "botocore", "kinesis"):
            logging.getLogger(logger_name).setLevel(logging.WARNING)

    report = asyncio.run(run(args))
    json_path, md_path = write_reports(report, args.report_dir)
    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    if not args.yes:
        return 2
    return 0 if report["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
