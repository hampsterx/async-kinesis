import json
import sys

import click

from kinesis.base import Base
from kinesis.checkpointers import MemoryCheckPointer
from kinesis.consumer import Consumer
from kinesis.processors import JsonProcessor, StringProcessor
from kinesis.producer import Producer

from . import async_command, format_kv, format_table

PROCESSORS = {
    "json": JsonProcessor,
    "string": StringProcessor,
}


class _ClientHelper(Base):
    """Minimal Base subclass for lightweight Kinesis API access (describe/list only)."""

    def __init__(self, stream_name="", endpoint_url=None, region_name=None):
        super().__init__(
            stream_name=stream_name,
            endpoint_url=endpoint_url,
            region_name=region_name,
            skip_describe_stream=True,
        )

    async def close(self):
        if self.client is not None:
            await self.client.close()


@click.command()
@click.argument("stream")
@click.pass_context
@async_command
async def describe(ctx, stream):
    """Show details about a Kinesis stream."""
    endpoint_url = ctx.obj["endpoint_url"]
    region = ctx.obj["region"]

    async with _ClientHelper(stream_name=stream, endpoint_url=endpoint_url, region_name=region) as helper:
        info = await helper.get_stream_description()

        # Stream info
        pairs = [
            ("Name", info["StreamName"]),
            ("ARN", info["StreamARN"]),
            ("Status", info["StreamStatus"]),
            ("Retention", f'{info.get("RetentionPeriodHours", "?")} hours'),
            ("Encryption", info.get("EncryptionType", "NONE")),
            ("Shards", str(len(info["Shards"]))),
        ]
        mode = info.get("StreamModeDetails", {}).get("StreamMode")
        if mode:
            pairs.append(("Mode", mode))

        click.echo(format_kv(pairs))

        # Shard table
        if info["Shards"]:
            click.echo()
            headers = ["Shard ID", "Status", "Start Hash", "End Hash", "Start Seq", "End Seq"]
            rows = []
            for shard in info["Shards"]:
                seq_range = shard.get("SequenceNumberRange", {})
                hash_range = shard.get("HashKeyRange", {})
                status = "CLOSED" if "EndingSequenceNumber" in seq_range else "OPEN"
                rows.append(
                    [
                        shard["ShardId"],
                        status,
                        hash_range.get("StartingHashKey", ""),
                        hash_range.get("EndingHashKey", ""),
                        seq_range.get("StartingSequenceNumber", ""),
                        seq_range.get("EndingSequenceNumber", ""),
                    ]
                )
            click.echo(format_table(headers, rows))


@click.command("list")
@click.option("--limit", default=100, type=int, help="Maximum number of streams to list.")
@click.pass_context
@async_command
async def list_streams(ctx, limit):
    """List Kinesis streams."""
    endpoint_url = ctx.obj["endpoint_url"]
    region = ctx.obj["region"]

    async with _ClientHelper(endpoint_url=endpoint_url, region_name=region) as helper:
        response = await helper.client.list_streams(Limit=limit)

        # AWS returns StreamSummaries; kinesalite returns only StreamNames
        summaries = response.get("StreamSummaries")
        if summaries is not None:
            if not summaries:
                click.echo("No streams found.")
                return
            headers = ["Name", "Status", "Shards", "Mode"]
            rows = []
            for s in summaries:
                mode = s.get("StreamModeDetails", {}).get("StreamMode", "")
                rows.append(
                    [
                        s.get("StreamName", ""),
                        s.get("StreamStatus", ""),
                        str(s.get("OpenShardCount", "?")),
                        mode,
                    ]
                )
            click.echo(format_table(headers, rows))
        else:
            names = response.get("StreamNames", [])
            if not names:
                click.echo("No streams found.")
                return
            # Kinesalite / basic: just names, fetch details per stream
            headers = ["Name", "Status", "Shards"]
            rows = []
            for name in names:
                try:
                    resp = await helper.client.describe_stream(StreamName=name)
                    info = resp["StreamDescription"]
                    rows.append(
                        [
                            info["StreamName"],
                            info["StreamStatus"],
                            str(len(info["Shards"])),
                        ]
                    )
                except Exception:
                    rows.append([name, "?", "?"])
            click.echo(format_table(headers, rows))


ITERATOR_TYPES = click.Choice(["LATEST", "TRIM_HORIZON", "AT_TIMESTAMP"])
OUTPUT_FORMATS = click.Choice(["json-pretty", "json", "raw", "raw-short"])
PROCESSOR_NAMES = click.Choice(["json", "string"])


@click.command()
@click.argument("stream")
@click.option("-i", "--iterator-type", type=ITERATOR_TYPES, default="LATEST", help="Shard iterator type.")
@click.option("-f", "--format", "output_format", type=OUTPUT_FORMATS, default="json-pretty", help="Output format.")
@click.option("-p", "--processor", "processor_name", type=PROCESSOR_NAMES, default="json", help="Record processor.")
@click.option("-n", "--max-records", type=int, default=None, help="Stop after N records.")
@click.pass_context
@async_command
async def tail(ctx, stream, iterator_type, output_format, processor_name, max_records):
    """Tail records from a Kinesis stream."""
    endpoint_url = ctx.obj["endpoint_url"]
    region = ctx.obj["region"]

    processor = PROCESSORS[processor_name]()
    consumer = Consumer(
        stream_name=stream,
        endpoint_url=endpoint_url,
        region_name=region,
        iterator_type=iterator_type,
        checkpointer=MemoryCheckPointer(),
        processor=processor,
    )

    count = 0
    async with consumer:
        while True:
            async for record in consumer:
                _print_record(record, output_format)
                count += 1
                if max_records and count >= max_records:
                    return


def _print_record(record, output_format):
    """Print a single record in the requested format."""
    if output_format in ("json-pretty", "json"):
        if isinstance(record, (dict, list)):
            indent = 2 if output_format == "json-pretty" else None
            click.echo(json.dumps(record, indent=indent, default=str))
        else:
            click.echo(record)
    elif output_format == "raw":
        click.echo(repr(record))
    elif output_format == "raw-short":
        text = repr(record)
        if len(text) > 120:
            text = text[:117] + "..."
        click.echo(text)


@click.command()
@click.argument("stream")
@click.argument("data", required=False)
@click.option("-k", "--partition-key", default=None, help="Explicit partition key.")
@click.option("-p", "--processor", "processor_name", type=PROCESSOR_NAMES, default="json", help="Record processor.")
@click.option("--create", is_flag=True, help="Create stream if it does not exist.")
@click.pass_context
@async_command
async def put(ctx, stream, data, partition_key, processor_name, create):
    """Put records into a Kinesis stream.

    DATA is a single record. If omitted, reads lines from stdin (JSONL support).
    """
    endpoint_url = ctx.obj["endpoint_url"]
    region = ctx.obj["region"]

    processor = PROCESSORS[processor_name]()
    producer = Producer(
        stream_name=stream,
        endpoint_url=endpoint_url,
        region_name=region,
        processor=processor,
        create_stream=create,
        create_stream_shards=1,
    )

    async with producer:
        if data is not None:
            record = _parse_input(data, processor_name)
            await producer.put(record, partition_key=partition_key)
        else:
            if sys.stdin.isatty():
                click.echo("Reading from stdin (Ctrl+D to finish)...", err=True)
            for line in sys.stdin:
                line = line.rstrip("\n")
                if not line:
                    continue
                record = _parse_input(line, processor_name)
                await producer.put(record, partition_key=partition_key)

        await producer.flush()

    click.echo("Done.", err=True)


def _parse_input(text, processor_name):
    """Parse input text based on the processor type."""
    if processor_name == "json":
        return json.loads(text)
    return text
