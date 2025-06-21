#!/usr/bin/env python3
"""
Async Kinesis Benchmark Tool

This tool benchmarks the async-kinesis library against real AWS Kinesis streams.

IMPORTANT: This will create temporary Kinesis streams prefixed with 'test-'.
The tool ensures proper cleanup of all created resources.

Usage:
    python benchmark.py [options]

Examples:
    # Run with default settings
    python benchmark.py

    # Dry run (no AWS resources created)
    python benchmark.py --dry-run

    # Custom test parameters
    python benchmark.py --records 10000 --shards 2 --iterations 3

    # Test specific processors
    python benchmark.py --processors json msgpack
"""

import argparse
import asyncio
import atexit
import copy
import itertools
import logging
import math
import signal
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import coloredlogs
import humanize
from contexttimer import Timer
from terminaltables import AsciiTable

from kinesis import (
    Consumer,
    JsonLineProcessor,
    JsonListProcessor,
    JsonProcessor,
    MsgpackProcessor,
    Producer,
    StringProcessor,
)

try:
    from kinesis import KPLJsonProcessor, KPLStringProcessor

    HAS_KPL = True
except ImportError:
    HAS_KPL = False

from mimesis import Address, Datetime, Person

# Configure logging
coloredlogs.install(level="INFO", fmt="%(asctime)s %(name)s[%(process)d] %(levelname)s %(message)s")

logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("kinesis.consumer").setLevel(logging.INFO)
logging.getLogger("kinesis.checkpointers").setLevel(logging.INFO)

log = logging.getLogger(__name__)

# Global registry of created streams for cleanup
CREATED_STREAMS = set()
CLEANUP_IN_PROGRESS = False


class StreamManager:
    """Manages Kinesis streams with automatic cleanup"""

    def __init__(self, stream_name: str, shards: int = 1):
        self.stream_name = stream_name
        self.shards = shards
        self.created = False

    async def __aenter__(self):
        """Create the stream on entry"""
        global CREATED_STREAMS

        try:
            # Create a test producer with stream creation enabled
            async with Producer(
                stream_name=self.stream_name,
                create_stream=True,
                create_stream_shards=self.shards,
            ) as producer:
                self.created = True
                CREATED_STREAMS.add(self.stream_name)
                log.info(f"Created Kinesis stream: {self.stream_name} with {self.shards} shard(s)")

                # Wait for stream to become active
                await self._wait_for_stream_active(producer)

        except Exception as e:
            log.error(f"Failed to create stream {self.stream_name}: {e}")
            raise

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Delete the stream on exit"""
        await self.cleanup()

    async def _wait_for_stream_active(self, producer, max_wait: int = 120):
        """Wait for stream to become active"""
        start_time = time.time()
        while time.time() - start_time < max_wait:
            try:
                response = await producer.client.describe_stream(StreamName=self.stream_name)
                status = response["StreamDescription"]["StreamStatus"]
                if status == "ACTIVE":
                    log.info(f"Stream {self.stream_name} is now ACTIVE")
                    return
                elif status in ["DELETING", "DELETED"]:
                    raise Exception(f"Stream {self.stream_name} is being deleted")
                log.debug(f"Stream {self.stream_name} status: {status}, waiting...")
            except Exception as e:
                log.debug(f"Error checking stream status: {e}")

            await asyncio.sleep(3)

        raise Exception(f"Stream {self.stream_name} did not become active within {max_wait} seconds")

    async def cleanup(self):
        """Clean up the stream"""
        global CREATED_STREAMS, CLEANUP_IN_PROGRESS

        if not self.created or self.stream_name not in CREATED_STREAMS:
            return

        if CLEANUP_IN_PROGRESS:
            return

        try:
            log.info(f"Deleting Kinesis stream: {self.stream_name}")
            async with Producer(stream_name=self.stream_name) as producer:
                await producer.client.delete_stream(StreamName=self.stream_name)
                CREATED_STREAMS.discard(self.stream_name)
                log.info(f"Successfully deleted stream: {self.stream_name}")
        except Exception as e:
            if "ResourceNotFoundException" not in str(e):
                log.error(f"Error deleting stream {self.stream_name}: {e}")


async def cleanup_all_streams():
    """Clean up all created streams"""
    global CREATED_STREAMS, CLEANUP_IN_PROGRESS

    if CLEANUP_IN_PROGRESS:
        return

    CLEANUP_IN_PROGRESS = True

    if CREATED_STREAMS:
        log.info(f"Cleaning up {len(CREATED_STREAMS)} stream(s)...")
        for stream_name in list(CREATED_STREAMS):
            try:
                async with Producer(stream_name=stream_name) as producer:
                    await producer.client.delete_stream(StreamName=stream_name)
                    log.info(f"Deleted stream: {stream_name}")
            except Exception as e:
                if "ResourceNotFoundException" not in str(e):
                    log.error(f"Error deleting stream {stream_name}: {e}")

        CREATED_STREAMS.clear()

    CLEANUP_IN_PROGRESS = False


def generate_random_data(size_kb: float = 1.0) -> Dict[str, Any]:
    """Generate random data of approximately the specified size in KB"""
    p = Person()
    a = Address()

    base_data = {
        "name": p.full_name(),
        "email": p.email(),
        "nationality": p.nationality(),
        "occupation": p.occupation(),
        "password": p.password(),
        "phone": p.telephone(),
        "address": a.address(),
        "city": a.city(),
        "street_no": a.street_number(),
        "created": Datetime().date().isoformat(),
    }

    # Calculate current size and pad if needed
    import json

    current_size = len(json.dumps(base_data).encode("utf-8"))
    target_size = int(size_kb * 1024)

    if current_size < target_size:
        # Add padding to reach target size
        padding_size = target_size - current_size - 20  # Account for JSON overhead
        if padding_size > 0:
            base_data["padding"] = "x" * padding_size

    return base_data


def generate_dataset(n: int, size_kb: float = 1.0) -> List[Dict[str, Any]]:
    """Generate a dataset with n records"""
    return [generate_random_data(size_kb) for _ in range(n)]


def copy_dataset(data: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    """Create n copies of the dataset"""
    return list(itertools.chain.from_iterable([copy.copy(data) for _ in range(n)]))


async def test_producer(
    stream_name: str, data: List[Dict[str, Any]], processor: Any, dry_run: bool = False
) -> Tuple[bool, Optional[float], Dict[str, Any]]:
    """Test a producer with the given processor"""

    log.info(f"Testing with {processor.__class__.__name__}")

    if dry_run:
        log.info("DRY RUN: Simulating producer/consumer operations")
        # Simulate processing time
        await asyncio.sleep(0.1)
        return True, 0.1, {"records_written": len(data), "records_read": len(data)}

    stats = {
        "records_written": 0,
        "records_read": 0,
        "write_errors": 0,
        "read_errors": 0,
    }

    try:
        async with Producer(stream_name=stream_name, processor=processor, max_queue_size=100000) as producer:

            async with Consumer(
                stream_name=stream_name,
                processor=processor,
                max_queue_size=100000,
                iterator_type="LATEST",
            ) as consumer:

                # Ensure consumer is set up before producing
                await consumer.start_consumer(wait_iterations=0)

                # Add small delay to ensure consumer is ready
                await asyncio.sleep(1)

                with Timer() as t:
                    # Write records
                    for item in data:
                        try:
                            await producer.put(item)
                            stats["records_written"] += 1
                        except Exception as e:
                            log.error(f"Error writing record: {e}")
                            stats["write_errors"] += 1

                    await producer.flush()

                    # Read records
                    total_read = 0
                    empty_iterations = 0
                    max_empty_iterations = 5

                    while total_read < len(data) and empty_iterations < max_empty_iterations:
                        records_in_iteration = 0
                        async for record in consumer:
                            total_read += 1
                            records_in_iteration += 1
                            stats["records_read"] += 1

                            if total_read >= len(data):
                                break

                        if records_in_iteration == 0:
                            empty_iterations += 1
                            await asyncio.sleep(1)
                        else:
                            empty_iterations = 0

        if stats["records_read"] != stats["records_written"]:
            log.error(f"Record mismatch: expected {stats['records_written']}, " f"read {stats['records_read']}")
            return False, None, stats

        log.info(f"Completed {stats['records_written']} records in {round(t.elapsed, 2)} seconds")

        return True, round(t.elapsed, 2), stats

    except Exception as e:
        log.error(f"Test failed: {e}")
        return False, None, stats


async def run_benchmark(args):
    """Run the benchmark with the specified arguments"""

    # Generate unique stream name with timestamp
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    stream_name = f"test-async-kinesis-benchmark-{timestamp}"

    log.info(f"Starting benchmark with stream: {stream_name}")

    # Calculate dataset parameters
    n_records = args.records
    sample_size = min(500, n_records)  # Use smaller sample for large datasets
    data = generate_dataset(sample_size, args.record_size_kb)

    multiplier = math.ceil(n_records / sample_size)

    # Calculate data sizes
    python_bytes = sum([sys.getsizeof(x) for x in data]) * multiplier

    # Select processors to test
    available_processors = {
        "string": StringProcessor,
        "json": JsonProcessor,
        "jsonline": JsonLineProcessor,
        "jsonlist": JsonListProcessor,
        "msgpack": MsgpackProcessor,
    }

    if HAS_KPL:
        available_processors["kpljson"] = KPLJsonProcessor
        available_processors["kplstring"] = KPLStringProcessor

    if args.processors:
        selected_processors = []
        for name in args.processors:
            if name in available_processors:
                selected_processors.append(available_processors[name]())
            else:
                log.warning(f"Unknown processor: {name}")
    else:
        selected_processors = [cls() for cls in available_processors.values()]

    # Run benchmarks
    results = []

    if not args.dry_run:
        # Create the stream
        async with StreamManager(stream_name, args.shards) as stream:

            for iteration in range(args.iterations):
                if args.iterations > 1:
                    log.info(f"\n=== Iteration {iteration + 1} of {args.iterations} ===")

                for processor in selected_processors:
                    all_data = copy_dataset(data, multiplier)

                    # Calculate aggregated size
                    aggregator_bytes = 0
                    for x in data:
                        for size, _, _ in processor.add_item(x):
                            aggregator_bytes += size

                    if processor.has_items():
                        for size, _, _ in processor.get_items():
                            aggregator_bytes += size

                    aggregator_bytes *= multiplier

                    success, elapsed_ts, stats = await test_producer(
                        stream_name=stream_name,
                        data=all_data,
                        processor=processor,
                        dry_run=args.dry_run,
                    )

                    if success and elapsed_ts:
                        results.append(
                            [
                                processor.__class__.__name__,
                                iteration + 1,
                                humanize.naturalsize(python_bytes),
                                humanize.naturalsize(aggregator_bytes),
                                elapsed_ts,
                                round(n_records / elapsed_ts) if elapsed_ts > 0 else 0,
                                (humanize.naturalsize(python_bytes / elapsed_ts) if elapsed_ts > 0 else "0 B"),
                                (humanize.naturalsize(aggregator_bytes / elapsed_ts) if elapsed_ts > 0 else "0 B"),
                            ]
                        )

                    # Pause between tests
                    await asyncio.sleep(2)
    else:
        # Dry run mode
        for processor in selected_processors:
            success, elapsed_ts, stats = await test_producer(
                stream_name=stream_name, data=[], processor=processor, dry_run=True
            )

            results.append(
                [
                    processor.__class__.__name__,
                    1,
                    humanize.naturalsize(python_bytes),
                    "N/A (dry run)",
                    "N/A",
                    "N/A",
                    "N/A",
                    "N/A",
                ]
            )

    # Display results
    print(f"\n\nResults for {n_records} records with {args.shards} shard(s):\n")

    headers = [
        "Processor",
        "Iteration",
        "Python Bytes",
        "Kinesis Bytes",
        "Time (s)",
        "Records/s",
        "Python MB/s",
        "Kinesis MB/s",
    ]

    table = AsciiTable([headers] + results)
    print(table.table)

    # Generate markdown table for easy copying
    if args.markdown:
        print("\n\nMarkdown format:\n")
        print("| " + " | ".join(headers) + " |")
        print("| " + " | ".join(["---"] * len(headers)) + " |")
        for row in results:
            print("| " + " | ".join(str(x) for x in row) + " |")

    print("\n")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Benchmark async-kinesis library with AWS Kinesis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--records",
        "-n",
        type=int,
        default=50000,
        help="Number of records to test (default: 50000)",
    )

    parser.add_argument(
        "--record-size-kb",
        "-s",
        type=float,
        default=1.0,
        help="Approximate size of each record in KB (default: 1.0)",
    )

    parser.add_argument("--shards", type=int, default=1, help="Number of shards to create (default: 1)")

    parser.add_argument(
        "--iterations",
        "-i",
        type=int,
        default=1,
        help="Number of iterations to run (default: 1)",
    )

    parser.add_argument(
        "--processors",
        "-p",
        nargs="+",
        choices=[
            "string",
            "json",
            "jsonline",
            "jsonlist",
            "msgpack",
            "kpljson",
            "kplstring",
        ],
        help="Processors to test (default: all available)",
    )

    parser.add_argument("--dry-run", action="store_true", help="Run without creating AWS resources")

    parser.add_argument(
        "--markdown",
        "-m",
        action="store_true",
        help="Also output results as markdown table",
    )

    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    if args.verbose:
        coloredlogs.install(level="DEBUG")
        logging.getLogger("kinesis.consumer").setLevel(logging.DEBUG)

    # Set up cleanup handlers
    def cleanup_handler(signum=None, frame=None):
        """Handle cleanup on exit"""
        try:
            loop = asyncio.get_running_loop()
            asyncio.create_task(cleanup_all_streams())
        except RuntimeError:
            # No running loop, create one
            asyncio.run(cleanup_all_streams())

        if signum:
            sys.exit(1)

    # Register cleanup handlers
    atexit.register(cleanup_handler)
    signal.signal(signal.SIGINT, cleanup_handler)
    signal.signal(signal.SIGTERM, cleanup_handler)

    try:
        # Run the benchmark
        asyncio.run(run_benchmark(args))

        # Clean up
        asyncio.run(cleanup_all_streams())

    except KeyboardInterrupt:
        log.info("\nBenchmark interrupted by user")
        asyncio.run(cleanup_all_streams())
        sys.exit(1)
    except Exception as e:
        log.error(f"Benchmark failed: {e}")
        asyncio.run(cleanup_all_streams())
        sys.exit(1)


if __name__ == "__main__":
    main()
