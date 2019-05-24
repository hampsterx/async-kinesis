"""
    pip install -r benchmark-requirements.txt

    Note: This will create a shard called "test" on your AWS.
    Your responsibility to delete it afterwards!!


"""
import asyncio
import math
import logging
import coloredlogs
import copy
import itertools
import sys
import humanize
from terminaltables import AsciiTable
from contexttimer import Timer
from kinesis import (
    Producer,
    Consumer,
    JsonProcessor,
    JsonLineProcessor,
    MsgpackProcessor,
)
from mimesis import Person, Address, Datetime

coloredlogs.install(level="DEBUG")

logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("kinesis.consumer").setLevel(logging.DEBUG)
logging.getLogger("kinesis.checkpointers").setLevel(logging.INFO)

log = logging.getLogger(__name__)


def generate_random_data():
    p = Person()
    a = Address()

    return {
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


def generate_dataset(n):
    return [generate_random_data() for _ in range(n)]


def copy_dataset(data, n):
    return list(itertools.chain.from_iterable([copy.copy(data) for _ in range(n)]))


async def test_producer(data, processor):
    log.info("Testing with {}".format(processor.__class__.__name__))
    async with Producer(
        stream_name="test", processor=processor, max_queue_size=100000
    ) as producer:

        await producer.create_stream(shards=1, ignore_exists=True)

        async with Consumer(
            stream_name="test",
            processor=processor,
            max_queue_size=100000,
            iterator_type="LATEST",
        ) as consumer:

            # ensure set up before producer puts records as using LATEST
            await consumer.start_consumer(wait_iterations=0)

            with Timer() as t:
                for item in data:
                    await producer.put(item)
                await producer.flush()

            total = 0
            while total < len(data):
                async for _ in consumer:
                    total += 1

    if len(data) != total:
        log.error(
            "Failed to read all records.. expected {} read {}".format(len(data), total)
        )
        return False, None

    log.info(
        "Completed {} records (read: {}) in {} seconds".format(
            len(data), total, round(t.elapsed, 2)
        )
    )

    return True, round(t.elapsed, 2)


async def test():
    n = 50000

    data = generate_dataset(500)

    multiplier = math.ceil(n / 500)

    python_bytes = sum([sys.getsizeof(x) for x in data]) * multiplier

    result = []

    for processor in [JsonLineProcessor(), MsgpackProcessor()]: # JsonProcessor()

        all_data = copy_dataset(data, multiplier)

        aggregator_bytes = 0
        for x in data:
            for size, _, _ in processor.add_item(x):
                aggregator_bytes += size

        if processor.has_items():
            for size, _, _ in processor.get_items():
                aggregator_bytes += size

        aggregator_bytes *= multiplier

        success, elapsed_ts = await test_producer(data=all_data, processor=processor)

        if success:
            result.append(
                [
                    processor.__class__.__name__,
                    humanize.naturalsize(python_bytes),
                    humanize.naturalsize(aggregator_bytes),
                    elapsed_ts,
                    round(n / elapsed_ts),
                    humanize.naturalsize(python_bytes / elapsed_ts),
                    humanize.naturalsize(aggregator_bytes / elapsed_ts),
                ]
            )

            # Pause a bit
            await asyncio.sleep(2)

    print("\n\n Results for {} records:\n".format(n))
    print(
        AsciiTable(
            [
                [
                    "Aggregator",
                    "Python Bytes",
                    "Kinesis Bytes",
                    "Time (Seconds)",
                    "RPS",
                    "Python BPS",
                    "Kinesis BPS",
                ]
            ]
            + result
        ).table
    )
    print("\n")


asyncio.run(test())
