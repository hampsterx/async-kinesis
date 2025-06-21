import asyncio
import logging
import os
import uuid
from unittest import TestCase, skipUnless

import coloredlogs
from aiobotocore.session import AioSession
from asynctest import TestCase as AsynTestCase
from asynctest import fail_on
from dotenv import load_dotenv

from kinesis import Consumer, MemoryCheckPointer, Producer, RedisCheckPointer, exceptions
from kinesis.aggregators import (
    Aggregator,
    KPLAggregator,
    ListAggregator,
    NetstringAggregator,
    NewlineAggregator,
    OutputItem,
    SimpleAggregator,
)
from kinesis.processors import (
    JsonLineProcessor,
    JsonListProcessor,
    JsonProcessor,
    MsgpackProcessor,
    Processor,
    StringProcessor,
)
from kinesis.serializers import JsonSerializer, Serializer, StringSerializer

coloredlogs.install(level="DEBUG", fmt="%(name)s %(levelname)s %(message)s")

logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("aiobotocore").setLevel(logging.INFO)


log = logging.getLogger(__name__)

load_dotenv()

# https://github.com/mhart/kinesalite
# ./node_modules/.bin/kinesalite --shardLimit 1000
# see also docker-compose.yaml
ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4567")

TESTING_USE_AWS_KINESIS = os.environ.get("TESTING_USE_AWS_KINESIS", "0") == "1"

# Use docker-compose one
if "REDIS_PORT" not in os.environ:
    os.environ["REDIS_PORT"] = "16379"


class BaseTests:
    def random_string(self, length):
        from random import choice
        from string import ascii_uppercase

        return "".join(choice(ascii_uppercase) for i in range(length))


class BaseKinesisTests(AsynTestCase, BaseTests):
    async def setUp(self):
        self.stream_name = "test_{}".format(str(uuid.uuid4())[0:8])
        producer = await Producer(
            stream_name=self.stream_name,
            endpoint_url=ENDPOINT_URL,
            create_stream=self.stream_name,
            create_stream_shards=1,
        ).__aenter__()
        await producer.__aexit__(None, None, None)

    async def add_record_delayed(self, msg, producer, delay):
        log.debug("Adding record. delay={}".format(delay))
        await asyncio.sleep(delay)
        await producer.put(msg)


class ProcessorAndAggregatorTests(TestCase, BaseTests):
    """
    Processor and Aggregator Tests
    """

    def test_aggregator_min_size(self):

        with self.assertRaises(exceptions.ValidationError):
            Aggregator(max_size=20)

    def test_aggregator_max_size(self):

        with self.assertRaises(exceptions.ValidationError):
            Aggregator(max_size=2000)

    def test_processor_exceed_put_limit(self):
        processor = StringProcessor()

        with self.assertRaises(exceptions.ExceededPutLimit):
            list(processor.add_item(self.random_string(1024 * 1024 + 1)))

    def test_newline_aggregator(self):

        # in reality does not make sense as strings can contain new lines
        # so is not a suitable combination to use
        class NewlineTestProcessor(NewlineAggregator, StringSerializer):
            pass

        processor = NewlineTestProcessor()

        # Expect nothing as batching
        self.assertEqual([], list(processor.add_item(123)))
        self.assertEqual([], list(processor.add_item("test")))

        self.assertTrue(processor.has_items())

        output = list(processor.get_items())

        self.assertEqual(len(output), 1)

        self.assertEqual(output[0].size, 9)
        self.assertEqual(output[0].n, 2)
        self.assertEqual(output[0].data, b"123\ntest\n")

        self.assertListEqual(list(processor.parse(output[0].data)), ["123", "test"])

    def test_list_aggregator(self):
        class JsonListTestProcessor(ListAggregator, JsonSerializer):
            pass

        processor = JsonListTestProcessor()

        # Expect nothing as batching
        self.assertEqual([], list(processor.add_item(123)))
        self.assertEqual([], list(processor.add_item("test")))

        self.assertTrue(processor.has_items())

        output = list(processor.get_items())

        self.assertEqual(len(output), 1)

        self.assertEqual(output[0].size, 11)
        self.assertEqual(output[0].n, 2)
        self.assertEqual(output[0].data, b'[123, "test"]')

        self.assertListEqual(next(processor.parse(output[0].data)), [123, "test"])

    def test_netstring_aggregator(self):
        class NetstringTestProcessor(NetstringAggregator, StringSerializer):
            pass

        processor = NetstringTestProcessor()

        # Expect nothing as batching
        self.assertEqual([], list(processor.add_item(123)))
        self.assertEqual([], list(processor.add_item("test")))

        self.assertTrue(processor.has_items())

        output = list(processor.get_items())

        self.assertEqual(len(output), 1)

        self.assertEqual(output[0].size, 13)
        self.assertEqual(output[0].n, 2)
        self.assertEqual(output[0].data, b"3:123,4:test,")

        self.assertListEqual(list(processor.parse(output[0].data)), ["123", "test"])

    def test_kpl_aggregator(self):
        class KPLTestProcessor(KPLAggregator, StringSerializer):
            pass

        processor = KPLTestProcessor()

        # Expect nothing as batching
        self.assertEqual([], list(processor.add_item(123)))
        self.assertEqual([], list(processor.add_item("test")))

        self.assertTrue(processor.has_items())

        output = list(processor.get_items())

        self.assertEqual(len(output), 1)

        self.assertEqual(output[0].n, 2)

        self.assertListEqual(list(processor.parse(output[0].data)), ["123", "test"])

    def test_kpl_aggregator_max_size(self):
        class BytesSerializer:
            def serialize(self, item):
                return item

            def deserialize(self, data):
                return data

        class KPLTestProcessor(KPLAggregator, BytesSerializer):
            pass

        # 100 K max_size
        processor = KPLTestProcessor(max_size=1024 * 100)

        # Expect nothing as batching first two 40K records
        self.assertEqual([], list(processor.add_item(bytes(40 * 1024))))
        self.assertEqual([], list(processor.add_item(bytes(40 * 1024))))

        # output as we exceed
        output = list(processor.add_item(bytes(40 * 1024)))

        self.assertEqual(len(output), 1)

        self.assertEqual(output[0].n, 2)

    def test_string_processor(self):

        processor = StringProcessor()

        self.assertEquals(processor.max_bytes, 1024 * 25 * 40)

        output = list(processor.add_item("test"))

        self.assertEqual(len(output), 1)
        self.assertIsInstance(output[0], OutputItem)

        self.assertEqual(output[0].size, len("test"))
        self.assertEqual(output[0].n, 1)
        self.assertEqual(output[0].data, b"test")

        self.assertFalse(processor.has_items())

    def test_json_processor(self):

        processor = JsonProcessor()

        output = list(processor.add_item({"test": 123}))

        self.assertEqual(len(output), 1)
        self.assertIsInstance(output[0], OutputItem)

        self.assertEqual(output[0].size, 13)
        self.assertEqual(output[0].n, 1)
        self.assertEqual(output[0].data, b'{"test": 123}')

        self.assertFalse(processor.has_items())

        self.assertListEqual(list(processor.parse(output[0].data)), [{"test": 123}])

    def test_json_line_processor(self):

        processor = JsonLineProcessor(max_size=25)

        # Expect nothing as batching
        self.assertEqual([], list(processor.add_item({"test": 123})))
        self.assertEqual([], list(processor.add_item({"test": 456})))

        self.assertTrue(processor.has_items())

        output = list(processor.get_items())

        self.assertEqual(len(output), 1)

        self.assertEqual(output[0].size, 28)
        self.assertEqual(output[0].n, 2)
        self.assertEqual(output[0].data, b'{"test": 123}\n{"test": 456}\n')

        self.assertListEqual(
            list(processor.parse(output[0].data)),
            [{"test": 123}, {"test": 456}],
        )

        # Expect empty now
        self.assertFalse(processor.has_items())

        result = []
        for x in range(1000):
            output = list(processor.add_item({"test": "test with some more data"}))
            if output:
                self.assertEqual(len(output), 1)
                result.append(output[0])

        # Expected at least one record to be output
        self.assertEqual(len(result), 1)

        self.assertEqual(result[0].size, 25567)  # expect below 25*1024=25600
        self.assertEqual(result[0].n, 691)

        # Expect some left
        self.assertTrue(processor.has_items())

        output = list(processor.get_items())

        self.assertEqual(len(output), 1)

        self.assertEqual(output[0].size, 11432)
        self.assertEqual(output[0].n, 309)

        self.assertFalse(processor.has_items())

    def test_json_list_processor(self):

        processor = JsonListProcessor(max_size=25)

        # Expect nothing as batching
        self.assertEqual([], list(processor.add_item({"test": 123})))
        self.assertEqual([], list(processor.add_item({"test": 456})))

        self.assertTrue(processor.has_items())

        output = list(processor.get_items())

        self.assertEqual(len(output), 1)

        self.assertEqual(output[0].size, 28)
        self.assertEqual(output[0].n, 2)
        self.assertEqual(output[0].data, b'[{"test": 123}, {"test": 456}]')

        # Need to use next() otherwise list() creates double nested list
        self.assertListEqual(next(processor.parse(output[0].data)), [{"test": 123}, {"test": 456}])

        # Expect empty now
        self.assertFalse(processor.has_items())

        result = []
        for x in range(1000):
            output = list(processor.add_item({"test": "test with some more data"}))
            if output:
                self.assertEqual(len(output), 1)
                result.append(output[0])

        # Expected at least one record to be output
        self.assertEqual(len(result), 1)

        self.assertEqual(result[0].size, 25567)  # expect below 25*1024=25600
        self.assertEqual(result[0].n, 691)

        # Expect some left
        self.assertTrue(processor.has_items())

        output = list(processor.get_items())

        self.assertEqual(len(output), 1)

        self.assertEqual(output[0].size, 11432)
        self.assertEqual(output[0].n, 309)

        self.assertFalse(processor.has_items())

    def test_msgpack_processor(self):

        processor = MsgpackProcessor(max_size=25)

        # Expect nothing as batching
        self.assertEqual([], list(processor.add_item({"test": 123})))
        self.assertEqual([], list(processor.add_item({"test": 456})))

        self.assertTrue(processor.has_items())

        output = list(processor.get_items())

        self.assertEqual(len(output), 1)

        self.assertEqual(output[0].size, 22)
        self.assertEqual(output[0].n, 2)
        self.assertEqual(output[0].data, b"7:\x81\xa4test{,9:\x81\xa4test\xcd\x01\xc8,")

        self.assertListEqual(list(processor.parse(output[0].data)), [{"test": 123}, {"test": 456}])

        # Expect empty now
        self.assertFalse(processor.has_items())

        result = []
        for x in range(1000):
            output = list(processor.add_item({"test": "test with some more data"}))
            if output:
                self.assertEqual(len(output), 1)
                result.append(output[0])

        # Expected at least one record to be output
        self.assertEqual(len(result), 1)

        self.assertEqual(result[0].size, 25585)  # expect below 25*1024=25600
        self.assertEqual(result[0].n, 731)

        # Expect some left
        self.assertTrue(processor.has_items())

        output = list(processor.get_items())

        self.assertEqual(len(output), 1)

        self.assertEqual(output[0].size, 9411)
        self.assertEqual(output[0].n, 269)

        self.assertFalse(processor.has_items())


class CheckpointTests(BaseKinesisTests):
    """
    Checkpoint Tests
    """

    @classmethod
    def patch_consumer_fetch(cls, consumer):
        async def get_shard_iterator(shard_id, last_sequence_number=None):
            log.info("getting shard iterator for {} @ {}".format(shard_id, last_sequence_number))
            return True

        consumer.get_shard_iterator = get_shard_iterator

        async def get_records(shard):
            log.info("get records shard={}".format(shard["ShardId"]))
            return {}

        consumer.get_records = get_records

        consumer.is_fetching = True

    async def test_memory_checkpoint(self):
        # first consumer
        checkpointer = MemoryCheckPointer(name="test")

        consumer_a = Consumer(
            stream_name=None,
            checkpointer=checkpointer,
            max_shard_consumers=1,
            endpoint_url=ENDPOINT_URL,
        )

        self.patch_consumer_fetch(consumer_a)

        consumer_a.shards = [{"ShardId": "test-1"}, {"ShardId": "test-2"}]

        await consumer_a.fetch()

        shards = [s["ShardId"] for s in consumer_a.shards if s.get("stats")]

        # Expect only one shard assigned as max = 1
        self.assertEqual(["test-1"], shards)

        # second consumer (note: max_shard_consumers needs to be 2 as uses checkpointer to get allocated shards)

        consumer_b = Consumer(
            stream_name=None,
            checkpointer=checkpointer,
            max_shard_consumers=2,
            endpoint_url=ENDPOINT_URL,
        )

        self.patch_consumer_fetch(consumer_b)

        consumer_b.shards = [{"ShardId": "test-1"}, {"ShardId": "test-2"}]

        await consumer_b.fetch()

        shards = [s["ShardId"] for s in consumer_b.shards if s.get("stats")]

        # Expect only one shard assigned as max = 1
        self.assertEqual(["test-2"], shards)

    async def test_redis_checkpoint_locking(self):
        name = "test-{}".format(str(uuid.uuid4())[0:8])

        # first consumer
        checkpointer_a = RedisCheckPointer(name=name, id="proc-1")

        # second consumer
        checkpointer_b = RedisCheckPointer(name=name, id="proc-2")

        # try to allocate the same shard

        result = await asyncio.gather(*[checkpointer_a.allocate("test"), checkpointer_b.allocate("test")])

        result = list(sorted([x[0] for x in result]))

        # Expect only one to have succeeded
        self.assertEquals([False, True], result)

        await checkpointer_a.close()
        await checkpointer_b.close()

    async def test_redis_checkpoint_reallocate(self):
        name = "test-{}".format(str(uuid.uuid4())[0:8])

        # first consumer
        checkpointer_a = RedisCheckPointer(name=name, id="proc-1")

        await checkpointer_a.allocate("test")

        # checkpoint
        await checkpointer_a.checkpoint("test", "123")

        # stop on this shard
        await checkpointer_a.deallocate("test")

        # second consumer
        checkpointer_b = RedisCheckPointer(name=name, id="proc-2")

        success, sequence = await checkpointer_b.allocate("test")

        self.assertTrue(success)
        self.assertEquals("123", sequence)

        await checkpointer_b.close()

        self.assertEquals(checkpointer_b.get_all_checkpoints(), {})

        await checkpointer_a.close()

    async def test_redis_checkpoint_hearbeat(self):
        name = "test-{}".format(str(uuid.uuid4())[0:8])

        checkpointer = RedisCheckPointer(name=name, heartbeat_frequency=0.5)

        await checkpointer.allocate("test")
        await checkpointer.checkpoint("test", "123")

        await asyncio.sleep(1)

        await checkpointer.close()

        # nothing to assert
        self.assertTrue(True)


class KinesisTests(BaseKinesisTests):
    """
    Kinesalite Tests
    """

    async def test_stream_does_not_exist(self):

        await asyncio.sleep(2)

        # Producer
        with self.assertRaises(exceptions.StreamDoesNotExist):
            async with Producer(
                session=AioSession(),
                stream_name="test_stream_does_not_exist",
                endpoint_url=ENDPOINT_URL,
            ) as producer:
                await producer.put("test")

        # Consumer
        with self.assertRaises(exceptions.StreamDoesNotExist):
            async with Consumer(stream_name="test_stream_does_not_exist", endpoint_url=ENDPOINT_URL):
                pass

    @fail_on(unused_loop=True, active_handles=True)
    async def test_producer_put(self):
        async with Producer(stream_name=self.stream_name, endpoint_url=ENDPOINT_URL) as producer:
            await producer.put("test")

    async def test_producer_put_below_limit(self):
        async with Producer(
            stream_name=self.stream_name,
            processor=StringProcessor(),
            endpoint_url=ENDPOINT_URL,
        ) as producer:
            # The maximum size of the data payload of a record before base64-encoding is up to 1 MiB.
            # Limit is set in aggregators.BaseAggregator (few bytes short of 1MiB)
            await producer.put(self.random_string(40 * 25 * 1024))

    async def test_producer_put_exceed_batch_size(self):
        # Expect to complete by lowering batch size until successful (500 is max)
        async with Producer(stream_name=self.stream_name, endpoint_url=ENDPOINT_URL, batch_size=600) as producer:

            for x in range(1000):
                await producer.put("test")

    async def test_producer_and_consumer(self):

        async with Producer(stream_name=self.stream_name, endpoint_url=ENDPOINT_URL) as producer:
            pass

            async with Consumer(stream_name=self.stream_name, endpoint_url=ENDPOINT_URL):
                pass

    async def test_producer_and_consumer_consume_from_start_flush(self):
        async with Producer(stream_name=self.stream_name, endpoint_url=ENDPOINT_URL) as producer:

            await producer.put({"test": 123})

            await producer.flush()

            results = []

            async with Consumer(stream_name=self.stream_name, endpoint_url=ENDPOINT_URL) as consumer:
                async for item in consumer:
                    results.append(item)

            # Expect to have consumed from start as default iterator_type=TRIM_HORIZON
            self.assertEquals([{"test": 123}], results)

    async def test_producer_and_consumer_consume_from_start_after(self):

        # Don't flush, close producer immediately to test all data is written to stream on exit.
        async with Producer(
            stream_name=self.stream_name,
            endpoint_url=ENDPOINT_URL,
            processor=StringProcessor(),
        ) as producer:
            # Put enough data to ensure it will require more than one put
            # ie test overflow behaviour
            for _ in range(15):
                await producer.put(self.random_string(100 * 1024))

        results = []

        async with Consumer(
            stream_name=self.stream_name,
            endpoint_url=ENDPOINT_URL,
            processor=StringProcessor(),
        ) as consumer:
            async for item in consumer:
                results.append(item)

        # Expect to have consumed from start as default iterator_type=TRIM_HORIZON
        self.assertEquals(len(results), 15)

    async def test_producer_and_consumer_consume_with_json_line_aggregator(self):

        processor = JsonLineProcessor()

        async with Producer(stream_name=self.stream_name, endpoint_url=ENDPOINT_URL, processor=processor) as producer:

            for x in range(0, 10):
                await producer.put({"test": x})

            await producer.flush()

            results = []

            async with Consumer(
                stream_name=self.stream_name,
                endpoint_url=ENDPOINT_URL,
                processor=processor,
            ) as consumer:
                async for item in consumer:
                    results.append(item)

            # Expect to have consumed from start as default iterator_type=TRIM_HORIZON

            self.assertEqual(len(results), 10)

            self.assertEquals(results[0], {"test": 0})
            self.assertEquals(results[-1], {"test": 9})

    async def test_producer_and_consumer_consume_with_msgpack_aggregator(self):

        processor = MsgpackProcessor()

        async with Producer(stream_name=self.stream_name, endpoint_url=ENDPOINT_URL, processor=processor) as producer:

            for x in range(0, 10):
                await producer.put({"test": x})

            await producer.flush()

            results = []

            async with Consumer(
                stream_name=self.stream_name,
                endpoint_url=ENDPOINT_URL,
                processor=processor,
            ) as consumer:
                async for item in consumer:
                    results.append(item)

            # Expect to have consumed from start as default iterator_type=TRIM_HORIZON

            self.assertEqual(len(results), 10)

            self.assertEquals(results[0], {"test": 0})
            self.assertEquals(results[-1], {"test": 9})

    async def test_producer_and_consumer_consume_with_bytes(self):
        class ByteSerializer(Serializer):
            def serialize(self, msg):
                result = str.encode(msg)
                return result

            def deserialize(self, data):
                return data

        class ByteProcessor(Processor, NetstringAggregator, ByteSerializer):
            pass

        processor = ByteProcessor()

        async with Producer(stream_name=self.stream_name, endpoint_url=ENDPOINT_URL, processor=processor) as producer:

            for x in range(0, 2):
                await producer.put(f"{x}")

            await producer.flush()

            results = []

            checkpointer = MemoryCheckPointer(name="test")

            async with Consumer(
                stream_name=self.stream_name,
                endpoint_url=ENDPOINT_URL,
                processor=processor,
                checkpointer=checkpointer,
            ) as consumer:
                async for item in consumer:
                    results.append(item)
                    await checkpointer.checkpoint(shard_id=consumer.shards[0]["ShardId"], sequence="seq")

                async for item in consumer:
                    results.append(item)

            self.assertEquals(len(results), 2)

            await checkpointer.close()

            self.assertEquals(len(checkpointer.get_all_checkpoints()), 1)

    async def test_producer_and_consumer_consume_queue_full(self):
        async with Producer(stream_name=self.stream_name, endpoint_url=ENDPOINT_URL) as producer:

            for i in range(0, 100):
                await producer.put("test")

            await producer.flush()

            results = []

            async with Consumer(
                stream_name=self.stream_name,
                endpoint_url=ENDPOINT_URL,
                max_queue_size=20,
            ) as consumer:

                async for item in consumer:
                    results.append(item)

            # Expect 20 only as queue is full and we don't wait on queue
            self.assertEqual(20, len(results))

    async def test_producer_and_consumer_consume_throttle(self):
        async with Producer(stream_name=self.stream_name, endpoint_url=ENDPOINT_URL) as producer:

            for i in range(0, 100):
                await producer.put("test")

            await producer.flush()

            results = []

            async with Consumer(
                stream_name=self.stream_name,
                endpoint_url=ENDPOINT_URL,
                record_limit=10,
                # 2 per second
                shard_fetch_rate=2,
            ) as consumer:

                from datetime import datetime

                dt = datetime.now()

                while (datetime.now() - dt).total_seconds() < 3.05:
                    async for item in consumer:
                        results.append(item)

            # Expect 2*3*10 = 60  ie at most 6 iterations of 10 records
            self.assertGreaterEqual(len(results), 50)
            self.assertLessEqual(len(results), 70)

    async def test_producer_and_consumer_consume_with_checkpointer_and_latest(self):
        async with Producer(stream_name=self.stream_name, endpoint_url=ENDPOINT_URL) as producer:

            await producer.put("test.A")

            results = []

            checkpointer = MemoryCheckPointer(name="test")

            async with Consumer(
                stream_name=self.stream_name,
                endpoint_url=ENDPOINT_URL,
                checkpointer=checkpointer,
                iterator_type="LATEST",
            ) as consumer:

                async for item in consumer:
                    results.append(item)

            # Expect none as LATEST
            self.assertEquals([], results)

            checkpoints = checkpointer.get_all_checkpoints()

            # Expect 1 as only 1 shard
            self.assertEquals(1, len(checkpoints))

            # none as no records yet (using LATEST)
            self.assertIsNone(checkpoints[list(checkpoints.keys())[0]]["sequence"])

            results = []

            log.info("checkpointer checkpoints: {}".format(checkpoints))

            log.info("Starting consumer again..")

            async with Consumer(
                stream_name=self.stream_name,
                endpoint_url=ENDPOINT_URL,
                checkpointer=checkpointer,
                iterator_type="LATEST",
                sleep_time_no_records=0.5,
            ) as consumer:

                # Manually start
                await consumer.start_consumer()

                await producer.put("test.B")

                await producer.flush()

                log.info("waiting..")

                await asyncio.sleep(1)

                log.info("about to consume..")

                async for item in consumer:
                    results.append(item)

            self.assertEquals(["test.B"], results)

            checkpoints = checkpointer.get_all_checkpoints()

            log.info("checkpointer checkpoints: {}".format(checkpoints))

            # expect not None as has processed records
            self.assertIsNotNone(checkpoints[list(checkpoints.keys())[0]]["sequence"])

            # now add some records
            for i in range(0, 10):
                await producer.put("test.{}".format(i))

            await producer.flush()

            await asyncio.sleep(1)

            results = []

            async with Consumer(
                stream_name=self.stream_name,
                endpoint_url=ENDPOINT_URL,
                checkpointer=checkpointer,
                iterator_type="LATEST",
                sleep_time_no_records=0.5,
            ) as consumer:

                async for item in consumer:
                    results.append(item)

            # Expect results as checkpointer resumed from prior sequence
            self.assertEquals(10, len(results))

    async def test_producer_and_consumer_consume_multiple_shards_with_redis_checkpointer(
        self,
    ):
        stream_name = "test_{}".format(str(uuid.uuid4())[0:8])
        async with Producer(
            stream_name=stream_name,
            endpoint_url=ENDPOINT_URL,
            create_stream=stream_name,
            create_stream_shards=2,
        ) as producer:

            for i in range(0, 100):
                await producer.put("test.{}".format(i))

            await producer.flush()

            results = []

            checkpointer = RedisCheckPointer(name="test-{}".format(str(uuid.uuid4())[0:8]), heartbeat_frequency=3)

            async with Consumer(
                stream_name=stream_name,
                endpoint_url=ENDPOINT_URL,
                checkpointer=checkpointer,
                record_limit=10,
            ) as consumer:

                # consumer will stop if no msgs
                for i in range(0, 6):
                    async for item in consumer:
                        results.append(item)
                    await asyncio.sleep(0.5)

                self.assertEquals(100, len(results))

                checkpoints = checkpointer.get_all_checkpoints()

                self.assertEquals(2, len(checkpoints))

                # Expect both shards to have been used/set
                for item in checkpoints.values():
                    self.assertIsNotNone(item)


class AWSKinesisTests(BaseKinesisTests):
    """
    AWS Kinesis Tests
    """

    STREAM_NAME_SINGLE_SHARD = "pykinesis-test-single-shard"
    STREAM_NAME_MULTI_SHARD = "pykinesis-test-multi-shard"

    forbid_get_event_loop = True

    @classmethod
    def setUpClass(cls):
        if not TESTING_USE_AWS_KINESIS:
            return

        log.info("Creating (or ignoring if exists) *Actual* Kinesis stream: {}".format(cls.STREAM_NAME_SINGLE_SHARD))

        async def create(stream_name, shards):
            async with Producer(stream_name=stream_name, create_stream=True, create_stream_shards=shards) as producer:
                await producer.start()

        asyncio.run(create(stream_name=cls.STREAM_NAME_SINGLE_SHARD, shards=1))

    @classmethod
    def tearDownClass(cls):
        if not TESTING_USE_AWS_KINESIS:
            return

        log.warning(
            "Don't forget to delete your $$ streams: {} and {}".format(
                cls.STREAM_NAME_SINGLE_SHARD, cls.STREAM_NAME_MULTI_SHARD
            )
        )

    @skipUnless(TESTING_USE_AWS_KINESIS, "Requires TESTING_USE_AWS_KINESIS flag to be set")
    async def test_consumer_checkpoint(self):

        checkpointer = MemoryCheckPointer(name="test")

        results = []

        async with Producer(
            stream_name=self.STREAM_NAME_SINGLE_SHARD,
            processor=StringProcessor(),
        ) as producer:

            async with Consumer(
                stream_name=self.STREAM_NAME_SINGLE_SHARD,
                checkpointer=checkpointer,
                processor=StringProcessor(),
                iterator_type="LATEST",
            ) as consumer:

                # Manually start
                await consumer.start_consumer()

                await producer.put("test")

                await producer.flush()

                for i in range(3):
                    async for item in consumer:
                        results.append(item)

            checkpoints = checkpointer.get_all_checkpoints()

            # Expect 1 as only 1 shard
            self.assertEquals(1, len(checkpoints))

            self.assertIsNotNone(checkpoints[list(checkpoints.keys())[0]]["sequence"])

            self.assertListEqual(results, ["test"])

    @skipUnless(TESTING_USE_AWS_KINESIS, "Requires TESTING_USE_AWS_KINESIS flag to be set")
    async def test_consumer_consume_fetch_limit(self):

        async with Consumer(
            stream_name=self.STREAM_NAME_SINGLE_SHARD,
            sleep_time_no_records=0.0001,
            shard_fetch_rate=500,
            iterator_type="LATEST",
        ) as consumer:
            await consumer.start()

            # GetShardIterator has a limit of five transactions per second per account per open shard

            for i in range(0, 500):
                await consumer.fetch()
                # sleep 50ms
                await asyncio.sleep(0.05)

            shard_stats = [s["stats"] for s in consumer.shards][0].to_data()

            self.assertTrue(shard_stats["throttled"] > 0, msg="Expected to be throttled")

    @skipUnless(TESTING_USE_AWS_KINESIS, "Requires TESTING_USE_AWS_KINESIS flag to be set")
    async def test_producer_producer_limit(self):
        # Expect some throughput errors

        async with Producer(
            stream_name=self.STREAM_NAME_SINGLE_SHARD,
            processor=StringProcessor(),
            put_bandwidth_limit_per_shard=1500,
        ) as producer:

            async with Consumer(
                stream_name=self.STREAM_NAME_SINGLE_SHARD,
                processor=StringProcessor(),
                iterator_type="LATEST",
            ) as consumer:

                await consumer.start_consumer()

                # Wait a bit just to be sure iterator is gonna get late
                await asyncio.sleep(3)

                for x in range(20):
                    await producer.put(self.random_string(1024 * 250))

                # todo: async timeout
                output = []
                while len(output) < 20:
                    async for item in consumer:
                        output.append(item)

                self.assertEquals(len(output), 20)
                self.assertTrue(producer.throughput_exceeded_count > 0)
