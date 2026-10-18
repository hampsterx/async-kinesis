import pytest

from kinesis import exceptions
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


class TestProcessorAndAggregator:
    """Test processors and aggregators functionality."""

    def test_aggregator_min_size(self):
        with pytest.raises(exceptions.ValidationError):
            Aggregator(max_size=20)

    def test_aggregator_max_size(self):
        with pytest.raises(exceptions.ValidationError):
            Aggregator(max_size=2000)

    def test_processor_exceed_put_limit(self, random_string):
        processor = StringProcessor()

        with pytest.raises(exceptions.ExceededPutLimit):
            list(processor.add_item(random_string(1024 * 1024 + 1)))

    def test_newline_aggregator(self):
        # in reality does not make sense as strings can contain new lines
        # so is not a suitable combination to use
        class NewlineTestProcessor(NewlineAggregator, StringSerializer):
            pass

        processor = NewlineTestProcessor()

        # Expect nothing as batching
        assert [] == list(processor.add_item(123))
        assert [] == list(processor.add_item("test"))

        assert processor.has_items()

        output = list(processor.get_items())

        assert len(output) == 1
        assert output[0].size == 9
        assert output[0].n == 2
        assert output[0].data == b"123\ntest\n"

        assert list(processor.parse(output[0].data)) == ["123", "test"]

    def test_list_aggregator(self):
        class JsonListTestProcessor(ListAggregator, JsonSerializer):
            pass

        processor = JsonListTestProcessor()

        # Expect nothing as batching
        assert [] == list(processor.add_item(123))
        assert [] == list(processor.add_item("test"))

        assert processor.has_items()

        output = list(processor.get_items())

        assert len(output) == 1
        assert output[0].size == 11
        assert output[0].n == 2
        assert output[0].data == b'[123, "test"]'

        assert next(processor.parse(output[0].data)) == [123, "test"]

    def test_netstring_aggregator(self):
        class NetstringTestProcessor(NetstringAggregator, StringSerializer):
            pass

        processor = NetstringTestProcessor()

        # Expect nothing as batching
        assert [] == list(processor.add_item(123))
        assert [] == list(processor.add_item("test"))

        assert processor.has_items()

        output = list(processor.get_items())

        assert len(output) == 1
        assert output[0].size == 13
        assert output[0].n == 2
        assert output[0].data == b"3:123,4:test,"

        assert list(processor.parse(output[0].data)) == ["123", "test"]

    def test_kpl_aggregator(self):
        class KPLTestProcessor(KPLAggregator, StringSerializer):
            pass

        processor = KPLTestProcessor()

        # Expect nothing as batching
        assert [] == list(processor.add_item(123))
        assert [] == list(processor.add_item("test"))

        assert processor.has_items()

        output = list(processor.get_items())

        assert len(output) == 1
        assert output[0].n == 2

        assert list(processor.parse(output[0].data)) == ["123", "test"]

    @pytest.mark.parametrize("missing", ["aws_kinesis_agg", "aws_kinesis_agg.messages_pb2"])
    def test_kpl_aggregator_requires_aws_kinesis_agg(self, missing):
        import importlib
        import sys

        import kinesis.aggregators

        try:
            with pytest.MonkeyPatch.context() as mp:
                # A None entry makes the import raise ModuleNotFoundError, as an absent package
                # or a release older than 1.2.0 (no messages_pb2) does
                mp.setitem(sys.modules, missing, None)
                importlib.reload(kinesis.aggregators)

                assert kinesis.aggregators.aws_kinesis_agg is None
                with pytest.raises(ImportError, match=r"async-kinesis\[kpl\]"):
                    kinesis.aggregators.KPLAggregator()
        finally:
            importlib.reload(kinesis.aggregators)

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

        # 10K record
        data = b"x" * 1024 * 10

        # should batch
        for i in range(9):
            output = list(processor.add_item(data))
            assert len(output) == 0

        # Ten 10K records plus KPL framing exceed 100K, so the tenth flushes the first nine
        output = list(processor.add_item(data))
        assert len(output) == 1
        assert output[0].n == 9
        assert output[0].size <= 1024 * 100

        items = list(processor.parse(output[0].data))
        assert len(items) == 9
        for item in items:
            assert item == data

        # The tenth record stays buffered for the next flush
        output = list(processor.get_items())
        assert len(output) == 1
        assert output[0].n == 1
        assert list(processor.parse(output[0].data)) == [data]

    def test_simple_aggregator(self):
        class SimpleTestProcessor(SimpleAggregator, StringSerializer):
            pass

        processor = SimpleTestProcessor()

        # Expect output immediately as no batching
        output = list(processor.add_item(123))
        assert len(output) == 1
        assert output[0].size == 3
        assert output[0].n == 1
        assert output[0].data == b"123"

        output = list(processor.add_item("test"))
        assert len(output) == 1
        assert output[0].size == 4
        assert output[0].n == 1
        assert output[0].data == b"test"

        assert not processor.has_items()

    def test_json_processor(self):
        processor = JsonProcessor()

        # Test serialization
        output = list(processor.add_item({"key": "value"}))
        assert len(output) == 1
        assert output[0].n == 1

        # Test deserialization
        items = list(processor.parse(output[0].data))
        assert len(items) == 1
        assert items[0] == {"key": "value"}

    def test_string_processor(self):
        processor = StringProcessor()

        # Test serialization
        output = list(processor.add_item("test string"))
        assert len(output) == 1
        assert output[0].n == 1
        assert output[0].data == b"test string"

        # Test deserialization
        items = list(processor.parse(output[0].data))
        assert len(items) == 1
        assert items[0] == "test string"

    def test_json_line_processor(self):
        processor = JsonLineProcessor()

        # Should batch until flush
        assert [] == list(processor.add_item({"line": 1}))
        assert [] == list(processor.add_item({"line": 2}))

        assert processor.has_items()

        output = list(processor.get_items())
        assert len(output) == 1
        assert output[0].n == 2

        # Test deserialization
        items = list(processor.parse(output[0].data))
        assert len(items) == 2
        assert items == [{"line": 1}, {"line": 2}]

    def test_json_list_processor(self):
        processor = JsonListProcessor()

        # Should batch until flush
        assert [] == list(processor.add_item({"item": 1}))
        assert [] == list(processor.add_item({"item": 2}))

        assert processor.has_items()

        output = list(processor.get_items())
        assert len(output) == 1
        assert output[0].n == 2

        # Test deserialization
        items = list(processor.parse(output[0].data))
        assert len(items) == 1  # Single list
        assert items[0] == [{"item": 1}, {"item": 2}]

    def test_msgpack_processor(self):
        processor = MsgpackProcessor()

        # Should batch until flush
        assert [] == list(processor.add_item({"data": "test"}))
        assert [] == list(processor.add_item({"data": "test2"}))

        assert processor.has_items()

        output = list(processor.get_items())
        assert len(output) == 1
        assert output[0].n == 2

        # Test deserialization
        items = list(processor.parse(output[0].data))
        assert len(items) == 2
        assert items == [{"data": "test"}, {"data": "test2"}]
