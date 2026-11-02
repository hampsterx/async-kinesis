"""An aggregated record stays within max_bytes, framing included, whichever JSON backend is in use."""

import json
import logging

import pytest

from kinesis import exceptions
from kinesis.aggregators import NetstringAggregator, NewlineAggregator
from kinesis.processors import JsonLineProcessor, JsonListProcessor, KPLStringProcessor, MsgpackProcessor
from kinesis.serializers import StringSerializer

MIXED_ITEMS = [
    1,
    -0.0,
    1e22,
    0.1,
    None,
    True,
    "",
    'quote " backslash \\ slash /',
    "café \U0001f600",
    [],
    {},
    {"nested": [1, {"deep": "value"}], "n": 1.5},
]


def unpack(processor, output):
    parsed = list(processor.parse(output.data))
    # A list aggregator's record parses to one list, the others to one row per item
    return parsed[0] if isinstance(processor, JsonListProcessor) else parsed


def run(processor, items):
    outputs = []
    for item in items:
        outputs.extend(processor.add_item(item))
    outputs.extend(processor.get_items())
    return outputs


def framed_size(processor, item):
    serialized = processor.serialize(item)
    return len(serialized) + processor.get_header_size(serialized)


def item_framed_to(processor, target):
    """A string of `target` bytes once serialized and framed, on the processor's serializer."""
    # The largest framing here is netstring's: a length of up to seven digits, ":" and ","
    for length in range(target, target - 16, -1):
        item = "x" * length
        if framed_size(processor, item) == target:
            return item
    raise AssertionError("no string frames to {} bytes".format(target))


@pytest.fixture(
    params=[
        pytest.param((JsonListProcessor, "json"), id="list-json"),
        pytest.param((JsonListProcessor, "ujson"), id="list-ujson"),
        pytest.param((JsonLineProcessor, "json"), id="line-json"),
        pytest.param((JsonLineProcessor, "ujson"), id="line-ujson"),
        pytest.param((MsgpackProcessor, None), id="msgpack"),
    ]
)
def processor_class(request, monkeypatch):
    cls, backend = request.param
    if backend:
        # kinesis.serializers takes ujson when it is installed, so name the backend rather than inherit one
        monkeypatch.setattr("kinesis.serializers.json", pytest.importorskip(backend))
    return cls


class TestRecordsStayWithinMaxBytes:
    @pytest.mark.parametrize(
        "max_size,count",
        [
            pytest.param(25, 50000, id="25kb"),
            # Past 1024000 bytes of items, where an undercount can pass Kinesis's 1048576 byte record limit
            pytest.param(None, 600000, id="default"),
        ],
    )
    def test_many_small_items(self, processor_class, max_size, count, caplog):
        # One debug line per item takes this test from under a second to over ten
        caplog.set_level(logging.INFO, logger="kinesis.aggregators")
        processor = processor_class(max_size=max_size)

        outputs = run(processor, [1] * count)

        assert len(outputs) > 1
        assert sum(output.n for output in outputs) == count
        for output in outputs:
            assert 0 < len(output.data) <= processor.max_bytes
            assert len(output.data) <= output.size <= processor.max_bytes
            assert unpack(processor, output) == [1] * output.n

    def test_mixed_items(self, processor_class):
        processor = processor_class(max_size=25)
        items = MIXED_ITEMS * 1000

        outputs = run(processor, items)

        assert len(outputs) > 1
        received = []
        for output in outputs:
            assert 0 < len(output.data) <= processor.max_bytes
            assert len(output.data) <= output.size <= processor.max_bytes
            received.extend(unpack(processor, output))
        assert received == items

    def test_first_item_after_a_flush_is_accounted(self):
        processor = MsgpackProcessor(max_size=25)
        # A long netstring length, left out of the count, puts the record over by several bytes
        items = ["a" * 1000, "x" * 25000] + ["b"] * 1000

        outputs = run(processor, items)

        assert [output.size for output in outputs] == [len(output.data) for output in outputs]
        assert all(len(output.data) <= processor.max_bytes for output in outputs)


class TestAccountedSize:
    @pytest.mark.parametrize(
        "cls,backend",
        [
            pytest.param(JsonListProcessor, "json", id="list-json"),
            pytest.param(JsonLineProcessor, "json", id="line-json"),
            pytest.param(JsonLineProcessor, "ujson", id="line-ujson"),
            pytest.param(MsgpackProcessor, None, id="msgpack"),
        ],
    )
    def test_exact_on_every_record(self, monkeypatch, cls, backend):
        if backend:
            monkeypatch.setattr("kinesis.serializers.json", pytest.importorskip(backend))
        processor = cls(max_size=25)

        outputs = run(processor, ["item"] * 10000)

        assert len(outputs) > 1
        assert [output.size for output in outputs] == [len(output.data) for output in outputs]

    def test_ujson_list_overcounts_one_byte_per_separator(self, monkeypatch):
        monkeypatch.setattr("kinesis.serializers.json", pytest.importorskip("ujson"))
        processor = JsonListProcessor(max_size=25)

        outputs = run(processor, ["item"] * 10000)

        assert len(outputs) > 1
        assert [output.size for output in outputs] == [len(output.data) + output.n - 1 for output in outputs]

    def test_stdlib_list_framing(self, monkeypatch):
        monkeypatch.setattr("kinesis.serializers.json", json)

        for items in ([1], [1, 2], [1, 2, 3]):
            output = run(JsonListProcessor(), items)[0]
            assert output.size == len(output.data) == len(items) * 3


class TestSingleItemAtTheLimit:
    def test_item_that_fills_a_record_exactly(self, processor_class):
        processor = processor_class(max_size=25)
        item = item_framed_to(processor, processor.max_bytes)

        assert list(processor.add_item(item)) == []
        outputs = list(processor.get_items())

        assert len(outputs) == 1
        assert outputs[0].n == 1
        assert len(outputs[0].data) == processor.max_bytes
        assert unpack(processor, outputs[0]) == [item]

    def test_item_over_the_limit_only_with_its_framing_raises(self, processor_class):
        processor = processor_class(max_size=25)
        item = item_framed_to(processor, processor.max_bytes) + "x"
        assert len(processor.serialize(item)) <= processor.max_bytes

        with pytest.raises(exceptions.ExceededPutLimit, match=r"^Put of 25601 bytes exceeded the 25600 byte limit$"):
            list(processor.add_item(item))

        assert not processor.has_items()

    def test_rejected_item_leaves_the_batch_alone(self, processor_class):
        processor = processor_class(max_size=25)
        too_big = item_framed_to(processor, processor.max_bytes) + "x"
        assert list(processor.add_item("kept", partition_key="a")) == []

        with pytest.raises(exceptions.ExceededPutLimit):
            list(processor.add_item(too_big, partition_key="b"))

        outputs = list(processor.get_items())
        assert len(outputs) == 1
        assert outputs[0].partition_key == "a"
        assert unpack(processor, outputs[0]) == ["kept"]

    def test_no_item_size_emits_a_record_without_items(self, processor_class):
        for excess in range(-16, 17):
            processor = processor_class(max_size=25)
            try:
                outputs = run(processor, ["x" * (processor.max_bytes + excess)])
            except exceptions.ExceededPutLimit:
                continue
            assert [output.n for output in outputs] == [1]

    def test_full_batch_flushes_before_an_item_that_fills_a_record(self, processor_class):
        processor = processor_class(max_size=25)
        item = item_framed_to(processor, processor.max_bytes)

        assert list(processor.add_item("first")) == []
        flushed = list(processor.add_item(item))

        assert [output.n for output in flushed] == [1]
        assert unpack(processor, flushed[0]) == ["first"]
        assert processor.has_items()
        assert processor.size == processor.max_bytes


class TestBatchBoundary:
    def test_batch_may_reach_max_bytes(self, processor_class):
        processor = processor_class(max_size=25)
        first = "x" * 1000
        second = item_framed_to(processor, processor.max_bytes - framed_size(processor, first))

        assert list(processor.add_item(first)) == []
        assert list(processor.add_item(second)) == []
        assert processor.size == processor.max_bytes

        outputs = list(processor.get_items())
        assert len(outputs) == 1
        assert outputs[0].n == 2
        assert len(outputs[0].data) <= processor.max_bytes

    def test_batch_flushes_one_byte_past_max_bytes(self, processor_class):
        processor = processor_class(max_size=25)
        first = "x" * 1000
        second = item_framed_to(processor, processor.max_bytes - framed_size(processor, first)) + "x"

        assert list(processor.add_item(first)) == []
        flushed = list(processor.add_item(second))

        assert [output.n for output in flushed] == [1]
        assert unpack(processor, flushed[0]) == [first]
        assert unpack(processor, list(processor.get_items())[0]) == [second]


class TestEmptyItemAfterFlush:
    @pytest.mark.parametrize("aggregator", [NewlineAggregator, NetstringAggregator])
    def test_buffered_empty_item_is_still_reported(self, aggregator):
        class EmptyItemProcessor(aggregator, StringSerializer):
            pass

        processor = EmptyItemProcessor(max_size=25)
        filler = item_framed_to(processor, processor.max_bytes)

        assert list(processor.add_item(filler)) == []
        assert [output.n for output in processor.add_item("")] == [1]

        # Producer.flush() asks has_items() before it collects the buffer
        assert processor.has_items()
        assert [output.n for output in processor.get_items()] == [1]


class TestKPLLimit:
    def test_oversized_item_raises_value_error(self):
        # KPLAggregator has its own add_item(): aws-kinesis-agg enforces the limit, not validate_size()
        with pytest.raises(ValueError, match="too big"):
            list(KPLStringProcessor().add_item("x" * 1024 * 1024))
