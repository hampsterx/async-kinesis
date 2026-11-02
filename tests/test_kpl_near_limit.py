"""Every item given to a KPL processor comes back out of it or raises, whatever its size and whatever is buffered."""

import pytest
from aws_kinesis_agg.aggregator import AggRecord

from kinesis.processors import KPLJsonProcessor, KPLStringProcessor
from kinesis.testing import MockProducer

LIMITS = [25600, 1048576]

# Bytes under max_size, at any limit where an item's length takes a three-byte varint, which both
# of LIMITS do. Smaller limits have narrower framing. Up to 12 under is too big even without the
# record's 20 bytes of framing, and the library raises. 13 to 32 under is too big only with them.
TOO_BIG = range(0, 33)
FITS = [33, 34, 1024]


def item_of(processor, size):
    """An item that serializes to exactly `size` bytes, on either processor's serializer."""
    length = size - (len(processor.serialize("x")) - 1)
    item = "x" * length
    assert len(processor.serialize(item)) == size
    return item


@pytest.fixture(params=[KPLStringProcessor, KPLJsonProcessor], ids=["string", "json"])
def processor_class(request):
    return request.param


@pytest.mark.parametrize("limit", LIMITS)
class TestFirstItem:
    @pytest.mark.parametrize("under", TOO_BIG)
    def test_too_big_raises(self, processor_class, limit, under):
        processor = processor_class(max_size=limit)

        with pytest.raises(ValueError, match="too big"):
            list(processor.add_item(item_of(processor, limit - under)))

        # Nothing was accepted, so there is nothing to flush: no empty record reaches the stream
        assert not processor.has_items()
        assert list(processor.get_items()) == []

    @pytest.mark.parametrize("under", FITS)
    def test_item_that_fits_round_trips(self, processor_class, limit, under):
        processor = processor_class(max_size=limit)
        item = item_of(processor, limit - under)

        outputs = list(processor.add_item(item)) + list(processor.get_items())

        assert [output.n for output in outputs] == [1]
        assert outputs[0].size <= limit
        assert list(processor.parse(outputs[0].data)) == [item]


@pytest.mark.parametrize("limit", LIMITS)
class TestBufferedRecord:
    @pytest.mark.parametrize("under", TOO_BIG)
    def test_refused_item_leaves_buffered_record_to_be_delivered(self, processor_class, limit, under):
        processor = processor_class(max_size=limit)
        assert list(processor.add_item("small")) == []

        # list() discards anything yielded before a raise, so a guard that flushed first would lose it
        with pytest.raises(ValueError, match="too big"):
            list(processor.add_item(item_of(processor, limit - under)))

        assert processor.has_items()
        outputs = list(processor.get_items())
        assert [output.n for output in outputs] == [1]
        assert list(processor.parse(outputs[0].data)) == ["small"]

    def test_refused_item_after_several_smaller_items(self, processor_class, limit):
        processor = processor_class(max_size=limit)
        smaller = [item_of(processor, limit // 5) for _ in range(3)]
        for item in smaller:
            assert list(processor.add_item(item)) == []

        with pytest.raises(ValueError, match="too big"):
            list(processor.add_item(item_of(processor, limit - 20)))

        # The aggregator still works: the next item joins the same record
        assert list(processor.add_item("next")) == []
        outputs = list(processor.get_items())
        assert [output.n for output in outputs] == [4]
        assert list(processor.parse(outputs[0].data)) == [*smaller, "next"]

    def test_largest_item_that_fits_flushes_and_stays_buffered(self, processor_class, limit):
        processor = processor_class(max_size=limit)
        largest = item_of(processor, limit - 33)
        assert list(processor.add_item("small")) == []

        flushed = list(processor.add_item(largest))

        assert [output.n for output in flushed] == [1]
        assert list(processor.parse(flushed[0].data)) == ["small"]
        remainder = list(processor.get_items())
        assert [output.n for output in remainder] == [1]
        assert remainder[0].size == limit
        assert list(processor.parse(remainder[0].data)) == [largest]


# 160 and 16420 accept items on both sides of where the framing's length varints gain a byte
@pytest.mark.parametrize("limit", [160, 256, 1024, 16384, 16420, *LIMITS])
@pytest.mark.parametrize("buffered", [0, 1, 3])
def test_no_item_is_lost_near_the_limit(limit, buffered):
    processor = KPLStringProcessor(max_size=limit)
    items = [str(i) for i in range(buffered)]
    sent, received = [], []

    def collect(outputs):
        for output in outputs:
            assert output.n > 0
            assert output.size <= limit
            received.extend(processor.parse(output.data))

    for item in items:
        sent.append(item)
        collect(processor.add_item(item))

    for under in range(0, 48):
        item = "x" * (limit - under)
        try:
            collect(processor.add_item(item))
        except ValueError:
            # A refusal has to be earned: with room to spare, the item's record is over the limit
            record = AggRecord(limit * 2 + 64)
            assert record.add_user_record("a", item)
            assert record.get_size_bytes() > limit
            continue
        sent.append(item)

    collect(processor.get_items())

    assert received == sent


async def test_put_raises_and_buffered_record_reaches_the_stream(kinesis_backend):
    stream = kinesis_backend.create_stream("kpl-near-limit")
    limit = 25600
    processor = KPLStringProcessor(max_size=limit)
    producer = MockProducer(stream_name="kpl-near-limit", processor=processor)
    await producer.put("small")

    with pytest.raises(ValueError, match="too big"):
        await producer.put("x" * (limit - 20))

    # Nothing was flushed by the refusal, so no empty record reaches the stream either
    assert stream.record_count == 0

    await producer.flush()

    records = [record for shard in stream.shards for record in shard.records]
    assert [list(processor.parse(data)) for _, data, _ in records] == [["small"]]
