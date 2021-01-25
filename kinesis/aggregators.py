import logging
import math
from collections import namedtuple
from .exceptions import ValidationError
from .exceptions import ExceededPutLimit

try:
    import aws_kinesis_agg
    import aws_kinesis_agg.aggregator
    import aws_kinesis_agg.kpl_pb2
except ModuleNotFoundError:
    pass

log = logging.getLogger(__name__)

OutputItem = namedtuple("OutputItem", ["size", "n", "data"])


class BaseAggregator:
    def __init__(self, max_size=None):

        if not max_size:
            max_size = 1024

        put_units = math.floor(max_size / 25)

        if put_units <= 0:
            raise ValidationError(
                "max_size is too low. Should be at least one PUT Payload Unit (25Kb)"
            )

        if put_units > 40:
            raise ValidationError(
                "max_size is too high. Should be no higher than 40x PUT Payload Units (25Kb)"
            )

        self.max_bytes = put_units * 25 * 1024

        log.debug(
            "setting max_bytes to {} ({} PUT Payload Units (25kb))".format(
                self.max_bytes, put_units
            )
        )

        self.buffer = []
        self.size = 0

    def validate_size(self, size):
        if size > self.max_bytes:
            raise ExceededPutLimit("Put of {} bytes exceeded 1MB limit".format(size))

    def parse(self, data):
        yield self.deserialize(data)


class SimpleAggregator(BaseAggregator):
    """
    Simple Aggregator (Does NOT aggregate)
    Sends a single record only (high inefficient)
    """

    def has_items(self):
        return False

    def add_item(self, item):
        output = self.serialize(item)
        size = len(output)

        self.validate_size(size)

        yield OutputItem(size=size, n=1, data=output)


class Aggregator(BaseAggregator):
    """
    Aggregator
    Sends an aggregated record
    """

    def has_items(self):
        return self.size > 0

    def get_header_size(self, data):
        raise NotImplementedError()

    def add_item(self, item):
        output = self.serialize(item)
        size = len(output)

        self.validate_size(size)

        header_size = self.get_header_size(output)

        if size + self.size + header_size < self.max_bytes:

            self.buffer.append((size, output))
            self.size += size + header_size

        else:
            log.debug(
                "Yielding item to queue with {} individual records with size of {} kb".format(
                    len(self.buffer), round(self.size / 1024)
                )
            )
            yield OutputItem(size=self.size, n=len(self.buffer), data=self.output())
            self.buffer = [(size, output)]
            self.size = size

        log.debug("Adding item to queue with size of {} kb".format(round(size / 1024)))

    def get_items(self):
        log.debug(
            "Yielding (final) item to queue with {} individual records with size of {} kb".format(
                len(self.buffer), round(self.size / 1024)
            )
        )
        yield OutputItem(size=self.size, n=len(self.buffer), data=self.output())
        self.buffer = []
        self.size = 0


class NewlineAggregator(Aggregator):
    def get_header_size(self, output):
        return 1

    def output(self):
        return b"\n".join([x[1] for x in self.buffer] + [b""])

    def parse(self, data):
        for row in data.split(b"\n"):
            if row:
                yield self.deserialize(row)


class ListAggregator(Aggregator):
    def get_header_size(self, output):
        return 1

    def output(self):
        return self.serialize([self.deserialize(x[1]) for x in self.buffer])

    def parse(self, data):
        yield self.deserialize(data)


class NetstringAggregator(Aggregator):
    """
    Netstring Aggregation
    Framing = {x} bytes (ascii int for size) + 1 byte (":") + data + trailing ","
    See: https://en.wikipedia.org/wiki/Netstring
    """

    def get_header_size(self, output):
        return len(str(len(output))) + 2

    def output(self):
        frame = []

        for size, data in self.buffer:
            frame.append(str(size).encode("ascii"))
            frame.append(b":")
            frame.append(data)
            frame.append(b",")

        return b"".join(frame)

    def parse(self, data):

        i = 0
        length = len(data)

        while True:
            header_offset = data[i:].index(b":")
            size = int(data[i : i + header_offset].decode("ascii"))
            item = data[i + header_offset + 1 : i + header_offset + 1 + size]
            yield self.deserialize(item)

            i += header_offset + size + 2
            if i == length:
                break


class KPLAggregator(Aggregator):
    """
    KPL Aggregated Record Aggregation
    See: https://github.com/awslabs/kinesis-aggregation/tree/master/python
    """

    def __init__(self, max_size=None):
        if max_size:
            self.agg = aws_kinesis_agg.aggregator.RecordAggregator(max_size=max_size)
        else:
            self.agg = aws_kinesis_agg.aggregator.RecordAggregator()

    def has_items(self):
        return self.agg.get_num_user_records() > 0

    def add_item(self, item):
        output = self.serialize(item)
        record = self.agg.add_user_record("a", output)
        self.size = self.agg.get_num_user_records()
        if record:
            size = record.get_size_bytes()
            n = record.get_num_user_records()
            partition_key, explicit_hash_key, data = record.get_contents()
            yield OutputItem(size=size, n=n, data=data)

    def get_items(self):
        record = self.agg.clear_and_get()
        if record:
            size = record.get_size_bytes()
            n = record.get_num_user_records()
            partition_key, explicit_hash_key, data = record.get_contents()
            yield OutputItem(size=size, n=n, data=data)

    def parse(self, data):
        message_data = data[len(aws_kinesis_agg.MAGIC) : -aws_kinesis_agg.DIGEST_SIZE]
        ar = aws_kinesis_agg.kpl_pb2.AggregatedRecord()
        ar.ParseFromString(message_data)
        for record in ar.records:
            yield self.deserialize(record.data)
