import json
import logging
import math
from collections import namedtuple
from bases import Bases
from .exceptions import ValidationError

try:
    import msgpack

    bases = Bases()

except:
    pass

from .exceptions import ExceededPutLimit

log = logging.getLogger(__name__)

OutputItem = namedtuple("OutputItem", ["size", "n", "data"])


class Aggregator:
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

    def has_items(self):
        return False

    def add_item(self, item):
        size = len(item)
        self.validate_size(size)
        yield OutputItem(size=size, n=1, data=item)

    def parse(self, data):
        return data


class StringWithoutAggregation(Aggregator):
    pass


class JsonWithoutAggregation(Aggregator):
    def serialize(self, item):
        return json.dumps(item)

    def deserialize(self, data):
        return json.loads(data)

    def add_item(self, item):
        output = self.serialize(item)
        size = len(output)
        self.validate_size(size)

        yield OutputItem(size=size, n=1, data=output)

    def parse(self, data):
        yield self.deserialize(data)


class BaseAggregation(JsonWithoutAggregation):
    def has_items(self):
        return self.size > 0

    def add_item(self, item):

        output = self.serialize(item)
        size = len(output)

        self.validate_size(size)

        if size + self.size + self.HEADER_SIZE < self.max_bytes:
            self.buffer.append((size, output))
            self.size += size + self.HEADER_SIZE

        else:
            log.debug(
                "Adding (full) item to queue with {} individual records with size of {} kb".format(
                    len(self.buffer), round(self.size / 1024)
                )
            )
            yield OutputItem(size=self.size, n=len(self.buffer), data=self.output())
            self.buffer = [(size, output)]
            self.size = 1

    def get_items(self):
        log.debug(
            "Adding (partial) item to queue with {} individual records with size of {} kb".format(
                len(self.buffer), len(self.buffer), round(self.size / 1024)
            )
        )
        yield OutputItem(size=self.size, n=len(self.buffer), data=self.output())
        self.buffer = []
        self.size = 0


class JsonLineAggregation(BaseAggregation):

    HEADER_SIZE = 1

    def output(self):
        return "\n".join([x[1] for x in self.buffer])

    def parse(self, data):
        for row in data.split(b"\n"):
            yield self.deserialize(row)


class MsgPackAggregation(BaseAggregation):
    """
    Msg Pack
    Framing = 4 bytes (for size) + data
    """

    HEADER_SIZE = 4

    def output(self):
        frame = []

        for size, data in self.buffer:
            frame.append(
                bases.toBase62(size).ljust(self.HEADER_SIZE, " ").encode("utf-8")
            )
            frame.append(data)

        return b"".join(frame)

    def serialize(self, item):
        result = msgpack.packb(item, use_bin_type=True)
        return result

    def deserialize(self, data):
        return msgpack.unpackb(data, raw=False)

    def parse(self, data):

        i = 0
        length = len(data)

        while True:
            header = data[i : i + self.HEADER_SIZE].decode("utf-8").strip(" ")
            size = bases.fromBase62(header)
            item = data[i + self.HEADER_SIZE : i + self.HEADER_SIZE + size]
            yield self.deserialize(item)
            i += self.HEADER_SIZE + size
            if i == length:
                break
