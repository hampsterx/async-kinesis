import json
import logging

try:
    import msgpack
except:
    pass

from .exceptions import ExceededPutLimit

log = logging.getLogger(__name__)


class Aggregator:

    @classmethod
    def max_bytes(self):
        return 1024 * 1024

    @classmethod
    def validate_size(cls, size):
        if size > cls.max_bytes():
            raise ExceededPutLimit(
                "Put of {} bytes exceeded 1MB limit".format(size)
            )

    def has_items(self):
        return False


class StringWithoutAggregation(Aggregator):

    def add_item(self, item):
        size = len(item)
        self.validate_size(size)
        return item


class JsonWithoutAggregation(Aggregator):

    def serialize(self, item):
        return json.dumps(item)

    def deserialize(self, data):
        return json.loads(data)

    def add_item(self, item):
        output = self.serialize(item)
        size = len(output)
        self.validate_size(size)

        yield output

    def parse(self, data):
        yield self.deserialize(data)


class JsonLineAggregation(JsonWithoutAggregation):

    def __init__(self):
        self.buffer = []
        self.size = 0

    def has_items(self):
        return self.size > 0

    def output(self):
        return "\n".join(self.buffer)

    def add_item(self, item):

        output = self.serialize(item)
        size = len(output)

        self.validate_size(size)

        if size + self.size < self.max_bytes():
            self.buffer.append(output)
            self.size += size + 1

        else:
            log.debug(
                "Overflowing item to queue with {} individual records with size of {} bytes".format(len(self.buffer),
                                                                                                    self.size))
            yield self.output()
            self.buffer = []
            self.size = 0

    def get_items(self):
        log.debug("Flushing item to queue with {} individual records with size of {} bytes".format(len(self.buffer),
                                                                                                   self.size))
        yield self.output()
        self.buffer = []
        self.size = 0

    def parse(self, data):
        for row in data.split(b'\n'):
            yield self.deserialize(row)


from bases import Bases

bases = Bases()


class MsgPackAggregation(Aggregator):
    """
    Msg Pack
    Framing = 4 bytes (for size) + data
    """

    HEADER_SIZE = 4

    def __init__(self):
        self.buffer = []
        self.size = 0

    def has_items(self):
        return self.size > 0

    def output(self):
        frame = []
        for size, data in self.buffer:
            frame.append(bases.toBase62(size).ljust(self.HEADER_SIZE, " ").encode('utf-8'))
            frame.append(data)

        return b''.join(frame)

    def serialize(self, item):
        result = msgpack.packb(item, use_bin_type=True)
        return result

    def deserialize(self, data):
        return msgpack.unpackb(data, raw=False)

    def add_item(self, item):

        output = self.serialize(item)
        size = len(output)

        self.validate_size(size)

        if size + self.size + self.HEADER_SIZE < self.max_bytes():
            self.buffer.append((size, output))
            self.size += size + self.HEADER_SIZE

        else:

            log.debug(
                "Overflowing item to queue with {} individual records with size of {} bytes".format(len(self.buffer),
                                                                                                    self.size))

            yield self.output()
            self.buffer = []
            self.size = 0

    def get_items(self):
        log.debug("Flushing item to queue with {} individual records with size of {} bytes".format(len(self.buffer),
                                                                                                   self.size))
        yield self.output()
        self.buffer = []
        self.size = 0

    def parse(self, data):

        i = 0
        length = len(data)

        while True:
            header = data[i:i + self.HEADER_SIZE].decode('utf-8').strip(" ")
            size = bases.fromBase62(header)
            item = data[i + self.HEADER_SIZE:i + self.HEADER_SIZE + size]
            yield self.deserialize(item)
            i += self.HEADER_SIZE + size
            if i == length:
                break
