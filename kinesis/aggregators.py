import json
import logging
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

    def deserialize(self, item):
        return json.loads(item)

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
            log.debug("Overflowing item to queue with {} individual records with size of {} bytes".format(len(self.buffer), self.size))
            yield self.output()
            self.buffer = []
            self.size = 0

    def get_items(self):
        log.debug("Flushing item to queue with {} individual records with size of {} bytes".format(len(self.buffer), self.size))
        yield self.output()
        self.buffer = []
        self.size = 0


    def parse(self, data):
        log.info(data)
        for row in data.split(b'\n'):
            yield self.deserialize(row)
