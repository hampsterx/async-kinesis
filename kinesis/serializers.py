try:
    import ujson as json
except ModuleNotFoundError:
    # https://github.com/python/mypy/issues/1153 (mypy bug with try/except conditional imports)
    import json  # type: ignore

try:
    import msgpack
except ModuleNotFoundError:
    # MsgpackSerializer refuses to construct
    msgpack = None


class Serializer:
    pass


class StringSerializer(Serializer):
    def serialize(self, item):
        return str(item).encode("utf-8")

    def deserialize(self, data):
        return data.decode("utf-8")


class JsonSerializer(Serializer):
    def serialize(self, item):
        return json.dumps(item).encode("utf-8")

    def deserialize(self, data):
        return json.loads(data.decode("utf-8"))


class MsgpackSerializer(Serializer):
    # __new__ rather than __init__: a processor puts its aggregator first in the MRO, and
    # the aggregators' __init__ never calls super(), so this class's __init__ would not run
    def __new__(cls, *args, **kwargs):
        if msgpack is None:
            raise ImportError(f"{cls.__name__} requires msgpack>=1.2.3: pip install async-kinesis[msgpack]")
        # Overriding __new__ stops object.__init__ rejecting stray arguments, so reject them here
        if (args or kwargs) and cls.__init__ is object.__init__:
            raise TypeError(f"{cls.__name__}() takes no arguments")
        return super().__new__(cls)

    def serialize(self, item):
        result = msgpack.packb(item, use_bin_type=True)
        return result

    def deserialize(self, data):
        return msgpack.unpackb(data, raw=False)
