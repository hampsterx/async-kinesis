try:
    import ujson as json
except ModuleNotFoundError:
    # https://github.com/python/mypy/issues/1153 (mypy bug with try/except conditional imports)
    import json  # type: ignore

try:
    import msgpack
except ModuleNotFoundError:
    pass


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
    def serialize(self, item):
        result = msgpack.packb(item, use_bin_type=True)
        return result

    def deserialize(self, data):
        return msgpack.unpackb(data, raw=False)
