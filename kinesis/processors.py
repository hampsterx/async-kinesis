from .aggregators import NewlineAggregator, SimpleAggregator, NetstringAggregator, ListAggregator
from .serializers import StringSerializer, JsonSerializer, MsgpackSerializer


class Processor:
    pass


class StringProcessor(Processor, SimpleAggregator, StringSerializer):
    pass


class JsonProcessor(Processor, SimpleAggregator, JsonSerializer):
    pass


class JsonLineProcessor(Processor, NewlineAggregator, JsonSerializer):
    pass


class JsonListProcessor(Processor, ListAggregator, JsonSerializer):
    pass


class MsgpackProcessor(Processor, NetstringAggregator, MsgpackSerializer):
    pass
