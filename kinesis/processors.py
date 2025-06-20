from typing import Protocol, Any, Iterator, List
from .aggregators import (
    NewlineAggregator,
    SimpleAggregator,
    NetstringAggregator,
    ListAggregator,
    KPLAggregator,
    OutputItem,
)
from .serializers import StringSerializer, JsonSerializer, MsgpackSerializer


class Processor:
    """Base class for processors that combine aggregation and serialization."""
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


class KPLJsonProcessor(Processor, KPLAggregator, JsonSerializer):
    pass


class KPLStringProcessor(Processor, KPLAggregator, StringSerializer):
    pass
