class StreamExists(Exception):
    pass


class StreamDoesNotExist(Exception):
    pass


class StreamShardLimit(Exception):
    pass


class StreamStatusInvalid(Exception):
    pass


class ExceededPutLimit(Exception):
    pass


class UnknownException(Exception):
    pass


class ValidationError(Exception):
    pass
