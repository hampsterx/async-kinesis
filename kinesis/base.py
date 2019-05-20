import asyncio
import aioboto3
import logging
from async_timeout import timeout
from asyncio import CancelledError
from botocore.exceptions import ClientError

from . import exceptions


log = logging.getLogger(__name__)


class Base:
    def __init__(self, stream_name, loop=None, endpoint_url=None, region_name=None):

        self.stream_name = stream_name
        self.loop = loop if loop else asyncio.get_event_loop()

        self.endpoint_url = endpoint_url
        self.region_name = region_name

        self.client = None
        self.shards = None

        self.stream_status = None

    async def __aenter__(self):

        self.client = aioboto3.client(
            "kinesis",
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            loop=self.loop,
        )

        return self

    async def __aexit__(self, exc_type, exc, tb):

        await self.flush()
        await self.close()

    async def get_stream_description(self):
        try:
            return (await self.client.describe_stream(StreamName=self.stream_name))[
                "StreamDescription"
            ]
        except ClientError as err:
            code = err.response["Error"]["Code"]
            if code == "ResourceNotFoundException":
                raise exceptions.StreamDoesNotExist(
                    "Stream '{}' does not exist".format(self.stream_name)
                ) from None

    async def start(self):

        log.debug("Checking stream '{}' is active".format(self.stream_name))

        async with timeout(60, loop=self.loop) as cm:
            try:
                while True:
                    stream_info = await self.get_stream_description()
                    stream_status = stream_info["StreamStatus"]

                    if stream_status == "ACTIVE":
                        self.stream_status = stream_status
                        break

                    if stream_status in ["CREATING", "UPDATING"]:
                        await asyncio.sleep(0.25)

                    else:
                        raise exceptions.StreamStatusInvalid(
                            "Stream '{}' is {}".format(self.stream_name, stream_status)
                        )
            except CancelledError:
                pass

            else:
                self.shards = stream_info["Shards"]

        if cm.expired:
            raise exceptions.StreamStatusInvalid(
                "Stream '{}' is still {}".format(self.stream_name, stream_status)
            )
