import asyncio
import aiobotocore
import logging
from contextlib import AsyncExitStack
from async_timeout import timeout
from asyncio import CancelledError
from botocore.exceptions import ClientError
from botocore.config import Config

from . import exceptions


log = logging.getLogger(__name__)


class Base:
    def __init__(self, stream_name, endpoint_url=None, region_name=None):

        self.stream_name = stream_name

        self.endpoint_url = endpoint_url
        self.region_name = region_name

        self.exit_stack = AsyncExitStack()
        self.client = None
        self.shards = None

        self.stream_status = None

    async def __aenter__(self):

        log.info(
            "creating client with {}".format(
                self.endpoint_url if self.endpoint_url else "AWS default endpoint"
            )
        )

        session = aiobotocore.session.AioSession()

        # Note: max_attempts = 0
        # Boto RetryHandler only handles these errors:
        #  GENERAL_CONNECTION_ERROR => ConnectionError, ConnectionClosedError, ReadTimeoutError, EndpointConnectionError
        # Still have to handle ClientError anyway~

        self.client = await self.exit_stack.enter_async_context(session.create_client(
            "kinesis",
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            config=Config(
                connect_timeout=5, read_timeout=90, retries={"max_attempts": 0}
            ),
        ))

        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
        await self.exit_stack.__aexit__(exc_type, exc, tb)

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
            raise

    async def start(self, skip_describe_stream=False):

        if skip_describe_stream:
            log.debug(
                "Skipping Describe stream '{}'. Assuming it exists..".format(
                    self.stream_name
                )
            )
            self.shards = []

        log.debug("Checking stream '{}' is active".format(self.stream_name))

        async with timeout(60) as cm:
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
