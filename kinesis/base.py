import asyncio
import aiobotocore
import logging
from async_timeout import timeout
from asyncio import CancelledError
from botocore.exceptions import ClientError
from botocore.config import Config
import time

from . import exceptions


log = logging.getLogger(__name__)


class Base:
    def __init__(self, stream_name, endpoint_url=None, region_name=None,
                 retry_limit=None, expo_backoff=None, expo_backoff_limit=None,
                 skip_describe_stream=False):

        self.stream_name = stream_name

        self.endpoint_url = endpoint_url
        self.region_name = region_name

        self.client = None
        self.shards = None

        self.stream_status = None

        self.retry_limit = retry_limit
        self.expo_backoff = expo_backoff
        self.expo_backoff_limit = expo_backoff_limit
        self.stream_status = "INITIALIZE"
        # Short Lived producer might want to skip describing stream on startup
        self.skip_describe_stream = skip_describe_stream
        self.conn_lock = asyncio.Lock()
        self.reconnect_timeout = time.monotonic()
        self.exception = None
        self.exception_msg = None



    async def __aenter__(self):

        log.info(
            "creating client with {}".format(
                self.endpoint_url if self.endpoint_url else "AWS default endpoint"
            )
        )

        await self.get_conn()



        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
        await self.client.__aexit__(exc_type, exc, tb)

    async def get_client(self):
        session = aiobotocore.session.AioSession()

        # Note: max_attempts = 0
        # Boto RetryHandler only handles these errors:
        #  GENERAL_CONNECTION_ERROR => ConnectionError, ConnectionClosedError, ReadTimeoutError, EndpointConnectionError
        # Still have to handle ClientError anyway~

        self.client = await session.create_client(
            "kinesis",
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            config=Config(
                connect_timeout=5, read_timeout=90, retries={"max_attempts": 0}
            ),
        ).__aenter__()

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

    async def start(self):

        await self.get_client()

        if self.skip_describe_stream:
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


    async def close(self):
        raise NotImplementedError


    async def get_conn(self):

        async with self.conn_lock:
            if self.stream_status == "INITIALIZE":
                try:
                    await self.start()
                    log.warning(f"Connection Successfully Initialized")
                except Exception:
                    log.warning(f"Connection Failed to Initialize")
                    await self._get_reconn_helper()
            elif self.stream_status == "ACTIVE" and (time.monotonic() - self.reconnect_timeout) > 120:
                # reconnect_timeout is a Lock so a new connection is not created immediately
                # after a successfully reconnection has been made since self.start() sets self.stream_status = "ACTIVE"
                # immediately after a successful reconnect.
                await self._get_reconn_helper()


    async def _get_reconn_helper(self):

        self.stream_status = "RECONNECT"
        backoff_delay = 5
        conn_attempts = 1
        await self.close()
        while True:
            self.reconnect_timeout = time.monotonic()
            try:
                log.warning(
                    f"Connection Error. Rebuilding connection. Sleeping for {backoff_delay} seconds. Reconnection Attempt: {conn_attempts}")
                await asyncio.sleep(backoff_delay)
                await self.start()
                log.warning(f"Connection Reestablished After {conn_attempts} and Sleeping for {backoff_delay}")
                break
            except Exception as e:
                log.warning(e)
                conn_attempts += 1
                if isinstance(self.retry_limit, int):
                    if conn_attempts >= (self.retry_limit + 1):
                        await self.close()
                        raise ConnectionError(f'Kinesis client has exceeded {self.retry_limit} connection attempts')
                if self.expo_backoff:
                    backoff_delay = (conn_attempts ** 2) * self.expo_backoff
                    if backoff_delay >= self.expo_backoff_limit:
                        backoff_delay = self.expo_backoff_limit
                await self.close()