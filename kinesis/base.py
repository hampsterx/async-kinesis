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
    def __init__(
        self,
        stream_name,
        endpoint_url=None,
        region_name=None,
        retry_limit=None,
        expo_backoff=None,
        expo_backoff_limit=120,
        skip_describe_stream=False,
        create_stream=False,
        create_stream_shards=1,
    ):

        self.stream_name = stream_name

        self.endpoint_url = endpoint_url
        self.region_name = region_name

        self.client = None
        self.shards = None

        self.stream_status = None

        self.retry_limit = retry_limit
        self.expo_backoff = expo_backoff
        self.expo_backoff_limit = expo_backoff_limit

        # connection states of kinesis client
        self.RECONNECT = "RECONNECT"
        self.ACTIVE = "ACTIVE"
        self.INITIALIZE = "INITIALIZE"

        self.stream_status = self.INITIALIZE
        # Short Lived producer might want to skip describing stream on startup
        self.skip_describe_stream = skip_describe_stream
        self._conn_lock = asyncio.Lock()
        self._reconnect_timeout = time.monotonic()
        self.create_stream = create_stream
        self.create_stream_shards = create_stream_shards

    async def __aenter__(self):

        log.info(
            "creating client with {}".format(
                self.endpoint_url if self.endpoint_url else "AWS default endpoint"
            )
        )

        try:
            await self.get_conn()
        except exceptions.StreamDoesNotExist:
            await self.close()
            raise
        except:
            raise

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

        if self.create_stream:
            await self._create_stream()
            self.create_stream = False

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

                    if stream_status == self.ACTIVE:
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

        async with self._conn_lock:

            log.debug(
                f"Get Connection (stream name: {self.stream_name}), stream status: {self.stream_status})"
            )

            if self.stream_status == self.INITIALIZE:
                try:
                    await self.start()
                    log.info(f"Connection Successfully Initialized")
                except exceptions.StreamDoesNotExist:
                    # Do not attempt to reconnect if stream does not exist
                    log.error(f"Stream does not exist ({self.stream_name})")
                    raise
                except Exception as e:
                    log.warning(f"Connection Failed to Initialize : {e.__class__} {e}")
                    await self._get_reconn_helper()
            elif (
                self.stream_status == self.ACTIVE
                and (time.monotonic() - self._reconnect_timeout) > 120
            ):
                # reconnect_timeout is a Lock so a new connection is not created immediately
                # after a successfully reconnection has been made since self.start() sets self.stream_status = "ACTIVE"
                # immediately after a successful reconnect.
                # Based on testing a hardcode 120 seconds backoff is best since, there could be a lot of pending
                # coroutines reattempting the connection when the client connection it's already healthy.
                await self._get_reconn_helper()

    async def _get_reconn_helper(self):
        # Logic used to reconnect to connect to kinesis if there is a error

        self.stream_status = self.RECONNECT
        backoff_delay = 5
        conn_attempts = 1
        await self.close()
        while True:
            self._reconnect_timeout = time.monotonic()
            try:
                log.warning(
                    f"Connection Error. Rebuilding connection. Sleeping for {backoff_delay} seconds. Reconnection Attempt: {conn_attempts}"
                )
                await asyncio.sleep(backoff_delay)
                await self.start()
                log.warning(
                    f"Connection Reestablished After {conn_attempts} and Sleeping for {backoff_delay}"
                )
                break
            except Exception as e:
                if isinstance(e, exceptions.StreamDoesNotExist):
                    raise e
                log.warning(e)
                conn_attempts += 1
                if isinstance(self.retry_limit, int):
                    if conn_attempts >= (self.retry_limit + 1):
                        await self.close()
                        raise ConnectionError(
                            f"Kinesis client has exceeded {self.retry_limit} connection attempts"
                        )
                if self.expo_backoff:
                    backoff_delay = (conn_attempts ** 2) * self.expo_backoff
                    if backoff_delay >= self.expo_backoff_limit:
                        backoff_delay = self.expo_backoff_limit
                await self.close()

    async def _create_stream(self, ignore_exists=True):

        log.debug(
            "Creating (or ignoring) stream {} with {} shards".format(
                self.stream_name, self.create_stream_shards
            )
        )

        if self.create_stream_shards < 1:
            raise Exception("Min shard count is one")

        try:
            await self.client.create_stream(
                StreamName=self.stream_name, ShardCount=self.create_stream_shards
            )
        except ClientError as err:
            code = err.response["Error"]["Code"]

            if code == "ResourceInUseException":
                if not ignore_exists:
                    raise exceptions.StreamExists(
                        "Stream '{}' exists, cannot create it".format(self.stream_name)
                    ) from None
            elif code == "LimitExceededException":
                raise exceptions.StreamShardLimit(
                    "Stream '{}' exceeded shard limit".format(self.stream_name)
                )
            else:
                raise
