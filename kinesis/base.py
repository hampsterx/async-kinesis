import asyncio
import logging
import time
from asyncio import CancelledError
from typing import Any, Dict, List, Optional

from aiobotocore.session import AioSession
from botocore.config import Config
from botocore.exceptions import ClientError

from . import exceptions
from .timeout_compat import timeout

log = logging.getLogger(__name__)


class Base:
    def __init__(
        self,
        stream_name: str,
        session: Optional[AioSession] = None,
        endpoint_url: Optional[str] = None,
        region_name: Optional[str] = None,
        retry_limit: Optional[int] = None,
        expo_backoff: Optional[float] = None,
        expo_backoff_limit: int = 120,
        skip_describe_stream: bool = False,
        use_list_shards: bool = False,
        create_stream: bool = False,
        create_stream_shards: int = 1,
    ) -> None:

        self.stream_name: str = stream_name

        if session:
            assert isinstance(session, AioSession)
            self.session: AioSession = session
        else:
            self.session = AioSession()

        self.endpoint_url: Optional[str] = endpoint_url
        self.region_name: Optional[str] = region_name

        self.client: Optional[Any] = None  # aiobotocore client
        self._client_cm: Optional[Any] = None  # client context manager
        self.shards: Optional[List[Dict[str, Any]]] = None

        self.stream_status: Optional[str] = None

        self.retry_limit: Optional[int] = retry_limit
        self.expo_backoff: Optional[float] = expo_backoff
        self.expo_backoff_limit: int = expo_backoff_limit

        # connection states of kinesis client
        self.RECONNECT = "RECONNECT"
        self.ACTIVE = "ACTIVE"
        self.INITIALIZE = "INITIALIZE"
        self.UPDATING = "UPDATING"
        self.CREATING = "CREATING"
        self.DELETING = "DELETING"

        self.stream_status = self.INITIALIZE
        # Short Lived producer might want to skip describing stream on startup
        self.skip_describe_stream = skip_describe_stream
        # Use ListShards API instead of DescribeStream for better rate limits
        self.use_list_shards = use_list_shards
        self._conn_lock = asyncio.Lock()
        self._reconnect_timeout = time.monotonic()
        self.create_stream = create_stream
        self.create_stream_shards = create_stream_shards

    async def __aenter__(self) -> "Base":

        log.info("creating client with {}".format(self.endpoint_url if self.endpoint_url else "AWS default endpoint"))

        try:
            await self.get_conn()
        except exceptions.StreamDoesNotExist:
            await self.close()
            raise
        except Exception:
            raise

        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()
        if self._client_cm is not None:
            try:
                await self._client_cm.__aexit__(exc_type, exc, tb)
            except (AttributeError, TypeError) as e:
                # Handle cases where client context manager doesn't have __aexit__ or session is malformed
                log.debug(f"Client context manager exit failed: {e}, attempting direct close")
                try:
                    if self.client is not None:
                        await self.client.close()
                except Exception as close_error:
                    log.debug(f"Client close also failed: {close_error}")
                    # Continue cleanup even if client close fails

    @property
    def address(self) -> Dict[str, str]:
        """
        Return address of stream, either as StreamName or StreamARN, when applicable.

        https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StreamDescription.html#Streams-Type-StreamDescription-StreamName
        https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StreamDescription.html#Streams-Type-StreamDescription-StreamARN
        """
        if self.stream_name.startswith("arn:"):
            return {"StreamARN": self.stream_name}
        else:
            return {"StreamName": self.stream_name}

    async def get_client(self):

        # Note: max_attempts = 0
        # Boto RetryHandler only handles these errors:
        #  GENERAL_CONNECTION_ERROR => ConnectionError, ConnectionClosedError, ReadTimeoutError, EndpointConnectionError
        # Still have to handle ClientError anyway~

        self._client_cm = self.session.create_client(
            "kinesis",
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            config=Config(connect_timeout=5, read_timeout=90, retries={"max_attempts": 0}),
        )
        self.client = await self._client_cm.__aenter__()

    async def get_stream_description(self):

        try:
            return (await self.client.describe_stream(**self.address))["StreamDescription"]
        except ClientError as err:
            code = err.response["Error"]["Code"]
            if code == "ResourceNotFoundException":
                raise exceptions.StreamDoesNotExist("Stream '{}' does not exist".format(self.stream_name)) from None
            raise

    async def list_shards(self, max_results: int = 1000):
        """
        Use ListShards API as alternative to DescribeStream for getting shard information.
        ListShards has a higher rate limit (100 ops/s per stream vs 10 ops/s account-wide for DescribeStream).
        """
        try:
            # Use ListShards API which has better rate limits
            response = await self.client.list_shards(**self.address, MaxResults=max_results)
            return response["Shards"]
        except ClientError as err:
            code = err.response["Error"]["Code"]
            if code == "ResourceNotFoundException":
                raise exceptions.StreamDoesNotExist("Stream '{}' does not exist".format(self.stream_name)) from None
            raise

    async def start(self):

        await self.get_client()

        if self.create_stream:
            await self._create_stream()
            self.create_stream = False

        if self.skip_describe_stream:
            log.debug("Skipping Describe stream '{}'. Assuming it exists and is active.".format(self.stream_name))
            self.shards = []
            self.stream_status = self.ACTIVE
            return

        log.debug("Checking stream '{}' is active".format(self.stream_name))

        if self.use_list_shards:
            # Use ListShards API for better rate limits (100 ops/s per stream vs 10 ops/s account-wide)
            log.debug("Using ListShards API for better rate limits")
            try:
                self.shards = await self.list_shards()
                self.stream_status = self.ACTIVE
            except Exception as e:
                # Fall back to DescribeStream if ListShards fails
                log.warning(f"ListShards failed ({e}), falling back to DescribeStream")
                self.use_list_shards = False

        if not self.use_list_shards:
            # Use traditional DescribeStream API
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
                raise exceptions.StreamStatusInvalid("Stream '{}' is still {}".format(self.stream_name, stream_status))

    async def close(self):
        raise NotImplementedError

    async def get_conn(self):

        async with self._conn_lock:

            log.debug(f"Get Connection (stream name: {self.stream_name}), stream status: {self.stream_status})")

            if self.stream_status == self.INITIALIZE:
                try:
                    await self.start()
                    log.info("Connection Successfully Initialized")
                except exceptions.StreamDoesNotExist:
                    # Do not attempt to reconnect if stream does not exist
                    log.error(f"Stream does not exist ({self.stream_name})")
                    raise
                except Exception as e:
                    log.warning(f"Connection Failed to Initialize : {e.__class__} {e}")
                    await self._get_reconn_helper()
            elif self.stream_status == self.ACTIVE and (time.monotonic() - self._reconnect_timeout) > 120:
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
        # Reset client context manager after close
        self._client_cm = None
        self.client = None
        while True:
            self._reconnect_timeout = time.monotonic()
            try:
                log.warning(
                    f"Connection Error. Rebuilding connection. Sleeping for {backoff_delay} seconds. "
                    f"Reconnection Attempt: {conn_attempts}"
                )
                await asyncio.sleep(backoff_delay)
                await self.start()
                log.warning(f"Connection Reestablished After {conn_attempts} and Sleeping for {backoff_delay}")
                break
            except Exception as e:
                if isinstance(e, exceptions.StreamDoesNotExist):
                    raise e
                log.warning(e)
                conn_attempts += 1
                # Default retry limit of 5 if not specified
                retry_limit = self.retry_limit if isinstance(self.retry_limit, int) else 5
                if conn_attempts >= (retry_limit + 1):
                    await self.close()
                    raise ConnectionError(f"Kinesis client has exceeded {retry_limit} connection attempts")
                if self.expo_backoff:
                    backoff_delay = (conn_attempts**2) * self.expo_backoff
                    if backoff_delay >= self.expo_backoff_limit:
                        backoff_delay = self.expo_backoff_limit
                await self.close()
                # Reset client context manager after close
                self._client_cm = None
                self.client = None

    async def _create_stream(self, ignore_exists=True):

        log.debug("Creating (or ignoring) stream {} with {} shards".format(self.stream_name, self.create_stream_shards))

        if self.create_stream_shards < 1:
            raise Exception("Min shard count is one")

        try:
            await self.client.create_stream(**self.address, ShardCount=self.create_stream_shards)
        except ClientError as err:
            code = err.response["Error"]["Code"]

            if code == "ResourceInUseException":
                if not ignore_exists:
                    raise exceptions.StreamExists(
                        "Stream '{}' exists, cannot create it".format(self.stream_name)
                    ) from None
            elif code == "LimitExceededException":
                raise exceptions.StreamShardLimit("Stream '{}' exceeded shard limit".format(self.stream_name))
            else:
                raise
