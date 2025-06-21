import asyncio
import logging
import os
import uuid
from unittest.mock import patch

import pytest
import pytest_asyncio
from dotenv import load_dotenv

from kinesis import Consumer, Producer

# Load environment variables
load_dotenv()

# Test configuration
ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4567")
TESTING_USE_AWS_KINESIS = os.environ.get("TESTING_USE_AWS_KINESIS", "0") == "1"

# Set up Redis port for docker-compose
if "REDIS_PORT" not in os.environ:
    os.environ["REDIS_PORT"] = "16379"

# Set default AWS region for tests
if "AWS_DEFAULT_REGION" not in os.environ:
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("aiobotocore").setLevel(logging.INFO)


@pytest.fixture
def random_stream_name():
    """Generate a random stream name for testing."""
    return f"test_{str(uuid.uuid4())[0:8]}"


@pytest.fixture
def endpoint_url():
    """Provide the endpoint URL for testing."""
    return ENDPOINT_URL


@pytest.fixture
def aws_testing_enabled():
    """Check if AWS testing is enabled."""
    return TESTING_USE_AWS_KINESIS


@pytest_asyncio.fixture
async def test_stream(random_stream_name, endpoint_url):
    """Create a test stream and clean up after test."""
    # Create the stream
    async with Producer(
        stream_name=random_stream_name,
        endpoint_url=endpoint_url,
        region_name="ap-southeast-2",
        create_stream=True,
        create_stream_shards=1,
    ):
        pass  # Stream created

    yield random_stream_name

    # Cleanup would go here if needed
    # For now, kinesalite doesn't persist between runs


@pytest_asyncio.fixture
async def producer(random_stream_name, endpoint_url):
    """Create a producer for testing."""
    async with Producer(
        stream_name=random_stream_name,
        endpoint_url=endpoint_url,
        region_name="ap-southeast-2",
        create_stream=True,
        create_stream_shards=1,
    ) as prod:
        yield prod


@pytest_asyncio.fixture
async def consumer(random_stream_name, endpoint_url):
    """Create a consumer for testing."""
    async with Consumer(
        stream_name=random_stream_name,
        endpoint_url=endpoint_url,
        region_name="ap-southeast-2",
    ) as cons:
        yield cons


@pytest.fixture
def random_string():
    """Generate random strings for testing."""

    def _random_string(length):
        from random import choice
        from string import ascii_uppercase

        return "".join(choice(ascii_uppercase) for i in range(length))

    return _random_string


# Skip markers for conditional tests
skip_if_no_aws = pytest.mark.skipif(
    not TESTING_USE_AWS_KINESIS,
    reason="AWS testing not enabled (set TESTING_USE_AWS_KINESIS=1)",
)

skip_if_no_redis = pytest.mark.skipif(not os.environ.get("REDIS_HOST"), reason="Redis not available (set REDIS_HOST)")
