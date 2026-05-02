"""Tests for DynamoDB checkpointer."""

import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Mock the imports before importing DynamoDBCheckPointer
mock_aioboto3 = MagicMock()
mock_botocore = MagicMock()


# Mock ClientError for tests
class ClientError(Exception):
    def __init__(self, error_response, operation_name):
        self.response = error_response
        self.operation_name = operation_name
        super().__init__(f"{operation_name}: {error_response}")


mock_botocore.exceptions.ClientError = ClientError

with patch.dict(
    sys.modules,
    {
        "aioboto3": mock_aioboto3,
        "botocore.exceptions": mock_botocore.exceptions,
    },
):
    # Mock the HAS_DYNAMODB flag
    with patch("kinesis.dynamodb.HAS_DYNAMODB", True):
        with patch("kinesis.dynamodb.aioboto3", mock_aioboto3):
            with patch("kinesis.dynamodb.ClientError", ClientError):
                from kinesis.dynamodb import DynamoDBCheckPointer


@patch("kinesis.dynamodb.HAS_DYNAMODB", True)
@pytest.mark.dynamodb
class TestDynamoDBCheckPointer:
    """Test DynamoDB checkpointer functionality."""

    @pytest.fixture
    def mock_dynamodb(self):
        """Mock DynamoDB resource."""
        with patch("kinesis.dynamodb.ClientError", ClientError):
            with patch("kinesis.dynamodb.aioboto3") as mock_aioboto3:
                # Mock session
                mock_session = MagicMock()
                mock_aioboto3.Session.return_value = mock_session

                # Mock DynamoDB resource
                mock_dynamodb = AsyncMock()
                mock_table = AsyncMock()

                # Configure async context managers
                mock_resource_cm = AsyncMock()
                mock_resource_cm.__aenter__.return_value = mock_dynamodb
                mock_resource_cm.__aexit__.return_value = None
                mock_session.resource.return_value = mock_resource_cm

                mock_client_cm = AsyncMock()
                mock_client_cm.__aenter__.return_value = AsyncMock()
                mock_client_cm.__aexit__.return_value = None
                mock_session.client.return_value = mock_client_cm

                mock_dynamodb.Table.return_value = mock_table
                mock_dynamodb.create_table = AsyncMock(return_value=mock_table)

                # Mock table methods
                mock_table.load = AsyncMock()
                mock_table.wait_until_exists = AsyncMock()
                mock_table.put_item = AsyncMock()
                mock_table.get_item = AsyncMock()
                mock_table.update_item = AsyncMock()

                yield {
                    "session": mock_session,
                    "dynamodb": mock_dynamodb,
                    "table": mock_table,
                    "aioboto3": mock_aioboto3,
                }

    @pytest.mark.asyncio
    async def test_initialize_existing_table(self, mock_dynamodb):
        """Test initialization with existing table."""
        checkpointer = DynamoDBCheckPointer("test-app")

        # Initialize should load existing table
        await checkpointer._initialize()

        mock_dynamodb["dynamodb"].Table.assert_called_once_with("kinesis-checkpoints-test-app")
        mock_dynamodb["table"].load.assert_called_once()

    @pytest.mark.asyncio
    async def test_initialize_create_table(self, mock_dynamodb):
        """Test table creation when it doesn't exist."""
        # Mock table not found error
        mock_dynamodb["table"].load.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable"
        )

        checkpointer = DynamoDBCheckPointer("test-app", create_table=True)
        await checkpointer._initialize()

        # Should create table
        mock_dynamodb["dynamodb"].create_table.assert_called_once()
        call_args = mock_dynamodb["dynamodb"].create_table.call_args[1]
        assert call_args["TableName"] == "kinesis-checkpoints-test-app"
        assert call_args["BillingMode"] == "PAY_PER_REQUEST"

    @pytest.mark.asyncio
    async def test_allocate_new_shard(self, mock_dynamodb):
        """Test allocating a new shard."""
        checkpointer = DynamoDBCheckPointer("test-app")
        await checkpointer._initialize()

        # Mock successful put_item (new shard)
        mock_dynamodb["table"].put_item.return_value = {}

        success, sequence = await checkpointer.allocate("shard-001")

        assert success is True
        assert sequence is None
        assert "shard-001" in checkpointer._items

        # Verify put_item was called with correct params
        mock_dynamodb["table"].put_item.assert_called_once()
        call_args = mock_dynamodb["table"].put_item.call_args[1]
        assert call_args["Item"]["shard_id"] == "shard-001"
        assert call_args["Item"]["ref"] == checkpointer.get_ref()

    @pytest.mark.asyncio
    async def test_allocate_existing_shard_available(self, mock_dynamodb):
        """Test allocating an existing shard that's available."""
        checkpointer = DynamoDBCheckPointer("test-app")
        await checkpointer._initialize()

        # Mock shard already exists
        mock_dynamodb["table"].put_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem"
        )

        # Mock get_item returns expired shard
        old_ts = checkpointer.get_ts() - 120  # 2 minutes ago
        mock_dynamodb["table"].get_item.return_value = {
            "Item": {"shard_id": "shard-001", "ref": "old-consumer", "ts": old_ts, "sequence": "seq-123"}
        }

        # Mock successful update
        mock_dynamodb["table"].update_item.return_value = {}

        success, sequence = await checkpointer.allocate("shard-001")

        assert success is True
        assert sequence == "seq-123"
        assert checkpointer._items["shard-001"] == "seq-123"

    @pytest.mark.asyncio
    async def test_allocate_existing_shard_busy(self, mock_dynamodb):
        """Test allocating a shard that's still in use."""
        checkpointer = DynamoDBCheckPointer("test-app")
        await checkpointer._initialize()

        # Mock shard already exists
        mock_dynamodb["table"].put_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem"
        )

        # Mock get_item returns active shard
        current_ts = checkpointer.get_ts()
        mock_dynamodb["table"].get_item.return_value = {
            "Item": {"shard_id": "shard-001", "ref": "other-consumer", "ts": current_ts, "sequence": "seq-123"}
        }

        success, sequence = await checkpointer.allocate("shard-001")

        assert success is False
        assert sequence is None
        assert "shard-001" not in checkpointer._items

    @pytest.mark.asyncio
    async def test_checkpoint(self, mock_dynamodb):
        """Test checkpointing a shard."""
        checkpointer = DynamoDBCheckPointer("test-app")
        await checkpointer._initialize()

        # First allocate the shard
        checkpointer._items["shard-001"] = None

        # Mock successful update
        mock_dynamodb["table"].update_item.return_value = {}

        await checkpointer.checkpoint("shard-001", "seq-456")

        assert checkpointer._items["shard-001"] == "seq-456"

        # Verify update_item was called
        mock_dynamodb["table"].update_item.assert_called_once()
        call_args = mock_dynamodb["table"].update_item.call_args[1]
        assert call_args["Key"]["shard_id"] == "shard-001"
        assert ":seq" in call_args["ExpressionAttributeValues"]
        assert call_args["ExpressionAttributeValues"][":seq"] == "seq-456"

    @pytest.mark.asyncio
    async def test_checkpoint_not_owner(self, mock_dynamodb):
        """Test checkpointing fails if not owner."""
        checkpointer = DynamoDBCheckPointer("test-app")
        await checkpointer._initialize()

        # Mock conditional check failure
        mock_dynamodb["table"].update_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException"}}, "UpdateItem"
        )

        with pytest.raises(Exception, match="does not own it"):
            await checkpointer.checkpoint("shard-001", "seq-456")

    @pytest.mark.asyncio
    async def test_deallocate(self, mock_dynamodb):
        """Test deallocating a shard."""
        checkpointer = DynamoDBCheckPointer("test-app")
        await checkpointer._initialize()

        # Set up shard ownership
        checkpointer._items["shard-001"] = "seq-789"

        # Mock successful update
        mock_dynamodb["table"].update_item.return_value = {}

        await checkpointer.deallocate("shard-001")

        assert "shard-001" not in checkpointer._items

        # Verify update cleared ownership
        mock_dynamodb["table"].update_item.assert_called_once()
        call_args = mock_dynamodb["table"].update_item.call_args[1]
        assert call_args["ExpressionAttributeValues"][":null"] is None
        assert call_args["ExpressionAttributeValues"][":seq"] == "seq-789"

    @pytest.mark.asyncio
    async def test_heartbeat(self, mock_dynamodb):
        """Test heartbeat updates."""
        checkpointer = DynamoDBCheckPointer("test-app", heartbeat_frequency=0.1)
        await checkpointer._initialize()

        # Set up shard ownership
        checkpointer._items["shard-001"] = "seq-123"

        # Mock successful update
        mock_dynamodb["table"].update_item.return_value = {}

        # Wait for heartbeat
        await asyncio.sleep(0.15)

        # Verify heartbeat update was called
        assert mock_dynamodb["table"].update_item.called
        call_args = mock_dynamodb["table"].update_item.call_args[1]
        assert call_args["Key"]["shard_id"] == "shard-001"
        assert "ConditionExpression" in call_args  # Should check ownership

    @pytest.mark.asyncio
    async def test_custom_table_name(self, mock_dynamodb):
        """Test using custom table name."""
        checkpointer = DynamoDBCheckPointer("test-app", table_name="custom-table")
        await checkpointer._initialize()

        mock_dynamodb["dynamodb"].Table.assert_called_once_with("custom-table")

    @pytest.mark.asyncio
    async def test_ttl_values(self, mock_dynamodb):
        """Test TTL values are set correctly."""
        checkpointer = DynamoDBCheckPointer("test-app", ttl_hours=48)
        await checkpointer._initialize()

        mock_dynamodb["table"].put_item.return_value = {}

        await checkpointer.allocate("shard-001")

        call_args = mock_dynamodb["table"].put_item.call_args[1]
        item = call_args["Item"]

        # TTL should be 48 hours in the future
        expected_ttl = checkpointer.get_ts() + (48 * 3600)
        assert abs(item["ttl"] - expected_ttl) < 2  # Allow small time difference

    @pytest.mark.asyncio
    async def test_manual_checkpoint(self, mock_dynamodb):
        """Test manual checkpointing mode + metrics emission on backend write."""
        from kinesis import InMemoryMetricsCollector

        collector = InMemoryMetricsCollector()
        checkpointer = DynamoDBCheckPointer("test-app", auto_checkpoint=False)
        checkpointer.bind_metrics(collector, {"stream_name": "test-stream"})
        await checkpointer._initialize()

        # Manual mode: checkpoint() buffers, does NOT call DynamoDB or emit metrics.
        await checkpointer.checkpoint("shard-001", "seq-123")
        await checkpointer.checkpoint("shard-002", "seq-456")

        assert not mock_dynamodb["table"].update_item.called
        assert checkpointer._manual_checkpoints == {"shard-001": "seq-123", "shard-002": "seq-456"}
        assert not any(k.startswith("consumer_checkpoint_") for k in collector.counters)

        # manual_checkpoint() drives real backend writes and emits one success per shard.
        await checkpointer.manual_checkpoint()

        assert mock_dynamodb["table"].update_item.called
        assert collector.counters["consumer_checkpoint_success_total{shard_id=shard-001,stream_name=test-stream}"] == 1
        assert collector.counters["consumer_checkpoint_success_total{shard_id=shard-002,stream_name=test-stream}"] == 1

    @pytest.mark.asyncio
    async def test_manual_checkpoint_retains_buffer_on_mid_loop_failure(self, mock_dynamodb):
        """A per-shard flush raise leaves only the failing shard in the buffer
        so the caller can retry on the next manual_checkpoint() call."""
        from kinesis import CheckpointFlushError

        checkpointer = DynamoDBCheckPointer("test-app", auto_checkpoint=False)
        await checkpointer._initialize()

        await checkpointer.checkpoint("shard-001", "seq-123")
        await checkpointer.checkpoint("shard-002", "seq-456")

        # First update_item succeeds, second raises a transient backend error.
        mock_dynamodb["table"].update_item.side_effect = [
            {},
            RuntimeError("backend flake"),
        ]

        with pytest.raises(CheckpointFlushError) as exc_info:
            await checkpointer.manual_checkpoint()

        assert [shard_id for shard_id, _ in exc_info.value.errors] == ["shard-002"]
        assert isinstance(exc_info.value.errors[0][1], RuntimeError)

        # shard-001 was popped on success, shard-002 raised before pop and stays
        # buffered for retry. If the clear-before-iterate bug regressed, both
        # would have been dropped and this would fail.
        assert checkpointer._manual_checkpoints == {"shard-002": "seq-456"}

        # A subsequent successful flush drains the remaining entry.
        mock_dynamodb["table"].update_item.side_effect = None
        mock_dynamodb["table"].update_item.return_value = {}
        await checkpointer.manual_checkpoint()
        assert checkpointer._manual_checkpoints == {}

    @pytest.mark.asyncio
    async def test_manual_checkpoint_continues_past_head_failure(self, mock_dynamodb):
        """Issue #79: a shard that fails at the head of the buffer must not
        block healthy shards behind it from being flushed. Poison-pill fix."""
        from kinesis import CheckpointFlushError

        checkpointer = DynamoDBCheckPointer("test-app", auto_checkpoint=False)
        await checkpointer._initialize()

        await checkpointer.checkpoint("shard-001", "seq-123")
        await checkpointer.checkpoint("shard-002", "seq-456")

        # shard-001 raises (poison), shard-002 succeeds. Without the fix the
        # loop would abort on shard-001 and never attempt shard-002.
        mock_dynamodb["table"].update_item.side_effect = [
            RuntimeError("poison"),
            {},
        ]

        with pytest.raises(CheckpointFlushError) as exc_info:
            await checkpointer.manual_checkpoint()

        assert [shard_id for shard_id, _ in exc_info.value.errors] == ["shard-001"]

        # shard-001 still buffered, shard-002 was attempted and popped.
        assert checkpointer._manual_checkpoints == {"shard-001": "seq-123"}

    @pytest.mark.asyncio
    async def test_manual_checkpoint_collects_all_failures(self, mock_dynamodb):
        """When every shard fails, CheckpointFlushError.errors lists every
        failure in insertion order; the buffer is unchanged."""
        from kinesis import CheckpointFlushError

        checkpointer = DynamoDBCheckPointer("test-app", auto_checkpoint=False)
        await checkpointer._initialize()

        await checkpointer.checkpoint("shard-001", "seq-123")
        await checkpointer.checkpoint("shard-002", "seq-456")

        mock_dynamodb["table"].update_item.side_effect = [
            RuntimeError("first"),
            RuntimeError("second"),
        ]

        with pytest.raises(CheckpointFlushError) as exc_info:
            await checkpointer.manual_checkpoint()

        assert [shard_id for shard_id, _ in exc_info.value.errors] == ["shard-001", "shard-002"]
        assert [str(exc) for _, exc in exc_info.value.errors] == ["first", "second"]
        assert checkpointer._manual_checkpoints == {"shard-001": "seq-123", "shard-002": "seq-456"}

    @pytest.mark.asyncio
    async def test_checkpoint_failure_emits_metric(self, mock_dynamodb):
        """ConditionalCheckFailedException (lost ownership) counts as a failure."""
        from kinesis import InMemoryMetricsCollector

        collector = InMemoryMetricsCollector()
        checkpointer = DynamoDBCheckPointer("test-app")
        checkpointer.bind_metrics(collector, {"stream_name": "test-stream"})
        await checkpointer._initialize()

        mock_dynamodb["table"].update_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException"}}, "UpdateItem"
        )

        with pytest.raises(Exception, match="does not own it"):
            await checkpointer._checkpoint("shard-001", "seq-123")

        assert collector.counters["consumer_checkpoint_failure_total{shard_id=shard-001,stream_name=test-stream}"] == 1
        assert not any(k.startswith("consumer_checkpoint_success_") for k in collector.counters)

    @pytest.mark.asyncio
    async def test_close(self, mock_dynamodb):
        """Test closing the checkpointer."""
        checkpointer = DynamoDBCheckPointer("test-app")
        await checkpointer._initialize()

        # Set up some shards
        checkpointer._items["shard-001"] = "seq-123"
        checkpointer._items["shard-002"] = "seq-456"

        mock_dynamodb["table"].update_item.return_value = {}

        await checkpointer.close()

        # Should deallocate all shards
        assert mock_dynamodb["table"].update_item.call_count >= 2
        assert len(checkpointer._items) == 0

    def test_import_error(self):
        """Test error when aioboto3 is not installed."""
        with patch("kinesis.dynamodb.HAS_DYNAMODB", False):
            with pytest.raises(ImportError, match="DynamoDB support requires aioboto3"):
                DynamoDBCheckPointer("test-app")

    @pytest.mark.asyncio
    async def test_no_create_table_error(self, mock_dynamodb):
        """Test error when table doesn't exist and create_table=False."""
        mock_dynamodb["table"].load.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable"
        )

        checkpointer = DynamoDBCheckPointer("test-app", create_table=False)

        with pytest.raises(Exception, match="does not exist"):
            await checkpointer._initialize()

    @pytest.mark.asyncio
    async def test_allocate_race_condition_retry(self, mock_dynamodb):
        """Test allocation retries on race condition."""
        checkpointer = DynamoDBCheckPointer("test-app")
        await checkpointer._initialize()

        # Mock shard already exists
        mock_dynamodb["table"].put_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem"
        )

        # Mock get_item returns empty on first two calls (race condition), then returns item
        mock_dynamodb["table"].get_item.side_effect = [
            {},  # First call - no item (race condition)
            {},  # Second call - no item (race condition)
            {  # Third call - item exists
                "Item": {"shard_id": "shard-001", "ref": None, "ts": None, "sequence": "seq-123"}
            },
        ]

        # Mock successful update
        mock_dynamodb["table"].update_item.return_value = {}

        success, sequence = await checkpointer.allocate("shard-001")

        assert success is True
        assert sequence == "seq-123"
        # Verify get_item was called 3 times
        assert mock_dynamodb["table"].get_item.call_count == 3

    @pytest.mark.asyncio
    async def test_allocate_race_condition_max_retries(self, mock_dynamodb):
        """Test allocation fails after max retries on persistent race condition."""
        checkpointer = DynamoDBCheckPointer("test-app")
        await checkpointer._initialize()

        # Mock shard already exists
        mock_dynamodb["table"].put_item.side_effect = ClientError(
            {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem"
        )

        # Mock get_item always returns empty (persistent race condition)
        mock_dynamodb["table"].get_item.return_value = {}

        success, sequence = await checkpointer.allocate("shard-001")

        assert success is False
        assert sequence is None
        # Verify get_item was called 3 times (max retries)
        assert mock_dynamodb["table"].get_item.call_count == 3

    # ------------------------------------------------------------------
    # endpoint_url propagation regression tests
    #
    # Historically, endpoint_url was only honoured during _initialize() (table
    # discovery). Every subsequent per-operation session.resource / session.client
    # call bypassed it and silently hit real AWS, breaking LocalStack / DynamoDB
    # Local based dev and testing for any flow beyond startup. These tests assert
    # that endpoint_url is threaded through each of the 5 remaining call sites.
    # ------------------------------------------------------------------

    ENDPOINT_URL = "http://localhost:4566"

    def _assert_endpoint_url_in_last_resource_call(self, mock_session):
        """session.resource(...) most recent call must include endpoint_url kwarg."""
        assert mock_session.resource.called, "session.resource was not called"
        _args, kwargs = mock_session.resource.call_args
        assert (
            kwargs.get("endpoint_url") == self.ENDPOINT_URL
        ), f"expected endpoint_url={self.ENDPOINT_URL!r}, got kwargs={kwargs!r}"

    def _assert_endpoint_url_in_last_client_call(self, mock_session):
        """session.client(...) most recent call must include endpoint_url kwarg."""
        assert mock_session.client.called, "session.client was not called"
        _args, kwargs = mock_session.client.call_args
        assert (
            kwargs.get("endpoint_url") == self.ENDPOINT_URL
        ), f"expected endpoint_url={self.ENDPOINT_URL!r}, got kwargs={kwargs!r}"

    @pytest.mark.asyncio
    async def test_endpoint_url_propagates_to_do_heartbeat(self, mock_dynamodb):
        checkpointer = DynamoDBCheckPointer("test-app", endpoint_url=self.ENDPOINT_URL)
        await checkpointer._initialize()
        mock_dynamodb["session"].resource.reset_mock()

        await checkpointer.do_heartbeat("shard-001", {"ref": "x", "ts": 1, "sequence": "s"})

        self._assert_endpoint_url_in_last_resource_call(mock_dynamodb["session"])

    @pytest.mark.asyncio
    async def test_endpoint_url_propagates_to_checkpoint(self, mock_dynamodb):
        checkpointer = DynamoDBCheckPointer("test-app", endpoint_url=self.ENDPOINT_URL)
        await checkpointer._initialize()
        mock_dynamodb["session"].resource.reset_mock()

        mock_dynamodb["table"].update_item.return_value = {}
        await checkpointer._checkpoint("shard-001", "seq-123")

        self._assert_endpoint_url_in_last_resource_call(mock_dynamodb["session"])

    @pytest.mark.asyncio
    async def test_endpoint_url_propagates_to_deallocate(self, mock_dynamodb):
        checkpointer = DynamoDBCheckPointer("test-app", endpoint_url=self.ENDPOINT_URL)
        await checkpointer._initialize()
        checkpointer._items["shard-001"] = "seq-123"
        mock_dynamodb["session"].resource.reset_mock()

        mock_dynamodb["table"].update_item.return_value = {}
        await checkpointer.deallocate("shard-001")

        self._assert_endpoint_url_in_last_resource_call(mock_dynamodb["session"])

    @pytest.mark.asyncio
    async def test_endpoint_url_propagates_to_allocate(self, mock_dynamodb):
        checkpointer = DynamoDBCheckPointer("test-app", endpoint_url=self.ENDPOINT_URL)
        await checkpointer._initialize()
        mock_dynamodb["session"].resource.reset_mock()

        mock_dynamodb["table"].put_item.return_value = {}
        await checkpointer.allocate("shard-001")

        self._assert_endpoint_url_in_last_resource_call(mock_dynamodb["session"])

    @pytest.mark.asyncio
    async def test_endpoint_url_propagates_to_create_table_ttl(self, mock_dynamodb):
        """_create_table's TTL update uses session.client; a missed kwarg would be
        hidden by the ClientError-is-logged-as-warning fallback, so assert explicitly."""
        mock_dynamodb["table"].load.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "DescribeTable"
        )

        checkpointer = DynamoDBCheckPointer("test-app", endpoint_url=self.ENDPOINT_URL, create_table=True)
        await checkpointer._initialize()

        self._assert_endpoint_url_in_last_client_call(mock_dynamodb["session"])
