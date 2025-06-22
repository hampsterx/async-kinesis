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
                mock_session.close = AsyncMock()
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
        """Test manual checkpointing mode."""
        checkpointer = DynamoDBCheckPointer("test-app", auto_checkpoint=False)
        await checkpointer._initialize()

        # Manual checkpoint should not call DynamoDB immediately
        await checkpointer.checkpoint("shard-001", "seq-123")

        assert not mock_dynamodb["table"].update_item.called
        assert checkpointer._manual_checkpoints["shard-001"] == "seq-123"

        # Manual checkpoint should update DynamoDB
        await checkpointer.manual_checkpoint()

        assert mock_dynamodb["table"].update_item.called

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
