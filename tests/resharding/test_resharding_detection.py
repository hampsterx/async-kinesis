#!/usr/bin/env python3
"""
Test the new resharding detection and status handling functionality.

Tests specifically for:
1. is_resharding_likely_in_progress() method
2. Enhanced shard status reporting with resharding_in_progress field
3. Stream status handling (UPDATING, CREATING, etc.)
4. Proper handling of UPDATING streams during shard refresh
"""

import asyncio
import logging
import os

# Add the parent directory to Python path for imports
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from kinesis import Consumer

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class ReshardingDetectionTest:
    """Test resharding detection functionality"""

    def create_mock_consumer(self, stream_name: str):
        """Create a consumer for testing without connecting"""
        return Consumer(
            stream_name=stream_name,
            endpoint_url="http://mock:4566",  # Won't actually connect
            create_stream=False,
        )

    async def test_resharding_detection_no_topology(self):
        """Test resharding detection with no parent-child relationships"""
        logger.info("=== Testing No Resharding Scenario ===")

        consumer = self.create_mock_consumer("test-no-resharding")

        # Simulate normal stream with independent shards
        consumer.shards = [
            {"ShardId": "shard-000000000000"},
            {"ShardId": "shard-000000000001"},
            {"ShardId": "shard-000000000002"},
        ]

        # No parent-child relationships
        consumer._parent_shards = set()
        consumer._child_shards = set()
        consumer._closed_shards = set()
        consumer._exhausted_parents = set()
        consumer._shard_topology = {}

        # Test detection
        resharding_detected = consumer.is_resharding_likely_in_progress()
        status = consumer.get_shard_status()

        assert resharding_detected == False, "Should not detect resharding with no topology"
        assert status["resharding_in_progress"] == False, "Status should indicate no resharding"
        assert status["parent_shards"] == 0, "Should have no parent shards"
        assert status["child_shards"] == 0, "Should have no child shards"

        logger.info("✅ No resharding scenario detected correctly")
        return {"test": "no_resharding", "status": "PASS"}

    async def test_resharding_detection_active_parents(self):
        """Test resharding detection with active parent shards"""
        logger.info("=== Testing Active Resharding Scenario ===")

        consumer = self.create_mock_consumer("test-active-resharding")

        # Simulate stream with active parent and children (mid-resharding)
        consumer.shards = [
            {"ShardId": "shard-parent-001"},
            {"ShardId": "shard-child-001"},
            {"ShardId": "shard-child-002"},
        ]

        # Set up parent-child topology
        consumer._parent_shards = {"shard-parent-001"}
        consumer._child_shards = {"shard-child-001", "shard-child-002"}
        consumer._closed_shards = set()  # Parent not closed yet
        consumer._exhausted_parents = set()  # Parent not exhausted yet
        consumer._shard_topology = {
            "shard-parent-001": {
                "parent": None,
                "children": {"shard-child-001", "shard-child-002"},
            },
            "shard-child-001": {"parent": "shard-parent-001", "children": set()},
            "shard-child-002": {"parent": "shard-parent-001", "children": set()},
        }

        # Test detection
        resharding_detected = consumer.is_resharding_likely_in_progress()
        status = consumer.get_shard_status()

        assert resharding_detected == True, "Should detect active resharding"
        assert status["resharding_in_progress"] == True, "Status should indicate resharding in progress"
        assert status["parent_shards"] == 1, "Should have 1 parent shard"
        assert status["child_shards"] == 2, "Should have 2 child shards"

        logger.info("✅ Active resharding scenario detected correctly")
        return {"test": "active_resharding", "status": "PASS"}

    async def test_resharding_detection_completed_resharding(self):
        """Test resharding detection after resharding is complete"""
        logger.info("=== Testing Completed Resharding Scenario ===")

        consumer = self.create_mock_consumer("test-completed-resharding")

        # Simulate stream after resharding (parent closed, children active)
        consumer.shards = [
            {"ShardId": "shard-parent-001"},  # Closed
            {"ShardId": "shard-child-001"},  # Active
            {"ShardId": "shard-child-002"},  # Active
        ]

        # Set up completed resharding topology
        consumer._parent_shards = {"shard-parent-001"}
        consumer._child_shards = {"shard-child-001", "shard-child-002"}
        consumer._closed_shards = {"shard-parent-001"}  # Parent is closed
        consumer._exhausted_parents = {"shard-parent-001"}  # Parent is exhausted
        consumer._shard_topology = {
            "shard-parent-001": {
                "parent": None,
                "children": {"shard-child-001", "shard-child-002"},
            },
            "shard-child-001": {"parent": "shard-parent-001", "children": set()},
            "shard-child-002": {"parent": "shard-parent-001", "children": set()},
        }

        # Test detection
        resharding_detected = consumer.is_resharding_likely_in_progress()
        status = consumer.get_shard_status()

        # Resharding is complete but topology exists
        assert resharding_detected == False, "Should not detect active resharding when complete"
        assert status["resharding_in_progress"] == False, "Status should indicate resharding complete"
        assert status["parent_shards"] == 1, "Should still have 1 parent shard"
        assert status["child_shards"] == 2, "Should still have 2 child shards"
        assert status["closed_shards"] == 1, "Should have 1 closed shard"
        assert status["exhausted_parents"] == 1, "Should have 1 exhausted parent"

        logger.info("✅ Completed resharding scenario detected correctly")
        return {"test": "completed_resharding", "status": "PASS"}

    async def test_resharding_detection_high_closed_ratio(self):
        """Test resharding detection based on high closed shard ratio"""
        logger.info("=== Testing High Closed Shard Ratio Scenario ===")

        consumer = self.create_mock_consumer("test-high-closed-ratio")

        # Simulate stream with many closed shards (post-resharding)
        consumer.shards = [
            {"ShardId": "shard-000000000000"},  # Closed
            {"ShardId": "shard-000000000001"},  # Closed
            {"ShardId": "shard-000000000002"},  # Active
            {"ShardId": "shard-000000000003"},  # Active
        ]

        # High ratio of closed shards (50% > 30% threshold)
        consumer._parent_shards = set()
        consumer._child_shards = set()
        consumer._closed_shards = {"shard-000000000000", "shard-000000000001"}
        consumer._exhausted_parents = set()
        consumer._shard_topology = {}

        # Test detection
        resharding_detected = consumer.is_resharding_likely_in_progress()
        status = consumer.get_shard_status()

        assert resharding_detected == True, "Should detect resharding based on high closed ratio"
        assert status["resharding_in_progress"] == True, "Status should indicate resharding detected"
        assert status["closed_shards"] == 2, "Should have 2 closed shards"
        assert status["total_shards"] == 4, "Should have 4 total shards"

        logger.info("✅ High closed ratio scenario detected correctly")
        return {"test": "high_closed_ratio", "status": "PASS"}

    async def test_stream_status_constants(self):
        """Test that all stream status constants are properly defined"""
        logger.info("=== Testing Stream Status Constants ===")

        consumer = self.create_mock_consumer("test-constants")

        # Test all status constants exist
        assert hasattr(consumer, "ACTIVE"), "Should have ACTIVE constant"
        assert hasattr(consumer, "UPDATING"), "Should have UPDATING constant"
        assert hasattr(consumer, "CREATING"), "Should have CREATING constant"
        assert hasattr(consumer, "DELETING"), "Should have DELETING constant"
        assert hasattr(consumer, "INITIALIZE"), "Should have INITIALIZE constant"
        assert hasattr(consumer, "RECONNECT"), "Should have RECONNECT constant"

        # Test constant values
        assert consumer.ACTIVE == "ACTIVE", "ACTIVE constant should be 'ACTIVE'"
        assert consumer.UPDATING == "UPDATING", "UPDATING constant should be 'UPDATING'"
        assert consumer.CREATING == "CREATING", "CREATING constant should be 'CREATING'"
        assert consumer.DELETING == "DELETING", "DELETING constant should be 'DELETING'"

        logger.info("✅ All stream status constants defined correctly")
        return {"test": "status_constants", "status": "PASS"}

    async def test_shard_status_comprehensive(self):
        """Test comprehensive shard status reporting"""
        logger.info("=== Testing Comprehensive Shard Status ===")

        consumer = self.create_mock_consumer("test-comprehensive-status")

        # Create complex scenario
        consumer.shards = [
            {
                "ShardId": "shard-parent-A",
                "SequenceNumberRange": {
                    "StartingSequenceNumber": "100",
                    "EndingSequenceNumber": "200",
                },
                "ParentShardId": None,
            },
            {
                "ShardId": "shard-child-A1",
                "SequenceNumberRange": {"StartingSequenceNumber": "201"},
                "ParentShardId": "shard-parent-A",
            },
            {
                "ShardId": "shard-child-A2",
                "SequenceNumberRange": {"StartingSequenceNumber": "202"},
                "ParentShardId": "shard-parent-A",
            },
            {
                "ShardId": "shard-independent-B",
                "SequenceNumberRange": {"StartingSequenceNumber": "300"},
                "ParentShardId": None,
            },
        ]

        # Build topology
        consumer._build_shard_topology(consumer.shards)
        consumer._closed_shards.add("shard-parent-A")
        consumer._exhausted_parents.add("shard-parent-A")

        # Get comprehensive status
        status = consumer.get_shard_status()

        # Verify all fields are present and correct
        required_fields = [
            "total_shards",
            "active_shards",
            "closed_shards",
            "allocated_shards",
            "parent_shards",
            "child_shards",
            "exhausted_parents",
            "resharding_in_progress",
            "topology",
            "shard_details",
        ]

        for field in required_fields:
            assert field in status, f"Status should include {field}"

        assert status["total_shards"] == 4, "Should have 4 total shards"
        assert status["parent_shards"] == 1, "Should have 1 parent shard"
        assert status["child_shards"] == 2, "Should have 2 child shards"
        assert status["closed_shards"] == 1, "Should have 1 closed shard"
        assert status["exhausted_parents"] == 1, "Should have 1 exhausted parent"

        # Check topology structure
        assert "parent_child_map" in status["topology"], "Should have parent-child map"
        assert "child_parent_map" in status["topology"], "Should have child-parent map"

        # Check shard details
        assert len(status["shard_details"]) == 4, "Should have details for all 4 shards"

        shard_detail_fields = [
            "shard_id",
            "is_allocated",
            "is_closed",
            "has_iterator",
            "sequence_range",
            "parent_shard_id",
            "is_parent",
            "is_child",
            "can_allocate",
        ]

        for detail in status["shard_details"]:
            for field in shard_detail_fields:
                assert field in detail, f"Shard detail should include {field}"

        logger.info("✅ Comprehensive shard status reporting working correctly")
        return {"test": "comprehensive_status", "status": "PASS"}

    async def run_all_tests(self):
        """Run all resharding detection tests"""
        logger.info("🚀 Starting Resharding Detection Tests")
        logger.info("=" * 60)

        tests = [
            self.test_resharding_detection_no_topology,
            self.test_resharding_detection_active_parents,
            self.test_resharding_detection_completed_resharding,
            self.test_resharding_detection_high_closed_ratio,
            self.test_stream_status_constants,
            self.test_shard_status_comprehensive,
        ]

        results = []
        for test in tests:
            try:
                result = await test()
                results.append(result)
                logger.info(f"✅ {result['test']}: {result['status']}")
            except Exception as e:
                logger.error(f"❌ {test.__name__}: FAIL - {e}")
                results.append({"test": test.__name__, "status": "FAIL", "error": str(e)})

        # Summary
        logger.info("=" * 60)
        passed = len([r for r in results if r["status"] == "PASS"])
        total = len(results)

        if passed == total:
            logger.info(f"🎉 ALL RESHARDING DETECTION TESTS PASSED ({passed}/{total})")
            logger.info("✅ Resharding detection logic working correctly")
            logger.info("✅ Stream status constants properly defined")
            logger.info("✅ Comprehensive status reporting available")
        else:
            logger.error(f"⚠️ {total - passed} tests failed ({passed}/{total} passed)")

        return results


async def main():
    """Run resharding detection tests"""
    test_suite = ReshardingDetectionTest()
    results = await test_suite.run_all_tests()

    # Print detailed results
    print("\n" + "=" * 60)
    print("RESHARDING DETECTION TEST RESULTS")
    print("=" * 60)

    for result in results:
        status_icon = "✅" if result["status"] == "PASS" else "❌"
        print(f"{status_icon} {result['test']}: {result['status']}")
        if "error" in result:
            print(f"   Error: {result['error']}")

    return len([r for r in results if r["status"] == "PASS"]) == len(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
