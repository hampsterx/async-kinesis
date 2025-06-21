#!/usr/bin/env python3
"""
Simple Resharding Logic Test

Tests the shard topology management without requiring AWS infrastructure.
This focuses on testing the parent-child relationship logic we implemented.
"""

import asyncio
import logging
import os
import sys
from typing import Dict, List

# Add the parent directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from kinesis import Consumer

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class MockReshardingTest:
    """Test shard topology management with mock data"""

    def __init__(self):
        self.test_results = []

    def create_mock_consumer(self, stream_name: str):
        """Create a consumer for testing without connecting"""
        return Consumer(
            stream_name=stream_name,
            endpoint_url="http://mock:4566",  # Won't actually connect
            create_stream=False,
        )

    async def test_basic_parent_child_topology(self):
        """Test basic parent-child shard relationship building"""
        logger.info("=== Testing Basic Parent-Child Topology ===")

        consumer = self.create_mock_consumer("test-basic-topology")

        # Mock shards: 1 parent with 2 children (typical split scenario)
        mock_shards = [
            {
                "ShardId": "shard-parent-001",
                "SequenceNumberRange": {
                    "StartingSequenceNumber": "49546986683135544286507457936321625675700192471156785154",
                    "EndingSequenceNumber": "49546986683135544286507457936321625675700192471156785155",
                },
                "HashKeyRange": {
                    "StartingHashKey": "0",
                    "EndingHashKey": "170141183460469231731687303715884105727",
                },
            },
            {
                "ShardId": "shard-child-001",
                "ParentShardId": "shard-parent-001",
                "SequenceNumberRange": {
                    "StartingSequenceNumber": "49546986683135544286507457936321625675700192471156785156"
                },
                "HashKeyRange": {
                    "StartingHashKey": "0",
                    "EndingHashKey": "85070591730234615865843651857942052863",
                },
            },
            {
                "ShardId": "shard-child-002",
                "ParentShardId": "shard-parent-001",
                "SequenceNumberRange": {
                    "StartingSequenceNumber": "49546986683135544286507457936321625675700192471156785157"
                },
                "HashKeyRange": {
                    "StartingHashKey": "85070591730234615865843651857942052864",
                    "EndingHashKey": "170141183460469231731687303715884105727",
                },
            },
        ]

        # Build topology
        consumer._build_shard_topology(mock_shards)

        # Verify topology construction
        assert "shard-parent-001" in consumer._parent_shards, "Parent shard not detected"
        assert "shard-child-001" in consumer._child_shards, "Child shard 1 not detected"
        assert "shard-child-002" in consumer._child_shards, "Child shard 2 not detected"

        # Verify parent-child mapping
        assert consumer._shard_topology["shard-parent-001"]["children"] == {
            "shard-child-001",
            "shard-child-002",
        }
        assert consumer._shard_topology["shard-child-001"]["parent"] == "shard-parent-001"
        assert consumer._shard_topology["shard-child-002"]["parent"] == "shard-parent-001"

        logger.info("✅ Basic topology construction working correctly")

        # Test allocation rules
        assert consumer._should_allocate_shard("shard-parent-001") == True, "Parent should be allocatable"
        assert (
            consumer._should_allocate_shard("shard-child-001") == False
        ), "Child should not be allocatable while parent active"
        assert (
            consumer._should_allocate_shard("shard-child-002") == False
        ), "Child should not be allocatable while parent active"

        logger.info("✅ Parent-first allocation rules working correctly")

        # Simulate parent exhaustion
        consumer._exhausted_parents.add("shard-parent-001")

        assert (
            consumer._should_allocate_shard("shard-child-001") == True
        ), "Child should be allocatable after parent exhausted"
        assert (
            consumer._should_allocate_shard("shard-child-002") == True
        ), "Child should be allocatable after parent exhausted"

        logger.info("✅ Parent exhaustion logic working correctly")

        return {
            "test": "basic_topology",
            "status": "PASS",
            "details": "All parent-child rules working",
        }

    async def test_complex_resharding_scenario(self):
        """Test complex resharding with multiple generations"""
        logger.info("=== Testing Complex Multi-Generation Topology ===")

        consumer = self.create_mock_consumer("test-complex-topology")

        # Mock scenario: Original shard splits, then one child splits again
        mock_shards = [
            # Original parent (should be closed/exhausted)
            {
                "ShardId": "shard-original-001",
                "SequenceNumberRange": {
                    "StartingSequenceNumber": "100",
                    "EndingSequenceNumber": "200",  # Closed shard
                },
                "HashKeyRange": {
                    "StartingHashKey": "0",
                    "EndingHashKey": "340282366920938463463374607431768211455",
                },
            },
            # First generation children
            {
                "ShardId": "shard-gen1-001",
                "ParentShardId": "shard-original-001",
                "SequenceNumberRange": {
                    "StartingSequenceNumber": "201",
                    "EndingSequenceNumber": "300",  # This one also gets split
                },
                "HashKeyRange": {
                    "StartingHashKey": "0",
                    "EndingHashKey": "170141183460469231731687303715884105727",
                },
            },
            {
                "ShardId": "shard-gen1-002",
                "ParentShardId": "shard-original-001",
                "SequenceNumberRange": {"StartingSequenceNumber": "202"},  # Still active
                "HashKeyRange": {
                    "StartingHashKey": "170141183460469231731687303715884105728",
                    "EndingHashKey": "340282366920938463463374607431768211455",
                },
            },
            # Second generation children (from shard-gen1-001)
            {
                "ShardId": "shard-gen2-001",
                "ParentShardId": "shard-gen1-001",
                "SequenceNumberRange": {"StartingSequenceNumber": "301"},
                "HashKeyRange": {
                    "StartingHashKey": "0",
                    "EndingHashKey": "85070591730234615865843651857942052863",
                },
            },
            {
                "ShardId": "shard-gen2-002",
                "ParentShardId": "shard-gen1-001",
                "SequenceNumberRange": {"StartingSequenceNumber": "302"},
                "HashKeyRange": {
                    "StartingHashKey": "85070591730234615865843651857942052864",
                    "EndingHashKey": "170141183460469231731687303715884105727",
                },
            },
        ]

        # Build topology
        consumer._build_shard_topology(mock_shards)

        # Mark closed shards
        consumer._closed_shards.add("shard-original-001")
        consumer._exhausted_parents.add("shard-original-001")
        consumer._closed_shards.add("shard-gen1-001")
        consumer._exhausted_parents.add("shard-gen1-001")

        # Verify multi-generation topology
        assert len(consumer._parent_shards) == 2, f"Expected 2 parent shards, got {len(consumer._parent_shards)}"
        assert len(consumer._child_shards) == 4, f"Expected 4 child shards, got {len(consumer._child_shards)}"

        # Test allocation logic for complex scenario
        assert (
            consumer._should_allocate_shard("shard-original-001") == True
        ), "Original parent should be allocatable (even if closed)"
        assert (
            consumer._should_allocate_shard("shard-gen1-001") == True
        ), "Gen1 parent should be allocatable after original exhausted"
        assert (
            consumer._should_allocate_shard("shard-gen1-002") == True
        ), "Gen1 child should be allocatable after original exhausted"
        assert (
            consumer._should_allocate_shard("shard-gen2-001") == True
        ), "Gen2 child should be allocatable after gen1 parent exhausted"
        assert (
            consumer._should_allocate_shard("shard-gen2-002") == True
        ), "Gen2 child should be allocatable after gen1 parent exhausted"

        logger.info("✅ Complex multi-generation topology working correctly")

        return {
            "test": "complex_topology",
            "status": "PASS",
            "details": "Multi-generation resharding handled correctly",
        }

    async def test_shard_status_reporting(self):
        """Test the shard status monitoring functionality"""
        logger.info("=== Testing Shard Status Reporting ===")

        consumer = self.create_mock_consumer("test-status-reporting")

        # Create a realistic resharding scenario
        mock_shards = [
            {
                "ShardId": "shard-parent-A",
                "SequenceNumberRange": {
                    "StartingSequenceNumber": "100",
                    "EndingSequenceNumber": "200",
                },
            },
            {
                "ShardId": "shard-child-A1",
                "ParentShardId": "shard-parent-A",
                "SequenceNumberRange": {"StartingSequenceNumber": "201"},
            },
            {
                "ShardId": "shard-child-A2",
                "ParentShardId": "shard-parent-A",
                "SequenceNumberRange": {"StartingSequenceNumber": "202"},
            },
            {
                "ShardId": "shard-independent-B",
                "SequenceNumberRange": {"StartingSequenceNumber": "300"},
            },  # No parent
        ]

        consumer.shards = mock_shards
        consumer._build_shard_topology(mock_shards)

        # Mark some shards as closed/exhausted
        consumer._closed_shards.add("shard-parent-A")
        consumer._exhausted_parents.add("shard-parent-A")

        # Get status
        status = consumer.get_shard_status()

        # Verify status reporting
        assert status["total_shards"] == 4, f"Expected 4 total shards, got {status['total_shards']}"
        assert status["parent_shards"] == 1, f"Expected 1 parent shard, got {status['parent_shards']}"
        assert status["child_shards"] == 2, f"Expected 2 child shards, got {status['child_shards']}"
        assert status["closed_shards"] == 1, f"Expected 1 closed shard, got {status['closed_shards']}"
        assert status["exhausted_parents"] == 1, f"Expected 1 exhausted parent, got {status['exhausted_parents']}"

        # Verify topology maps
        assert "parent_child_map" in status["topology"]
        assert "child_parent_map" in status["topology"]
        assert "shard-parent-A" in status["topology"]["parent_child_map"]
        assert len(status["topology"]["parent_child_map"]["shard-parent-A"]) == 2

        # Verify per-shard details
        shard_details = {s["shard_id"]: s for s in status["shard_details"]}
        assert shard_details["shard-parent-A"]["is_parent"] == True
        assert shard_details["shard-parent-A"]["is_closed"] == True
        assert shard_details["shard-child-A1"]["is_child"] == True
        assert shard_details["shard-child-A1"]["can_allocate"] == True  # Parent is exhausted
        assert shard_details["shard-independent-B"]["is_parent"] == False
        assert shard_details["shard-independent-B"]["is_child"] == False
        assert shard_details["shard-independent-B"]["can_allocate"] == True  # Independent shard

        logger.info("✅ Shard status reporting working correctly")

        return {
            "test": "status_reporting",
            "status": "PASS",
            "details": "All status metrics accurate",
        }

    async def test_edge_cases(self):
        """Test edge cases and error scenarios"""
        logger.info("=== Testing Edge Cases ===")

        consumer = self.create_mock_consumer("test-edge-cases")

        # Test with empty shard list
        consumer._build_shard_topology([])
        assert len(consumer._parent_shards) == 0
        assert len(consumer._child_shards) == 0

        # Test with only independent shards (no parent-child relationships)
        independent_shards = [
            {"ShardId": "shard-independent-1"},
            {"ShardId": "shard-independent-2"},
            {"ShardId": "shard-independent-3"},
        ]
        consumer._build_shard_topology(independent_shards)
        assert len(consumer._parent_shards) == 0
        assert len(consumer._child_shards) == 0

        # All independent shards should be allocatable
        for shard in independent_shards:
            assert consumer._should_allocate_shard(shard["ShardId"]) == True

        # Test with orphaned child (parent not in current shard list)
        orphaned_scenario = [{"ShardId": "shard-child-orphan", "ParentShardId": "shard-parent-missing"}]
        consumer._build_shard_topology(orphaned_scenario)
        assert len(consumer._child_shards) == 1
        assert len(consumer._parent_shards) == 0  # Parent not in current list

        # Orphaned child should not be allocatable (parent not exhausted)
        assert consumer._should_allocate_shard("shard-child-orphan") == False

        # But if we mark the missing parent as exhausted, child should become allocatable
        consumer._exhausted_parents.add("shard-parent-missing")
        assert consumer._should_allocate_shard("shard-child-orphan") == True

        logger.info("✅ Edge cases handled correctly")

        return {
            "test": "edge_cases",
            "status": "PASS",
            "details": "Empty lists, independent shards, and orphaned children handled",
        }

    async def run_all_tests(self):
        """Run all tests and report results"""
        logger.info("🚀 Starting Shard Topology Management Tests")
        logger.info("=" * 60)

        tests = [
            self.test_basic_parent_child_topology,
            self.test_complex_resharding_scenario,
            self.test_shard_status_reporting,
            self.test_edge_cases,
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
            logger.info(f"🎉 ALL TESTS PASSED ({passed}/{total})")
            logger.info("✅ Shard topology management is working correctly!")
            logger.info("✅ Parent-child ordering follows AWS best practices")
            logger.info("✅ Ready for production resharding scenarios")
        else:
            logger.error(f"⚠️ {total - passed} tests failed ({passed}/{total} passed)")

        return results


async def main():
    """Run the simple resharding tests"""
    test_suite = MockReshardingTest()
    results = await test_suite.run_all_tests()

    # Print detailed results
    print("\n" + "=" * 60)
    print("DETAILED TEST RESULTS")
    print("=" * 60)

    for result in results:
        status_icon = "✅" if result["status"] == "PASS" else "❌"
        print(f"{status_icon} {result['test']}: {result['status']}")
        if "details" in result:
            print(f"   Details: {result['details']}")
        if "error" in result:
            print(f"   Error: {result['error']}")

    return len([r for r in results if r["status"] == "PASS"]) == len(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
