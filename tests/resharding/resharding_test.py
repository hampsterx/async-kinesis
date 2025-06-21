#!/usr/bin/env python3
"""
Async Kinesis Resharding Test Tool

This tool tests the resharding capabilities of the async-kinesis library against real AWS Kinesis streams.
It simulates real-world resharding scenarios and measures how well the consumer handles shard topology changes.

IMPORTANT: This will create temporary Kinesis streams and perform resharding operations.
Ensure you have appropriate AWS permissions for Kinesis operations.

Usage:
    python resharding_test.py [options]

Examples:
    # Test 1→3 shard scaling
    python resharding_test.py --scenario scale-up-small

    # Test 2→6 shard scaling
    python resharding_test.py --scenario scale-up-large

    # Test 3→1 shard scaling down
    python resharding_test.py --scenario scale-down

    # Run all scenarios
    python resharding_test.py --all-scenarios

    # Dry run (no AWS resources)
    python resharding_test.py --dry-run --scenario scale-up-small
"""

import argparse
import asyncio
import atexit
import json
import logging
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    import coloredlogs

    HAS_COLOREDLOGS = True
except ImportError:
    HAS_COLOREDLOGS = False

try:
    import boto3
    from botocore.exceptions import ClientError

    HAS_BOTO3 = True
except ImportError:
    # Use the aiobotocore session that kinesis library already uses
    HAS_BOTO3 = False

import os

# Add the parent directory to Python path for imports
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from kinesis import Consumer, JsonProcessor, Producer

# Configure logging
logger = logging.getLogger(__name__)


class ReshardingTestSuite:
    """Test suite for evaluating resharding behavior"""

    def __init__(self, endpoint_url: Optional[str] = None, region_name: str = "us-east-1"):
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.test_streams = []  # Track streams for cleanup
        self.kinesis_client = None

        # Test scenarios configuration
        self.scenarios = {
            "scale-up-small": {
                "name": "Small Scale Up (1→3 shards)",
                "initial_shards": 1,
                "target_shards": 3,
                "description": "Tests basic split operation handling",
            },
            "scale-up-large": {
                "name": "Large Scale Up (2→6 shards)",
                "initial_shards": 2,
                "target_shards": 6,
                "description": "Tests multiple simultaneous splits",
            },
            "scale-down": {
                "name": "Scale Down (3→1 shards)",
                "initial_shards": 3,
                "target_shards": 1,
                "description": "Tests shard merging and parent exhaustion",
            },
            "complex": {
                "name": "Complex Resharding (4→2→8 shards)",
                "initial_shards": 4,
                "intermediate_shards": 2,
                "target_shards": 8,
                "description": "Tests multiple resharding events in sequence",
            },
        }

    async def setup(self):
        """Initialize AWS clients and test environment"""
        if not HAS_BOTO3:
            logger.error("boto3 is required for resharding tests. Install with: pip install boto3")
            raise ImportError("boto3 not available")

        if not self.endpoint_url:
            # Real AWS
            session = boto3.Session()
            self.kinesis_client = session.client("kinesis", region_name=self.region_name)
        else:
            # LocalStack or test environment
            self.kinesis_client = boto3.client(
                "kinesis",
                endpoint_url=self.endpoint_url,
                region_name=self.region_name,
                aws_access_key_id="testing",
                aws_secret_access_key="testing",
            )

        logger.info(f"Connected to Kinesis {'(LocalStack)' if self.endpoint_url else '(AWS)'}")

    async def cleanup(self):
        """Clean up all test streams"""
        if not self.test_streams:
            return

        logger.info(f"Cleaning up {len(self.test_streams)} test streams...")
        for stream_name in self.test_streams:
            try:
                self.kinesis_client.delete_stream(StreamName=stream_name)
                logger.info(f"Deleted stream: {stream_name}")
            except Exception as e:
                logger.warning(f"Failed to delete stream {stream_name}: {e}")

        self.test_streams.clear()

    async def create_test_stream(self, base_name: str, shard_count: int) -> str:
        """Create a test stream with specified shard count"""
        stream_name = f"resharding-test-{base_name}-{int(time.time())}-{uuid.uuid4().hex[:8]}"

        try:
            self.kinesis_client.create_stream(StreamName=stream_name, ShardCount=shard_count)
            self.test_streams.append(stream_name)

            # Wait for stream to become active
            logger.info(f"Creating stream {stream_name} with {shard_count} shards...")
            await self.wait_for_stream_active(stream_name)
            logger.info(f"Stream {stream_name} is now ACTIVE")

            return stream_name

        except Exception as e:
            logger.error(f"Failed to create stream {stream_name}: {e}")
            raise

    async def wait_for_stream_active(self, stream_name: str, timeout: int = 300):
        """Wait for stream to become ACTIVE"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = self.kinesis_client.describe_stream(StreamName=stream_name)
                status = response["StreamDescription"]["StreamStatus"]

                if status == "ACTIVE":
                    return
                elif status in ["CREATING", "UPDATING"]:
                    logger.debug(f"Stream {stream_name} status: {status}")
                    await asyncio.sleep(2)
                else:
                    raise Exception(f"Stream {stream_name} in unexpected status: {status}")

            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    logger.debug(f"Stream {stream_name} not found yet, waiting...")
                    await asyncio.sleep(2)
                else:
                    raise

        raise TimeoutError(f"Stream {stream_name} did not become ACTIVE within {timeout}s")

    async def reshard_stream(self, stream_name: str, target_shards: int) -> Dict[str, Any]:
        """Perform resharding operation and measure timing"""
        logger.info(f"Resharding {stream_name} to {target_shards} shards...")

        # Get current shard count
        current_description = self.kinesis_client.describe_stream(StreamName=stream_name)
        current_shards = len(current_description["StreamDescription"]["Shards"])
        current_shard_ids = [s["ShardId"] for s in current_description["StreamDescription"]["Shards"]]

        logger.info(f"Current shards: {current_shards} → Target: {target_shards}")
        logger.debug(f"Current shard IDs: {current_shard_ids}")

        start_time = time.time()

        try:
            # Use UpdateShardCount for resharding
            self.kinesis_client.update_shard_count(
                StreamName=stream_name,
                TargetShardCount=target_shards,
                ScalingType="UNIFORM_SCALING",
            )

            # Wait for resharding to complete
            await self.wait_for_stream_active(stream_name)

            # Get final shard information
            final_description = self.kinesis_client.describe_stream(StreamName=stream_name)
            final_shards = final_description["StreamDescription"]["Shards"]
            final_shard_ids = [s["ShardId"] for s in final_shards]

            resharding_time = time.time() - start_time

            # Analyze shard topology changes
            parent_child_relationships = []
            for shard in final_shards:
                if "ParentShardId" in shard:
                    parent_child_relationships.append({"parent": shard["ParentShardId"], "child": shard["ShardId"]})

            result = {
                "success": True,
                "resharding_time": resharding_time,
                "previous_shard_count": current_shards,
                "new_shard_count": len(final_shards),
                "previous_shard_ids": current_shard_ids,
                "new_shard_ids": final_shard_ids,
                "parent_child_relationships": parent_child_relationships,
                "topology_changes": len(parent_child_relationships),
            }

            logger.info(f"Resharding completed in {resharding_time:.1f}s")
            logger.info(f"Topology changes: {len(parent_child_relationships)} parent-child relationships")

            return result

        except Exception as e:
            logger.error(f"Resharding failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "resharding_time": time.time() - start_time,
            }

    async def saturate_stream(self, stream_name: str, duration: int = 30) -> Dict[str, Any]:
        """Saturate the stream with data before resharding"""
        logger.info(f"Saturating stream {stream_name} for {duration} seconds...")

        messages_sent = 0
        start_time = time.time()

        try:
            async with Producer(
                stream_name=stream_name,
                endpoint_url=self.endpoint_url,
                processor=JsonProcessor(),
                batch_size=500,
                buffer_time=0.1,
                create_stream=False,
            ) as producer:

                end_time = start_time + duration
                while time.time() < end_time:
                    batch_start = time.time()

                    # Send a batch of messages
                    for i in range(100):
                        message = {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "sequence": messages_sent + i,
                            "payload": f"saturation-data-{messages_sent + i}",
                            "batch": int(time.time()),
                        }
                        await producer.put(message)

                    await producer.flush()
                    messages_sent += 100

                    # Brief pause to avoid overwhelming
                    await asyncio.sleep(0.1)

                # Final flush
                await producer.flush()

        except Exception as e:
            logger.error(f"Stream saturation failed: {e}")

        saturation_time = time.time() - start_time
        rate = messages_sent / saturation_time if saturation_time > 0 else 0

        result = {
            "messages_sent": messages_sent,
            "duration": saturation_time,
            "rate_per_second": rate,
        }

        logger.info(f"Saturation complete: {messages_sent} messages in {saturation_time:.1f}s ({rate:.0f} msg/s)")
        return result

    async def test_consumer_behavior(self, stream_name: str, test_duration: int = 60) -> Dict[str, Any]:
        """Test consumer behavior during and after resharding"""
        logger.info(f"Testing consumer behavior on {stream_name} for {test_duration}s...")

        messages_consumed = 0
        shard_status_history = []
        error_count = 0
        start_time = time.time()

        try:
            async with Consumer(
                stream_name=stream_name,
                endpoint_url=self.endpoint_url,
                processor=JsonProcessor(),
                sleep_time_no_records=0.5,
                max_shard_consumers=None,  # Use all available shards
                create_stream=False,
            ) as consumer:

                # Start consumer and track status changes
                end_time = start_time + test_duration
                last_status_check = 0

                while time.time() < end_time:
                    try:
                        # Check shard status every 5 seconds
                        current_time = time.time()
                        if current_time - last_status_check >= 5:
                            status = consumer.get_shard_status()
                            status_entry = {
                                "timestamp": current_time - start_time,
                                "total_shards": status["total_shards"],
                                "active_shards": status["active_shards"],
                                "closed_shards": status["closed_shards"],
                                "allocated_shards": status["allocated_shards"],
                                "parent_shards": status["parent_shards"],
                                "child_shards": status["child_shards"],
                                "exhausted_parents": status["exhausted_parents"],
                            }
                            shard_status_history.append(status_entry)
                            last_status_check = current_time

                            logger.debug(
                                f"Status: {status['allocated_shards']}/{status['total_shards']} shards, "
                                f"{status['parent_shards']}P/{status['child_shards']}C, "
                                f"{status['closed_shards']} closed"
                            )

                        # Consume messages with timeout
                        try:
                            async with asyncio.timeout(1.0):
                                async for message in consumer:
                                    messages_consumed += 1
                                    # Just count messages, don't process
                                    break  # Get one message then check status
                        except asyncio.TimeoutError:
                            # No messages available, continue
                            pass

                    except Exception as e:
                        error_count += 1
                        logger.warning(f"Consumer error #{error_count}: {e}")
                        if error_count > 10:
                            logger.error("Too many consumer errors, stopping test")
                            break
                        await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Consumer test failed: {e}")

        test_time = time.time() - start_time
        rate = messages_consumed / test_time if test_time > 0 else 0

        result = {
            "messages_consumed": messages_consumed,
            "test_duration": test_time,
            "consumption_rate": rate,
            "error_count": error_count,
            "shard_status_history": shard_status_history,
            "topology_transitions": len(
                [s for s in shard_status_history if s["parent_shards"] > 0 or s["child_shards"] > 0]
            ),
        }

        logger.info(f"Consumer test complete: {messages_consumed} messages in {test_time:.1f}s ({rate:.1f} msg/s)")
        logger.info(f"Topology transitions detected: {result['topology_transitions']}")

        return result

    async def run_scenario(self, scenario_name: str) -> Dict[str, Any]:
        """Run a complete resharding test scenario"""
        if scenario_name not in self.scenarios:
            raise ValueError(f"Unknown scenario: {scenario_name}")

        scenario = self.scenarios[scenario_name]
        logger.info(f"\n=== Starting Scenario: {scenario['name']} ===")
        logger.info(f"Description: {scenario['description']}")

        results = {
            "scenario": scenario_name,
            "scenario_info": scenario,
            "start_time": time.time(),
            "phases": {},
        }

        try:
            # Phase 1: Create initial stream
            logger.info(f"\n--- Phase 1: Creating stream with {scenario['initial_shards']} shards ---")
            stream_name = await self.create_test_stream(scenario_name, scenario["initial_shards"])
            results["stream_name"] = stream_name

            # Phase 2: Saturate stream
            logger.info(f"\n--- Phase 2: Saturating stream ---")
            saturation_result = await self.saturate_stream(stream_name, duration=20)
            results["phases"]["saturation"] = saturation_result

            # Phase 3: Start consumer and reshard
            logger.info(
                f"\n--- Phase 3: Resharding {scenario['initial_shards']} → {scenario['target_shards']} shards ---"
            )

            # Start consumer behavior test in background
            consumer_task = asyncio.create_task(self.test_consumer_behavior(stream_name, test_duration=120))

            # Wait a moment for consumer to start
            await asyncio.sleep(5)

            # Perform resharding
            resharding_result = await self.reshard_stream(stream_name, scenario["target_shards"])
            results["phases"]["resharding"] = resharding_result

            # Wait for consumer test to complete
            consumer_result = await consumer_task
            results["phases"]["consumer_behavior"] = consumer_result

            # Phase 4: Complex scenario handling
            if scenario_name == "complex" and resharding_result["success"]:
                logger.info(
                    f"\n--- Phase 4: Second resharding {scenario['target_shards']} → "
                    f"{scenario.get('final_shards', 8)} ---"
                )

                # Brief pause between resharding operations
                await asyncio.sleep(10)

                consumer_task2 = asyncio.create_task(self.test_consumer_behavior(stream_name, test_duration=90))

                await asyncio.sleep(5)

                resharding_result2 = await self.reshard_stream(stream_name, scenario.get("final_shards", 8))
                results["phases"]["second_resharding"] = resharding_result2

                consumer_result2 = await consumer_task2
                results["phases"]["second_consumer_behavior"] = consumer_result2

            results["success"] = True
            results["total_time"] = time.time() - results["start_time"]

            logger.info(f"\n=== Scenario {scenario['name']} completed successfully in {results['total_time']:.1f}s ===")

        except Exception as e:
            logger.error(f"Scenario {scenario_name} failed: {e}")
            results["success"] = False
            results["error"] = str(e)
            results["total_time"] = time.time() - results["start_time"]

        return results

    def print_results(self, all_results: List[Dict[str, Any]]):
        """Print formatted test results"""
        print("\n" + "=" * 80)
        print("RESHARDING TEST RESULTS")
        print("=" * 80)

        # Summary table
        summary_data = [["Scenario", "Status", "Duration", "Resharding Time", "Topology Changes"]]

        for result in all_results:
            scenario_name = result["scenario_info"]["name"]
            status = "✅ PASS" if result["success"] else "❌ FAIL"
            duration = f"{result['total_time']:.1f}s"

            if result["success"] and "resharding" in result["phases"]:
                resharding_time = f"{result['phases']['resharding']['resharding_time']:.1f}s"
                topology_changes = result["phases"]["resharding"].get("topology_changes", 0)
            else:
                resharding_time = "N/A"
                topology_changes = "N/A"

            summary_data.append(
                [
                    scenario_name,
                    status,
                    duration,
                    resharding_time,
                    str(topology_changes),
                ]
            )

        # Simple table formatting without external dependencies
        print("\nTest Summary:")
        print("-" * 80)
        for row in summary_data:
            print(f"{row[0]:<35} {row[1]:<10} {row[2]:<12} {row[3]:<15} {row[4]:<15}")
        print("-" * 80)

        # Detailed results for each scenario
        for result in all_results:
            if not result["success"]:
                continue

            print(f"\n--- {result['scenario_info']['name']} Details ---")

            # Saturation phase
            if "saturation" in result["phases"]:
                sat = result["phases"]["saturation"]
                print(f"Saturation: {sat['messages_sent']} messages @ {sat['rate_per_second']:.0f} msg/s")

            # Resharding phase
            if "resharding" in result["phases"]:
                res = result["phases"]["resharding"]
                print(
                    f"Resharding: {res['previous_shard_count']} → {res['new_shard_count']} shards "
                    f"in {res['resharding_time']:.1f}s"
                )
                print(f"Parent-child relationships created: {res['topology_changes']}")

            # Consumer behavior
            if "consumer_behavior" in result["phases"]:
                cons = result["phases"]["consumer_behavior"]
                print(f"Consumer: {cons['messages_consumed']} messages @ {cons['consumption_rate']:.1f} msg/s")
                print(f"Topology transitions detected: {cons['topology_transitions']}")
                print(f"Consumer errors: {cons['error_count']}")

        # Overall assessment
        successful_tests = len([r for r in all_results if r["success"]])
        print(f"\n🎯 Overall: {successful_tests}/{len(all_results)} scenarios passed")

        if successful_tests == len(all_results):
            print("✅ All resharding scenarios handled successfully!")
        else:
            print("⚠️ Some scenarios failed - check logs for details")


async def main():
    parser = argparse.ArgumentParser(description="Test async-kinesis resharding capabilities")
    parser.add_argument(
        "--scenario",
        choices=["scale-up-small", "scale-up-large", "scale-down", "complex"],
        help="Specific scenario to run",
    )
    parser.add_argument("--all-scenarios", action="store_true", help="Run all resharding scenarios")
    parser.add_argument("--endpoint-url", help="Kinesis endpoint URL (for LocalStack)")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be tested without creating resources",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    if HAS_COLOREDLOGS:
        coloredlogs.install(level=log_level, fmt="%(asctime)s %(levelname)s %(message)s")
    else:
        logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(message)s")

    if args.dry_run:
        print("DRY RUN MODE - No AWS resources will be created")
        test_suite = ReshardingTestSuite(endpoint_url=args.endpoint_url, region_name=args.region)

        scenarios_to_show = []
        if args.all_scenarios:
            scenarios_to_show = list(test_suite.scenarios.keys())
        elif args.scenario:
            scenarios_to_show = [args.scenario]
        else:
            scenarios_to_show = ["scale-up-small"]  # default

        print("\nScenarios that would be tested:")
        for scenario_name in scenarios_to_show:
            scenario = test_suite.scenarios[scenario_name]
            print(f"  {scenario['name']}: {scenario['description']}")

        return

    # Real test execution
    test_suite = ReshardingTestSuite(endpoint_url=args.endpoint_url, region_name=args.region)

    # Setup cleanup handlers
    def cleanup_handler(signum=None, frame=None):
        logger.info("Interrupt received, cleaning up...")
        asyncio.create_task(test_suite.cleanup())

    signal.signal(signal.SIGINT, cleanup_handler)
    signal.signal(signal.SIGTERM, cleanup_handler)
    atexit.register(lambda: asyncio.run(test_suite.cleanup()))

    try:
        await test_suite.setup()

        # Determine which scenarios to run
        scenarios_to_run = []
        if args.all_scenarios:
            scenarios_to_run = ["scale-up-small", "scale-up-large", "scale-down"]
        elif args.scenario:
            scenarios_to_run = [args.scenario]
        else:
            scenarios_to_run = ["scale-up-small"]  # default

        # Run scenarios
        all_results = []
        for scenario_name in scenarios_to_run:
            try:
                result = await test_suite.run_scenario(scenario_name)
                all_results.append(result)
            except Exception as e:
                logger.error(f"Failed to run scenario {scenario_name}: {e}")
                all_results.append(
                    {
                        "scenario": scenario_name,
                        "scenario_info": test_suite.scenarios[scenario_name],
                        "success": False,
                        "error": str(e),
                        "total_time": 0,
                    }
                )

        # Print results
        test_suite.print_results(all_results)

    finally:
        await test_suite.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
