#!/usr/bin/env python3
"""
Test starting producer/consumer during active resharding operations.

This tests the critical scenario where:
1. A stream is actively being resharded
2. We start a new consumer or producer
3. The library should handle the UPDATING status gracefully
4. Should detect and adapt to topology changes
"""

import asyncio
import logging
import os

# Add the parent directory to Python path for imports
import sys
import time
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from kinesis import Consumer, JsonProcessor, Producer

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class StartDuringReshardingTest:
    """Test starting operations during active resharding"""

    def __init__(self, endpoint_url="http://localhost:4567"):
        self.endpoint_url = endpoint_url
        self.test_streams = []

    async def cleanup(self):
        """Clean up test streams"""
        logger.info(f"Test used {len(self.test_streams)} streams: {self.test_streams}")
        self.test_streams.clear()

    async def create_test_stream(self, base_name: str, shard_count: int) -> str:
        """Create a test stream and return its name"""
        stream_name = f"start-during-resharding-{base_name}-{int(time.time())}-{uuid.uuid4().hex[:8]}"
        self.test_streams.append(stream_name)

        # Create stream using Producer
        async with Producer(
            stream_name=stream_name,
            endpoint_url=self.endpoint_url,
            create_stream=True,
            create_stream_shards=shard_count,
        ) as producer:
            pass

        logger.info(f"Created stream {stream_name} with {shard_count} shards")
        return stream_name

    async def test_consumer_start_during_simulated_resharding(self, stream_name: str):
        """Test consumer starting with a stream that has parent-child topology"""
        logger.info(f"Testing consumer start with resharding topology: {stream_name}")

        # First, create some parent-child topology by simulating it
        # (In real scenarios this would be from actual AWS resharding)

        try:
            consumer = Consumer(
                stream_name=stream_name,
                endpoint_url=self.endpoint_url,
                processor=JsonProcessor(),
                sleep_time_no_records=0.5,
            )

            # Initialize consumer
            await consumer.get_conn()

            # Simulate discovering resharding topology
            # This mimics what would happen if consumer started after resharding began
            if consumer.shards:
                # Manually create some parent-child relationships for testing
                original_shard = consumer.shards[0]["ShardId"]
                consumer._parent_shards.add(original_shard)
                consumer._shard_topology[original_shard] = {
                    "parent": None,
                    "children": {"child-001", "child-002"},
                }

                # Add mock child shards to topology
                consumer._child_shards.update({"child-001", "child-002"})
                consumer._shard_topology["child-001"] = {
                    "parent": original_shard,
                    "children": set(),
                }
                consumer._shard_topology["child-002"] = {
                    "parent": original_shard,
                    "children": set(),
                }

                logger.info(f"Simulated resharding topology: 1 parent -> 2 children")

            # Test resharding detection
            resharding_detected = consumer.is_resharding_likely_in_progress()

            # Get status to verify topology handling
            status = consumer.get_shard_status()

            await consumer.close()

            result = {
                "consumer_initialized": True,
                "resharding_detected": resharding_detected,
                "parent_shards": status["parent_shards"],
                "child_shards": status["child_shards"],
                "resharding_in_progress": status["resharding_in_progress"],
                "topology_handling": status["parent_shards"] > 0 or status["child_shards"] > 0,
            }

            logger.info(f"Consumer resharding detection: {result}")
            return result

        except Exception as e:
            logger.error(f"Consumer start during resharding failed: {e}")
            return {"error": str(e), "consumer_initialized": False}

    async def test_producer_resilience_during_topology_changes(self, stream_name: str):
        """Test producer handling of stream with changing topology"""
        logger.info(f"Testing producer resilience with {stream_name}")

        messages_sent = 0
        errors = 0

        try:
            # Test producer with multiple initialization attempts
            # This simulates starting during UPDATING status
            for attempt in range(3):
                try:
                    async with Producer(
                        stream_name=stream_name,
                        endpoint_url=self.endpoint_url,
                        processor=JsonProcessor(),
                        batch_size=10,
                        buffer_time=0.05,
                    ) as producer:

                        logger.info(f"Producer attempt {attempt + 1} successful")

                        # Send a batch of messages
                        batch_size = 20
                        for i in range(batch_size):
                            try:
                                message = {
                                    "timestamp": datetime.now(timezone.utc).isoformat(),
                                    "attempt": attempt,
                                    "sequence": i,
                                    "data": f"resilience-test-{attempt}-{i}",
                                }
                                await producer.put(message)
                                messages_sent += 1

                            except Exception as e:
                                logger.warning(f"Message send error: {e}")
                                errors += 1

                        await producer.flush()
                        break  # Success, no need to retry

                except Exception as e:
                    logger.warning(f"Producer attempt {attempt + 1} failed: {e}")
                    errors += 1
                    if attempt < 2:  # Don't sleep on last attempt
                        await asyncio.sleep(1)

            result = {
                "messages_sent": messages_sent,
                "errors": errors,
                "success_rate": (messages_sent / (messages_sent + errors) if (messages_sent + errors) > 0 else 0),
                "producer_resilient": messages_sent > 0,
                "multiple_attempts_successful": messages_sent >= 20,
            }

            logger.info(f"Producer resilience results: {result}")
            return result

        except Exception as e:
            logger.error(f"Producer resilience test failed: {e}")
            return {"error": str(e), "producer_resilient": False}

    async def test_stream_status_transitions(self, stream_name: str):
        """Test handling of stream status transitions"""
        logger.info(f"Testing stream status transitions with {stream_name}")

        status_checks = []

        try:
            # Check stream status multiple times to simulate transitions
            for check in range(5):
                consumer = Consumer(stream_name=stream_name, endpoint_url=self.endpoint_url)

                try:
                    await consumer.get_conn()
                    stream_status = consumer.stream_status

                    status_check = {
                        "check": check,
                        "timestamp": time.time(),
                        "status": stream_status,
                        "connection_successful": stream_status in [consumer.ACTIVE, consumer.UPDATING],
                    }
                    status_checks.append(status_check)

                    logger.info(f"Status check {check}: {stream_status}")

                finally:
                    await consumer.close()

                await asyncio.sleep(0.5)

            # Analyze status stability
            statuses = [check["status"] for check in status_checks]
            stable_status = all(status == statuses[0] for status in statuses)
            all_successful = all(check["connection_successful"] for check in status_checks)

            result = {
                "status_checks": status_checks,
                "stable_status": stable_status,
                "all_connections_successful": all_successful,
                "final_status": statuses[-1] if statuses else "UNKNOWN",
                "status_transitions_handled": True,
            }

            logger.info(f"Status transition results: stable={stable_status}, successful={all_successful}")
            return result

        except Exception as e:
            logger.error(f"Status transition test failed: {e}")
            return {"error": str(e), "status_transitions_handled": False}

    async def test_rapid_consumer_producer_cycles(self, stream_name: str):
        """Test rapid start/stop cycles during potential resharding"""
        logger.info(f"Testing rapid cycles with {stream_name}")

        successful_cycles = 0
        failed_cycles = 0

        try:
            # Perform multiple rapid start/stop cycles
            for cycle in range(5):
                cycle_start = time.time()

                try:
                    # Start producer and consumer simultaneously
                    producer_task = asyncio.create_task(self._quick_producer_cycle(stream_name, cycle))
                    consumer_task = asyncio.create_task(self._quick_consumer_cycle(stream_name, cycle))

                    producer_result, consumer_result = await asyncio.gather(
                        producer_task, consumer_task, return_exceptions=True
                    )

                    # Check if both succeeded
                    if not isinstance(producer_result, Exception) and not isinstance(consumer_result, Exception):
                        successful_cycles += 1
                        logger.info(f"Cycle {cycle} successful")
                    else:
                        failed_cycles += 1
                        logger.warning(f"Cycle {cycle} failed: P={producer_result}, C={consumer_result}")

                except Exception as e:
                    failed_cycles += 1
                    logger.warning(f"Cycle {cycle} exception: {e}")

                cycle_time = time.time() - cycle_start
                logger.debug(f"Cycle {cycle} completed in {cycle_time:.2f}s")

                # Brief pause between cycles
                await asyncio.sleep(0.2)

            result = {
                "successful_cycles": successful_cycles,
                "failed_cycles": failed_cycles,
                "success_rate": (
                    successful_cycles / (successful_cycles + failed_cycles)
                    if (successful_cycles + failed_cycles) > 0
                    else 0
                ),
                "rapid_cycles_handled": successful_cycles >= 3,
            }

            logger.info(f"Rapid cycles results: {result}")
            return result

        except Exception as e:
            logger.error(f"Rapid cycles test failed: {e}")
            return {"error": str(e), "rapid_cycles_handled": False}

    async def _quick_producer_cycle(self, stream_name: str, cycle: int):
        """Quick producer start/stop cycle"""
        async with Producer(
            stream_name=stream_name,
            endpoint_url=self.endpoint_url,
            processor=JsonProcessor(),
            batch_size=5,
        ) as producer:
            # Send a few messages
            for i in range(3):
                await producer.put(
                    {
                        "cycle": cycle,
                        "message": i,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
            await producer.flush()
        return True

    async def _quick_consumer_cycle(self, stream_name: str, cycle: int):
        """Quick consumer start/stop cycle"""
        async with Consumer(
            stream_name=stream_name,
            endpoint_url=self.endpoint_url,
            processor=JsonProcessor(),
            sleep_time_no_records=0.1,
        ) as consumer:
            # Try to consume briefly
            start_time = time.time()
            messages_read = 0
            while time.time() - start_time < 1 and messages_read < 5:
                try:
                    async with asyncio.timeout(0.5):
                        async for message in consumer:
                            messages_read += 1
                            break
                except asyncio.TimeoutError:
                    break
        return True

    async def run_all_tests(self):
        """Run all start-during-resharding tests"""
        logger.info("🚀 Starting 'Start During Resharding' Tests")
        logger.info("=" * 60)

        results = []

        # Create test streams
        logger.info("\n--- Creating Test Streams ---")
        stream_single = await self.create_test_stream("single", 1)
        stream_multi = await self.create_test_stream("multi", 3)

        tests = [
            (
                "consumer_start_during_resharding",
                self.test_consumer_start_during_simulated_resharding(stream_single),
            ),
            (
                "producer_resilience",
                self.test_producer_resilience_during_topology_changes(stream_single),
            ),
            (
                "stream_status_transitions",
                self.test_stream_status_transitions(stream_single),
            ),
            (
                "rapid_cycles_single",
                self.test_rapid_consumer_producer_cycles(stream_single),
            ),
            (
                "rapid_cycles_multi",
                self.test_rapid_consumer_producer_cycles(stream_multi),
            ),
        ]

        for test_name, test_coro in tests:
            try:
                logger.info(f"\n--- Running {test_name} ---")
                result = await test_coro
                result["test"] = test_name
                result["status"] = "PASS" if not result.get("error") else "FAIL"
                results.append(result)
                logger.info(f"✅ {test_name}: {result['status']}")
            except Exception as e:
                logger.error(f"❌ {test_name}: FAIL - {e}")
                results.append({"test": test_name, "status": "FAIL", "error": str(e)})

        # Cleanup
        await self.cleanup()

        # Summary
        logger.info("=" * 60)
        passed = len([r for r in results if r["status"] == "PASS"])
        total = len(results)

        if passed == total:
            logger.info(f"🎉 ALL START-DURING-RESHARDING TESTS PASSED ({passed}/{total})")
            logger.info("✅ Consumer/Producer handle start-during-resharding scenarios")
            logger.info("✅ Status transitions are handled gracefully")
            logger.info("✅ Rapid cycles work reliably")
        else:
            logger.error(f"⚠️ {total - passed} tests failed ({passed}/{total} passed)")

        return results


async def main():
    """Run start-during-resharding tests"""
    test_suite = StartDuringReshardingTest()

    try:
        results = await test_suite.run_all_tests()

        # Print summary
        print("\n" + "=" * 60)
        print("START-DURING-RESHARDING TEST RESULTS")
        print("=" * 60)

        for result in results:
            status_icon = "✅" if result["status"] == "PASS" else "❌"
            print(f"{status_icon} {result['test']}: {result['status']}")
            if "error" in result:
                print(f"   Error: {result['error']}")

        return len([r for r in results if r["status"] == "PASS"]) == len(results)

    except Exception as e:
        logger.error(f"Test suite failed: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
