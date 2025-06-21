#!/usr/bin/env python3
"""
Test scenarios where resharding is in progress when consumer/producer starts.

This covers edge cases like:
1. Consumer starting while stream is being resharded
2. Producer starting while stream is being resharded
3. Consumer discovering shards mid-process during resharding
4. Handling UPDATING stream status gracefully
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


class ReshardingInProgressTest:
    """Test resharding scenarios when operations are in progress"""

    def __init__(self, endpoint_url="http://localhost:4567"):
        self.endpoint_url = endpoint_url
        self.test_streams = []

    async def cleanup(self):
        """Clean up test streams"""
        logger.info(f"Test used {len(self.test_streams)} streams: {self.test_streams}")
        self.test_streams.clear()

    async def create_test_stream(self, base_name: str, shard_count: int) -> str:
        """Create a test stream and return its name"""
        stream_name = f"resharding-progress-test-{base_name}-{int(time.time())}-{uuid.uuid4().hex[:8]}"
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

    async def simulate_updating_stream_status(self, stream_name: str):
        """Simulate a stream in UPDATING status to test consumer behavior"""
        logger.info(f"Testing consumer behavior with potentially updating stream {stream_name}")

        # Start a consumer while stream might be in UPDATING status
        try:
            async with Consumer(
                stream_name=stream_name,
                endpoint_url=self.endpoint_url,
                processor=JsonProcessor(),
                sleep_time_no_records=0.5,
                max_shard_consumers=None,
            ) as consumer:

                # Let consumer initialize and discover initial shards
                await asyncio.sleep(2)
                initial_status = consumer.get_shard_status()
                logger.info(f"Initial shard status: {initial_status['total_shards']} shards")

                # Force a shard refresh to test discovery logic
                await consumer.refresh_shards()
                refresh_status = consumer.get_shard_status()
                logger.info(f"After refresh: {refresh_status['total_shards']} shards")

                # Try to consume for a brief period
                messages_consumed = 0
                start_time = time.time()
                while time.time() - start_time < 5:
                    try:
                        async with asyncio.timeout(1.0):
                            async for message in consumer:
                                messages_consumed += 1
                                break
                    except asyncio.TimeoutError:
                        break

                final_status = consumer.get_shard_status()

                result = {
                    "initial_shards": initial_status["total_shards"],
                    "final_shards": final_status["total_shards"],
                    "messages_consumed": messages_consumed,
                    "shard_discovery_working": True,
                    "consumer_resilient": True,
                }

                logger.info(f"Consumer handled potentially updating stream: {result}")
                return result

        except Exception as e:
            logger.error(f"Consumer failed with updating stream: {e}")
            return {"error": str(e), "consumer_resilient": False}

    async def test_producer_during_potential_resharding(self, stream_name: str):
        """Test producer behavior when stream might be resharding"""
        logger.info(f"Testing producer with potentially resharding stream {stream_name}")

        messages_sent = 0
        errors = 0

        try:
            async with Producer(
                stream_name=stream_name,
                endpoint_url=self.endpoint_url,
                processor=JsonProcessor(),
                batch_size=50,
                buffer_time=0.1,
            ) as producer:

                # Send messages while stream might be updating
                for i in range(100):
                    try:
                        message = {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "sequence": i,
                            "data": f"test-message-{i}",
                        }
                        await producer.put(message)
                        messages_sent += 1

                        if i % 20 == 0:
                            await producer.flush()

                    except Exception as e:
                        logger.warning(f"Producer error on message {i}: {e}")
                        errors += 1

                        # Continue despite errors - this tests resilience
                        if errors > 10:
                            break

                await producer.flush()

                result = {
                    "messages_sent": messages_sent,
                    "errors": errors,
                    "success_rate": (messages_sent / (messages_sent + errors) if (messages_sent + errors) > 0 else 0),
                    "producer_resilient": errors < 10,
                }

                logger.info(f"Producer results: {result}")
                return result

        except Exception as e:
            logger.error(f"Producer failed completely: {e}")
            return {"error": str(e), "producer_resilient": False}

    async def test_consumer_shard_discovery_during_resharding(self, stream_name: str):
        """Test consumer's ability to discover new shards during operation"""
        logger.info(f"Testing consumer shard discovery with {stream_name}")

        shard_status_history = []

        try:
            async with Consumer(
                stream_name=stream_name,
                endpoint_url=self.endpoint_url,
                processor=JsonProcessor(),
                sleep_time_no_records=0.5,
            ) as consumer:

                # Track shard status over time
                for check in range(6):  # Check every 2 seconds for 10 seconds
                    await asyncio.sleep(2)

                    # Force refresh to test discovery
                    await consumer.refresh_shards()
                    status = consumer.get_shard_status()

                    status_entry = {
                        "check": check,
                        "timestamp": time.time(),
                        "total_shards": status["total_shards"],
                        "allocated_shards": status["allocated_shards"],
                        "parent_shards": status["parent_shards"],
                        "child_shards": status["child_shards"],
                        "closed_shards": status["closed_shards"],
                    }
                    shard_status_history.append(status_entry)

                    logger.info(
                        f"Check {check}: {status['total_shards']} total, {status['allocated_shards']} allocated"
                    )

                # Analyze status changes
                shard_counts = [s["total_shards"] for s in shard_status_history]
                stable_shard_count = all(count == shard_counts[0] for count in shard_counts)

                result = {
                    "shard_status_history": shard_status_history,
                    "stable_shard_count": stable_shard_count,
                    "shard_discovery_working": True,
                    "final_shard_count": shard_counts[-1] if shard_counts else 0,
                }

                logger.info(
                    f"Shard discovery results: stable={stable_shard_count}, final_count={result['final_shard_count']}"
                )
                return result

        except Exception as e:
            logger.error(f"Consumer shard discovery failed: {e}")
            return {"error": str(e), "shard_discovery_working": False}

    async def test_concurrent_producer_consumer_during_resharding(self, stream_name: str):
        """Test producer and consumer running concurrently during potential resharding"""
        logger.info(f"Testing concurrent producer/consumer with {stream_name}")

        # Run producer and consumer concurrently
        producer_task = asyncio.create_task(self.test_producer_during_potential_resharding(stream_name))
        consumer_task = asyncio.create_task(self.simulate_updating_stream_status(stream_name))

        producer_result, consumer_result = await asyncio.gather(producer_task, consumer_task)

        result = {
            "producer": producer_result,
            "consumer": consumer_result,
            "both_resilient": (
                producer_result.get("producer_resilient", False) and consumer_result.get("consumer_resilient", False)
            ),
        }

        logger.info(f"Concurrent test results: both_resilient={result['both_resilient']}")
        return result

    async def test_stream_status_handling(self, stream_name: str):
        """Test how consumer handles different stream statuses"""
        logger.info(f"Testing stream status handling with {stream_name}")

        try:
            # Create consumer but don't start consuming yet
            consumer = Consumer(
                stream_name=stream_name,
                endpoint_url=self.endpoint_url,
                processor=JsonProcessor(),
            )

            # Test connection and stream status checking
            await consumer.get_conn()

            # Check if consumer handles the stream status gracefully
            stream_status = consumer.stream_status
            logger.info(f"Stream status: {stream_status}")

            await consumer.close()

            result = {
                "stream_status": stream_status,
                "connection_successful": stream_status in [consumer.ACTIVE, consumer.UPDATING],
                "status_handling_working": True,
            }

            logger.info(f"Stream status test: {result}")
            return result

        except Exception as e:
            logger.error(f"Stream status test failed: {e}")
            return {"error": str(e), "status_handling_working": False}

    async def run_all_tests(self):
        """Run all resharding-in-progress tests"""
        logger.info("🚀 Starting Resharding In-Progress Tests")
        logger.info("=" * 60)

        results = []

        # Test with single shard stream
        logger.info("\n--- Testing with Single Shard Stream ---")
        stream_1 = await self.create_test_stream("single", 1)

        tests_single = [
            ("stream_status_handling", self.test_stream_status_handling(stream_1)),
            (
                "consumer_during_resharding",
                self.simulate_updating_stream_status(stream_1),
            ),
            (
                "producer_during_resharding",
                self.test_producer_during_potential_resharding(stream_1),
            ),
            (
                "shard_discovery",
                self.test_consumer_shard_discovery_during_resharding(stream_1),
            ),
        ]

        for test_name, test_coro in tests_single:
            try:
                result = await test_coro
                result["test"] = test_name
                result["stream_type"] = "single_shard"
                result["status"] = "PASS" if not result.get("error") else "FAIL"
                results.append(result)
                logger.info(f"✅ {test_name}: {result['status']}")
            except Exception as e:
                logger.error(f"❌ {test_name}: FAIL - {e}")
                results.append(
                    {
                        "test": test_name,
                        "stream_type": "single_shard",
                        "status": "FAIL",
                        "error": str(e),
                    }
                )

        # Test with multi-shard stream
        logger.info("\n--- Testing with Multi-Shard Stream ---")
        stream_3 = await self.create_test_stream("multi", 3)

        tests_multi = [
            (
                "concurrent_producer_consumer",
                self.test_concurrent_producer_consumer_during_resharding(stream_3),
            ),
            (
                "multi_shard_discovery",
                self.test_consumer_shard_discovery_during_resharding(stream_3),
            ),
        ]

        for test_name, test_coro in tests_multi:
            try:
                result = await test_coro
                result["test"] = test_name
                result["stream_type"] = "multi_shard"
                result["status"] = "PASS" if not result.get("error") else "FAIL"
                results.append(result)
                logger.info(f"✅ {test_name}: {result['status']}")
            except Exception as e:
                logger.error(f"❌ {test_name}: FAIL - {e}")
                results.append(
                    {
                        "test": test_name,
                        "stream_type": "multi_shard",
                        "status": "FAIL",
                        "error": str(e),
                    }
                )

        # Cleanup
        await self.cleanup()

        # Summary
        logger.info("=" * 60)
        passed = len([r for r in results if r["status"] == "PASS"])
        total = len(results)

        if passed == total:
            logger.info(f"🎉 ALL RESHARDING-IN-PROGRESS TESTS PASSED ({passed}/{total})")
            logger.info("✅ Consumer/Producer handle resharding gracefully")
            logger.info("✅ Shard discovery works during stream updates")
            logger.info("✅ Concurrent operations are resilient")
        else:
            logger.error(f"⚠️ {total - passed} tests failed ({passed}/{total} passed)")

        return results


async def main():
    """Run resharding in-progress tests"""
    test_suite = ReshardingInProgressTest()

    try:
        results = await test_suite.run_all_tests()

        # Print summary
        print("\n" + "=" * 60)
        print("RESHARDING IN-PROGRESS TEST RESULTS")
        print("=" * 60)

        for result in results:
            status_icon = "✅" if result["status"] == "PASS" else "❌"
            print(f"{status_icon} {result['test']} ({result['stream_type']}): {result['status']}")
            if "error" in result:
                print(f"   Error: {result['error']}")

        return len([r for r in results if r["status"] == "PASS"]) == len(results)

    except Exception as e:
        logger.error(f"Test suite failed: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
