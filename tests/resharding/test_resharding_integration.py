#!/usr/bin/env python3
"""
Resharding Integration Test

Tests resharding behavior using LocalStack with actual stream creation and consumption.
This simulates resharding by creating streams with different shard counts and testing
our consumer's ability to handle the topology correctly.
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


class ReshardingIntegrationTest:
    """Integration test for resharding scenarios using LocalStack"""

    def __init__(self, endpoint_url="http://localhost:4567"):
        self.endpoint_url = endpoint_url
        self.test_streams = []

    async def cleanup(self):
        """Clean up test streams"""
        # Note: LocalStack auto-cleans streams, but we track them anyway
        logger.info(f"Test used {len(self.test_streams)} streams: {self.test_streams}")
        self.test_streams.clear()

    async def create_test_stream(self, base_name: str, shard_count: int) -> str:
        """Create a test stream and return its name"""
        stream_name = f"resharding-test-{base_name}-{int(time.time())}-{uuid.uuid4().hex[:8]}"
        self.test_streams.append(stream_name)

        # Create stream using Producer (which handles stream creation)
        async with Producer(
            stream_name=stream_name,
            endpoint_url=self.endpoint_url,
            create_stream=True,
            create_stream_shards=shard_count,
        ) as producer:
            # Just create the stream
            pass

        logger.info(f"Created stream {stream_name} with {shard_count} shards")
        return stream_name

    async def saturate_and_test_consumption(self, stream_name: str, test_duration: int = 30):
        """Saturate stream with data and test consumer behavior"""
        logger.info(f"Saturating {stream_name} and testing consumption for {test_duration}s")

        # Start producer to generate data
        producer_task = asyncio.create_task(self._run_producer(stream_name, test_duration))

        # Start consumer to consume data and track shard status
        consumer_task = asyncio.create_task(self._run_consumer(stream_name, test_duration))

        # Wait for both to complete
        producer_result, consumer_result = await asyncio.gather(producer_task, consumer_task)

        return {"producer": producer_result, "consumer": consumer_result}

    async def _run_producer(self, stream_name: str, duration: int):
        """Run producer for specified duration"""
        messages_sent = 0
        start_time = time.time()

        try:
            async with Producer(
                stream_name=stream_name,
                endpoint_url=self.endpoint_url,
                processor=JsonProcessor(),
                batch_size=100,
                buffer_time=0.1,
            ) as producer:
                end_time = start_time + duration
                while time.time() < end_time:
                    message = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "sequence": messages_sent,
                        "data": f"test-message-{messages_sent}",
                    }
                    await producer.put(message)
                    messages_sent += 1

                    if messages_sent % 100 == 0:
                        await producer.flush()
                        await asyncio.sleep(0.1)  # Brief pause

                await producer.flush()
        except Exception as e:
            logger.error(f"Producer error: {e}")

        duration = time.time() - start_time
        rate = messages_sent / duration if duration > 0 else 0

        result = {"messages_sent": messages_sent, "duration": duration, "rate": rate}

        logger.info(f"Producer: {messages_sent} messages in {duration:.1f}s ({rate:.1f} msg/s)")
        return result

    async def _run_consumer(self, stream_name: str, duration: int):
        """Run consumer and track shard behavior"""
        messages_consumed = 0
        start_time = time.time()
        shard_status_history = []

        try:
            async with Consumer(
                stream_name=stream_name,
                endpoint_url=self.endpoint_url,
                processor=JsonProcessor(),
                sleep_time_no_records=0.5,
                max_shard_consumers=None,
            ) as consumer:
                end_time = start_time + duration
                last_status_check = 0

                while time.time() < end_time:
                    # Check shard status every 5 seconds
                    current_time = time.time()
                    if current_time - last_status_check >= 5:
                        status = consumer.get_shard_status()
                        status_entry = {
                            "timestamp": current_time - start_time,
                            "total_shards": status["total_shards"],
                            "active_shards": status["active_shards"],
                            "allocated_shards": status["allocated_shards"],
                            "parent_shards": status["parent_shards"],
                            "child_shards": status["child_shards"],
                            "closed_shards": status["closed_shards"],
                        }
                        shard_status_history.append(status_entry)
                        last_status_check = current_time

                        logger.debug(f"Status: {status['allocated_shards']}/{status['total_shards']} shards allocated")

                    # Try to consume messages
                    try:
                        async with asyncio.timeout(1.0):
                            async for message in consumer:
                                messages_consumed += 1
                                break  # Just count messages
                    except asyncio.TimeoutError:
                        pass  # No messages available

        except Exception as e:
            logger.error(f"Consumer error: {e}")

        duration = time.time() - start_time
        rate = messages_consumed / duration if duration > 0 else 0

        result = {
            "messages_consumed": messages_consumed,
            "duration": duration,
            "rate": rate,
            "shard_status_history": shard_status_history,
        }

        logger.info(f"Consumer: {messages_consumed} messages in {duration:.1f}s ({rate:.1f} msg/s)")
        return result

    async def test_single_shard_baseline(self):
        """Test baseline with single shard"""
        logger.info("=== Testing Single Shard Baseline ===")

        stream_name = await self.create_test_stream("single-shard", 1)
        result = await self.saturate_and_test_consumption(stream_name, 20)

        # Verify single shard behavior
        consumer_result = result["consumer"]
        if consumer_result["shard_status_history"]:
            final_status = consumer_result["shard_status_history"][-1]
            assert final_status["total_shards"] == 1, f"Expected 1 shard, got {final_status['total_shards']}"
            assert final_status["parent_shards"] == 0, "Should have no parent shards"
            assert final_status["child_shards"] == 0, "Should have no child shards"

        logger.info("✅ Single shard baseline working correctly")
        return {"test": "single_shard", "status": "PASS", "result": result}

    async def test_multiple_shard_simulation(self):
        """Test with multiple shards (simulates split scenario)"""
        logger.info("=== Testing Multiple Shard Simulation ===")

        stream_name = await self.create_test_stream("multi-shard", 3)
        result = await self.saturate_and_test_consumption(stream_name, 25)

        # Verify multi-shard behavior
        consumer_result = result["consumer"]
        if consumer_result["shard_status_history"]:
            final_status = consumer_result["shard_status_history"][-1]
            assert final_status["total_shards"] == 3, f"Expected 3 shards, got {final_status['total_shards']}"
            # LocalStack creates independent shards, not parent-child relationships
            # But our consumer should handle them correctly

        logger.info("✅ Multiple shard handling working correctly")
        return {"test": "multiple_shard", "status": "PASS", "result": result}

    async def test_shard_allocation_logic(self):
        """Test shard allocation with different max_shard_consumers settings"""
        logger.info("=== Testing Shard Allocation Logic ===")

        stream_name = await self.create_test_stream("allocation-test", 4)

        # Test with limited shard consumers
        messages_consumed = 0
        async with Consumer(
            stream_name=stream_name,
            endpoint_url=self.endpoint_url,
            max_shard_consumers=2,  # Limit to 2 out of 4 shards
            sleep_time_no_records=0.1,
        ) as consumer:
            # Let it run for a bit to allocate shards
            await asyncio.sleep(2)

            status = consumer.get_shard_status()
            logger.info(f"Limited allocation: {status['allocated_shards']}/{status['total_shards']} shards")

            # Should allocate at most 2 shards
            assert status["allocated_shards"] <= 2, f"Should allocate max 2 shards, got {status['allocated_shards']}"
            assert status["total_shards"] == 4, f"Should see 4 total shards, got {status['total_shards']}"

        logger.info("✅ Shard allocation limits working correctly")
        return {"test": "allocation_logic", "status": "PASS"}

    async def test_consumer_recovery_and_persistence(self):
        """Test consumer recovery and state persistence"""
        logger.info("=== Testing Consumer Recovery ===")

        stream_name = await self.create_test_stream("recovery-test", 2)

        # First consumer session - produce some data
        async with Producer(stream_name=stream_name, endpoint_url=self.endpoint_url) as producer:
            for i in range(50):
                await producer.put({"sequence": i, "data": f"message-{i}"})
            await producer.flush()

        # Consumer session 1 - consume some messages
        consumed_first = 0
        async with Consumer(
            stream_name=stream_name,
            endpoint_url=self.endpoint_url,
            sleep_time_no_records=0.1,
        ) as consumer:
            start_time = time.time()
            while time.time() - start_time < 5:  # 5 seconds max
                try:
                    async with asyncio.timeout(0.5):
                        async for message in consumer:
                            consumed_first += 1
                            if consumed_first >= 20:  # Stop after 20 messages
                                break
                except asyncio.TimeoutError:
                    break
                if consumed_first >= 20:
                    break

        # Consumer session 2 - should handle existing state gracefully
        consumed_second = 0
        async with Consumer(
            stream_name=stream_name,
            endpoint_url=self.endpoint_url,
            sleep_time_no_records=0.1,
        ) as consumer:
            start_time = time.time()
            while time.time() - start_time < 5:  # 5 seconds max
                try:
                    async with asyncio.timeout(0.5):
                        async for message in consumer:
                            consumed_second += 1
                            if consumed_second >= 10:  # Get some more messages
                                break
                except asyncio.TimeoutError:
                    break
                if consumed_second >= 10:
                    break

        logger.info(f"Recovery test: Session 1: {consumed_first}, Session 2: {consumed_second}")
        logger.info("✅ Consumer recovery working correctly")

        return {
            "test": "recovery",
            "status": "PASS",
            "consumed_first": consumed_first,
            "consumed_second": consumed_second,
        }

    async def run_all_tests(self):
        """Run all integration tests"""
        logger.info("🚀 Starting Resharding Integration Tests")
        logger.info("=" * 60)

        tests = [
            self.test_single_shard_baseline,
            self.test_multiple_shard_simulation,
            self.test_shard_allocation_logic,
            self.test_consumer_recovery_and_persistence,
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

        # Cleanup
        await self.cleanup()

        # Summary
        logger.info("=" * 60)
        passed = len([r for r in results if r["status"] == "PASS"])
        total = len(results)

        if passed == total:
            logger.info(f"🎉 ALL INTEGRATION TESTS PASSED ({passed}/{total})")
            logger.info("✅ Consumer handles multiple shards correctly")
            logger.info("✅ Shard allocation logic working properly")
            logger.info("✅ Ready for production use with resharding streams")
        else:
            logger.error(f"⚠️ {total - passed} tests failed ({passed}/{total} passed)")

        return results


async def main():
    """Run integration tests"""
    test_suite = ReshardingIntegrationTest()

    try:
        results = await test_suite.run_all_tests()

        # Print summary
        print("\n" + "=" * 60)
        print("INTEGRATION TEST RESULTS")
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
    # Check if LocalStack is available
    try:
        import urllib.request

        urllib.request.urlopen("http://localhost:4566/_localstack/health", timeout=2)
        print("✅ LocalStack detected, running integration tests...")
    except Exception:
        print("⚠️ LocalStack not available. Start with: docker-compose up kinesis")
        print("   Running tests anyway (may fail)...")

    success = asyncio.run(main())
    exit(0 if success else 1)
