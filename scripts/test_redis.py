"""
scripts/test_redis.py
Verifies Upstash Redis connection with a real LPUSH / BRPOP round-trip.

Usage:
    python scripts/test_redis.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.redis_client import enqueue_job, dequeue_job, set_job_status, get_job_status

TEST_ORDER_ID = "test_order_SETUP"
TEST_QUERY    = "test product"

def main():
    print("Testing Redis connection...")

    # Test job state SET/GET
    try:
        set_job_status(TEST_ORDER_ID, "processing")
        state = get_job_status(TEST_ORDER_ID)
        assert state["status"] == "processing", f"Expected 'processing', got {state}"
        print("✓ Job state SET/GET works")
    except Exception as e:
        print(f"✗ Job state test failed: {e}")
        sys.exit(1)

    # Test queue LPUSH / BRPOP
    try:
        queued = enqueue_job(TEST_ORDER_ID, TEST_QUERY)
        # Note: idempotency lock means second call returns False
        # So we use a fresh test order
        test_order_2 = "test_order_QUEUE_TEST"
        queued = enqueue_job(test_order_2, TEST_QUERY)
        assert queued is True, "Expected True from enqueue_job"
        print("✓ LPUSH works")

        job = dequeue_job(timeout=3)
        assert job is not None, "Expected a job from queue"
        assert job["order_id"] == test_order_2
        print("✓ BRPOP works")
    except Exception as e:
        print(f"✗ Queue test failed: {e}")
        sys.exit(1)

    print("\nAll done. Redis is ready.")

if __name__ == "__main__":
    main()
