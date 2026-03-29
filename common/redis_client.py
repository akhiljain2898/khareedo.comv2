"""
common/redis_client.py
Upstash Redis client.
Handles job queue (LPUSH/BRPOP) and job state (GET/SET).
"""

import json
import redis
from common.config import UPSTASH_REDIS_URL, REDIS_QUEUE_KEY


def get_client() -> redis.Redis:
    """
    Returns a Redis client connected to Upstash.
    Upstash uses rediss:// (TLS) — ssl_cert_reqs must be None for compatibility.
    """
    return redis.from_url(
        UPSTASH_REDIS_URL,
        decode_responses=True,
        ssl_cert_reqs=None,
    )


# ── JOB QUEUE ────────────────────────────────────────────────────────────────

def enqueue_job(order_id: str, query: str) -> bool:
    """
    LPUSH a job payload onto the queue.
    Returns True on success, False on failure.
    Uses SET NX for idempotency — only one job per order_id ever enters the queue.
    """
    client = get_client()

    # Idempotency: only enqueue if this order hasn't been queued before
    # SET NX on a lock key — expires after 24h
    lock_key = f"queued:{order_id}"
    acquired = client.set(lock_key, "1", nx=True, ex=86400)  # 24h TTL
    if not acquired:
        # Already queued — Razorpay webhook retry, ignore safely
        return False

    payload = json.dumps({"order_id": order_id, "query": query})
    client.lpush(REDIS_QUEUE_KEY, payload)
    return True


def dequeue_job(timeout: int = 0) -> dict | None:
    """
    BRPOP from the queue. Blocks until a job arrives (timeout=0 = infinite).
    Returns the parsed job dict, or None on timeout.
    """
    client = get_client()
    result = client.brpop(REDIS_QUEUE_KEY, timeout=timeout)
    if result is None:
        return None
    _, payload = result
    return json.loads(payload)


# ── JOB STATE ────────────────────────────────────────────────────────────────
# Separate from Postgres — Redis job state is the fast polling layer.
# Postgres is the permanent audit trail.

JOB_TTL = 86400  # 24 hours


def set_job_status(order_id: str, status: str, result_count: int | None = None):
    """Store job status in Redis. Polled by frontend every 2 seconds."""
    client = get_client()
    data = {"status": status}
    if result_count is not None:
        data["result_count"] = result_count
    client.set(f"job:{order_id}", json.dumps(data), ex=JOB_TTL)


def get_job_status(order_id: str) -> dict | None:
    """Return job state dict or None if key doesn't exist / has expired."""
    client = get_client()
    val = client.get(f"job:{order_id}")
    if val is None:
        return None
    return json.loads(val)
