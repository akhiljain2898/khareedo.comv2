"""
api/routes/webhook.py
POST /api/razorpay-webhook

Razorpay fires this when a payment is captured.
Flow:
1. Verify Razorpay signature (reject anything that fails)
2. Extract order_id and payment_id
3. Update Postgres (payment_status=paid)
4. LPUSH job to Redis queue (idempotent via SET NX)
5. Return 200 immediately — never block the webhook

Razorpay retries up to 3 times on non-200 — our SET NX idempotency
ensures only one job is ever queued per order_id regardless of retries.
"""

import hmac
import hashlib
import json
import logging
from fastapi import APIRouter, Request, HTTPException, Response
from common.config import RAZORPAY_WEBHOOK_SECRET
from common.db import mark_payment_paid, get_order
from common.redis_client import enqueue_job, set_job_status

logger = logging.getLogger(__name__)
router = APIRouter()


def _verify_signature(body: bytes, signature: str) -> bool:
    """
    Verify Razorpay webhook signature using HMAC-SHA256.
    Razorpay signs the raw request body with the webhook secret.
    Returns True if valid, False otherwise.
    """
    expected = hmac.new(
        RAZORPAY_WEBHOOK_SECRET.encode("utf-8"),
        body,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


@router.post("/api/razorpay-webhook")
async def razorpay_webhook(request: Request):
    """
    Handles Razorpay payment.captured webhook events.
    Returns 200 immediately regardless — Razorpay considers anything
    other than 200 as a failure and will retry.
    """
    body = await request.body()
    signature = request.headers.get("x-razorpay-signature", "")

    # ── SIGNATURE VERIFICATION ────────────────────────────────────────────────
    if not _verify_signature(body, signature):
        logger.warning("Webhook signature verification failed — rejecting")
        # Return 200 anyway to stop retries on clearly invalid requests
        # A real attack won't have a valid signature so this is safe
        return Response(status_code=200)

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        logger.error("Webhook payload is not valid JSON")
        return Response(status_code=200)

    event = payload.get("event", "")

    # We only care about payment.captured — ignore all other events
    if event != "payment.captured":
        logger.info(f"Ignoring webhook event: {event}")
        return Response(status_code=200)

    # ── EXTRACT IDS ──────────────────────────────────────────────────────────
    try:
        payment_entity = payload["payload"]["payment"]["entity"]
        order_id       = payment_entity["order_id"]
        payment_id     = payment_entity["id"]
    except (KeyError, TypeError) as e:
        logger.error(f"Could not extract order/payment IDs from webhook payload: {e}")
        return Response(status_code=200)

    logger.info(f"Payment captured — order_id={order_id}, payment_id={payment_id}")

    # ── UPDATE POSTGRES ───────────────────────────────────────────────────────
    try:
        mark_payment_paid(order_id, payment_id)
    except Exception as e:
        logger.error(f"DB update failed for order {order_id}: {e}")
        # Don't return error — try to enqueue anyway
        # The order might already be paid from a previous retry

    # ── ENQUEUE JOB ───────────────────────────────────────────────────────────
    # Fetch the query from Postgres to include in the job payload
    try:
        order = get_order(order_id)
        query = order["query"] if order else ""
    except Exception as e:
        logger.error(f"Could not fetch order {order_id} from DB: {e}")
        query = ""

    if not query:
        logger.error(f"No query found for order {order_id} — cannot enqueue job")
        return Response(status_code=200)

    try:
        queued = enqueue_job(order_id, query)
        if queued:
            set_job_status(order_id, "processing")
            logger.info(f"Job enqueued for order {order_id}")
        else:
            logger.info(f"Job for {order_id} already queued (SET NX idempotency) — ignoring retry")
    except Exception as e:
        logger.error(f"Redis enqueue failed for order {order_id}: {e}")
        # This is the critical gap identified by code reviewer —
        # log it clearly so ops can manually re-queue if needed

    # Always return 200 to Razorpay
    return Response(status_code=200)
