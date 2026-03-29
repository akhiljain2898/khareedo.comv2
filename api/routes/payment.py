"""
api/routes/payment.py
POST /api/initiate-payment

Flow:
1. Validate product name (regex)
2. Create Razorpay order
3. Write pending row to Postgres
4. Return Razorpay payment URL to frontend
"""

import re
import logging
import razorpay
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from common.config import RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET, APP_BASE_URL
from common.db import create_pending_order

logger = logging.getLogger(__name__)
router = APIRouter()

# Matches the same regex validated client-side in index.html
QUERY_REGEX = re.compile(r'^[a-zA-Z0-9\s\-]{3,100}$')

# ₹99 in paise (Razorpay uses smallest currency unit)
AMOUNT_PAISE = 9900

_razorpay_client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))


class InitiatePaymentRequest(BaseModel):
    query: str

    @field_validator("query")
    @classmethod
    def validate_query(cls, v: str) -> str:
        v = v.strip()
        if not QUERY_REGEX.match(v):
            raise ValueError("Invalid product name")
        return v


@router.post("/api/initiate-payment")
async def initiate_payment(body: InitiatePaymentRequest):
    """
    Creates a Razorpay order and a pending row in Postgres.
    Returns the payment_url for frontend redirect.
    """
    query = body.query

    # Create Razorpay order
    try:
        order = _razorpay_client.order.create({
            "amount": AMOUNT_PAISE,
            "currency": "INR",
            "payment_capture": 1,  # Auto-capture on payment
            "notes": {
                "query": query,
                "product": "khareedo",
            }
        })
    except Exception as e:
        logger.error(f"Razorpay order creation failed: {e}")
        raise HTTPException(status_code=502, detail="Payment gateway error")

    order_id = order["id"]

    # Write pending row to Postgres
    try:
        create_pending_order(order_id, query)
    except Exception as e:
        logger.error(f"DB write failed for order {order_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal error")

    logger.info(f"Order created: {order_id} for query='{query}'")

    # Return order details — the frontend uses Razorpay's JS SDK to open
    # the checkout modal directly (see index.html script block).
    # We return the order_id, key_id, amount, and callback URL.
    return {
        "order_id":   order_id,
        "key_id":     RAZORPAY_KEY_ID,
        "amount":     AMOUNT_PAISE,
        "currency":   "INR",
        "callback_url": f"{APP_BASE_URL}/result?id={order_id}",
        "query":      query,
    }
