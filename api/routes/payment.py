"""
api/routes/payment.py
POST /api/initiate-payment

Flow:
1. Validate product name (regex)
2. Create Razorpay order via direct HTTP (no SDK — avoids pkg_resources
   incompatibility with Python 3.13)
3. Write pending row to Postgres
4. Return Razorpay order details to frontend
"""

import re
import base64
import logging
import httpx
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

# Razorpay Orders API endpoint
RAZORPAY_ORDERS_URL = "https://api.razorpay.com/v1/orders"


def _razorpay_auth() -> str:
    """
    Razorpay uses HTTP Basic Auth.
    Encode key_id:key_secret as base64.
    """
    credentials = f"{RAZORPAY_KEY_ID}:{RAZORPAY_KEY_SECRET}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return f"Basic {encoded}"


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
    Creates a Razorpay order via direct HTTP POST.
    Returns order details for frontend Razorpay JS SDK checkout.
    """
    query = body.query

    # Create Razorpay order via direct HTTP — no SDK needed
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                RAZORPAY_ORDERS_URL,
                headers={
                    "Authorization": _razorpay_auth(),
                    "Content-Type": "application/json",
                },
                json={
                    "amount": AMOUNT_PAISE,
                    "currency": "INR",
                    "payment_capture": 1,
                    "notes": {
                        "query": query,
                        "product": "khareedo",
                    }
                }
            )
        response.raise_for_status()
        order = response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Razorpay order creation failed: {e.response.status_code} {e.response.text}")
        raise HTTPException(status_code=502, detail="Payment gateway error")
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

    # Return order details — frontend uses Razorpay JS SDK to open
    # checkout modal directly (see index.html script block).
    return {
        "order_id":     order_id,
        "key_id":       RAZORPAY_KEY_ID,
        "amount":       AMOUNT_PAISE,
        "currency":     "INR",
        "callback_url": f"{APP_BASE_URL}/result?id={order_id}",
        "query":        query,
    }
```

---

**Then push:**
```
cd ~/Desktop/khareedo
git add api/routes/payment.py requirements.txt
git commit -m "fix: replace razorpay SDK with direct httpx call - Python 3.13 compat"
git push
