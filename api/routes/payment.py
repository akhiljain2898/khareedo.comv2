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

QUERY_REGEX = re.compile(r'^[a-zA-Z0-9\s\-]{3,100}$')
AMOUNT_PAISE = 2000
RAZORPAY_ORDERS_URL = "https://api.razorpay.com/v1/orders"

def _razorpay_auth() -> str:
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
    query = body.query
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
                    "notes": {"query": query, "product": "khareedo"}
                }
            )
        response.raise_for_status()
        order = response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Razorpay order creation failed: {e.response.status_code}")
        raise HTTPException(status_code=502, detail="Payment gateway error")
    except Exception as e:
        logger.error(f"Razorpay order creation failed: {e}")
        raise HTTPException(status_code=502, detail="Payment gateway error")

    order_id = order["id"]

    try:
        create_pending_order(order_id, query)
    except Exception as e:
        logger.error(f"DB write failed for order {order_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal error")

    logger.info(f"Order created: {order_id} for query='{query}'")

    return {
        "order_id":     order_id,
        "key_id":       RAZORPAY_KEY_ID,
        "amount":       AMOUNT_PAISE,
        "currency":     "INR",
        "callback_url": f"{APP_BASE_URL}/result?id={order_id}",
        "query":        query,
    }
