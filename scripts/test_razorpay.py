"""
scripts/test_razorpay.py
Verifies Razorpay credentials by creating and fetching a test order.
Run this AFTER completing Razorpay KYC and switching to test mode.

Usage:
    python scripts/test_razorpay.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import razorpay
from common.config import RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET

def main():
    print("Testing Razorpay connection...")
    print(f"  Key ID: {RAZORPAY_KEY_ID[:12]}...")

    client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))

    print("  Creating test order for ₹99 (9900 paise)...")
    try:
        order = client.order.create({
            "amount": 9900,
            "currency": "INR",
            "payment_capture": 1,
            "notes": {"test": "setup_check"}
        })
        order_id = order["id"]
        print(f"  ✓ Order created: {order_id}")
        print(f"  ✓ Status: {order['status']}")
        print(f"  ✓ Amount: ₹{order['amount'] / 100}")
    except Exception as e:
        print(f"  ✗ Order creation failed: {e}")
        print("\nCommon causes:")
        print("  - Wrong RAZORPAY_KEY_ID or RAZORPAY_KEY_SECRET")
        print("  - Account not activated (KYC pending)")
        sys.exit(1)

    print("\nAll done. Razorpay is ready.")
    print("\nNext steps:")
    print("  1. Go to Razorpay dashboard → Settings → Webhooks")
    print("  2. Add webhook URL: https://YOUR_DOMAIN/api/razorpay-webhook")
    print("  3. Select event: payment.captured")
    print("  4. Copy the webhook secret into RAZORPAY_WEBHOOK_SECRET in your .env")

if __name__ == "__main__":
    main()
