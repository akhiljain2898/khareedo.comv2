"""
api/routes/job.py
GET /api/job-status?id={order_id}  — polled by result.html every 2 seconds
GET /api/download?id={order_id}    — streams XLSX from R2 to the browser
"""

import logging
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response, StreamingResponse
import io
from common.redis_client import get_job_status
from common.db import get_order, mark_download_initiated
from common.r2_client import get_bytes, delete_object, object_exists
from worker.sheets_log import log_download

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/api/job-status")
async def job_status(id: str = Query(..., min_length=1)):
    """
    Returns current job status for a given order_id.
    Frontend polls this every 2 seconds.

    Response shape:
    {
        "status": "pending" | "processing" | "done" | "partial" | "failed",
        "result_count": 12  // only present when done or partial
    }

    Reads from Redis first (fast, ephemeral).
    Falls back to Postgres if Redis key has expired.
    """
    order_id = id.strip()

    # Try Redis first — this is the hot path
    redis_state = get_job_status(order_id)
    if redis_state:
        return redis_state

    # Redis key expired (>24h) or doesn't exist yet — fall back to Postgres
    order = get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    response = {"status": order["job_status"]}
    if order.get("result_count") is not None:
        response["result_count"] = order["result_count"]
    return response


@router.get("/api/download")
async def download_excel(id: str = Query(..., min_length=1)):
    """
    Stream the XLSX file from R2 to the browser.

    Flow:
    1. Verify order exists and payment is paid
    2. Fetch bytes from R2 download key
    3. Stream response
    4. Mark download_initiated in Postgres
    5. Delete download/ key from R2 (archive/ copy untouched)
    6. Log to Google Sheets Downloads tab
    """
    order_id = id.strip()

    # Verify order
    order = get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    if order.get("payment_status") != "paid":
        raise HTTPException(status_code=403, detail="Payment not confirmed")

    job_status = order.get("job_status")
    if job_status not in ("done", "partial"):
        raise HTTPException(status_code=404, detail="File not ready")

    # Fetch from R2
    download_key = f"download/{order_id}.xlsx"

    if not object_exists(download_key):
        # Link has expired (30 min TTL)
        raise HTTPException(
            status_code=410,
            detail="Download link has expired. Email admin@khareedo.com with your Order ID."
        )

    try:
        xlsx_bytes = get_bytes(download_key)
    except Exception as e:
        logger.error(f"R2 fetch failed for {order_id}: {e}")
        raise HTTPException(status_code=502, detail="Could not retrieve file")

    # Build safe filename from query
    query_slug = order.get("query", "suppliers").replace(" ", "_")[:40]
    filename = f"khareedo_{query_slug}_{order_id[:8]}.xlsx"

    # Mark download initiated in Postgres
    try:
        mark_download_initiated(order_id)
    except Exception as e:
        logger.error(f"Failed to mark download_initiated for {order_id}: {e}")
        # Non-fatal — continue with delivery

    # Delete the download key from R2 (archive copy stays)
    try:
        delete_object(download_key)
    except Exception as e:
        logger.error(f"Failed to delete R2 download key for {order_id}: {e}")
        # Non-fatal — file may be re-downloaded until TTL expires naturally

    # Log to Sheets Downloads tab
    try:
        log_download(order_id, order.get("query", ""))
    except Exception as e:
        logger.error(f"Sheets download log failed for {order_id}: {e}")
        # Non-fatal

    # Stream the file
    return StreamingResponse(
        io.BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(xlsx_bytes)),
        }
    )
