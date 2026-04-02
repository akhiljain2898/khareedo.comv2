"""
api/routes/job.py
GET /api/job-status?id={order_id}  — polled by result.html every 2 seconds
GET /api/download?id={order_id}    — streams XLSX from R2 to the browser
"""

import logging
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
import io
from botocore.exceptions import ClientError
from common.redis_client import get_job_status
from common.db import get_order, mark_download_initiated
from common.r2_client import get_bytes
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
    2. Attempt to fetch bytes directly from R2 (no separate existence check)
    3. If R2 returns 404/NoSuchKey → file expired, return 410
    4. Stream response to browser
    5. Mark download_initiated in Postgres
    6. Log to Google Sheets Downloads tab

    NOTE: We do NOT delete the R2 download key after download.
    The 30-minute lifecycle TTL on the download/ prefix handles cleanup automatically.
    This allows the user to re-download within the 30-minute window.

    NOTE: No separate object_exists() check — we go straight to get_bytes().
    One R2 call instead of two. ClientError with NoSuchKey = expired.
    """
    order_id = id.strip()

    # Verify order exists
    order = get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    # Verify payment is confirmed
    if order.get("payment_status") != "paid":
        raise HTTPException(status_code=403, detail="Payment not confirmed")

    # Verify job is in a downloadable state
    job_status = order.get("job_status")
    if job_status not in ("done", "partial"):
        raise HTTPException(status_code=404, detail="File not ready")

    # Fetch file bytes directly from R2
    # If the file doesn't exist (expired TTL), ClientError is raised with NoSuchKey
    download_key = f"download/{order_id}.xlsx"
    try:
        xlsx_bytes = get_bytes(download_key)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("NoSuchKey", "404"):
            logger.warning(f"R2 key not found for {order_id} — likely expired")
            raise HTTPException(
                status_code=410,
                detail="Download link has expired. Email admin@verifiedwork.co with your Order ID."
            )
        logger.error(f"R2 ClientError for {order_id}: {e}")
        raise HTTPException(status_code=502, detail="Could not retrieve file")
    except Exception as e:
        logger.error(f"R2 fetch failed for {order_id}: {e}")
        raise HTTPException(status_code=502, detail="Could not retrieve file")

    # Build a clean, readable filename for the downloaded file
    query_slug = order.get("query", "suppliers").replace(" ", "_")[:40]
    filename = f"vendordhundo_{query_slug}_{order_id[:8]}.xlsx"

    # Mark download initiated in Postgres (non-fatal if it fails)
    try:
        mark_download_initiated(order_id)
    except Exception as e:
        logger.error(f"Failed to mark download_initiated for {order_id}: {e}")

    # Log to Sheets Downloads tab (non-fatal if it fails)
    try:
        log_download(order_id, order.get("query", ""))
    except Exception as e:
        logger.error(f"Sheets download log failed for {order_id}: {e}")

    # Stream the file to the browser
    return StreamingResponse(
        io.BytesIO(xlsx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(xlsx_bytes)),
        }
    )
