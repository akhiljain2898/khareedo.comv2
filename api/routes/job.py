"""
api/routes/job.py
GET /api/job-status?id={order_id}  — polled by result.html every 2 seconds
GET /api/download?id={order_id}    — streams XLSX from R2 to the browser

FIX (Apr 2026):
The download/ prefix has a 30-minute R2 lifecycle TTL.
When that expires, fall back to archive/ (7-day TTL) before returning 410.
This means users can re-download within 7 days of job completion.
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
    2. Try download/{order_id}.xlsx (30-min TTL, may be expired)
    3. If expired (NoSuchKey), fall back to archive/{order_id}.xlsx (7-day TTL)
    4. If both missing, return 410
    5. Stream response to browser
    6. Mark download_initiated in Postgres
    7. Log to Google Sheets Downloads tab
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

    # ── FETCH FROM R2 ─────────────────────────────────────────────────────────
    # Try download/ first (short TTL). Fall back to archive/ (7-day TTL).
    # This lets users re-download within 7 days even after the 30-min window.
    xlsx_bytes = None
    download_key = f"download/{order_id}.xlsx"
    archive_key  = f"archive/{order_id}.xlsx"

    try:
        xlsx_bytes = get_bytes(download_key)
        logger.info(f"Served from download/: {order_id}")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("NoSuchKey", "404"):
            # download/ expired — try archive/
            logger.info(f"download/ expired for {order_id}, trying archive/")
            try:
                xlsx_bytes = get_bytes(archive_key)
                logger.info(f"Served from archive/: {order_id}")
            except ClientError as e2:
                error_code2 = e2.response.get("Error", {}).get("Code", "")
                if error_code2 in ("NoSuchKey", "404"):
                    logger.warning(f"Both download/ and archive/ missing for {order_id} — fully expired")
                    raise HTTPException(
                        status_code=410,
                        detail="Download link has expired. Email admin@verifiedwork.co with your Order ID."
                    )
                logger.error(f"R2 ClientError on archive/ for {order_id}: {e2}")
                raise HTTPException(status_code=502, detail="Could not retrieve file")
            except Exception as e2:
                logger.error(f"R2 fetch failed on archive/ for {order_id}: {e2}")
                raise HTTPException(status_code=502, detail="Could not retrieve file")
        else:
            logger.error(f"R2 ClientError on download/ for {order_id}: {e}")
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
