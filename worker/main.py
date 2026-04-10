"""
worker/main.py
BRPOP loop — the background worker service.
Blocks on Redis queue, receives jobs, runs the full pipeline per job.

Change in this version:
- run_pipeline() is now async — wrapped with asyncio.run() here.
  All other logic is unchanged from the previous version.

Run with: python -m worker.main
"""

import asyncio
import time
import logging
import sys
from common.redis_client import dequeue_job, set_job_status
from common.db import mark_job_done, mark_job_partial, mark_job_failed, get_order
from worker.pipeline import run_pipeline
from worker.xlsx_builder import build_and_upload
from worker.sheets_log import log_job

# Configure logging — Railway captures stdout/stderr
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def process_job(order_id: str, query: str):
    """
    Full job lifecycle for a single order:
    1. Run adaptive pipeline (async — wrapped with asyncio.run)
    2. Build Excel if any results
    3. Update Postgres + Redis
    4. Log to Google Sheets
    """
    start_time = time.time()
    logger.info(f"Processing job — order_id={order_id}, query='{query}'")

    # Mark as processing in Redis (frontend polls this)
    set_job_status(order_id, "processing")

    results = []
    keywords_used = 0

    try:
        # run_pipeline is now async — run it to completion from this sync context.
        # asyncio.run() creates a new event loop, runs the coroutine, then closes it.
        # Safe here because process_job is called from a synchronous BRPOP loop.
        results, keywords_used = asyncio.run(run_pipeline(query))
    except Exception as e:
        logger.error(f"Pipeline error for {order_id}: {e}", exc_info=True)
        # Pipeline itself threw — mark failed
        set_job_status(order_id, "failed")
        mark_job_failed(order_id)
        log_job(order_id, query, 0, "failed", time.time() - start_time)
        return

    elapsed = time.time() - start_time
    result_count = len(results)
    logger.info(f"Pipeline done — {result_count} results in {elapsed:.1f}s")

    # ── NO RESULTS ──────────────────────────────────────────────────────────
    if result_count == 0:
        logger.warning(f"Zero results for order {order_id} — marking failed")
        set_job_status(order_id, "failed")
        mark_job_failed(order_id)
        log_job(order_id, query, 0, "failed", elapsed)
        return

    # ── BUILD EXCEL + UPLOAD TO R2 ──────────────────────────────────────────
    try:
        archive_key = build_and_upload(order_id, query, results)
    except Exception as e:
        logger.error(f"Excel build/upload failed for {order_id}: {e}", exc_info=True)
        set_job_status(order_id, "failed")
        mark_job_failed(order_id)
        log_job(order_id, query, result_count, "failed", elapsed)
        return

    # ── UPDATE STATUS ────────────────────────────────────────────────────────
    from common.config import TARGET_RESULT_COUNT

    if result_count >= TARGET_RESULT_COUNT:
        job_status = "done"
        set_job_status(order_id, "done", result_count)
        mark_job_done(order_id, result_count, archive_key)
    else:
        # 1 to TARGET-1 results — partial delivery
        job_status = "partial"
        set_job_status(order_id, "partial", result_count)
        mark_job_partial(order_id, result_count, archive_key)

    # ── LOG TO SHEETS ────────────────────────────────────────────────────────
    log_job(order_id, query, result_count, job_status, elapsed)

    logger.info(f"Job complete — order={order_id}, status={job_status}, results={result_count}")


def main():
    """
    Infinite BRPOP loop.
    Blocks until a job arrives, processes it, then blocks again.
    Railway will restart this process automatically if it crashes.
    """
    logger.info("Worker started — waiting for jobs on Redis queue")

    while True:
        try:
            job = dequeue_job(timeout=0)  # Blocks indefinitely
            if job is None:
                # Timeout (shouldn't happen with timeout=0 but defensive)
                continue

            order_id = job.get("order_id")
            query    = job.get("query")

            if not order_id or not query:
                logger.warning(f"Malformed job payload: {job}")
                continue

            # Verify the order exists and is paid before processing
            order = get_order(order_id)
            if not order:
                logger.warning(f"Order {order_id} not found in Postgres — skipping")
                continue

            if order.get("payment_status") != "paid":
                logger.warning(f"Order {order_id} payment_status={order.get('payment_status')} — skipping")
                continue

            process_job(order_id, query)

        except KeyboardInterrupt:
            logger.info("Worker shutting down")
            break
        except Exception as e:
            # Catch-all — log and keep the loop alive
            logger.error(f"Unhandled error in worker loop: {e}", exc_info=True)
            time.sleep(2)  # Brief pause before next BRPOP to avoid tight error loops


if __name__ == "__main__":
    main()
