"""
common/db.py
Postgres connection + all read/write helpers.
Shared by both the API service and the worker service.
"""

import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from common.config import DATABASE_URL


def get_connection():
    """Return a new psycopg2 connection. Caller is responsible for closing."""
    return psycopg2.connect(DATABASE_URL)


@contextmanager
def get_cursor():
    """Context manager: yields a cursor, commits on success, rolls back on error."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── SCHEMA SETUP ─────────────────────────────────────────────────────────────

def create_tables():
    """
    Run once on startup or manually to create the transactions table.
    Safe to run multiple times (uses IF NOT EXISTS).
    """
    sql = """
    CREATE TABLE IF NOT EXISTS transactions (
        id                  SERIAL PRIMARY KEY,
        order_id            TEXT UNIQUE NOT NULL,
        razorpay_txn_id     TEXT,
        query               TEXT NOT NULL,
        payment_status      TEXT NOT NULL DEFAULT 'pending',
        job_status          TEXT NOT NULL DEFAULT 'pending',
        result_count        INTEGER,
        download_initiated  BOOLEAN NOT NULL DEFAULT FALSE,
        archive_key         TEXT,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_transactions_order_id
        ON transactions(order_id);
    """
    with get_cursor() as cur:
        cur.execute(sql)


# ── WRITE OPERATIONS ─────────────────────────────────────────────────────────

def create_pending_order(order_id: str, query: str):
    """
    Called by API on payment initiation.
    Creates a new row with payment_status='pending', job_status='pending'.
    """
    sql = """
    INSERT INTO transactions (order_id, query, payment_status, job_status)
    VALUES (%s, %s, 'pending', 'pending')
    ON CONFLICT (order_id) DO NOTHING;
    """
    with get_cursor() as cur:
        cur.execute(sql, (order_id, query))


def mark_payment_paid(order_id: str, razorpay_txn_id: str):
    """
    Called by webhook handler after signature verification.
    Updates payment_status=paid, job_status=processing.
    """
    sql = """
    UPDATE transactions
    SET payment_status = 'paid',
        job_status     = 'processing',
        razorpay_txn_id = %s,
        updated_at     = NOW()
    WHERE order_id = %s;
    """
    with get_cursor() as cur:
        cur.execute(sql, (razorpay_txn_id, order_id))


def mark_job_done(order_id: str, result_count: int, archive_key: str):
    """
    Called by worker on successful pipeline completion.
    """
    sql = """
    UPDATE transactions
    SET job_status   = 'done',
        result_count = %s,
        archive_key  = %s,
        updated_at   = NOW()
    WHERE order_id = %s;
    """
    with get_cursor() as cur:
        cur.execute(sql, (result_count, archive_key, order_id))


def mark_job_partial(order_id: str, result_count: int, archive_key: str):
    """
    Called by worker when loop exits with 1–14 results.
    """
    sql = """
    UPDATE transactions
    SET job_status   = 'partial',
        result_count = %s,
        archive_key  = %s,
        updated_at   = NOW()
    WHERE order_id = %s;
    """
    with get_cursor() as cur:
        cur.execute(sql, (result_count, archive_key, order_id))


def mark_job_failed(order_id: str):
    """
    Called by worker when loop exits with 0 results or unrecoverable error.
    """
    sql = """
    UPDATE transactions
    SET job_status = 'failed',
        updated_at = NOW()
    WHERE order_id = %s;
    """
    with get_cursor() as cur:
        cur.execute(sql, (order_id,))


def mark_download_initiated(order_id: str):
    """
    Called by API when /api/download is hit.
    Sets download_initiated=true. Note: this fires on request,
    not on confirmed delivery (see architecture doc for rationale).
    """
    sql = """
    UPDATE transactions
    SET download_initiated = TRUE,
        updated_at         = NOW()
    WHERE order_id = %s;
    """
    with get_cursor() as cur:
        cur.execute(sql, (order_id,))


# ── READ OPERATIONS ───────────────────────────────────────────────────────────

def get_order(order_id: str) -> dict | None:
    """Return the full transaction row for a given order_id, or None."""
    sql = "SELECT * FROM transactions WHERE order_id = %s;"
    with get_cursor() as cur:
        cur.execute(sql, (order_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def get_stuck_processing_orders() -> list[dict]:
    """
    Returns orders stuck in job_status='processing' for more than 5 minutes.
    Used by worker startup sweep (if enabled in future).
    """
    sql = """
    SELECT * FROM transactions
    WHERE job_status = 'processing'
      AND updated_at < NOW() - INTERVAL '5 minutes';
    """
    with get_cursor() as cur:
        cur.execute(sql)
        return [dict(row) for row in cur.fetchall()]
