"""
common/db.py
Postgres connection + all read/write helpers.
Shared by both the API service and the worker service.
Uses psycopg v3 — compatible with Python 3.13.
"""

import psycopg
from psycopg.rows import dict_row
from contextlib import contextmanager
from common.config import DATABASE_URL


def get_connection():
    return psycopg.connect(DATABASE_URL)


@contextmanager
def get_cursor():
    conn = get_connection()
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def create_tables():
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
    CREATE INDEX IF NOT EXISTS idx_transactions_order_id ON transactions(order_id);
    """
    with get_cursor() as cur:
        cur.execute(sql)


def create_pending_order(order_id: str, query: str):
    sql = """
    INSERT INTO transactions (order_id, query, payment_status, job_status)
    VALUES (%s, %s, 'pending', 'pending')
    ON CONFLICT (order_id) DO NOTHING;
    """
    with get_cursor() as cur:
        cur.execute(sql, (order_id, query))


def mark_payment_paid(order_id: str, razorpay_txn_id: str):
    sql = """
    UPDATE transactions
    SET payment_status  = 'paid',
        job_status      = 'processing',
        razorpay_txn_id = %s,
        updated_at      = NOW()
    WHERE order_id = %s;
    """
    with get_cursor() as cur:
        cur.execute(sql, (razorpay_txn_id, order_id))


def mark_job_done(order_id: str, result_count: int, archive_key: str):
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
    sql = """
    UPDATE transactions
    SET job_status = 'failed',
        updated_at = NOW()
    WHERE order_id = %s;
    """
    with get_cursor() as cur:
        cur.execute(sql, (order_id,))


def mark_download_initiated(order_id: str):
    sql = """
    UPDATE transactions
    SET download_initiated = TRUE,
        updated_at         = NOW()
    WHERE order_id = %s;
    """
    with get_cursor() as cur:
        cur.execute(sql, (order_id,))


def get_order(order_id: str) -> dict | None:
    sql = "SELECT * FROM transactions WHERE order_id = %s;"
    with get_cursor() as cur:
        cur.execute(sql, (order_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def get_stuck_processing_orders() -> list[dict]:
    sql = """
    SELECT * FROM transactions
    WHERE job_status = 'processing'
      AND updated_at < NOW() - INTERVAL '5 minutes';
    """
    with get_cursor() as cur:
        cur.execute(sql)
        return [dict(row) for row in cur.fetchall()]
