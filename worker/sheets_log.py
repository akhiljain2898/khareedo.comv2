"""
worker/sheets_log.py
Google Sheets ops log — two tabs:
  Tab 1 "Jobs Log"      — written by worker on job completion
  Tab 2 "Downloads Log" — written by API on /api/download request

Uses gspread with a service account (no OAuth, no user login required).
The Sheet is created programmatically on first run via setup_sheet().
"""

import json
import logging
from datetime import datetime, timezone
import gspread
from google.oauth2.service_account import Credentials
from common.config import GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SHEET_ID

logger = logging.getLogger(__name__)

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Tab names
JOBS_TAB      = "Jobs Log"
DOWNLOADS_TAB = "Downloads Log"

# Column headers for each tab
JOBS_HEADERS = [
    "Timestamp", "Order ID", "Query",
    "Result Count", "Job Status", "Time Taken (s)", "Payment Status"
]

DOWNLOADS_HEADERS = [
    "Timestamp", "Order ID", "Query", "Downloaded"
]


def _get_client() -> gspread.Client:
    """Build and return an authenticated gspread client from service account JSON."""
    sa_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return gspread.authorize(creds)


def _get_or_create_tab(sheet, tab_name: str, headers: list[str]) -> gspread.Worksheet:
    """
    Get an existing worksheet by name, or create it with the given headers.
    """
    try:
        ws = sheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=tab_name, rows=1000, cols=len(headers))
        ws.append_row(headers, value_input_option="RAW")
        # Bold and freeze the header row
        ws.format("1:1", {"textFormat": {"bold": True}})
        ws.freeze(rows=1)
        logger.info(f"Created Sheets tab: {tab_name}")
    return ws


def setup_sheet():
    """
    Create and configure the Google Sheet.
    Called once during build setup (see setup scripts).
    Creates both tabs with headers if they don't exist.
    Prints the Sheet URL so the founder can access it.
    """
    client = _get_client()
    sheet = client.open_by_key(GOOGLE_SHEET_ID)
    _get_or_create_tab(sheet, JOBS_TAB, JOBS_HEADERS)
    _get_or_create_tab(sheet, DOWNLOADS_TAB, DOWNLOADS_HEADERS)
    print(f"Sheet ready: {sheet.url}")
    return sheet.url


def log_job(
    order_id: str,
    query: str,
    result_count: int,
    job_status: str,
    time_taken_seconds: float,
    payment_status: str = "paid",
):
    """
    Append one row to Tab 1 (Jobs Log) on job completion.
    Called by worker/main.py after pipeline finishes.
    Swallows all errors — Sheets logging failure must never break job delivery.
    """
    try:
        client = _get_client()
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        ws = _get_or_create_tab(sheet, JOBS_TAB, JOBS_HEADERS)
        row = [
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            order_id,
            query,
            result_count,
            job_status,
            round(time_taken_seconds, 1),
            payment_status,
        ]
        ws.append_row(row, value_input_option="RAW")
        logger.info(f"Sheets Jobs Log: appended row for {order_id}")
    except Exception as e:
        # Critical: never let Sheets failure break the job
        logger.error(f"Sheets log_job failed for {order_id}: {e}")


def log_download(order_id: str, query: str):
    """
    Append one row to Tab 2 (Downloads Log) when customer clicks download.
    Called by api/routes/job.py on /api/download.
    Swallows all errors.
    """
    try:
        client = _get_client()
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        ws = _get_or_create_tab(sheet, DOWNLOADS_TAB, DOWNLOADS_HEADERS)
        row = [
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            order_id,
            query,
            "Yes",
        ]
        ws.append_row(row, value_input_option="RAW")
        logger.info(f"Sheets Downloads Log: appended row for {order_id}")
    except Exception as e:
        logger.error(f"Sheets log_download failed for {order_id}: {e}")
