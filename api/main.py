"""
api/main.py
FastAPI application entry point.
Mounts all API routes and serves the static HTML pages.

Run with: uvicorn api.main:app --host 0.0.0.0 --port $PORT
"""

import logging
import sys
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from api.routes.payment import router as payment_router
from api.routes.webhook import router as webhook_router
from api.routes.job import router as job_router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="khareedo API",
    docs_url=None,   # Disable Swagger UI in production
    redoc_url=None,
)

# ── API ROUTES ────────────────────────────────────────────────────────────────
app.include_router(payment_router)
app.include_router(webhook_router)
app.include_router(job_router)

# ── STATIC FILES ──────────────────────────────────────────────────────────────
# Serves index.html, result.html, and policy pages from api/static/
app.mount("/static", StaticFiles(directory="api/static"), name="static")


# ── PAGE ROUTES ───────────────────────────────────────────────────────────────

@app.get("/")
async def index():
    return FileResponse("api/static/index.html")


@app.get("/result")
async def result():
    return FileResponse("api/static/result.html")


@app.get("/refund-policy")
async def refund_policy():
    return FileResponse("api/static/refund-policy.html")


@app.get("/terms")
async def terms():
    return FileResponse("api/static/terms.html")


@app.get("/privacy-policy")
async def privacy_policy():
    return FileResponse("api/static/privacy-policy.html")


# ── HEALTH CHECK ──────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Railway uses this to confirm the service is alive."""
    return {"status": "ok", "service": "khareedo-api"}


# ── STARTUP EVENT ─────────────────────────────────────────────────────────────

@app.on_event("startup")
async def on_startup():
    """
    Ensure DB tables exist on startup.
    Safe to run multiple times — uses CREATE TABLE IF NOT EXISTS.
    """
    from common.db import create_tables
    try:
        create_tables()
        logger.info("Database tables verified/created")
    except Exception as e:
        logger.error(f"Startup DB check failed: {e}")
