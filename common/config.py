import os
from dotenv import load_dotenv

load_dotenv()

# ── Razorpay ──────────────────────────────────────────────────────────────────
RAZORPAY_KEY_ID         = os.environ.get("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET     = os.environ.get("RAZORPAY_KEY_SECRET", "")
RAZORPAY_WEBHOOK_SECRET = os.environ.get("RAZORPAY_WEBHOOK_SECRET", "")
# ── Redis ─────────────────────────────────────────────────────────────────────
UPSTASH_REDIS_URL   = os.environ["UPSTASH_REDIS_URL"]

# ── Postgres ──────────────────────────────────────────────────────────────────
DATABASE_URL = os.environ["DATABASE_URL"]

# ── R2 ────────────────────────────────────────────────────────────────────────
R2_ACCOUNT_ID      = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY_ID   = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
R2_BUCKET_NAME     = os.environ["R2_BUCKET_NAME"]
R2_ENDPOINT_URL    = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# ── APIs ──────────────────────────────────────────────────────────────────────
SERPER_API_KEY    = os.environ["SERPER_API_KEY"]
FIRECRAWL_API_KEY = os.environ["FIRECRAWL_API_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

# ── Google Sheets ─────────────────────────────────────────────────────────────
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
GOOGLE_SHEET_ID             = os.environ["GOOGLE_SHEET_ID"]

# ── App ───────────────────────────────────────────────────────────────────────
APP_BASE_URL = os.environ.get("APP_BASE_URL", "http://localhost:8000")

# ── Pipeline constants (hardcoded as per architecture decision) ───────────────
SCRAPE_TIMEOUT_SECONDS = 120
TARGET_RESULT_COUNT    = 20
MAX_KEYWORDS           = 8

# ── Redis keys ────────────────────────────────────────────────────────────────
REDIS_QUEUE_KEY = "khareedo:jobs"
