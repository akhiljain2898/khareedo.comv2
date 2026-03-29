"""
worker/scraper.py
Sequential scrape-and-extract cycle.
One URL at a time: Firecrawl → Haiku → is_valid → return contact or None.

Also handles:
- Serper search (returns URL list for a query)
- URL deduplication
- Directory domain filtering
"""

import logging
import httpx
from common.config import SERPER_API_KEY, FIRECRAWL_API_KEY
from worker.extractor import extract_contact

logger = logging.getLogger(__name__)

# ── DIRECTORY DOMAIN FILTER ──────────────────────────────────────────────────
# URLs from these domains are filtered out before scraping.
# They are directories, not vendor websites — scraping them violates their ToS
# and is blocked by their anti-bot systems anyway.
DIRECTORY_DOMAINS = {
    "indiamart.com",
    "tradeindia.com",
    "exportersindia.com",
    "justdial.com",
    "kompass.com",
    "industrybuying.com",
    "alibaba.com",
    "aliexpress.com",
    "amazon.in",
    "amazon.com",
    "flipkart.com",
    "snapdeal.com",
    "udaan.com",
    "moglix.com",
    "tolexo.com",
    "bijnis.com",
    "pepagora.com",
    "globalpiyasa.com",
    "dir.indiamart.com",
    "wikipedia.org",
    "linkedin.com",
    "facebook.com",
    "twitter.com",
    "instagram.com",
    "youtube.com",
    "quora.com",
    "reddit.com",
    "zaubacorp.com",
    "tofler.in",
    "mca.gov.in",
    "zauba.com",
    "yellowpages.com",
    "sulekha.com",
    "2gle.in",
}


def _is_directory_url(url: str) -> bool:
    """Return True if the URL belongs to a known directory/aggregator domain."""
    url_lower = url.lower()
    for domain in DIRECTORY_DOMAINS:
        if domain in url_lower:
            return True
    return False


# ── SERPER SEARCH ────────────────────────────────────────────────────────────

def serper_search(query: str) -> list[str]:
    """
    Fire a Google search via Serper API.
    Returns a list of up to 10 result URLs.
    Returns empty list on any error — loop continues with next keyword.
    """
    try:
        resp = httpx.post(
            "https://google.serper.dev/search",
            headers={
                "X-API-KEY": SERPER_API_KEY,
                "Content-Type": "application/json",
            },
            json={"q": query, "num": 10, "gl": "in", "hl": "en"},
            timeout=15.0,
        )
        resp.raise_for_status()
        data = resp.json()
        urls = []
        for item in data.get("organic", []):
            link = item.get("link", "")
            if link:
                urls.append(link)
        return urls
    except Exception as e:
        logger.warning(f"Serper search failed for query '{query}': {e}")
        return []


# ── FIRECRAWL SCRAPE ─────────────────────────────────────────────────────────

def firecrawl_scrape(url: str) -> str | None:
    """
    Scrape a single URL via Firecrawl.
    Returns the page markdown content, or None on failure.
    Firecrawl handles anti-bot, JavaScript rendering, etc.
    """
    try:
        resp = httpx.post(
            "https://api.firecrawl.dev/v1/scrape",
            headers={
                "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "url": url,
                "formats": ["markdown"],
                "onlyMainContent": True,
                "timeout": 20000,  # 20s in ms
            },
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("success"):
            return data.get("data", {}).get("markdown", "")
        logger.info(f"Firecrawl returned success=false for {url}")
        return None
    except Exception as e:
        logger.warning(f"Firecrawl failed for {url}: {e}")
        return None


# ── URL FILTERING ─────────────────────────────────────────────────────────────

def filter_urls(urls: list[str], seen: set[str]) -> list[str]:
    """
    Given a list of URLs from Serper:
    1. Remove duplicates (already seen in this session)
    2. Remove known directory domains
    Returns clean list of new vendor URLs to scrape.
    """
    clean = []
    for url in urls:
        if url in seen:
            continue
        if _is_directory_url(url):
            continue
        clean.append(url)
        seen.add(url)
    return clean


# ── SINGLE URL: SCRAPE + EXTRACT ─────────────────────────────────────────────

def scrape_and_extract(url: str) -> dict | None:
    """
    Scrape one URL with Firecrawl, then extract contact with Claude Haiku.
    Returns a valid contact dict or None.
    Logs and swallows all errors — the loop continues regardless.
    """
    logger.info(f"Scraping: {url}")
    markdown = firecrawl_scrape(url)
    if not markdown:
        return None

    contact = extract_contact(markdown, url)
    if contact:
        logger.info(f"Valid contact extracted from {url}: {contact.get('name')}")
    return contact
