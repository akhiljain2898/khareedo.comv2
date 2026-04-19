"""
worker/scraper.py

Changes in this version:
- Firecrawl scraping is now ASYNC and PARALLEL (batch endpoint, 5 concurrent max)
- DGFT Track A: serper_dgft_search() fires site:trade.gov.in queries
- seen_urls is protected by asyncio.Lock to prevent race conditions
- Homepage fallback is capped at MAX_FALLBACKS_PER_BATCH (2) per batch
- source field added to every contact dict ("track_b" or "track_a_dgft")

FIX (Apr 2026): Domain-level deduplication added.
- filter_urls() now accepts seen_domains set and deduplicates by domain, not just URL
- scrape_batch_async() now accepts seen_domains set and checks extracted website domain
  before accepting a contact — prevents same company appearing via deep page + homepage
- This eliminates the 3-4x duplicate entries (e.g. Urif India, Dhanraj Sugar) seen in output
"""

import asyncio
import logging
from urllib.parse import urlparse

import httpx

from common.config import SERPER_API_KEY, FIRECRAWL_API_KEY
from worker.extractor import extract_contact

logger = logging.getLogger(__name__)

# ── CONCURRENCY CAP ───────────────────────────────────────────────────────────
# Firecrawl Hobby plan: 5 concurrent browsers.
# We cap at 5 to stay within plan limits across all simultaneous scrapes.
FIRECRAWL_CONCURRENCY = 5

# Max homepage fallbacks attempted per batch.
# Prevents a full batch of failed product pages from exhausting time on fallbacks.
MAX_FALLBACKS_PER_BATCH = 2

# ── DIRECTORY DOMAIN FILTER ───────────────────────────────────────────────────
# URLs from these domains are filtered out before scraping.
# IMPORTANT: trade.gov.in must NEVER be added here — DGFT IEC pages are
# individual verified supplier profiles and are the target of Track A.
DIRECTORY_DOMAINS = {
    "indiamart.com",
    "tradeindia.com",
    "exportersindia.com",
    "justdial.com",
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
    "seair.co.in",
    "dial4trade.com",
    "appointdistributors.com",
    "volza.com",
    "cybex.in",
    "shipmentdata.in",
    "exportgenius.in",
    "industryarc.com",
    "technavio.com",
    "mordorintelligence.com",
    "grandviewresearch.com",
    "marketsandmarkets.com",
}

# Platforms/aggregators that will never have single-supplier contact info
# on their homepage. Skip homepage fallback immediately to save time.
NO_FALLBACK_DOMAINS = {
    "ensun.io",
    "getdistributors.com",
    "dial4trade.com",
    "thomasnet.com",
    "made-in-china.com",
    "12taste.com",
    "acrossbiotech.com",
    "stdmfood.com",
}


# ── URL HELPERS ───────────────────────────────────────────────────────────────

def _is_directory_url(url: str) -> bool:
    url_lower = url.lower()
    for domain in DIRECTORY_DOMAINS:
        if domain in url_lower:
            return True
    return False


def _should_attempt_fallback(url: str) -> bool:
    url_lower = url.lower()
    for domain in NO_FALLBACK_DOMAINS:
        if domain in url_lower:
            return False
    return True


def _get_homepage(url: str) -> str | None:
    """
    Returns the scheme+domain root of a URL only if the URL is a deep page.
    Returns None if the URL is already a homepage or is malformed.
    """
    try:
        parsed = urlparse(url)
        path = parsed.path.rstrip("/")
        if not path:
            return None
        return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return None


def _get_domain(url: str) -> str | None:
    """Extract the netloc (domain) from a URL for deduplication."""
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        # Strip www. prefix for consistent domain matching
        # e.g. www.urifindia.com and urifindia.com are the same company
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc if netloc else None
    except Exception:
        return None


def filter_urls(urls: list[str], seen_urls: set[str], seen_domains: set[str]) -> list[str]:
    """
    Remove duplicates and directory domains from a URL list.

    Two-layer deduplication:
    1. URL-level: exact URL already seen → skip
    2. Domain-level: same company domain already queued/scraped → skip
       Prevents urifindia.com/product-page AND urifindia.com/ both entering the queue.

    Adds surviving URLs to seen_urls and their domains to seen_domains immediately
    to prevent concurrent batches from picking up the same company.
    """
    clean = []
    for url in urls:
        if url in seen_urls:
            continue
        if _is_directory_url(url):
            continue
        domain = _get_domain(url)
        if domain and domain in seen_domains:
            logger.info(f"Domain dedup (pre-scrape): skipping {url} — domain {domain} already queued")
            continue
        clean.append(url)
        seen_urls.add(url)
        if domain:
            seen_domains.add(domain)
    return clean


# ── SERPER SEARCH ─────────────────────────────────────────────────────────────

def serper_search(query: str) -> list[str]:
    """
    Track B: fire a general Google search via Serper.
    Returns up to 10 result URLs. Empty list on any error.
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
        return [
            item.get("link", "")
            for item in data.get("organic", [])
            if item.get("link")
        ]
    except Exception as e:
        logger.warning(f"Serper search failed for '{query}': {e}")
        return []


def serper_dgft_search(product_name: str) -> list[str]:
    """
    Track A: search for DGFT 'Source from India' exporter pages.

    Fires: site:trade.gov.in/pages/source-from-india {product_name}

    Returns a list of IEC page URLs like:
        https://www.trade.gov.in/pages/source-from-india/0288014359

    These are publicly accessible government-verified exporter profiles.
    No login required. Firecrawl can scrape them directly.
    """
    query = f"site:trade.gov.in/pages/source-from-india {product_name}"
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
            # Only keep actual IEC profile pages (have a numeric IEC at the end)
            if "trade.gov.in/pages/source-from-india/" in link:
                # Exclude the directory listing page itself
                path = link.rstrip("/").split("/")[-1]
                if path and path.isdigit():
                    urls.append(link)
        logger.info(f"DGFT Serper returned {len(urls)} IEC pages for '{product_name}'")
        return urls
    except Exception as e:
        logger.warning(f"DGFT Serper search failed for '{product_name}': {e}")
        return []


# ── ASYNC FIRECRAWL ───────────────────────────────────────────────────────────

async def firecrawl_scrape_async(
    url: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> tuple[str, str | None]:
    """
    Async scrape of a single URL via Firecrawl.
    Semaphore limits to FIRECRAWL_CONCURRENCY simultaneous requests.

    Returns (url, markdown_or_None).
    Never raises — logs and returns None on any failure.
    """
    async with semaphore:
        try:
            resp = await client.post(
                "https://api.firecrawl.dev/v1/scrape",
                headers={
                    "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "url": url,
                    "formats": ["markdown"],
                    "onlyMainContent": True,
                    "timeout": 30000,  # 30s in ms — Firecrawl internal timeout
                },
                timeout=45.0,  # httpx timeout — must be > Firecrawl internal
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("success"):
                markdown = data.get("data", {}).get("markdown", "")
                return url, markdown if markdown else None
            logger.info(f"Firecrawl success=false for {url}")
            return url, None
        except Exception as e:
            logger.warning(f"Firecrawl failed for {url}: {e}")
            return url, None


async def scrape_batch_async(
    urls: list[str],
    seen_urls: set[str],
    seen_domains: set[str],
    seen_lock: asyncio.Lock,
    source: str = "track_b",
) -> list[dict]:
    """
    Scrape a batch of URLs in parallel (up to FIRECRAWL_CONCURRENCY at once).

    Domain deduplication on extracted contacts:
    After Haiku extracts a contact, we check the extracted website domain against
    seen_domains. This catches cases where:
    - A deep page (urifindia.com/castor-sugar) was scraped and passed domain check
    - But Haiku extracts website = urifindia.com (root domain)
    - A later batch tries homepage fallback urifindia.com — domain is now in seen_domains
    This is the post-extraction layer of the two-layer dedup system.

    For Track B URLs:
    - Runs homepage fallback if product page fails (capped at MAX_FALLBACKS_PER_BATCH)
    - Fallback uses the same semaphore — counts against concurrency cap

    For Track A (DGFT) URLs:
    - No homepage fallback — IEC pages are already the canonical supplier page
    - Deduplication by domain against seen_domains prevents Track A/B overlap

    Adds source field to every returned contact dict.
    Returns list of valid contact dicts (may be empty).
    """
    if not urls:
        return []

    semaphore = asyncio.Semaphore(FIRECRAWL_CONCURRENCY)
    results = []

    async with httpx.AsyncClient() as client:
        # ── PHASE 1: Scrape all URLs in parallel ────────────────────────────
        tasks = [firecrawl_scrape_async(url, client, semaphore) for url in urls]
        scraped = await asyncio.gather(*tasks)

        # ── PHASE 2: Extract contacts + collect fallback candidates ─────────
        fallback_needed = []  # (original_url, homepage_url)

        for url, markdown in scraped:
            if not markdown:
                # No content — queue for homepage fallback if Track B
                if source == "track_b" and _should_attempt_fallback(url):
                    homepage = _get_homepage(url)
                    if homepage:
                        homepage_domain = _get_domain(homepage)
                        async with seen_lock:
                            if homepage not in seen_urls and (
                                not homepage_domain or homepage_domain not in seen_domains
                            ):
                                seen_urls.add(homepage)
                                if homepage_domain:
                                    seen_domains.add(homepage_domain)
                                fallback_needed.append((url, homepage))
                continue

            contact = extract_contact(markdown, url)
            if contact:
                # Post-extraction domain dedup — check the website Haiku extracted
                extracted_domain = _get_domain(contact.get("website", ""))
                async with seen_lock:
                    if extracted_domain and extracted_domain in seen_domains:
                        logger.info(
                            f"Domain dedup (post-extraction): discarding {contact.get('name')} "
                            f"— domain {extracted_domain} already in results"
                        )
                        continue
                    # Accept this contact and mark domain as seen
                    if extracted_domain:
                        seen_domains.add(extracted_domain)

                contact["source"] = source
                results.append(contact)
                logger.info(f"[{source}] Contact extracted: {contact.get('name')} from {url}")
            else:
                # Content present but extraction failed — try homepage fallback for Track B
                if source == "track_b" and _should_attempt_fallback(url):
                    homepage = _get_homepage(url)
                    if homepage:
                        homepage_domain = _get_domain(homepage)
                        async with seen_lock:
                            if homepage not in seen_urls and (
                                not homepage_domain or homepage_domain not in seen_domains
                            ):
                                seen_urls.add(homepage)
                                if homepage_domain:
                                    seen_domains.add(homepage_domain)
                                fallback_needed.append((url, homepage))

        # ── PHASE 3: Homepage fallbacks (Track B only, capped) ──────────────
        if fallback_needed and source == "track_b":
            # Cap fallbacks per batch to protect time budget
            capped = fallback_needed[:MAX_FALLBACKS_PER_BATCH]
            skipped = len(fallback_needed) - len(capped)
            if skipped > 0:
                logger.info(f"Homepage fallback cap: skipping {skipped} fallbacks")

            fallback_urls = [hp for _, hp in capped]
            logger.info(f"Attempting {len(fallback_urls)} homepage fallbacks")

            fallback_tasks = [
                firecrawl_scrape_async(hp, client, semaphore)
                for hp in fallback_urls
            ]
            fallback_scraped = await asyncio.gather(*fallback_tasks)

            for homepage_url, markdown in fallback_scraped:
                if not markdown:
                    continue
                contact = extract_contact(markdown, homepage_url)
                if contact:
                    extracted_domain = _get_domain(contact.get("website", ""))
                    async with seen_lock:
                        if extracted_domain and extracted_domain in seen_domains:
                            logger.info(
                                f"Domain dedup (fallback post-extraction): discarding "
                                f"{contact.get('name')} — domain {extracted_domain} already in results"
                            )
                            continue
                        if extracted_domain:
                            seen_domains.add(extracted_domain)

                    contact["source"] = source
                    results.append(contact)
                    logger.info(
                        f"[{source}] Contact from homepage fallback: "
                        f"{contact.get('name')} from {homepage_url}"
                    )

    return results


# ── DGFT DEDUPLICATION ────────────────────────────────────────────────────────

def filter_dgft_urls(
    dgft_urls: list[str],
    seen_urls: set[str],
    seen_domains: set[str],
    track_b_results: list[dict],
) -> list[str]:
    """
    Filter DGFT IEC page URLs before scraping to prevent duplication.

    Pre-scrape check (done here):
    - URL already in seen_urls → skip (same IEC page already queued or scraped)

    Note: DGFT IEC page URLs are trade.gov.in paths — their domain is always
    trade.gov.in and is not added to seen_domains (that would block all DGFT pages).
    Domain dedup on the *extracted supplier website* is done post-scrape in pipeline.py.

    Post-scrape domain check (done in pipeline.py after scraping):
    - We cannot know a supplier's website domain until after the IEC page is scraped.
    - pipeline.py compares extracted website domains against seen_domains (which includes
      all Track B domains) before extending the final results list.

    Returns clean list of DGFT IEC page URLs to scrape.
    """
    clean = []
    for url in dgft_urls:
        if url in seen_urls:
            logger.info(f"DGFT URL already seen — skipping: {url}")
            continue
        clean.append(url)
        seen_urls.add(url)

    logger.info(f"DGFT: {len(dgft_urls)} IEC pages → {len(clean)} after pre-scrape dedup")
    return clean
