"""
worker/scraper.py

Bug-fix release — changes from previous version:

1. CRITICAL: filter_urls() no longer pre-registers domains in seen_domains.
   Domains are added to seen_domains ONLY when a contact is accepted in
   scrape_batch_async(). Pre-registration was the root cause of zero-result runs.

2. _is_directory_url() now uses proper netloc parsing instead of substring
   matching. Previous version blocked legitimate URLs whose raw string happened
   to contain a blocked domain name (e.g. "not-amazon.in" blocked by "amazon.in"
   substring match). Now checks netloc == domain OR netloc.endswith("." + domain).

3. _is_low_value_page() replaces _is_contact_or_about_page().
   Two-layer path filter:
     Layer A — exact match on last path segment (contact, about, careers, etc.)
     Layer B — exact match on any path component (blog, news, commodity, etc.)
               PLUS startswith prefix for 'gst' and 'hsn' ONLY.
   Previous version applied startswith to ALL Layer B terms, causing false
   positives: "commodity-chemicals" blocked by "commodity-" prefix,
   "articles-of-association" blocked by "article-" prefix, etc.
   "resources" and "insights" excluded from Layer B — too broad, would block
   legitimate supplier datasheets and product insight pages.

4. _get_domain() handles None input explicitly — returns None safely.
   Previous version passed None to urlparse() which silently returned empty
   netloc; now guarded at entry with explicit None/empty check.

5. scrape_batch_async(): domain-register + list-append moved inside seen_lock.
   Makes accept-and-register atomic; safe in asyncio today, safe if threading
   is ever introduced.

6. filter_dgft_urls(): dead track_b_results parameter removed.
   Was used by old pipeline.py manual DGFT dedup loop (removed last release).
   Call site in pipeline.py updated to match.

7. Log message: "already has an accepted result" replaces "already in results".
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
FIRECRAWL_CONCURRENCY = 5

# Max homepage fallbacks attempted per batch.
MAX_FALLBACKS_PER_BATCH = 2

# ── DIRECTORY DOMAIN FILTER ───────────────────────────────────────────────────
# Checked via netloc match (exact or subdomain) — NOT raw string substring.
# IMPORTANT: trade.gov.in must NEVER be added here.
# kompass.com / in.kompass.com intentionally NOT blocked — yield real contacts.
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
    # Confirmed from production logs to never yield supplier contacts:
    "chennaiyellowpagesonline.com",
    "officedial.com",
    "commodityonline.com",
    "cbic-gst.gov.in",
    "go4worldbusiness.com",
    "imarcgroup.com",
    "tradingeconomics.com",
    "in.investing.com",
    "mcxindia.com",
    "agriwatch.com",
    "cleartax.in",
    "hubco.in",
    "scribd.com",
    "investmentguruindia.com",
    "patronaccounting.com",
    "tradewheel.com",
    "misefa.com",
    "hyperpure.com",
    "getdistributors.com",
}

# Platforms that will never have single-supplier contact info on their homepage.
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

# ── PATH-LEVEL FILTERS ────────────────────────────────────────────────────────
# A URL is skipped if it matches Layer A OR Layer B.
#
# Layer A — _SKIP_PATH_SEGMENTS:
#   Exact match on the LAST path segment (file extension stripped first).
#   e.g. /about-us.html → "about-us" → blocked.
#
# Layer B — _SKIP_PATH_COMPONENTS_EXACT:
#   Exact match on ANY path component (directory).
#   e.g. /blog/top-10-suppliers → "blog" component → blocked.
#   "resources" and "insights" are deliberately excluded — they appear in
#   legitimate supplier product/datasheet paths and cause false positives.
#
# Layer B prefix — _SKIP_PATH_COMPONENTS_PREFIX:
#   Startswith match on any component — ONLY "gst" and "hsn".
#   Catches compound slugs: "gst-goods-services-rates", "hsn-code-1511".
#   Do NOT add other terms here — causes false positives on supplier URLs
#   (e.g. "commodity-" would block "commodity-chemicals" supplier pages).

_SKIP_PATH_SEGMENTS = {
    # Contact / reach
    "contact", "contact-us", "contactus", "about-us", "about",
    "reach-us", "reach_us", "get-in-touch",
    # Company boilerplate
    "careers", "career", "jobs",
    "team", "our-team", "leadership", "management",
    "gallery", "testimonials",
    "investors", "investor-relations",
    "csr", "sustainability",
    "sitemap", "faq",
    # Legal
    "privacy", "privacy-policy",
    "terms", "terms-of-service", "terms-and-conditions",
    "disclaimer",
    # Auth
    "login", "signin", "signup", "register",
}

_SKIP_PATH_COMPONENTS_EXACT = {
    "blog", "news", "newsdetail", "articleshow", "article", "articles",
    "press", "press-release", "media",
    "case-study", "case-studies",
    "commodity", "commodities", "mandiprices", "agro-commodities",
    "buyers", "buyer",
}

_SKIP_PATH_COMPONENTS_PREFIX = {"gst", "hsn"}  # startswith — only these two


# ── URL HELPERS ───────────────────────────────────────────────────────────────

def _get_domain(url: str | None) -> str | None:
    """
    Extract netloc from a URL, stripping www. prefix.
    Returns None for None input, empty string, or any malformed URL.
    Safe to call with contact.get("website") which may return None.
    """
    if not url:
        return None
    try:
        parsed = urlparse(str(url))
        netloc = parsed.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc if netloc else None
    except Exception:
        return None


def _is_directory_url(url: str) -> bool:
    """
    Returns True if the URL's netloc matches a blocked directory/aggregator domain.

    Uses exact netloc match or subdomain match — NOT substring of the raw URL.
    Prevents false positives like "not-amazon.in" being blocked by "amazon.in".

    Examples:
      https://www.amazon.in/castor  → True  (netloc == amazon.in)
      https://dir.indiamart.com/a   → True  (netloc endswith .indiamart.com)
      https://not-amazon.in/castor  → False (netloc == not-amazon.in, no match)
      https://mca.gov.in.fake.com/  → False (netloc == mca.gov.in.fake.com)
    """
    try:
        netloc = _get_domain(url)
        if not netloc:
            return False
        for domain in DIRECTORY_DOMAINS:
            if netloc == domain or netloc.endswith("." + domain):
                return True
        return False
    except Exception:
        return False


def _is_low_value_page(url: str) -> bool:
    """
    Returns True if the URL path indicates a page that will never yield
    structured supplier contact data.

    See module-level comments for full design rationale.

    Correctly allows:
      /commodity-chemicals/castor  (commodity-chemicals ≠ exact "commodity")
      /resources/castor-datasheet  (resources excluded from Layer B)
      /articles-of-association     (articles-of-association ≠ exact "articles")
    """
    try:
        parsed = urlparse(url)
        path = parsed.path.lower().rstrip("/")
        components = [c for c in path.split("/") if c]

        # Layer A: last segment
        if components:
            last = components[-1]
            last_no_ext = last.rsplit(".", 1)[0] if "." in last else last
            if last_no_ext in _SKIP_PATH_SEGMENTS:
                return True

        # Layer B: any component
        for component in components:
            if component in _SKIP_PATH_COMPONENTS_EXACT:
                return True
            for prefix in _SKIP_PATH_COMPONENTS_PREFIX:
                if component == prefix or \
                   component.startswith(prefix + "-") or \
                   component.startswith(prefix + "_"):
                    return True

        return False
    except Exception:
        return False


def _should_attempt_fallback(url: str) -> bool:
    """Returns False for platforms that never have single-supplier homepages."""
    url_lower = url.lower()
    for domain in NO_FALLBACK_DOMAINS:
        if domain in url_lower:
            return False
    return True


def _get_homepage(url: str) -> str | None:
    """
    Returns scheme+netloc root of a URL only if it is a deep page.
    Returns None if already a homepage or malformed.
    """
    try:
        parsed = urlparse(url)
        path = parsed.path.rstrip("/")
        if not path:
            return None
        return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return None


def filter_urls(urls: list[str], seen_urls: set[str], seen_domains: set[str]) -> list[str]:
    """
    Filter a URL list before scraping. Removes:
      - Already-queued URLs (seen_urls — URL-level dedup)
      - Directory/aggregator domains
      - Low-value pages (contact, blog, GST, commodity price, etc.)
      - URLs whose domain already has an accepted contact (seen_domains)

    Domain registration contract:
    This function does NOT write to seen_domains. Domains are added to
    seen_domains only in scrape_batch_async() at the moment a contact is
    accepted. Writing here was the root cause of zero-result runs.

    seen_urls IS written here — URL-level dedup only, prevents same URL
    being queued twice across keyword rounds.
    """
    clean = []
    for url in urls:
        if url in seen_urls:
            continue
        if _is_directory_url(url):
            continue
        if _is_low_value_page(url):
            logger.info(f"Path filter: skipping low-value page {url}")
            continue
        domain = _get_domain(url)
        if domain and domain in seen_domains:
            logger.info(
                f"Domain dedup (pre-scrape): skipping {url} "
                f"— domain {domain} already has an accepted result"
            )
            continue
        clean.append(url)
        seen_urls.add(url)
        # DO NOT add to seen_domains here. See contract above.
    return clean


# ── SERPER SEARCH ─────────────────────────────────────────────────────────────

def serper_search(query: str) -> list[str]:
    """Track B: Google search via Serper. Returns up to 10 URLs. Empty on error."""
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
    Track A: Search for DGFT 'Source from India' exporter IEC pages.
    Returns numeric IEC profile URLs from trade.gov.in only.
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
            if "trade.gov.in/pages/source-from-india/" in link:
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
    Returns (url, markdown_or_None). Never raises.
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
                    "timeout": 30000,
                },
                timeout=45.0,
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

    Domain registration contract:
    seen_domains is written ONLY here, inside seen_lock, at the moment a contact
    is accepted. The domain-register + list-append are both inside the lock so
    the operation is atomic — no window where a concurrent coroutine can accept
    a duplicate from the same domain.

    Fallback (Track B only, capped at MAX_FALLBACKS_PER_BATCH):
    If a page returns no content or fails extraction, the homepage is attempted.
    Homepage is only queued if its domain is not already in seen_domains.
    """
    if not urls:
        return []

    semaphore = asyncio.Semaphore(FIRECRAWL_CONCURRENCY)
    results = []

    async with httpx.AsyncClient() as client:
        # ── PHASE 1: Scrape all URLs in parallel ────────────────────────────
        tasks = [firecrawl_scrape_async(url, client, semaphore) for url in urls]
        scraped = await asyncio.gather(*tasks)

        # ── PHASE 2: Extract + collect fallback candidates ──────────────────
        fallback_needed = []

        for url, markdown in scraped:
            if not markdown:
                if source == "track_b" and _should_attempt_fallback(url):
                    homepage = _get_homepage(url)
                    if homepage:
                        homepage_domain = _get_domain(homepage)
                        async with seen_lock:
                            if homepage not in seen_urls and (
                                not homepage_domain or homepage_domain not in seen_domains
                            ):
                                seen_urls.add(homepage)
                                fallback_needed.append((url, homepage))
                continue

            contact = extract_contact(markdown, url)
            if contact:
                # _get_domain safely handles None website (returns None)
                extracted_domain = _get_domain(contact.get("website"))
                async with seen_lock:
                    if extracted_domain and extracted_domain in seen_domains:
                        logger.info(
                            f"Domain dedup (post-extraction): discarding {contact.get('name')} "
                            f"— domain {extracted_domain} already has an accepted result"
                        )
                        continue
                    # Accept: register domain + append — both inside lock (atomic)
                    if extracted_domain:
                        seen_domains.add(extracted_domain)
                    contact["source"] = source
                    results.append(contact)

                logger.info(f"[{source}] Contact extracted: {contact.get('name')} from {url}")
            else:
                if source == "track_b" and _should_attempt_fallback(url):
                    homepage = _get_homepage(url)
                    if homepage:
                        homepage_domain = _get_domain(homepage)
                        async with seen_lock:
                            if homepage not in seen_urls and (
                                not homepage_domain or homepage_domain not in seen_domains
                            ):
                                seen_urls.add(homepage)
                                fallback_needed.append((url, homepage))

        # ── PHASE 3: Homepage fallbacks (Track B only, capped) ──────────────
        if fallback_needed and source == "track_b":
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
                    extracted_domain = _get_domain(contact.get("website"))
                    async with seen_lock:
                        if extracted_domain and extracted_domain in seen_domains:
                            logger.info(
                                f"Domain dedup (fallback post-extraction): discarding "
                                f"{contact.get('name')} — domain {extracted_domain} "
                                f"already has an accepted result"
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
) -> list[str]:
    """
    Filter DGFT IEC page URLs before scraping — URL-level dedup only.

    Domain dedup against Track B results is handled automatically:
    scrape_batch_async() checks seen_domains (which contains all accepted
    Track B contact domains) before accepting any DGFT contact.

    Does NOT add trade.gov.in to seen_domains — that would block all DGFT pages.
    The track_b_results parameter from the previous version is removed —
    it was dead code after the manual dedup loop in pipeline.py was removed.
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
