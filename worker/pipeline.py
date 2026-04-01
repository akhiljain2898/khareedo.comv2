"""
worker/pipeline.py
The adaptive scrape loop — core of the product.

Logic:
- Fire keywords in priority order
- For each keyword: Serper search → filter URLs → scrape sequentially
- Exit when TARGET_RESULT_COUNT valid contacts found OR SCRAPE_TIMEOUT_SECONDS elapsed
- Max MAX_KEYWORDS keyword rounds regardless of time remaining
- Returns list of valid contact dicts (may be empty)

Keyword strategy:
- All Serper queries use B2B-specific modifiers (bulk, wholesale, manufacturer, B2B)
- This prevents consumer/retail pages from dominating results for ambiguous product names
- Modifiers are appended by get_keywords() in worker/keywords.py
"""

import time
import logging
from common.config import (
    SCRAPE_TIMEOUT_SECONDS,
    TARGET_RESULT_COUNT,
)
from worker.keywords import get_keywords
from worker.scraper import serper_search, filter_urls, scrape_and_extract

logger = logging.getLogger(__name__)


# B2B search modifiers appended to every Serper query.
# Forces Google to surface manufacturer/supplier pages instead of
# consumer retail, Amazon listings, or gym supplement shops.
B2B_MODIFIERS = [
    "bulk supplier India",
    "manufacturer India wholesale",
    "raw material supplier India B2B",
    "wholesale distributor India contact",
    "industrial supplier India",
    "B2B supplier India manufacturer",
    "supplier India factory contact",
    "exporter manufacturer India",
]


def build_b2b_keywords(product_name: str) -> list[str]:
    """
    Combine the product name with each B2B modifier to produce
    search queries that surface manufacturer/supplier pages.

    Example for 'whey protein':
      - 'whey protein bulk supplier India'
      - 'whey protein manufacturer India wholesale'
      - 'whey protein raw material supplier India B2B'
      ... etc up to MAX_KEYWORDS

    Falls back to get_keywords() from keywords.py as a supplementary
    source if B2B_MODIFIERS are exhausted before TARGET_RESULT_COUNT is hit.
    """
    b2b_queries = [f"{product_name} {modifier}" for modifier in B2B_MODIFIERS]

    # Append the original keyword list as a fallback after B2B modifiers
    # This ensures backward compatibility if keywords.py has product-specific logic
    original_keywords = get_keywords(product_name)

    # Deduplicate while preserving order (B2B modifiers take priority)
    seen = set()
    combined = []
    for kw in b2b_queries + original_keywords:
        if kw not in seen:
            seen.add(kw)
            combined.append(kw)

    return combined


def run_pipeline(product_name: str) -> tuple[list[dict], int]:
    """
    Main pipeline entry point.

    Args:
        product_name: validated product query string from the customer

    Returns:
        (results, keywords_used)
        results: list of valid contact dicts (0 to TARGET_RESULT_COUNT)
        keywords_used: how many keyword rounds were fired (for Sheets log)
    """
    results: list[dict] = []
    seen_urls: set[str] = set()
    start_time = time.time()
    keywords_used = 0

    # Use B2B-weighted keyword list instead of generic keywords
    keywords = build_b2b_keywords(product_name)
    logger.info(f"Starting pipeline for '{product_name}' — {len(keywords)} keywords queued")

    for keyword in keywords:

        # Check exit conditions at the top of each keyword round
        elapsed = time.time() - start_time
        if elapsed >= SCRAPE_TIMEOUT_SECONDS:
            logger.info(f"Timeout reached after {elapsed:.1f}s — exiting loop")
            break

        if len(results) >= TARGET_RESULT_COUNT:
            logger.info(f"Target hit ({len(results)} results) — exiting loop")
            break

        keywords_used += 1
        logger.info(f"Keyword {keywords_used}: '{keyword}'")

        # Serper search
        raw_urls = serper_search(keyword)
        logger.info(f"Serper returned {len(raw_urls)} URLs")

        # Filter duplicates and known directory domains
        clean_urls = filter_urls(raw_urls, seen_urls)
        logger.info(f"After filter: {len(clean_urls)} clean URLs")

        # Scrape each URL sequentially
        for url in clean_urls:

            # Check exit conditions inside the URL loop too
            elapsed = time.time() - start_time
            if elapsed >= SCRAPE_TIMEOUT_SECONDS:
                logger.info(f"Timeout mid-batch at {elapsed:.1f}s")
                break

            if len(results) >= TARGET_RESULT_COUNT:
                logger.info(f"Target hit mid-batch — stopping")
                break

            contact = scrape_and_extract(url)
            if contact:
                results.append(contact)
                logger.info(f"Results so far: {len(results)}/{TARGET_RESULT_COUNT}")

    elapsed_total = time.time() - start_time
    logger.info(
        f"Pipeline complete — {len(results)} results, "
        f"{keywords_used} keywords, {elapsed_total:.1f}s elapsed"
    )

    return results, keywords_used
