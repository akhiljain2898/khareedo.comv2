"""
worker/pipeline.py
The adaptive scrape loop — core of the product.

Logic:
- Fire keywords in priority order
- For each keyword: Serper search → filter URLs → scrape sequentially
- Exit when TARGET_RESULT_COUNT valid contacts found OR SCRAPE_TIMEOUT_SECONDS elapsed
- Max MAX_KEYWORDS keyword rounds regardless of time remaining
- Returns list of valid contact dicts (may be empty)
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

    keywords = get_keywords(product_name)
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

        # Filter duplicates and directories
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
