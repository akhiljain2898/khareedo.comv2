"""
worker/pipeline.py

Bug-fix release — changes from previous version:

1. filter_dgft_urls() call updated: track_b_results argument removed
   (dead parameter eliminated in scraper.py).

2. DGFT scrape_batch_async() wrapped in asyncio.wait_for() with a time-budget
   derived from remaining pipeline time. Prevents DGFT from overrunning
   SCRAPE_TIMEOUT_SECONDS when Track B runs close to the limit — previously
   the 180s hard timeout was only checked at the top of the Track B loop,
   meaning a DGFT scrape could push total runtime to 220+ seconds and risk
   the worker process being killed by Railway mid-job.

3. All other logic unchanged: async Track B keyword loop, time budget
   management, result counting, logging.
"""

import asyncio
import time
import logging

from common.config import (
    SCRAPE_TIMEOUT_SECONDS,
    TARGET_RESULT_COUNT,
)
from worker.keywords import get_keywords
from worker.scraper import (
    serper_search,
    serper_dgft_search,
    filter_urls,
    filter_dgft_urls,
    scrape_batch_async,
)

logger = logging.getLogger(__name__)

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

DGFT_TIME_RESERVE_SECONDS = 15


def build_b2b_keywords(product_name: str) -> list[str]:
    """
    Combine product name with B2B modifiers, then append keywords.py suffixes.
    Deduplicates while preserving priority order.
    """
    b2b_queries = [f"{product_name} {modifier}" for modifier in B2B_MODIFIERS]
    original_keywords = get_keywords(product_name)

    seen = set()
    combined = []
    for kw in b2b_queries + original_keywords:
        if kw not in seen:
            seen.add(kw)
            combined.append(kw)
    return combined


async def run_pipeline(product_name: str) -> tuple[list[dict], int]:
    """
    Main async pipeline entry point.

    Flow:
    1. Track B — B2B keyword rounds, each scraping URLs in parallel.
       seen_domains accumulates only domains with accepted contacts.
    2. Track A — DGFT search + scrape, wrapped in asyncio.wait_for() so it
       cannot overrun the remaining time budget.

    Returns: (results, keywords_used)
    """
    results: list[dict] = []
    seen_urls: set[str] = set()
    seen_domains: set[str] = set()  # Only domains with accepted contacts
    seen_lock = asyncio.Lock()
    start_time = time.time()
    keywords_used = 0

    keywords = build_b2b_keywords(product_name)
    logger.info(
        f"Pipeline start — '{product_name}', "
        f"{len(keywords)} Track B keywords queued"
    )

    # ── TRACK B: PARALLEL KEYWORD ROUNDS ─────────────────────────────────────
    for keyword in keywords:

        elapsed = time.time() - start_time
        remaining = SCRAPE_TIMEOUT_SECONDS - elapsed

        if remaining <= DGFT_TIME_RESERVE_SECONDS:
            logger.info(
                f"Stopping Track B at {elapsed:.1f}s — "
                f"reserving {DGFT_TIME_RESERVE_SECONDS}s for DGFT"
            )
            break

        if len(results) >= TARGET_RESULT_COUNT:
            logger.info(f"Target hit ({len(results)} results) — skipping remaining Track B keywords")
            break

        keywords_used += 1
        logger.info(f"Track B keyword {keywords_used}: '{keyword}'")

        raw_urls = serper_search(keyword)
        logger.info(f"Serper returned {len(raw_urls)} URLs")

        clean_urls = filter_urls(raw_urls, seen_urls, seen_domains)
        logger.info(f"After filter: {len(clean_urls)} clean URLs to scrape")

        if not clean_urls:
            continue

        batch_results = await scrape_batch_async(
            clean_urls,
            seen_urls,
            seen_domains,
            seen_lock,
            source="track_b",
        )

        results.extend(batch_results)
        logger.info(
            f"Track B keyword {keywords_used} done — "
            f"+{len(batch_results)} results, total {len(results)}/{TARGET_RESULT_COUNT}"
        )

    # ── TRACK A: DGFT 'SOURCE FROM INDIA' ────────────────────────────────────
    elapsed = time.time() - start_time
    remaining = SCRAPE_TIMEOUT_SECONDS - elapsed

    if len(results) >= TARGET_RESULT_COUNT:
        logger.info(f"Target already hit — skipping DGFT Track A")
    elif remaining < 10:
        logger.warning(
            f"Only {remaining:.1f}s remaining — skipping DGFT Track A (insufficient budget)"
        )
    else:
        logger.info(f"Starting DGFT Track A — {remaining:.1f}s remaining")

        dgft_urls = serper_dgft_search(product_name)

        if dgft_urls:
            # filter_dgft_urls: URL-level dedup only.
            # Domain dedup against Track B is handled by scrape_batch_async
            # via the shared seen_domains set — no separate pass needed.
            clean_dgft_urls = filter_dgft_urls(
                dgft_urls,
                seen_urls,
                seen_domains,
                # track_b_results removed — was dead parameter
            )

            if clean_dgft_urls:
                logger.info(f"DGFT Track A: scraping {len(clean_dgft_urls)} IEC pages")

                # Compute a hard time budget for DGFT scraping.
                # This prevents DGFT from overrunning SCRAPE_TIMEOUT_SECONDS:
                # Track B only checks the timeout at the top of each keyword loop,
                # so without this wrapper the DGFT scrape could push total runtime
                # to 220+ seconds and risk the worker being killed mid-job.
                elapsed_now = time.time() - start_time
                dgft_time_budget = max(
                    10.0,  # always give at least 10s even if we're close to limit
                    SCRAPE_TIMEOUT_SECONDS - elapsed_now - 2,  # 2s safety margin
                )
                logger.info(f"DGFT time budget: {dgft_time_budget:.1f}s")

                try:
                    dgft_results = await asyncio.wait_for(
                        scrape_batch_async(
                            clean_dgft_urls,
                            seen_urls,
                            seen_domains,
                            seen_lock,
                            source="track_a_dgft",
                        ),
                        timeout=dgft_time_budget,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        f"DGFT Track A timed out after {dgft_time_budget:.1f}s "
                        f"— using partial results already in 'results' list"
                    )
                    dgft_results = []

                slots_remaining = TARGET_RESULT_COUNT - len(results)
                dgft_to_add = dgft_results[:slots_remaining]
                results.extend(dgft_to_add)

                logger.info(
                    f"DGFT Track A done — "
                    f"+{len(dgft_to_add)} results, "
                    f"total {len(results)}/{TARGET_RESULT_COUNT}"
                )
            else:
                logger.info("DGFT Track A: all IEC pages already seen")
        else:
            logger.info("DGFT Track A: Serper returned no IEC pages for this product")

    # ── DONE ──────────────────────────────────────────────────────────────────
    elapsed_total = time.time() - start_time
    track_b_count = sum(1 for r in results if r.get("source") == "track_b")
    dgft_count = sum(1 for r in results if r.get("source") == "track_a_dgft")

    logger.info(
        f"Pipeline complete — {len(results)} total results "
        f"(Track B: {track_b_count}, DGFT: {dgft_count}), "
        f"{keywords_used} keywords, {elapsed_total:.1f}s"
    )

    return results, keywords_used
