"""
worker/pipeline.py

Changes in this version:
- run_pipeline() is now async — call with asyncio.run(run_pipeline(...))
- Track B: parallel batch scraping per keyword round (5 concurrent via Firecrawl Hobby)
- Track A: DGFT 'Source from India' — fires after Track B keywords complete
- seen_urls protected by asyncio.Lock throughout
- DGFT results deduplicated against Track B results before scraping
- source field ("track_b" / "track_a_dgft") on every contact dict

Keyword strategy unchanged:
- B2B modifiers fire first (highest intent)
- keywords.py suffixes fire as fallback if target not hit
- DGFT fires once at end as a separate track, not per-keyword
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
    _get_domain,
)

logger = logging.getLogger(__name__)

# B2B modifiers — unchanged from previous version
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

# Time budget reserved for DGFT Track A at the end of the pipeline.
# If less than this many seconds remain, DGFT is skipped.
DGFT_TIME_RESERVE_SECONDS = 25


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
    1. Track B — fire B2B keyword rounds, each round scrapes URLs in parallel
    2. Track A — DGFT 'Source from India' search + scrape (if time budget allows)
    3. Deduplicate Track A results against Track B before adding

    Args:
        product_name: validated product query string

    Returns:
        (results, keywords_used)
        results: list of valid contact dicts with source field
        keywords_used: Track B keyword rounds fired (for Sheets log)
    """
    results: list[dict] = []
    seen_urls: set[str] = set()
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

        # Exit if timeout reached — but preserve DGFT time reserve
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

        # Serper search (synchronous — fast, ~1s)
        raw_urls = serper_search(keyword)
        logger.info(f"Serper returned {len(raw_urls)} URLs")

        # Filter in main thread — seen_urls not yet async-contested here
        clean_urls = filter_urls(raw_urls, seen_urls)
        logger.info(f"After filter: {len(clean_urls)} clean URLs to scrape")

        if not clean_urls:
            continue

        # Parallel scrape + extract for this keyword's URL batch
        batch_results = await scrape_batch_async(
            clean_urls,
            seen_urls,
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

        # DGFT Serper search (synchronous)
        dgft_urls = serper_dgft_search(product_name)

        if dgft_urls:
            # Deduplicate against Track B results and seen_urls
            clean_dgft_urls = filter_dgft_urls(
                dgft_urls,
                seen_urls,
                results,  # pass Track B results for domain dedup
            )

            if clean_dgft_urls:
                logger.info(f"DGFT Track A: scraping {len(clean_dgft_urls)} IEC pages")

                dgft_results = await scrape_batch_async(
                    clean_dgft_urls,
                    seen_urls,
                    seen_lock,
                    source="track_a_dgft",
                )

                # Post-scrape domain dedup — remove DGFT contacts whose
                # website domain already appears in Track B results.
                # Cannot be done pre-scrape — supplier website only known after IEC page scraped.
                track_b_domains = {
                    _get_domain(r.get("website", ""))
                    for r in results
                    if r.get("source") == "track_b"
                }
                deduped_dgft = []
                for r in dgft_results:
                    domain = _get_domain(r.get("website", ""))
                    if domain and domain in track_b_domains:
                        logger.info(
                            f"DGFT post-scrape dedup: {r.get('name')} "
                            f"already in Track B ({domain}) — skipping"
                        )
                        continue
                    deduped_dgft.append(r)

                # Only add up to TARGET_RESULT_COUNT
                slots_remaining = TARGET_RESULT_COUNT - len(results)
                dgft_to_add = deduped_dgft[:slots_remaining]
                results.extend(dgft_to_add)

                logger.info(
                    f"DGFT Track A done — "
                    f"+{len(dgft_to_add)} results (after domain dedup), "
                    f"total {len(results)}/{TARGET_RESULT_COUNT}"
                )
            else:
                logger.info("DGFT Track A: all IEC pages already seen or deduped")
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
