"""
worker/keywords.py
Generates the ordered list of 8 search queries from a product name.
Keywords fire in priority order — highest intent first.
The loop exits early if TARGET_RESULT_COUNT is hit before all 8 are used.
"""

from common.config import MAX_KEYWORDS


# Ordered suffix templates — do not reorder without reviewing the architecture doc.
# Each entry: (suffix, rationale)
_SUFFIXES = [
    ("supplier India contact",           "Direct supplier pages with phone/email intent"),
    ("manufacturer India wholesale price","Factory pages — full contact usually prominent"),
    ("India direct factory quote",        "RFQ-intent pages publish contact to attract buyers"),
    ("exporter India phone email",        "Export-oriented pages — detailed contact data"),
    ("bulk supplier India",              "Broadens to trading companies if manufacturer pages thin"),
    ("dealer distributor India contact", "Pulls distributor/dealer tier"),
    ("company India",                    "Wide sweep — catches overview pages missed above"),
    ("India GST registered supplier",    "Last resort — surfaces compliance-listed pages"),
]


def get_keywords(product_name: str) -> list[str]:
    """
    Returns up to MAX_KEYWORDS search query strings for a given product name.
    product_name is already validated (regex) before reaching here.
    """
    product = product_name.strip()
    keywords = []
    for suffix, _ in _SUFFIXES[:MAX_KEYWORDS]:
        keywords.append(f"{product} {suffix}")
    return keywords
