"""
worker/extractor.py
Claude Haiku contact extraction.
Reads a scraped page's markdown and returns a structured contact dict or None.

Validation rule (confirmed with founder 10 Apr 2026):
- REQUIRED: name, address, website
- OPTIONAL: phone, email

Both phone and email are scraped and included in output if found,
but their absence does not discard a contact. Indian B2B supplier
websites frequently hide both behind contact forms or WhatsApp buttons.
"""

import json
import logging
import anthropic
from common.config import ANTHROPIC_API_KEY

logger = logging.getLogger(__name__)

_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# Fields required for a contact to pass validation.
# phone and email are intentionally excluded — scraped but not required.
REQUIRED_FIELDS = {"name", "address", "website"}

_SYSTEM_PROMPT = """You are a data extraction assistant. You will be given the markdown content of a company website page. Your job is to extract the primary supplier contact information.

Return ONLY a valid JSON object with exactly these fields:
{
  "name": "Company or trading name",
  "phone": "Primary phone number",
  "email": "Contact email address",
  "address": "Business address (city and state minimum)",
  "product_description": "What the company makes or supplies (1-2 sentences)",
  "website": "The URL of this page"
}

Rules:
- If any field cannot be found, set that field to null
- Do not invent or guess data — only extract what is clearly present on the page
- Return ONLY the JSON object — no explanation, no markdown, no backticks
- product_description should describe what the company supplies, not their company history
- phone is optional — set to null if not clearly visible on the page
- email is optional — set to null if not visible on the page, do not guess"""

_STRICT_SYSTEM_PROMPT = (
    _SYSTEM_PROMPT
    + "\n\nCRITICAL: Return ONLY raw JSON. "
    "No backticks, no markdown, no text before or after the JSON object."
)


def _call_haiku(page_markdown: str, system_prompt: str) -> str:
    """Make a single Haiku API call and return the raw text response."""
    message = _client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=400,
        system=system_prompt,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Extract the supplier contact from this page:\n\n"
                    f"{page_markdown[:8000]}"
                ),
            }
        ],
    )
    return message.content[0].text.strip()


def _parse_json(raw: str) -> dict | None:
    """
    Attempt to parse JSON from the raw Haiku response.
    Handles common formatting quirks: markdown fences, leading text.
    """
    raw = raw.strip()

    # Strip markdown code fences if present
    if raw.startswith("```"):
        lines = raw.split("\n")
        raw = "\n".join(lines[1:-1]) if len(lines) > 2 else raw

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Try to find JSON object within the response
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(raw[start:end])
            except json.JSONDecodeError:
                return None
        return None


def is_valid(contact: dict | None) -> bool:
    """
    A contact is valid if name, address, and website are all present and non-empty.
    phone and email are optional — their absence does not fail validation.
    """
    if not contact:
        return False
    for field in REQUIRED_FIELDS:
        val = contact.get(field)
        if not val or not str(val).strip():
            return False
    return True


def extract_contact(page_markdown: str, url: str) -> dict | None:
    """
    Main entry point. Given a page's markdown content and its URL:
    1. Calls Claude Haiku to extract contact JSON
    2. Retries once with stricter prompt on parse failure
    3. Validates required fields (name, address, website)
    4. Returns validated contact dict or None

    The URL is injected as 'website' if Haiku doesn't find it.
    phone and email are included in output if found; absence is acceptable.

    Note: 'source' field (track_b / track_a_dgft) is added by scraper.py,
    not here — extractor has no knowledge of which track called it.
    """
    if not page_markdown or len(page_markdown.strip()) < 100:
        return None

    # Attempt 1: standard prompt
    try:
        raw = _call_haiku(page_markdown, _SYSTEM_PROMPT)
        contact = _parse_json(raw)
    except Exception as e:
        logger.warning(f"Haiku call failed on first attempt for {url}: {e}")
        contact = None

    # Attempt 2: stricter prompt on parse failure
    if contact is None:
        try:
            logger.info(f"Retrying with strict prompt for {url}")
            raw = _call_haiku(page_markdown, _STRICT_SYSTEM_PROMPT)
            contact = _parse_json(raw)
        except Exception as e:
            logger.warning(f"Haiku call failed on second attempt for {url}: {e}")
            return None

    if contact is None:
        logger.info(f"Could not parse JSON from Haiku for {url}")
        return None

    # Inject source URL if Haiku left website null
    if not contact.get("website"):
        contact["website"] = url

    if not is_valid(contact):
        logger.info(f"Contact from {url} missing required fields — discarded")
        return None

    return contact
