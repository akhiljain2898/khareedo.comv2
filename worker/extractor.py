"""
worker/extractor.py
Claude Haiku contact extraction.
Reads a scraped page's markdown and returns a structured contact dict or None.

Validation rule:
- REQUIRED: name, phone, address, website (all 4 must be present)
- OPTIONAL: email (scraped and included if found, but not a blocker)

Indian B2B supplier websites commonly hide email behind contact forms.
Requiring email was discarding valid suppliers — removed as a hard requirement.
"""

import json
import logging
import anthropic
from common.config import ANTHROPIC_API_KEY

logger = logging.getLogger(__name__)

_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# Fields required for a contact to be considered valid.
# Email is intentionally excluded — it is scraped but not required.
# Many legitimate Indian B2B supplier sites don't expose email on the page.
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
- email is optional — set to null if not visible on the page, do not guess"""

_STRICT_SYSTEM_PROMPT = _SYSTEM_PROMPT + "\n\nCRITICAL: Return ONLY raw JSON. No backticks, no markdown, no text before or after the JSON object."


def _call_haiku(page_markdown: str, system_prompt: str) -> str:
    """Make a single Haiku API call and return the raw text response."""
    message = _client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=400,
        system=system_prompt,
        messages=[
            {
                "role": "user",
                "content": f"Extract the supplier contact from this page:\n\n{page_markdown[:8000]}"
            }
        ]
    )
    return message.content[0].text.strip()


def _parse_json(raw: str) -> dict | None:
    """Attempt to parse JSON from the raw response, handling common Haiku formatting quirks."""
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
    A contact is valid if all 4 required fields are present and non-empty:
    name, phone, address, website.

    Email is optional — included in output if found, not checked here.
    product_description is also optional.
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
    3. Returns validated contact dict or None

    The URL is injected into the result as 'website' if Haiku doesn't find it.
    Email will be included in the output if Haiku finds it, but absence of
    email does not discard the contact.
    """
    if not page_markdown or len(page_markdown.strip()) < 100:
        # Page too short / empty — skip
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
            logger.info(f"Retrying extraction for {url} with strict prompt")
            raw = _call_haiku(page_markdown, _STRICT_SYSTEM_PROMPT)
            contact = _parse_json(raw)
        except Exception as e:
            logger.warning(f"Haiku call failed on second attempt for {url}: {e}")
            return None

    if contact is None:
        logger.info(f"Could not parse JSON from Haiku response for {url}")
        return None

    # Ensure website is populated — use the source URL if Haiku left it null
    if not contact.get("website"):
        contact["website"] = url

    # Validate required fields (email not included in check)
    if not is_valid(contact):
        logger.info(f"Contact from {url} missing required fields — discarded")
        return None

    return contact
