"""
C23 — Structured quote table extraction.

Adds a second enrichment rule (E2) to the C20 enrichment pass:

    E2 — header-gated tail-position qty/unit enrichment

    Applies only on pages where an explicit table header line has been
    detected. A table header is a single line that contains at least
    TWO distinct whitelist header tokens (QTY, UNIT, PRICE, AMOUNT,
    TOTAL, DESCRIPTION). Partial or fuzzy matches do not count.

    For accepted rows whose source page carries a detected header, E2
    looks for `<qty-numeric> <UNIT>` as the FINAL pair of tokens in the
    description text (tail position). When found, qty and unit are
    stamped with field_sources `explicit_table_header_qty` and
    `explicit_table_header_unit`.

    E2 is strictly additive: it only fires on rows that C20 E1 did NOT
    already enrich. It never overrides E1. It never overrides row state
    that already carries a non-None qty or unit.

Hard rules (same as C20):
    - No inference of missing values.
    - No semantic guessing.
    - No fuzzy header matching.
    - No column-position / pixel-based alignment — token position only.
    - Consistency guard: if unit_price and amount are both present,
      qty*unit_price must reconcile with amount within 1%.
    - Rejected / ambiguous layouts remain non-enriched.
    - Never mutates inputs.
"""
from __future__ import annotations

import re
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from .quote_enrichment import KNOWN_UNITS

TABLE_RULE_VERSION = "quote_table_extraction/v1"

SRC_TABLE_QTY = "explicit_table_header_qty"
SRC_TABLE_UNIT = "explicit_table_header_unit"

# Closed whitelist of header-label tokens. A line that contains at least
# TWO distinct tokens from this set is treated as a table header.
# Tokens are matched whole-word, case-insensitive.
_HEADER_TOKEN_PATTERNS = {
    "DESCRIPTION": re.compile(r'\bDESCRIPTION\b', re.IGNORECASE),
    "QTY": re.compile(r'\bQTY\b|\bQUANTITY\b', re.IGNORECASE),
    "UNIT": re.compile(r'\bUNIT\b', re.IGNORECASE),
    "PRICE": re.compile(r'\bPRICE\b', re.IGNORECASE),
    "AMOUNT": re.compile(r'\bAMOUNT\b', re.IGNORECASE),
    "TOTAL": re.compile(r'\bTOTAL\b', re.IGNORECASE),
    "ITEM": re.compile(r'\bITEM\b', re.IGNORECASE),
}

# Exactly the same consistency drift as C20 and the validator.
_CONSISTENCY_DRIFT = 0.01

# A tail-position <qty-num> <UNIT> pair. Anchored to the END of the
# description (after stripping trailing whitespace). Non-greedy so only
# the final token pair is considered.
_TAIL_QTY_UNIT_RE = re.compile(
    r'(?<![\w.])(?P<qty>\d{1,6}(?:,\d{3})*(?:\.\d+)?)\s+(?P<unit>[A-Za-z]{1,5})\s*$',
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_table_metadata(pages: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """
    Walk the extracted pages and return, per page, deterministic table
    header metadata. Pages without a detected header appear in the
    return dict with `header_detected=False`.

    Returns: {page_index: {
        "header_detected": bool,
        "header_tokens": [token_name, ...],
        "header_line": str,   # empty when not detected
        "header_line_index": int | None,
    }}
    """
    metadata: Dict[int, Dict[str, Any]] = {}
    for page in pages:
        page_idx = page.get("page_index", 0)
        text = page.get("text") or ""
        header_detected = False
        header_tokens: List[str] = []
        header_line = ""
        header_line_index: Optional[int] = None

        for idx, raw_line in enumerate(text.splitlines()):
            line = raw_line.strip()
            if not line:
                continue
            tokens = _match_header_tokens(line)
            if len(tokens) >= 2:
                header_detected = True
                header_tokens = tokens
                header_line = line
                header_line_index = idx
                break  # first deterministic header wins

        metadata[page_idx] = {
            "header_detected": header_detected,
            "header_tokens": header_tokens,
            "header_line": header_line,
            "header_line_index": header_line_index,
        }
    return metadata


def enrich_quote_rows_with_tables(
    rows: List[Dict[str, Any]],
    pages: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Apply the C23 header-gated tail-position rule (E2) to a list of
    rows that have already been processed by C20 quote_enrichment.

    Rows that were already enriched by E1 are left untouched. E2 only
    fires when both:
        1. The row's source page has an explicit table header detected.
        2. The row's description ends in a tail-position `<qty-num> <UNIT>`
           pair whose UNIT is in the KNOWN_UNITS whitelist.

    Also applies the same consistency guard as C20: if unit_price and
    amount are both present, qty*unit_price must reconcile with amount
    within 1%.

    Returns a new list. Never mutates the inputs.
    """
    metadata = detect_table_metadata(pages)
    out: List[Dict[str, Any]] = []

    for row in rows:
        new_row = deepcopy(row)
        page_idx = row.get("source_page", 0)
        page_meta = metadata.get(page_idx) or {
            "header_detected": False,
            "header_tokens": [],
            "header_line": "",
            "header_line_index": None,
        }

        trace_entry = _apply_rule_e2(row, page_meta)

        # Thread the trace entry through the existing enrichment_trace.
        existing_trace = new_row.get("enrichment_trace") or {"rules_attempted": []}
        rules = list(existing_trace.get("rules_attempted") or [])
        rules.append(trace_entry)
        new_row["enrichment_trace"] = {"rules_attempted": rules}

        if trace_entry.get("applied"):
            new_row["qty"] = trace_entry["qty"]
            new_row["unit"] = trace_entry["unit"]
            field_sources = dict(new_row.get("field_sources") or {})
            field_sources["qty"] = SRC_TABLE_QTY
            field_sources["unit"] = SRC_TABLE_UNIT
            new_row["field_sources"] = field_sources
            # Stamp the table rule version alongside the existing enricher
            # version so provenance is explicit.
            new_row["table_rule_version"] = TABLE_RULE_VERSION

        out.append(new_row)

    return out


# ---------------------------------------------------------------------------
# Rule E2 — header-gated tail-position qty/unit
# ---------------------------------------------------------------------------

def _apply_rule_e2(
    row: Dict[str, Any],
    page_meta: Dict[str, Any],
) -> Dict[str, Any]:
    """Return a trace dict describing the outcome of rule E2 for one row."""
    trace = {
        "rule": "E2_table_header_tail_qty_unit",
        "applied": False,
        "page_header_detected": bool(page_meta.get("header_detected")),
        "header_tokens_detected": list(page_meta.get("header_tokens") or []),
    }

    # If either qty or unit is already set, leave alone. E2 never overrides.
    if row.get("qty") is not None or row.get("unit") is not None:
        trace["skip_reason"] = "row_already_has_qty_or_unit"
        return trace

    if not page_meta.get("header_detected"):
        trace["skip_reason"] = "no_table_header_on_page"
        return trace

    description = row.get("description") or ""
    match = _TAIL_QTY_UNIT_RE.search(description)
    if match is None:
        trace["skip_reason"] = "no_tail_qty_unit_token"
        return trace

    unit_val = match.group("unit").upper()
    if unit_val not in KNOWN_UNITS:
        trace["skip_reason"] = "tail_unit_not_in_whitelist"
        trace["matched_span"] = match.group(0)
        return trace

    qty_val = _parse_qty(match.group("qty"))
    if qty_val is None:
        trace["skip_reason"] = "bad_qty_numeric"
        trace["matched_span"] = match.group(0)
        return trace
    if qty_val <= 0:
        trace["skip_reason"] = "non_positive_qty"
        trace["matched_span"] = match.group(0)
        return trace

    # Consistency guard — identical to the C20 E1 guard.
    up = row.get("unit_price")
    amt = row.get("amount")
    if up is not None and amt is not None:
        try:
            expected = qty_val * float(up)
            amount = float(amt)
            if amount <= 0 or expected <= 0:
                trace["skip_reason"] = "non_positive_arithmetic"
                trace["matched_span"] = match.group(0)
                return trace
            drift = abs(expected - amount) / amount
            if drift > _CONSISTENCY_DRIFT:
                trace["skip_reason"] = "arithmetic_mismatch"
                trace["matched_span"] = match.group(0)
                trace["expected"] = round(expected, 4)
                trace["amount"] = amount
                trace["drift"] = round(drift, 4)
                return trace
        except (TypeError, ValueError):
            trace["skip_reason"] = "bad_numeric_types"
            trace["matched_span"] = match.group(0)
            return trace

    trace["applied"] = True
    trace["qty"] = qty_val
    trace["unit"] = unit_val
    trace["matched_span"] = match.group(0)
    return trace


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _match_header_tokens(line: str) -> List[str]:
    """Return the list of distinct whitelist header tokens present on a line."""
    tokens: List[str] = []
    for name, pat in _HEADER_TOKEN_PATTERNS.items():
        if pat.search(line):
            tokens.append(name)
    return tokens


def _parse_qty(raw: str) -> Optional[float]:
    try:
        return float(raw.replace(",", ""))
    except (TypeError, ValueError):
        return None
