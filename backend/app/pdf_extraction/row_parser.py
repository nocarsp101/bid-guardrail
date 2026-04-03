# backend/app/pdf_extraction/row_parser.py
"""
Deterministic row parser for DOT schedule-of-items.

Supports two extraction formats:
  1. Single-line: LINE ITEM DESC UNIT QTY (synthetic / pre-formatted PDFs)
  2. Stacked: each field on its own line (real Iowa DOT proposal PDFs)

Format is auto-detected. No fuzzy matching. No heuristics.
Rejects any row that cannot be fully assembled.
"""
from __future__ import annotations

import re
from typing import List, Dict, Any, Optional, Tuple

from .extractor import ExtractionError


# ---------------------------------------------------------------------------
# Known DOT unit vocabulary (uppercase, closed set for Iowa DOT schedules)
# ---------------------------------------------------------------------------
KNOWN_UNITS = frozenset({
    "EACH", "CY", "LF", "SY", "TON", "STA", "ACRE", "SF",
    "LUMP SUM", "CDAY", "UNIT", "GAL", "LB", "MG", "HR",
    "MILE", "CF", "DAY", "MGAL", "SQ",
})

# Single-word units for stacked-format matching (exact full-line match)
_SINGLE_WORD_UNITS = frozenset(u for u in KNOWN_UNITS if " " not in u)

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Standalone 4-digit line number (stacked format)
_LINE_NUM_RE = re.compile(r'^\d{4}$')

# DOT item number: DDDD-DDDDDDD
_ITEM_NUM_RE = re.compile(r'^\d{4}-\d{7}$')

# Single-line row: LINE_NUM  ITEM_NUM  DESCRIPTION  UNIT  QTY
_ROW_START_RE = re.compile(r'^\s*(\d{4})\s+(\d{4}-\d{7})\s+(.*)')

# Quantity: number with optional commas and decimals
_QTY_RE = re.compile(r'^[\d,]+\.\d+$')

# Price placeholder (blank bid form)
_PLACEHOLDER_RE = re.compile(r'^_+\._+$')

# Filled price value (digits, commas, decimal, or just digits)
_PRICE_RE = re.compile(r'^[\d,]+\.\d{2,5}$')

# Section / summary / total lines
_SUMMARY_RE = re.compile(
    r'(?:section\s+total|section[:\s]+\d|total[:\s]|total\s+bid|subtotal|grand\s+total)',
    re.IGNORECASE,
)

# Page header lines to skip (stacked DOT format)
_SKIP_LINES = frozenset({
    "contracts and specifications bureau",
    "proposal schedule of items",
    "roadway items",
    "bid amount",
    "unit price",
    "item quantity",
    "and units",
    "item number",
    "item description",
    "proposal",
    "line",
    "number",
    "dollars",
    "cents",
})

# Pattern for header lines with variable content
_SKIP_PATTERNS_RE = re.compile(
    r'^(?:'
    r'proposal\s+id[:\s]|'
    r'call\s+order[:\s]|'
    r'letting\s+date[:\s]|'
    r'section[:\s]+\d|'
    r'page\s+\d|'
    r'\d{2}/\d{2}/\d{4}\s+\d{1,2}:\d{2}\s+[AP]M'  # timestamp like 11/13/2025 11:14 AM
    r')',
    re.IGNORECASE,
)

# Legacy single-line page header pattern
_LEGACY_PAGE_HEADER_RE = re.compile(
    r'^\s*(line\s+item|schedule\s+of\s+items|proposal\s+id|page\s+\d)',
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_schedule_rows(
    pages: List[Dict[str, Any]],
    schedule_page_indices: List[int],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Parse DOT schedule rows from the identified schedule pages.

    Auto-detects stacked vs single-line format.

    Returns:
        (rows, parse_meta)

    Raises ExtractionError if no rows could be parsed.
    """
    # Collect all lines from schedule pages with page tracking
    tagged_lines = _collect_lines(pages, schedule_page_indices)

    # Detect format
    fmt = _detect_format(tagged_lines)

    if fmt == "stacked":
        raw_rows = _parse_stacked(tagged_lines)
    else:
        raw_rows = _parse_single_line(tagged_lines)

    parse_meta = {
        "rows_detected": len(raw_rows),
        "rows_rejected_lines": 0,
        "schedule_pages": schedule_page_indices,
        "rejected_samples": [],
        "format_detected": fmt,
    }

    if len(raw_rows) == 0:
        raise ExtractionError(
            "No schedule rows could be parsed from schedule pages.",
            meta=parse_meta,
        )

    return raw_rows, parse_meta


# ---------------------------------------------------------------------------
# Line collection and format detection
# ---------------------------------------------------------------------------

def _collect_lines(
    pages: List[Dict[str, Any]],
    schedule_page_indices: List[int],
) -> List[Tuple[str, int]]:
    """Collect (stripped_text, page_index) for all non-empty lines on schedule pages."""
    result: List[Tuple[str, int]] = []
    for page_idx in schedule_page_indices:
        text = pages[page_idx]["text"]
        for line in text.splitlines():
            stripped = line.strip()
            if stripped:
                result.append((stripped, page_idx))
    return result


def _detect_format(tagged_lines: List[Tuple[str, int]]) -> str:
    """
    Deterministically detect whether the schedule uses stacked or single-line format.

    Stacked: standalone 4-digit line number on one line, DOT item number on the next.
    Single-line: LINE ITEM DESC UNIT QTY all on one line.
    """
    texts = [t for t, _ in tagged_lines]
    for i in range(len(texts) - 1):
        if _LINE_NUM_RE.fullmatch(texts[i]) and _ITEM_NUM_RE.fullmatch(texts[i + 1]):
            return "stacked"
    return "single_line"


# ---------------------------------------------------------------------------
# Stacked-format parser (state machine)
# ---------------------------------------------------------------------------

# States
_S_SEEK_LINE = "seek_line"      # looking for a 4-digit line number
_S_EXPECT_ITEM = "expect_item"  # next must be DOT item number
_S_COLLECT_DESC = "collect_desc"  # collecting description until unit found
_S_EXPECT_QTY = "expect_qty"    # unit found, next numeric is qty
_S_SKIP_TAIL = "skip_tail"      # skip prices/placeholders until next line num


def _parse_stacked(tagged_lines: List[Tuple[str, int]]) -> List[Dict[str, Any]]:
    """Parse stacked-format DOT schedule into rows using a state machine."""
    rows: List[Dict[str, Any]] = []

    state = _S_SEEK_LINE
    cur_line_num: str = ""
    cur_item: str = ""
    cur_desc_parts: List[str] = []
    cur_unit: str = ""
    cur_page: int = 0

    def _emit():
        """Emit the current row if complete."""
        nonlocal cur_line_num, cur_item, cur_desc_parts, cur_unit, cur_page
        desc = " ".join(cur_desc_parts).strip()
        if cur_line_num and cur_item and desc and cur_unit:
            qty = 1.0 if cur_unit == "LUMP SUM" else None
            rows.append({
                "line_number": cur_line_num,
                "item": cur_item,
                "description": desc,
                "unit": cur_unit,
                "qty": qty,  # will be filled for non-LUMP-SUM in EXPECT_QTY
                "source_page": cur_page,
            })

    for text, page_idx in tagged_lines:
        # Always skip known header/boilerplate/placeholder lines
        if _is_skip_line(text):
            continue

        if state == _S_SEEK_LINE:
            if _LINE_NUM_RE.fullmatch(text):
                cur_line_num = text
                cur_item = ""
                cur_desc_parts = []
                cur_unit = ""
                cur_page = page_idx
                state = _S_EXPECT_ITEM
            # else: skip (stray values, section text, etc.)

        elif state == _S_EXPECT_ITEM:
            if _ITEM_NUM_RE.fullmatch(text):
                cur_item = text
                state = _S_COLLECT_DESC
            else:
                # Not a valid item number — abandon this row, re-evaluate
                state = _S_SEEK_LINE
                # Re-check if this line is itself a line number
                if _LINE_NUM_RE.fullmatch(text):
                    cur_line_num = text
                    cur_item = ""
                    cur_desc_parts = []
                    cur_unit = ""
                    cur_page = page_idx
                    state = _S_EXPECT_ITEM

        elif state == _S_COLLECT_DESC:
            upper = text.upper()
            # Check for LUMP SUM (two-word unit) as exact line
            if upper == "LUMP SUM":
                cur_unit = "LUMP SUM"
                # LUMP SUM items have implicit qty=1.0 — emit and skip tail
                _emit()
                state = _S_SKIP_TAIL
            elif upper in _SINGLE_WORD_UNITS:
                cur_unit = upper
                state = _S_EXPECT_QTY
            else:
                # Must be a description line — require at least one letter
                if any(c.isalpha() for c in text):
                    cur_desc_parts.append(text)
                else:
                    # Unexpected non-alpha line during description collection
                    # Could be a stray number — abandon row
                    state = _S_SEEK_LINE

        elif state == _S_EXPECT_QTY:
            qty = _parse_qty(text)
            if qty is not None:
                rows.append({
                    "line_number": cur_line_num,
                    "item": cur_item,
                    "description": " ".join(cur_desc_parts).strip(),
                    "unit": cur_unit,
                    "qty": qty,
                    "source_page": cur_page,
                })
                state = _S_SKIP_TAIL
            else:
                # Expected quantity but got something else — abandon row
                state = _S_SEEK_LINE
                if _LINE_NUM_RE.fullmatch(text):
                    cur_line_num = text
                    cur_item = ""
                    cur_desc_parts = []
                    cur_unit = ""
                    cur_page = page_idx
                    state = _S_EXPECT_ITEM

        elif state == _S_SKIP_TAIL:
            # Skip price placeholders, filled prices, repeated LUMP SUM, stray values
            # until we see the next line number
            if _LINE_NUM_RE.fullmatch(text):
                cur_line_num = text
                cur_item = ""
                cur_desc_parts = []
                cur_unit = ""
                cur_page = page_idx
                state = _S_EXPECT_ITEM
            # else: skip (prices, placeholders, "LUMP SUM" in bid-amount column, etc.)

    return rows


def _is_skip_line(text: str) -> bool:
    """Return True if line is a known header, boilerplate, or placeholder to skip."""
    lower = text.lower().strip()

    # Exact match against known boilerplate
    if lower in _SKIP_LINES:
        return True

    # Pattern match against variable header lines
    if _SKIP_PATTERNS_RE.match(text):
        return True

    # Price placeholders: _________._____
    if _PLACEHOLDER_RE.fullmatch(text):
        return True

    # Summary/total lines
    if _SUMMARY_RE.search(text):
        return True

    # Lone dot
    if text == ".":
        return True

    return False


# ---------------------------------------------------------------------------
# Single-line format parser (original C8A logic, for synthetic fixtures)
# ---------------------------------------------------------------------------

def _parse_single_line(tagged_lines: List[Tuple[str, int]]) -> List[Dict[str, Any]]:
    """Parse single-line format: LINE ITEM DESC UNIT QTY per line."""
    raw_rows: List[Dict[str, Any]] = []
    continuation_buffer: Optional[Dict[str, Any]] = None
    prev_page: Optional[int] = None

    for text, page_idx in tagged_lines:
        # Flush continuation buffer at page boundary
        if page_idx != prev_page and continuation_buffer is not None:
            raw_rows.append(continuation_buffer)
            continuation_buffer = None
        prev_page = page_idx

        # Skip headers
        if _LEGACY_PAGE_HEADER_RE.match(text):
            continue
        if _SUMMARY_RE.search(text):
            continue

        # Try to parse as a single-line data row
        parsed = _parse_dot_row_single(text, page_idx)

        if parsed is not None:
            if continuation_buffer is not None:
                raw_rows.append(continuation_buffer)
            continuation_buffer = parsed
        else:
            if continuation_buffer is not None and _is_continuation_line(text):
                continuation_buffer["description"] += " " + text
            else:
                if continuation_buffer is not None:
                    raw_rows.append(continuation_buffer)
                    continuation_buffer = None

    if continuation_buffer is not None:
        raw_rows.append(continuation_buffer)

    return raw_rows


def _parse_dot_row_single(line: str, page_idx: int) -> Optional[Dict[str, Any]]:
    """Parse a single-line DOT schedule row: LINE ITEM DESC UNIT QTY."""
    m = _ROW_START_RE.match(line)
    if not m:
        return None

    line_number = m.group(1)
    item = m.group(2)
    remainder = m.group(3).strip()

    parsed_tail = _parse_unit_and_qty(remainder)
    if parsed_tail is None:
        return None

    description, unit, qty = parsed_tail
    if not description.strip():
        return None

    return {
        "line_number": line_number,
        "item": item,
        "description": description.strip(),
        "unit": unit,
        "qty": qty,
        "source_page": page_idx,
    }


def _parse_unit_and_qty(remainder: str) -> Optional[Tuple[str, str, float]]:
    """Extract (description, unit, qty) from remainder, working right-to-left."""
    tokens = remainder.split()
    if len(tokens) < 3:
        return None

    qty_str = tokens[-1]
    qty = _parse_qty(qty_str)
    if qty is None:
        return None

    # Try 2-word unit first
    if len(tokens) >= 4:
        two_word = tokens[-3].upper() + " " + tokens[-2].upper()
        if two_word in KNOWN_UNITS:
            return " ".join(tokens[:-3]), two_word, qty

    # Try 1-word unit
    if len(tokens) >= 3:
        one_word = tokens[-2].upper()
        if one_word in KNOWN_UNITS:
            return " ".join(tokens[:-2]), one_word, qty

    return None


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _parse_qty(s: str) -> Optional[float]:
    """Parse a quantity string. Plain number with optional commas and decimals."""
    s = s.strip().replace(",", "")
    if not s:
        return None
    try:
        val = float(s)
        return val if val > 0 else None
    except (ValueError, TypeError):
        return None


def _is_continuation_line(line: str) -> bool:
    """Check if a line is a description continuation (single-line format only)."""
    stripped = line.strip()
    if not stripped:
        return False
    if re.match(r'^\d{4}\s', stripped):
        return False
    if _LEGACY_PAGE_HEADER_RE.match(stripped) or _SUMMARY_RE.search(stripped):
        return False
    if not any(c.isalpha() for c in stripped):
        return False
    return True
