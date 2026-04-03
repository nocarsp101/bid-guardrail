# backend/app/pdf_extraction/quote_parser.py
"""
Deterministic quote row parser for subcontractor/vendor quote PDFs.

COMPLETELY SEPARATE from the DOT schedule parser.
Does NOT assume DOT item numbers, proposal line numbers, or DOT layout.

Identifies rows by structural signals:
    - Lines containing dollar amounts ($X.XX) are candidate row lines
    - Row boundaries are determined by dollar-amount patterns, not DOT structure
    - Optional leading numbers may be preserved as line_ref (NOT invented)
    - qty and unit are only populated if explicitly present in the text

Fail-closed: if row structure is ambiguous, rows are rejected.
"""
from __future__ import annotations

import re
from typing import List, Dict, Any, Optional, Tuple

from .extractor import ExtractionError


# Dollar amount: $1,234.56 — must have $ prefix
_DOLLAR_RE = re.compile(r'\$([\d,]+\.\d{2})')

# Leading line reference: 3-4 digit number at start of line (e.g., "530", "0520")
_LINE_REF_RE = re.compile(r'^(\d{3,4})\s+')

# Total/subtotal lines to exclude
_TOTAL_RE = re.compile(r'\b(?:total|subtotal|grand\s+total|sub\s+total)\b', re.IGNORECASE)

# Header/boilerplate to skip
_SKIP_PATTERNS = [
    re.compile(r'^\s*$'),
    re.compile(r'^conditions|^notes|^bond|^this quote|^lump sum|^mark\b|^cell\s*#|^[\w.]+@[\w.]+', re.IGNORECASE),
    re.compile(r'associated general|safer roads|telephone|fax|p\.o\.\s+box|avenue|letting date|county|project', re.IGNORECASE),
]


def parse_quote_rows(
    pages: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Parse quote rows from all pages.

    Returns:
        (rows, parse_meta)

    rows: list of quote row dicts with schema:
        {row_id, line_ref, description, qty, unit, unit_price, amount, source_page}

    Raises ExtractionError if no parseable rows found.
    """
    candidate_rows: List[Dict[str, Any]] = []
    row_ordinal = 0

    for page in pages:
        page_idx = page["page_index"]
        text = page["text"]

        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue

            # Skip boilerplate
            if _is_skip_line(stripped):
                continue

            # Skip total/subtotal lines
            if _TOTAL_RE.search(stripped):
                continue

            # A quote row candidate must contain at least one dollar amount
            dollars = _DOLLAR_RE.findall(stripped)
            if len(dollars) < 1:
                continue

            parsed = _parse_quote_line(stripped, dollars, page_idx, row_ordinal)
            if parsed is not None:
                candidate_rows.append(parsed)
                row_ordinal += 1

    parse_meta = {
        "rows_detected": len(candidate_rows),
        "format": "quote_tabular",
    }

    if len(candidate_rows) == 0:
        raise ExtractionError(
            "No deterministic quote rows could be parsed. "
            "Document may not contain structured tabular pricing.",
            meta=parse_meta,
        )

    return candidate_rows, parse_meta


def _parse_quote_line(
    line: str,
    dollars: List[str],
    page_idx: int,
    ordinal: int,
) -> Optional[Dict[str, Any]]:
    """
    Parse a single quote line into a structured row.

    Expected patterns:
        [line_ref] description $unit_price $amount
        [line_ref] description $amount
        description $unit_price $amount
    """
    # Extract optional leading line reference
    line_ref: Optional[str] = None
    remainder = line
    m = _LINE_REF_RE.match(line)
    if m:
        line_ref = m.group(1)
        remainder = line[m.end():]

    # Find dollar positions in the original line to split description from prices
    dollar_positions = [(m.start(), m.end(), m.group(1)) for m in _DOLLAR_RE.finditer(line)]

    if not dollar_positions:
        return None

    # Description is everything before the first dollar sign
    first_dollar_pos = dollar_positions[0][0]
    desc_text = line[:first_dollar_pos].strip()

    # If we extracted a line_ref, strip it from the description
    if line_ref and desc_text.startswith(line_ref):
        desc_text = desc_text[len(line_ref):].strip()
        # Clean up any pipe chars from OCR artifacts
        desc_text = desc_text.lstrip('|').strip()

    if not desc_text:
        return None

    # Parse dollar values
    unit_price: Optional[float] = None
    amount: Optional[float] = None

    if len(dollar_positions) >= 2:
        unit_price = _parse_dollar(dollar_positions[0][2])
        amount = _parse_dollar(dollar_positions[1][2])
    elif len(dollar_positions) == 1:
        amount = _parse_dollar(dollar_positions[0][2])

    # Must have at least an amount to be a valid row
    if amount is None:
        return None

    return {
        "row_id": ordinal,
        "line_ref": line_ref,
        "description": desc_text,
        "qty": None,      # NOT inferred — absent from quote text
        "unit": None,      # NOT inferred — absent from quote text
        "unit_price": unit_price,
        "amount": amount,
        "source_page": page_idx,
    }


def _parse_dollar(s: str) -> Optional[float]:
    """Parse a dollar string like '1,234.56' into float."""
    try:
        return float(s.replace(",", ""))
    except (ValueError, TypeError):
        return None


def _is_skip_line(line: str) -> bool:
    """Check if line is boilerplate/header/notes."""
    for pat in _SKIP_PATTERNS:
        if pat.search(line):
            return True
    return False
