# backend/app/pdf_extraction/document_router.py
"""
Deterministic document class router.

Classifies a PDF as one of:
    - dot_schedule: Iowa DOT proposal schedule of items
    - quote: subcontractor/vendor quote (commercial pricing document)
    - unknown: unrecognized / ambiguous document type

Routing is based on structural signals in the extracted text.
No fuzzy matching. No content-meaning heuristics.

Rules:
    - dot_schedule requires BOTH a DOT header AND deterministic DOT row
      structure (stacked pairs or full single-line rows). Header alone is
      not sufficient — a scanned vendor document on a DOT template (Rasch-
      style) with garbled OCR must fall through to unknown.
    - quote requires explicit monetary patterns ($X.XX) and no DOT structure.
    - Anything else is unknown.
"""
from __future__ import annotations

import re
from typing import List, Dict, Any, Literal, Tuple

DocumentClass = Literal["dot_schedule", "quote", "unknown"]

# --- DOT schedule structural signals -----------------------------------------
# Stacked format: standalone 4-digit line number followed by DOT item number
_STACKED_LINE_RE = re.compile(r'^\d{4}$')
_DOT_ITEM_RE = re.compile(r'^\d{4}-\d{7}$')

# Single-line row: LINE_NUM  ITEM_NUM  DESCRIPTION...
# Tightened: description must start with an alphabetic character. Fragments
# like "0340 2303-6911000" (bare) or "1110 2599-9999005 7.000" (no
# description column, just a trailing quantity — common in OCR of scanned
# vendor markups like Rasch) will NOT match.
_DOT_ROW_RE = re.compile(r'^\s*\d{4}\s+\d{4}-\d{7}\s+[A-Za-z]')

# --- DOT header keywords (structural, not fuzzy) -----------------------------
_DOT_HEADER_KEYWORDS = (
    "PROPOSAL SCHEDULE OF ITEMS",
    "CONTRACTS AND SPECIFICATIONS BUREAU",
    "SCHEDULE OF ITEMS",
)

# --- Quote signals -----------------------------------------------------------
# Dollar amounts: $1,234.56 or $1234.56 — must have explicit $ prefix
_DOLLAR_RE = re.compile(r'\$[\d,]+\.\d{2}')
# Explicit quote-context keywords (tight set, no fuzzy matching)
_QUOTE_KEYWORD_RE = re.compile(
    r'\b(?:quote|quotation|pricing|bid\s+price)\b',
    re.IGNORECASE,
)

# --- Thresholds (named constants, no magic numbers in decision logic) --------
_MIN_DOT_ROWS = 3            # minimum structural DOT rows to call it a schedule
_MIN_DOLLAR_QUOTE_STRONG = 4  # dollar amounts to call it a quote without keyword
_MIN_DOLLAR_QUOTE_WEAK = 2    # dollar amounts needed when quote keyword present
_MIN_TEXT_CHARS = 40          # minimum total non-whitespace chars for any decision


def classify_document(pages: List[Dict[str, Any]]) -> DocumentClass:
    """
    Classify a document as dot_schedule, quote, or unknown.

    Deterministic: based on structural signals only.
    Fail-closed: if signals are ambiguous or weak, returns "unknown".
    """
    signals = collect_classification_signals(pages)

    # Hard floor: no usable text at all → unknown
    if signals["non_ws_chars"] < _MIN_TEXT_CHARS:
        return "unknown"

    dot_rows = signals["dot_row_count"]
    has_header = signals["has_dot_header"]
    dollar_count = signals["dollar_count"]
    has_quote_kw = signals["has_quote_keyword"]

    # 1. DOT schedule requires BOTH header and structural rows.
    if has_header and dot_rows >= _MIN_DOT_ROWS:
        return "dot_schedule"

    # 2. Quote requires explicit monetary patterns AND no DOT structure.
    if dot_rows < _MIN_DOT_ROWS:
        if dollar_count >= _MIN_DOLLAR_QUOTE_STRONG:
            return "quote"
        if has_quote_kw and dollar_count >= _MIN_DOLLAR_QUOTE_WEAK:
            return "quote"

    # 3. Everything else is unknown (fail-closed default).
    return "unknown"


def collect_classification_signals(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Extract the structural signals used by classify_document.

    Exposed separately so endpoints can surface signal counts in diagnostics
    when a document fails to classify.
    """
    all_text = ""
    all_lines: List[str] = []
    non_ws_chars = 0

    for page in pages:
        text = page.get("text", "") or ""
        all_text += text + "\n"
        for raw in text.splitlines():
            stripped = raw.strip()
            if stripped:
                all_lines.append(stripped)
        non_ws_chars += sum(1 for c in text if not c.isspace())

    upper_text = all_text.upper()
    has_dot_header = any(kw in upper_text for kw in _DOT_HEADER_KEYWORDS)

    stacked_pairs = 0
    for i in range(len(all_lines) - 1):
        if _STACKED_LINE_RE.fullmatch(all_lines[i]) and _DOT_ITEM_RE.fullmatch(all_lines[i + 1]):
            stacked_pairs += 1

    single_line_dots = sum(1 for line in all_lines if _DOT_ROW_RE.match(line))
    dot_row_count = max(stacked_pairs, single_line_dots)

    dollar_count = len(_DOLLAR_RE.findall(all_text))
    has_quote_keyword = bool(_QUOTE_KEYWORD_RE.search(all_text))

    return {
        "non_ws_chars": non_ws_chars,
        "has_dot_header": has_dot_header,
        "stacked_pairs": stacked_pairs,
        "single_line_dots": single_line_dots,
        "dot_row_count": dot_row_count,
        "dollar_count": dollar_count,
        "has_quote_keyword": has_quote_keyword,
    }
