# backend/app/pdf_extraction/document_router.py
"""
Deterministic document class router.

Classifies a PDF as one of:
    - dot_schedule: Iowa DOT proposal schedule of items
    - quote: subcontractor/vendor quote (commercial pricing document)
    - unknown: unrecognized document type

Routing is based on structural signals in the extracted text.
No fuzzy matching. No heuristics about content meaning.
"""
from __future__ import annotations

import re
from typing import List, Dict, Any, Literal

DocumentClass = Literal["dot_schedule", "quote", "unknown"]

# --- DOT schedule signals ---
# Stacked line number followed by DOT item number (DDDD-DDDDDDD)
_STACKED_LINE_RE = re.compile(r'^\d{4}$')
_DOT_ITEM_RE = re.compile(r'^\d{4}-\d{7}$')
# Single-line DOT row prefix
_DOT_ROW_RE = re.compile(r'^\s*\d{4}\s+\d{4}-\d{7}\b')

# --- Quote signals ---
# Dollar amounts: $1,234.56 or $1234.56
_DOLLAR_RE = re.compile(r'\$[\d,]+\.\d{2}')
# "quote" or "quotation" in text
_QUOTE_KEYWORD_RE = re.compile(r'\b(?:quote|quotation|pricing|bid\s+price)\b', re.IGNORECASE)


def classify_document(pages: List[Dict[str, Any]]) -> DocumentClass:
    """
    Classify document based on text content of all pages.

    Returns "dot_schedule", "quote", or "unknown".
    """
    dot_score = 0
    quote_score = 0

    all_text = ""
    all_lines: List[str] = []

    for page in pages:
        text = page.get("text", "")
        all_text += text + "\n"
        all_lines.extend(l.strip() for l in text.splitlines() if l.strip())

    upper_text = all_text.upper()

    # DOT schedule signals
    if "PROPOSAL SCHEDULE OF ITEMS" in upper_text:
        dot_score += 3
    if "CONTRACTS AND SPECIFICATIONS BUREAU" in upper_text:
        dot_score += 2

    # Count stacked DOT row starts (line_num then item_num on consecutive lines)
    stacked_pairs = 0
    for i in range(len(all_lines) - 1):
        if _STACKED_LINE_RE.fullmatch(all_lines[i]) and _DOT_ITEM_RE.fullmatch(all_lines[i + 1]):
            stacked_pairs += 1
    if stacked_pairs >= 3:
        dot_score += 4

    # Count single-line DOT rows
    single_line_dots = sum(1 for l in all_lines if _DOT_ROW_RE.match(l))
    if single_line_dots >= 3:
        dot_score += 4

    # Quote signals
    dollar_matches = _DOLLAR_RE.findall(all_text)
    if len(dollar_matches) >= 4:
        quote_score += 2

    if _QUOTE_KEYWORD_RE.search(all_text):
        quote_score += 2

    # Vendor letterhead signals (no DOT header)
    if "PROPOSAL SCHEDULE OF ITEMS" not in upper_text and dot_score == 0:
        if len(dollar_matches) >= 2:
            quote_score += 1

    # Decision
    if dot_score >= 3:
        return "dot_schedule"
    if quote_score >= 3 and dot_score < 2:
        return "quote"
    if quote_score >= 2 and dot_score == 0:
        return "quote"

    return "unknown"
