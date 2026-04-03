# backend/app/pdf_extraction/schedule_detector.py
"""
Deterministic schedule-of-items section detection.

Identifies which pages in a PDF contain the bid schedule table
by looking for the column header pattern and DOT row patterns.

No heuristics. No fuzzy matching.
"""
from __future__ import annotations

import re
from typing import List, Dict, Any

from .extractor import ExtractionError


# The DOT schedule header contains these column labels.
# We require at least 3 of these to appear on the same page to consider it a schedule page.
_SCHEDULE_HEADER_TOKENS = {"LINE", "ITEM", "DESCRIPTION", "UNIT", "QUANTITY"}

# DOT schedule rows: single-line format (NNNN NNNN-NNNNNNN ...)
_DOT_ROW_PREFIX_RE = re.compile(r'^\s*\d{4}\s+\d{4}-\d{7}\b')

# Stacked format patterns: standalone line number and item number
_STACKED_LINE_NUM_RE = re.compile(r'^\d{4}$')
_STACKED_ITEM_NUM_RE = re.compile(r'^\d{4}-\d{7}$')


def detect_schedule_pages(
    pages: List[Dict[str, Any]],
) -> List[int]:
    """
    Return sorted list of page indices that contain schedule-of-items content.

    Detection criteria (deterministic):
    1. Page contains a column header matching >= 3 of the known schedule header tokens, OR
    2. Page contains >= 2 lines matching the DOT row prefix pattern (NNNN NNNN-NNNNNNN), OR
    3. Page contains >= 2 stacked row starts (standalone 4-digit line followed by DOT item)

    Raises ExtractionError if no schedule pages detected.
    """
    schedule_pages: List[int] = []

    for page_info in pages:
        idx = page_info["page_index"]
        text = page_info["text"]

        if (_page_has_schedule_header(text)
                or _page_has_dot_rows(text)
                or _page_has_stacked_rows(text)):
            schedule_pages.append(idx)

    if not schedule_pages:
        raise ExtractionError(
            "No schedule-of-items pages detected. "
            "Expected DOT column headers or row patterns not found.",
            meta={"pages_scanned": len(pages)},
        )

    return sorted(schedule_pages)


def _page_has_schedule_header(text: str) -> bool:
    """Check if page text contains >= 3 known schedule column header tokens."""
    upper = text.upper()
    hits = sum(1 for token in _SCHEDULE_HEADER_TOKENS if token in upper)
    return hits >= 3


def _page_has_dot_rows(text: str) -> bool:
    """Check if page has >= 2 lines matching DOT schedule row prefix."""
    count = 0
    for line in text.splitlines():
        if _DOT_ROW_PREFIX_RE.match(line):
            count += 1
            if count >= 2:
                return True
    return False


def _page_has_stacked_rows(text: str) -> bool:
    """Check if page has >= 2 stacked DOT row starts (line_num then item_num)."""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    count = 0
    for i in range(len(lines) - 1):
        if (_STACKED_LINE_NUM_RE.fullmatch(lines[i])
                and _STACKED_ITEM_NUM_RE.fullmatch(lines[i + 1])):
            count += 1
            if count >= 2:
                return True
    return False
