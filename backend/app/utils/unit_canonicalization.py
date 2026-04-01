# backend/app/utils/unit_canonicalization.py
"""
Unit canonicalization layer for construction bid/quote reconciliation.

Normalizes equivalent unit expressions to a single canonical form so
that EA vs EACH, LS vs LUMP SUM, etc. compare as equal.

Design:
  - Centralized alias table mapping variant strings to canonical units.
  - Input is normalized (strip, uppercase, collapse whitespace, remove
    trailing periods) before lookup.
  - Unknown units pass through normalized but are NOT mapped to any
    canonical — they remain distinct (fail-closed).
  - Expandable: add new aliases by extending UNIT_ALIAS_TABLE.
"""
from __future__ import annotations

import re
from typing import Optional


# ---------------------------------------------------------------------------
# Canonical unit alias table
#
# Keys are the canonical form. Values are sets of known aliases
# (already uppercase, no trailing periods).
# The canonical form is also implicitly a member of its own alias set.
# ---------------------------------------------------------------------------

UNIT_ALIAS_TABLE: dict[str, set[str]] = {
    "EACH": {"EACH", "EA"},
    "LUMP SUM": {"LUMP SUM", "LS", "L S", "LUMPSUM"},
    "SF": {"SF", "SQ FT", "SQUARE FOOT", "SQUARE FEET", "SQFT"},
    "SY": {"SY", "SQ YD", "SQUARE YARD", "SQUARE YARDS", "SQYD"},
    "LF": {"LF", "LINEAR FOOT", "LINEAR FEET", "LIN FT", "LINFT"},
    "CY": {"CY", "CU YD", "CUBIC YARD", "CUBIC YARDS", "CUYD"},
    "TON": {"TON", "TONS"},
    "STA": {"STA", "STATION", "STATIONS"},
    "ACRE": {"ACRE", "ACRES", "AC"},
    "UNIT": {"UNIT", "UNITS"},
    "GAL": {"GAL", "GALLON", "GALLONS"},
    "LB": {"LB", "LBS", "POUND", "POUNDS"},
    "CDAY": {"CDAY", "C DAY", "CALENDAR DAY", "CALENDAR DAYS", "CAL DAY"},
    "HOUR": {"HOUR", "HOURS", "HR", "HRS"},
    "DAY": {"DAY", "DAYS"},
    "MO": {"MO", "MONTH", "MONTHS"},
}


# ---------------------------------------------------------------------------
# Build reverse lookup: alias string -> canonical form
# ---------------------------------------------------------------------------

def _build_reverse_lookup() -> dict[str, str]:
    lookup: dict[str, str] = {}
    for canonical, aliases in UNIT_ALIAS_TABLE.items():
        for alias in aliases:
            if alias in lookup:
                raise ValueError(
                    f"Ambiguous unit alias: '{alias}' maps to both "
                    f"'{lookup[alias]}' and '{canonical}'"
                )
            lookup[alias] = canonical
    return lookup


_REVERSE_LOOKUP = _build_reverse_lookup()


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

_WS_RE = re.compile(r"\s+")
_TRAILING_DOTS_RE = re.compile(r"\.(?=\s|$)")


def _normalize_raw(value: str) -> str:
    """
    Pre-process a raw unit string before alias lookup:
      - strip whitespace
      - uppercase
      - remove trailing/embedded periods (L.F. -> LF, SQ. FT. -> SQ FT)
      - collapse internal whitespace
    """
    s = value.strip().upper()
    # Remove periods (common in DOT abbreviations like L.F., SQ. YD.)
    s = s.replace(".", "")
    # Collapse whitespace
    s = _WS_RE.sub(" ", s).strip()
    return s


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def canonicalize_unit(value: Optional[str]) -> str:
    """
    Normalize a unit string to its canonical form.

    - Known aliases map to the canonical form (e.g., "EA" -> "EACH").
    - Unknown units are returned normalized (uppercase, no periods,
      collapsed whitespace) but NOT mapped — they remain distinct
      so that truly different units still fail comparison.

    Args:
        value: Raw unit string from bid or quote data. May be None.

    Returns:
        Canonical unit string, or normalized fallback for unknowns.
    """
    if value is None:
        return ""
    s = _normalize_raw(str(value))
    if not s:
        return ""
    return _REVERSE_LOOKUP.get(s, s)
