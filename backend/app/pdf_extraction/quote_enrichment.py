"""
C20 — Deterministic quote data enrichment.

Increases downstream comparability by capturing explicit qty/unit tokens
that are already present in the source line of a parsed quote row. This
module is run AFTER `parse_quote_rows` and BEFORE `validate_quote_rows`
so the existing consistency rule (qty * unit_price ≈ amount within 1%)
acts as a final safety net.

Hard rules:
    - Enrichment is a pure function. The input row is never mutated.
    - Values are enriched ONLY when an explicit deterministic pattern
      matches.
    - qty/unit are NEVER inferred from description semantics, from
      amount/unit_price arithmetic, or from surrounding context.
    - If multiple candidate tokens are present in a single row, the
      enrichment is ambiguous and must fail (leave qty/unit as None).
    - Enrichment never elevates a rejected_candidate into accepted_rows.
    - Every enriched field carries an explicit field_sources marker and
      an enrichment_trace entry so the decision is auditable.

Rule vocabulary:

    E1 — explicit inline qty + unit token
        Pattern: `<qty> <UNIT>` appearing exactly once in the description
        text, where UNIT is a member of the closed KNOWN_UNITS whitelist.
        Example matches: "24 LF", "1,200 SY", "8.5 TON".
        Required: exactly ONE whitelist-valid match. Multiple matches
        fail closed. A single whitelist-invalid token (e.g. "4 in") is
        ignored, not treated as a match.
        Consistency guard: if unit_price and amount are both already
        present, qty*unit_price must match amount within 1% drift.
        Otherwise enrichment is rejected to preserve determinism.

No other enrichment rules are applied. unit_price and amount stay exactly
as parsed by C9 quote_parser. Missing unit_price/amount are never
synthesised.
"""
from __future__ import annotations

import re
from copy import deepcopy
from typing import Any, Dict, List, Optional

ENRICHER_VERSION = "quote_enrichment/v1"

# Closed whitelist of recognised unit tokens. Members are canonical (upper).
# Used via case-insensitive matching but always stored canonicalised.
KNOWN_UNITS = frozenset({
    "LF", "LS", "SY", "CY", "EA", "EACH",
    "FT", "SF", "SQFT",
    "TON", "TONS",
    "GAL", "HR", "HRS",
    "LB", "LBS",
    "MSF", "MBF", "MGAL",
    "YD", "YDS", "CUYD",
    "MI", "AC",
})

# Regex for a candidate `<qty> <UNIT>` token:
#   - qty: positive number, optionally with thousands commas and decimal.
#   - unit: 1–5 alphabetic characters, word-bounded.
# KNOWN_UNITS is applied after the regex match to filter out noise like
# "4 in", "2 x", "10 at", etc.
_CANDIDATE_QTY_UNIT_RE = re.compile(
    r'(?<![\w.])(?P<qty>\d{1,6}(?:,\d{3})*(?:\.\d+)?)\s+(?P<unit>[A-Za-z]{1,5})\b',
)

# Field-source vocabulary (closed set, deterministic).
SRC_EXPLICIT_INLINE = "explicit_inline_qty_unit"
SRC_EXPLICIT_DOLLAR = "explicit_dollar_parser"
SRC_PARSER_PRE_EXISTING = "parser_pre_existing"
SRC_NOT_PRESENT = "not_present"

# Consistency guard tolerance — matches the validator.
_CONSISTENCY_DRIFT = 0.01


def enrich_quote_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply deterministic enrichment rules to a parsed quote row.

    Returns a NEW dict. The original row is never mutated. Fields that
    are not affected by any rule are copied verbatim. Every return value
    carries `field_sources`, `enrichment_trace`, and `enricher_version`.
    """
    new = deepcopy(row)

    # Start with baseline field_sources derived from the row state as
    # parsed by C9. Every key is always present — determinism.
    field_sources: Dict[str, str] = {
        "qty": _baseline_source(row.get("qty")),
        "unit": _baseline_source(row.get("unit")),
        "unit_price": SRC_EXPLICIT_DOLLAR if row.get("unit_price") is not None else SRC_NOT_PRESENT,
        "amount": SRC_EXPLICIT_DOLLAR if row.get("amount") is not None else SRC_NOT_PRESENT,
    }
    rules_attempted: List[Dict[str, Any]] = []

    # ----- Rule E1: explicit inline qty + unit token -----
    only_enrich = row.get("qty") is None and row.get("unit") is None
    if only_enrich:
        e1 = _apply_rule_e1(row)
        rules_attempted.append(e1)
        if e1.get("applied"):
            new["qty"] = e1["qty"]
            new["unit"] = e1["unit"]
            field_sources["qty"] = SRC_EXPLICIT_INLINE
            field_sources["unit"] = SRC_EXPLICIT_INLINE
    else:
        rules_attempted.append({
            "rule": "E1_inline_qty_unit",
            "applied": False,
            "skip_reason": "row_already_has_qty_or_unit",
        })

    new["field_sources"] = field_sources
    new["enrichment_trace"] = {
        "rules_attempted": rules_attempted,
    }
    new["enricher_version"] = ENRICHER_VERSION
    return new


def enrich_quote_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Enrich a list of parsed quote rows. Never mutates the inputs."""
    return [enrich_quote_row(r) for r in rows]


# ---------------------------------------------------------------------------
# Rule E1 — explicit inline qty + unit token
# ---------------------------------------------------------------------------

def _apply_rule_e1(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Rule E1: find an explicit `<qty> <UNIT>` token in the description.

    Returns a trace dict shaped like:
        {"rule": "E1_inline_qty_unit", "applied": bool, ...}
    """
    description = row.get("description") or ""

    matches = _find_whitelist_candidates(description)
    if len(matches) == 0:
        return {
            "rule": "E1_inline_qty_unit",
            "applied": False,
            "skip_reason": "no_inline_qty_unit_token",
        }
    if len(matches) > 1:
        return {
            "rule": "E1_inline_qty_unit",
            "applied": False,
            "skip_reason": "multiple_qty_unit_candidates",
            "candidate_count": len(matches),
        }

    match = matches[0]
    qty_val = _parse_qty(match.group("qty"))
    unit_val = match.group("unit").upper()

    if qty_val is None:
        return {
            "rule": "E1_inline_qty_unit",
            "applied": False,
            "skip_reason": "bad_qty_numeric",
            "matched_span": match.group(0),
        }
    if qty_val <= 0:
        return {
            "rule": "E1_inline_qty_unit",
            "applied": False,
            "skip_reason": "non_positive_qty",
            "matched_span": match.group(0),
        }

    # Consistency guard: if unit_price and amount are already present,
    # the enriched qty must reconcile with them within 1%. Otherwise the
    # row shape is ambiguous — refuse to enrich.
    up = row.get("unit_price")
    amt = row.get("amount")
    if up is not None and amt is not None:
        try:
            expected = qty_val * float(up)
            amount = float(amt)
            if amount <= 0 or expected <= 0:
                return {
                    "rule": "E1_inline_qty_unit",
                    "applied": False,
                    "skip_reason": "non_positive_arithmetic",
                    "matched_span": match.group(0),
                }
            drift = abs(expected - amount) / amount
            if drift > _CONSISTENCY_DRIFT:
                return {
                    "rule": "E1_inline_qty_unit",
                    "applied": False,
                    "skip_reason": "arithmetic_mismatch",
                    "matched_span": match.group(0),
                    "expected": round(expected, 4),
                    "amount": amount,
                    "drift": round(drift, 4),
                }
        except (TypeError, ValueError):
            return {
                "rule": "E1_inline_qty_unit",
                "applied": False,
                "skip_reason": "bad_numeric_types",
                "matched_span": match.group(0),
            }

    return {
        "rule": "E1_inline_qty_unit",
        "applied": True,
        "qty": qty_val,
        "unit": unit_val,
        "matched_span": match.group(0),
    }


def _find_whitelist_candidates(text: str) -> List[re.Match]:
    """Return all regex matches whose `unit` group is in KNOWN_UNITS."""
    out: List[re.Match] = []
    for m in _CANDIDATE_QTY_UNIT_RE.finditer(text):
        unit_token = m.group("unit").upper()
        if unit_token in KNOWN_UNITS:
            out.append(m)
    return out


def _parse_qty(raw: str) -> Optional[float]:
    try:
        return float(raw.replace(",", ""))
    except (TypeError, ValueError):
        return None


def _baseline_source(value: Any) -> str:
    """Baseline field_source marker for a pre-parse value."""
    if value is None:
        return SRC_NOT_PRESENT
    return SRC_PARSER_PRE_EXISTING
