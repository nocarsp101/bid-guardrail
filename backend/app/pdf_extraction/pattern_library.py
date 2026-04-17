"""
C27 — Deterministic pattern library expansion.

Registry-style catalogue of deterministic extraction/enrichment pattern
rules. Each rule carries:

    - rule_id:     a stable deterministic identifier
    - description: what the rule recognizes
    - motivation:  what observed gap the rule addresses (honest; says
                   "no real-corpus evidence" when the rule is added as
                   infrastructure for future corpora)
    - applicability: what inputs the rule looks at
    - skip_reasons:  closed vocabulary of why the rule may skip
    - false_positive_guard: what prevents the rule from overmatching

The registry itself produces no side effects — it exists so future
auditing layers (C26 coverage audit, C28 exception surfacing) can list
the deterministic rule set and trace which rules are available vs
actually fired in the pipeline.

Rules registered here are IMPLEMENTED in their respective modules
(e.g. C20 in `quote_enrichment.py`, C23 in `quote_table_extraction.py`).
This module does not duplicate rule logic; it records the contract.

Current C27 additions — narrow, explicitly guarded, infrastructure
level. Each is documented below with honest corpus evidence.

    Rule C27-U1 — Dotted unit-token normalization
        Recognises `<num> <DOTTED_UNIT>` tail-position tokens where the
        unit is a member of the closed dotted-variant whitelist
        (L.F., S.Y., C.Y., E.A., T.N., etc.). The dots are stripped
        during normalization so the result is identical to the plain
        whitelist match. Applied in the same place as C20 E1 / C23 E2.

        Motivation: industry vendor documents commonly abbreviate units
        as `L.F.` / `S.Y.` / etc. This rule recognises that convention
        deterministically without heuristic guessing.

        Real-corpus evidence from C26: ZERO observed instances in
        ipsi_quote.pdf or rasch_quote.pdf. The rule is added as
        infrastructure — measurable impact on the current real corpus
        is 0. It is proved on a controlled synthetic fixture and
        validated not to regress existing behavior.

        False-positive guard: the dotted unit must still match a closed
        whitelist after dot-stripping. Only 1–5 character alphabetic
        cores (with up to 4 optional internal dots) are scanned. The
        existing consistency guard (qty*unit_price≈amount within 1%)
        still applies.
"""
from __future__ import annotations

import re
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from .quote_enrichment import (
    KNOWN_UNITS, SRC_EXPLICIT_INLINE, SRC_NOT_PRESENT,
)

PATTERN_LIBRARY_VERSION = "pattern_library/v1"

# Canonical stripped whitelist — dotted and non-dotted unit tokens
# reduce to these identical canonical forms. We build the closed set
# from the existing KNOWN_UNITS so we cannot drift out of sync.
_STRIPPED_WHITELIST = frozenset({u for u in KNOWN_UNITS})

# Regex for a candidate `<qty> <UNIT>` token allowing the unit to carry
# internal dots (e.g., `L.F.`, `S.Y.`, `E.A.`). Alpha chars + up to 4
# dots + up to 5 alpha chars total after normalization.
# Example tail matches: `24 L.F.`, `550 S.Y.`, `8 T.N.`, `10 E.A.`
_C27_U1_TAIL_RE = re.compile(
    r'(?<![\w.])(?P<qty>\d{1,6}(?:,\d{3})*(?:\.\d+)?)'
    r'\s+(?P<unit>[A-Za-z](?:\.[A-Za-z]){1,4}\.?)\s*$',
)

# Same consistency guard tolerance as C20/C23.
_CONSISTENCY_DRIFT = 0.01

SRC_EXPLICIT_DOTTED_INLINE = "explicit_dotted_inline_qty_unit"


# ---------------------------------------------------------------------------
# Rule registry — introspectable catalogue
# ---------------------------------------------------------------------------

def list_registered_rules() -> List[Dict[str, Any]]:
    """Return the deterministic pattern-rule registry.

    Each entry is a stable dict consumed by C26/C28 layers that want to
    list available rules without importing their implementations.
    """
    return [
        {
            "rule_id": "C20-E1",
            "module": "quote_enrichment",
            "description": "Explicit inline <qty> <UNIT> token anywhere in description",
            "motivation": "Baseline enrichment for quotes that inline qty/unit",
            "real_corpus_evidence": "Zero hits on ipsi_quote.pdf and rasch_quote.pdf",
            "false_positive_guard": "Exactly one whitelist match required; "
                                    "consistency guard qty*unit_price≈amount within 1%",
        },
        {
            "rule_id": "C23-E2",
            "module": "quote_table_extraction",
            "description": "Header-gated tail-position <qty> <UNIT> token",
            "motivation": "Unlocks ambiguous inline cases when a table header "
                          "is explicitly present on the page",
            "real_corpus_evidence": "Zero hits on ipsi_quote.pdf and rasch_quote.pdf",
            "false_positive_guard": "Page must contain ≥2 closed-whitelist header "
                                    "tokens; tail-position only; same consistency guard",
        },
        {
            "rule_id": "C24-A1",
            "module": "quote_multi_row_aggregation",
            "description": "Two-line group: <line_ref> <alpha desc> + price-only line",
            "motivation": "Rescues explicit split-line row fragments",
            "real_corpus_evidence": "Zero hits on ipsi_quote.pdf and rasch_quote.pdf",
            "false_positive_guard": "Block must have exactly 2 lines; line 1 must "
                                    "match fragment line_ref regex with no dollars; "
                                    "line 2 must be price-only; row goes through the "
                                    "existing parser + validator before promotion",
        },
        {
            "rule_id": "C27-U1",
            "module": "pattern_library",
            "description": "Dotted unit token normalization (L.F., S.Y., C.Y., E.A., T.N. …)",
            "motivation": "Industry vendor documents commonly abbreviate units with "
                          "embedded dots; deterministic normalization strips dots and "
                          "checks the existing closed whitelist",
            "real_corpus_evidence": "ZERO observed instances in ipsi_quote.pdf or "
                                    "rasch_quote.pdf. Infrastructure rule; proved on "
                                    "a controlled synthetic fixture and validated not "
                                    "to regress real corpus.",
            "false_positive_guard": "Unit core (after dot-stripping) must be in the "
                                    "existing closed KNOWN_UNITS whitelist; "
                                    "tail-position only; same consistency guard as C20",
        },
    ]


# ---------------------------------------------------------------------------
# Rule C27-U1 — dotted unit normalization
# ---------------------------------------------------------------------------

def enrich_quote_rows_with_pattern_library(
    rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Apply pattern-library rules to rows that have already been through
    C20 E1 and C23 E2.

    Currently runs rule C27-U1 (dotted unit normalization). The rule only
    fires on rows whose qty and unit are both still None after the
    upstream passes — i.e., it never overrides any earlier rule.
    """
    out: List[Dict[str, Any]] = []
    for row in rows:
        new_row = deepcopy(row)
        if row.get("qty") is None and row.get("unit") is None:
            trace = _apply_rule_c27_u1(row)
            existing = new_row.get("enrichment_trace") or {"rules_attempted": []}
            rules = list(existing.get("rules_attempted") or [])
            rules.append(trace)
            new_row["enrichment_trace"] = {"rules_attempted": rules}
            if trace.get("applied"):
                new_row["qty"] = trace["qty"]
                new_row["unit"] = trace["unit"]
                field_sources = dict(new_row.get("field_sources") or {})
                field_sources["qty"] = SRC_EXPLICIT_DOTTED_INLINE
                field_sources["unit"] = SRC_EXPLICIT_DOTTED_INLINE
                new_row["field_sources"] = field_sources
                new_row["pattern_library_version"] = PATTERN_LIBRARY_VERSION
        else:
            # Thread the skip trace so downstream auditing can see C27 was
            # evaluated and deliberately did not fire.
            existing = new_row.get("enrichment_trace") or {"rules_attempted": []}
            rules = list(existing.get("rules_attempted") or [])
            rules.append({
                "rule": "C27-U1_dotted_unit_normalization",
                "applied": False,
                "skip_reason": "row_already_has_qty_or_unit",
            })
            new_row["enrichment_trace"] = {"rules_attempted": rules}
        out.append(new_row)
    return out


def _apply_rule_c27_u1(row: Dict[str, Any]) -> Dict[str, Any]:
    description = row.get("description") or ""
    match = _C27_U1_TAIL_RE.search(description)
    trace: Dict[str, Any] = {
        "rule": "C27-U1_dotted_unit_normalization",
        "applied": False,
    }
    if match is None:
        trace["skip_reason"] = "no_dotted_tail_qty_unit_token"
        return trace

    raw_unit = match.group("unit")
    normalized_unit = raw_unit.replace(".", "").upper()
    if normalized_unit not in _STRIPPED_WHITELIST:
        trace["skip_reason"] = "normalized_unit_not_in_whitelist"
        trace["matched_span"] = match.group(0)
        trace["normalized_unit"] = normalized_unit
        return trace

    qty = _parse_qty(match.group("qty"))
    if qty is None:
        trace["skip_reason"] = "bad_qty_numeric"
        trace["matched_span"] = match.group(0)
        return trace
    if qty <= 0:
        trace["skip_reason"] = "non_positive_qty"
        trace["matched_span"] = match.group(0)
        return trace

    # Consistency guard — identical to C20 E1 / C23 E2.
    up = row.get("unit_price")
    amt = row.get("amount")
    if up is not None and amt is not None:
        try:
            expected = qty * float(up)
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
    trace["qty"] = qty
    trace["unit"] = normalized_unit
    trace["matched_span"] = match.group(0)
    trace["raw_unit"] = raw_unit
    return trace


def _parse_qty(raw: str) -> Optional[float]:
    try:
        return float(raw.replace(",", ""))
    except (TypeError, ValueError):
        return None
