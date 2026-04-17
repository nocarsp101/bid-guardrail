"""
C18 — Deterministic discrepancy classification + office review summaries.

Consumes the C17 reconciliation_contract output (the hardened contract) and
assigns every reconciliation row a deterministic `discrepancy_class` from a
closed, explicit vocabulary. Then produces office-facing summary buckets
(counts) that the office can review without needing to interpret comparison
flags or mapping outcomes directly.

Hard rules:
    - Classes are a closed vocabulary. No heuristic. No confidence.
    - No business judgment. No claims language. No "maybe" softening.
    - Blocked / unmapped / ambiguous / comparable categories stay distinct.
    - Every row gets exactly one class. Classification is a pure function
      of the row state (mapping_outcome + comparison_status + comparison
      flags).
    - Never mutates its input — produces a new enriched contract.
    - Classification preserves full traceability: each row carries the
      deterministic decision path in `classification_trace`.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Tuple

# ---------------------------------------------------------------------------
# Deterministic discrepancy class vocabulary (closed set).
# ---------------------------------------------------------------------------

# Structural / upstream-blocking states
CLASS_BLOCKED_BY_PAIRING = "blocked_by_pairing"
CLASS_UNMAPPED_QUOTE_ROW = "unmapped_quote_row"
CLASS_AMBIGUOUS_MAPPING = "ambiguous_mapping"

# Rows that were mapped but could not be compared
CLASS_MISSING_QUOTE_INFORMATION = "missing_quote_information"
CLASS_MISSING_BID_INFORMATION = "missing_bid_information"
CLASS_STRUCTURALLY_NON_COMPARABLE = "structurally_non_comparable"

# Rows that were compared
CLASS_COMPARABLE_MATCH = "comparable_match"
CLASS_COMPARABLE_MISMATCH_UNIT = "comparable_mismatch_unit"
CLASS_COMPARABLE_MISMATCH_QTY = "comparable_mismatch_qty"
CLASS_COMPARABLE_MISMATCH_MULTI = "comparable_mismatch_multi"

# Catch-all
CLASS_REVIEW_REQUIRED_OTHER = "review_required_other"

_ALL_CLASSES = (
    CLASS_BLOCKED_BY_PAIRING,
    CLASS_UNMAPPED_QUOTE_ROW,
    CLASS_AMBIGUOUS_MAPPING,
    CLASS_MISSING_QUOTE_INFORMATION,
    CLASS_MISSING_BID_INFORMATION,
    CLASS_STRUCTURALLY_NON_COMPARABLE,
    CLASS_COMPARABLE_MATCH,
    CLASS_COMPARABLE_MISMATCH_UNIT,
    CLASS_COMPARABLE_MISMATCH_QTY,
    CLASS_COMPARABLE_MISMATCH_MULTI,
    CLASS_REVIEW_REQUIRED_OTHER,
)

# Office-facing summary bucket keys.
BUCKET_KEYS = (
    "blocked_count",
    "unmapped_count",
    "ambiguous_count",
    "missing_quote_info_count",
    "missing_bid_info_count",
    "structurally_non_comparable_count",
    "comparable_match_count",
    "comparable_mismatch_unit_count",
    "comparable_mismatch_qty_count",
    "comparable_mismatch_multi_count",
    "review_required_other_count",
)

_CLASS_TO_BUCKET = {
    CLASS_BLOCKED_BY_PAIRING: "blocked_count",
    CLASS_UNMAPPED_QUOTE_ROW: "unmapped_count",
    CLASS_AMBIGUOUS_MAPPING: "ambiguous_count",
    CLASS_MISSING_QUOTE_INFORMATION: "missing_quote_info_count",
    CLASS_MISSING_BID_INFORMATION: "missing_bid_info_count",
    CLASS_STRUCTURALLY_NON_COMPARABLE: "structurally_non_comparable_count",
    CLASS_COMPARABLE_MATCH: "comparable_match_count",
    CLASS_COMPARABLE_MISMATCH_UNIT: "comparable_mismatch_unit_count",
    CLASS_COMPARABLE_MISMATCH_QTY: "comparable_mismatch_qty_count",
    CLASS_COMPARABLE_MISMATCH_MULTI: "comparable_mismatch_multi_count",
    CLASS_REVIEW_REQUIRED_OTHER: "review_required_other_count",
}

CLASSIFICATION_VERSION = "discrepancy_classification/v1"


def classify_contract(contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Classify every reconciliation row in a C17 contract.

    Returns a NEW contract dict (deep-copied) with:
      - every reconciliation_row enriched with a deterministic
        `discrepancy_class` and `classification_trace`
      - a top-level `office_review_summary` containing deterministic
        bucket counts
      - a `classification_version`
    """
    out = deepcopy(contract)
    rows = out.get("reconciliation_rows", []) or []

    enriched: List[Dict[str, Any]] = []
    for row in rows:
        cls, trace = classify_row(row)
        # Stable key ordering: insert after comparison_trace.
        new_row = dict(row)
        new_row["discrepancy_class"] = cls
        new_row["classification_trace"] = trace
        enriched.append(new_row)

    out["reconciliation_rows"] = enriched
    out["office_review_summary"] = _build_office_summary(enriched)
    out["classification_version"] = CLASSIFICATION_VERSION
    return out


def classify_row(row: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Return (discrepancy_class, classification_trace) for a contract row.

    Pure function of the row state. Order of evaluation is fixed.
    """
    mapping_outcome = row.get("mapping_outcome")
    comparison_status = row.get("comparison_status")
    flags = list(row.get("comparison_flags") or [])
    non_comparable_reason = row.get("non_comparable_reason")
    bid_values = row.get("bid_values")

    trace: Dict[str, Any] = {
        "inputs": {
            "mapping_outcome": mapping_outcome,
            "comparison_status": comparison_status,
            "non_comparable_reason": non_comparable_reason,
            "has_conflict_flags": _collect_conflict_flags(flags),
            "has_missing_quote_flags": _collect_missing_quote_flags(flags),
            "has_missing_bid_flags": _collect_missing_bid_flags(flags),
        },
        "rule_fired": None,
    }

    # Rule 1 — structural upstream blockers (highest priority).
    if comparison_status == "blocked":
        trace["rule_fired"] = "R1_blocked_comparison_status"
        return CLASS_BLOCKED_BY_PAIRING, trace

    if mapping_outcome == "blocked":
        trace["rule_fired"] = "R1b_blocked_mapping_outcome"
        return CLASS_BLOCKED_BY_PAIRING, trace

    if mapping_outcome == "unmapped":
        trace["rule_fired"] = "R2_unmapped"
        return CLASS_UNMAPPED_QUOTE_ROW, trace

    if mapping_outcome == "ambiguous":
        trace["rule_fired"] = "R3_ambiguous"
        return CLASS_AMBIGUOUS_MAPPING, trace

    # Rule 4 — mapped rows that were not comparable.
    if comparison_status == "non_comparable":
        # Missing quote info dominates: if the quote side is missing *any*
        # of the comparable fields, the office sees this as a quote gap.
        # Bid missing is structural (DOT bids never carry amount).
        missing_q_qty = "missing_quote_qty" in flags
        missing_q_unit = "missing_quote_unit" in flags
        missing_b_qty = "missing_bid_qty" in flags
        missing_b_unit = "missing_bid_unit" in flags

        if missing_q_qty or missing_q_unit:
            trace["rule_fired"] = "R4a_non_comparable_missing_quote_info"
            return CLASS_MISSING_QUOTE_INFORMATION, trace
        if missing_b_qty or missing_b_unit:
            trace["rule_fired"] = "R4b_non_comparable_missing_bid_info"
            return CLASS_MISSING_BID_INFORMATION, trace
        trace["rule_fired"] = "R4c_non_comparable_structural"
        return CLASS_STRUCTURALLY_NON_COMPARABLE, trace

    # Rule 5 — mapped + comparable rows.
    if comparison_status == "match":
        trace["rule_fired"] = "R5_comparable_match"
        return CLASS_COMPARABLE_MATCH, trace

    if comparison_status == "mismatch":
        unit_conflict = "unit_conflict" in flags
        qty_conflict = "qty_conflict" in flags
        if unit_conflict and qty_conflict:
            trace["rule_fired"] = "R6c_comparable_mismatch_multi"
            return CLASS_COMPARABLE_MISMATCH_MULTI, trace
        if unit_conflict:
            trace["rule_fired"] = "R6a_comparable_mismatch_unit"
            return CLASS_COMPARABLE_MISMATCH_UNIT, trace
        if qty_conflict:
            trace["rule_fired"] = "R6b_comparable_mismatch_qty"
            return CLASS_COMPARABLE_MISMATCH_QTY, trace
        # Mismatch without a known conflict flag: fail safe, send to review.
        trace["rule_fired"] = "R6d_comparable_mismatch_unknown"
        return CLASS_REVIEW_REQUIRED_OTHER, trace

    # Default — never silently succeed.
    trace["rule_fired"] = "R7_unrecognized_state"
    return CLASS_REVIEW_REQUIRED_OTHER, trace


# ---------------------------------------------------------------------------
# Office summary
# ---------------------------------------------------------------------------

def _build_office_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Count rows by discrepancy class into the office-facing buckets."""
    counts = {k: 0 for k in BUCKET_KEYS}
    class_counts = {c: 0 for c in _ALL_CLASSES}
    for r in rows:
        cls = r.get("discrepancy_class")
        bucket = _CLASS_TO_BUCKET.get(cls)
        if bucket is None:
            # Unknown class — bump review_required_other so nothing hides.
            counts["review_required_other_count"] += 1
            continue
        counts[bucket] += 1
        class_counts[cls] = class_counts.get(cls, 0) + 1

    total = len(rows)
    # Deterministic, machine-readable, no narrative text.
    return {
        "rows_total": total,
        **counts,
        "class_counts": class_counts,
    }


# ---------------------------------------------------------------------------
# Flag helpers — deterministic, never guess.
# ---------------------------------------------------------------------------

_CONFLICT_FLAGS = ("unit_conflict", "qty_conflict")
_MISSING_QUOTE_FLAGS = (
    "missing_quote_qty",
    "missing_quote_unit",
    "missing_quote_unit_price",
    "missing_quote_amount",
)
_MISSING_BID_FLAGS = (
    "missing_bid_qty",
    "missing_bid_unit",
    "missing_bid_amount",
)


def _collect_conflict_flags(flags: List[str]) -> List[str]:
    return [f for f in _CONFLICT_FLAGS if f in flags]


def _collect_missing_quote_flags(flags: List[str]) -> List[str]:
    return [f for f in _MISSING_QUOTE_FLAGS if f in flags]


def _collect_missing_bid_flags(flags: List[str]) -> List[str]:
    return [f for f in _MISSING_BID_FLAGS if f in flags]
