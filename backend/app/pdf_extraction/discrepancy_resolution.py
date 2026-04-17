"""
C31 — Discrepancy resolution framework.

Consumes a C30-augmented reconciliation contract and produces a
resolution-ready structured output per row. Each row receives a closed
`resolution_category`, a templated `resolution_reason`, a deterministic
`resolution_priority`, and a compact `resolution_trace` summarizing the
exact data that supports the category.

NOTHING is auto-resolved. The resolution layer classifies and packages
discrepancies so an office reviewer or engineer can act on them. It
never rewrites a disputed value, never chooses a winner between
conflicting sources, never summarizes as narrative prose.

Closed resolution-category vocabulary:

    blocked_pairing_resolution_required
    unmapped_scope_review_required
    ambiguous_mapping_review_required
    source_conflict_review_required
    quantity_discrepancy_review_required
    unit_discrepancy_review_required
    non_comparable_missing_quote_source
    non_comparable_missing_external_source
    clean_match_no_resolution_needed
    review_required_other

Closed resolution_priority vocabulary (mirror of C21 priority):
    critical | high | medium | low | informational

Hard rules:
    - Pure function of row state. Same input always produces same output.
    - Blocked / unmapped / ambiguous rows never collapse into "clean".
    - Conflicts are always preserved as review_required.
    - Categories never overlap: exactly one fires per row.
    - Resolution layer never modifies upstream contract fields.
    - Never narrativizes: reasons are fixed templated strings.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

RESOLUTION_VERSION = "discrepancy_resolution/v1"

# Resolution statuses (closed set).
RESOLUTION_STATUS_OPEN = "open"
RESOLUTION_STATUS_REVIEW_REQUIRED = "review_required"
RESOLUTION_STATUS_NOT_APPLICABLE = "not_applicable"

# Resolution categories (closed set).
CAT_BLOCKED_PAIRING = "blocked_pairing_resolution_required"
CAT_UNMAPPED_SCOPE = "unmapped_scope_review_required"
CAT_AMBIGUOUS_MAPPING = "ambiguous_mapping_review_required"
CAT_SOURCE_CONFLICT = "source_conflict_review_required"
CAT_QTY_DISCREPANCY = "quantity_discrepancy_review_required"
CAT_UNIT_DISCREPANCY = "unit_discrepancy_review_required"
CAT_NON_COMPARABLE_MISSING_QUOTE_SOURCE = "non_comparable_missing_quote_source"
CAT_NON_COMPARABLE_MISSING_EXTERNAL_SOURCE = "non_comparable_missing_external_source"
CAT_CLEAN_MATCH = "clean_match_no_resolution_needed"
CAT_REVIEW_REQUIRED_OTHER = "review_required_other"

_ALL_CATEGORIES = (
    CAT_BLOCKED_PAIRING, CAT_UNMAPPED_SCOPE, CAT_AMBIGUOUS_MAPPING,
    CAT_SOURCE_CONFLICT, CAT_QTY_DISCREPANCY, CAT_UNIT_DISCREPANCY,
    CAT_NON_COMPARABLE_MISSING_QUOTE_SOURCE,
    CAT_NON_COMPARABLE_MISSING_EXTERNAL_SOURCE,
    CAT_CLEAN_MATCH, CAT_REVIEW_REQUIRED_OTHER,
)

# Closed vocabulary → priority.
_CATEGORY_PRIORITY: Dict[str, str] = {
    CAT_BLOCKED_PAIRING: "critical",
    CAT_UNMAPPED_SCOPE: "high",
    CAT_AMBIGUOUS_MAPPING: "high",
    CAT_SOURCE_CONFLICT: "high",
    CAT_QTY_DISCREPANCY: "high",
    CAT_UNIT_DISCREPANCY: "high",
    CAT_NON_COMPARABLE_MISSING_QUOTE_SOURCE: "medium",
    CAT_NON_COMPARABLE_MISSING_EXTERNAL_SOURCE: "medium",
    CAT_CLEAN_MATCH: "low",
    CAT_REVIEW_REQUIRED_OTHER: "critical",
}

# Templated resolution reasons (fixed strings).
_CATEGORY_REASON: Dict[str, str] = {
    CAT_BLOCKED_PAIRING:
        "pairing_rejected_comparison_cannot_run",
    CAT_UNMAPPED_SCOPE:
        "quote_row_unmapped_from_bid_schedule",
    CAT_AMBIGUOUS_MAPPING:
        "quote_row_mapping_ambiguous_between_candidates",
    CAT_SOURCE_CONFLICT:
        "external_quantity_sources_disagree_no_basis_selected",
    CAT_QTY_DISCREPANCY:
        "quote_qty_disagrees_with_effective_basis",
    CAT_UNIT_DISCREPANCY:
        "quote_unit_disagrees_with_effective_basis",
    CAT_NON_COMPARABLE_MISSING_QUOTE_SOURCE:
        "mapped_row_has_no_quote_qty_unit_and_no_external_source",
    CAT_NON_COMPARABLE_MISSING_EXTERNAL_SOURCE:
        "mapped_row_has_no_external_source_and_partial_quote_values",
    CAT_CLEAN_MATCH:
        "comparison_basis_matches_effective_values",
    CAT_REVIEW_REQUIRED_OTHER:
        "unrecognized_row_state_requires_human_review",
}

# Numeric tolerance for qty comparison (mirror of C16/C30).
_QTY_TOLERANCE = 0.005


def build_resolution(augmented_contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a resolution-ready view from a C30-augmented contract.

    Returns a dict shaped:

        {
            "resolution_version": "...",
            "resolution_status": "open" | "review_required" | "not_applicable",
            "resolution_summary": {
                "rows_total": N,
                "category_counts": { CAT: n, ... },
                "priority_counts": { "critical": n, "high": n, ... },
            },
            "resolution_rows": [
                {
                    "normalized_row_id": ...,
                    "resolution_category": ...,
                    "resolution_priority": ...,
                    "resolution_reason": ...,
                    "quote_values": { ... },
                    "external_sources": [ ... ],
                    "comparison_basis": { ... },
                    "comparison_result": { ... },
                    "resolution_trace": { ... },
                },
                ...
            ],
        }

    Never mutates the input. The resolution_rows preserve every input
    field from the contract row so nothing is hidden.
    """
    rows = (augmented_contract or {}).get("reconciliation_rows") or []
    packet_status = augmented_contract.get("packet_status")

    resolution_rows: List[Dict[str, Any]] = []
    for row in rows:
        resolution_rows.append(_resolve_row(row))

    category_counts: Dict[str, int] = {c: 0 for c in _ALL_CATEGORIES}
    priority_counts: Dict[str, int] = {
        "critical": 0, "high": 0, "medium": 0, "low": 0, "informational": 0,
    }
    for rr in resolution_rows:
        cat = rr["resolution_category"]
        category_counts[cat] = category_counts.get(cat, 0) + 1
        prio = rr["resolution_priority"]
        if prio in priority_counts:
            priority_counts[prio] += 1

    resolution_status = _derive_resolution_status(packet_status, category_counts)

    return {
        "resolution_version": RESOLUTION_VERSION,
        "resolution_status": resolution_status,
        "packet_status": packet_status,
        "pairing_status": augmented_contract.get("pairing_status"),
        "contract_version": augmented_contract.get("contract_version"),
        "augmentation_rules_version": augmented_contract.get("augmentation_rules_version"),
        "resolution_summary": {
            "rows_total": len(resolution_rows),
            "category_counts": category_counts,
            "priority_counts": priority_counts,
        },
        "resolution_rows": resolution_rows,
    }


# ---------------------------------------------------------------------------
# Per-row resolution
# ---------------------------------------------------------------------------

def _resolve_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Classify one augmented contract row into a resolution record."""
    category, comparison_result, trace = _classify(row)
    priority = _CATEGORY_PRIORITY[category]
    reason = _CATEGORY_REASON[category]

    result = {
        "normalized_row_id": row.get("normalized_row_id"),
        "resolution_category": category,
        "resolution_priority": priority,
        "resolution_reason": reason,
        "quote_values": deepcopy(row.get("quote_values") or {}),
        "external_sources": deepcopy(row.get("external_quantity_sources") or []),
        "comparison_basis": {
            "basis": row.get("comparison_basis"),
            "augmentation_reason": row.get("augmentation_reason"),
            "effective_comparison_values": deepcopy(row.get("effective_comparison_values")),
            "augmentation_flags": list(row.get("augmentation_flags") or []),
            "source_conflict_status": row.get("source_conflict_status"),
        },
        "comparison_result": comparison_result,
        "resolution_trace": trace,
    }
    # C44 provenance propagation.
    for pkey in ("row_origin", "source_provenance", "manual_entry_ref", "source_block_ref"):
        val = row.get(pkey)
        if val is not None:
            result[pkey] = deepcopy(val)
    return result


def _classify(row: Dict[str, Any]) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """Assign exactly one category to a row based on C30 augmentation state.

    Returns (category, comparison_result, resolution_trace).
    """
    mapping_outcome = row.get("mapping_outcome")
    comparison_status = row.get("comparison_status")
    basis = row.get("comparison_basis")

    trace: Dict[str, Any] = {
        "inputs": {
            "mapping_outcome": mapping_outcome,
            "comparison_status": comparison_status,
            "comparison_basis": basis,
            "source_conflict_status": row.get("source_conflict_status"),
            "quote_qty_present": (row.get("quote_values") or {}).get("qty") is not None,
            "quote_unit_present": (row.get("quote_values") or {}).get("unit") is not None,
            "external_source_count": len(row.get("external_quantity_sources") or []),
        },
        "rule_fired": None,
    }

    # Rule 1 — structural upstream blockers.
    if mapping_outcome == "blocked" or comparison_status == "blocked":
        trace["rule_fired"] = "RR1_blocked_pairing"
        return CAT_BLOCKED_PAIRING, _empty_comparison_result(), trace
    if mapping_outcome == "unmapped":
        trace["rule_fired"] = "RR2_unmapped"
        return CAT_UNMAPPED_SCOPE, _empty_comparison_result(), trace
    if mapping_outcome == "ambiguous":
        trace["rule_fired"] = "RR3_ambiguous"
        return CAT_AMBIGUOUS_MAPPING, _empty_comparison_result(), trace

    # Rule 2 — conflict from C30 augmentation.
    if basis == "conflicted_sources":
        trace["rule_fired"] = "RR4_source_conflict"
        return CAT_SOURCE_CONFLICT, _empty_comparison_result(), trace

    # Rule 3 — no comparable basis at all.
    if basis == "unavailable":
        external_count = len(row.get("external_quantity_sources") or [])
        if external_count == 0:
            trace["rule_fired"] = "RR5_missing_quote_source"
            return CAT_NON_COMPARABLE_MISSING_QUOTE_SOURCE, _empty_comparison_result(), trace
        trace["rule_fired"] = "RR6_missing_external_source"
        return CAT_NON_COMPARABLE_MISSING_EXTERNAL_SOURCE, _empty_comparison_result(), trace

    # Rule 4 — comparable basis exists. Compute qty/unit agreement.
    quote_values = row.get("quote_values") or {}
    effective = row.get("effective_comparison_values") or {}
    quote_qty = quote_values.get("qty")
    quote_unit = quote_values.get("unit")
    eff_qty = effective.get("qty") if isinstance(effective, dict) else None
    eff_unit = effective.get("unit") if isinstance(effective, dict) else None

    qty_match, unit_match = _compare_values(quote_qty, quote_unit, eff_qty, eff_unit, basis)

    comparison_result = {
        "qty_match": qty_match,
        "unit_match": unit_match,
        "quote_qty": quote_qty,
        "quote_unit": quote_unit,
        "effective_qty": eff_qty,
        "effective_unit": eff_unit,
    }

    # Decide category based on match flags.
    if qty_match is None and unit_match is None:
        # Neither side had comparable values — fall back to missing quote.
        trace["rule_fired"] = "RR7_no_values_compared"
        return CAT_NON_COMPARABLE_MISSING_QUOTE_SOURCE, comparison_result, trace

    mismatch_unit = unit_match is False
    mismatch_qty = qty_match is False

    if mismatch_unit and mismatch_qty:
        trace["rule_fired"] = "RR8_unit_and_qty_discrepancy"
        # When both disagree we emit the qty discrepancy bucket first;
        # the augmentation_flags already carry both flags for trace.
        return CAT_QTY_DISCREPANCY, comparison_result, trace
    if mismatch_unit:
        trace["rule_fired"] = "RR9_unit_discrepancy"
        return CAT_UNIT_DISCREPANCY, comparison_result, trace
    if mismatch_qty:
        trace["rule_fired"] = "RR10_qty_discrepancy"
        return CAT_QTY_DISCREPANCY, comparison_result, trace

    # Everything that was comparable matched — clean match.
    trace["rule_fired"] = "RR11_clean_match"
    return CAT_CLEAN_MATCH, comparison_result, trace


def _compare_values(
    quote_qty: Any,
    quote_unit: Any,
    eff_qty: Any,
    eff_unit: Any,
    basis: Optional[str],
) -> Tuple[Optional[bool], Optional[bool]]:
    """
    Return (qty_match, unit_match). Each is:
        True  — both sides present and equal (within tolerance)
        False — both sides present and unequal
        None  — at least one side missing; no comparison possible
    """
    # For quote_native and dot_augmented, the effective side drives
    # comparison. For quote_native_with_external_reference, compare quote
    # against the first external source (the basis is the quote; external
    # acts as a reference check).
    unit_match = None
    if quote_unit is not None and eff_unit is not None:
        unit_match = _canon(quote_unit) == _canon(eff_unit)

    qty_match = None
    if quote_qty is not None and eff_qty is not None:
        qty_match = _qty_equal(quote_qty, eff_qty)

    return qty_match, unit_match


def _canon(u: Any) -> str:
    return str(u).strip().upper()


def _qty_equal(a: Any, b: Any) -> bool:
    try:
        fa, fb = float(a), float(b)
    except (TypeError, ValueError):
        return False
    if fa == fb:
        return True
    if fb == 0:
        return fa == 0
    return abs(fa - fb) / abs(fb) <= _QTY_TOLERANCE


def _empty_comparison_result() -> Dict[str, Any]:
    return {
        "qty_match": None,
        "unit_match": None,
        "quote_qty": None,
        "quote_unit": None,
        "effective_qty": None,
        "effective_unit": None,
    }


def _derive_resolution_status(
    packet_status: Optional[str],
    category_counts: Dict[str, int],
) -> str:
    """Derive the top-level resolution_status.

    - `open` / `review_required` when any row needs action
    - `not_applicable` when nothing can be compared (blocked everywhere)
    """
    if packet_status == "blocked":
        return RESOLUTION_STATUS_REVIEW_REQUIRED
    review_buckets = (
        CAT_UNMAPPED_SCOPE, CAT_AMBIGUOUS_MAPPING, CAT_SOURCE_CONFLICT,
        CAT_QTY_DISCREPANCY, CAT_UNIT_DISCREPANCY, CAT_REVIEW_REQUIRED_OTHER,
        CAT_NON_COMPARABLE_MISSING_QUOTE_SOURCE,
        CAT_NON_COMPARABLE_MISSING_EXTERNAL_SOURCE,
    )
    review_needed = sum(category_counts.get(c, 0) for c in review_buckets)
    if review_needed > 0:
        return RESOLUTION_STATUS_REVIEW_REQUIRED
    if category_counts.get(CAT_CLEAN_MATCH, 0) > 0:
        return RESOLUTION_STATUS_OPEN
    return RESOLUTION_STATUS_NOT_APPLICABLE
