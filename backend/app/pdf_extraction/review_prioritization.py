"""
C21 — Deterministic review prioritization layer.

Assigns a closed-vocabulary priority class to every reconciliation /
findings row based on its existing discrepancy state. Produces
packet-level rollup counts and a top_issues_summary for office triage.

Hard rules:
    - Priority is a pure function of row state (discrepancy_class +
      comparison_status + mapping_outcome + packet_status).
    - No heuristics. No subjective weighting. No narrative output.
    - Blocked / unmapped / ambiguous rows never become lower priority
      than clean matches.
    - Discrepancy classes are NEVER overwritten — prioritization is a
      decoration, not a replacement.
    - Lower-priority issues are never hidden from the output — they are
      ordered after higher-priority rows.
    - Priority is explainable from explicit row state alone; a human
      reviewer can read priority_reason and verify the decision.
    - Never mutates its inputs.

Priority vocabulary (closed set):

    critical       — upstream blockers that invalidate comparison
                     (blocked_by_pairing, review_required_other)
    high           — concrete, reviewer-actionable gaps or conflicts
                     (unmapped_quote_row, ambiguous_mapping,
                      comparable_mismatch_unit,
                      comparable_mismatch_qty,
                      comparable_mismatch_multi)
    medium         — informational gaps that cannot be auto-resolved
                     but are less urgent than blocked / mismatch
                     (missing_quote_information,
                      missing_bid_information,
                      structurally_non_comparable)
    low            — clean comparable matches
                     (comparable_match)
    informational  — reserved; not currently produced by the classifier
                     for any deterministic state

Every class in the C18 vocabulary maps to exactly one priority class
via a fixed table. No tie-breaking. No dynamic weighting.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

PRIORITIZATION_VERSION = "review_prioritization/v1"

PRIORITY_CRITICAL = "critical"
PRIORITY_HIGH = "high"
PRIORITY_MEDIUM = "medium"
PRIORITY_LOW = "low"
PRIORITY_INFORMATIONAL = "informational"

# Deterministic ordering for sorting (lower = higher priority).
_PRIORITY_ORDER = {
    PRIORITY_CRITICAL: 0,
    PRIORITY_HIGH: 1,
    PRIORITY_MEDIUM: 2,
    PRIORITY_LOW: 3,
    PRIORITY_INFORMATIONAL: 4,
}

_ALL_PRIORITIES = (
    PRIORITY_CRITICAL,
    PRIORITY_HIGH,
    PRIORITY_MEDIUM,
    PRIORITY_LOW,
    PRIORITY_INFORMATIONAL,
)

# Closed mapping from discrepancy_class -> priority. Every row in the
# C18 vocabulary has exactly one priority. Missing/unknown classes fall
# through to review_required_other → critical.
_CLASS_TO_PRIORITY: Dict[str, Tuple[str, str]] = {
    # (priority_class, priority_reason)
    "blocked_by_pairing":
        (PRIORITY_CRITICAL, "upstream_pairing_rejected_comparison_invalid"),
    "review_required_other":
        (PRIORITY_CRITICAL, "unrecognized_row_state_requires_human_review"),

    "unmapped_quote_row":
        (PRIORITY_HIGH, "quote_row_has_no_deterministic_bid_mapping"),
    "ambiguous_mapping":
        (PRIORITY_HIGH, "quote_row_has_multiple_candidate_bid_items"),
    "comparable_mismatch_unit":
        (PRIORITY_HIGH, "mapped_row_unit_conflict_between_quote_and_bid"),
    "comparable_mismatch_qty":
        (PRIORITY_HIGH, "mapped_row_qty_conflict_between_quote_and_bid"),
    "comparable_mismatch_multi":
        (PRIORITY_HIGH, "mapped_row_unit_and_qty_conflict_between_quote_and_bid"),

    "missing_quote_information":
        (PRIORITY_MEDIUM, "mapped_row_has_missing_quote_qty_or_unit"),
    "missing_bid_information":
        (PRIORITY_MEDIUM, "mapped_row_has_missing_bid_qty_or_unit"),
    "structurally_non_comparable":
        (PRIORITY_MEDIUM, "mapped_row_has_no_overlapping_comparable_fields"),

    "comparable_match":
        (PRIORITY_LOW, "mapped_row_fully_reconciled_no_discrepancies"),
}

_BUCKET_KEYS = (
    "critical_count",
    "high_count",
    "medium_count",
    "low_count",
    "informational_count",
)

_PRIORITY_TO_BUCKET = {
    PRIORITY_CRITICAL: "critical_count",
    PRIORITY_HIGH: "high_count",
    PRIORITY_MEDIUM: "medium_count",
    PRIORITY_LOW: "low_count",
    PRIORITY_INFORMATIONAL: "informational_count",
}


# ---------------------------------------------------------------------------
# Public API — two entry points: findings-packet decorator + contract decorator.
# ---------------------------------------------------------------------------

def prioritize_findings_packet(findings_packet: Dict[str, Any]) -> Dict[str, Any]:
    """
    Decorate a C19 findings packet with deterministic priority fields.

    For every findings row, adds:
        - priority_class
        - priority_reason
        - priority_trace

    At the packet level, adds:
        - priority_summary (closed bucket counts + top_issues_summary)
        - prioritization_version

    Findings rows are reordered: higher-priority rows first, with stable
    ties broken by original packet ordering. Lower-priority rows are
    never hidden — they always appear after the higher ones.
    """
    out = deepcopy(findings_packet)
    rows = out.get("findings_rows") or []
    decorated = _decorate_rows(rows)
    out["findings_rows"] = _stable_sort_by_priority(decorated)
    out["priority_summary"] = _build_priority_summary(out["findings_rows"])
    out["prioritization_version"] = PRIORITIZATION_VERSION
    return out


def prioritize_classified_contract(classified_contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Decorate a C18 classified reconciliation contract with priority fields.

    Mirrors prioritize_findings_packet but operates on the smaller
    classified-contract shape. Useful for callers that only consume the
    reconciliation layer.
    """
    out = deepcopy(classified_contract)
    rows = out.get("reconciliation_rows") or []
    decorated = _decorate_rows(rows)
    out["reconciliation_rows"] = _stable_sort_by_priority(decorated)
    out["priority_summary"] = _build_priority_summary(out["reconciliation_rows"])
    out["prioritization_version"] = PRIORITIZATION_VERSION
    return out


# ---------------------------------------------------------------------------
# Row decoration
# ---------------------------------------------------------------------------

def assign_priority(row: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
    """
    Pure function: assign (priority_class, priority_reason, priority_trace)
    to a row based on its discrepancy_class + comparison_status fields.
    """
    discrepancy_class = row.get("discrepancy_class")
    mapping_outcome = row.get("mapping_outcome")
    comparison_status = row.get("comparison_status")

    if discrepancy_class in _CLASS_TO_PRIORITY:
        priority, reason = _CLASS_TO_PRIORITY[discrepancy_class]
    else:
        priority, reason = (PRIORITY_CRITICAL, "unknown_discrepancy_class_fail_safe")

    trace = {
        "input_discrepancy_class": discrepancy_class,
        "input_mapping_outcome": mapping_outcome,
        "input_comparison_status": comparison_status,
        "mapping_table_version": PRIORITIZATION_VERSION,
    }
    return priority, reason, trace


def _decorate_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return a new list of rows with priority fields added. Never mutates."""
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        priority, reason, trace = assign_priority(row)
        new_row = dict(row)
        new_row["priority_class"] = priority
        new_row["priority_reason"] = reason
        new_row["priority_trace"] = trace
        new_row["_original_order"] = idx
        out.append(new_row)
    return out


def _stable_sort_by_priority(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort by (priority_order, original index). Strip the helper key after."""
    rows_sorted = sorted(
        rows,
        key=lambda r: (_PRIORITY_ORDER.get(r["priority_class"], 99), r["_original_order"]),
    )
    # Strip the helper key so the row shape stays clean.
    for r in rows_sorted:
        r.pop("_original_order", None)
    return rows_sorted


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def _build_priority_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts = {k: 0 for k in _BUCKET_KEYS}
    for r in rows:
        priority = r.get("priority_class")
        bucket = _PRIORITY_TO_BUCKET.get(priority)
        if bucket is None:
            counts["critical_count"] += 1
            continue
        counts[bucket] += 1

    top_issues = _build_top_issues(rows)
    return {
        "rows_total": len(rows),
        **counts,
        "top_issues_summary": top_issues,
    }


def _build_top_issues(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deterministic top-issues slice: up to the first 5 critical/high rows,
    in stable priority order. Each entry carries only structured fields —
    no narrative."""
    top: List[Dict[str, Any]] = []
    for r in rows:
        if r.get("priority_class") in (PRIORITY_CRITICAL, PRIORITY_HIGH):
            top.append({
                "normalized_row_id": r.get("normalized_row_id"),
                "priority_class": r.get("priority_class"),
                "priority_reason": r.get("priority_reason"),
                "discrepancy_class": r.get("discrepancy_class"),
                "comparison_status": r.get("comparison_status"),
            })
        if len(top) >= 5:
            break
    return top
