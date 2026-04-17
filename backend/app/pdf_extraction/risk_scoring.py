"""
C49 — Bid risk / exposure scoring.

Deterministic, explainable risk scoring from review readiness, scope
interpretation, comparability gaps, unresolved evidence, manual
dependence, and conflict signals.

No black-box scoring. No fuzzy AI confidence. Every factor is an
explicit deterministic check with a closed vocabulary.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

RISK_SCORING_VERSION = "risk_scoring/v1"

RISK_CRITICAL = "critical"
RISK_HIGH = "high"
RISK_MEDIUM = "medium"
RISK_LOW = "low"

FACTOR_BLOCKED_PAIRING = "blocked_pairing"
FACTOR_UNRESOLVED_EVIDENCE = "unresolved_evidence_blocks"
FACTOR_UNAPPROVED_MANUAL = "unapproved_manual_interpretations"
FACTOR_NO_MANUAL_STARTED = "manual_interpretation_not_started"
FACTOR_LOW_COMPARABILITY = "low_comparability_coverage"
FACTOR_SOURCE_CONFLICTS = "source_quantity_conflicts"
FACTOR_UNMAPPED_SCOPE = "unmapped_quote_scope"
FACTOR_AMBIGUOUS_SCOPE = "ambiguous_scope_items"
FACTOR_HIGH_MANUAL_DEPENDENCE = "high_manual_dependence"
FACTOR_MISSING_QUOTE_FIELDS = "missing_quote_quantity_fields"
FACTOR_NO_EXTERNAL_SOURCES = "no_external_quantity_sources"
FACTOR_UNADDRESSED_BID_ITEMS = "unaddressed_bid_items"

_FACTOR_SEVERITY: Dict[str, str] = {
    FACTOR_BLOCKED_PAIRING: RISK_CRITICAL,
    FACTOR_UNRESOLVED_EVIDENCE: RISK_HIGH,
    FACTOR_UNAPPROVED_MANUAL: RISK_HIGH,
    FACTOR_NO_MANUAL_STARTED: RISK_HIGH,
    FACTOR_LOW_COMPARABILITY: RISK_HIGH,
    FACTOR_SOURCE_CONFLICTS: RISK_HIGH,
    FACTOR_UNMAPPED_SCOPE: RISK_MEDIUM,
    FACTOR_AMBIGUOUS_SCOPE: RISK_MEDIUM,
    FACTOR_HIGH_MANUAL_DEPENDENCE: RISK_MEDIUM,
    FACTOR_MISSING_QUOTE_FIELDS: RISK_MEDIUM,
    FACTOR_NO_EXTERNAL_SOURCES: RISK_MEDIUM,
    FACTOR_UNADDRESSED_BID_ITEMS: RISK_MEDIUM,
}


def score_bid_risk(
    review_workflow: Optional[Dict[str, Any]] = None,
    scope_interpretation: Optional[Dict[str, Any]] = None,
    resolution_output: Optional[Dict[str, Any]] = None,
    source_management: Optional[Dict[str, Any]] = None,
    handwritten_review: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a deterministic risk score from governed pipeline outputs.

    Every risk factor is an explicit check. The overall risk level is
    the maximum severity of any triggered factor.
    """
    factors: List[Dict[str, Any]] = []

    _check_pairing(factors, resolution_output)
    _check_evidence(factors, review_workflow, handwritten_review)
    _check_approval(factors, review_workflow)
    _check_comparability(factors, resolution_output)
    _check_source_conflicts(factors, source_management)
    _check_scope(factors, scope_interpretation)
    _check_manual_dependence(factors, review_workflow)
    _check_missing_fields(factors, resolution_output)
    _check_external_sources(factors, source_management)
    _check_unaddressed_bid(factors, scope_interpretation)

    blocking = [f for f in factors if f["severity"] == RISK_CRITICAL]
    warnings = [f for f in factors if f["severity"] in (RISK_HIGH, RISK_MEDIUM)]
    informational = [f for f in factors if f["severity"] == RISK_LOW]

    overall = _derive_overall_risk(factors)

    recommended_actions = _build_recommended_actions(factors)

    return {
        "risk_scoring_version": RISK_SCORING_VERSION,
        "overall_risk_level": overall,
        "risk_factors": factors,
        "blocking_risks": blocking,
        "warning_risks": warnings,
        "informational_risks": informational,
        "risk_summary": {
            "total_factors": len(factors),
            "critical_count": sum(1 for f in factors if f["severity"] == RISK_CRITICAL),
            "high_count": sum(1 for f in factors if f["severity"] == RISK_HIGH),
            "medium_count": sum(1 for f in factors if f["severity"] == RISK_MEDIUM),
            "low_count": sum(1 for f in factors if f["severity"] == RISK_LOW),
        },
        "recommended_actions": recommended_actions,
    }


# ---------------------------------------------------------------------------
# Factor checks — each is a pure, deterministic check
# ---------------------------------------------------------------------------

def _add(factors: List, factor_id: str, detail: str, count: int = 1) -> None:
    factors.append({
        "factor_id": factor_id,
        "severity": _FACTOR_SEVERITY.get(factor_id, RISK_MEDIUM),
        "detail": detail,
        "count": count,
    })


def _check_pairing(factors, resolution):
    if not resolution:
        return
    if resolution.get("packet_status") == "blocked":
        _add(factors, FACTOR_BLOCKED_PAIRING, "pairing_rejected_packet_blocked")


def _check_evidence(factors, review_workflow, handwritten_review):
    rw = review_workflow or {}
    hr = handwritten_review or {}
    progress = rw.get("progress_metrics") or {}
    ev = progress.get("evidence_coverage_ratio") or {}
    unresolved = int((hr.get("unresolved_evidence_summary") or {}).get("unresolved_block_count") or 0)
    if unresolved > 0:
        _add(factors, FACTOR_UNRESOLVED_EVIDENCE,
             f"{unresolved}_evidence_blocks_without_manual_interpretation", unresolved)
    readiness = rw.get("readiness_status")
    if readiness == "not_started":
        _add(factors, FACTOR_NO_MANUAL_STARTED,
             "document_requires_manual_interpretation_but_none_entered")


def _check_approval(factors, review_workflow):
    rw = review_workflow or {}
    summary = rw.get("review_summary") or {}
    unapproved = int(summary.get("unapproved_count") or 0)
    if unapproved > 0:
        _add(factors, FACTOR_UNAPPROVED_MANUAL,
             f"{unapproved}_manual_entries_awaiting_approval", unapproved)


def _check_comparability(factors, resolution):
    if not resolution:
        return
    res_summary = resolution.get("resolution_summary") or {}
    cats = res_summary.get("category_counts") or {}
    non_comp = (int(cats.get("non_comparable_missing_quote_source") or 0) +
                int(cats.get("non_comparable_missing_external_source") or 0))
    total = int(res_summary.get("rows_total") or 0)
    if total > 0 and non_comp > 0 and (non_comp / total) >= 0.5:
        _add(factors, FACTOR_LOW_COMPARABILITY,
             f"{non_comp}_of_{total}_rows_non_comparable", non_comp)


def _check_source_conflicts(factors, source_management):
    if not source_management:
        return
    sm_summary = (source_management.get("source_management_summary") or
                  source_management.get("source_management") or {})
    conflicts = int(sm_summary.get("rows_with_conflicted_sources") or 0)
    if conflicts > 0:
        _add(factors, FACTOR_SOURCE_CONFLICTS,
             f"{conflicts}_rows_have_conflicting_quantity_sources", conflicts)


def _check_scope(factors, scope_interpretation):
    if not scope_interpretation:
        return
    summary = scope_interpretation.get("scope_summary") or {}
    ambiguous = int(summary.get("ambiguous_count") or 0)
    if ambiguous > 0:
        _add(factors, FACTOR_AMBIGUOUS_SCOPE,
             f"{ambiguous}_scope_items_classified_ambiguous", ambiguous)


def _check_manual_dependence(factors, review_workflow):
    rw = review_workflow or {}
    summary = rw.get("review_summary") or {}
    manual = int(summary.get("manual_rows_total") or 0)
    machine = int(summary.get("machine_rows_total") or 0)
    total = manual + machine
    if total > 0 and manual > 0 and (manual / total) >= 0.5:
        _add(factors, FACTOR_HIGH_MANUAL_DEPENDENCE,
             f"{manual}_of_{total}_rows_are_manual_interpretations", manual)


def _check_missing_fields(factors, resolution):
    if not resolution:
        return
    cats = (resolution.get("resolution_summary") or {}).get("category_counts") or {}
    missing_q = int(cats.get("non_comparable_missing_quote_source") or 0)
    if missing_q > 0:
        _add(factors, FACTOR_MISSING_QUOTE_FIELDS,
             f"{missing_q}_rows_missing_quote_qty_or_unit", missing_q)
    unmapped = int(cats.get("unmapped_scope_review_required") or 0)
    if unmapped > 0:
        _add(factors, FACTOR_UNMAPPED_SCOPE,
             f"{unmapped}_rows_unmapped_from_bid_schedule", unmapped)


def _check_external_sources(factors, source_management):
    if not source_management:
        return
    sm_summary = (source_management.get("source_management_summary") or
                  source_management.get("source_management") or {})
    with_sources = int(sm_summary.get("rows_with_sources") or 0)
    total = int(sm_summary.get("rows_total") or 0)
    if total > 0 and with_sources == 0:
        _add(factors, FACTOR_NO_EXTERNAL_SOURCES,
             "no_rows_have_external_quantity_sources")


def _check_unaddressed_bid(factors, scope_interpretation):
    if not scope_interpretation:
        return
    summary = scope_interpretation.get("scope_summary") or {}
    not_addr = int(summary.get("not_addressed_count") or 0)
    if not_addr > 0:
        _add(factors, FACTOR_UNADDRESSED_BID_ITEMS,
             f"{not_addr}_bid_items_not_addressed_in_quote", not_addr)


# ---------------------------------------------------------------------------
# Overall risk + recommended actions
# ---------------------------------------------------------------------------

_RISK_ORDER = {RISK_CRITICAL: 0, RISK_HIGH: 1, RISK_MEDIUM: 2, RISK_LOW: 3}


def _derive_overall_risk(factors: List[Dict[str, Any]]) -> str:
    if not factors:
        return RISK_LOW
    worst = min((_RISK_ORDER.get(f["severity"], 99) for f in factors), default=99)
    return {0: RISK_CRITICAL, 1: RISK_HIGH, 2: RISK_MEDIUM}.get(worst, RISK_LOW)


_FACTOR_TO_ACTION: Dict[str, str] = {
    FACTOR_BLOCKED_PAIRING: "resolve_pairing_or_correct_document_pair",
    FACTOR_UNRESOLVED_EVIDENCE: "enter_manual_interpretation_for_unresolved_blocks",
    FACTOR_UNAPPROVED_MANUAL: "review_and_approve_pending_manual_entries",
    FACTOR_NO_MANUAL_STARTED: "begin_manual_interpretation_of_unreadable_document",
    FACTOR_LOW_COMPARABILITY: "attach_external_quantity_sources_or_enter_manual_qty",
    FACTOR_SOURCE_CONFLICTS: "resolve_conflicting_quantity_sources",
    FACTOR_UNMAPPED_SCOPE: "verify_unmapped_rows_or_add_line_ref",
    FACTOR_AMBIGUOUS_SCOPE: "clarify_ambiguous_scope_items",
    FACTOR_HIGH_MANUAL_DEPENDENCE: "verify_manual_interpretations_against_source",
    FACTOR_MISSING_QUOTE_FIELDS: "enter_qty_unit_for_missing_quote_fields",
    FACTOR_NO_EXTERNAL_SOURCES: "attach_external_quantity_sources",
    FACTOR_UNADDRESSED_BID_ITEMS: "confirm_unaddressed_bid_items_are_out_of_scope",
}


def _build_recommended_actions(factors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deterministic templated action recommendations — one per unique factor."""
    seen = set()
    actions: List[Dict[str, Any]] = []
    for f in factors:
        fid = f["factor_id"]
        if fid in seen:
            continue
        seen.add(fid)
        action_text = _FACTOR_TO_ACTION.get(fid)
        if action_text:
            actions.append({
                "factor_id": fid,
                "severity": f["severity"],
                "recommended_action": action_text,
            })
    actions.sort(key=lambda a: (_RISK_ORDER.get(a["severity"], 99), a["factor_id"]))
    return actions
