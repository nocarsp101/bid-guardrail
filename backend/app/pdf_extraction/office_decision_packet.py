"""
C50 — Office action packet / estimator decision view.

Primary fast-read artifact for estimator/PM review. Rolls up risk,
readiness, scope, comparability, blocking issues, and recommended
actions into one deterministic structured object.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

DECISION_PACKET_VERSION = "office_decision_packet/v1"

POSTURE_READY_FOR_USE = "ready_for_use"
POSTURE_USABLE_WITH_CAVEATS = "usable_with_caveats"
POSTURE_REQUIRES_ACTION = "requires_action"
POSTURE_BLOCKED = "blocked"


def build_decision_packet(
    risk_output: Dict[str, Any],
    review_workflow: Optional[Dict[str, Any]] = None,
    scope_interpretation: Optional[Dict[str, Any]] = None,
    resolution_output: Optional[Dict[str, Any]] = None,
    handwritten_review: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    risk = deepcopy(risk_output or {})
    rw = deepcopy(review_workflow or {})
    si = deepcopy(scope_interpretation or {})
    res = deepcopy(resolution_output or {})
    hr = deepcopy(handwritten_review or {})

    overall_risk = risk.get("overall_risk_level", "low")
    readiness = rw.get("readiness_status")
    blocking = risk.get("blocking_risks") or []
    warnings = risk.get("warning_risks") or []
    recommended = risk.get("recommended_actions") or []

    scope_summary = si.get("scope_summary") or {}
    res_summary = res.get("resolution_summary") or {}
    unresolved = (hr.get("unresolved_evidence_summary") or {})

    comparability = _build_comparability_posture(res_summary)
    scope_gaps = _build_scope_gaps(scope_summary)
    evidence_status = _build_evidence_status(unresolved, rw)
    decision_posture = _derive_posture(overall_risk, readiness, blocking)

    return {
        "decision_packet_version": DECISION_PACKET_VERSION,
        "decision_posture": decision_posture,
        "overall_risk_level": overall_risk,
        "readiness_status": readiness,
        "blocking_issues": [{
            "factor_id": b["factor_id"],
            "severity": b["severity"],
            "detail": b["detail"],
        } for b in blocking],
        "warning_issues": [{
            "factor_id": w["factor_id"],
            "severity": w["severity"],
            "detail": w["detail"],
        } for w in warnings],
        "comparability_posture": comparability,
        "scope_gaps": scope_gaps,
        "evidence_status": evidence_status,
        "recommended_actions": recommended,
        "resolution_category_counts": deepcopy(res_summary.get("category_counts") or {}),
        "priority_counts": deepcopy(res_summary.get("priority_counts") or {}),
        "risk_summary": deepcopy(risk.get("risk_summary") or {}),
    }


def _build_comparability_posture(res_summary: Dict[str, Any]) -> Dict[str, Any]:
    cats = res_summary.get("category_counts") or {}
    total = int(res_summary.get("rows_total") or 0)
    matched = int(cats.get("clean_match_no_resolution_needed") or 0)
    non_comp = (int(cats.get("non_comparable_missing_quote_source") or 0)
                + int(cats.get("non_comparable_missing_external_source") or 0))
    conflicts = int(cats.get("source_conflict_review_required") or 0)
    mismatches = (int(cats.get("quantity_discrepancy_review_required") or 0)
                  + int(cats.get("unit_discrepancy_review_required") or 0))
    return {
        "total_rows": total,
        "comparable_matched": matched,
        "non_comparable": non_comp,
        "conflicts": conflicts,
        "mismatches": mismatches,
        "comparability_ratio": (matched / total) if total else None,
    }


def _build_scope_gaps(scope_summary: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "not_addressed_count": int(scope_summary.get("not_addressed_count") or 0),
        "ambiguous_count": int(scope_summary.get("ambiguous_count") or 0),
        "explicitly_excluded_count": int(scope_summary.get("explicitly_excluded_count") or 0),
        "total_topics": int(scope_summary.get("total_topics") or 0),
    }


def _build_evidence_status(
    unresolved: Dict[str, Any],
    rw: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "unresolved_block_count": int(unresolved.get("unresolved_block_count") or 0),
        "blocks_with_manual_entries": int(unresolved.get("blocks_with_manual_entries") or 0),
        "review_queue_items": int(
            (rw.get("progress_metrics") or {}).get("queue_items_remaining") or 0
        ),
    }


def _derive_posture(overall_risk: str, readiness: Optional[str], blocking: list) -> str:
    if blocking:
        return POSTURE_BLOCKED
    if overall_risk == "critical":
        return POSTURE_BLOCKED
    if overall_risk == "high" or readiness in (
        "blocked_pending_manual_interpretation",
        "blocked_pending_approval",
        "blocked_pending_evidence",
        "not_started",
    ):
        return POSTURE_REQUIRES_ACTION
    if overall_risk == "medium":
        return POSTURE_USABLE_WITH_CAVEATS
    return POSTURE_READY_FOR_USE
