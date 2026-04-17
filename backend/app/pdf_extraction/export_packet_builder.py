"""
C76 — Export packet builder.

Deterministic export payload builders. Assembles canonical outputs only.
Stable, auditable structure. No new inference.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

EXPORT_VERSION = "export_packet_builder/v1"

EXPORT_SUB_CLARIFICATION = "subcontractor_clarification_packet"
EXPORT_ESTIMATOR_REVIEW = "estimator_review_packet"
EXPORT_AUTHORITY_ACTION = "authority_action_packet_export"
EXPORT_BID_READINESS = "bid_readiness_packet"
EXPORT_FINAL_CARRY = "final_bid_carry_justification_packet"

_ALL_EXPORT_TYPES = frozenset({
    EXPORT_SUB_CLARIFICATION, EXPORT_ESTIMATOR_REVIEW,
    EXPORT_AUTHORITY_ACTION, EXPORT_BID_READINESS, EXPORT_FINAL_CARRY,
})


def build_sub_clarification_export(
    dossier: Optional[Dict[str, Any]] = None,
    clarification_output: Optional[Dict[str, Any]] = None,
    tracking_state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    d = dossier or {}
    cl = clarification_output or {}
    tracking = tracking_state or {}

    pending_ids = {t["clarification_id"] for t in (tracking.get("tracked_clarifications") or [])
                   if t.get("current_status") in ("pending_send", "sent")}

    items = [
        {"clarification_id": c.get("clarification_id"),
         "clarification_type": c.get("clarification_type"),
         "text": c.get("clarification_text"),
         "source_ref": c.get("source_ref"),
         "evidence_refs": deepcopy(c.get("evidence_refs") or [])}
        for c in (cl.get("clarification_items") or [])
        if (not pending_ids) or c.get("clarification_id") in pending_ids
    ]

    return _envelope(EXPORT_SUB_CLARIFICATION, {
        "vendor_name": d.get("vendor_name"),
        "job_id": d.get("job_id"),
        "clarification_items": items,
        "item_count": len(items),
        "tracking_summary": deepcopy(tracking.get("tracking_summary") or {}),
    }, source_refs={
        "dossier_version": d.get("dossier_version"),
        "tracking_version": tracking.get("tracking_version"),
    })


def build_estimator_review_export(
    dossier: Dict[str, Any],
    decision_packet: Optional[Dict[str, Any]] = None,
    recommendation_output: Optional[Dict[str, Any]] = None,
    risk_output: Optional[Dict[str, Any]] = None,
    clarification_output: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    d = dossier or {}
    dp = decision_packet or {}
    rec = recommendation_output or {}
    risk = risk_output or {}
    cl = clarification_output or {}

    return _envelope(EXPORT_ESTIMATOR_REVIEW, {
        "vendor_name": d.get("vendor_name"),
        "job_id": d.get("job_id"),
        "decision_posture": dp.get("decision_posture") or d.get("decision_posture"),
        "overall_risk_level": risk.get("overall_risk_level")
                              or (d.get("latest_risk") or {}).get("overall_risk_level"),
        "readiness_status": d.get("readiness_status"),
        "blocking_issues": deepcopy(dp.get("blocking_issues") or risk.get("blocking_risks") or []),
        "warning_issues": deepcopy(dp.get("warning_issues") or risk.get("warning_risks") or []),
        "recommended_actions": deepcopy(risk.get("recommended_actions") or []),
        "carry_gap_summary": deepcopy(rec.get("recommendation_summary") or {}),
        "comparability_posture": deepcopy(d.get("comparability_posture") or {}),
        "scope_gaps": deepcopy(d.get("scope_gaps") or {}),
        "evidence_status": deepcopy(d.get("evidence_status") or {}),
        "open_clarifications": deepcopy(d.get("open_clarifications") or {}),
        "total_clarifications": len(cl.get("clarification_items") or []),
    }, source_refs={
        "dossier_version": d.get("dossier_version"),
        "decision_packet_version": dp.get("decision_packet_version"),
        "risk_scoring_version": risk.get("risk_scoring_version"),
        "recommendation_version": rec.get("recommendation_version"),
    })


def build_authority_action_export(
    authority_action_packet: Dict[str, Any],
    authority_posture: Optional[Dict[str, Any]] = None,
    authority_reference: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    aap = authority_action_packet or {}
    ap = authority_posture or {}
    ar = authority_reference or {}

    return _envelope(EXPORT_AUTHORITY_ACTION, {
        "authority_package_posture": ap.get("authority_package_posture"),
        "action_item_count": aap.get("action_item_count"),
        "top_priority_actions": deepcopy(aap.get("top_priority_actions") or []),
        "implication_groups": deepcopy(aap.get("implication_groups") or []),
        "action_summary": deepcopy(aap.get("action_summary") or {}),
        "posture_summary": deepcopy(ap.get("posture_summary") or {}),
        "authority_summary": deepcopy(ar.get("authority_summary") or {}),
    }, source_refs={
        "authority_action_version": aap.get("authority_action_version"),
        "authority_posture_version": ap.get("authority_posture_version"),
        "authority_version": ar.get("authority_version"),
        "package_ref": deepcopy(aap.get("package_ref") or {}),
    })


def build_bid_readiness_export(
    readiness_snapshot: Dict[str, Any],
    priority_queue: Optional[Dict[str, Any]] = None,
    package_gate: Optional[Dict[str, Any]] = None,
    authority_posture: Optional[Dict[str, Any]] = None,
    deadline_pressure: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rs = readiness_snapshot or {}
    pq = priority_queue or {}
    pg = package_gate or {}
    ap = authority_posture or {}
    dp = deadline_pressure or {}

    return _envelope(EXPORT_BID_READINESS, {
        "bid_id": rs.get("bid_id"),
        "overall_readiness": rs.get("overall_readiness"),
        "package_confidence": deepcopy(rs.get("package_confidence") or {}),
        "authority_posture": deepcopy(rs.get("authority_posture") or {}),
        "deadline_pressure": deepcopy(rs.get("deadline_pressure") or {}),
        "top_unresolved_items": deepcopy(rs.get("top_unresolved_items") or []),
        "top_priority_queue_actions": deepcopy(rs.get("top_priority_queue_actions") or []),
        "carry_decision_posture": deepcopy(rs.get("carry_decision_posture") or {}),
        "vendor_highlights": deepcopy(rs.get("vendor_highlights") or {}),
        "package_summary_counts": deepcopy(rs.get("package_summary_counts") or {}),
        "top_reasons": deepcopy(rs.get("top_reasons") or []),
        "queue_bucket_counts": deepcopy(pq.get("bucket_counts") or {}),
        "package_gate_reasons": deepcopy(pg.get("gate_reasons") or []),
        "authority_posture_reasons": deepcopy(ap.get("posture_reasons") or []),
        "deadline_pressure_reasons": deepcopy(dp.get("pressure_reasons") or []),
    }, source_refs={
        "readiness_snapshot_version": rs.get("readiness_snapshot_version"),
        "priority_queue_version": pq.get("priority_queue_version"),
        "package_gate_version": pg.get("package_gate_version"),
        "authority_posture_version": ap.get("authority_posture_version"),
        "deadline_pressure_version": dp.get("deadline_pressure_version"),
    })


def build_final_carry_export(
    carry_justification: Dict[str, Any],
    readiness_snapshot: Optional[Dict[str, Any]] = None,
    authority_action_packet: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cj = carry_justification or {}
    rs = readiness_snapshot or {}
    aap = authority_action_packet or {}

    return _envelope(EXPORT_FINAL_CARRY, {
        "bid_id": cj.get("bid_id"),
        "record_id": cj.get("record_id"),
        "carry_decision": cj.get("carry_decision"),
        "decided_by": cj.get("decided_by"),
        "decided_at": cj.get("decided_at"),
        "decision_note": cj.get("decision_note"),
        "package_gate_outcome": cj.get("package_gate_outcome"),
        "authority_package_posture": cj.get("authority_package_posture"),
        "unresolved_authority_gaps": deepcopy(cj.get("unresolved_authority_gaps") or {}),
        "internal_carry_snapshot": deepcopy(cj.get("internal_carry_snapshot") or {}),
        "authority_snapshot": deepcopy(cj.get("authority_snapshot") or {}),
        "acknowledged_review_items": list(cj.get("acknowledged_review_items") or []),
        "package_gate_reasons": deepcopy(cj.get("package_gate_reasons") or []),
        "authority_posture_reasons": deepcopy(cj.get("authority_posture_reasons") or []),
        "readiness_at_carry": {
            "overall_readiness": rs.get("overall_readiness"),
            "top_unresolved_item_count": len(rs.get("top_unresolved_items") or []),
        },
        "authority_action_item_count": int(aap.get("action_item_count") or 0),
    }, source_refs={
        "carry_justification_version": cj.get("carry_justification_version"),
        "readiness_snapshot_version": rs.get("readiness_snapshot_version"),
        "authority_action_version": aap.get("authority_action_version"),
    })


def _envelope(export_type: str, payload: Dict[str, Any],
              source_refs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "export_version": EXPORT_VERSION,
        "export_type": export_type,
        "export_type_valid": export_type in _ALL_EXPORT_TYPES,
        "payload": payload,
        "source_refs": source_refs or {},
    }
