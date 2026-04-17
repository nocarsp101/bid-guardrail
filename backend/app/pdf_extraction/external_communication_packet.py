"""
C60 — External communication packet.

Deterministic outbound communication packet generation for subcontractor
clarification requests, internal estimator summaries, and escalation
summaries. Structured templated outputs only — every element traces
back to existing clarifications, risks, scope gaps, or evidence refs.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

COMM_PACKET_VERSION = "external_communication_packet/v1"

COMM_SUB_CLARIFICATION = "subcontractor_clarification_request"
COMM_INTERNAL_SUMMARY = "internal_estimator_summary"
COMM_ESCALATION = "escalation_summary"

_ALL_COMM_TYPES = (COMM_SUB_CLARIFICATION, COMM_INTERNAL_SUMMARY, COMM_ESCALATION)


def build_communication_packet(
    communication_type: str,
    dossier: Optional[Dict[str, Any]] = None,
    clarification_output: Optional[Dict[str, Any]] = None,
    tracking_state: Optional[Dict[str, Any]] = None,
    risk_output: Optional[Dict[str, Any]] = None,
    decision_packet: Optional[Dict[str, Any]] = None,
    recommendation_output: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    dossier = dossier or {}
    cl = clarification_output or {}
    tracking = tracking_state or {}
    risk = risk_output or {}
    dp = decision_packet or {}
    rec = recommendation_output or {}

    if communication_type == COMM_SUB_CLARIFICATION:
        sections = _build_sub_clarification(cl, tracking)
    elif communication_type == COMM_INTERNAL_SUMMARY:
        sections = _build_internal_summary(dossier, dp, risk, rec)
    elif communication_type == COMM_ESCALATION:
        sections = _build_escalation(dossier, dp, risk, rec)
    else:
        sections = []

    return {
        "comm_packet_version": COMM_PACKET_VERSION,
        "communication_type": communication_type,
        "vendor_name": dossier.get("vendor_name"),
        "job_id": dossier.get("job_id"),
        "sections": sections,
        "section_count": len(sections),
        "comm_diagnostics": {
            "valid_type": communication_type in _ALL_COMM_TYPES,
            "source_dossier_present": bool(dossier.get("dossier_version")),
        },
    }


def _build_sub_clarification(cl, tracking) -> List[Dict[str, Any]]:
    sections: List[Dict[str, Any]] = []
    pending = []
    for t in tracking.get("tracked_clarifications") or []:
        if t.get("current_status") in ("pending_send", "sent"):
            pending.append(t)

    items = cl.get("clarification_items") or []
    pending_ids = {p["clarification_id"] for p in pending}

    clarification_lines: List[Dict[str, Any]] = []
    for item in items:
        if item.get("clarification_id") in pending_ids or not pending_ids:
            clarification_lines.append({
                "clarification_id": item.get("clarification_id"),
                "clarification_type": item.get("clarification_type"),
                "text": item.get("clarification_text"),
                "source_ref": item.get("source_ref"),
                "evidence_refs": deepcopy(item.get("evidence_refs") or []),
            })

    if clarification_lines:
        sections.append({
            "section_id": "clarification_questions",
            "label": "Items Requiring Clarification",
            "items": clarification_lines,
            "item_count": len(clarification_lines),
        })

    sections.append({
        "section_id": "response_instructions",
        "label": "Response Instructions",
        "text": "Please respond to each item above with confirmation, correction, or explanation. Reference the item number in your response.",
    })

    return sections


def _build_internal_summary(dossier, dp, risk, rec) -> List[Dict[str, Any]]:
    sections: List[Dict[str, Any]] = []

    sections.append({
        "section_id": "posture_overview",
        "label": "Decision Posture Overview",
        "content": {
            "decision_posture": dp.get("decision_posture") or dossier.get("decision_posture"),
            "overall_risk_level": dp.get("overall_risk_level") or dossier.get("latest_risk", {}).get("overall_risk_level"),
            "readiness_status": dossier.get("readiness_status"),
        },
    })

    blocking = dp.get("blocking_issues") or risk.get("blocking_risks") or []
    if blocking:
        sections.append({
            "section_id": "blocking_issues",
            "label": "Blocking Issues",
            "items": [{"factor_id": b.get("factor_id"), "detail": b.get("detail")} for b in blocking],
            "item_count": len(blocking),
        })

    warnings = dp.get("warning_issues") or risk.get("warning_risks") or []
    if warnings:
        sections.append({
            "section_id": "warning_issues",
            "label": "Warning Issues",
            "items": [{"factor_id": w.get("factor_id"), "detail": w.get("detail")} for w in warnings[:10]],
            "item_count": len(warnings),
        })

    rec_summary = rec.get("recommendation_summary") or {}
    sections.append({
        "section_id": "carry_gap_summary",
        "label": "Carry / Gap Summary",
        "content": deepcopy(rec_summary),
    })

    return sections


def _build_escalation(dossier, dp, risk, rec) -> List[Dict[str, Any]]:
    sections = _build_internal_summary(dossier, dp, risk, rec)
    open_cl = dossier.get("open_clarifications") or {}
    if int(open_cl.get("total_open") or 0) > 0:
        sections.append({
            "section_id": "open_clarifications",
            "label": "Open Clarifications",
            "content": deepcopy(open_cl),
        })
    sections.append({
        "section_id": "escalation_reason",
        "label": "Escalation Reason",
        "text": "This quote requires management review due to unresolved blocking or high-risk issues.",
    })
    return sections
