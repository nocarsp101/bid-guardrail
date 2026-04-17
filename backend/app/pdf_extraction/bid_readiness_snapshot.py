"""
C73 — Bid readiness control room snapshot.

Consolidated readiness snapshot for the full bid package.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

READINESS_SNAPSHOT_VERSION = "bid_readiness_snapshot/v1"


def build_readiness_snapshot(
    bid_id: str,
    package_overview: Optional[Dict[str, Any]] = None,
    package_gate: Optional[Dict[str, Any]] = None,
    authority_posture: Optional[Dict[str, Any]] = None,
    deadline_pressure: Optional[Dict[str, Any]] = None,
    priority_queue: Optional[Dict[str, Any]] = None,
    vendor_comparison: Optional[Dict[str, Any]] = None,
    carry_justification: Optional[Dict[str, Any]] = None,
    authority_action_packet: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    po = package_overview or {}
    pg = package_gate or {}
    ap = authority_posture or {}
    dp = deadline_pressure or {}
    pq = priority_queue or {}
    vc = vendor_comparison or {}
    cj = carry_justification or {}
    aap = authority_action_packet or {}

    top_unresolved = _top_unresolved_items(pq, aap)
    top_queue_actions = (pq.get("top_priority_actions") or [])[:10]
    vendor_highlights = _vendor_highlights(vc)
    carry_posture = _carry_posture_snap(cj)
    top_reasons = _collect_top_reasons(pg, ap, dp)

    overall_readiness = _derive_overall_readiness(pg, ap, dp)

    return {
        "readiness_snapshot_version": READINESS_SNAPSHOT_VERSION,
        "bid_id": bid_id,
        "overall_readiness": overall_readiness,
        "package_confidence": {
            "package_gate_outcome": pg.get("package_gate_outcome"),
            "reason_count": (pg.get("gate_summary") or {}).get("reason_count", 0),
        },
        "authority_posture": {
            "authority_package_posture": ap.get("authority_package_posture"),
            "required_not_covered": (ap.get("posture_summary") or {}).get("required_not_covered", 0),
            "required_weakly_covered": (ap.get("posture_summary") or {}).get("required_weakly_covered", 0),
        },
        "deadline_pressure": {
            "pressure": dp.get("deadline_pressure"),
            "hours_until_due": dp.get("hours_until_due"),
        },
        "top_unresolved_items": top_unresolved,
        "top_priority_queue_actions": top_queue_actions,
        "carry_decision_posture": carry_posture,
        "vendor_highlights": vendor_highlights,
        "package_summary_counts": deepcopy(po.get("package_summary") or {}),
        "top_reasons": top_reasons,
        "traceability_refs": {
            "package_overview_present": bool(po.get("package_overview_version")),
            "package_gate_present": bool(pg.get("package_gate_version")),
            "authority_posture_present": bool(ap.get("authority_posture_version")),
            "deadline_pressure_present": bool(dp.get("deadline_pressure_version")),
            "priority_queue_present": bool(pq.get("priority_queue_version")),
            "vendor_comparison_present": bool(vc.get("comparison_version")),
            "carry_justification_present": bool(cj.get("carry_justification_version")),
            "authority_action_packet_present": bool(aap.get("authority_action_version")),
        },
    }


def _top_unresolved_items(pq: Dict[str, Any], aap: Dict[str, Any]) -> List[Dict[str, Any]]:
    queue_items = pq.get("queue_items") or []
    top_actions = [q for q in queue_items if q.get("action_bucket") in ("resolve_today", "resolve_before_bid")][:10]
    return top_actions


def _vendor_highlights(vc: Dict[str, Any]) -> Dict[str, Any]:
    entries = vc.get("vendor_entries") or []
    if not entries:
        return {"best": None, "worst": None, "rank_distribution": {}}
    summary = vc.get("comparison_summary") or {}
    return {
        "best": {"vendor_name": entries[0].get("vendor_name"),
                 "job_id": entries[0].get("job_id"),
                 "deterministic_score": entries[0].get("deterministic_score"),
                 "vendor_rank": entries[0].get("vendor_rank")},
        "worst": {"vendor_name": entries[-1].get("vendor_name"),
                  "job_id": entries[-1].get("job_id"),
                  "deterministic_score": entries[-1].get("deterministic_score"),
                  "vendor_rank": entries[-1].get("vendor_rank")},
        "rank_distribution": deepcopy(summary.get("rank_distribution") or {}),
    }


def _carry_posture_snap(cj: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "carry_decision": cj.get("carry_decision"),
        "decided_by": cj.get("decided_by"),
        "decided_at": cj.get("decided_at"),
        "package_gate_outcome": cj.get("package_gate_outcome"),
        "authority_package_posture": cj.get("authority_package_posture"),
    }


def _collect_top_reasons(pg, ap, dp) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in (pg.get("gate_reasons") or [])[:5]:
        out.append({"origin": "package_gate", **r})
    for r in (ap.get("posture_reasons") or [])[:5]:
        out.append({"origin": "authority_posture", **r})
    for r in (dp.get("pressure_reasons") or [])[:5]:
        out.append({"origin": "deadline_pressure", **r})
    return out


def _derive_overall_readiness(pg, ap, dp) -> str:
    pg_out = pg.get("package_gate_outcome")
    ap_pos = ap.get("authority_package_posture")
    pressure = dp.get("deadline_pressure")

    if pg_out == "PACKAGE_BLOCKED" or ap_pos == "authority_blocked" or pressure == "deadline_blocked":
        return "not_ready_blocked"
    if pg_out == "PACKAGE_HIGH_RISK" or ap_pos == "authority_action_required" or pressure == "critical_due_to_time":
        return "action_required"
    if pg_out == "PACKAGE_CONDITIONAL" or ap_pos == "authority_watch" or pressure == "at_risk_due_to_time":
        return "ready_with_caveats"
    if pg_out == "PACKAGE_READY" and (ap_pos in (None, "authority_clear")) and (pressure in (None, "on_track")):
        return "ready"
    return "ready_with_caveats"
