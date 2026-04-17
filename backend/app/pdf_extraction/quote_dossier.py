"""
C59 — Review case file / quote dossier.

Consolidates the latest quote review state for a vendor/quote pair into
one deterministic inspection object with links to all underlying evidence.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

DOSSIER_VERSION = "quote_dossier/v1"


def build_dossier(
    job_id: Optional[str] = None,
    vendor_name: Optional[str] = None,
    current_cycle: Optional[Dict[str, Any]] = None,
    gate_output: Optional[Dict[str, Any]] = None,
    risk_output: Optional[Dict[str, Any]] = None,
    decision_packet: Optional[Dict[str, Any]] = None,
    recommendation_output: Optional[Dict[str, Any]] = None,
    tracking_state: Optional[Dict[str, Any]] = None,
    response_integration: Optional[Dict[str, Any]] = None,
    scenario_whatif: Optional[Dict[str, Any]] = None,
    reeval_history: Optional[Dict[str, Any]] = None,
    review_workflow: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    gate = gate_output or {}
    risk = risk_output or {}
    dp = decision_packet or {}
    rec = recommendation_output or {}
    tracking = tracking_state or {}
    resp = response_integration or {}
    whatif = scenario_whatif or {}
    reeval = reeval_history or {}
    rw = review_workflow or {}

    open_clarifications = _count_open_clarifications(tracking)
    response_summary = _response_summary(resp)
    active_assumptions = _active_assumptions(whatif)
    reliance_posture = _reliance_posture(gate, dp, rec)

    return {
        "dossier_version": DOSSIER_VERSION,
        "job_id": job_id,
        "vendor_name": vendor_name,
        "current_cycle": {
            "cycle_id": (current_cycle or {}).get("cycle_id"),
            "cycle_reason": (current_cycle or {}).get("cycle_reason"),
            "metrics": deepcopy((current_cycle or {}).get("metrics") or {}),
        },
        "latest_gate": {
            "gate_outcome": gate.get("gate_outcome"),
            "reason_count": (gate.get("gate_summary") or {}).get("reason_count", 0),
        },
        "latest_risk": {
            "overall_risk_level": risk.get("overall_risk_level"),
            "factor_count": (risk.get("risk_summary") or {}).get("total_factors", 0),
            "blocking_count": len(risk.get("blocking_risks") or []),
        },
        "decision_posture": dp.get("decision_posture"),
        "readiness_status": rw.get("readiness_status"),
        "open_clarifications": open_clarifications,
        "response_history_summary": response_summary,
        "active_assumptions": active_assumptions,
        "reliance_posture": reliance_posture,
        "recommendation_summary": deepcopy(rec.get("recommendation_summary") or {}),
        "comparability_posture": deepcopy(dp.get("comparability_posture") or {}),
        "scope_gaps": deepcopy(dp.get("scope_gaps") or {}),
        "evidence_status": deepcopy(dp.get("evidence_status") or {}),
        "reeval_cycle_count": int(reeval.get("cycle_count") or 0),
        "dossier_diagnostics": {
            "has_gate": bool(gate.get("gate_outcome")),
            "has_risk": bool(risk.get("overall_risk_level")),
            "has_tracking": bool(tracking.get("tracked_clarifications")),
            "has_responses": bool(resp.get("integrated_responses")),
            "has_scenarios": bool(whatif.get("scenario_results")),
            "has_reeval_history": int(reeval.get("cycle_count") or 0) > 1,
        },
    }


def _count_open_clarifications(tracking: Dict[str, Any]) -> Dict[str, int]:
    counts = {"pending_send": 0, "sent": 0, "unresolved": 0, "total_open": 0}
    for t in tracking.get("tracked_clarifications") or []:
        s = t.get("current_status")
        if s in ("pending_send", "sent", "unresolved"):
            counts[s] = counts.get(s, 0) + 1
            counts["total_open"] += 1
    return counts


def _response_summary(resp: Dict[str, Any]) -> Dict[str, Any]:
    summary = resp.get("integration_summary") or {}
    return {
        "total_responses": int(summary.get("total_responses") or 0),
        "scope_updates": int(summary.get("scope_updates_count") or 0),
        "comparability_updates": int(summary.get("comparability_updates_count") or 0),
        "risk_updates": int(summary.get("risk_updates_count") or 0),
    }


def _active_assumptions(whatif: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for s in whatif.get("scenario_results") or []:
        applied = s.get("assumptions_applied") or []
        if applied:
            out.append({
                "scenario_id": s.get("scenario_id"),
                "assumptions_count": len(applied),
                "scenario_risk_level": s.get("scenario_risk_level"),
                "scenario_posture": s.get("scenario_decision_posture"),
            })
    return out


def _reliance_posture(gate, dp, rec) -> Dict[str, Any]:
    rec_summary = rec.get("recommendation_summary") or {}
    return {
        "gate_outcome": gate.get("gate_outcome"),
        "decision_posture": dp.get("decision_posture"),
        "carry_in_sub_quote_count": int(rec_summary.get("carry_in_sub_quote_count") or 0),
        "clarify_before_reliance_count": int(rec_summary.get("clarify_before_reliance_count") or 0),
        "block_quote_reliance_count": int(rec_summary.get("block_quote_reliance_count") or 0),
    }
