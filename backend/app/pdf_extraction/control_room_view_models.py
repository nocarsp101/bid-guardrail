"""
C75 — Control room view models.

Deterministic view-model builders for control-room screens. Consume
canonical artifacts only. Never recompute business truth.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

VIEW_MODEL_VERSION = "control_room_view_models/v1"


def build_quote_case_view(
    dossier: Dict[str, Any],
    resolution_output: Optional[Dict[str, Any]] = None,
    tracking_state: Optional[Dict[str, Any]] = None,
    reeval_history: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    d = dossier or {}
    res = resolution_output or {}
    tracking = tracking_state or {}
    reeval = reeval_history or {}

    return {
        "view_model_version": VIEW_MODEL_VERSION,
        "view_type": "quote_case_view",
        "job_id": d.get("job_id"),
        "vendor_name": d.get("vendor_name"),
        "header": {
            "decision_posture": d.get("decision_posture"),
            "readiness_status": d.get("readiness_status"),
            "gate_outcome": (d.get("latest_gate") or {}).get("gate_outcome"),
            "risk_level": (d.get("latest_risk") or {}).get("overall_risk_level"),
        },
        "sections": {
            "comparability": deepcopy(d.get("comparability_posture") or {}),
            "scope_gaps": deepcopy(d.get("scope_gaps") or {}),
            "evidence_status": deepcopy(d.get("evidence_status") or {}),
            "reliance_posture": deepcopy(d.get("reliance_posture") or {}),
            "open_clarifications": deepcopy(d.get("open_clarifications") or {}),
            "response_history_summary": deepcopy(d.get("response_history_summary") or {}),
            "active_assumptions": deepcopy(d.get("active_assumptions") or []),
            "recommendation_summary": deepcopy(d.get("recommendation_summary") or {}),
        },
        "state_labels": {
            "decision_posture": d.get("decision_posture"),
            "readiness_status": d.get("readiness_status"),
            "gate_outcome": (d.get("latest_gate") or {}).get("gate_outcome"),
        },
        "tracking_summary": deepcopy(tracking.get("tracking_summary") or {}),
        "reeval_cycle_count": int(reeval.get("cycle_count") or d.get("reeval_cycle_count") or 0),
        "source_refs": {"dossier_version": d.get("dossier_version"), "job_id": d.get("job_id")},
    }


def build_package_overview_view(
    package_overview: Dict[str, Any],
    package_gate: Optional[Dict[str, Any]] = None,
    vendor_comparison: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    po = package_overview or {}
    pg = package_gate or {}
    vc = vendor_comparison or {}

    return {
        "view_model_version": VIEW_MODEL_VERSION,
        "view_type": "package_overview_view",
        "bid_id": po.get("bid_id"),
        "header": {
            "package_gate_outcome": pg.get("package_gate_outcome"),
            "quote_count": po.get("quote_count"),
        },
        "sections": {
            "quote_summaries": deepcopy(po.get("quote_summaries") or []),
            "package_summary": deepcopy(po.get("package_summary") or {}),
            "gate_reasons": deepcopy(pg.get("gate_reasons") or []),
            "vendor_ranking": [
                {"vendor_name": e.get("vendor_name"), "job_id": e.get("job_id"),
                 "vendor_rank": e.get("vendor_rank"),
                 "deterministic_score": e.get("deterministic_score")}
                for e in (vc.get("vendor_entries") or [])
            ],
        },
        "state_labels": {
            "package_gate_outcome": pg.get("package_gate_outcome"),
            "reliance_distribution": deepcopy(
                (po.get("package_summary") or {}).get("reliance_decision_distribution") or {}
            ),
        },
        "source_refs": {
            "package_overview_version": po.get("package_overview_version"),
            "package_gate_version": pg.get("package_gate_version"),
            "vendor_comparison_version": vc.get("comparison_version"),
        },
    }


def build_authority_action_view(
    authority_action_packet: Dict[str, Any],
    authority_posture: Optional[Dict[str, Any]] = None,
    authority_reference: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    aap = authority_action_packet or {}
    ap = authority_posture or {}
    ar = authority_reference or {}

    return {
        "view_model_version": VIEW_MODEL_VERSION,
        "view_type": "authority_action_view",
        "header": {
            "authority_package_posture": ap.get("authority_package_posture"),
            "action_item_count": aap.get("action_item_count"),
        },
        "sections": {
            "top_priority_actions": deepcopy(aap.get("top_priority_actions") or []),
            "implication_groups": deepcopy(aap.get("implication_groups") or []),
            "action_summary": deepcopy(aap.get("action_summary") or {}),
            "posture_reasons": deepcopy(ap.get("posture_reasons") or []),
            "authority_summary": deepcopy(ar.get("authority_summary") or {}),
        },
        "state_labels": {
            "authority_package_posture": ap.get("authority_package_posture"),
        },
        "source_refs": {
            "authority_action_version": aap.get("authority_action_version"),
            "authority_posture_version": ap.get("authority_posture_version"),
            "authority_version": ar.get("authority_version"),
            "package_ref": deepcopy(aap.get("package_ref") or {}),
        },
    }


def build_bid_readiness_view(
    readiness_snapshot: Dict[str, Any],
    priority_queue: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rs = readiness_snapshot or {}
    pq = priority_queue or {}

    return {
        "view_model_version": VIEW_MODEL_VERSION,
        "view_type": "bid_readiness_view",
        "bid_id": rs.get("bid_id"),
        "header": {
            "overall_readiness": rs.get("overall_readiness"),
            "package_gate_outcome": (rs.get("package_confidence") or {}).get("package_gate_outcome"),
            "authority_package_posture": (rs.get("authority_posture") or {}).get("authority_package_posture"),
            "deadline_pressure": (rs.get("deadline_pressure") or {}).get("pressure"),
        },
        "sections": {
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
        },
        "state_labels": {
            "overall_readiness": rs.get("overall_readiness"),
        },
        "source_refs": {
            "readiness_snapshot_version": rs.get("readiness_snapshot_version"),
            "priority_queue_version": pq.get("priority_queue_version"),
            "traceability_refs": deepcopy(rs.get("traceability_refs") or {}),
        },
    }
