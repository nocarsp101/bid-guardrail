"""
C95 — Render-ready report payload layer.

Deterministic render-ready report payload builders. Assemble canonical
artifacts only, preserve source refs and state labels, and never
recompute business truth or UI layout. These payloads are intended to
be consumed by any render layer (HTML, PDF, JSON) without additional
interpretation.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

REPORT_VERSION = "render_reports/v1"


def build_estimator_review_report(
    repository: Any,
    job_id: str,
    revision_sequence: Optional[int] = None,
) -> Dict[str, Any]:
    dossier_rec = _fetch(repository, "quote_dossier", job_id=job_id,
                          revision_sequence=revision_sequence)
    risk_rec = _fetch(repository, "risk_output", job_id=job_id)
    dp_rec = _fetch(repository, "decision_packet", job_id=job_id)
    rec_rec = _fetch(repository, "recommendation_output", job_id=job_id)
    clar_rec = _fetch(repository, "clarification_output", job_id=job_id)
    dossier = _artifact(dossier_rec) or {}

    sections: List[Dict[str, Any]] = []
    sections.append(_section("header", "Estimator Review",
                             _header_from_dossier(dossier, job_id)))
    sections.append(_section("risk", "Risk",
                             _risk_block(dossier, _artifact(risk_rec))))
    sections.append(_section("gate", "Gate",
                             _gate_block(dossier)))
    sections.append(_section("clarifications", "Open Clarifications",
                             _clarification_block(dossier, _artifact(clar_rec))))
    sections.append(_section("recommendation", "Recommendation",
                             _artifact(rec_rec) or dossier.get("recommendation_summary") or {}))
    sections.append(_section("decision_packet", "Decision Packet",
                             _artifact(dp_rec) or {}))

    return _report_envelope(
        report_kind="estimator_review_report",
        title="Estimator Review Report",
        sections=sections,
        identity={"job_id": job_id,
                  "bid_id": _dig(dossier, "package_ref", "bid_id"),
                  "vendor_name": dossier.get("vendor_name")},
        source_records=[dossier_rec, risk_rec, dp_rec, rec_rec, clar_rec],
        state_labels={
            "risk_level": _dig(dossier, "latest_risk", "overall_risk_level"),
            "gate_outcome": _dig(dossier, "latest_gate", "gate_outcome"),
            "decision_posture": dossier.get("decision_posture"),
        },
    )


def build_authority_action_report(repository: Any, bid_id: str,
                                   revision_sequence: Optional[int] = None) -> Dict[str, Any]:
    aap_rec = _fetch(repository, "authority_action_packet",
                      bid_id=bid_id, revision_sequence=revision_sequence)
    posture_rec = _fetch(repository, "authority_posture", bid_id=bid_id)
    ref_rec = _fetch(repository, "authority_reference", bid_id=bid_id)
    exp_rec = _fetch(repository, "authority_exposure", bid_id=bid_id)

    aap = _artifact(aap_rec) or {}
    posture = _artifact(posture_rec) or {}

    sections = [
        _section("header", "Authority Action", {
            "bid_id": bid_id,
            "action_item_count": aap.get("action_item_count"),
        }),
        _section("posture", "Authority Posture", posture),
        _section("exposure", "Authority Exposure", _artifact(exp_rec) or {}),
        _section("actions", "Action Items", {
            "top_priority_actions": aap.get("top_priority_actions") or [],
            "implication_groups": aap.get("implication_groups") or [],
            "action_summary": aap.get("action_summary") or {},
        }),
        _section("reference", "Authority Reference", _artifact(ref_rec) or {}),
    ]

    return _report_envelope(
        report_kind="authority_action_report",
        title="Authority Action Report",
        sections=sections,
        identity={"bid_id": bid_id},
        source_records=[aap_rec, posture_rec, ref_rec, exp_rec],
        state_labels={
            "authority_posture": posture.get("authority_package_posture")
                                  or posture.get("posture"),
        },
    )


def build_bid_readiness_report(repository: Any, bid_id: str,
                                revision_sequence: Optional[int] = None) -> Dict[str, Any]:
    rs_rec = _fetch(repository, "bid_readiness_snapshot", bid_id=bid_id,
                     revision_sequence=revision_sequence)
    pq_rec = _fetch(repository, "priority_queue", bid_id=bid_id)
    pg_rec = _fetch(repository, "package_gate", bid_id=bid_id)
    ap_rec = _fetch(repository, "authority_posture", bid_id=bid_id)
    dp_rec = _fetch(repository, "deadline_pressure", bid_id=bid_id)
    rs = _artifact(rs_rec) or {}

    sections = [
        _section("header", "Bid Readiness", {
            "bid_id": bid_id,
            "overall_readiness": rs.get("overall_readiness"),
        }),
        _section("package_confidence", "Package Confidence",
                 rs.get("package_confidence") or {}),
        _section("authority_posture", "Authority Posture",
                 rs.get("authority_posture") or _artifact(ap_rec) or {}),
        _section("deadline_pressure", "Deadline Pressure",
                 rs.get("deadline_pressure") or _artifact(dp_rec) or {}),
        _section("priority_queue", "Priority Queue", _artifact(pq_rec) or {}),
        _section("top_reasons", "Top Reasons", rs.get("top_reasons") or []),
        _section("top_items", "Top Unresolved Items",
                 rs.get("top_unresolved_items") or []),
    ]

    return _report_envelope(
        report_kind="bid_readiness_report",
        title="Bid Readiness Report",
        sections=sections,
        identity={"bid_id": bid_id},
        source_records=[rs_rec, pq_rec, pg_rec, ap_rec, dp_rec],
        state_labels={
            "overall_readiness": rs.get("overall_readiness"),
            "package_gate_outcome": _dig(_artifact(pg_rec) or {},
                                          "package_gate_outcome"),
        },
    )


def build_final_carry_report(repository: Any, bid_id: str,
                              revision_sequence: Optional[int] = None) -> Dict[str, Any]:
    cj_rec = _fetch(repository, "bid_carry_justification", bid_id=bid_id,
                     revision_sequence=revision_sequence)
    rs_rec = _fetch(repository, "bid_readiness_snapshot", bid_id=bid_id)
    aap_rec = _fetch(repository, "authority_action_packet", bid_id=bid_id)
    cj = _artifact(cj_rec) or {}
    rs = _artifact(rs_rec) or {}

    sections = [
        _section("header", "Final Carry Justification", {
            "bid_id": bid_id,
            "carry_decision": cj.get("carry_decision"),
            "decided_by": cj.get("decided_by"),
            "decided_at": cj.get("decided_at"),
        }),
        _section("justification", "Justification",
                 cj.get("justification_summary") or cj.get("justification") or {}),
        _section("readiness", "Readiness Context", {
            "overall_readiness": rs.get("overall_readiness"),
            "top_reasons": rs.get("top_reasons") or [],
        }),
        _section("authority_action", "Authority Action",
                 _artifact(aap_rec) or {}),
        _section("traceability", "Traceability", {
            "readiness_record_id": (rs_rec or {}).get("record_id"),
            "authority_action_record_id": (aap_rec or {}).get("record_id"),
            "carry_record_id": (cj_rec or {}).get("record_id"),
        }),
    ]

    return _report_envelope(
        report_kind="final_carry_report",
        title="Final Carry Justification Report",
        sections=sections,
        identity={"bid_id": bid_id},
        source_records=[cj_rec, rs_rec, aap_rec],
        state_labels={
            "carry_decision": cj.get("carry_decision"),
            "overall_readiness": rs.get("overall_readiness"),
        },
    )


def list_report_kinds() -> List[str]:
    return ["estimator_review_report", "authority_action_report",
            "bid_readiness_report", "final_carry_report"]


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _report_envelope(*, report_kind: str, title: str,
                     sections: List[Dict[str, Any]],
                     identity: Dict[str, Any],
                     source_records: List[Optional[Dict[str, Any]]],
                     state_labels: Dict[str, Any]) -> Dict[str, Any]:
    refs: List[Dict[str, Any]] = []
    for rec in source_records:
        if rec is None:
            continue
        refs.append({
            "artifact_type": rec.get("artifact_type"),
            "record_id": rec.get("record_id"),
            "revision_sequence": rec.get("revision_sequence"),
        })
    return {
        "render_report_version": REPORT_VERSION,
        "report_kind": report_kind,
        "title": title,
        "identity": identity,
        "state_labels": state_labels,
        "sections": sections,
        "source_refs": refs,
        "diagnostics": {
            "section_count": len(sections),
            "source_ref_count": len(refs),
        },
    }


def _section(section_id: str, title: str, body: Any) -> Dict[str, Any]:
    return {
        "section_id": section_id,
        "title": title,
        "body": deepcopy(body) if isinstance(body, (dict, list)) else body,
    }


def _fetch(repo, artifact_type, bid_id=None, job_id=None, revision_sequence=None):
    if revision_sequence is not None:
        return repo.by_revision_sequence(artifact_type, revision_sequence, bid_id=bid_id)
    return repo.latest(artifact_type, bid_id=bid_id, job_id=job_id)


def _artifact(record: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if record is None:
        return None
    return deepcopy((record.get("envelope") or {}).get("artifact") or {})


def _dig(obj: Any, *keys: str) -> Any:
    cur = obj
    for k in keys:
        if isinstance(cur, dict):
            cur = cur.get(k)
        else:
            return None
    return cur


def _header_from_dossier(dossier: Dict[str, Any], job_id: str) -> Dict[str, Any]:
    return {
        "job_id": job_id,
        "vendor_name": dossier.get("vendor_name"),
        "decision_posture": dossier.get("decision_posture"),
        "readiness_status": dossier.get("readiness_status"),
    }


def _risk_block(dossier: Dict[str, Any],
                risk_output: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "overall_risk_level": _dig(dossier, "latest_risk", "overall_risk_level"),
        "factor_count": _dig(dossier, "latest_risk", "factor_count"),
        "blocking_count": _dig(dossier, "latest_risk", "blocking_count"),
        "risk_output": risk_output or {},
    }


def _gate_block(dossier: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "gate_outcome": _dig(dossier, "latest_gate", "gate_outcome"),
        "reason_count": _dig(dossier, "latest_gate", "reason_count"),
    }


def _clarification_block(dossier: Dict[str, Any],
                          clar_output: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "open_clarifications": dossier.get("open_clarifications") or {},
        "clarification_output": clar_output or {},
    }
