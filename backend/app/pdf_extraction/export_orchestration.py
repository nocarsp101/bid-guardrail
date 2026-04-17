"""
C82 — Export orchestration service.

Service-level export orchestration for latest and explicit-version
export generation. Uses canonical export builders only; preserves
source refs and revision metadata.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

EXPORT_ORCH_VERSION = "export_orchestration/v1"


def generate_sub_clarification_export(
    repository: Any,
    job_id: str,
    revision_sequence: Optional[int] = None,
) -> Dict[str, Any]:
    from .export_packet_builder import build_sub_clarification_export
    dossier_rec = _fetch(repository, "quote_dossier", job_id=job_id, revision_sequence=revision_sequence)
    tracking_rec = _fetch(repository, "clarification_tracking", job_id=job_id)
    clar_rec = _fetch(repository, "clarification_output", job_id=job_id)
    export = build_sub_clarification_export(
        dossier=_artifact(dossier_rec),
        clarification_output=_artifact(clar_rec),
        tracking_state=_artifact(tracking_rec),
    )
    return _wrap_orch(export, [dossier_rec, tracking_rec, clar_rec])


def generate_estimator_review_export(
    repository: Any,
    job_id: str,
    revision_sequence: Optional[int] = None,
) -> Dict[str, Any]:
    from .export_packet_builder import build_estimator_review_export
    dossier_rec = _fetch(repository, "quote_dossier", job_id=job_id, revision_sequence=revision_sequence)
    dp_rec = _fetch(repository, "decision_packet", job_id=job_id)
    rec_rec = _fetch(repository, "recommendation_output", job_id=job_id)
    risk_rec = _fetch(repository, "risk_output", job_id=job_id)
    clar_rec = _fetch(repository, "clarification_output", job_id=job_id)
    export = build_estimator_review_export(
        dossier=_artifact(dossier_rec) or {},
        decision_packet=_artifact(dp_rec),
        recommendation_output=_artifact(rec_rec),
        risk_output=_artifact(risk_rec),
        clarification_output=_artifact(clar_rec),
    )
    return _wrap_orch(export, [dossier_rec, dp_rec, rec_rec, risk_rec, clar_rec])


def generate_authority_action_export(
    repository: Any,
    bid_id: str,
    revision_sequence: Optional[int] = None,
) -> Dict[str, Any]:
    from .export_packet_builder import build_authority_action_export
    aap_rec = _fetch(repository, "authority_action_packet", bid_id=bid_id,
                      revision_sequence=revision_sequence)
    posture_rec = _fetch(repository, "authority_posture", bid_id=bid_id)
    ref_rec = _fetch(repository, "authority_reference", bid_id=bid_id)
    export = build_authority_action_export(
        authority_action_packet=_artifact(aap_rec) or {},
        authority_posture=_artifact(posture_rec),
        authority_reference=_artifact(ref_rec),
    )
    return _wrap_orch(export, [aap_rec, posture_rec, ref_rec])


def generate_bid_readiness_export(
    repository: Any,
    bid_id: str,
    revision_sequence: Optional[int] = None,
) -> Dict[str, Any]:
    from .export_packet_builder import build_bid_readiness_export
    rs_rec = _fetch(repository, "bid_readiness_snapshot", bid_id=bid_id,
                     revision_sequence=revision_sequence)
    pq_rec = _fetch(repository, "priority_queue", bid_id=bid_id)
    pg_rec = _fetch(repository, "package_gate", bid_id=bid_id)
    ap_rec = _fetch(repository, "authority_posture", bid_id=bid_id)
    dp_rec = _fetch(repository, "deadline_pressure", bid_id=bid_id)
    export = build_bid_readiness_export(
        readiness_snapshot=_artifact(rs_rec) or {},
        priority_queue=_artifact(pq_rec),
        package_gate=_artifact(pg_rec),
        authority_posture=_artifact(ap_rec),
        deadline_pressure=_artifact(dp_rec),
    )
    return _wrap_orch(export, [rs_rec, pq_rec, pg_rec, ap_rec, dp_rec])


def generate_final_carry_export(
    repository: Any,
    bid_id: str,
    revision_sequence: Optional[int] = None,
) -> Dict[str, Any]:
    from .export_packet_builder import build_final_carry_export
    cj_rec = _fetch(repository, "bid_carry_justification", bid_id=bid_id,
                     revision_sequence=revision_sequence)
    rs_rec = _fetch(repository, "bid_readiness_snapshot", bid_id=bid_id)
    aap_rec = _fetch(repository, "authority_action_packet", bid_id=bid_id)
    export = build_final_carry_export(
        carry_justification=_artifact(cj_rec) or {},
        readiness_snapshot=_artifact(rs_rec),
        authority_action_packet=_artifact(aap_rec),
    )
    return _wrap_orch(export, [cj_rec, rs_rec, aap_rec])


def _fetch(repository, artifact_type, bid_id=None, job_id=None, revision_sequence=None):
    if revision_sequence is not None:
        return repository.by_revision_sequence(artifact_type, revision_sequence, bid_id=bid_id)
    return repository.latest(artifact_type, bid_id=bid_id, job_id=job_id)


def _artifact(record: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if record is None:
        return None
    return deepcopy((record.get("envelope") or {}).get("artifact") or {})


def _wrap_orch(export: Dict[str, Any], source_records: List[Optional[Dict[str, Any]]]) -> Dict[str, Any]:
    source_refs = []
    for rec in source_records:
        if rec is None:
            continue
        source_refs.append({
            "artifact_type": rec.get("artifact_type"),
            "record_id": rec.get("record_id"),
            "revision_sequence": rec.get("revision_sequence"),
        })
    return {
        "export_orchestration_version": EXPORT_ORCH_VERSION,
        "export": export,
        "source_records": source_refs,
    }
