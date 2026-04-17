"""
C81 — Control room assembly service.

Orchestrates full control-room payloads from persisted canonical
artifacts. Consumes canonical artifacts only; never recomputes truth.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

ASSEMBLY_VERSION = "control_room_assembly/v1"


def assemble_quote_case_payload(
    repository: Any,
    job_id: str,
) -> Dict[str, Any]:
    from .control_room_view_models import build_quote_case_view
    dossier_record = repository.latest("quote_dossier", job_id=job_id)
    dossier = _artifact(dossier_record)
    view = build_quote_case_view(dossier) if dossier else {}
    return {
        "assembly_version": ASSEMBLY_VERSION,
        "payload_type": "quote_case_payload",
        "job_id": job_id,
        "dossier_record": dossier_record,
        "view_model": view,
        "assembly_diagnostics": {
            "dossier_present": dossier is not None,
        },
    }


def assemble_package_overview_payload(
    repository: Any,
    bid_id: str,
) -> Dict[str, Any]:
    from .control_room_view_models import build_package_overview_view
    po_rec = repository.latest("package_overview", bid_id=bid_id)
    gate_rec = repository.latest("package_gate", bid_id=bid_id) or \
               _find_package_gate(repository, bid_id)
    vc_rec = repository.latest("vendor_comparison", bid_id=bid_id)

    po = _artifact(po_rec)
    pg = _artifact(gate_rec)
    vc = _artifact(vc_rec)
    view = build_package_overview_view(po or {}, pg or {}, vc or {}) if po else {}

    return {
        "assembly_version": ASSEMBLY_VERSION,
        "payload_type": "package_overview_payload",
        "bid_id": bid_id,
        "package_overview_record": po_rec,
        "package_gate_record": gate_rec,
        "vendor_comparison_record": vc_rec,
        "view_model": view,
        "assembly_diagnostics": {
            "package_overview_present": po is not None,
            "package_gate_present": pg is not None,
            "vendor_comparison_present": vc is not None,
        },
    }


def assemble_authority_action_payload(
    repository: Any,
    bid_id: Optional[str] = None,
) -> Dict[str, Any]:
    from .control_room_view_models import build_authority_action_view
    aap_rec = repository.latest("authority_action_packet", bid_id=bid_id)
    posture_rec = repository.latest("authority_posture", bid_id=bid_id)
    ref_rec = repository.latest("authority_reference", bid_id=bid_id)

    aap = _artifact(aap_rec)
    posture = _artifact(posture_rec)
    ref = _artifact(ref_rec)
    view = build_authority_action_view(aap or {}, posture or {}, ref or {}) if aap else {}

    return {
        "assembly_version": ASSEMBLY_VERSION,
        "payload_type": "authority_action_payload",
        "bid_id": bid_id,
        "authority_action_record": aap_rec,
        "authority_posture_record": posture_rec,
        "authority_reference_record": ref_rec,
        "view_model": view,
        "assembly_diagnostics": {
            "authority_action_present": aap is not None,
            "authority_posture_present": posture is not None,
            "authority_reference_present": ref is not None,
        },
    }


def assemble_bid_readiness_payload(
    repository: Any,
    bid_id: str,
) -> Dict[str, Any]:
    from .control_room_view_models import build_bid_readiness_view
    rs_rec = repository.latest("bid_readiness_snapshot", bid_id=bid_id)
    pq_rec = repository.latest("priority_queue", bid_id=bid_id)
    carry_rec = repository.latest("bid_carry_justification", bid_id=bid_id)
    rs = _artifact(rs_rec)
    pq = _artifact(pq_rec)
    view = build_bid_readiness_view(rs or {}, pq or {}) if rs else {}

    return {
        "assembly_version": ASSEMBLY_VERSION,
        "payload_type": "bid_readiness_payload",
        "bid_id": bid_id,
        "readiness_snapshot_record": rs_rec,
        "priority_queue_record": pq_rec,
        "carry_justification_record": carry_rec,
        "view_model": view,
        "assembly_diagnostics": {
            "readiness_present": rs is not None,
            "priority_queue_present": pq is not None,
            "carry_justification_present": carry_rec is not None,
        },
    }


def assemble_timeline_payload(
    repository: Any,
    bid_id: Optional[str] = None,
    job_id: Optional[str] = None,
    artifact_kinds: Optional[List[str]] = None,
) -> Dict[str, Any]:
    from .revision_timeline import build_revision_timeline, merge_timelines
    kinds = artifact_kinds or ["bid_carry_justification", "bid_readiness_snapshot",
                                "quote_dossier", "package_overview"]
    timelines: List[Dict[str, Any]] = []
    for kind in kinds:
        recs = repository.history(kind, bid_id=bid_id, job_id=job_id)
        artifacts = [_artifact_from_record(r) for r in recs]
        # Inject lineage fields into artifacts for the timeline builder.
        enriched = []
        for i, a in enumerate(artifacts):
            if a is None:
                continue
            rec = recs[i]
            a = dict(a)
            a["record_id"] = rec.get("record_id")
            a["revision_sequence"] = rec.get("revision_sequence")
            a["superseded_by"] = rec.get("superseded_by")
            a["created_at"] = rec.get("created_at")
            a["created_by"] = rec.get("created_by")
            enriched.append(a)
        state_field = _state_field_for(kind)
        tl = build_revision_timeline(enriched, artifact_kind=kind, state_field=state_field)
        timelines.append(tl)

    merged = merge_timelines(*timelines)

    return {
        "assembly_version": ASSEMBLY_VERSION,
        "payload_type": "timeline_payload",
        "bid_id": bid_id,
        "job_id": job_id,
        "kind_timelines": timelines,
        "merged_timeline": merged,
    }


def _state_field_for(kind: str) -> Optional[str]:
    if kind == "bid_carry_justification":
        return "carry_decision"
    if kind == "bid_readiness_snapshot":
        return "overall_readiness"
    if kind == "package_overview":
        return None
    if kind == "quote_dossier":
        return "decision_posture"
    return None


def _artifact(record: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if record is None:
        return None
    return _artifact_from_record(record)


def _artifact_from_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    env = record.get("envelope") or {}
    return deepcopy(env.get("artifact") or {})


def _find_package_gate(repository: Any, bid_id: str) -> Optional[Dict[str, Any]]:
    # package_gate may be stored under the package_overview bid — try by bid_id across all records.
    for rec in repository.by_bid_id(bid_id):
        if rec.get("artifact_type") == "package_gate":
            return rec
    return None
