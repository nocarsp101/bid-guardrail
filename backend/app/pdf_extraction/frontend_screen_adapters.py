"""
C104 — Frontend screen payload adapters.

Deterministic app-facing payload adapters for the quote case,
package overview, authority action, bid readiness, and timeline /
revision inspection screens. These adapters consume canonical
artifacts and existing frontend/API payloads only — no business
truth is recomputed here. Source refs, revision metadata, state
labels, and identity refs are preserved intact.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

SCREEN_ADAPTER_VERSION = "frontend_screen_adapters/v1"

# Closed screen vocabulary -------------------------------------------------
SCREEN_QUOTE_CASE = "quote_case"
SCREEN_PACKAGE_OVERVIEW = "package_overview"
SCREEN_AUTHORITY_ACTION = "authority_action"
SCREEN_BID_READINESS = "bid_readiness"
SCREEN_TIMELINE = "timeline"
SCREEN_REVISION_INSPECTION = "revision_inspection"

_ALL_SCREENS = frozenset({
    SCREEN_QUOTE_CASE, SCREEN_PACKAGE_OVERVIEW, SCREEN_AUTHORITY_ACTION,
    SCREEN_BID_READINESS, SCREEN_TIMELINE, SCREEN_REVISION_INSPECTION,
})


def list_screens() -> List[str]:
    return sorted(_ALL_SCREENS)


def adapt_quote_case(repository: Any, job_id: str) -> Dict[str, Any]:
    from .control_room_assembly import assemble_quote_case_payload
    payload = assemble_quote_case_payload(repository, job_id)
    view = payload.get("view_model") or {}
    dossier_rec = payload.get("dossier_record") or {}
    env_art = _artifact_from_record(dossier_rec)
    return _screen_envelope(
        SCREEN_QUOTE_CASE,
        identity={"job_id": job_id,
                   "vendor_name": (env_art or {}).get("vendor_name"),
                   "bid_id": _dig(env_art or {}, "package_ref", "bid_id")},
        state_labels={
            "risk_level": _dig(env_art or {}, "latest_risk",
                                "overall_risk_level"),
            "gate_outcome": _dig(env_art or {}, "latest_gate",
                                  "gate_outcome"),
            "decision_posture": (env_art or {}).get("decision_posture"),
            "readiness_status": (env_art or {}).get("readiness_status"),
        },
        body={
            "view_model": view,
            "dossier_record_ref": _record_ref(dossier_rec),
            "clarifications": (env_art or {}).get("open_clarifications") or {},
            "reliance_posture": (env_art or {}).get("reliance_posture") or {},
            "scope_gaps": (env_art or {}).get("scope_gaps") or {},
        },
        source_refs=_source_refs(dossier_rec),
        diagnostics=payload.get("assembly_diagnostics") or {},
    )


def adapt_package_overview(repository: Any, bid_id: str) -> Dict[str, Any]:
    from .control_room_assembly import assemble_package_overview_payload
    payload = assemble_package_overview_payload(repository, bid_id)
    po_rec = payload.get("package_overview_record") or {}
    pg_rec = payload.get("package_gate_record") or {}
    vc_rec = payload.get("vendor_comparison_record") or {}
    po = _artifact_from_record(po_rec) or {}
    pg = _artifact_from_record(pg_rec) or {}
    vc = _artifact_from_record(vc_rec) or {}
    return _screen_envelope(
        SCREEN_PACKAGE_OVERVIEW,
        identity={"bid_id": bid_id},
        state_labels={
            "package_confidence": pg.get("package_confidence")
                                   or pg.get("confidence_level"),
            "gate_outcome": pg.get("package_gate_outcome")
                             or pg.get("gate_outcome"),
            "risk_level": po.get("overall_risk_level"),
        },
        body={
            "view_model": payload.get("view_model") or {},
            "quote_summaries": po.get("quote_summaries") or [],
            "vendor_comparison": vc,
            "package_gate": pg,
            "package_summary": po.get("package_summary") or {},
        },
        source_refs=_source_refs(po_rec, pg_rec, vc_rec),
        diagnostics=payload.get("assembly_diagnostics") or {},
    )


def adapt_authority_action(repository: Any,
                            bid_id: Optional[str] = None) -> Dict[str, Any]:
    from .control_room_assembly import assemble_authority_action_payload
    payload = assemble_authority_action_payload(repository, bid_id)
    aap_rec = payload.get("authority_action_record") or {}
    posture_rec = payload.get("authority_posture_record") or {}
    ref_rec = payload.get("authority_reference_record") or {}
    aap = _artifact_from_record(aap_rec) or {}
    posture = _artifact_from_record(posture_rec) or {}
    ref = _artifact_from_record(ref_rec) or {}
    return _screen_envelope(
        SCREEN_AUTHORITY_ACTION,
        identity={"bid_id": bid_id},
        state_labels={
            "authority_posture": posture.get("authority_package_posture")
                                   or posture.get("posture"),
            "authority_status": aap.get("authority_status"),
            "action_item_count": aap.get("action_item_count"),
        },
        body={
            "view_model": payload.get("view_model") or {},
            "action_items": aap.get("action_items") or [],
            "top_priority_actions": aap.get("top_priority_actions") or [],
            "implication_groups": aap.get("implication_groups") or [],
            "authority_reference": ref,
        },
        source_refs=_source_refs(aap_rec, posture_rec, ref_rec),
        diagnostics=payload.get("assembly_diagnostics") or {},
    )


def adapt_bid_readiness(repository: Any, bid_id: str) -> Dict[str, Any]:
    from .control_room_assembly import assemble_bid_readiness_payload
    payload = assemble_bid_readiness_payload(repository, bid_id)
    rs_rec = payload.get("readiness_snapshot_record") or {}
    pq_rec = payload.get("priority_queue_record") or {}
    cj_rec = payload.get("carry_justification_record") or {}
    rs = _artifact_from_record(rs_rec) or {}
    pq = _artifact_from_record(pq_rec) or {}
    cj = _artifact_from_record(cj_rec) or {}
    return _screen_envelope(
        SCREEN_BID_READINESS,
        identity={"bid_id": bid_id},
        state_labels={
            "overall_readiness": rs.get("overall_readiness"),
            "readiness_state": rs.get("readiness_state"),
            "readiness_level": rs.get("readiness_level"),
            "carry_decision": cj.get("carry_decision"),
            "carry_progression_state": cj.get("carry_progression_state"),
        },
        body={
            "view_model": payload.get("view_model") or {},
            "priority_queue": pq,
            "carry_justification": cj,
            "package_confidence": rs.get("package_confidence") or {},
            "authority_posture": rs.get("authority_posture") or {},
            "deadline_pressure": rs.get("deadline_pressure") or {},
            "top_unresolved_items": rs.get("top_unresolved_items") or [],
            "top_priority_queue_actions":
                rs.get("top_priority_queue_actions") or [],
            "top_reasons": rs.get("top_reasons") or [],
        },
        source_refs=_source_refs(rs_rec, pq_rec, cj_rec),
        diagnostics=payload.get("assembly_diagnostics") or {},
    )


def adapt_timeline(repository: Any,
                    bid_id: Optional[str] = None,
                    job_id: Optional[str] = None,
                    artifact_kinds: Optional[List[str]] = None
                    ) -> Dict[str, Any]:
    from .control_room_assembly import assemble_timeline_payload
    payload = assemble_timeline_payload(repository, bid_id=bid_id,
                                          job_id=job_id,
                                          artifact_kinds=artifact_kinds)
    kind_timelines = payload.get("kind_timelines") or []
    merged = payload.get("merged_timeline") or {}
    return _screen_envelope(
        SCREEN_TIMELINE,
        identity={"bid_id": bid_id, "job_id": job_id},
        state_labels={
            "kind_count": len(kind_timelines),
            "merged_event_count": (merged.get("timeline_summary") or {})
                                    .get("event_count"),
        },
        body={
            "kind_timelines": kind_timelines,
            "merged_timeline": merged,
        },
        source_refs=[],
        diagnostics={"has_events": bool(kind_timelines)},
    )


def adapt_revision_inspection(repository: Any,
                                artifact_type: str,
                                bid_id: Optional[str] = None,
                                job_id: Optional[str] = None,
                                before_revision: Optional[int] = None,
                                after_revision: Optional[int] = None
                                ) -> Dict[str, Any]:
    from .revision_diff import diff_revisions, diff_lineage, diff_summary
    history = repository.history(artifact_type, bid_id=bid_id, job_id=job_id)
    before = after = None
    if before_revision is not None and after_revision is not None:
        before = repository.by_revision_sequence(artifact_type,
                                                   before_revision,
                                                   bid_id=bid_id)
        after = repository.by_revision_sequence(artifact_type,
                                                  after_revision,
                                                  bid_id=bid_id)
    elif len(history) >= 2:
        before = history[-2]
        after = history[-1]
    elif len(history) == 1:
        after = history[-1]

    diff = diff_revisions(before, after) if after else None
    summary = diff_summary(diff) if diff else None
    lineage = diff_lineage(history)

    return _screen_envelope(
        SCREEN_REVISION_INSPECTION,
        identity={"artifact_type": artifact_type,
                   "bid_id": bid_id, "job_id": job_id},
        state_labels={
            "history_length": len(history),
            "diff_status": (diff or {}).get("status"),
            "before_revision": (summary or {}).get("before_revision"),
            "after_revision": (summary or {}).get("after_revision"),
        },
        body={
            "history_count": len(history),
            "latest_diff": diff,
            "diff_summary": summary,
            "lineage_diffs": lineage,
        },
        source_refs=_source_refs(before, after),
        diagnostics={"history_present": bool(history)},
    )


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _screen_envelope(screen_id: str, *, identity: Dict[str, Any],
                      state_labels: Dict[str, Any],
                      body: Dict[str, Any],
                      source_refs: List[Dict[str, Any]],
                      diagnostics: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "screen_adapter_version": SCREEN_ADAPTER_VERSION,
        "screen_id": screen_id,
        "identity": deepcopy(identity),
        "state_labels": deepcopy(state_labels),
        "body": deepcopy(body),
        "source_refs": deepcopy(source_refs),
        "diagnostics": deepcopy(diagnostics),
    }


def _record_ref(record: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not record:
        return None
    return {
        "artifact_type": record.get("artifact_type"),
        "record_id": record.get("record_id"),
        "revision_sequence": record.get("revision_sequence"),
        "supersedes": record.get("supersedes"),
        "superseded_by": record.get("superseded_by"),
        "created_at": record.get("created_at"),
        "created_by": record.get("created_by"),
    }


def _source_refs(*records: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for rec in records:
        ref = _record_ref(rec)
        if ref is not None:
            out.append(ref)
    return out


def _artifact_from_record(record: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not record:
        return None
    env = record.get("envelope") or {}
    art = env.get("artifact") or {}
    return deepcopy(art) if isinstance(art, dict) else None


def _dig(obj: Any, *keys: str) -> Any:
    cur = obj
    for k in keys:
        if isinstance(cur, dict):
            cur = cur.get(k)
        else:
            return None
    return cur
