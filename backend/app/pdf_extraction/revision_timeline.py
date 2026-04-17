"""
C78 — Revision timeline / audit view.

Deterministic timeline/audit artifacts showing creation, revision,
supersession, state transitions, carry decision changes, readiness
changes, and source refs across cycles.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

TIMELINE_VERSION = "revision_timeline/v1"

EVENT_CREATED = "created"
EVENT_REVISED = "revised"
EVENT_SUPERSEDED = "superseded"
EVENT_STATE_TRANSITION = "state_transition"
EVENT_CARRY_DECISION_CHANGED = "carry_decision_changed"
EVENT_READINESS_CHANGED = "readiness_changed"


def build_revision_timeline(
    records: List[Dict[str, Any]],
    artifact_kind: Optional[str] = None,
    state_field: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a deterministic timeline from an append-only record list.

    Each record is expected to be a canonical artifact (carry justification,
    readiness snapshot, dossier, etc.) with optional `superseded_by`,
    `revision_sequence`, and a state field like `carry_decision` or
    `overall_readiness`.
    """
    records = records or []
    events: List[Dict[str, Any]] = []

    prev_state: Optional[Any] = None
    for idx, rec in enumerate(records):
        rid = rec.get("record_id") or f"rec-{idx}"
        seq = int(rec.get("revision_sequence") or idx)
        created_at = rec.get("decided_at") or rec.get("created_at") or rec.get("persisted_at")
        actor = rec.get("decided_by") or rec.get("persisted_by") or rec.get("created_by")
        superseded_by = rec.get("superseded_by")

        # Event 1: created or revised.
        event_type = EVENT_CREATED if idx == 0 else EVENT_REVISED
        events.append({
            "event_id": f"evt-{len(events):04d}",
            "event_type": event_type,
            "revision_sequence": seq,
            "record_id": rid,
            "artifact_kind": artifact_kind,
            "actor": actor,
            "timestamp": created_at,
            "source_ref": {"record_id": rid, "revision_sequence": seq},
        })

        # Event 2: supersession (if applicable).
        if superseded_by:
            events.append({
                "event_id": f"evt-{len(events):04d}",
                "event_type": EVENT_SUPERSEDED,
                "revision_sequence": seq,
                "record_id": rid,
                "superseded_by": superseded_by,
                "artifact_kind": artifact_kind,
                "source_ref": {"record_id": rid, "superseded_by": superseded_by},
            })

        # Event 3: state transition based on state_field.
        if state_field:
            curr_state = rec.get(state_field)
            if curr_state is not None and curr_state != prev_state:
                transition_type = _derive_transition_type(state_field)
                events.append({
                    "event_id": f"evt-{len(events):04d}",
                    "event_type": transition_type,
                    "revision_sequence": seq,
                    "record_id": rid,
                    "artifact_kind": artifact_kind,
                    "state_field": state_field,
                    "state_before": prev_state,
                    "state_after": curr_state,
                    "timestamp": created_at,
                    "actor": actor,
                    "source_ref": {"record_id": rid, "state_field": state_field},
                })
            prev_state = curr_state

    return {
        "timeline_version": TIMELINE_VERSION,
        "artifact_kind": artifact_kind,
        "state_field": state_field,
        "record_count": len(records),
        "events": events,
        "timeline_summary": {
            "event_count": len(events),
            "creation_events": sum(1 for e in events if e["event_type"] == EVENT_CREATED),
            "revision_events": sum(1 for e in events if e["event_type"] == EVENT_REVISED),
            "supersession_events": sum(1 for e in events if e["event_type"] == EVENT_SUPERSEDED),
            "state_transition_events": sum(1 for e in events
                                            if e["event_type"] in (EVENT_STATE_TRANSITION,
                                                                    EVENT_CARRY_DECISION_CHANGED,
                                                                    EVENT_READINESS_CHANGED)),
        },
    }


def _derive_transition_type(state_field: str) -> str:
    if state_field == "carry_decision":
        return EVENT_CARRY_DECISION_CHANGED
    if state_field in ("overall_readiness", "package_gate_outcome"):
        return EVENT_READINESS_CHANGED
    return EVENT_STATE_TRANSITION


def merge_timelines(
    *timelines: Dict[str, Any],
) -> Dict[str, Any]:
    """Merge multiple timelines into one ordered event stream."""
    all_events: List[Dict[str, Any]] = []
    for tl in timelines:
        if not tl:
            continue
        for e in tl.get("events") or []:
            all_events.append(deepcopy(e))

    # Stable sort: by timestamp (string), then artifact_kind, then event_id.
    all_events.sort(key=lambda e: (
        str(e.get("timestamp") or ""),
        str(e.get("artifact_kind") or ""),
        e.get("event_id", ""),
    ))

    return {
        "timeline_version": TIMELINE_VERSION,
        "merged": True,
        "timeline_count": len(timelines),
        "events": all_events,
        "timeline_summary": {
            "event_count": len(all_events),
        },
    }
