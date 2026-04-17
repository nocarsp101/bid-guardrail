"""
C56 — Clarification tracking layer.

Tracks lifecycle of generated clarifications with append-only state
transitions. Every clarification links back to its scope topic, bid
item, and/or risk factor.

Closed status vocabulary:
    pending_send, sent, responded, unresolved, closed
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

TRACKING_VERSION = "clarification_tracking/v1"

STATUS_PENDING_SEND = "pending_send"
STATUS_SENT = "sent"
STATUS_RESPONDED = "responded"
STATUS_UNRESOLVED = "unresolved"
STATUS_CLOSED = "closed"

_ALL_STATUSES = frozenset({
    STATUS_PENDING_SEND, STATUS_SENT, STATUS_RESPONDED,
    STATUS_UNRESOLVED, STATUS_CLOSED,
})


def create_tracking_state(
    clarification_output: Dict[str, Any],
) -> Dict[str, Any]:
    items = (clarification_output or {}).get("clarification_items") or []
    tracked: List[Dict[str, Any]] = []
    for item in items:
        tracked.append({
            "clarification_id": item.get("clarification_id"),
            "clarification_type": item.get("clarification_type"),
            "source_ref": item.get("source_ref"),
            "evidence_refs": deepcopy(item.get("evidence_refs") or []),
            "current_status": STATUS_PENDING_SEND,
            "status_history": [{
                "status": STATUS_PENDING_SEND,
                "timestamp": None,
                "actor": None,
                "note": "initial_creation",
            }],
            "response_ref": None,
        })
    return {
        "tracking_version": TRACKING_VERSION,
        "tracked_clarifications": tracked,
        "tracking_summary": _build_summary(tracked),
    }


def update_clarification_status(
    tracking_state: Dict[str, Any],
    updates: List[Dict[str, Any]],
) -> Dict[str, Any]:
    out = deepcopy(tracking_state)
    tracked = out.get("tracked_clarifications") or []
    lookup = {t["clarification_id"]: t for t in tracked}
    unknown_ids: List[str] = []

    for u in updates:
        cid = u.get("clarification_id")
        new_status = u.get("status")
        if cid not in lookup:
            unknown_ids.append(str(cid))
            continue
        entry = lookup[cid]
        if new_status in _ALL_STATUSES:
            entry["current_status"] = new_status
            entry["status_history"].append({
                "status": new_status,
                "timestamp": u.get("timestamp"),
                "actor": u.get("actor"),
                "note": u.get("note"),
            })
            if u.get("response_ref"):
                entry["response_ref"] = u.get("response_ref")

    out["tracking_summary"] = _build_summary(tracked)
    out["tracking_summary"]["unknown_ids"] = unknown_ids
    return out


def _build_summary(tracked: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts: Dict[str, int] = {s: 0 for s in _ALL_STATUSES}
    for t in tracked:
        s = t.get("current_status")
        if s in counts:
            counts[s] += 1
    return {
        "total": len(tracked),
        "status_counts": counts,
        "pending_send_count": counts[STATUS_PENDING_SEND],
        "sent_count": counts[STATUS_SENT],
        "responded_count": counts[STATUS_RESPONDED],
        "unresolved_count": counts[STATUS_UNRESOLVED],
        "closed_count": counts[STATUS_CLOSED],
    }
