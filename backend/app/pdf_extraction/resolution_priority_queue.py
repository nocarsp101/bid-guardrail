"""
C72 — Resolution priority queue.

Deterministic cross-package priority queue for unresolved items.

Closed action buckets:
    resolve_today, resolve_before_bid, safe_to_carry_with_caveat, monitor_only
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

QUEUE_VERSION = "resolution_priority_queue/v1"

BUCKET_RESOLVE_TODAY = "resolve_today"
BUCKET_RESOLVE_BEFORE_BID = "resolve_before_bid"
BUCKET_SAFE_WITH_CAVEAT = "safe_to_carry_with_caveat"
BUCKET_MONITOR_ONLY = "monitor_only"

_BUCKET_PRIORITY = {
    BUCKET_RESOLVE_TODAY: 0, BUCKET_RESOLVE_BEFORE_BID: 1,
    BUCKET_SAFE_WITH_CAVEAT: 2, BUCKET_MONITOR_ONLY: 3,
}


def build_priority_queue(
    package_overview: Optional[Dict[str, Any]] = None,
    authority_action_packet: Optional[Dict[str, Any]] = None,
    deadline_pressure: Optional[Dict[str, Any]] = None,
    dossiers: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    dossiers = dossiers or []
    po = package_overview or {}
    aap = authority_action_packet or {}
    dp = deadline_pressure or {}

    pressure = dp.get("deadline_pressure") or "on_track"
    under_pressure = pressure in ("critical_due_to_time", "deadline_blocked")

    items: List[Dict[str, Any]] = []
    ordinal = 0

    # Source 1: blocked quote issues from dossiers.
    for d in dossiers:
        jid = d.get("job_id")
        gate = (d.get("latest_gate") or {}).get("gate_outcome")
        risk = (d.get("latest_risk") or {}).get("overall_risk_level")
        if gate == "BLOCKED":
            items.append(_item(ordinal, "blocked_quote", BUCKET_RESOLVE_TODAY,
                                 source_ref={"job_id": jid, "vendor": d.get("vendor_name")},
                                 reason="quote_gate_blocked"))
            ordinal += 1
        elif gate == "HIGH_RISK" or risk == "high":
            items.append(_item(ordinal, "high_risk_quote",
                                 BUCKET_RESOLVE_TODAY if under_pressure else BUCKET_RESOLVE_BEFORE_BID,
                                 source_ref={"job_id": jid, "vendor": d.get("vendor_name")},
                                 reason="quote_high_risk"))
            ordinal += 1

        open_cl = (d.get("open_clarifications") or {}).get("total_open", 0)
        if open_cl > 0:
            items.append(_item(ordinal, "open_clarifications",
                                 BUCKET_RESOLVE_TODAY if under_pressure else BUCKET_RESOLVE_BEFORE_BID,
                                 source_ref={"job_id": jid, "vendor": d.get("vendor_name")},
                                 reason=f"{open_cl}_open_clarifications",
                                 count=int(open_cl)))
            ordinal += 1

        rel_posture = d.get("reliance_posture") or {}
        if int(rel_posture.get("clarify_before_reliance_count") or 0) > 0:
            items.append(_item(ordinal, "unresolved_carry_decision",
                                 BUCKET_RESOLVE_BEFORE_BID,
                                 source_ref={"job_id": jid, "vendor": d.get("vendor_name")},
                                 reason="carry_decision_pending_clarification",
                                 count=int(rel_posture.get("clarify_before_reliance_count") or 0)))
            ordinal += 1

    # Source 2: authority action items.
    for action in aap.get("action_items") or []:
        imp = action.get("handling_implication")
        posture = action.get("authority_posture")
        bucket = _authority_bucket(imp, posture, under_pressure)
        items.append(_item(ordinal, "authority_gap", bucket,
                             source_ref={"authority_topic_id": action.get("authority_topic_id"),
                                         "authority_source_type": action.get("authority_source_type")},
                             reason=f"{action.get('exposure_level')}_{posture}",
                             extra={"authority_description": action.get("authority_description"),
                                    "handling_implication": imp}))
        ordinal += 1

    # Deterministic sort.
    items.sort(key=lambda i: (_BUCKET_PRIORITY.get(i["action_bucket"], 99), i["queue_item_id"]))

    bucket_counts = _bucket_counts(items)
    top_actions = [i for i in items
                   if i["action_bucket"] in (BUCKET_RESOLVE_TODAY, BUCKET_RESOLVE_BEFORE_BID)][:20]

    return {
        "priority_queue_version": QUEUE_VERSION,
        "queue_items": items,
        "top_priority_actions": top_actions,
        "bucket_counts": bucket_counts,
        "queue_summary": {
            "total_items": len(items),
            "resolve_today_count": bucket_counts.get(BUCKET_RESOLVE_TODAY, 0),
            "resolve_before_bid_count": bucket_counts.get(BUCKET_RESOLVE_BEFORE_BID, 0),
            "safe_with_caveat_count": bucket_counts.get(BUCKET_SAFE_WITH_CAVEAT, 0),
            "monitor_only_count": bucket_counts.get(BUCKET_MONITOR_ONLY, 0),
            "under_deadline_pressure": under_pressure,
        },
    }


def _item(
    ordinal: int,
    item_type: str,
    bucket: str,
    source_ref: Dict[str, Any],
    reason: str,
    count: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = {
        "queue_item_id": f"qi-{ordinal:04d}",
        "item_type": item_type,
        "action_bucket": bucket,
        "source_ref": deepcopy(source_ref),
        "reason": reason,
    }
    if count is not None:
        out["count"] = count
    if extra:
        out.update(extra)
    return out


def _authority_bucket(imp: Optional[str], posture: Optional[str], under_pressure: bool) -> str:
    if imp == "clarify_or_carry_internally_required_authority":
        return BUCKET_RESOLVE_TODAY if under_pressure else BUCKET_RESOLVE_BEFORE_BID
    if imp == "carry_internally_or_clarify":
        return BUCKET_RESOLVE_BEFORE_BID
    if imp == "clarify_before_reliance":
        return BUCKET_RESOLVE_BEFORE_BID
    if imp == "estimator_review_required":
        return BUCKET_SAFE_WITH_CAVEAT
    if imp == "carry_in_sub_quote":
        return BUCKET_MONITOR_ONLY
    return BUCKET_SAFE_WITH_CAVEAT


def _bucket_counts(items: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for i in items:
        b = i.get("action_bucket", "unknown")
        counts[b] = counts.get(b, 0) + 1
    return counts
