"""
C68 — Authority action packet.

Estimator-facing summary of authority-backed exposure for the package.
Prioritizes actions without legal compliance conclusions.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

AUTHORITY_ACTION_VERSION = "authority_action_packet/v1"

_IMPLICATION_PRIORITY = {
    "clarify_or_carry_internally_required_authority": 0,
    "carry_internally_or_clarify": 1,
    "clarify_before_reliance": 2,
    "estimator_review_required": 3,
    "carry_in_sub_quote": 4,
}


def build_authority_action_packet(
    authority_exposure: Dict[str, Any],
    authority_reference: Optional[Dict[str, Any]] = None,
    package_overview: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    items = (authority_exposure or {}).get("exposure_items") or []
    exposure_summary = (authority_exposure or {}).get("exposure_summary") or {}

    action_items: List[Dict[str, Any]] = []
    for item in items:
        exposure = item.get("exposure_level")
        if exposure == "covered":
            continue
        action_items.append({
            "authority_topic_id": item.get("authority_topic_id"),
            "authority_description": item.get("authority_description"),
            "authority_posture": item.get("authority_posture"),
            "authority_source_type": item.get("authority_source_type"),
            "authority_source_ref": deepcopy(item.get("authority_source_ref") or {}),
            "exposure_level": exposure,
            "handling_implication": item.get("handling_implication"),
            "matched_scope_ref": deepcopy(item.get("matched_scope_ref")),
        })

    action_items.sort(key=lambda a: (
        _IMPLICATION_PRIORITY.get(a.get("handling_implication"), 99),
        a.get("authority_topic_id") or "",
    ))

    implication_groups = _group_by_implication(action_items)
    top_actions = action_items[:10]

    return {
        "authority_action_version": AUTHORITY_ACTION_VERSION,
        "action_item_count": len(action_items),
        "action_items": action_items,
        "top_priority_actions": top_actions,
        "implication_groups": implication_groups,
        "action_summary": {
            "total_gaps": len(action_items),
            "not_covered_count": int(exposure_summary.get("not_covered_count") or 0),
            "weakly_covered_count": int(exposure_summary.get("weakly_covered_count") or 0),
            "ambiguously_covered_count": int(exposure_summary.get("ambiguously_covered_count") or 0),
            "review_required_count": int(exposure_summary.get("review_required_count") or 0),
            "implication_counts": deepcopy(exposure_summary.get("implication_counts") or {}),
        },
        "package_ref": {
            "bid_id": (package_overview or {}).get("bid_id"),
            "quote_count": (package_overview or {}).get("quote_count"),
        },
    }


def _group_by_implication(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[str, Dict[str, Any]] = {}
    for item in items:
        imp = item.get("handling_implication") or "unknown"
        if imp not in groups:
            groups[imp] = {"handling_implication": imp, "count": 0, "topic_ids": []}
        groups[imp]["count"] += 1
        tid = item.get("authority_topic_id")
        if tid and len(groups[imp]["topic_ids"]) < 10:
            groups[imp]["topic_ids"].append(tid)
    return sorted(groups.values(), key=lambda g: _IMPLICATION_PRIORITY.get(g["handling_implication"], 99))
