"""
C69 — Authority-aware package posture.

Package-level authority posture signal from authority exposure outputs.
Closed vocabulary, deterministic, explainable.

Posture vocabulary:
    authority_clear, authority_watch, authority_action_required, authority_blocked
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

AUTHORITY_POSTURE_VERSION = "authority_package_posture/v1"

POSTURE_CLEAR = "authority_clear"
POSTURE_WATCH = "authority_watch"
POSTURE_ACTION_REQUIRED = "authority_action_required"
POSTURE_BLOCKED = "authority_blocked"

_POSTURE_ORDER = {POSTURE_BLOCKED: 0, POSTURE_ACTION_REQUIRED: 1, POSTURE_WATCH: 2, POSTURE_CLEAR: 3}


def evaluate_authority_posture(
    authority_exposure: Dict[str, Any],
    authority_action_packet: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    exposure_summary = (authority_exposure or {}).get("exposure_summary") or {}
    items = (authority_exposure or {}).get("exposure_items") or []

    not_covered = int(exposure_summary.get("not_covered_count") or 0)
    weakly_covered = int(exposure_summary.get("weakly_covered_count") or 0)
    ambiguous = int(exposure_summary.get("ambiguously_covered_count") or 0)
    review_req = int(exposure_summary.get("review_required_count") or 0)
    covered = int(exposure_summary.get("covered_count") or 0)
    total = int(exposure_summary.get("total_items") or 0)

    required_not_covered = sum(1 for i in items
                                if i.get("authority_posture") == "required"
                                and i.get("exposure_level") == "not_covered")
    required_weakly = sum(1 for i in items
                          if i.get("authority_posture") == "required"
                          and i.get("exposure_level") in ("weakly_covered", "ambiguously_covered"))

    reasons: List[Dict[str, Any]] = []
    posture = POSTURE_CLEAR

    if required_not_covered > 0:
        posture = _escalate(posture, POSTURE_BLOCKED)
        reasons.append({"check": "required_authority_not_covered", "severity": "critical",
                        "detail": f"{required_not_covered}_required_authority_topics_not_covered"})

    if required_weakly > 0:
        posture = _escalate(posture, POSTURE_ACTION_REQUIRED)
        reasons.append({"check": "required_authority_weakly_covered", "severity": "high",
                        "detail": f"{required_weakly}_required_authority_topics_weakly_or_ambiguously_covered"})

    if not_covered > required_not_covered:
        posture = _escalate(posture, POSTURE_WATCH)
        reasons.append({"check": "non_required_authority_not_covered", "severity": "medium",
                        "detail": f"{not_covered - required_not_covered}_non_required_authority_topics_not_covered"})

    if review_req > 0:
        posture = _escalate(posture, POSTURE_WATCH)
        reasons.append({"check": "authority_needs_review", "severity": "medium",
                        "detail": f"{review_req}_authority_topics_need_review"})

    if ambiguous > 0 and posture == POSTURE_CLEAR:
        posture = _escalate(posture, POSTURE_WATCH)
        reasons.append({"check": "ambiguous_authority_coverage", "severity": "medium",
                        "detail": f"{ambiguous}_authority_topics_ambiguously_covered"})

    return {
        "authority_posture_version": AUTHORITY_POSTURE_VERSION,
        "authority_package_posture": posture,
        "posture_reasons": reasons,
        "posture_summary": {
            "total_authority_topics": total,
            "covered": covered,
            "not_covered": not_covered,
            "weakly_covered": weakly_covered,
            "ambiguously_covered": ambiguous,
            "review_required": review_req,
            "required_not_covered": required_not_covered,
            "required_weakly_covered": required_weakly,
            "reason_count": len(reasons),
        },
    }


def _escalate(current: str, candidate: str) -> str:
    if _POSTURE_ORDER.get(candidate, 99) < _POSTURE_ORDER.get(current, 99):
        return candidate
    return current
