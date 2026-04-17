"""
C67 — Authority exposure layer.

Deterministic exposure outputs showing where authority-backed scope
topics are weakly covered, ambiguously covered, or require estimator
review. Surfaces carry/clarify/review implications without legal/spec
compliance conclusions.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

EXPOSURE_VERSION = "authority_exposure/v1"

EXPOSURE_COVERED = "covered"
EXPOSURE_WEAKLY_COVERED = "weakly_covered"
EXPOSURE_AMBIGUOUSLY_COVERED = "ambiguously_covered"
EXPOSURE_NOT_COVERED = "not_covered"
EXPOSURE_REVIEW_REQUIRED = "review_required"

_ALL_EXPOSURES = frozenset({
    EXPOSURE_COVERED, EXPOSURE_WEAKLY_COVERED, EXPOSURE_AMBIGUOUSLY_COVERED,
    EXPOSURE_NOT_COVERED, EXPOSURE_REVIEW_REQUIRED,
})

# Mapping from authority comparison outcome -> exposure level.
_OUTCOME_TO_EXPOSURE = {
    "authority_addressed": EXPOSURE_COVERED,
    "authority_conditionally_addressed": EXPOSURE_WEAKLY_COVERED,
    "authority_ambiguous": EXPOSURE_AMBIGUOUSLY_COVERED,
    "authority_not_addressed": EXPOSURE_NOT_COVERED,
    "authority_needs_review": EXPOSURE_REVIEW_REQUIRED,
}

# Mapping from exposure level -> handling implication (closed, deterministic).
_EXPOSURE_IMPLICATION = {
    EXPOSURE_COVERED: "carry_in_sub_quote",
    EXPOSURE_WEAKLY_COVERED: "clarify_before_reliance",
    EXPOSURE_AMBIGUOUSLY_COVERED: "clarify_before_reliance",
    EXPOSURE_NOT_COVERED: "carry_internally_or_clarify",
    EXPOSURE_REVIEW_REQUIRED: "estimator_review_required",
}


def build_authority_exposure(
    authority_comparison: Dict[str, Any],
    authority_reference: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build the exposure layer from a C66 authority comparison result.
    """
    comparisons = (authority_comparison or {}).get("comparisons") or []
    auth_topics_lookup = {}
    if authority_reference:
        for t in authority_reference.get("authority_topics") or []:
            auth_topics_lookup[t.get("topic_id")] = t

    exposure_items: List[Dict[str, Any]] = []
    exposure_counts: Dict[str, int] = {e: 0 for e in _ALL_EXPOSURES}
    implication_counts: Dict[str, int] = {}

    for comp in comparisons:
        outcome = comp.get("comparison_outcome")
        exposure = _OUTCOME_TO_EXPOSURE.get(outcome, EXPOSURE_REVIEW_REQUIRED)
        implication = _EXPOSURE_IMPLICATION.get(exposure, "estimator_review_required")

        auth_tid = comp.get("authority_topic_id")
        auth_posture = comp.get("authority_posture")
        auth_source_type = comp.get("authority_source_type")

        # Escalate: required authority + weak/ambiguous coverage = higher concern.
        if auth_posture == "required" and exposure in (EXPOSURE_WEAKLY_COVERED, EXPOSURE_AMBIGUOUSLY_COVERED):
            implication = "clarify_or_carry_internally_required_authority"

        exposure_counts[exposure] = exposure_counts.get(exposure, 0) + 1
        implication_counts[implication] = implication_counts.get(implication, 0) + 1

        exposure_items.append({
            "authority_topic_id": auth_tid,
            "authority_description": comp.get("authority_description"),
            "authority_posture": auth_posture,
            "authority_source_type": auth_source_type,
            "authority_source_ref": deepcopy(comp.get("authority_source_ref") or {}),
            "comparison_outcome": outcome,
            "exposure_level": exposure,
            "handling_implication": implication,
            "matched_scope_ref": deepcopy(comp.get("matched_scope_ref")),
        })

    return {
        "exposure_version": EXPOSURE_VERSION,
        "exposure_items": exposure_items,
        "exposure_summary": {
            "total_items": len(exposure_items),
            "exposure_counts": exposure_counts,
            "covered_count": exposure_counts.get(EXPOSURE_COVERED, 0),
            "weakly_covered_count": exposure_counts.get(EXPOSURE_WEAKLY_COVERED, 0),
            "ambiguously_covered_count": exposure_counts.get(EXPOSURE_AMBIGUOUSLY_COVERED, 0),
            "not_covered_count": exposure_counts.get(EXPOSURE_NOT_COVERED, 0),
            "review_required_count": exposure_counts.get(EXPOSURE_REVIEW_REQUIRED, 0),
            "implication_counts": dict(sorted(implication_counts.items())),
        },
    }
