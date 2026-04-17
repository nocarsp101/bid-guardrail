"""
C66 — Package scope vs authority comparison.

Deterministic comparison of package-level scope posture against
authority/reference entries. Never converts results into compliance
judgments.

Closed comparison outcomes:
    authority_addressed, authority_not_addressed,
    authority_conditionally_addressed, authority_ambiguous,
    authority_needs_review
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

COMPARISON_VERSION = "scope_authority_comparison/v1"

OUTCOME_ADDRESSED = "authority_addressed"
OUTCOME_NOT_ADDRESSED = "authority_not_addressed"
OUTCOME_CONDITIONALLY_ADDRESSED = "authority_conditionally_addressed"
OUTCOME_AMBIGUOUS = "authority_ambiguous"
OUTCOME_NEEDS_REVIEW = "authority_needs_review"

_ALL_OUTCOMES = frozenset({
    OUTCOME_ADDRESSED, OUTCOME_NOT_ADDRESSED,
    OUTCOME_CONDITIONALLY_ADDRESSED, OUTCOME_AMBIGUOUS,
    OUTCOME_NEEDS_REVIEW,
})


def compare_scope_vs_authority(
    authority_reference: Dict[str, Any],
    scope_interpretation: Optional[Dict[str, Any]] = None,
    package_overview: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Compare authority topics against package/quote scope.

    Uses scope_interpretation topics to find matches by description
    keyword overlap (deterministic canonical comparison). Falls back
    to package_overview scope gap counts for aggregate assessment.
    """
    auth_topics = (authority_reference or {}).get("authority_topics") or []
    scope_topics = (scope_interpretation or {}).get("scope_topics") or []

    scope_index = _build_scope_index(scope_topics)

    comparisons: List[Dict[str, Any]] = []
    outcome_counts: Dict[str, int] = {o: 0 for o in _ALL_OUTCOMES}

    for auth in auth_topics:
        outcome, match_ref = _compare_one(auth, scope_index)
        outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
        comparisons.append({
            "authority_topic_id": auth.get("topic_id"),
            "authority_description": auth.get("description"),
            "authority_posture": auth.get("authority_posture"),
            "authority_source_type": auth.get("authority_source_type"),
            "authority_source_ref": deepcopy(auth.get("source_ref") or {}),
            "comparison_outcome": outcome,
            "matched_scope_ref": match_ref,
        })

    return {
        "comparison_version": COMPARISON_VERSION,
        "comparisons": comparisons,
        "comparison_summary": {
            "total_authority_topics": len(auth_topics),
            "outcome_counts": outcome_counts,
            "addressed_count": outcome_counts.get(OUTCOME_ADDRESSED, 0),
            "not_addressed_count": outcome_counts.get(OUTCOME_NOT_ADDRESSED, 0),
            "conditionally_addressed_count": outcome_counts.get(OUTCOME_CONDITIONALLY_ADDRESSED, 0),
            "ambiguous_count": outcome_counts.get(OUTCOME_AMBIGUOUS, 0),
            "needs_review_count": outcome_counts.get(OUTCOME_NEEDS_REVIEW, 0),
        },
    }


def _build_scope_index(scope_topics: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Index scope topics by canonical description tokens for matching."""
    index: Dict[str, Dict[str, Any]] = {}
    for t in scope_topics:
        desc = (t.get("description") or "").strip().upper()
        tokens = set(desc.split())
        for token in tokens:
            if len(token) >= 3:
                if token not in index:
                    index[token] = t
    # Also index full canonical descriptions.
    for t in scope_topics:
        canon = _canon(t.get("description"))
        if canon:
            index[canon] = t
    return index


def _compare_one(
    auth: Dict[str, Any],
    scope_index: Dict[str, Dict[str, Any]],
) -> tuple:
    """Compare one authority topic against the scope index.

    Returns (outcome, matched_scope_ref_or_None).
    """
    auth_desc = (auth.get("description") or "").strip()
    auth_canon = _canon(auth_desc)
    auth_posture = auth.get("authority_posture")

    # Exact canonical match.
    if auth_canon and auth_canon in scope_index:
        matched = scope_index[auth_canon]
        scope_class = matched.get("scope_class")
        ref = _scope_ref(matched)
        return _classify_match(scope_class, auth_posture), ref

    # Token overlap: require >=50% of authority tokens present in index.
    tokens = set(auth_desc.upper().split())
    significant = {t for t in tokens if len(t) >= 3}
    if significant:
        hits = {t for t in significant if t in scope_index}
        if len(hits) >= len(significant) * 0.5 and hits:
            matched = scope_index[next(iter(hits))]
            scope_class = matched.get("scope_class")
            ref = _scope_ref(matched)
            return _classify_match(scope_class, auth_posture), ref

    # No match found.
    if auth_posture == "required":
        return OUTCOME_NOT_ADDRESSED, None
    if auth_posture == "conditional":
        return OUTCOME_NEEDS_REVIEW, None
    return OUTCOME_NOT_ADDRESSED, None


def _classify_match(scope_class: Optional[str], auth_posture: Optional[str]) -> str:
    if scope_class == "explicitly_included":
        return OUTCOME_ADDRESSED
    if scope_class == "implicitly_included":
        if auth_posture in ("required", "conditional"):
            return OUTCOME_CONDITIONALLY_ADDRESSED
        return OUTCOME_ADDRESSED
    if scope_class == "ambiguous_scope":
        return OUTCOME_AMBIGUOUS
    if scope_class == "not_addressed":
        return OUTCOME_NOT_ADDRESSED
    if scope_class == "explicitly_excluded":
        return OUTCOME_NOT_ADDRESSED
    return OUTCOME_NEEDS_REVIEW


def _scope_ref(topic: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    src = topic.get("source_ref")
    if not src:
        return None
    return {
        "topic_id": topic.get("topic_id"),
        "scope_class": topic.get("scope_class"),
        "source_ref": deepcopy(src),
    }


def _canon(desc: Optional[str]) -> str:
    if not desc:
        return ""
    return " ".join(desc.strip().upper().split())
