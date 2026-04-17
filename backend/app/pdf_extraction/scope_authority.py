"""
C65 — Scope requirement reference layer.

Normalized authority/reference layer for scope topics. Separate from
quote/package truth — never infers quote failure.

Closed authority source types:
    dot, sudas, special_provision, plan_note, estimate_reference,
    internal_estimator_reference

Closed authority posture:
    required, conditional, allowance_note, incidental_candidate, review_required
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

AUTHORITY_VERSION = "scope_authority/v1"

SRC_DOT = "dot"
SRC_SUDAS = "sudas"
SRC_SPECIAL_PROVISION = "special_provision"
SRC_PLAN_NOTE = "plan_note"
SRC_ESTIMATE_REFERENCE = "estimate_reference"
SRC_INTERNAL_ESTIMATOR = "internal_estimator_reference"

_ALL_SOURCE_TYPES = frozenset({
    SRC_DOT, SRC_SUDAS, SRC_SPECIAL_PROVISION, SRC_PLAN_NOTE,
    SRC_ESTIMATE_REFERENCE, SRC_INTERNAL_ESTIMATOR,
})

POSTURE_REQUIRED = "required"
POSTURE_CONDITIONAL = "conditional"
POSTURE_ALLOWANCE_NOTE = "allowance_note"
POSTURE_INCIDENTAL_CANDIDATE = "incidental_candidate"
POSTURE_REVIEW_REQUIRED = "review_required"

_ALL_POSTURES = frozenset({
    POSTURE_REQUIRED, POSTURE_CONDITIONAL, POSTURE_ALLOWANCE_NOTE,
    POSTURE_INCIDENTAL_CANDIDATE, POSTURE_REVIEW_REQUIRED,
})


def build_authority_reference(
    entries: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Build the authority reference from a list of entry dicts.

    Each entry shaped:
        {
            "topic_id": "...",
            "description": "...",
            "authority_source_type": "dot" | "sudas" | ...,
            "authority_posture": "required" | "conditional" | ...,
            "source_ref": { ... },
            "note": "...",
        }
    """
    topics: List[Dict[str, Any]] = []
    unknown_sources: List[str] = []
    unknown_postures: List[str] = []
    posture_counts: Dict[str, int] = {p: 0 for p in _ALL_POSTURES}
    source_counts: Dict[str, int] = {}

    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            continue
        tid = entry.get("topic_id") or f"auth-{i}"
        src_type = entry.get("authority_source_type")
        posture = entry.get("authority_posture")

        src_valid = src_type in _ALL_SOURCE_TYPES
        posture_valid = posture in _ALL_POSTURES

        if not src_valid:
            unknown_sources.append(str(src_type))
        if not posture_valid:
            unknown_postures.append(str(posture))

        effective_posture = posture if posture_valid else POSTURE_REVIEW_REQUIRED
        if effective_posture in posture_counts:
            posture_counts[effective_posture] += 1

        source_counts[src_type or "unknown"] = source_counts.get(src_type or "unknown", 0) + 1

        topics.append({
            "topic_id": tid,
            "description": entry.get("description"),
            "authority_source_type": src_type,
            "authority_source_valid": src_valid,
            "authority_posture": effective_posture,
            "authority_posture_valid": posture_valid,
            "source_ref": deepcopy(entry.get("source_ref") or {}),
            "note": entry.get("note"),
        })

    return {
        "authority_version": AUTHORITY_VERSION,
        "authority_topics": topics,
        "authority_summary": {
            "total_topics": len(topics),
            "posture_counts": posture_counts,
            "source_type_counts": dict(sorted(source_counts.items())),
        },
        "authority_diagnostics": {
            "unknown_source_types": sorted(set(unknown_sources)),
            "unknown_postures": sorted(set(unknown_postures)),
        },
    }
