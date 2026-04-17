"""
C89 — Revision diff / change inspection.

Deterministic diff artifacts across revisions for readiness, package
confidence, authority posture, carry decision, clarifications, and key
summary counts. Surfaces before/after values, changed fields, unchanged
identity refs, and linked source refs. Never infers history beyond
canonical records; diffs only what the repository holds.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

REVISION_DIFF_VERSION = "revision_diff/v1"

_WATCHED_FIELDS_BY_TYPE: Dict[str, List[Tuple[str, ...]]] = {
    "bid_readiness_snapshot": [
        ("readiness_state",), ("readiness_level",), ("gate_outcome",),
        ("risk_level",), ("blocker_count",), ("caveat_count",),
        ("overall_status",),
    ],
    "package_overview": [
        ("package_confidence",), ("confidence_level",),
        ("gate_outcome",), ("risk_level",), ("vendor_count",),
        ("unresolved_count",), ("blocker_count",),
    ],
    "authority_posture": [
        ("posture",), ("authority_status",), ("exposure_level",),
        ("unresolved_authority_count",),
    ],
    "bid_carry_justification": [
        ("carry_decision",), ("carry_state",), ("justification_level",),
        ("carry_amount",), ("contingency_amount",),
    ],
    "quote_dossier": [
        ("risk_level",), ("gate_outcome",),
        ("clarification_count",), ("open_clarifications",),
        ("resolved_clarifications",),
    ],
    "priority_queue": [
        ("total_items",), ("blocking_items",), ("top_priority",),
    ],
    "deadline_pressure": [
        ("pressure_level",), ("days_remaining",),
    ],
    "authority_action_packet": [
        ("action_count",), ("authority_status",),
    ],
    "vendor_comparison": [
        ("selected_vendor",), ("vendor_count",), ("comparison_state",),
    ],
    "export_packet": [
        ("export_type",), ("item_count",),
    ],
}

_IDENTITY_KEYS = ("bid_id", "job_id", "package_ref", "record_id", "artifact_type")


def diff_revisions(
    before_record: Optional[Dict[str, Any]],
    after_record: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Deterministic diff between two canonical records.

    Each record is the append-only envelope stored by the repository.
    Returns a closed-vocabulary diff artifact. Does not infer history.
    """
    if before_record is None and after_record is None:
        return _empty_diff("both_missing")
    if before_record is None:
        return _initial_diff(after_record)
    if after_record is None:
        return _empty_diff("after_missing", before=before_record)

    before_art = _artifact_of(before_record)
    after_art = _artifact_of(after_record)
    artifact_type = after_record.get("artifact_type") or before_record.get("artifact_type")

    watched = _WATCHED_FIELDS_BY_TYPE.get(artifact_type or "", [])
    changed: List[Dict[str, Any]] = []
    unchanged: List[Dict[str, Any]] = []

    for path in watched:
        b_val = _get_path(before_art, path)
        a_val = _get_path(after_art, path)
        entry = {
            "field_path": ".".join(path),
            "before": b_val,
            "after": a_val,
        }
        if b_val != a_val:
            changed.append(entry)
        else:
            unchanged.append(entry)

    identity_refs = _identity_refs(before_record, after_record)
    identity_unchanged = _identity_unchanged(identity_refs)

    source_refs = {
        "before_record_id": before_record.get("record_id"),
        "after_record_id": after_record.get("record_id"),
        "before_revision": before_record.get("revision_sequence"),
        "after_revision": after_record.get("revision_sequence"),
        "supersedes": after_record.get("supersedes"),
    }

    return {
        "revision_diff_version": REVISION_DIFF_VERSION,
        "artifact_type": artifact_type,
        "status": "changed" if changed else "unchanged",
        "changed_fields": changed,
        "unchanged_fields": unchanged,
        "identity_refs": identity_refs,
        "identity_unchanged": identity_unchanged,
        "source_refs": source_refs,
    }


def diff_lineage(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Diff every adjacent pair in a lineage-ordered list."""
    out: List[Dict[str, Any]] = []
    for i in range(1, len(records or [])):
        out.append(diff_revisions(records[i - 1], records[i]))
    return out


def diff_summary(diff: Dict[str, Any]) -> Dict[str, Any]:
    """Compact summary of a single diff."""
    return {
        "revision_diff_version": REVISION_DIFF_VERSION,
        "artifact_type": diff.get("artifact_type"),
        "status": diff.get("status"),
        "changed_field_count": len(diff.get("changed_fields") or []),
        "unchanged_field_count": len(diff.get("unchanged_fields") or []),
        "identity_unchanged": diff.get("identity_unchanged"),
        "before_revision": (diff.get("source_refs") or {}).get("before_revision"),
        "after_revision": (diff.get("source_refs") or {}).get("after_revision"),
    }


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _artifact_of(record: Dict[str, Any]) -> Dict[str, Any]:
    env = record.get("envelope") or {}
    art = env.get("artifact") or {}
    return art if isinstance(art, dict) else {}


def _get_path(artifact: Dict[str, Any], path: Tuple[str, ...]) -> Any:
    cur: Any = artifact
    for seg in path:
        if isinstance(cur, dict):
            cur = cur.get(seg)
        else:
            return None
    return deepcopy(cur) if isinstance(cur, (dict, list)) else cur


def _identity_refs(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    before_art = _artifact_of(before)
    after_art = _artifact_of(after)
    refs: Dict[str, Any] = {}
    for key in _IDENTITY_KEYS:
        b = before_art.get(key) if key not in ("record_id", "artifact_type") else before.get(key)
        a = after_art.get(key) if key not in ("record_id", "artifact_type") else after.get(key)
        refs[key] = {"before": b, "after": a}
    return refs


def _identity_unchanged(identity_refs: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for key, pair in identity_refs.items():
        if key == "record_id":
            continue  # record_id is always expected to differ between revisions
        if pair.get("before") == pair.get("after") and pair.get("before") is not None:
            out.append(key)
    return sorted(out)


def _empty_diff(reason: str, before: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "revision_diff_version": REVISION_DIFF_VERSION,
        "artifact_type": (before or {}).get("artifact_type"),
        "status": "no_diff",
        "reason": reason,
        "changed_fields": [],
        "unchanged_fields": [],
        "identity_refs": {},
        "identity_unchanged": [],
        "source_refs": {
            "before_record_id": (before or {}).get("record_id"),
            "after_record_id": None,
        },
    }


def _initial_diff(after_record: Dict[str, Any]) -> Dict[str, Any]:
    artifact_type = after_record.get("artifact_type")
    watched = _WATCHED_FIELDS_BY_TYPE.get(artifact_type or "", [])
    after_art = _artifact_of(after_record)
    changed = [{
        "field_path": ".".join(path),
        "before": None,
        "after": _get_path(after_art, path),
    } for path in watched]
    return {
        "revision_diff_version": REVISION_DIFF_VERSION,
        "artifact_type": artifact_type,
        "status": "initial",
        "changed_fields": changed,
        "unchanged_fields": [],
        "identity_refs": {
            key: {"before": None, "after": after_art.get(key)
                   if key not in ("record_id", "artifact_type")
                   else after_record.get(key)}
            for key in _IDENTITY_KEYS
        },
        "identity_unchanged": [],
        "source_refs": {
            "before_record_id": None,
            "after_record_id": after_record.get("record_id"),
            "before_revision": None,
            "after_revision": after_record.get("revision_sequence"),
            "supersedes": after_record.get("supersedes"),
        },
    }
