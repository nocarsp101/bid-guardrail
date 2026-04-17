"""
C39 — Persistence + versioned job state.

Append-safe in-memory store contract for control-room job states. Each
store carries an immutable, ordered list of revisions. Older revisions
are NEVER mutated; new state is always appended as a new revision.

The module operates on plain dicts so callers can persist them with
any serialization layer (JSON file, database, etc.) without coupling
this module to storage implementation. Disk I/O is intentionally NOT
owned here.

Closed revision-type vocabulary:
    - initial_run
    - action_update
    - recompute
    - manual_note
    - import_external_source

Hard rules:
    - Append-only. `append_revision` returns a NEW store dict; the
      input store is never mutated and the input new_state is deep-copied.
    - Revision ids are stable: caller-supplied OR generated as
      `rev-<sequence>` where sequence starts at 0.
    - Change summaries compare the previous current revision against
      the new state and report deterministic counts (rows changed,
      actions added, status changes). They never speculate about why.
    - Loading a revision returns a deep copy so callers cannot mutate
      historical state.
    - Empty / missing inputs are handled deterministically.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

STORE_VERSION = "job_state_store/v1"

# Closed revision-type vocabulary.
REV_INITIAL_RUN = "initial_run"
REV_ACTION_UPDATE = "action_update"
REV_RECOMPUTE = "recompute"
REV_MANUAL_NOTE = "manual_note"
REV_IMPORT_EXTERNAL_SOURCE = "import_external_source"

_ALL_REVISION_TYPES = frozenset({
    REV_INITIAL_RUN, REV_ACTION_UPDATE, REV_RECOMPUTE,
    REV_MANUAL_NOTE, REV_IMPORT_EXTERNAL_SOURCE,
})


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_job_state(
    job_id: str,
    control_room_output: Dict[str, Any],
    revision_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a fresh store for a new job.

    Args:
        job_id: caller-supplied stable job identifier.
        control_room_output: a C35 control room dict.
        revision_metadata: optional caller metadata dict shaped:
            {
                "revision_id": "...",     # optional — defaults to "rev-0"
                "revision_type": "...",   # defaults to initial_run
                "created_at": "...",      # caller-supplied timestamp
                "revision_reason": "...", # short templated reason
                "actor": "...",
            }
    """
    rev = _build_revision(
        sequence=0,
        previous_state=None,
        new_state=control_room_output,
        revision_metadata=revision_metadata,
        default_revision_type=REV_INITIAL_RUN,
    )
    return {
        "store_version": STORE_VERSION,
        "job_id": job_id,
        "current_revision": rev["revision_id"],
        "current_revision_index": 0,
        "revisions": [rev],
        "store_diagnostics": {
            "revision_count": 1,
            "appended_revision_types": [rev["revision_type"]],
        },
    }


def append_revision(
    existing_store: Dict[str, Any],
    new_state: Dict[str, Any],
    revision_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Append a new revision to an existing store.

    Returns a NEW store dict. The input store is never mutated.
    """
    if not existing_store:
        raise ValueError("append_revision requires an existing store")

    out = deepcopy(existing_store)
    revisions = out.get("revisions") or []
    if not revisions:
        raise ValueError("store has no revisions to append after")

    sequence = len(revisions)
    previous_state = revisions[-1]["job_state"]
    rev = _build_revision(
        sequence=sequence,
        previous_state=previous_state,
        new_state=new_state,
        revision_metadata=revision_metadata,
        default_revision_type=REV_ACTION_UPDATE,
    )
    revisions.append(rev)
    out["revisions"] = revisions
    out["current_revision"] = rev["revision_id"]
    out["current_revision_index"] = sequence
    out["store_diagnostics"] = {
        "revision_count": len(revisions),
        "appended_revision_types": [r["revision_type"] for r in revisions],
    }
    return out


def load_current_revision(store: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return a deep copy of the current revision dict, or None if empty."""
    if not store:
        return None
    revisions = store.get("revisions") or []
    if not revisions:
        return None
    idx = store.get("current_revision_index")
    if not isinstance(idx, int) or idx < 0 or idx >= len(revisions):
        idx = len(revisions) - 1
    return deepcopy(revisions[idx])


def load_revision_history(store: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return a deep copy of the full ordered revision list."""
    if not store:
        return []
    return deepcopy(store.get("revisions") or [])


def load_revision(store: Dict[str, Any], revision_id: str) -> Optional[Dict[str, Any]]:
    """Return a deep copy of one revision by id, or None if not found."""
    if not store:
        return None
    for rev in store.get("revisions") or []:
        if rev.get("revision_id") == revision_id:
            return deepcopy(rev)
    return None


# ---------------------------------------------------------------------------
# Revision construction
# ---------------------------------------------------------------------------

def _build_revision(
    sequence: int,
    previous_state: Optional[Dict[str, Any]],
    new_state: Dict[str, Any],
    revision_metadata: Optional[Dict[str, Any]],
    default_revision_type: str,
) -> Dict[str, Any]:
    metadata = revision_metadata or {}
    revision_id = str(metadata.get("revision_id") or f"rev-{sequence}")

    raw_revision_type = metadata.get("revision_type") or default_revision_type
    if raw_revision_type not in _ALL_REVISION_TYPES:
        revision_type = raw_revision_type  # preserve unknown literal
        revision_validation = "unknown_revision_type"
    else:
        revision_type = raw_revision_type
        revision_validation = "valid"

    change_summary = _build_change_summary(previous_state, new_state)

    return {
        "revision_id": revision_id,
        "sequence": sequence,
        "revision_type": revision_type,
        "revision_validation_status": revision_validation,
        "created_at": metadata.get("created_at"),
        "actor": metadata.get("actor"),
        "revision_reason": metadata.get("revision_reason"),
        "job_state": deepcopy(new_state) if new_state is not None else None,
        "change_summary": change_summary,
    }


def _build_change_summary(
    previous: Optional[Dict[str, Any]],
    new: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Deterministic comparison between two job states.

    Both inputs are expected to be C35 control-room dicts (or None for
    the initial revision). The summary reports counts only — never
    speculates about why anything changed.
    """
    new = new or {}
    if previous is None:
        return {
            "is_initial": True,
            "rows_changed_count": 0,
            "actions_added_count": _count_actions(new),
            "status_changed": False,
            "previous_job_status": None,
            "current_job_status": new.get("job_status"),
        }

    prev_rows = _resolution_rows(previous)
    new_rows = _resolution_rows(new)
    rows_changed = _count_rows_changed(prev_rows, new_rows)

    prev_actions = _count_actions(previous)
    new_actions = _count_actions(new)

    return {
        "is_initial": False,
        "rows_changed_count": rows_changed,
        "actions_added_count": max(0, new_actions - prev_actions),
        "previous_actions_count": prev_actions,
        "current_actions_count": new_actions,
        "status_changed": previous.get("job_status") != new.get("job_status"),
        "previous_job_status": previous.get("job_status"),
        "current_job_status": new.get("job_status"),
    }


def _resolution_rows(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    return ((state.get("resolution") or {}).get("resolution_rows")) or []


def _count_actions(state: Dict[str, Any]) -> int:
    rows = ((state.get("office_actions_output") or {}).get("resolution_rows")) or []
    return sum(len(r.get("office_actions") or []) for r in rows)


def _count_rows_changed(
    prev_rows: List[Dict[str, Any]],
    new_rows: List[Dict[str, Any]],
) -> int:
    """Count rows whose comparable projection differs between revisions.

    Compares (resolution_category, resolution_priority, comparison_basis)
    by normalized_row_id. Rows added, removed, or reclassified all count.
    """
    def project(row: Dict[str, Any]) -> tuple:
        return (
            row.get("resolution_category"),
            row.get("resolution_priority"),
            ((row.get("comparison_basis") or {}).get("basis")),
        )

    prev_map = {r.get("normalized_row_id"): project(r) for r in prev_rows}
    new_map = {r.get("normalized_row_id"): project(r) for r in new_rows}

    changed = 0
    all_ids = set(prev_map) | set(new_map)
    for rid in all_ids:
        if prev_map.get(rid) != new_map.get(rid):
            changed += 1
    return changed
