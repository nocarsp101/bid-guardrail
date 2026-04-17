"""
C45 — Manual interpretation approval gate.

Append-only approval layer over C42 manual interpretation entries.
Manual entries start as `draft_manual_interpretation` and must be
explicitly approved (or rejected) before the hybrid evaluation pipeline
treats them as effective rows.

Hard rules:
    - Approvals are append-only. A new approval record is appended;
      the underlying manual entry is NEVER mutated.
    - No auto-approval. Every draft manual entry is excluded from
      effective hybrid rows by default.
    - Rejected entries remain in history with their rejection trace.
    - Superseded manual entries (from C42) cannot be approved — only
      the current active entry on a manual_row_key can be approved.
    - Unknown manual_entry_ids are surfaced in diagnostics.
    - Inputs are deep-copied on read; never mutated.

Closed approval-status vocabulary:
    - draft_manual_interpretation   (initial state; not yet reviewed)
    - submitted_for_review          (optional intermediate state)
    - approved_for_evaluation       (cleared for hybrid pipeline)
    - rejected_interpretation       (rejected; stays in history)
    - superseded                    (underlying entry superseded in C42)
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

APPROVAL_VERSION = "manual_interpretation_approval/v1"

STATUS_DRAFT = "draft_manual_interpretation"
STATUS_SUBMITTED = "submitted_for_review"
STATUS_APPROVED = "approved_for_evaluation"
STATUS_REJECTED = "rejected_interpretation"
STATUS_SUPERSEDED = "superseded"

_ALL_APPROVAL_STATUSES = frozenset({
    STATUS_DRAFT, STATUS_SUBMITTED, STATUS_APPROVED,
    STATUS_REJECTED, STATUS_SUPERSEDED,
})


def build_approval_state(
    manual_store: Dict[str, Any],
    approval_actions: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Build the approval state for a C42 manual interpretation store.

    Args:
        manual_store: the C42 `create_manual_interpretation()` or
                      `append_manual_revision()` output.
        approval_actions: optional list of approval actions, each shaped:
            {
                "manual_entry_id": "...",
                "approval_status": "approved_for_evaluation" | "rejected_interpretation" | ...,
                "reviewed_by": "...",
                "reviewed_at": "...",
                "approval_note": "...",
            }

    Returns a new approval-state dict. Never mutates inputs.
    """
    manual_store = deepcopy(manual_store or {})
    entries = manual_store.get("entries") or []

    # Build the per-entry approval map. Default: everything starts as draft.
    entry_approvals: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        eid = entry.get("manual_entry_id")
        entry_status = entry.get("entry_status")
        if entry_status == "superseded":
            default_approval = STATUS_SUPERSEDED
        elif entry_status == "rejected":
            default_approval = STATUS_REJECTED
        else:
            default_approval = STATUS_DRAFT
        entry_approvals[eid] = {
            "manual_entry_id": eid,
            "approval_status": default_approval,
            "approval_history": [],
        }

    unknown_ids: List[str] = []

    # Apply approval actions in order (append-only).
    for action in (approval_actions or []):
        if not isinstance(action, dict):
            continue
        eid = action.get("manual_entry_id")
        if eid not in entry_approvals:
            unknown_ids.append(str(eid))
            continue

        record = _build_approval_record(action)
        target = entry_approvals[eid]

        # Cannot approve a superseded or C42-rejected entry.
        if target["approval_status"] in (STATUS_SUPERSEDED, STATUS_REJECTED):
            record["action_result"] = "skipped_entry_not_approvable"
            target["approval_history"].append(record)
            continue

        target["approval_history"].append(record)
        new_status = action.get("approval_status")
        if new_status in _ALL_APPROVAL_STATUSES:
            target["approval_status"] = new_status
        else:
            record["action_result"] = "unknown_approval_status"

    # Build per-entry summary.
    approved_count = 0
    unapproved_count = 0
    rejected_count = 0
    superseded_count = 0
    for info in entry_approvals.values():
        st = info["approval_status"]
        if st == STATUS_APPROVED:
            approved_count += 1
        elif st == STATUS_REJECTED:
            rejected_count += 1
        elif st == STATUS_SUPERSEDED:
            superseded_count += 1
        else:
            unapproved_count += 1

    return {
        "approval_version": APPROVAL_VERSION,
        "entry_approvals": entry_approvals,
        "approval_summary": {
            "manual_rows_total": len(entry_approvals),
            "approved_rows_count": approved_count,
            "unapproved_rows_count": unapproved_count,
            "rejected_rows_count": rejected_count,
            "superseded_rows_count": superseded_count,
        },
        "approval_diagnostics": {
            "unknown_entry_ids": sorted(set(unknown_ids)),
            "actions_applied": len(approval_actions or []),
        },
    }


def get_approved_manual_rows(
    manual_store: Dict[str, Any],
    approval_state: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Return current manual rows that are `approved_for_evaluation`."""
    from .manual_quote_interpretation import get_current_manual_rows
    all_rows = get_current_manual_rows(manual_store)
    approved_ids = _approved_entry_ids(approval_state)
    return [r for r in all_rows
            if r.get("manual_entry_ref", {}).get("manual_entry_id") in approved_ids]


def get_unapproved_manual_rows(
    manual_store: Dict[str, Any],
    approval_state: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Return current manual rows that are NOT `approved_for_evaluation`."""
    from .manual_quote_interpretation import get_current_manual_rows
    all_rows = get_current_manual_rows(manual_store)
    approved_ids = _approved_entry_ids(approval_state)
    return [r for r in all_rows
            if r.get("manual_entry_ref", {}).get("manual_entry_id") not in approved_ids]


def _approved_entry_ids(approval_state: Dict[str, Any]) -> set:
    out = set()
    for eid, info in (approval_state.get("entry_approvals") or {}).items():
        if info.get("approval_status") == STATUS_APPROVED:
            out.add(eid)
    return out


def _build_approval_record(action: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "approval_id": action.get("approval_id") or f"appr-{id(action)}",
        "manual_entry_id": action.get("manual_entry_id"),
        "approval_status": action.get("approval_status"),
        "reviewed_by": action.get("reviewed_by"),
        "reviewed_at": action.get("reviewed_at"),
        "approval_note": action.get("approval_note"),
        "action_result": "applied",
    }
