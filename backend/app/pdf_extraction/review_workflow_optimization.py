"""
C47 — Review workflow optimization.

Deterministic review prioritization, readiness status, progress metrics,
and review summary contracts on top of the C46 handwritten review
control room and C45 approval gate.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

REVIEW_WORKFLOW_OPT_VERSION = "review_workflow_optimization/v1"

READINESS_COMPLETE = "review_complete"
READINESS_ACTIONABLE = "actionable"
READINESS_BLOCKED_PENDING_MANUAL = "blocked_pending_manual_interpretation"
READINESS_BLOCKED_PENDING_APPROVAL = "blocked_pending_approval"
READINESS_BLOCKED_PENDING_EVIDENCE = "blocked_pending_evidence"
READINESS_NOT_STARTED = "not_started"

PRIORITY_CRITICAL = "critical"
PRIORITY_HIGH = "high"
PRIORITY_MEDIUM = "medium"
PRIORITY_LOW = "low"


def build_review_workflow(
    handwritten_review: Dict[str, Any],
    resolution_output: Optional[Dict[str, Any]] = None,
    office_action_output: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build optimized review workflow state."""
    hr = deepcopy(handwritten_review or {})
    resolution = deepcopy(resolution_output or {})
    actioned = deepcopy(office_action_output or {})

    approval_summary = hr.get("approval_summary") or {}
    unresolved = hr.get("unresolved_evidence_summary") or {}
    manual_summary = hr.get("manual_interpretation_summary") or {}
    intake_summary = hr.get("intake_summary") or {}

    queue_items = _build_review_queue(hr, resolution, actioned)
    progress = _build_progress_metrics(hr, resolution, actioned, queue_items)
    readiness = _derive_readiness(hr, approval_summary, unresolved, progress)

    return {
        "review_workflow_version": REVIEW_WORKFLOW_OPT_VERSION,
        "readiness_status": readiness,
        "review_queue": queue_items,
        "progress_metrics": progress,
        "review_summary": {
            "document_status": hr.get("document_status"),
            "total_evidence_blocks": int(intake_summary.get("evidence_blocks_count") or 0),
            "blocks_with_manual_entries": int(unresolved.get("blocks_with_manual_entries") or 0),
            "unresolved_blocks": int(unresolved.get("unresolved_block_count") or 0),
            "manual_rows_total": int(manual_summary.get("rows_manual_count") or 0),
            "machine_rows_total": int(manual_summary.get("rows_machine_count") or
                                      intake_summary.get("accepted_rows_count") or 0),
            "approved_count": int(approval_summary.get("approved_rows_count") or 0),
            "unapproved_count": int(approval_summary.get("unapproved_rows_count") or 0),
            "rejected_count": int(approval_summary.get("rejected_rows_count") or 0),
        },
        "review_diagnostics": {
            "queue_item_count": len(queue_items),
            "readiness_basis": _readiness_basis(hr, approval_summary, unresolved, progress),
        },
    }


def _build_review_queue(
    hr: Dict[str, Any],
    resolution: Dict[str, Any],
    actioned: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Build a prioritized review queue from all available work items."""
    items: List[Dict[str, Any]] = []
    ordinal = 0

    # Queue item type 1: unapproved manual entries needing approval.
    for mr in hr.get("manual_row_index") or []:
        if mr.get("approval_status") in ("draft_manual_interpretation", "submitted_for_review"):
            items.append({
                "queue_item_id": f"qi-{ordinal}",
                "item_type": "pending_approval",
                "priority": PRIORITY_HIGH,
                "manual_entry_id": mr.get("manual_entry_id"),
                "manual_row_key": mr.get("manual_row_key"),
                "source_block_id": mr.get("source_block_id"),
                "action_needed": "review_and_approve_or_reject",
            })
            ordinal += 1

    # Queue item type 2: unresolved evidence blocks needing manual entry.
    for b in hr.get("block_index") or []:
        if b.get("machine_readability") in ("partial", "unreadable") and not b.get("has_manual_entry"):
            items.append({
                "queue_item_id": f"qi-{ordinal}",
                "item_type": "pending_manual_interpretation",
                "priority": PRIORITY_CRITICAL if b.get("machine_readability") == "unreadable" else PRIORITY_MEDIUM,
                "block_id": b.get("block_id"),
                "source_page": b.get("source_page"),
                "action_needed": "enter_manual_interpretation",
            })
            ordinal += 1

    # Queue item type 3: resolution rows needing office action (from C33).
    actioned_rows = actioned.get("resolution_rows") or []
    for row in actioned_rows:
        has_action = bool(row.get("office_actions"))
        cat = row.get("resolution_category") or ""
        if not has_action and "review_required" in cat or "missing" in cat or "conflict" in cat:
            items.append({
                "queue_item_id": f"qi-{ordinal}",
                "item_type": "pending_office_action",
                "priority": _resolution_to_queue_priority(row.get("resolution_priority")),
                "normalized_row_id": row.get("normalized_row_id"),
                "resolution_category": cat,
                "action_needed": "record_office_action",
            })
            ordinal += 1

    _PRIO_ORDER = {PRIORITY_CRITICAL: 0, PRIORITY_HIGH: 1, PRIORITY_MEDIUM: 2, PRIORITY_LOW: 3}
    items.sort(key=lambda i: (_PRIO_ORDER.get(i["priority"], 99), i["queue_item_id"]))
    return items


def _resolution_to_queue_priority(p: Optional[str]) -> str:
    if p in (PRIORITY_CRITICAL, PRIORITY_HIGH, PRIORITY_MEDIUM, PRIORITY_LOW):
        return p
    return PRIORITY_MEDIUM


def _build_progress_metrics(
    hr: Dict[str, Any],
    resolution: Dict[str, Any],
    actioned: Dict[str, Any],
    queue_items: List[Dict[str, Any]],
) -> Dict[str, Any]:
    unresolved = hr.get("unresolved_evidence_summary") or {}
    total_blocks = int(unresolved.get("total_partial_or_unreadable") or 0)
    covered = int(unresolved.get("blocks_with_manual_entries") or 0)
    approval_summary = hr.get("approval_summary") or {}
    approved = int(approval_summary.get("approved_rows_count") or 0)
    unapproved = int(approval_summary.get("unapproved_rows_count") or 0)
    manual_total = approved + unapproved

    actioned_rows = actioned.get("resolution_rows") or []
    rows_with_actions = sum(1 for r in actioned_rows if r.get("office_actions"))
    total_resolution_rows = len(actioned_rows) or len((resolution.get("resolution_rows") or []))

    return {
        "evidence_coverage_ratio": {
            "covered": covered,
            "total": total_blocks,
            "ratio": (covered / total_blocks) if total_blocks else 1.0,
        },
        "approval_ratio": {
            "approved": approved,
            "total": manual_total,
            "ratio": (approved / manual_total) if manual_total else 1.0,
        },
        "office_action_ratio": {
            "actioned": rows_with_actions,
            "total": total_resolution_rows,
            "ratio": (rows_with_actions / total_resolution_rows) if total_resolution_rows else 1.0,
        },
        "queue_items_remaining": len(queue_items),
        "queue_priority_counts": {
            PRIORITY_CRITICAL: sum(1 for i in queue_items if i["priority"] == PRIORITY_CRITICAL),
            PRIORITY_HIGH: sum(1 for i in queue_items if i["priority"] == PRIORITY_HIGH),
            PRIORITY_MEDIUM: sum(1 for i in queue_items if i["priority"] == PRIORITY_MEDIUM),
            PRIORITY_LOW: sum(1 for i in queue_items if i["priority"] == PRIORITY_LOW),
        },
    }


def _derive_readiness(
    hr: Dict[str, Any],
    approval_summary: Dict[str, Any],
    unresolved: Dict[str, Any],
    progress: Dict[str, Any],
) -> str:
    unresolved_blocks = int(unresolved.get("unresolved_block_count") or 0)
    unapproved = int(approval_summary.get("unapproved_rows_count") or 0)
    queue_remaining = int(progress.get("queue_items_remaining") or 0)

    doc_status = hr.get("document_status")
    if doc_status in ("machine_unreadable_human_required", "machine_partial_human_required"):
        if unresolved_blocks > 0 and int(unresolved.get("blocks_with_manual_entries") or 0) == 0:
            return READINESS_NOT_STARTED
        if unresolved_blocks > 0:
            return READINESS_BLOCKED_PENDING_EVIDENCE
        if unapproved > 0:
            return READINESS_BLOCKED_PENDING_APPROVAL

    if unapproved > 0:
        return READINESS_BLOCKED_PENDING_APPROVAL
    if queue_remaining == 0:
        return READINESS_COMPLETE
    return READINESS_ACTIONABLE


def _readiness_basis(hr, approval_summary, unresolved, progress) -> str:
    unresolved_blocks = int(unresolved.get("unresolved_block_count") or 0)
    unapproved = int(approval_summary.get("unapproved_rows_count") or 0)
    queue = int(progress.get("queue_items_remaining") or 0)
    if unresolved_blocks > 0:
        return f"unresolved_blocks={unresolved_blocks}>0"
    if unapproved > 0:
        return f"unapproved_entries={unapproved}>0"
    if queue == 0:
        return "all_queue_items_resolved"
    return f"queue_items_remaining={queue}>0"
