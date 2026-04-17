"""
C46 — Handwritten review control-room contract.

Backend/view-state contract for reviewing handwritten/manual quote intake.
Parallels C38's interaction model but focused on the manual interpretation
workflow: evidence blocks, manual entry queue, approval state, and
unresolved evidence.

Closed view vocabulary:
    - intake_overview
    - unreadable_blocks
    - partial_blocks
    - manual_entry_queue
    - approval_queue
    - approved_manual_rows
    - unresolved_evidence_blocks
    - hybrid_evaluation_preview

Default-view derivation (deterministic cascade):
    1. approval_queue       — when unapproved manual rows exist
    2. unreadable_blocks    — when unreadable blocks with no manual entries exist
    3. partial_blocks       — when partial blocks remain unresolved
    4. hybrid_evaluation_preview — when approved rows + unresolved evidence exist
    5. intake_overview      — fallback
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

HANDWRITTEN_REVIEW_VERSION = "handwritten_review_control_room/v1"

VIEW_INTAKE_OVERVIEW = "intake_overview"
VIEW_UNREADABLE_BLOCKS = "unreadable_blocks"
VIEW_PARTIAL_BLOCKS = "partial_blocks"
VIEW_MANUAL_ENTRY_QUEUE = "manual_entry_queue"
VIEW_APPROVAL_QUEUE = "approval_queue"
VIEW_APPROVED_MANUAL_ROWS = "approved_manual_rows"
VIEW_UNRESOLVED_EVIDENCE = "unresolved_evidence_blocks"
VIEW_HYBRID_PREVIEW = "hybrid_evaluation_preview"

_ALL_VIEWS = [
    VIEW_INTAKE_OVERVIEW,
    VIEW_UNREADABLE_BLOCKS,
    VIEW_PARTIAL_BLOCKS,
    VIEW_MANUAL_ENTRY_QUEUE,
    VIEW_APPROVAL_QUEUE,
    VIEW_APPROVED_MANUAL_ROWS,
    VIEW_UNRESOLVED_EVIDENCE,
    VIEW_HYBRID_PREVIEW,
]


def build_handwritten_review(
    intake_output: Dict[str, Any],
    manual_store: Optional[Dict[str, Any]] = None,
    approval_state: Optional[Dict[str, Any]] = None,
    hybrid_summary: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build the handwritten/manual review control-room contract.

    All inputs are deep-copied on read; never mutated.
    """
    intake = deepcopy(intake_output or {})
    manual = deepcopy(manual_store or {})
    approval = deepcopy(approval_state or {})

    blocks = intake.get("evidence_blocks") or []
    intake_status = intake.get("machine_intake_status")

    unreadable_blocks = [b for b in blocks if b.get("machine_readability") == "unreadable"]
    partial_blocks = [b for b in blocks if b.get("machine_readability") == "partial"]
    readable_blocks = [b for b in blocks if b.get("machine_readability") == "readable"]

    # Manual entries + approval state.
    all_entries = manual.get("entries") or []
    active_entries = [e for e in all_entries if e.get("entry_status") == "active"]
    superseded_entries = [e for e in all_entries if e.get("entry_status") == "superseded"]
    rejected_entries = [e for e in all_entries if e.get("entry_status") == "rejected"]

    approval_summary = approval.get("approval_summary") or {}
    approved_count = int(approval_summary.get("approved_rows_count") or 0)
    unapproved_count = int(approval_summary.get("unapproved_rows_count") or 0)

    # Blocks that have at least one active manual entry (by block_id).
    manual_block_ids = set()
    for e in active_entries:
        bid = (e.get("source_block_ref") or {}).get("block_id")
        if bid:
            manual_block_ids.add(bid)

    unresolved_blocks = [
        b for b in blocks
        if b.get("machine_readability") in ("partial", "unreadable")
        and b.get("block_id") not in manual_block_ids
    ]

    default_view = _derive_default_view(
        unapproved_count=unapproved_count,
        active_entry_count=len(active_entries),
        unreadable_blocks=unreadable_blocks,
        partial_blocks=partial_blocks,
        unresolved_blocks=unresolved_blocks,
        approved_count=approved_count,
    )

    view_summaries = {
        VIEW_INTAKE_OVERVIEW: {
            "document_status": intake_status,
            "accepted_rows_count": intake.get("intake_summary", {}).get("accepted_rows_count", 0),
            "evidence_blocks_count": len(blocks),
            "page_count": intake.get("intake_summary", {}).get("page_count", 0),
        },
        VIEW_UNREADABLE_BLOCKS: {"block_count": len(unreadable_blocks)},
        VIEW_PARTIAL_BLOCKS: {"block_count": len(partial_blocks)},
        VIEW_MANUAL_ENTRY_QUEUE: {
            "active_entries": len(active_entries),
            "superseded_entries": len(superseded_entries),
            "rejected_entries": len(rejected_entries),
        },
        VIEW_APPROVAL_QUEUE: {
            "unapproved_count": unapproved_count,
            "approved_count": approved_count,
        },
        VIEW_APPROVED_MANUAL_ROWS: {"approved_count": approved_count},
        VIEW_UNRESOLVED_EVIDENCE: {"unresolved_block_count": len(unresolved_blocks)},
        VIEW_HYBRID_PREVIEW: deepcopy(hybrid_summary) if hybrid_summary else {"present": False},
    }

    # Row index: every block + every active manual entry, with reachability info.
    block_index = [
        {
            "block_id": b.get("block_id"),
            "source_page": b.get("source_page"),
            "machine_readability": b.get("machine_readability"),
            "has_manual_entry": b.get("block_id") in manual_block_ids,
            "view_bucket": (
                VIEW_UNREADABLE_BLOCKS if b.get("machine_readability") == "unreadable" else
                VIEW_PARTIAL_BLOCKS if b.get("machine_readability") == "partial" else
                VIEW_INTAKE_OVERVIEW
            ),
        }
        for b in blocks
    ]

    manual_row_index = [
        {
            "manual_entry_id": e.get("manual_entry_id"),
            "manual_row_key": e.get("manual_row_key"),
            "entry_status": e.get("entry_status"),
            "source_block_id": (e.get("source_block_ref") or {}).get("block_id"),
            "approval_status": _get_approval_status(e.get("manual_entry_id"), approval),
        }
        for e in all_entries
    ]

    return {
        "handwritten_review_version": HANDWRITTEN_REVIEW_VERSION,
        "document_status": intake_status,
        "default_view": default_view,
        "available_views": list(_ALL_VIEWS),
        "view_state": {
            "selected_view": default_view,
            "expanded_sections": [default_view],
        },
        "view_summaries": view_summaries,
        "block_index": block_index,
        "manual_row_index": manual_row_index,
        "intake_summary": deepcopy(intake.get("intake_summary") or {}),
        "manual_interpretation_summary": deepcopy(manual.get("summary") or {}),
        "approval_summary": deepcopy(approval_summary),
        "unresolved_evidence_summary": {
            "unresolved_block_count": len(unresolved_blocks),
            "total_partial_or_unreadable": len(partial_blocks) + len(unreadable_blocks),
            "blocks_with_manual_entries": len(manual_block_ids),
        },
        "review_diagnostics": {
            "blocks_total": len(blocks),
            "entries_total": len(all_entries),
            "default_view_basis": _default_view_basis(
                unapproved_count, len(active_entries),
                unreadable_blocks, partial_blocks, unresolved_blocks, approved_count,
            ),
        },
    }


# ---------------------------------------------------------------------------
# Default view cascade
# ---------------------------------------------------------------------------

def _derive_default_view(
    unapproved_count: int,
    active_entry_count: int,
    unreadable_blocks: list,
    partial_blocks: list,
    unresolved_blocks: list,
    approved_count: int,
) -> str:
    if active_entry_count > 0 and unapproved_count > 0:
        return VIEW_APPROVAL_QUEUE
    if unreadable_blocks and active_entry_count == 0:
        return VIEW_UNREADABLE_BLOCKS
    if unresolved_blocks:
        if partial_blocks:
            return VIEW_PARTIAL_BLOCKS
        return VIEW_UNRESOLVED_EVIDENCE
    if approved_count > 0:
        return VIEW_HYBRID_PREVIEW
    return VIEW_INTAKE_OVERVIEW


def _default_view_basis(
    unapproved_count, active_entry_count,
    unreadable_blocks, partial_blocks, unresolved_blocks, approved_count,
) -> str:
    if active_entry_count > 0 and unapproved_count > 0:
        return f"unapproved_manual_entries={unapproved_count}>0"
    if unreadable_blocks and active_entry_count == 0:
        return f"unreadable_blocks={len(unreadable_blocks)}>0_no_manual_entries"
    if unresolved_blocks and partial_blocks:
        return f"partial_blocks={len(partial_blocks)}>0_unresolved"
    if unresolved_blocks:
        return f"unresolved_evidence_blocks={len(unresolved_blocks)}>0"
    if approved_count > 0:
        return f"approved_count={approved_count}>0_preview_available"
    return "no_actionable_items_fallback_overview"


def _get_approval_status(
    manual_entry_id: Optional[str],
    approval: Dict[str, Any],
) -> Optional[str]:
    if not manual_entry_id:
        return None
    entry_approvals = approval.get("entry_approvals") or {}
    info = entry_approvals.get(manual_entry_id) or {}
    return info.get("approval_status")
