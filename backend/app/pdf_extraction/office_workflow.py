"""
C25 — Office workflow integration layer.

Wraps a C21-prioritized findings packet in a deterministic workflow
artifact suitable for office review queues. Supports append-only
reviewer metadata (notes, dispositions, state transitions) without
mutating any governed finding.

Hard rules:
    - Workflow wraps findings, never replaces or overwrites them.
    - Reviewer metadata is APPEND-ONLY. Every note is added to a
      `review_notes` list on the relevant queue row; nothing is edited
      in place.
    - Reviewer metadata NEVER alters discrepancy_class, priority_class,
      comparison_status, mapping_outcome, or packet_status.
    - Queue rows carry a `source_finding_ref` pointer that ties the row
      back to the original findings row so the governed truth remains
      recoverable.
    - Queue ordering is deterministic: priority first (critical → high
      → medium → low → informational), then the input order of the
      findings row (which is itself deterministic after C21).
    - Lower-priority issues are never hidden. Rows in review_state
      `resolved` or `deferred` remain in the queue with that state
      surfaced in the summary.
    - Review state is a closed set: open, reviewed, resolved, deferred.
    - Append-only note shape is fixed and small.
    - Workflow packet never mutates its input (deepcopy on every entry).

The C25 layer does not own storage or persistence. Callers supply the
current review metadata blob (or None, meaning "all rows still open")
and get back a self-contained workflow packet. Storage happens in a
later layer.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional, Tuple

WORKFLOW_VERSION = "office_workflow/v1"

# Closed review state vocabulary.
STATE_OPEN = "open"
STATE_REVIEWED = "reviewed"
STATE_RESOLVED = "resolved"
STATE_DEFERRED = "deferred"
_ALL_STATES = (STATE_OPEN, STATE_REVIEWED, STATE_RESOLVED, STATE_DEFERRED)

# Closed note-type vocabulary.
NOTE_COMMENT = "comment"
NOTE_RESOLUTION = "resolution_note"
NOTE_FOLLOWUP = "followup"
_ALL_NOTE_TYPES = (NOTE_COMMENT, NOTE_RESOLUTION, NOTE_FOLLOWUP)

# Closed workflow-status vocabulary.
WORKFLOW_OPEN = "open"
WORKFLOW_IN_REVIEW = "in_review"
WORKFLOW_RESOLVED_PARTIAL = "resolved_partial"
WORKFLOW_RESOLVED_COMPLETE = "resolved_complete"

# Priority ordering used for queue sorting.
_PRIORITY_ORDER = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
    "informational": 4,
}


def build_workflow_packet(
    findings_packet: Dict[str, Any],
    review_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a deterministic workflow packet from a prioritized findings
    packet plus optional reviewer metadata.

    Args:
        findings_packet:  a C19 findings packet, ideally already passed
                          through C21 `prioritize_findings_packet` so
                          every row carries a priority_class.
        review_metadata:  optional dict shaped like:
                          {
                              "rows": {
                                  "<normalized_row_id>": {
                                      "review_state": "open"|"reviewed"
                                                     |"resolved"|"deferred",
                                      "review_disposition": str | None,
                                      "notes": [ note_dict, ... ],
                                  },
                                  ...
                              }
                          }
                          Notes are append-only. Unknown states default
                          to "open". Unknown row ids are ignored (the
                          workflow layer never invents queue rows).

    Returns a stable workflow packet dict. Never mutates its inputs.
    """
    findings_packet = deepcopy(findings_packet or {})
    review_metadata = review_metadata or {}
    review_rows_meta = (review_metadata.get("rows") or {}) if isinstance(review_metadata, dict) else {}

    findings_rows = findings_packet.get("findings_rows") or []
    queue_rows = _build_queue_rows(findings_rows, review_rows_meta)
    queue_rows = _stable_sort_queue(queue_rows)

    queue_summary = _build_queue_summary(queue_rows)
    workflow_status = _derive_workflow_status(queue_summary, findings_packet.get("packet_status"))

    return {
        "workflow_version": WORKFLOW_VERSION,
        "workflow_status": workflow_status,
        "packet_status": findings_packet.get("packet_status"),
        "packet_version": findings_packet.get("packet_version"),
        "prioritization_version": findings_packet.get("prioritization_version"),
        "queue_summary": queue_summary,
        "queue_rows": queue_rows,
        "workflow_diagnostics": {
            "findings_row_count": len(findings_rows),
            "review_metadata_rows_applied": sum(
                1 for r in queue_rows if r["review_state"] != STATE_OPEN or r["review_notes"]
            ),
            "unknown_review_row_ids": _collect_unknown_ids(review_rows_meta, findings_rows),
        },
    }


# ---------------------------------------------------------------------------
# Queue row construction
# ---------------------------------------------------------------------------

def _build_queue_rows(
    findings_rows: List[Dict[str, Any]],
    review_rows_meta: Dict[str, Any],
) -> List[Dict[str, Any]]:
    queue: List[Dict[str, Any]] = []
    for idx, finding in enumerate(findings_rows):
        row_id = finding.get("normalized_row_id")
        meta = review_rows_meta.get(row_id) if isinstance(review_rows_meta, dict) else None

        review_state = _normalize_state((meta or {}).get("review_state"))
        review_disposition = (meta or {}).get("review_disposition")
        raw_notes = (meta or {}).get("notes") or []
        review_notes = _normalize_notes(raw_notes)

        queue.append({
            "normalized_row_id": row_id,
            "priority_class": finding.get("priority_class"),
            "priority_reason": finding.get("priority_reason"),
            "discrepancy_class": finding.get("discrepancy_class"),
            "comparison_status": finding.get("comparison_status"),
            "mapping_outcome": finding.get("mapping_outcome"),
            "quote_description": finding.get("quote_description"),
            "review_state": review_state,
            "review_disposition": review_disposition,
            "review_notes": review_notes,
            "source_finding_ref": {
                "normalized_row_id": row_id,
                "finding_index": idx,
                "packet_version": None,  # packet_version threaded in at caller level
            },
        })
    return queue


def _normalize_state(state: Any) -> str:
    if isinstance(state, str) and state in _ALL_STATES:
        return state
    return STATE_OPEN


def _normalize_notes(raw_notes: Iterable[Any]) -> List[Dict[str, Any]]:
    """Deep-copy and shape-validate append-only note records."""
    out: List[Dict[str, Any]] = []
    if not isinstance(raw_notes, list):
        return out
    for idx, raw in enumerate(raw_notes):
        if not isinstance(raw, dict):
            continue
        note_type = raw.get("note_type")
        if note_type not in _ALL_NOTE_TYPES:
            note_type = NOTE_COMMENT
        out.append({
            "note_id": str(raw.get("note_id") or f"note-{idx}"),
            "author": raw.get("author"),
            "timestamp": raw.get("timestamp"),
            "note_type": note_type,
            "text": str(raw.get("text") or ""),
        })
    return out


def _stable_sort_queue(queue: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort by (priority_order, original index). Stable."""
    # Capture original index to keep stable ordering on ties.
    decorated = [(i, r) for i, r in enumerate(queue)]
    decorated.sort(
        key=lambda pair: (_PRIORITY_ORDER.get(pair[1].get("priority_class"), 99), pair[0])
    )
    return [r for _, r in decorated]


# ---------------------------------------------------------------------------
# Summary + workflow status
# ---------------------------------------------------------------------------

def _build_queue_summary(queue: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows_total = len(queue)
    priority_open = {
        "critical_open": 0, "high_open": 0, "medium_open": 0,
        "low_open": 0, "informational_open": 0,
    }
    rows_reviewed = 0
    rows_resolved = 0
    rows_deferred = 0
    rows_unreviewed = 0

    for r in queue:
        state = r.get("review_state", STATE_OPEN)
        priority = r.get("priority_class")
        if state == STATE_OPEN:
            rows_unreviewed += 1
            key = f"{priority}_open"
            if key in priority_open:
                priority_open[key] += 1
        elif state == STATE_REVIEWED:
            rows_reviewed += 1
        elif state == STATE_RESOLVED:
            rows_resolved += 1
        elif state == STATE_DEFERRED:
            rows_deferred += 1

    return {
        "rows_total": rows_total,
        "rows_unreviewed": rows_unreviewed,
        "rows_reviewed": rows_reviewed,
        "rows_resolved": rows_resolved,
        "rows_deferred": rows_deferred,
        **priority_open,
    }


def _derive_workflow_status(summary: Dict[str, Any], packet_status: Optional[str]) -> str:
    total = summary.get("rows_total", 0)
    resolved = summary.get("rows_resolved", 0)
    reviewed = summary.get("rows_reviewed", 0)
    unreviewed = summary.get("rows_unreviewed", 0)

    if total == 0:
        return WORKFLOW_OPEN
    if resolved == total:
        return WORKFLOW_RESOLVED_COMPLETE
    if resolved > 0 and resolved < total:
        return WORKFLOW_RESOLVED_PARTIAL
    if reviewed > 0 and unreviewed > 0:
        return WORKFLOW_IN_REVIEW
    if reviewed == total:
        return WORKFLOW_IN_REVIEW
    return WORKFLOW_OPEN


def _collect_unknown_ids(
    review_rows_meta: Dict[str, Any],
    findings_rows: List[Dict[str, Any]],
) -> List[str]:
    """List review-metadata row ids that do not match any finding.

    These are surfaced in diagnostics but never cause the workflow layer
    to invent queue rows.
    """
    known = {r.get("normalized_row_id") for r in findings_rows}
    if not isinstance(review_rows_meta, dict):
        return []
    return sorted(rid for rid in review_rows_meta.keys() if rid not in known)
