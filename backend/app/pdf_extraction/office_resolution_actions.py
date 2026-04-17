"""
C33 — Office resolution actions layer.

Records append-only office actions against C31 resolution rows WITHOUT
mutating governed discrepancy truth. Actions are workflow metadata;
they never rewrite discrepancy_category, priority, quote_values,
external_sources, or comparison_basis.

Closed action vocabulary:

    accept_dot_quantity_as_working_basis
    accept_internal_takeoff_as_working_basis
    accept_engineer_quantity_as_working_basis
    mark_lump_sum_non_comparable
    requires_field_verification
    escalate_to_engineer
    needs_manual_scope_review
    no_action_taken

Invalid / unknown action types are SURFACED in diagnostics but never
silently dropped and never invented as an approved type.

Hard rules:
    - Append-only. Every action is appended to the row's `office_actions`
      list. No in-place edit.
    - Multiple actions per row are supported; history is never collapsed.
    - No governed field on the resolution row is modified by recording
      an action.
    - Unknown row ids (actions targeting rows that don't exist in the
      resolution) are surfaced in diagnostics. They never create phantom
      queue rows.
    - Invalid action type strings are preserved in the row record (so
      the action history is not lossy) and tagged with an explicit
      `action_validation_status` of `unknown_action_type`. They are also
      counted in diagnostics.
    - Every action carries a deterministic `action_id` (caller-provided
      or generated as `act-<row_id>-<ordinal>`).
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional

OFFICE_ACTIONS_VERSION = "office_resolution_actions/v1"

# Closed action vocabulary.
ACTION_ACCEPT_DOT = "accept_dot_quantity_as_working_basis"
ACTION_ACCEPT_TAKEOFF = "accept_internal_takeoff_as_working_basis"
ACTION_ACCEPT_ENGINEER = "accept_engineer_quantity_as_working_basis"
ACTION_MARK_LUMP_SUM = "mark_lump_sum_non_comparable"
ACTION_FIELD_VERIFICATION = "requires_field_verification"
ACTION_ESCALATE_ENGINEER = "escalate_to_engineer"
ACTION_MANUAL_SCOPE_REVIEW = "needs_manual_scope_review"
ACTION_NO_ACTION = "no_action_taken"

_ALL_ACTIONS = frozenset({
    ACTION_ACCEPT_DOT, ACTION_ACCEPT_TAKEOFF, ACTION_ACCEPT_ENGINEER,
    ACTION_MARK_LUMP_SUM, ACTION_FIELD_VERIFICATION,
    ACTION_ESCALATE_ENGINEER, ACTION_MANUAL_SCOPE_REVIEW, ACTION_NO_ACTION,
})

# Closed office_action_status vocabulary.
OA_NONE = "none"
OA_RECORDED = "recorded"
OA_MULTIPLE = "multiple_actions"

# Action validation-status vocabulary.
AV_VALID = "valid"
AV_UNKNOWN_TYPE = "unknown_action_type"


def record_office_actions(
    resolution_output: Dict[str, Any],
    actions_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Attach append-only office actions to a C31 resolution output.

    Args:
        resolution_output: the C31 `build_resolution()` output dict.
        actions_metadata: optional dict shaped:
                          {
                              "rows": {
                                  "<normalized_row_id>": {
                                      "actions": [
                                          {
                                              "action_id": "...",   # optional
                                              "action_type": "...",
                                              "actor": "...",
                                              "timestamp": "...",
                                              "action_note": "...",
                                          },
                                          ...
                                      ]
                                  },
                                  ...
                              }
                          }

    Returns a new resolution output dict with per-row `office_actions`,
    `office_action_status`, `office_action_summary`, and a top-level
    `office_actions_diagnostics`. Governed resolution fields are never
    modified.
    """
    out = deepcopy(resolution_output or {})
    rows = out.get("resolution_rows") or []
    actions_metadata = actions_metadata or {}
    meta_rows = (actions_metadata.get("rows") or {}) if isinstance(actions_metadata, dict) else {}

    known_ids = {r.get("normalized_row_id") for r in rows}
    unknown_row_ids: List[str] = sorted(
        rid for rid in meta_rows.keys() if rid not in known_ids
    )

    action_type_histogram: Dict[str, int] = {}
    unknown_action_type_count = 0
    rows_with_actions = 0
    rows_marked_lump_sum = 0
    rows_escalated_to_engineer = 0
    rows_marked_field_verification = 0

    for row in rows:
        rid = row.get("normalized_row_id")
        row_meta = meta_rows.get(rid) if isinstance(meta_rows, dict) else None
        raw_actions = (row_meta or {}).get("actions") or []
        validated: List[Dict[str, Any]] = []

        for idx, raw in enumerate(raw_actions):
            if not isinstance(raw, dict):
                continue
            action = _build_action_record(raw, rid, idx)
            validated.append(action)

            action_type = action["action_type"]
            action_type_histogram[action_type] = action_type_histogram.get(action_type, 0) + 1
            if action["action_validation_status"] == AV_UNKNOWN_TYPE:
                unknown_action_type_count += 1
            if action_type == ACTION_MARK_LUMP_SUM:
                rows_marked_lump_sum += 1
            if action_type == ACTION_ESCALATE_ENGINEER:
                rows_escalated_to_engineer += 1
            if action_type == ACTION_FIELD_VERIFICATION:
                rows_marked_field_verification += 1

        row["office_actions"] = validated
        if len(validated) == 0:
            row["office_action_status"] = OA_NONE
        elif len(validated) == 1:
            row["office_action_status"] = OA_RECORDED
        else:
            row["office_action_status"] = OA_MULTIPLE

        if validated:
            rows_with_actions += 1

        row["office_action_summary"] = {
            "action_count": len(validated),
            "action_types": sorted({a["action_type"] for a in validated}),
            "has_unknown_action_type": any(
                a["action_validation_status"] == AV_UNKNOWN_TYPE for a in validated
            ),
        }

    out["office_actions_version"] = OFFICE_ACTIONS_VERSION
    out["office_actions_summary"] = {
        "rows_total": len(rows),
        "rows_with_actions": rows_with_actions,
        "action_type_histogram": dict(sorted(action_type_histogram.items())),
        "rows_marked_lump_sum": rows_marked_lump_sum,
        "rows_escalated_to_engineer": rows_escalated_to_engineer,
        "rows_marked_field_verification": rows_marked_field_verification,
    }
    out["office_actions_diagnostics"] = {
        "unknown_row_ids": unknown_row_ids,
        "unknown_action_type_count": unknown_action_type_count,
        "supplied_action_row_count": len(meta_rows),
    }
    return out


# ---------------------------------------------------------------------------
# Action record construction
# ---------------------------------------------------------------------------

def _build_action_record(
    raw: Dict[str, Any],
    row_id: Optional[str],
    ordinal: int,
) -> Dict[str, Any]:
    """Shape-validate one raw action record into the C33 contract shape."""
    action_type = raw.get("action_type")
    if action_type in _ALL_ACTIONS:
        validation = AV_VALID
    else:
        validation = AV_UNKNOWN_TYPE

    action_id = raw.get("action_id") or f"act-{row_id}-{ordinal}"
    return {
        "action_id": str(action_id),
        "action_type": str(action_type) if action_type is not None else None,
        "actor": raw.get("actor"),
        "timestamp": raw.get("timestamp"),
        "action_note": raw.get("action_note"),
        "action_scope": {"normalized_row_id": row_id},
        "action_validation_status": validation,
    }
