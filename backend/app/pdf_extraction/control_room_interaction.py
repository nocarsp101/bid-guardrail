"""
C38 — Control room interaction model.

Backend/UI-state contract that defines how an office user navigates
through the C35 control room. This module never builds a UI; it
produces a deterministic interaction-state object that a future UI can
consume directly.

Closed view vocabulary:
    - overview
    - blocked_items
    - unmapped_scope
    - source_conflicts
    - non_comparable
    - quantity_discrepancies
    - unit_discrepancies
    - scenarios
    - office_actions
    - engineer_packet

Default view derivation (deterministic):

    1. blocked_items     — when blocked_count > 0
    2. unmapped_scope    — when unmapped_count > 0
    3. source_conflicts  — when source_conflict_count > 0
    4. non_comparable    — when non_comparable_missing_quote/external > 0
    5. overview          — fallback

Hard rules:
    - Pure function. Same control room → same interaction model.
    - All rows remain reachable. No row id is hidden.
    - Filter and sort metadata is a closed enumerated set.
    - Inputs are deep-copied on read; never mutated.
    - No heuristic ranking. Default view is decided by the closed cascade.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

INTERACTION_MODEL_VERSION = "control_room_interaction/v1"

# Closed view vocabulary.
VIEW_OVERVIEW = "overview"
VIEW_BLOCKED_ITEMS = "blocked_items"
VIEW_UNMAPPED_SCOPE = "unmapped_scope"
VIEW_SOURCE_CONFLICTS = "source_conflicts"
VIEW_NON_COMPARABLE = "non_comparable"
VIEW_QTY_DISCREPANCIES = "quantity_discrepancies"
VIEW_UNIT_DISCREPANCIES = "unit_discrepancies"
VIEW_SCENARIOS = "scenarios"
VIEW_OFFICE_ACTIONS = "office_actions"
VIEW_ENGINEER_PACKET = "engineer_packet"

_ALL_VIEWS: List[str] = [
    VIEW_OVERVIEW,
    VIEW_BLOCKED_ITEMS,
    VIEW_UNMAPPED_SCOPE,
    VIEW_SOURCE_CONFLICTS,
    VIEW_NON_COMPARABLE,
    VIEW_QTY_DISCREPANCIES,
    VIEW_UNIT_DISCREPANCIES,
    VIEW_SCENARIOS,
    VIEW_OFFICE_ACTIONS,
    VIEW_ENGINEER_PACKET,
]

# Closed sort keys.
_SORT_KEYS: List[str] = [
    "priority_then_row_id",
    "row_id",
    "resolution_category",
    "office_action_status",
]

# Closed filter dimensions.
_FILTER_DIMENSIONS: List[str] = [
    "priority",
    "resolution_category",
    "office_action_status",
    "source_conflict_present",
    "mapping_outcome",
    "scenario_id",
]

# Mapping resolution_category → view bucket key (parallels claim_packet).
_CATEGORY_TO_VIEW: Dict[str, str] = {
    "blocked_pairing_resolution_required": VIEW_BLOCKED_ITEMS,
    "unmapped_scope_review_required": VIEW_UNMAPPED_SCOPE,
    "ambiguous_mapping_review_required": VIEW_UNMAPPED_SCOPE,
    "source_conflict_review_required": VIEW_SOURCE_CONFLICTS,
    "quantity_discrepancy_review_required": VIEW_QTY_DISCREPANCIES,
    "unit_discrepancy_review_required": VIEW_UNIT_DISCREPANCIES,
    "non_comparable_missing_quote_source": VIEW_NON_COMPARABLE,
    "non_comparable_missing_external_source": VIEW_NON_COMPARABLE,
    "review_required_other": VIEW_OVERVIEW,
    "clean_match_no_resolution_needed": VIEW_OVERVIEW,
}


def build_interaction_model(control_room: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the deterministic interaction model from a C35 control room.

    Returns a new dict. Never mutates the input.
    """
    cr = deepcopy(control_room or {})
    resolution = cr.get("resolution") or {}
    resolution_rows = resolution.get("resolution_rows") or []

    counts = _bucket_counts(resolution_rows)
    view_summaries = _build_view_summaries(cr, counts)
    default_view = _derive_default_view(counts, cr)
    focus_row_id = _derive_focus_row_id(default_view, resolution_rows)

    return {
        "interaction_model_version": INTERACTION_MODEL_VERSION,
        "job_id": cr.get("job_id"),
        "job_status": cr.get("job_status"),
        "default_view": default_view,
        "selected_view": default_view,
        "available_views": list(_ALL_VIEWS),
        "view_state": {
            "filters": _default_filters(),
            "sort": {
                "key": "priority_then_row_id",
                "available_keys": list(_SORT_KEYS),
            },
            "focus_row_id": focus_row_id,
            "expanded_sections": [default_view],
            "filter_dimensions": list(_FILTER_DIMENSIONS),
        },
        "view_summaries": view_summaries,
        "row_index": _build_row_index(resolution_rows),
        "interaction_diagnostics": {
            "rows_total": len(resolution_rows),
            "default_view_basis": _default_view_basis(counts, cr),
        },
    }


# ---------------------------------------------------------------------------
# View counts + summaries
# ---------------------------------------------------------------------------

def _bucket_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {v: 0 for v in _ALL_VIEWS}
    for row in rows:
        cat = row.get("resolution_category")
        view = _CATEGORY_TO_VIEW.get(cat, VIEW_OVERVIEW)
        counts[view] = counts.get(view, 0) + 1
    return counts


def _build_view_summaries(
    cr: Dict[str, Any],
    counts: Dict[str, int],
) -> Dict[str, Dict[str, Any]]:
    resolution = cr.get("resolution") or {}
    actioned_rows = (cr.get("office_actions_output") or {}).get("resolution_rows") or []
    rows_with_actions = sum(1 for r in actioned_rows if r.get("office_actions"))

    engineer_preview = cr.get("engineer_packet_preview") or {}
    source_management = cr.get("source_management") or {}
    source_management_summary = source_management.get("source_management_summary") or {}

    return {
        VIEW_OVERVIEW: {
            "row_count": len(resolution.get("resolution_rows") or []),
            "job_status": cr.get("job_status"),
            "pairing_status": (cr.get("pipeline_status") or {}).get("pairing_status"),
            "packet_status": (cr.get("pipeline_status") or {}).get("packet_status"),
        },
        VIEW_BLOCKED_ITEMS: {"row_count": counts.get(VIEW_BLOCKED_ITEMS, 0)},
        VIEW_UNMAPPED_SCOPE: {"row_count": counts.get(VIEW_UNMAPPED_SCOPE, 0)},
        VIEW_SOURCE_CONFLICTS: {
            "row_count": counts.get(VIEW_SOURCE_CONFLICTS, 0),
            "rows_with_conflicted_sources": source_management_summary.get(
                "rows_with_conflicted_sources", 0
            ),
        },
        VIEW_NON_COMPARABLE: {"row_count": counts.get(VIEW_NON_COMPARABLE, 0)},
        VIEW_QTY_DISCREPANCIES: {"row_count": counts.get(VIEW_QTY_DISCREPANCIES, 0)},
        VIEW_UNIT_DISCREPANCIES: {"row_count": counts.get(VIEW_UNIT_DISCREPANCIES, 0)},
        VIEW_SCENARIOS: {
            "scenario_count": 5,  # closed C36 vocabulary
            "available_scenarios": [
                "scenario_dot_basis", "scenario_takeoff_basis",
                "scenario_engineer_basis", "scenario_manual_basis",
                "scenario_no_external",
            ],
        },
        VIEW_OFFICE_ACTIONS: {
            "rows_with_actions": rows_with_actions,
        },
        VIEW_ENGINEER_PACKET: {
            "engineer_row_count": engineer_preview.get("engineer_row_count", 0),
            "packet_status": engineer_preview.get("packet_status"),
        },
    }


# ---------------------------------------------------------------------------
# Default view derivation
# ---------------------------------------------------------------------------

def _derive_default_view(counts: Dict[str, int], cr: Dict[str, Any]) -> str:
    """Closed cascade for default view selection."""
    if counts.get(VIEW_BLOCKED_ITEMS, 0) > 0:
        return VIEW_BLOCKED_ITEMS
    if counts.get(VIEW_UNMAPPED_SCOPE, 0) > 0:
        return VIEW_UNMAPPED_SCOPE
    if counts.get(VIEW_SOURCE_CONFLICTS, 0) > 0:
        return VIEW_SOURCE_CONFLICTS
    if counts.get(VIEW_QTY_DISCREPANCIES, 0) > 0:
        return VIEW_QTY_DISCREPANCIES
    if counts.get(VIEW_UNIT_DISCREPANCIES, 0) > 0:
        return VIEW_UNIT_DISCREPANCIES
    if counts.get(VIEW_NON_COMPARABLE, 0) > 0:
        return VIEW_NON_COMPARABLE
    return VIEW_OVERVIEW


def _default_view_basis(counts: Dict[str, int], cr: Dict[str, Any]) -> str:
    """Templated reason string explaining default-view selection."""
    if counts.get(VIEW_BLOCKED_ITEMS, 0) > 0:
        return f"blocked_count={counts[VIEW_BLOCKED_ITEMS]}>0"
    if counts.get(VIEW_UNMAPPED_SCOPE, 0) > 0:
        return f"unmapped_count={counts[VIEW_UNMAPPED_SCOPE]}>0"
    if counts.get(VIEW_SOURCE_CONFLICTS, 0) > 0:
        return f"source_conflict_count={counts[VIEW_SOURCE_CONFLICTS]}>0"
    if counts.get(VIEW_QTY_DISCREPANCIES, 0) > 0:
        return f"quantity_discrepancy_count={counts[VIEW_QTY_DISCREPANCIES]}>0"
    if counts.get(VIEW_UNIT_DISCREPANCIES, 0) > 0:
        return f"unit_discrepancy_count={counts[VIEW_UNIT_DISCREPANCIES]}>0"
    if counts.get(VIEW_NON_COMPARABLE, 0) > 0:
        return f"non_comparable_count={counts[VIEW_NON_COMPARABLE]}>0"
    return "no_review_required_rows_fallback_overview"


def _derive_focus_row_id(
    default_view: str,
    rows: List[Dict[str, Any]],
) -> Optional[str]:
    """Pick the first row id whose category lands in the default view.

    Determinism: rows are walked in original order; the FIRST match wins.
    Returns None when no row matches (e.g. overview default with no rows).
    """
    if default_view == VIEW_OVERVIEW:
        return rows[0].get("normalized_row_id") if rows else None
    for row in rows:
        cat = row.get("resolution_category")
        view = _CATEGORY_TO_VIEW.get(cat, VIEW_OVERVIEW)
        if view == default_view:
            return row.get("normalized_row_id")
    return None


# ---------------------------------------------------------------------------
# Filters + row index
# ---------------------------------------------------------------------------

def _default_filters() -> Dict[str, Any]:
    return {
        "priority": None,
        "resolution_category": None,
        "office_action_status": None,
        "source_conflict_present": None,
        "mapping_outcome": None,
        "scenario_id": None,
    }


def _build_row_index(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Compact index of every reachable row, with the view it belongs to.

    Used by the future UI to ensure no row is unreachable from any view.
    """
    out: List[Dict[str, Any]] = []
    for row in rows:
        cat = row.get("resolution_category")
        out.append({
            "normalized_row_id": row.get("normalized_row_id"),
            "resolution_category": cat,
            "resolution_priority": row.get("resolution_priority"),
            "view_bucket": _CATEGORY_TO_VIEW.get(cat, VIEW_OVERVIEW),
        })
    return out
