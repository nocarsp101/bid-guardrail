"""
C34 — Engineer output packet foundation.

Assembles a deterministic engineer-ready packet artifact from:

    - a C31 discrepancy resolution output
    - an optional C33 office-action-decorated resolution output
    - an optional C32 source management output

Produces a stable section-oriented dict ready for downstream
rendering/export (HTML/PDF/CSV) by a later layer. No prose is
generated. Every label is a fixed templated constant. Ordering is
deterministic and explicit.

Hard rules:
    - Blocked rows stay visible. Source conflicts stay visible.
    - Office actions are displayed alongside the row but never override
      `resolution_category`, `resolution_priority`, `comparison_basis`,
      `quote_values`, or `external_sources`.
    - Row ordering is priority-first (critical → high → medium → low →
      informational), with stable tie-breaking by original
      resolution-row index.
    - No narrative text. Section labels and flag ids are deterministic
      constants.
    - Inputs are deep-copied on read; the builder never mutates them.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

PACKET_VERSION = "engineer_output_packet/v1"

# Closed packet-status vocabulary.
PACKET_DRAFT_READY = "draft_ready"
PACKET_BLOCKED = "blocked"
PACKET_PARTIAL = "partial"
PACKET_NO_EXTERNAL_PACKET_NEEDED = "no_external_packet_needed"

# Closed engineer-packet-flag vocabulary.
FLAG_BLOCKED_PAIRING = "blocked_pairing"
FLAG_UNMAPPED_SCOPE = "unmapped_scope"
FLAG_AMBIGUOUS_MAPPING = "ambiguous_mapping"
FLAG_CONFLICTING_QUANTITY_SOURCES = "conflicting_quantity_sources"
FLAG_MISSING_QUOTE_QUANTITY = "missing_quote_quantity"
FLAG_MISSING_EXTERNAL_SOURCE = "missing_external_source"
FLAG_QTY_DISCREPANCY = "qty_discrepancy"
FLAG_UNIT_DISCREPANCY = "unit_discrepancy"
FLAG_ENGINEER_ACTION_RECORDED = "engineer_action_recorded"
FLAG_FIELD_VERIFICATION_REQUIRED = "field_verification_required"
FLAG_LUMP_SUM_MARKED = "lump_sum_marked"
FLAG_WORKING_BASIS_SELECTED_BY_OFFICE = "working_basis_selected_by_office"

# Priority ordering (for deterministic row sorting).
_PRIORITY_ORDER = {
    "critical": 0, "high": 1, "medium": 2, "low": 3, "informational": 4,
}


def build_engineer_packet(
    resolution_output: Dict[str, Any],
    office_action_output: Optional[Dict[str, Any]] = None,
    source_management_output: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Assemble the deterministic engineer packet.

    Args:
        resolution_output:
            C31 `build_resolution()` return value.
        office_action_output:
            Optional C33 `record_office_actions()` return value. When
            supplied, the per-row `office_actions` list is embedded in
            each engineer row. The resolution_output and
            office_action_output must share row identities; mismatched
            rows are surfaced in diagnostics.
        source_management_output:
            Optional C32 `manage_quantity_sources()` return value. When
            supplied, its top-level `source_management_summary` is
            embedded in the engineer packet as the `source_management_section`.

    Returns a deterministic engineer packet dict with:

        packet_version
        packet_status
        packet_header
        pairing_section
        source_management_section
        discrepancy_summary
        office_action_summary
        engineer_rows
        packet_diagnostics
    """
    resolution_output = resolution_output or {}
    rows = resolution_output.get("resolution_rows") or []

    # Build a lookup for office-action-decorated rows (if provided).
    action_lookup: Dict[str, Dict[str, Any]] = {}
    if office_action_output:
        for ar in office_action_output.get("resolution_rows") or []:
            rid = ar.get("normalized_row_id")
            if rid is not None:
                action_lookup[rid] = ar

    engineer_rows: List[Dict[str, Any]] = []
    missing_in_actions: List[str] = []

    for idx, row in enumerate(rows):
        rid = row.get("normalized_row_id")
        office_actions = []
        office_action_status = "none"
        office_action_summary = None
        if action_lookup:
            matched = action_lookup.get(rid)
            if matched is None:
                missing_in_actions.append(rid)
            else:
                office_actions = deepcopy(matched.get("office_actions") or [])
                office_action_status = matched.get("office_action_status") or "none"
                office_action_summary = deepcopy(matched.get("office_action_summary"))

        engineer_rows.append(_build_engineer_row(row, idx, office_actions,
                                                 office_action_status,
                                                 office_action_summary))

    engineer_rows = _stable_sort(engineer_rows)

    packet_status = _derive_packet_status(resolution_output, engineer_rows)
    header = _build_header(resolution_output)
    pairing_section = _build_pairing_section(resolution_output)
    source_section = _build_source_management_section(source_management_output)
    discrepancy_summary = deepcopy(resolution_output.get("resolution_summary") or {})
    office_action_summary = _build_office_action_summary(office_action_output)
    diagnostics = _build_diagnostics(engineer_rows, missing_in_actions,
                                     action_lookup, resolution_output)

    return {
        "packet_version": PACKET_VERSION,
        "packet_status": packet_status,
        "packet_header": header,
        "pairing_section": pairing_section,
        "source_management_section": source_section,
        "discrepancy_summary": discrepancy_summary,
        "office_action_summary": office_action_summary,
        "engineer_rows": engineer_rows,
        "packet_diagnostics": diagnostics,
    }


# ---------------------------------------------------------------------------
# Row assembly
# ---------------------------------------------------------------------------

def _build_engineer_row(
    row: Dict[str, Any],
    original_index: int,
    office_actions: List[Dict[str, Any]],
    office_action_status: str,
    office_action_summary: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    flags = _compute_flags(row, office_actions)
    result = {
        "normalized_row_id": row.get("normalized_row_id"),
        "quote_description": (row.get("quote_values") or {}).get("description"),
        "mapping_outcome": _infer_mapping_outcome(row),
        "resolution_category": row.get("resolution_category"),
        "resolution_priority": row.get("resolution_priority"),
        "resolution_reason": row.get("resolution_reason"),
        "comparison_basis": deepcopy(row.get("comparison_basis") or {}),
        "comparison_result": deepcopy(row.get("comparison_result") or {}),
        "quote_values": deepcopy(row.get("quote_values") or {}),
        "external_sources": deepcopy(row.get("external_sources") or []),
        "office_actions": office_actions,
        "office_action_status": office_action_status,
        "office_action_summary": office_action_summary,
        "engineer_packet_flags": flags,
        "engineer_trace": {
            "original_index": original_index,
            "resolution_trace": deepcopy(row.get("resolution_trace") or {}),
        },
    }
    # C44 provenance propagation.
    for pkey in ("row_origin", "source_provenance", "manual_entry_ref", "source_block_ref"):
        val = row.get(pkey)
        if val is not None:
            result[pkey] = deepcopy(val) if isinstance(val, dict) else val
    return result


def _infer_mapping_outcome(row: Dict[str, Any]) -> Optional[str]:
    """Recover the upstream mapping_outcome from resolution state.

    The C31 resolution row does not carry `mapping_outcome` directly,
    but the trace does. Prefer that when available; otherwise infer
    from the resolution_category.
    """
    trace_inputs = ((row.get("resolution_trace") or {}).get("inputs") or {})
    if trace_inputs.get("mapping_outcome"):
        return trace_inputs.get("mapping_outcome")
    cat = row.get("resolution_category")
    if cat == "blocked_pairing_resolution_required":
        return "blocked"
    if cat == "unmapped_scope_review_required":
        return "unmapped"
    if cat == "ambiguous_mapping_review_required":
        return "ambiguous"
    return "mapped"


def _compute_flags(
    row: Dict[str, Any],
    office_actions: List[Dict[str, Any]],
) -> List[str]:
    """Deterministic engineer-packet flags derived from row state."""
    flags: List[str] = []

    cat = row.get("resolution_category")
    if cat == "blocked_pairing_resolution_required":
        flags.append(FLAG_BLOCKED_PAIRING)
    if cat == "unmapped_scope_review_required":
        flags.append(FLAG_UNMAPPED_SCOPE)
    if cat == "ambiguous_mapping_review_required":
        flags.append(FLAG_AMBIGUOUS_MAPPING)
    if cat == "source_conflict_review_required":
        flags.append(FLAG_CONFLICTING_QUANTITY_SOURCES)
    if cat == "non_comparable_missing_quote_source":
        flags.append(FLAG_MISSING_QUOTE_QUANTITY)
    if cat == "non_comparable_missing_external_source":
        flags.append(FLAG_MISSING_EXTERNAL_SOURCE)
    if cat == "quantity_discrepancy_review_required":
        flags.append(FLAG_QTY_DISCREPANCY)
    if cat == "unit_discrepancy_review_required":
        flags.append(FLAG_UNIT_DISCREPANCY)

    # Office-action-derived flags (additive, never overriding).
    for a in office_actions:
        at = a.get("action_type")
        if at == "requires_field_verification":
            if FLAG_FIELD_VERIFICATION_REQUIRED not in flags:
                flags.append(FLAG_FIELD_VERIFICATION_REQUIRED)
        elif at == "mark_lump_sum_non_comparable":
            if FLAG_LUMP_SUM_MARKED not in flags:
                flags.append(FLAG_LUMP_SUM_MARKED)
        elif at == "escalate_to_engineer":
            if FLAG_ENGINEER_ACTION_RECORDED not in flags:
                flags.append(FLAG_ENGINEER_ACTION_RECORDED)
        elif at in (
            "accept_dot_quantity_as_working_basis",
            "accept_internal_takeoff_as_working_basis",
            "accept_engineer_quantity_as_working_basis",
        ):
            if FLAG_WORKING_BASIS_SELECTED_BY_OFFICE not in flags:
                flags.append(FLAG_WORKING_BASIS_SELECTED_BY_OFFICE)

    return flags


def _stable_sort(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Priority-first sort with stable tie-breaking by original index."""
    return sorted(
        rows,
        key=lambda r: (
            _PRIORITY_ORDER.get(r.get("resolution_priority"), 99),
            int((r.get("engineer_trace") or {}).get("original_index") or 0),
        ),
    )


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _build_header(resolution_output: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "packet_version": PACKET_VERSION,
        "resolution_version": resolution_output.get("resolution_version"),
        "augmentation_rules_version": resolution_output.get("augmentation_rules_version"),
        "contract_version": resolution_output.get("contract_version"),
        "packet_status_source": resolution_output.get("packet_status"),
        "pairing_status": resolution_output.get("pairing_status"),
    }


def _build_pairing_section(resolution_output: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "pairing_status": resolution_output.get("pairing_status"),
        "packet_status": resolution_output.get("packet_status"),
    }


def _build_source_management_section(
    source_management_output: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if not source_management_output:
        return {"present": False}
    return {
        "present": True,
        "source_management_version": source_management_output.get("source_management_version"),
        "source_management_summary": deepcopy(
            source_management_output.get("source_management_summary") or {}
        ),
    }


def _build_office_action_summary(
    office_action_output: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if not office_action_output:
        return {"present": False}
    return {
        "present": True,
        "office_actions_version": office_action_output.get("office_actions_version"),
        "office_actions_summary": deepcopy(
            office_action_output.get("office_actions_summary") or {}
        ),
        "office_actions_diagnostics": deepcopy(
            office_action_output.get("office_actions_diagnostics") or {}
        ),
    }


def _build_diagnostics(
    engineer_rows: List[Dict[str, Any]],
    missing_in_actions: List[str],
    action_lookup: Dict[str, Dict[str, Any]],
    resolution_output: Dict[str, Any],
) -> Dict[str, Any]:
    flag_histogram: Dict[str, int] = {}
    priority_histogram: Dict[str, int] = {k: 0 for k in _PRIORITY_ORDER}
    for r in engineer_rows:
        for f in r.get("engineer_packet_flags") or []:
            flag_histogram[f] = flag_histogram.get(f, 0) + 1
        prio = r.get("resolution_priority")
        if prio in priority_histogram:
            priority_histogram[prio] += 1

    return {
        "engineer_row_count": len(engineer_rows),
        "priority_histogram": priority_histogram,
        "engineer_packet_flag_histogram": dict(sorted(flag_histogram.items())),
        "rows_in_resolution_not_in_office_actions": missing_in_actions,
        "action_output_supplied": bool(action_lookup),
    }


def _derive_packet_status(
    resolution_output: Dict[str, Any],
    engineer_rows: List[Dict[str, Any]],
) -> str:
    """Derive the engineer packet status from upstream state.

    - If every row is `blocked_pairing_resolution_required` → blocked.
    - If there are no review-required rows AND no rows at all → no_external_packet_needed.
    - If the upstream packet_status is blocked → blocked.
    - If any review-required row exists → draft_ready.
    - Otherwise → partial.
    """
    upstream_packet_status = resolution_output.get("packet_status")
    if upstream_packet_status == "blocked":
        return PACKET_BLOCKED

    if not engineer_rows:
        return PACKET_NO_EXTERNAL_PACKET_NEEDED

    review_required_categories = {
        "blocked_pairing_resolution_required",
        "unmapped_scope_review_required",
        "ambiguous_mapping_review_required",
        "source_conflict_review_required",
        "quantity_discrepancy_review_required",
        "unit_discrepancy_review_required",
        "non_comparable_missing_quote_source",
        "non_comparable_missing_external_source",
        "review_required_other",
    }
    any_review_required = any(
        r.get("resolution_category") in review_required_categories
        for r in engineer_rows
    )
    if any_review_required:
        return PACKET_DRAFT_READY
    return PACKET_PARTIAL
