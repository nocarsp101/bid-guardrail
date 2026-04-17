"""
C37 — Claim / discrepancy packet builder.

Assembles a structured, engineer-facing discrepancy packet from C31
resolution output plus optional C33 office actions and C32 source
management. The output is explicitly not narrative — every label is a
deterministic constant and every row is placed into a closed set of
named issue sections.

Hard rules:
    - Groups rows by `resolution_category` into a closed set of
      issue-section buckets. Each row appears in exactly one bucket.
    - Never hides unresolved rows. Every row in the input appears in
      some section of the output.
    - Never rewrites discrepancy categories. The section name is a
      convenience alias; the raw `resolution_category` is preserved on
      every row.
    - Never generates prose; all free-text fields come from the
      upstream templated outputs (reasons, flags, trace).
    - Office actions are attached to each row as structured stance, not
      narrative commentary.
    - Deterministic row ordering within each section: priority first,
      then stable original index.
    - Deep-copies inputs on read; builder never mutates them.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

CLAIM_PACKET_VERSION = "claim_packet/v1"

# Closed packet-status vocabulary.
PACKET_DRAFT_READY = "draft_ready"
PACKET_BLOCKED = "blocked"
PACKET_PARTIAL = "partial"
PACKET_NO_ISSUES = "no_issues"

# Closed issue section keys (display alias → upstream resolution_category).
_CATEGORY_TO_SECTION: Dict[str, str] = {
    "blocked_pairing_resolution_required": "blocked_pairing",
    "unmapped_scope_review_required": "unmapped_scope",
    "ambiguous_mapping_review_required": "ambiguous_mapping",
    "source_conflict_review_required": "source_conflicts",
    "quantity_discrepancy_review_required": "quantity_discrepancies",
    "unit_discrepancy_review_required": "unit_discrepancies",
    "non_comparable_missing_quote_source": "non_comparable_missing_quote",
    "non_comparable_missing_external_source": "non_comparable_missing_external",
    "clean_match_no_resolution_needed": "clean_matches",
    "review_required_other": "review_required_other",
}

# Stable section ordering for the issue_sections dict.
_SECTION_ORDER: List[str] = [
    "blocked_pairing",
    "unmapped_scope",
    "ambiguous_mapping",
    "source_conflicts",
    "quantity_discrepancies",
    "unit_discrepancies",
    "non_comparable_missing_quote",
    "non_comparable_missing_external",
    "review_required_other",
    "clean_matches",
]

# Priority ordering (lower = higher priority).
_PRIORITY_ORDER = {
    "critical": 0, "high": 1, "medium": 2, "low": 3, "informational": 4,
}


def build_claim_packet(
    resolution_output: Dict[str, Any],
    office_action_output: Optional[Dict[str, Any]] = None,
    source_management_output: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Assemble the claim/discrepancy packet.

    Args:
        resolution_output: C31 `build_resolution()` return value.
        office_action_output: optional C33 `record_office_actions()` return.
        source_management_output: optional C32 `manage_quantity_sources()` return.

    Returns a deterministic dict shaped:

        {
            "packet_version": "...",
            "packet_status": "...",
            "summary_section": { ... },
            "issue_sections": {
                "blocked_pairing": [...],
                "unmapped_scope": [...],
                "source_conflicts": [...],
                "quantity_discrepancies": [...],
                "unit_discrepancies": [...],
                "non_comparable_missing_quote": [...],
                "non_comparable_missing_external": [...],
                "ambiguous_mapping": [...],
                "review_required_other": [...],
                "clean_matches": [...],
            },
            "supporting_data": { ... },
            "office_actions": [ ... flat list of all actions ... ],
            "source_management": { ... optional ... },
            "packet_diagnostics": { ... }
        }
    """
    resolution_output = resolution_output or {}
    rows = resolution_output.get("resolution_rows") or []
    action_lookup = _build_action_lookup(office_action_output)

    # Initialize every section — deterministic key order.
    issue_sections: Dict[str, List[Dict[str, Any]]] = {k: [] for k in _SECTION_ORDER}
    office_action_flat: List[Dict[str, Any]] = []

    for idx, row in enumerate(rows):
        category = row.get("resolution_category")
        section_key = _CATEGORY_TO_SECTION.get(category, "review_required_other")

        matched_actions = _lookup_actions(row.get("normalized_row_id"), action_lookup)
        packet_row = _build_packet_row(row, idx, matched_actions)
        issue_sections[section_key].append(packet_row)

        for a in matched_actions:
            office_action_flat.append({
                "normalized_row_id": row.get("normalized_row_id"),
                **deepcopy(a),
            })

    # Sort rows within each section: priority first, stable tie-break.
    for key, section_rows in issue_sections.items():
        issue_sections[key] = sorted(
            section_rows,
            key=lambda r: (
                _PRIORITY_ORDER.get(r.get("resolution_priority"), 99),
                r.get("_original_index", 0),
            ),
        )
        # Strip helper field.
        for r in issue_sections[key]:
            r.pop("_original_index", None)

    summary_section = _build_summary_section(rows, issue_sections)
    supporting_data = _build_supporting_data(rows)
    packet_status = _derive_packet_status(resolution_output, rows, issue_sections)
    diagnostics = _build_diagnostics(rows, issue_sections, action_lookup)

    packet: Dict[str, Any] = {
        "packet_version": CLAIM_PACKET_VERSION,
        "packet_status": packet_status,
        "resolution_version": resolution_output.get("resolution_version"),
        "pairing_status": resolution_output.get("pairing_status"),
        "upstream_packet_status": resolution_output.get("packet_status"),
        "summary_section": summary_section,
        "issue_sections": issue_sections,
        "supporting_data": supporting_data,
        "office_actions": office_action_flat,
        "packet_diagnostics": diagnostics,
    }

    if source_management_output is not None:
        packet["source_management"] = {
            "source_management_version": source_management_output.get("source_management_version"),
            "source_management_summary": deepcopy(
                source_management_output.get("source_management_summary") or {}
            ),
        }
    else:
        packet["source_management"] = None

    return packet


# ---------------------------------------------------------------------------
# Row / section helpers
# ---------------------------------------------------------------------------

def _build_packet_row(
    row: Dict[str, Any],
    original_index: int,
    matched_actions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Per-row dict placed into an issue section."""
    result = {
        "_original_index": original_index,
        "normalized_row_id": row.get("normalized_row_id"),
        "resolution_category": row.get("resolution_category"),
        "resolution_priority": row.get("resolution_priority"),
        "resolution_reason": row.get("resolution_reason"),
        "quote_description": (row.get("quote_values") or {}).get("description"),
        "quote_values": deepcopy(row.get("quote_values") or {}),
        "external_sources": deepcopy(row.get("external_sources") or []),
        "comparison_basis": deepcopy(row.get("comparison_basis") or {}),
        "comparison_result": deepcopy(row.get("comparison_result") or {}),
        "office_actions": deepcopy(matched_actions),
        "office_action_count": len(matched_actions),
        "resolution_trace": deepcopy(row.get("resolution_trace") or {}),
    }
    # C44 provenance propagation.
    for pkey in ("row_origin", "source_provenance", "manual_entry_ref", "source_block_ref"):
        val = row.get(pkey)
        if val is not None:
            result[pkey] = deepcopy(val) if isinstance(val, dict) else val
    return result


def _build_action_lookup(
    office_action_output: Optional[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    if not office_action_output:
        return {}
    lookup: Dict[str, List[Dict[str, Any]]] = {}
    for r in office_action_output.get("resolution_rows") or []:
        rid = r.get("normalized_row_id")
        actions = r.get("office_actions") or []
        if rid is not None and actions:
            lookup[rid] = list(actions)
    return lookup


def _lookup_actions(
    row_id: Optional[str],
    action_lookup: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    if row_id is None:
        return []
    return action_lookup.get(row_id, [])


# ---------------------------------------------------------------------------
# Summary / supporting data
# ---------------------------------------------------------------------------

def _build_summary_section(
    rows: List[Dict[str, Any]],
    issue_sections: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    total = len(rows)
    critical = sum(1 for r in rows if r.get("resolution_priority") == "critical")
    high = sum(1 for r in rows if r.get("resolution_priority") == "high")
    medium = sum(1 for r in rows if r.get("resolution_priority") == "medium")
    low = sum(1 for r in rows if r.get("resolution_priority") == "low")

    section_counts = {k: len(v) for k, v in issue_sections.items()}
    return {
        "total_rows": total,
        "critical_issues": critical,
        "high_priority_issues": high,
        "medium_priority_issues": medium,
        "low_priority_issues": low,
        "blocked_items": section_counts.get("blocked_pairing", 0),
        "section_counts": section_counts,
    }


def _build_supporting_data(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Flat projections of the quote values, external sources, and
    comparison bases for the whole packet. Supplies an easy index for
    downstream exporters."""
    quote_rows = []
    external_sources_all = []
    comparison_bases = []

    for r in rows:
        rid = r.get("normalized_row_id")
        quote_rows.append({
            "normalized_row_id": rid,
            "quote_values": deepcopy(r.get("quote_values") or {}),
        })
        for s in r.get("external_sources") or []:
            external_sources_all.append({
                "normalized_row_id": rid,
                **deepcopy(s),
            })
        comparison_bases.append({
            "normalized_row_id": rid,
            "comparison_basis": deepcopy(r.get("comparison_basis") or {}),
        })

    return {
        "quote_rows": quote_rows,
        "external_sources": external_sources_all,
        "comparison_basis": comparison_bases,
    }


# ---------------------------------------------------------------------------
# Status + diagnostics
# ---------------------------------------------------------------------------

def _derive_packet_status(
    resolution_output: Dict[str, Any],
    rows: List[Dict[str, Any]],
    issue_sections: Dict[str, List[Dict[str, Any]]],
) -> str:
    if resolution_output.get("packet_status") == "blocked":
        return PACKET_BLOCKED
    if not rows:
        return PACKET_NO_ISSUES

    review_sections = (
        "blocked_pairing", "unmapped_scope", "ambiguous_mapping",
        "source_conflicts", "quantity_discrepancies", "unit_discrepancies",
        "non_comparable_missing_quote", "non_comparable_missing_external",
        "review_required_other",
    )
    review_count = sum(len(issue_sections.get(k, [])) for k in review_sections)
    clean_count = len(issue_sections.get("clean_matches", []))

    if review_count == 0 and clean_count > 0:
        return PACKET_NO_ISSUES
    if review_count > 0:
        return PACKET_DRAFT_READY
    return PACKET_PARTIAL


def _build_diagnostics(
    rows: List[Dict[str, Any]],
    issue_sections: Dict[str, List[Dict[str, Any]]],
    action_lookup: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    section_counts = {k: len(v) for k, v in issue_sections.items()}
    # Sanity: every row belongs to exactly one section.
    assigned = sum(section_counts.values())
    return {
        "input_row_count": len(rows),
        "assigned_row_count": assigned,
        "section_counts": section_counts,
        "rows_with_actions": sum(1 for r in rows
                                  if action_lookup.get(r.get("normalized_row_id"))),
        "action_output_supplied": bool(action_lookup),
    }
