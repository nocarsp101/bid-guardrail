# backend/app/pdf_extraction/review_packet.py
"""
C15 — Mapping review packet / discrepancy output contract.

Builds a governed, auditable review packet on top of:
    - C14 pairing diagnostics
    - C13 mapping result
    - C12 normalized accepted quote rows
    - C8 DOT bid summary

The packet organizes what happened so an office reviewer can inspect
before reconciliation decisions. This module does NOT:
    - perform price / qty reconciliation
    - guess review flags
    - hide unmapped or blocked rows
    - blur the boundary between mapped / unmapped / ambiguous / blocked

Packet status semantics:
    ready    — trusted pairing AND every accepted quote row mapped
    partial  — trusted pairing but some rows unmapped / ambiguous, OR
               weak pairing (mapping ran but review must gate)
    blocked  — pairing rejected, mapping did not run; packet carries
               pairing diagnostics and row stubs for audit, but no
               mapping outcomes
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

PACKET_READY = "ready"
PACKET_PARTIAL = "partial"
PACKET_BLOCKED = "blocked"

# Deterministic row-level review flag codes.
FLAG_UNMAPPED = "unmapped_row"
FLAG_AMBIGUOUS = "ambiguous_mapping"
FLAG_BLOCKED_BY_PAIRING = "blocked_by_pairing"
FLAG_WEAK_PAIRING = "mapping_under_weak_pairing"
FLAG_MISSING_QTY = "missing_qty"
FLAG_MISSING_UNIT = "missing_unit"
FLAG_MISSING_UNIT_PRICE = "missing_unit_price"
FLAG_MISSING_AMOUNT = "missing_amount"

# Blocked mapping outcome used when pairing has rejected the pair.
BLOCKED_OUTCOME = "blocked"


def build_review_packet(
    pairing_diagnostics: Dict[str, Any],
    mapping_result: Optional[Dict[str, Any]],
    accepted_rows: List[Dict[str, Any]],
    quote_diagnostics: Dict[str, Any],
    bid_summary: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Assemble the governed review packet.

    Args:
        pairing_diagnostics: C14 analyze_pairing output
        mapping_result: C13 map_quote_to_bid output, or None when pairing
                        blocked mapping from running
        accepted_rows: C12 normalized accepted rows (audit copy)
        quote_diagnostics: C10 staging diagnostics
        bid_summary: projection of the C8 DOT summary

    Never mutates inputs.
    """
    pairing_status = pairing_diagnostics.get("pairing_status")

    # Build row-level review entries.
    review_rows: List[Dict[str, Any]] = []

    if pairing_status == "rejected":
        # Mapping did not run. Emit stub rows so reviewers still see
        # which accepted quote rows exist; every row carries the
        # blocked_by_pairing flag and blocked outcome.
        for row in accepted_rows:
            review_rows.append(
                _build_blocked_row(row, pairing_diagnostics)
            )
        packet_status = PACKET_BLOCKED
    else:
        # Pairing allowed mapping; walk the mapping results and stitch
        # each one to its accepted-row audit data.
        mapping_results = (mapping_result or {}).get("mapping_results", [])
        accepted_by_id = {
            r.get("normalized_row_id"): r for r in accepted_rows
        }
        for m in mapping_results:
            row = accepted_by_id.get(m.get("normalized_row_id"), {})
            review_rows.append(_build_review_row(row, m, pairing_status))
        packet_status = _derive_packet_status(
            pairing_status=pairing_status,
            mapping_results=mapping_results,
        )

    # Packet-level diagnostic counters.
    counts = _tally(review_rows)

    return {
        "packet_status": packet_status,
        "pairing_diagnostics": pairing_diagnostics,
        "quote_summary": {
            "accepted_rows_count": len(accepted_rows),
            "extraction_source": (quote_diagnostics or {}).get("extraction_source"),
            "ocr_used": (quote_diagnostics or {}).get("ocr_used"),
            "status": (quote_diagnostics or {}).get("status"),
        },
        "bid_summary": bid_summary,
        "mapping_summary": _mapping_summary(mapping_result, pairing_status),
        "review_rows": review_rows,
        "packet_diagnostics": {
            "mapped_count": counts["mapped"],
            "unmapped_count": counts["unmapped"],
            "ambiguous_count": counts["ambiguous"],
            "blocked_count": counts["blocked"],
            "rows_ready_for_reconciliation": counts["mapped"] if packet_status != PACKET_BLOCKED else 0,
            "total_review_rows": len(review_rows),
            "flag_counts": counts["flag_counts"],
        },
    }


def _build_review_row(
    row: Dict[str, Any],
    mapping: Dict[str, Any],
    pairing_status: str,
) -> Dict[str, Any]:
    """Construct a per-row review entry from a mapped/unmapped/ambiguous result."""
    outcome = mapping.get("mapping_outcome")
    flags = _row_flags_from_outcome(outcome, pairing_status)
    flags.extend(_row_flags_from_values(row))
    result = {
        "normalized_row_id": mapping.get("normalized_row_id"),
        "quote_description": mapping.get("quote_description"),
        "quote_line_ref": mapping.get("quote_line_ref"),
        "quote_amount": row.get("amount"),
        "quote_unit_price": row.get("unit_price"),
        "quote_qty": row.get("qty"),
        "quote_unit": row.get("unit"),
        "quote_source_page": row.get("source_page"),
        "mapping_outcome": outcome,
        "mapping_reason": mapping.get("mapping_reason"),
        "mapped_bid_item": mapping.get("mapped_bid_item"),
        "review_flags": flags,
        "mapping_trace_summary": _trace_summary(mapping.get("mapping_trace")),
    }
    _thread_provenance(result, row)
    return result


def _build_blocked_row(
    row: Dict[str, Any],
    pairing_diagnostics: Dict[str, Any],
) -> Dict[str, Any]:
    """Construct a stub review row when pairing blocked mapping."""
    flags = [FLAG_BLOCKED_BY_PAIRING]
    flags.extend(_row_flags_from_values(row))
    result = {
        "normalized_row_id": row.get("normalized_row_id"),
        "quote_description": row.get("description"),
        "quote_line_ref": row.get("line_ref"),
        "quote_amount": row.get("amount"),
        "quote_unit_price": row.get("unit_price"),
        "quote_qty": row.get("qty"),
        "quote_unit": row.get("unit"),
        "quote_source_page": row.get("source_page"),
        "mapping_outcome": BLOCKED_OUTCOME,
        "mapping_reason": pairing_diagnostics.get("pairing_reason"),
        "mapped_bid_item": None,
        "review_flags": flags,
        "mapping_trace_summary": {"blocked_by_pairing": True},
    }
    _thread_provenance(result, row)
    return result


def _row_flags_from_outcome(outcome: Optional[str], pairing_status: str) -> List[str]:
    """Deterministic flag mapping based on outcome + pairing status."""
    flags: List[str] = []
    if outcome == "unmapped":
        flags.append(FLAG_UNMAPPED)
    elif outcome == "ambiguous":
        flags.append(FLAG_AMBIGUOUS)
    if pairing_status == "weak":
        flags.append(FLAG_WEAK_PAIRING)
    return flags


def _row_flags_from_values(row: Dict[str, Any]) -> List[str]:
    """Deterministic flags based on which quote fields are missing."""
    flags: List[str] = []
    if row.get("qty") is None:
        flags.append(FLAG_MISSING_QTY)
    if row.get("unit") is None:
        flags.append(FLAG_MISSING_UNIT)
    if row.get("unit_price") is None:
        flags.append(FLAG_MISSING_UNIT_PRICE)
    if row.get("amount") is None:
        flags.append(FLAG_MISSING_AMOUNT)
    return flags


def _derive_packet_status(
    pairing_status: str,
    mapping_results: List[Dict[str, Any]],
) -> str:
    """Compute packet_status from pairing + per-row mapping outcomes."""
    if pairing_status == "weak":
        # Weak pairing is never "ready". The reviewer must gate on it.
        return PACKET_PARTIAL
    if pairing_status == "rejected":
        return PACKET_BLOCKED

    if not mapping_results:
        return PACKET_PARTIAL

    non_mapped = sum(
        1 for m in mapping_results if m.get("mapping_outcome") != "mapped"
    )
    if non_mapped == 0:
        return PACKET_READY
    return PACKET_PARTIAL


def _mapping_summary(
    mapping_result: Optional[Dict[str, Any]],
    pairing_status: str,
) -> Dict[str, Any]:
    if mapping_result is None or pairing_status == "rejected":
        return {
            "mapping_status": "blocked_by_pairing",
            "mapped_count": 0,
            "unmapped_count": 0,
            "ambiguous_count": 0,
        }
    diag = mapping_result.get("mapping_diagnostics", {})
    return {
        "mapping_status": mapping_result.get("mapping_status"),
        "mapped_count": diag.get("mapped_count", 0),
        "unmapped_count": diag.get("unmapped_count", 0),
        "ambiguous_count": diag.get("ambiguous_count", 0),
    }


def _tally(review_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    mapped = unmapped = ambiguous = blocked = 0
    flag_counts: Dict[str, int] = {}
    for r in review_rows:
        o = r.get("mapping_outcome")
        if o == "mapped":
            mapped += 1
        elif o == "unmapped":
            unmapped += 1
        elif o == "ambiguous":
            ambiguous += 1
        elif o == BLOCKED_OUTCOME:
            blocked += 1
        for f in r.get("review_flags", []):
            flag_counts[f] = flag_counts.get(f, 0) + 1
    return {
        "mapped": mapped,
        "unmapped": unmapped,
        "ambiguous": ambiguous,
        "blocked": blocked,
        "flag_counts": flag_counts,
    }


_PROVENANCE_KEYS = ("row_origin", "source_provenance", "manual_entry_ref", "source_block_ref")


def _thread_provenance(target: Dict[str, Any], source: Dict[str, Any]) -> None:
    """Copy C44 provenance keys from source into target when present."""
    for key in _PROVENANCE_KEYS:
        val = source.get(key)
        if val is not None:
            from copy import deepcopy
            target[key] = deepcopy(val)


def _trace_summary(trace: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Condense the mapping trace into a review-friendly summary."""
    if not trace:
        return {}
    rules = trace.get("rules_attempted", [])
    return {
        "rules_attempted_count": len(rules),
        "rules": [
            {
                "rule": r.get("rule"),
                "candidate_count": r.get("candidate_count"),
                "skipped": r.get("skipped"),
            }
            for r in rules
        ],
    }
