"""
C19 — Findings packet foundation.

Assembles upstream governed outputs (pairing, mapping, reconciliation,
discrepancy classification) into a single deterministic packet artifact
suitable for office review and later export. This module:

    - does NOT generate narrative / prose / letters / claims language
    - does NOT add new business judgment
    - does NOT mutate upstream data
    - does NOT hide blocked / unmapped / ambiguous / non-comparable states
    - uses only templated deterministic section + status labels

The packet has a stable structure with these sections:

    packet_status        — "ready" | "partial" | "blocked"
    pairing_section
    quote_section
    bid_section
    mapping_section
    reconciliation_section
    discrepancy_summary
    findings_rows[]
    packet_diagnostics
    packet_version

Each `findings_row` carries the minimum row-level evidence a reviewer
needs: quote description, mapping outcome, comparison status, discrepancy
class, review flags, and a deterministic `finding_trace` pointing back
to mapping + classification decisions.

The packet itself is a defensible, machine-readable artifact. It is
explicitly not the final transmittal or letter — that is a later layer
that takes this packet as input.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

PACKET_VERSION = "findings_packet/v1"

PACKET_READY = "ready"
PACKET_PARTIAL = "partial"
PACKET_BLOCKED = "blocked"


def build_findings_packet(
    review_packet: Dict[str, Any],
    classified_contract: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Assemble the C19 findings packet foundation.

    Args:
        review_packet:       the C15 review packet (contains pairing,
                             quote_summary, bid_summary, mapping_summary,
                             review_rows)
        classified_contract: the C17 contract enriched by the C18
                             discrepancy classifier (contains office
                             review summary + discrepancy_class per row)

    Returns a stable deterministic packet dict. Does not mutate its
    inputs.
    """
    # The packet_status surfaces the underlying review_packet status, which
    # already encodes "blocked" / "partial" / "ready" semantics from C15.
    review_packet_status = review_packet.get("packet_status") or PACKET_PARTIAL
    if review_packet_status not in (PACKET_READY, PACKET_PARTIAL, PACKET_BLOCKED):
        review_packet_status = PACKET_PARTIAL

    pairing_diag = review_packet.get("pairing_diagnostics") or {}
    pairing_status = pairing_diag.get("pairing_status")

    # Sections are deep-copied slices so the packet is self-contained and
    # does not share references with its inputs.
    pairing_section = _build_pairing_section(pairing_diag)
    quote_section = _build_quote_section(review_packet.get("quote_summary") or {})
    bid_section = _build_bid_section(review_packet.get("bid_summary") or {})
    mapping_section = _build_mapping_section(review_packet.get("mapping_summary") or {})
    reconciliation_section = _build_reconciliation_section(classified_contract)
    discrepancy_summary = _build_discrepancy_summary(classified_contract)
    findings_rows = _build_findings_rows(review_packet, classified_contract)
    packet_diagnostics = _build_packet_diagnostics(
        review_packet=review_packet,
        classified_contract=classified_contract,
        findings_rows=findings_rows,
    )

    return {
        "packet_version": PACKET_VERSION,
        "packet_status": review_packet_status,
        "pairing_section": pairing_section,
        "quote_section": quote_section,
        "bid_section": bid_section,
        "mapping_section": mapping_section,
        "reconciliation_section": reconciliation_section,
        "discrepancy_summary": discrepancy_summary,
        "findings_rows": findings_rows,
        "packet_diagnostics": packet_diagnostics,
    }


# ---------------------------------------------------------------------------
# Section builders — every builder returns a new dict with a stable shape.
# ---------------------------------------------------------------------------

def _build_pairing_section(pairing_diag: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "pairing_status": pairing_diag.get("pairing_status"),
        "pairing_reason": pairing_diag.get("pairing_reason"),
        "allow_mapping": pairing_diag.get("allow_mapping"),
        "signals": deepcopy(pairing_diag.get("signals") or {}),
        "warnings": list(pairing_diag.get("warnings") or []),
    }


def _build_quote_section(quote_summary: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "accepted_rows_count": quote_summary.get("accepted_rows_count"),
        "extraction_source": quote_summary.get("extraction_source"),
        "ocr_used": quote_summary.get("ocr_used"),
        "status": quote_summary.get("status"),
    }


def _build_bid_section(bid_summary: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "rows_extracted": bid_summary.get("rows_extracted"),
        "format_detected": bid_summary.get("format_detected"),
        "document_class": bid_summary.get("document_class"),
        "extraction_source": bid_summary.get("extraction_source"),
    }


def _build_mapping_section(mapping_summary: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "mapping_status": mapping_summary.get("mapping_status"),
        "mapped_count": mapping_summary.get("mapped_count", 0),
        "unmapped_count": mapping_summary.get("unmapped_count", 0),
        "ambiguous_count": mapping_summary.get("ambiguous_count", 0),
    }


def _build_reconciliation_section(contract: Dict[str, Any]) -> Dict[str, Any]:
    recon_summary = contract.get("reconciliation_summary") or {}
    return {
        "contract_version": contract.get("contract_version"),
        "reconciliation_status": contract.get("reconciliation_status"),
        "pairing_status": contract.get("pairing_status"),
        "packet_status": contract.get("packet_status"),
        "mapping_status": contract.get("mapping_status"),
        "rows_total": recon_summary.get("rows_total", 0),
        "rows_compared": recon_summary.get("rows_compared", 0),
        "rows_matched": recon_summary.get("rows_matched", 0),
        "rows_mismatched": recon_summary.get("rows_mismatched", 0),
        "rows_non_comparable": recon_summary.get("rows_non_comparable", 0),
        "rows_blocked": recon_summary.get("rows_blocked", 0),
        "flag_counts": deepcopy(recon_summary.get("flag_counts") or {}),
    }


def _build_discrepancy_summary(contract: Dict[str, Any]) -> Dict[str, Any]:
    office = contract.get("office_review_summary") or {}
    return {
        "classification_version": contract.get("classification_version"),
        **{k: v for k, v in office.items()},
    }


# ---------------------------------------------------------------------------
# Findings rows
# ---------------------------------------------------------------------------

def _build_findings_rows(
    review_packet: Dict[str, Any],
    contract: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Walk the contract rows and emit stable finding entries.

    Row ordering follows the C17 contract's deterministic ordering.
    Every contract row becomes exactly one finding row — nothing hides.
    """
    # Build a lookup from the C15 review rows so descriptions etc. are
    # available on the finding row without re-reading upstream data.
    review_rows = {
        r.get("normalized_row_id"): r
        for r in (review_packet.get("review_rows") or [])
    }

    findings: List[Dict[str, Any]] = []
    for row in contract.get("reconciliation_rows") or []:
        rid = row.get("normalized_row_id")
        source_row = review_rows.get(rid, {})
        findings.append(_build_finding_row(row, source_row))
    return findings


def _build_finding_row(
    contract_row: Dict[str, Any],
    source_row: Dict[str, Any],
) -> Dict[str, Any]:
    """A single stable finding row entry."""
    quote_description = source_row.get("quote_description")
    quote_line_ref = source_row.get("quote_line_ref")

    mapped_bid_item = contract_row.get("mapped_bid_item")
    mapped_bid_line = None
    mapped_bid_item_number = None
    if mapped_bid_item:
        mapped_bid_line = mapped_bid_item.get("line_number")
        mapped_bid_item_number = mapped_bid_item.get("item_number")

    classification_trace = deepcopy(contract_row.get("classification_trace") or {})
    comparison_trace = deepcopy(contract_row.get("comparison_trace") or {})
    mapping_trace_summary = deepcopy(source_row.get("mapping_trace_summary") or {})

    finding_trace = {
        "mapping_reason": source_row.get("mapping_reason"),
        "mapping_trace_summary": mapping_trace_summary,
        "comparison_trace": comparison_trace,
        "classification_trace": classification_trace,
    }

    result = {
        "normalized_row_id": contract_row.get("normalized_row_id"),
        "source_page": contract_row.get("source_page"),
        "quote_description": quote_description,
        "quote_line_ref": quote_line_ref,
        "mapped_bid_line_number": mapped_bid_line,
        "mapped_bid_item_number": mapped_bid_item_number,
        "mapping_outcome": contract_row.get("mapping_outcome"),
        "comparison_status": contract_row.get("comparison_status"),
        "compared_fields": list(contract_row.get("compared_fields") or []),
        "non_comparable_reason": contract_row.get("non_comparable_reason"),
        "discrepancy_class": contract_row.get("discrepancy_class"),
        "review_flags": list(source_row.get("review_flags") or []),
        "comparison_flags": list(contract_row.get("comparison_flags") or []),
        "quote_values": deepcopy(contract_row.get("quote_values") or {}),
        "bid_values": deepcopy(contract_row.get("bid_values")) if contract_row.get("bid_values") is not None else None,
        "finding_trace": finding_trace,
    }
    # C44 provenance propagation — prefer contract_row, fall back to source_row.
    for pkey in ("row_origin", "source_provenance", "manual_entry_ref", "source_block_ref"):
        val = contract_row.get(pkey) or source_row.get(pkey)
        if val is not None:
            result[pkey] = deepcopy(val) if isinstance(val, dict) else val
    return result


# ---------------------------------------------------------------------------
# Packet diagnostics
# ---------------------------------------------------------------------------

def _build_packet_diagnostics(
    review_packet: Dict[str, Any],
    classified_contract: Dict[str, Any],
    findings_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    packet_diag = review_packet.get("packet_diagnostics") or {}
    office = classified_contract.get("office_review_summary") or {}
    recon_summary = classified_contract.get("reconciliation_summary") or {}

    # Surface whether there are any unresolved states — blocked, unmapped,
    # ambiguous, missing-info, or comparable-mismatch. Pure arithmetic,
    # no judgment.
    unresolved_counts = {
        "blocked_count": office.get("blocked_count", 0),
        "unmapped_count": office.get("unmapped_count", 0),
        "ambiguous_count": office.get("ambiguous_count", 0),
        "missing_quote_info_count": office.get("missing_quote_info_count", 0),
        "missing_bid_info_count": office.get("missing_bid_info_count", 0),
        "comparable_mismatch_unit_count": office.get("comparable_mismatch_unit_count", 0),
        "comparable_mismatch_qty_count": office.get("comparable_mismatch_qty_count", 0),
        "comparable_mismatch_multi_count": office.get("comparable_mismatch_multi_count", 0),
        "review_required_other_count": office.get("review_required_other_count", 0),
    }
    unresolved_total = sum(unresolved_counts.values())

    return {
        "findings_row_count": len(findings_rows),
        "review_packet_total_rows": packet_diag.get("total_review_rows"),
        "rows_ready_for_reconciliation": packet_diag.get("rows_ready_for_reconciliation"),
        "reconciliation_rows_total": recon_summary.get("rows_total", 0),
        "unresolved_total": unresolved_total,
        "unresolved_counts": unresolved_counts,
    }
