"""
C17 — Reconciliation Output Contract Hardening.

Wraps the C16 reconciliation_foundation raw result in a stable, governed,
audit-friendly envelope so downstream layers (C18 discrepancy classification,
C19 findings packet, future reporting) can rely on a single explicit contract.

This module consumes the C16 reconcile_packet output and a C15 review packet
and produces a document-level contract with:

    - contract_version (deterministic constant)
    - reconciliation_status / pairing_status / packet_status / mapping_status
    - counts: rows_total, rows_compared, rows_matched, rows_mismatched,
              rows_non_comparable, rows_blocked
    - flag_counts (comparison flag histogram, deterministic ordering)
    - reconciliation_rows: deterministically ordered, each row carries
          normalized_row_id, mapping_outcome, comparison_status,
          comparison_flags, quote_values, bid_values, compared_fields,
          non_comparable_reason (if applicable), comparison_trace
    - pairing_diagnostics, mapping_summary (passed through unchanged)

Hard rules:
    - Never infer missing values.
    - Never collapse blocked / non_comparable / mismatch into softer buckets.
    - Never drop rows — every reconciliation row is represented.
    - Never mutate input objects.
    - Deterministic ordering: rows sorted by (source_page, normalized_row_id).
    - Stable key set: every row dict carries the same keys, regardless of
      its state, so downstream consumers can rely on presence.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

from .reconciliation_foundation import (
    COMPARISON_BLOCKED,
    COMPARISON_MATCH,
    COMPARISON_MISMATCH,
    COMPARISON_NON_COMPARABLE,
    RECON_BLOCKED,
    RECON_PARTIAL,
    RECON_READY,
)

CONTRACT_VERSION = "reconciliation_contract/v1"

# Row-level keys that must always appear on every contract row, in a stable
# order, so downstream consumers can rely on presence.
_ROW_KEYS = (
    "normalized_row_id",
    "source_page",
    "mapping_outcome",
    "mapped_bid_item",
    "comparison_status",
    "comparison_flags",
    "compared_fields",
    "non_comparable_reason",
    "quote_values",
    "bid_values",
    "comparison_trace",
)

# Comparison flags that indicate a field was actually compared (not just a
# missing-value marker). Used to compute compared_fields.
_FLAG_TO_COMPARED_FIELD = {
    "unit_match": "unit",
    "unit_conflict": "unit",
    "qty_match": "qty",
    "qty_conflict": "qty",
}


def build_reconciliation_contract(
    recon_result: Dict[str, Any],
    review_packet: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build the C17 hardened reconciliation contract.

    Args:
        recon_result:  output of reconcile_packet(...)
        review_packet: the C15 review packet the reconciliation was run on
                       (used for packet_status + source_page lookups)

    Returns a stable contract dict. Never mutates its inputs.
    """
    packet_status = review_packet.get("packet_status")
    pairing_diag = review_packet.get("pairing_diagnostics") or {}
    pairing_status = pairing_diag.get("pairing_status")
    mapping_summary = review_packet.get("mapping_summary") or {}
    mapping_status = mapping_summary.get("mapping_status")

    # Build a lookup for source_page + provenance by normalized_row_id from
    # the review packet so the contract row carries page provenance and C44
    # row-origin trace without re-reading the raw quote rows.
    page_lookup: Dict[str, Dict[str, Any]] = {}
    for rr in review_packet.get("review_rows", []) or []:
        rid = rr.get("normalized_row_id")
        if rid is not None:
            page_lookup[rid] = {
                "quote_source_page": rr.get("quote_source_page"),
                "row_origin": rr.get("row_origin"),
                "source_provenance": rr.get("source_provenance"),
                "manual_entry_ref": deepcopy(rr.get("manual_entry_ref")) if rr.get("manual_entry_ref") else None,
                "source_block_ref": deepcopy(rr.get("source_block_ref")) if rr.get("source_block_ref") else None,
            }

    raw_rows = recon_result.get("reconciliation_rows", []) or []

    contract_rows: List[Dict[str, Any]] = []
    for raw in raw_rows:
        contract_rows.append(_build_contract_row(raw, page_lookup))

    # Deterministic ordering: (source_page or -1, normalized_row_id).
    contract_rows.sort(key=_row_sort_key)

    counts = _tally_counts(contract_rows)
    flag_counts = _tally_flag_counts(contract_rows)

    return {
        "contract_version": CONTRACT_VERSION,
        "reconciliation_status": recon_result.get("reconciliation_status"),
        "packet_status": packet_status,
        "pairing_status": pairing_status,
        "mapping_status": mapping_status,
        "pairing_diagnostics": pairing_diag,
        "mapping_summary": mapping_summary,
        "reconciliation_summary": {
            "rows_total": counts["rows_total"],
            "rows_compared": counts["rows_compared"],
            "rows_matched": counts["rows_matched"],
            "rows_mismatched": counts["rows_mismatched"],
            "rows_non_comparable": counts["rows_non_comparable"],
            "rows_blocked": counts["rows_blocked"],
            "flag_counts": flag_counts,
        },
        "reconciliation_rows": contract_rows,
    }


# ---------------------------------------------------------------------------
# Row construction
# ---------------------------------------------------------------------------

def _build_contract_row(
    raw: Dict[str, Any],
    page_lookup: Dict[str, Any],
) -> Dict[str, Any]:
    """Assemble a single contract row with stable key presence.

    The raw row is a dict coming directly from reconcile_packet. This function
    never drops fields — it adds explicit stable keys and fills missing ones
    with None / [] / {} so downstream consumers can rely on presence.
    """
    normalized_row_id = raw.get("normalized_row_id")
    flags = list(raw.get("comparison_flags") or [])
    comparison_status = raw.get("comparison_status")
    comparison_trace = deepcopy(raw.get("comparison_trace") or {})

    compared_fields = _compared_fields_from_flags(flags)

    # non_comparable_reason is surfaced at the top level of the row (not just
    # buried inside trace) so consumers do not have to reach in. Kept as None
    # when the row is comparable, so the key is always present.
    non_comparable_reason: Optional[str] = None
    if comparison_status in (COMPARISON_NON_COMPARABLE, COMPARISON_BLOCKED):
        non_comparable_reason = comparison_trace.get("non_comparable_reason")

    quote_values = deepcopy(raw.get("quote_values") or {})
    bid_values_raw = raw.get("bid_values")
    bid_values = deepcopy(bid_values_raw) if bid_values_raw is not None else None

    mapped_bid_item = deepcopy(raw.get("mapped_bid_item")) if raw.get("mapped_bid_item") is not None else None

    lookup_entry = page_lookup.get(normalized_row_id) or {}
    source_page = lookup_entry.get("quote_source_page") if isinstance(lookup_entry, dict) else lookup_entry

    row: Dict[str, Any] = {
        "normalized_row_id": normalized_row_id,
        "source_page": source_page,
        "mapping_outcome": raw.get("mapping_outcome"),
        "mapped_bid_item": mapped_bid_item,
        "comparison_status": comparison_status,
        "comparison_flags": flags,
        "compared_fields": compared_fields,
        "non_comparable_reason": non_comparable_reason,
        "quote_values": quote_values,
        "bid_values": bid_values,
        "comparison_trace": comparison_trace,
    }

    # C44 provenance propagation — thread row_origin and related keys
    # from the review packet lookup so downstream layers can identify
    # machine vs manual rows.
    if isinstance(lookup_entry, dict):
        for pkey in ("row_origin", "source_provenance", "manual_entry_ref", "source_block_ref"):
            val = lookup_entry.get(pkey)
            if val is not None:
                row[pkey] = deepcopy(val)

    # Enforce stable key order for determinism in downstream serialization.
    # Include provenance keys after the base set so they appear in output.
    ordered = {k: row.get(k) for k in _ROW_KEYS}
    for extra in ("row_origin", "source_provenance", "manual_entry_ref", "source_block_ref"):
        if extra in row:
            ordered[extra] = row[extra]
    return ordered


def _compared_fields_from_flags(flags: List[str]) -> List[str]:
    """Explicitly list fields that were compared, in deterministic order."""
    seen: List[str] = []
    for f in flags:
        field = _FLAG_TO_COMPARED_FIELD.get(f)
        if field and field not in seen:
            seen.append(field)
    # Fixed canonical ordering: unit, then qty.
    order = ("unit", "qty")
    return [f for f in order if f in seen]


def _row_sort_key(row: Dict[str, Any]):
    """Deterministic sort key: (source_page or -1, normalized_row_id str)."""
    sp = row.get("source_page")
    sp_key = sp if isinstance(sp, int) else -1
    rid = row.get("normalized_row_id")
    rid_key = str(rid) if rid is not None else ""
    return (sp_key, rid_key)


# ---------------------------------------------------------------------------
# Counts
# ---------------------------------------------------------------------------

def _tally_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    rows_total = len(rows)
    rows_compared = 0
    rows_matched = 0
    rows_mismatched = 0
    rows_non_comparable = 0
    rows_blocked = 0
    for r in rows:
        st = r.get("comparison_status")
        if st == COMPARISON_MATCH:
            rows_matched += 1
            rows_compared += 1
        elif st == COMPARISON_MISMATCH:
            rows_mismatched += 1
            rows_compared += 1
        elif st == COMPARISON_BLOCKED:
            rows_blocked += 1
        elif st == COMPARISON_NON_COMPARABLE:
            rows_non_comparable += 1
        else:
            # Defensive: unknown status counted as non-comparable so nothing
            # silently disappears from the totals.
            rows_non_comparable += 1
    return {
        "rows_total": rows_total,
        "rows_compared": rows_compared,
        "rows_matched": rows_matched,
        "rows_mismatched": rows_mismatched,
        "rows_non_comparable": rows_non_comparable,
        "rows_blocked": rows_blocked,
    }


def _tally_flag_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """Flag histogram with deterministic key ordering (sorted)."""
    counts: Dict[str, int] = {}
    for r in rows:
        for f in r.get("comparison_flags", []) or []:
            counts[f] = counts.get(f, 0) + 1
    return {k: counts[k] for k in sorted(counts.keys())}
