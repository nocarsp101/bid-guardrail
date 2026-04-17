# backend/app/pdf_extraction/reconciliation_foundation.py
"""
C16 — Reconciliation foundation (trusted-pair only).

Performs explicit deterministic comparisons between governed C12
normalized accepted quote rows and the C8 DOT bid rows they were
mapped to — but ONLY when pairing was not rejected, and ONLY for rows
whose mapping_outcome is "mapped".

Hard rules:
    - Consume only the output of upstream governed stages.
    - Compare only fields that explicitly exist on BOTH sides.
    - Never infer a missing field.
    - Never auto-resolve a conflict.
    - Never produce commercial / claims language.
    - Unmapped, ambiguous, and blocked rows stay NON-COMPARABLE.
    - Weak pairing still allows comparison but the reconciliation
      status is at most "partial" — the packet_status determines
      whether comparison runs at all.

Comparable fields (DOT bid items have: line_number, item, description,
qty, unit; quote rows have: line_ref, description, qty, unit, unit_price,
amount):

    - unit: direct canonical comparison (upper-case trimmed), only when
      both sides have a non-null unit.
    - qty:  direct numeric comparison with a 0.5% tolerance, only when
      both sides have non-null qty.
    - description alignment: already enforced by the mapping rule.
    - amount: never compared because the C8 DOT bid items do not carry
      a monetary amount. `missing_bid_amount` is reported but this is
      not a conflict.

The module returns per-row comparison flags and a document-level
reconciliation_status:
    ready    — every mapped row produced at least one "match" signal and
               no mismatches; no missing-bid values on mapped rows
    partial  — some rows have missing values or mismatches; some rows
               are non-comparable; weak pairing; unmapped rows present
    blocked  — packet is blocked (pairing rejected) — comparison did
               not run
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from .quote_to_bid_mapping import _canonical_description

RECON_READY = "ready"
RECON_PARTIAL = "partial"
RECON_BLOCKED = "blocked"

COMPARISON_MATCH = "match"
COMPARISON_MISMATCH = "mismatch"
COMPARISON_NON_COMPARABLE = "non_comparable"
COMPARISON_BLOCKED = "blocked"

# Per-row comparison flags.
FLAG_MISSING_QUOTE_QTY = "missing_quote_qty"
FLAG_MISSING_QUOTE_UNIT = "missing_quote_unit"
FLAG_MISSING_QUOTE_UNIT_PRICE = "missing_quote_unit_price"
FLAG_MISSING_QUOTE_AMOUNT = "missing_quote_amount"
FLAG_MISSING_BID_QTY = "missing_bid_qty"
FLAG_MISSING_BID_UNIT = "missing_bid_unit"
FLAG_MISSING_BID_AMOUNT = "missing_bid_amount"  # always true structurally
FLAG_UNIT_MATCH = "unit_match"
FLAG_UNIT_CONFLICT = "unit_conflict"
FLAG_QTY_MATCH = "qty_match"
FLAG_QTY_CONFLICT = "qty_conflict"

# Non-comparable reasons.
NC_UNMAPPED = "row_not_mapped"
NC_AMBIGUOUS = "row_ambiguous"
NC_BLOCKED = "row_blocked_by_pairing"
NC_NO_OVERLAPPING_FIELDS = "no_overlapping_fields_to_compare"

# Numeric comparison tolerance (fractional). 0.5% — tight but tolerates
# typical rounding drift. Not a fuzzy signal; it's an explicit numeric
# equality rule with a named tolerance.
QTY_TOLERANCE = 0.005


def reconcile_packet(packet: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the reconciliation foundation result from a C15 review packet.

    Returns a dict with:
        {
            reconciliation_status: "ready" | "partial" | "blocked",
            pairing_diagnostics,
            mapping_summary,
            reconciliation_summary: {
                rows_compared, rows_non_comparable, matches, mismatches
            },
            reconciliation_rows: [
                {
                    normalized_row_id,
                    mapping_outcome,
                    mapped_bid_item,
                    comparison_status,
                    comparison_flags,
                    quote_values,
                    bid_values,
                    comparison_trace,
                }
            ]
        }
    """
    packet_status = packet.get("packet_status")
    pairing_diag = packet.get("pairing_diagnostics", {})
    review_rows = packet.get("review_rows", [])

    if packet_status == "blocked":
        rows = [_blocked_recon_row(r) for r in review_rows]
        return {
            "reconciliation_status": RECON_BLOCKED,
            "pairing_diagnostics": pairing_diag,
            "mapping_summary": packet.get("mapping_summary"),
            "reconciliation_summary": {
                "rows_compared": 0,
                "rows_non_comparable": len(rows),
                "matches": 0,
                "mismatches": 0,
            },
            "reconciliation_rows": rows,
        }

    matches = 0
    mismatches = 0
    rows_compared = 0
    rows_non_comparable = 0
    recon_rows: List[Dict[str, Any]] = []

    for r in review_rows:
        outcome = r.get("mapping_outcome")

        if outcome == "mapped":
            entry = _compare_mapped_row(r)
            status = entry["comparison_status"]
            if status == COMPARISON_MATCH:
                matches += 1
                rows_compared += 1
            elif status == COMPARISON_MISMATCH:
                mismatches += 1
                rows_compared += 1
            else:
                # non_comparable (mapped but no overlapping fields)
                rows_non_comparable += 1
            recon_rows.append(entry)
        elif outcome in ("unmapped", "ambiguous", "blocked"):
            entry = _non_comparable_row(r, outcome)
            rows_non_comparable += 1
            recon_rows.append(entry)
        else:
            entry = _non_comparable_row(r, "unknown_outcome")
            rows_non_comparable += 1
            recon_rows.append(entry)

    reconciliation_status = _derive_reconciliation_status(
        packet_status=packet_status,
        matches=matches,
        mismatches=mismatches,
        rows_compared=rows_compared,
        non_comparable=rows_non_comparable,
    )

    return {
        "reconciliation_status": reconciliation_status,
        "pairing_diagnostics": pairing_diag,
        "mapping_summary": packet.get("mapping_summary"),
        "reconciliation_summary": {
            "rows_compared": rows_compared,
            "rows_non_comparable": rows_non_comparable,
            "matches": matches,
            "mismatches": mismatches,
        },
        "reconciliation_rows": recon_rows,
    }


# ---------------------------------------------------------------------------
# Per-row comparison
# ---------------------------------------------------------------------------

def _compare_mapped_row(review_row: Dict[str, Any]) -> Dict[str, Any]:
    """Compare explicit fields between a mapped quote row and its bid target."""
    mapped_bid = review_row.get("mapped_bid_item") or {}

    quote_values = {
        "description": review_row.get("quote_description"),
        "line_ref": review_row.get("quote_line_ref"),
        "qty": review_row.get("quote_qty"),
        "unit": review_row.get("quote_unit"),
        "unit_price": review_row.get("quote_unit_price"),
        "amount": review_row.get("quote_amount"),
    }
    bid_values = {
        "line_number": mapped_bid.get("line_number"),
        "item_number": mapped_bid.get("item_number"),
        "description": mapped_bid.get("description"),
        "qty": mapped_bid.get("qty"),
        "unit": mapped_bid.get("unit"),
    }

    flags: List[str] = []
    trace: Dict[str, Any] = {"comparisons": []}

    # Missing-value flags (informational, deterministic).
    if quote_values["qty"] is None:
        flags.append(FLAG_MISSING_QUOTE_QTY)
    if quote_values["unit"] is None:
        flags.append(FLAG_MISSING_QUOTE_UNIT)
    if quote_values["unit_price"] is None:
        flags.append(FLAG_MISSING_QUOTE_UNIT_PRICE)
    if quote_values["amount"] is None:
        flags.append(FLAG_MISSING_QUOTE_AMOUNT)
    if bid_values["qty"] is None:
        flags.append(FLAG_MISSING_BID_QTY)
    if bid_values["unit"] is None:
        flags.append(FLAG_MISSING_BID_UNIT)
    # DOT bid items never carry a monetary amount — structural.
    flags.append(FLAG_MISSING_BID_AMOUNT)

    have_compared_any = False
    conflict = False

    # Unit comparison.
    qu = quote_values["unit"]
    bu = bid_values["unit"]
    if qu is not None and bu is not None:
        have_compared_any = True
        if _canonical_description(str(qu)) == _canonical_description(str(bu)):
            flags.append(FLAG_UNIT_MATCH)
            trace["comparisons"].append({"field": "unit", "result": "match"})
        else:
            flags.append(FLAG_UNIT_CONFLICT)
            conflict = True
            trace["comparisons"].append({
                "field": "unit",
                "result": "conflict",
                "quote": str(qu),
                "bid": str(bu),
            })

    # Qty comparison.
    qq = quote_values["qty"]
    bq = bid_values["qty"]
    if qq is not None and bq is not None:
        have_compared_any = True
        if _qty_match(float(qq), float(bq)):
            flags.append(FLAG_QTY_MATCH)
            trace["comparisons"].append({"field": "qty", "result": "match"})
        else:
            flags.append(FLAG_QTY_CONFLICT)
            conflict = True
            trace["comparisons"].append({
                "field": "qty",
                "result": "conflict",
                "quote": float(qq),
                "bid": float(bq),
                "tolerance": QTY_TOLERANCE,
            })

    if not have_compared_any:
        status = COMPARISON_NON_COMPARABLE
        trace["non_comparable_reason"] = NC_NO_OVERLAPPING_FIELDS
    elif conflict:
        status = COMPARISON_MISMATCH
    else:
        status = COMPARISON_MATCH

    return {
        "normalized_row_id": review_row.get("normalized_row_id"),
        "mapping_outcome": "mapped",
        "mapped_bid_item": mapped_bid,
        "comparison_status": status,
        "comparison_flags": flags,
        "quote_values": quote_values,
        "bid_values": bid_values,
        "comparison_trace": trace,
    }


def _non_comparable_row(review_row: Dict[str, Any], outcome: str) -> Dict[str, Any]:
    reason_map = {
        "unmapped": NC_UNMAPPED,
        "ambiguous": NC_AMBIGUOUS,
        "blocked": NC_BLOCKED,
    }
    return {
        "normalized_row_id": review_row.get("normalized_row_id"),
        "mapping_outcome": outcome,
        "mapped_bid_item": None,
        "comparison_status": COMPARISON_NON_COMPARABLE,
        "comparison_flags": [],
        "quote_values": {
            "description": review_row.get("quote_description"),
            "line_ref": review_row.get("quote_line_ref"),
            "qty": review_row.get("quote_qty"),
            "unit": review_row.get("quote_unit"),
            "unit_price": review_row.get("quote_unit_price"),
            "amount": review_row.get("quote_amount"),
        },
        "bid_values": None,
        "comparison_trace": {
            "non_comparable_reason": reason_map.get(outcome, "unknown_outcome"),
        },
    }


def _blocked_recon_row(review_row: Dict[str, Any]) -> Dict[str, Any]:
    entry = _non_comparable_row(review_row, "blocked")
    entry["comparison_status"] = COMPARISON_BLOCKED
    return entry


def _qty_match(q: float, b: float) -> bool:
    """Numeric equality within QTY_TOLERANCE fraction. Deterministic."""
    if q == b:
        return True
    if b == 0:
        return q == 0
    return abs(q - b) / abs(b) <= QTY_TOLERANCE


def _derive_reconciliation_status(
    packet_status: str,
    matches: int,
    mismatches: int,
    rows_compared: int,
    non_comparable: int,
) -> str:
    """Document-level reconciliation status."""
    if packet_status == "blocked":
        return RECON_BLOCKED
    if mismatches == 0 and non_comparable == 0 and rows_compared > 0:
        return RECON_READY
    return RECON_PARTIAL
