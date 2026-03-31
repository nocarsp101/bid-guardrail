# backend/app/quote_validation/compare.py
from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional

from app.bid_validation.normalize import to_num


def compare_quotes_to_bid(
    bid_rows: List[Dict[str, Any]],
    quote_rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Deterministic comparison rules (per customer):
      - match_key = item (normalized exact match)
      - unit must match exactly (no conversions) -> FAIL
      - if quote.unit_price > bid.unit_price -> FAIL
      - unmatched quote line -> FAIL (fail-closed)
      - totals cross-check reported; totals mismatch => WARN by default (tolerance 0)
    """
    findings: List[Dict[str, Any]] = []

    bid_by_item: Dict[str, Dict[str, Any]] = {}
    for b in bid_rows:
        key = str(b.get("item") or "").strip()
        if key:
            # if bid has duplicate same item code, treat as ambiguous deterministic FAIL
            if key in bid_by_item:
                findings.append({
                    "type": "bid_item_duplicate_key",
                    "severity": "FAIL",
                    "message": "Duplicate bid item identifier detected (ambiguous deterministic mapping).",
                    "pages": [],
                    "row_index": b.get("_row_index"),
                    "item_ref": key,
                    "meta": {"match_key": key},
                })
            else:
                bid_by_item[key] = b

    unmatched: List[Dict[str, Any]] = []
    ambiguous: List[Dict[str, Any]] = []  # reserved if you add future deterministic ambiguity detection
    comparisons: List[Dict[str, Any]] = []

    mapped_bid_subtotal = 0.0
    quote_subtotal = 0.0

    # Quote subtotal is sum(qty*unit_price) for all quote lines with numbers
    for q in quote_rows:
        q_qty = to_num(q.get("qty"))
        q_up = to_num(q.get("unit_price"))
        if q_qty is not None and q_up is not None:
            quote_subtotal += float(q_qty) * float(q_up)

    for q in quote_rows:
        q_item = str(q.get("item") or "").strip()
        q_unit = str(q.get("unit") or "").strip()

        if not q_item:
            findings.append({
                "type": "quote_line_missing_item_identifier",
                "severity": "FAIL",
                "message": "Quote line missing item identifier required for deterministic mapping (fail-closed).",
                "pages": [],
                "row_index": q.get("_row_index"),
                "item_ref": f"quote_row_{q.get('_row_index')}",
                "meta": {},
            })
            unmatched.append({"row_index": q.get("_row_index"), "item": "", "reason": "missing_item"})
            continue

        b = bid_by_item.get(q_item)
        if not b:
            findings.append({
                "type": "quote_line_unmatched",
                "severity": "FAIL",
                "message": "No deterministic match found for quote line item in bid items (fail-closed).",
                "pages": [],
                "row_index": q.get("_row_index"),
                "item_ref": q_item,
                "meta": {"match_key": q_item},
            })
            unmatched.append({"row_index": q.get("_row_index"), "item": q_item, "reason": "no_match"})
            continue

        b_unit = str(b.get("unit") or "").strip()
        if q_unit != b_unit:
            findings.append({
                "type": "quote_unit_mismatch",
                "severity": "FAIL",
                "message": "Unit mismatch between quote and bid (no conversions).",
                "pages": [],
                "row_index": q.get("_row_index"),
                "item_ref": q_item,
                "meta": {"quote_unit": q_unit, "bid_unit": b_unit},
            })
            comparisons.append({
                "item": q_item,
                "status": "FAIL",
                "reason": "unit_mismatch",
                "quote_unit": q_unit,
                "bid_unit": b_unit,
            })
            continue

        q_up = to_num(q.get("unit_price"))
        b_up = to_num(b.get("unit_price"))

        if q_up is None or b_up is None:
            findings.append({
                "type": "quote_or_bid_missing_unit_price",
                "severity": "FAIL",
                "message": "Missing unit_price prevents unit-price guardrail enforcement (fail-closed).",
                "pages": [],
                "row_index": q.get("_row_index"),
                "item_ref": q_item,
                "meta": {"quote_unit_price": q.get("unit_price"), "bid_unit_price": b.get("unit_price")},
            })
            comparisons.append({"item": q_item, "status": "FAIL", "reason": "missing_unit_price"})
            continue

        if float(q_up) > float(b_up):
            findings.append({
                "type": "quote_unit_price_above_bid",
                "severity": "FAIL",
                "message": "Quote unit_price exceeds bid unit_price (guardrail).",
                "pages": [],
                "row_index": q.get("_row_index"),
                "item_ref": q_item,
                "meta": {"quote_unit_price": float(q_up), "bid_unit_price": float(b_up)},
            })
            comparisons.append({
                "item": q_item,
                "status": "FAIL",
                "reason": "quote_unit_price_above_bid",
                "quote_unit_price": float(q_up),
                "bid_unit_price": float(b_up),
            })
        else:
            comparisons.append({
                "item": q_item,
                "status": "PASS",
                "quote_unit_price": float(q_up),
                "bid_unit_price": float(b_up),
            })

        # mapped_bid_subtotal = sum of bid extended amounts (or qty*unit_price) for matched items only
        b_total = to_num(b.get("total"))
        if b_total is None:
            b_qty = to_num(b.get("qty"))
            if b_qty is not None:
                b_total = float(b_qty) * float(b_up)
        if b_total is not None:
            mapped_bid_subtotal += float(b_total)

    tolerance = 0.0  # default none
    totals_mismatch = abs(mapped_bid_subtotal - quote_subtotal) > tolerance

    # totals mismatch is WARN only (cross-check)
    if totals_mismatch:
        findings.append({
            "type": "quote_totals_mismatch",
            "severity": "WARN",
            "message": "Totals cross-check mismatch between mapped bid subtotal and quote subtotal.",
            "pages": [],
            "row_index": None,
            "item_ref": None,
            "meta": {
                "mapped_bid_subtotal": round(mapped_bid_subtotal, 2),
                "quote_subtotal": round(quote_subtotal, 2),
                "tolerance": tolerance,
            },
        })

    summary = {
        "mapped_bid_subtotal": round(mapped_bid_subtotal, 2),
        "quote_subtotal": round(quote_subtotal, 2),
        "unmatched_quote_lines_count": len(unmatched),
        "unmatched_quote_lines": unmatched[:200],  # keep response bounded
        "ambiguous_quote_lines_count": len(ambiguous),
        "ambiguous_quote_lines": ambiguous,
        "comparisons": comparisons[:200],  # bounded
        "totals_mismatch": totals_mismatch,
        "tolerance": tolerance,
        "mapping_fields": {
            "match": "item (normalized exact match; leading zeros stripped)",
            "unit": "exact match",
        },
    }

    return findings, summary