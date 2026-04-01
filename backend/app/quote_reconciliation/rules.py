from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional

from app.audit.models import Finding
from app.utils.unit_canonicalization import canonicalize_unit

# Quantity mismatch tolerance — avoids floating-point noise, does NOT hide real differences.
_QTY_TOLERANCE = 0.0001


def reconcile_quote_lines_against_bid(
    bid_rows: List[Dict[str, Any]],
    quote_rows: List[Dict[str, Any]],
) -> Tuple[List[Finding], Dict[str, Any]]:
    """
    Deterministic reconciliation (fail-closed):
      - Primary match key: quote.item (line number) -> bid.item
      - Secondary match key: quote.pay_item -> bid.item (optional fallback, still deterministic)
      - Unmatched quote line -> FAIL
      - Ambiguous mapping -> FAIL
      - Unit mismatch -> FAIL
      - Quote unit_price > bid unit_price -> FAIL
      - Quote line missing unit_price -> FAIL
      - Totals cross-check -> WARN (default)
    """
    findings: List[Finding] = []

    bid_index = _build_bid_index(bid_rows)

    mapped_bid_subtotal = 0.0
    quote_subtotal = 0.0
    unmatched: List[Dict[str, Any]] = []
    ambiguous: List[Dict[str, Any]] = []
    comparisons: List[Dict[str, Any]] = []

    matched_lines_count = 0
    unit_mismatch_count = 0

    for q in quote_rows:
        q_idx = int(q.get("_row_index", -1))

        q_item_raw = _s(q.get("item"))
        q_item_key = _item_key(q_item_raw)  # strip leading zeros for numeric

        q_pay_item_raw = _s(q.get("pay_item"))
        q_pay_item_key = _item_key(q_pay_item_raw)

        q_unit_raw = _s(q.get("unit"))
        q_unit_key = canonicalize_unit(q_unit_raw)

        q_qty = _to_num(q.get("qty")) or 0.0
        q_up = _to_num(q.get("unit_price"))

        # quote subtotal uses qty*unit_price; if missing -> FAIL closed
        if q_up is None:
            findings.append(Finding(
                type="quote_line_missing_unit_price",
                severity="FAIL",
                message="Quote line is missing unit price (fail-closed).",
                row_index=q_idx,
                item_ref=q_item_raw or f"quote_row_{q_idx}",
            ))
            unmatched.append({"row_index": q_idx, "item": q_item_raw, "reason": "missing_unit_price"})
            continue

        quote_subtotal += (q_qty * q_up)

        # Need some deterministic identifier to match
        if not q_item_key and not q_pay_item_key:
            findings.append(Finding(
                type="quote_line_missing_item_identifier",
                severity="FAIL",
                message="Quote line missing item identifier required for deterministic mapping (fail-closed).",
                row_index=q_idx,
                item_ref=f"quote_row_{q_idx}",
            ))
            unmatched.append({"row_index": q_idx, "item": q_item_raw, "reason": "missing_item"})
            continue

        # Match attempts (deterministic): try line-number item first, then pay_item fallback
        match_method = None
        candidates: List[Dict[str, Any]] = []
        if q_item_key:
            candidates = bid_index.get(q_item_key, [])
            if candidates:
                match_method = "item"
        if (not candidates) and q_pay_item_key:
            candidates = bid_index.get(q_pay_item_key, [])
            if candidates:
                match_method = "pay_item"

        if not candidates:
            findings.append(Finding(
                type="quote_line_unmatched",
                severity="FAIL",
                message="No deterministic match found for quote line item in bid items (fail-closed).",
                row_index=q_idx,
                item_ref=q_item_raw or q_pay_item_raw or f"quote_row_{q_idx}",
                meta={"match_key": q_item_key or q_pay_item_key, "match_method": match_method},
            ))
            unmatched.append({"row_index": q_idx, "item": q_item_raw or q_pay_item_raw, "reason": "no_match"})
            continue

        if len(candidates) > 1:
            findings.append(Finding(
                type="quote_line_ambiguous_match",
                severity="FAIL",
                message="Ambiguous match: multiple bid items share the same item identifier (fail-closed).",
                row_index=q_idx,
                item_ref=q_item_raw or q_pay_item_raw or f"quote_row_{q_idx}",
                meta={
                    "match_key": q_item_key or q_pay_item_key,
                    "match_method": match_method,
                    "candidate_rows": [c.get("_row_index") for c in candidates],
                },
            ))
            ambiguous.append({
                "row_index": q_idx,
                "item": q_item_raw or q_pay_item_raw,
                "candidates": [c.get("_row_index") for c in candidates],
            })
            continue

        # Single deterministic match
        b = candidates[0]
        b_idx = int(b.get("_row_index", -1))
        b_item_raw = _s(b.get("item"))
        b_unit_raw = _s(b.get("unit"))
        b_unit_key = canonicalize_unit(b_unit_raw)
        b_qty = _to_num(b.get("qty")) or 0.0
        b_up = _to_num(b.get("unit_price"))
        b_total = _to_num(b.get("total"))

        if b_up is None:
            findings.append(Finding(
                type="bid_item_missing_unit_price_for_quote_match",
                severity="FAIL",
                message="Bid item matched to quote is missing unit price (fail-closed).",
                row_index=b_idx,
                item_ref=b_item_raw,
            ))
            continue

        # IMPORTANT: record match + subtotal EVEN IF unit mismatch, so reporting is useful
        matched_lines_count += 1
        mapped_bid_subtotal += (b_total if b_total is not None else (b_qty * b_up))

        # Add comparison row for debugging/reporting
        comparisons.append({
            "quote_row_index": q_idx,
            "bid_row_index": b_idx,
            "match_method": match_method,
            "quote_item": q_item_raw,
            "quote_pay_item": q_pay_item_raw,
            "match_key_used": (q_item_key if match_method == "item" else q_pay_item_key),
            "quote_unit": q_unit_raw,
            "bid_unit": b_unit_raw,
            "quote_qty": q_qty,
            "quote_unit_price": q_up,
            "bid_qty": b_qty,
            "bid_unit_price": b_up,
        })

        # Unit mismatch -> FAIL (no conversions)
        if (q_unit_key or "") != (b_unit_key or ""):
            unit_mismatch_count += 1
            findings.append(Finding(
                type="quote_bid_unit_mismatch",
                severity="FAIL",
                message="Unit mismatch between quote line and bid item (no conversions; fail-closed).",
                row_index=q_idx,
                item_ref=_s(q.get("item")) or _s(q.get("pay_item")) or f"quote_row_{q_idx}",
                meta={"quote_unit": q_unit_raw, "bid_unit": b_unit_raw, "bid_row_index": b_idx},
            ))
            # continue; still allow other checks? For MVP we stop here after unit mismatch.
            continue

        # Primary unit price guardrail
        if q_up > b_up:
            findings.append(Finding(
                type="quote_unit_price_above_bid_unit_price",
                severity="FAIL",
                message="Quote unit price is above bid unit price (hard FAIL).",
                row_index=q_idx,
                item_ref=_s(q.get("item")) or _s(q.get("pay_item")) or f"quote_row_{q_idx}",
                meta={"quote_unit_price": q_up, "bid_unit_price": b_up, "bid_row_index": b_idx},
            ))

        # Quantity mismatch detection (WARN — informational, not fatal)
        if abs(q_qty - b_qty) > _QTY_TOLERANCE:
            delta = round(q_qty - b_qty, 6)
            pct = round((delta / b_qty) * 100, 2) if b_qty != 0 else None
            _item_ref = _s(q.get("item")) or _s(q.get("pay_item")) or f"quote_row_{q_idx}"
            findings.append(Finding(
                type="quote_bid_quantity_mismatch",
                severity="WARN",
                message=f"Quote quantity ({q_qty}) differs from bid quantity ({b_qty}).",
                row_index=q_idx,
                item_ref=_item_ref,
                meta={
                    "quote_qty": q_qty,
                    "bid_qty": b_qty,
                    "delta": delta,
                    "percent_diff": pct,
                    "bid_row_index": b_idx,
                },
            ))

    # Totals cross-check (WARN by default)
    totals_mismatch = (round(mapped_bid_subtotal, 2) != round(quote_subtotal, 2))
    if totals_mismatch:
        findings.append(Finding(
            type="quote_totals_mismatch_crosscheck",
            severity="WARN",
            message="Totals cross-check mismatch (WARN by default).",
            meta={
                "mapped_bid_subtotal": round(mapped_bid_subtotal, 2),
                "quote_subtotal": round(quote_subtotal, 2),
            },
        ))

    summary = {
        "mapped_bid_subtotal": round(mapped_bid_subtotal, 2),
        "quote_subtotal": round(quote_subtotal, 2),
        "unmatched_quote_lines_count": len(unmatched),
        "unmatched_quote_lines": unmatched,
        "ambiguous_quote_lines_count": len(ambiguous),
        "ambiguous_quote_lines": ambiguous,
        "comparisons": comparisons,
        "matched_lines_count": matched_lines_count,
        "unit_mismatch_count": unit_mismatch_count,
        "totals_mismatch": totals_mismatch,
        "tolerance": 0.0,
        "mapping_fields": {
            "primary_match": "item (strip leading zeros + exact match)",
            "fallback_match": "pay_item (strip leading zeros + exact match)",
            "unit": "exact match (no conversions)",
        },
    }

    if not any(f.type.startswith("quote_") or f.type.startswith("quote_bid_") for f in findings):
        findings.append(Finding(
            type="quote_reconciliation",
            severity="INFO",
            message="Quote reconciliation passed without issues.",
        ))

    return findings, summary


def _build_bid_index(bid_rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    idx: Dict[str, List[Dict[str, Any]]] = {}
    for b in bid_rows:
        key = _item_key(_s(b.get("item")))
        if not key:
            continue
        idx.setdefault(key, []).append(b)
    return idx


def _s(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _key(s: str) -> str:
    s = _s(s)
    if not s:
        return ""
    return " ".join(s.split()).upper()


def _item_key(s: str) -> str:
    s = _key(s)
    if not s:
        return ""
    if s.isdigit():
        s2 = s.lstrip("0")
        return s2 if s2 != "" else "0"
    return s


def _to_num(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None