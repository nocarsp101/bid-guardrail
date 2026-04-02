# backend/app/operator_report.py
"""
Operator report transform — pure function, no side effects.

Takes raw /validate output and produces a structured report that
a normal internal operator can read without parsing nested JSON.

Sections: run_summary, mapping_provenance, counts, key_findings, next_action.
Full raw response preserved in 'detail'.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def build_operator_report(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a raw /validate response dict into an operator-readable report.
    """
    overall_status = raw.get("overall_status", "UNKNOWN")
    findings = raw.get("findings", [])
    bid_summary = raw.get("bid_summary") or {}
    quote_summary = raw.get("quote_summary") or {}
    has_quote = bool(raw.get("quote_summary"))

    bid_ingestion = bid_summary.get("ingestion", {})
    quote_ingestion = quote_summary.get("ingestion", {}) if has_quote else {}

    # --- categorise findings ---
    unmatched = [f for f in findings if f.get("type") == "quote_line_unmatched"]
    price_violations = [f for f in findings if f.get("type") == "quote_unit_price_above_bid_unit_price"]
    unit_mismatches = [f for f in findings if f.get("type") == "quote_bid_unit_mismatch"]
    qty_mismatches = [f for f in findings if f.get("type") == "quote_bid_quantity_mismatch"]
    missing_up = [f for f in findings if f.get("type") == "quote_line_missing_unit_price"]

    fail_count = sum(1 for f in findings if f.get("severity") == "FAIL")
    warn_count = sum(1 for f in findings if f.get("severity") == "WARN")

    mapping_applied = quote_ingestion.get("line_mapping_applied", False)
    mapping_source = quote_ingestion.get("mapping_source")
    mapping_name_used = quote_ingestion.get("mapping_name_used")

    return {
        "run_summary": _run_summary(raw, overall_status, fail_count, warn_count,
                                     unmatched, price_violations, unit_mismatches,
                                     qty_mismatches, missing_up),
        "mapping_provenance": _mapping_provenance(has_quote, mapping_applied,
                                                   mapping_source, mapping_name_used),
        "counts": _counts(bid_ingestion, quote_summary, has_quote,
                          price_violations, unit_mismatches, qty_mismatches),
        "key_findings": _key_findings(unmatched, price_violations, unit_mismatches,
                                       qty_mismatches, missing_up),
        "next_action": _next_action(overall_status, unmatched, price_violations,
                                     unit_mismatches, missing_up,
                                     mapping_applied, quote_summary, has_quote),
        "detail": raw,
    }


# ---------------------------------------------------------------------------
# run_summary
# ---------------------------------------------------------------------------

def _run_summary(
    raw: dict,
    overall_status: str,
    fail_count: int,
    warn_count: int,
    unmatched: list,
    price_violations: list,
    unit_mismatches: list,
    qty_mismatches: list,
    missing_up: list,
) -> Dict[str, Any]:
    return {
        "run_id": raw.get("run_id"),
        "doc_type": raw.get("doc_type"),
        "overall_status": overall_status,
        "status_description": _status_description(
            overall_status, unmatched, price_violations,
            unit_mismatches, qty_mismatches, missing_up,
        ),
        "total_findings": len(raw.get("findings", [])),
        "fail_count": fail_count,
        "warn_count": warn_count,
    }


def _status_description(
    status: str,
    unmatched: list,
    price_violations: list,
    unit_mismatches: list,
    qty_mismatches: list,
    missing_up: list,
) -> str:
    if status == "PASS":
        return "All checks passed. Quote reconciliation completed without issues."

    parts: List[str] = []
    if unmatched:
        parts.append(f"{len(unmatched)} quote line(s) unmatched")
    if price_violations:
        parts.append(f"{len(price_violations)} price violation(s)")
    if unit_mismatches:
        parts.append(f"{len(unit_mismatches)} unit mismatch(es)")
    if missing_up:
        parts.append(f"{len(missing_up)} missing unit price(s)")
    if qty_mismatches:
        parts.append(f"{len(qty_mismatches)} quantity difference(s)")

    detail = "; ".join(parts) if parts else "see findings"

    if status == "FAIL":
        return f"Validation failed: {detail}."
    if status == "WARN":
        return f"Passed with warnings: {detail}."
    return f"Status {status}: {detail}."


# ---------------------------------------------------------------------------
# mapping_provenance
# ---------------------------------------------------------------------------

def _mapping_provenance(
    has_quote: bool,
    mapping_applied: bool,
    mapping_source: Optional[str],
    mapping_name_used: Optional[str],
) -> Dict[str, Any]:
    prov: Dict[str, Any] = {
        "mapping_applied": mapping_applied,
        "mapping_source": mapping_source,
        "mapping_name_used": mapping_name_used,
    }

    if not has_quote:
        prov["description"] = "No quote reconciliation performed (PRIME_BID mode)."
    elif not mapping_applied or mapping_source is None:
        prov["description"] = (
            "No line-to-item mapping was applied. "
            "Quote line numbers were compared directly against bid item numbers."
        )
    elif mapping_source == "file_upload":
        prov["description"] = "Mapping applied from uploaded JSON file."
    elif mapping_source == "named":
        prov["description"] = f"Mapping loaded from saved mapping '{mapping_name_used}'."
    elif mapping_source == "auto_selected":
        prov["description"] = (
            f"Mapping auto-selected: saved mapping '{mapping_name_used}' "
            f"matched the provided project/vendor context."
        )
    else:
        prov["description"] = f"Mapping applied (source: {mapping_source})."

    return prov


# ---------------------------------------------------------------------------
# counts
# ---------------------------------------------------------------------------

def _counts(
    bid_ingestion: dict,
    quote_summary: dict,
    has_quote: bool,
    price_violations: list,
    unit_mismatches: list,
    qty_mismatches: list,
) -> Dict[str, Any]:
    c: Dict[str, Any] = {
        "bid_items_in_file": bid_ingestion.get("rows_raw_total"),
    }
    if has_quote:
        q_ing = quote_summary.get("ingestion", {})
        c["quote_lines_in_file"] = q_ing.get("rows_raw_total")
        c["matched"] = quote_summary.get("matched_lines_count", 0)
        c["unmatched"] = quote_summary.get("unmatched_quote_lines_count", 0)
        c["price_violations"] = len(price_violations)
        c["unit_mismatches"] = len(unit_mismatches)
        c["quantity_mismatches"] = len(qty_mismatches)
        c["totals_mismatch"] = quote_summary.get("totals_mismatch", False)
    return c


# ---------------------------------------------------------------------------
# key_findings  (grouped by category, operator-readable)
# ---------------------------------------------------------------------------

def _key_findings(
    unmatched: list,
    price_violations: list,
    unit_mismatches: list,
    qty_mismatches: list,
    missing_up: list,
) -> List[Dict[str, Any]]:
    groups: List[Dict[str, Any]] = []

    if unmatched:
        groups.append({
            "category": "unmatched_lines",
            "severity": "FAIL",
            "count": len(unmatched),
            "summary": f"{len(unmatched)} quote line(s) could not be matched to any bid item.",
            "items": [
                f.get("item_ref", f"row_{f.get('row_index', '?')}")
                for f in unmatched
            ],
        })

    if price_violations:
        groups.append({
            "category": "price_violations",
            "severity": "FAIL",
            "count": len(price_violations),
            "summary": f"{len(price_violations)} quote unit price(s) exceed the bid unit price.",
            "items": [
                {
                    "item": f.get("item_ref", "?"),
                    "quote_price": f.get("meta", {}).get("quote_unit_price"),
                    "bid_price": f.get("meta", {}).get("bid_unit_price"),
                }
                for f in price_violations
            ],
        })

    if unit_mismatches:
        groups.append({
            "category": "unit_mismatches",
            "severity": "FAIL",
            "count": len(unit_mismatches),
            "summary": f"{len(unit_mismatches)} unit mismatch(es) between quote and bid.",
            "items": [
                {
                    "item": f.get("item_ref", "?"),
                    "quote_unit": f.get("meta", {}).get("quote_unit"),
                    "bid_unit": f.get("meta", {}).get("bid_unit"),
                }
                for f in unit_mismatches
            ],
        })

    if missing_up:
        groups.append({
            "category": "missing_unit_price",
            "severity": "FAIL",
            "count": len(missing_up),
            "summary": f"{len(missing_up)} quote line(s) missing unit price.",
            "items": [
                f.get("item_ref", f"row_{f.get('row_index', '?')}")
                for f in missing_up
            ],
        })

    if qty_mismatches:
        groups.append({
            "category": "quantity_mismatches",
            "severity": "WARN",
            "count": len(qty_mismatches),
            "summary": f"{len(qty_mismatches)} quantity difference(s) between quote and bid.",
            "items": [
                {
                    "item": f.get("item_ref", "?"),
                    "quote_qty": f.get("meta", {}).get("quote_qty"),
                    "bid_qty": f.get("meta", {}).get("bid_qty"),
                    "delta": f.get("meta", {}).get("delta"),
                }
                for f in qty_mismatches
            ],
        })

    return groups


# ---------------------------------------------------------------------------
# next_action  (single most-important action for the operator)
# ---------------------------------------------------------------------------

def _next_action(
    overall_status: str,
    unmatched: list,
    price_violations: list,
    unit_mismatches: list,
    missing_up: list,
    mapping_applied: bool,
    quote_summary: dict,
    has_quote: bool,
) -> Dict[str, Any]:
    if unmatched and not mapping_applied:
        available = len(quote_summary.get("available_bid_items", []))
        return {
            "action": "create_mapping",
            "description": (
                f"{len(unmatched)} quote line(s) are unmatched because no line-to-item mapping was applied. "
                f"Save a mapping via POST /mapping/save or upload one with line_to_item_mapping. "
                f"{available} bid items available for mapping."
            ),
        }

    if unmatched and mapping_applied:
        return {
            "action": "update_mapping",
            "description": (
                f"{len(unmatched)} quote line(s) still unmatched after mapping. "
                f"The mapping may be incomplete or contain incorrect entries."
            ),
        }

    if price_violations:
        return {
            "action": "review_price_violations",
            "description": (
                f"{len(price_violations)} quote unit price(s) exceed bid unit price. "
                f"Review with the vendor before approval."
            ),
        }

    if unit_mismatches:
        return {
            "action": "review_unit_mismatches",
            "description": (
                f"{len(unit_mismatches)} unit mismatch(es). "
                f"Units must match exactly for comparison."
            ),
        }

    if missing_up:
        return {
            "action": "review_missing_prices",
            "description": f"{len(missing_up)} quote line(s) have no unit price.",
        }

    if overall_status == "WARN":
        return {
            "action": "review_warnings",
            "description": "Validation passed but has warnings. Review before final approval.",
        }

    if overall_status == "PASS":
        return {
            "action": "approved",
            "description": "All checks passed. Quote reconciliation is clean.",
        }

    return {
        "action": "review",
        "description": "Review the detailed findings.",
    }
