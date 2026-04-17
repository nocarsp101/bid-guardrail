"""
C48 — Subcontractor scope interpretation layer.

Deterministic scope classification from extracted/hybrid/manual rows.
Every scope topic carries supporting evidence refs and source provenance.
Ambiguity is always surfaced, never resolved.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

SCOPE_INTERPRETATION_VERSION = "scope_interpretation/v1"

SCOPE_EXPLICITLY_INCLUDED = "explicitly_included"
SCOPE_IMPLICITLY_INCLUDED = "implicitly_included"
SCOPE_EXPLICITLY_EXCLUDED = "explicitly_excluded"
SCOPE_NOT_ADDRESSED = "not_addressed"
SCOPE_AMBIGUOUS = "ambiguous_scope"

_ALL_SCOPE_CLASSES = (
    SCOPE_EXPLICITLY_INCLUDED,
    SCOPE_IMPLICITLY_INCLUDED,
    SCOPE_EXPLICITLY_EXCLUDED,
    SCOPE_NOT_ADDRESSED,
    SCOPE_AMBIGUOUS,
)


def build_scope_interpretation(
    effective_rows: List[Dict[str, Any]],
    bid_rows: Optional[List[Dict[str, Any]]] = None,
    mapping_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Classify scope topics from effective quote rows.

    Every quote row becomes a scope topic. Bid rows that have no quote
    counterpart become `not_addressed` topics.
    """
    bid_rows = bid_rows or []
    mapping_results = (mapping_result or {}).get("mapping_results") or []

    mapped_bid_ids = set()
    for mr in mapping_results:
        bid_item = mr.get("mapped_bid_item") or {}
        bid_key = bid_item.get("line_number") or bid_item.get("item_number")
        if bid_key and mr.get("mapping_outcome") == "mapped":
            mapped_bid_ids.add(bid_key)

    topics: List[Dict[str, Any]] = []
    ordinal = 0

    for row in effective_rows:
        topic = _classify_quote_row(row, ordinal)
        topics.append(topic)
        ordinal += 1

    for bid_row in bid_rows:
        bid_key = bid_row.get("line_number") or bid_row.get("item_number")
        if bid_key and bid_key not in mapped_bid_ids:
            topics.append({
                "topic_id": f"scope-{ordinal}",
                "scope_class": SCOPE_NOT_ADDRESSED,
                "description": bid_row.get("description"),
                "source_type": "dot_bid_item",
                "source_ref": {
                    "line_number": bid_row.get("line_number"),
                    "item_number": bid_row.get("item_number"),
                },
                "evidence_refs": [],
                "row_origin": "dot_bid_unmatched",
                "classification_reason": "bid_item_has_no_quote_counterpart",
            })
            ordinal += 1

    summary = _build_scope_summary(topics)

    return {
        "scope_interpretation_version": SCOPE_INTERPRETATION_VERSION,
        "scope_topics": topics,
        "scope_summary": summary,
    }


def _classify_quote_row(row: Dict[str, Any], ordinal: int) -> Dict[str, Any]:
    """Classify a single effective quote row into a scope topic."""
    description = row.get("description") or ""
    row_origin = row.get("row_origin") or row.get("extraction_source") or "unknown"
    amount = row.get("amount")
    qty = row.get("qty")
    unit = row.get("unit")
    unit_price = row.get("unit_price")
    line_ref = row.get("line_ref")
    manual_ref = row.get("manual_entry_ref")
    block_ref = row.get("source_block_ref")

    evidence_refs: List[Dict[str, Any]] = []
    if line_ref:
        evidence_refs.append({"type": "line_ref", "value": line_ref})
    if manual_ref:
        evidence_refs.append({"type": "manual_entry_ref", "value": deepcopy(manual_ref)})
    if block_ref:
        evidence_refs.append({"type": "source_block_ref", "value": deepcopy(block_ref)})
    if amount is not None:
        evidence_refs.append({"type": "monetary_amount", "value": amount})

    scope_class, reason = _determine_scope_class(
        description=description,
        amount=amount,
        qty=qty,
        unit=unit,
        unit_price=unit_price,
        row_origin=row_origin,
    )

    return {
        "topic_id": f"scope-{ordinal}",
        "scope_class": scope_class,
        "description": description,
        "source_type": "quote_row",
        "source_ref": {
            "normalized_row_id": row.get("normalized_row_id"),
            "source_page": row.get("source_page"),
        },
        "evidence_refs": evidence_refs,
        "row_origin": row_origin,
        "classification_reason": reason,
        "values": {
            "qty": qty,
            "unit": unit,
            "unit_price": unit_price,
            "amount": amount,
        },
    }


def _determine_scope_class(
    description: str,
    amount: Any,
    qty: Any,
    unit: Any,
    unit_price: Any,
    row_origin: str,
) -> tuple:
    """Deterministic scope classification.

    Rules (closed, in order):
    1. If both amount AND (qty or unit_price) present → explicitly_included
    2. If amount present but no qty/unit/unit_price → implicitly_included
       (a lump-sum-like item with a price but no measurable quantity)
    3. If manual_interpretation origin AND amount present → explicitly_included
       (human entered the value)
    4. If description present but no monetary values → ambiguous_scope
    5. Otherwise → not_addressed
    """
    has_amount = amount is not None
    has_qty = qty is not None
    has_unit_price = unit_price is not None
    has_unit = unit is not None
    has_description = bool(description and description.strip())

    if has_amount and (has_qty or has_unit_price):
        return SCOPE_EXPLICITLY_INCLUDED, "amount_plus_qty_or_unit_price_present"

    if has_amount and not has_qty and not has_unit_price:
        if row_origin == "manual_interpretation":
            return SCOPE_EXPLICITLY_INCLUDED, "manual_interpretation_with_amount"
        return SCOPE_IMPLICITLY_INCLUDED, "amount_only_no_measured_quantity"

    if has_description and not has_amount:
        return SCOPE_AMBIGUOUS, "description_present_but_no_monetary_values"

    return SCOPE_NOT_ADDRESSED, "insufficient_evidence_for_classification"


def _build_scope_summary(topics: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts = {c: 0 for c in _ALL_SCOPE_CLASSES}
    for t in topics:
        sc = t.get("scope_class")
        if sc in counts:
            counts[sc] += 1

    return {
        "total_topics": len(topics),
        "scope_class_counts": counts,
        "explicitly_included_count": counts[SCOPE_EXPLICITLY_INCLUDED],
        "implicitly_included_count": counts[SCOPE_IMPLICITLY_INCLUDED],
        "explicitly_excluded_count": counts[SCOPE_EXPLICITLY_EXCLUDED],
        "not_addressed_count": counts[SCOPE_NOT_ADDRESSED],
        "ambiguous_count": counts[SCOPE_AMBIGUOUS],
    }
