"""
C51 — Clarification draft generator.

Deterministic structured clarification prompts/questions from scope gaps,
unresolved evidence, missing commercial fields, and risk factors.
Every clarification points back to a specific evidence ref or signal.
No freeform AI guessing.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

CLARIFICATION_VERSION = "clarification_generator/v1"

CLAR_SCOPE_GAP = "scope_gap_clarification"
CLAR_MISSING_QTY = "missing_quantity_clarification"
CLAR_MISSING_UNIT = "missing_unit_clarification"
CLAR_UNIT_CONFLICT = "unit_conflict_clarification"
CLAR_QTY_CONFLICT = "quantity_conflict_clarification"
CLAR_SOURCE_CONFLICT = "source_conflict_clarification"
CLAR_UNRESOLVED_EVIDENCE = "unresolved_evidence_clarification"
CLAR_UNMAPPED_ROW = "unmapped_row_clarification"
CLAR_AMBIGUOUS_SCOPE = "ambiguous_scope_clarification"

_TEMPLATES: Dict[str, str] = {
    CLAR_SCOPE_GAP: "Bid item {ref} ({description}) is not addressed in the subcontractor quote. Please confirm whether this scope is included, excluded, or priced elsewhere.",
    CLAR_MISSING_QTY: "Quote row {ref} ({description}) does not include an explicit quantity. Please provide the quantity and unit of measure.",
    CLAR_MISSING_UNIT: "Quote row {ref} ({description}) does not include a unit of measure. Please confirm the unit.",
    CLAR_UNIT_CONFLICT: "Quote row {ref} ({description}) shows unit '{quote_unit}' but the bid schedule shows '{bid_unit}'. Please confirm the correct unit.",
    CLAR_QTY_CONFLICT: "Quote row {ref} ({description}) shows quantity {quote_qty} but the bid schedule shows {bid_qty}. Please confirm the correct quantity.",
    CLAR_SOURCE_CONFLICT: "Row {ref} ({description}) has conflicting quantity sources. Please confirm the intended quantity and unit.",
    CLAR_UNRESOLVED_EVIDENCE: "Evidence block on page {page} could not be machine-read. Please provide a typed version of the quoted values.",
    CLAR_UNMAPPED_ROW: "Quote row {ref} ({description}) could not be matched to any bid schedule item. Please confirm which bid item this corresponds to.",
    CLAR_AMBIGUOUS_SCOPE: "Quote row {ref} ({description}) has a description but no monetary values. Please confirm whether this item is priced.",
}


def generate_clarifications(
    scope_interpretation: Optional[Dict[str, Any]] = None,
    resolution_output: Optional[Dict[str, Any]] = None,
    risk_output: Optional[Dict[str, Any]] = None,
    handwritten_review: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    ordinal = 0

    ordinal = _from_scope(items, ordinal, scope_interpretation)
    ordinal = _from_resolution(items, ordinal, resolution_output)
    ordinal = _from_evidence(items, ordinal, handwritten_review)

    summary = _build_summary(items)
    return {
        "clarification_version": CLARIFICATION_VERSION,
        "clarification_items": items,
        "clarification_summary": summary,
    }


def _from_scope(items: List, ordinal: int, si: Optional[Dict[str, Any]]) -> int:
    if not si:
        return ordinal
    for topic in si.get("scope_topics") or []:
        sc = topic.get("scope_class")
        desc = topic.get("description") or ""
        ref = (topic.get("source_ref") or {}).get("normalized_row_id") or \
              (topic.get("source_ref") or {}).get("line_number") or f"topic-{ordinal}"

        if sc == "not_addressed":
            items.append(_item(ordinal, CLAR_SCOPE_GAP, ref=ref, description=desc,
                               evidence_refs=topic.get("evidence_refs")))
            ordinal += 1
        elif sc == "ambiguous_scope":
            items.append(_item(ordinal, CLAR_AMBIGUOUS_SCOPE, ref=ref, description=desc,
                               evidence_refs=topic.get("evidence_refs")))
            ordinal += 1
    return ordinal


def _from_resolution(items: List, ordinal: int, res: Optional[Dict[str, Any]]) -> int:
    if not res:
        return ordinal
    for row in res.get("resolution_rows") or []:
        cat = row.get("resolution_category") or ""
        rid = row.get("normalized_row_id") or f"row-{ordinal}"
        desc = (row.get("quote_values") or {}).get("description") or ""
        qv = row.get("quote_values") or {}
        ext = row.get("external_sources") or []
        basis = row.get("comparison_basis") or {}
        eff = basis.get("effective_comparison_values") or {}

        if cat == "non_comparable_missing_quote_source":
            if qv.get("qty") is None and qv.get("unit") is None:
                items.append(_item(ordinal, CLAR_MISSING_QTY, ref=rid, description=desc))
                ordinal += 1
            elif qv.get("unit") is None:
                items.append(_item(ordinal, CLAR_MISSING_UNIT, ref=rid, description=desc))
                ordinal += 1
        elif cat == "unmapped_scope_review_required":
            items.append(_item(ordinal, CLAR_UNMAPPED_ROW, ref=rid, description=desc))
            ordinal += 1
        elif cat == "unit_discrepancy_review_required":
            bid_unit = eff.get("unit") or ""
            items.append(_item(ordinal, CLAR_UNIT_CONFLICT, ref=rid, description=desc,
                               quote_unit=qv.get("unit"), bid_unit=bid_unit))
            ordinal += 1
        elif cat == "quantity_discrepancy_review_required":
            bid_qty = eff.get("qty")
            items.append(_item(ordinal, CLAR_QTY_CONFLICT, ref=rid, description=desc,
                               quote_qty=qv.get("qty"), bid_qty=bid_qty))
            ordinal += 1
        elif cat == "source_conflict_review_required":
            items.append(_item(ordinal, CLAR_SOURCE_CONFLICT, ref=rid, description=desc))
            ordinal += 1
    return ordinal


def _from_evidence(items: List, ordinal: int, hr: Optional[Dict[str, Any]]) -> int:
    if not hr:
        return ordinal
    for b in hr.get("block_index") or []:
        if b.get("machine_readability") in ("partial", "unreadable") and not b.get("has_manual_entry"):
            items.append(_item(ordinal, CLAR_UNRESOLVED_EVIDENCE,
                               page=b.get("source_page"),
                               evidence_refs=[{"type": "block_id", "value": b.get("block_id")}]))
            ordinal += 1
    return ordinal


def _item(ordinal: int, clar_type: str, **kw) -> Dict[str, Any]:
    ref = kw.get("ref", "")
    desc = kw.get("description", "")
    template = _TEMPLATES.get(clar_type, "")
    text = template.format(
        ref=ref, description=desc,
        quote_unit=kw.get("quote_unit", ""),
        bid_unit=kw.get("bid_unit", ""),
        quote_qty=kw.get("quote_qty", ""),
        bid_qty=kw.get("bid_qty", ""),
        page=kw.get("page", ""),
    )
    return {
        "clarification_id": f"clar-{ordinal}",
        "clarification_type": clar_type,
        "clarification_text": text,
        "source_ref": ref,
        "evidence_refs": kw.get("evidence_refs") or [],
    }


def _build_summary(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    type_counts: Dict[str, int] = {}
    for i in items:
        t = i["clarification_type"]
        type_counts[t] = type_counts.get(t, 0) + 1
    return {
        "total_clarifications": len(items),
        "type_counts": dict(sorted(type_counts.items())),
    }
