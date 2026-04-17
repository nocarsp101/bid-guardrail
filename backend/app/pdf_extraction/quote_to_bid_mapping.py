# backend/app/pdf_extraction/quote_to_bid_mapping.py
"""
C13 — Controlled quote-to-bid mapping foundation.

Maps governed normalized quote rows (C12 contract) to DOT bid items
(C8 lane output) using EXPLICIT DETERMINISTIC RULES ONLY.

Hard rules:
    - Mapping consumes only normalized accepted_rows (C12 contract).
    - Mapping target is the C8 DOT bid-item structure.
    - No fuzzy matching. No semantic guessing. No vector similarity.
      No "best guess" matches. No partial substring matches.
    - One-to-many candidate sets fail closed as "ambiguous" — never
      auto-resolved by ordering, position, or any tiebreaker.
    - Zero candidate sets are returned as "unmapped" with an explicit
      reason. They are never coerced into success.
    - Mapping never mutates accepted quote rows or DOT rows.

Deterministic rules (applied in order; first deterministic outcome wins):

    Rule R1 — line_ref → DOT line_number exact match
        quote_row.line_ref (string-equal, after stripping leading zeros
        if both parties use 4-digit padding) == bid_row.line_number
        Both must be present. String-typed comparison only.

    Rule R2 — description normalized exact match
        Whitespace-collapsed, case-folded, trimmed exact equality.
        quote_row.description.upper().split() == bid_row.description.upper().split()
        This is canonicalization (deterministic), not fuzzy matching.

If neither rule produces exactly one candidate, the row is returned
as unmapped (no candidates) or ambiguous (>1 candidates).

This module contains NO extraction or parser logic. It only consumes
already-governed data structures.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

# Outcome codes (exposed in mapping_outcome).
OUTCOME_MAPPED = "mapped"
OUTCOME_UNMAPPED = "unmapped"
OUTCOME_AMBIGUOUS = "ambiguous"

# Reason codes (exposed in mapping_reason).
REASON_LINE_REF_EXACT = "line_ref_exact_match"
REASON_DESCRIPTION_EXACT = "description_normalized_exact_match"
REASON_NO_CANDIDATES = "no_deterministic_candidates"
REASON_LINE_REF_AMBIGUOUS = "line_ref_multiple_candidates"
REASON_DESCRIPTION_AMBIGUOUS = "description_multiple_candidates"
REASON_NO_LINE_REF_PRESENT = "quote_row_has_no_line_ref"

# Mapping status codes (exposed in mapping_status at the document level).
STATUS_SUCCESS = "success"
STATUS_PARTIAL = "partial"
STATUS_FAILED = "mapping_failed"


def map_quote_to_bid(
    accepted_rows: List[Dict[str, Any]],
    bid_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Run the deterministic mapping foundation.

    Args:
        accepted_rows: list of C12 normalized accepted quote rows
        bid_rows:      list of C8 DOT bid item rows
                       (must contain "line_number", "item", "description")

    Returns the C13 result dict shaped per the spec:
        {
            "mapping_status": str,
            "mapping_results": [...],
            "mapping_diagnostics": {
                "mapped_count": int,
                "unmapped_count": int,
                "ambiguous_count": int,
                "rows_input": int,
                "bid_items_indexed": int,
            }
        }

    NEVER mutates the inputs.
    """
    # Build deterministic indices for the bid items.
    line_number_index: Dict[str, List[Dict[str, Any]]] = {}
    description_index: Dict[str, List[Dict[str, Any]]] = {}

    for bid in bid_rows:
        ln = bid.get("line_number")
        if ln is not None:
            key = _canonical_line_ref(str(ln))
            line_number_index.setdefault(key, []).append(bid)
        desc = bid.get("description")
        if desc:
            key = _canonical_description(desc)
            description_index.setdefault(key, []).append(bid)

    results: List[Dict[str, Any]] = []
    mapped = 0
    unmapped = 0
    ambiguous = 0

    for row in accepted_rows:
        outcome, reason, target, trace = _map_single_row(
            row, line_number_index, description_index
        )
        if outcome == OUTCOME_MAPPED:
            mapped += 1
        elif outcome == OUTCOME_AMBIGUOUS:
            ambiguous += 1
        else:
            unmapped += 1

        results.append({
            "normalized_row_id": row.get("normalized_row_id"),
            "quote_description": row.get("description"),
            "quote_line_ref": row.get("line_ref"),
            "mapping_outcome": outcome,
            "mapping_reason": reason,
            "mapped_bid_item": target,
            "mapping_trace": trace,
        })

    if mapped == len(accepted_rows) and accepted_rows:
        status = STATUS_SUCCESS
    elif mapped > 0:
        status = STATUS_PARTIAL
    else:
        status = STATUS_FAILED

    return {
        "mapping_status": status,
        "mapping_results": results,
        "mapping_diagnostics": {
            "mapped_count": mapped,
            "unmapped_count": unmapped,
            "ambiguous_count": ambiguous,
            "rows_input": len(accepted_rows),
            "bid_items_indexed": len(bid_rows),
        },
    }


def _map_single_row(
    row: Dict[str, Any],
    line_number_index: Dict[str, List[Dict[str, Any]]],
    description_index: Dict[str, List[Dict[str, Any]]],
) -> Tuple[str, str, Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    Apply deterministic rules to a single accepted quote row.

    Returns (outcome, reason, mapped_bid_item_or_none, trace_dict).
    """
    trace: Dict[str, Any] = {
        "rules_attempted": [],
    }

    # Rule R1: line_ref exact (canonicalized).
    line_ref = row.get("line_ref")
    if line_ref:
        canonical_lr = _canonical_line_ref(str(line_ref))
        trace["rules_attempted"].append({
            "rule": "R1_line_ref_exact",
            "canonical_key": canonical_lr,
        })
        candidates = line_number_index.get(canonical_lr, [])
        trace["rules_attempted"][-1]["candidate_count"] = len(candidates)
        if len(candidates) == 1:
            return (
                OUTCOME_MAPPED,
                REASON_LINE_REF_EXACT,
                _project_bid_target(candidates[0]),
                trace,
            )
        if len(candidates) > 1:
            trace["rules_attempted"][-1]["candidates"] = [
                _project_bid_target(c) for c in candidates
            ]
            return (
                OUTCOME_AMBIGUOUS,
                REASON_LINE_REF_AMBIGUOUS,
                None,
                trace,
            )
    else:
        trace["rules_attempted"].append({
            "rule": "R1_line_ref_exact",
            "skipped": REASON_NO_LINE_REF_PRESENT,
        })

    # Rule R2: description normalized exact.
    desc = row.get("description")
    if desc:
        canonical_desc = _canonical_description(desc)
        trace["rules_attempted"].append({
            "rule": "R2_description_normalized_exact",
            "canonical_key": canonical_desc,
        })
        candidates = description_index.get(canonical_desc, [])
        trace["rules_attempted"][-1]["candidate_count"] = len(candidates)
        if len(candidates) == 1:
            return (
                OUTCOME_MAPPED,
                REASON_DESCRIPTION_EXACT,
                _project_bid_target(candidates[0]),
                trace,
            )
        if len(candidates) > 1:
            trace["rules_attempted"][-1]["candidates"] = [
                _project_bid_target(c) for c in candidates
            ]
            return (
                OUTCOME_AMBIGUOUS,
                REASON_DESCRIPTION_AMBIGUOUS,
                None,
                trace,
            )

    # No rule produced a deterministic candidate.
    return OUTCOME_UNMAPPED, REASON_NO_CANDIDATES, None, trace


def _canonical_line_ref(raw: str) -> str:
    """
    Canonicalize a line reference for deterministic equality.

    Strip whitespace, then strip leading zeros (so "0520" == "520").
    Both halves of the comparison go through the same function — this
    is canonicalization, NOT fuzzy matching.
    """
    s = (raw or "").strip()
    s = s.lstrip("0") or "0"
    return s


def _canonical_description(raw: str) -> str:
    """
    Canonicalize a description for deterministic equality.

    Whitespace-collapse + case-fold + trim. Deterministic and reversible
    in intent — two descriptions that canonicalize to the same string are
    considered an exact match. NOT fuzzy similarity.
    """
    return " ".join((raw or "").upper().split())


def _project_bid_target(bid: Dict[str, Any]) -> Dict[str, Any]:
    """Project a DOT bid item into the mapping target shape (mutation-free)."""
    return {
        "line_number": bid.get("line_number"),
        "item_number": bid.get("item"),
        "description": bid.get("description"),
        "unit": bid.get("unit"),
        "qty": bid.get("qty"),
    }
