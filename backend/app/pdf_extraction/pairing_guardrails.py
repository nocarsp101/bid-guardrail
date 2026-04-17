# backend/app/pdf_extraction/pairing_guardrails.py
"""
C14 — Pairing guardrails + mapping trust controls.

Before any quote-to-bid mapping runs, analyze the two inputs (governed
C12 accepted quote rows + governed C8 DOT bid rows) and produce a
deterministic pairing_diagnostics object with one of three statuses:

    trusted  — strong deterministic evidence the pair belongs together
    weak     — some overlap, but not strong enough to fully trust
    rejected — no deterministic evidence; mapping must not run

This module does NOT use fuzzy matching, semantic similarity, or any
kind of heuristic scoring. Every signal is a count against an explicit
equality rule (canonicalized in the same way both sides are compared).

Signals:
    strict_confirmation_count:
        Number of accepted quote rows where the row's line_ref matches
        a bid row's line_number AND the same bid row's description is a
        canonical exact match of the quote description. Both rules must
        agree on the SAME bid row — this is the strongest deterministic
        alignment signal available without extra metadata.

    line_ref_domain_overlap:
        Number of accepted quote rows with a line_ref that exists
        anywhere in the bid line_number domain.

    description_exact_overlap:
        Number of accepted quote rows whose canonical description
        matches the canonical description of ANY bid row. Captures
        "standard item" commonality even when line_refs don't line up —
        useful only as a fallback when no quote rows carry line_refs.

    rows_with_line_ref_count:
        Accepted rows that have a non-null line_ref (determines which
        rule path applies).

Policy:
    - rejected → mapping must not run. Fail-closed.
    - weak     → mapping runs, but pairing_status is propagated into
                 downstream results so consumers can apply review
                 gates. Warnings list is non-empty.
    - trusted  → mapping runs normally. Warnings list is empty.
"""
from __future__ import annotations

from typing import Any, Dict, List

from .quote_to_bid_mapping import _canonical_description, _canonical_line_ref


STATUS_TRUSTED = "trusted"
STATUS_WEAK = "weak"
STATUS_REJECTED = "rejected"

REASON_EMPTY_QUOTE = "no_accepted_quote_rows"
REASON_EMPTY_BID = "no_bid_items_indexed"
REASON_NO_CROSS_CONFIRMATION = "no_strict_line_ref_plus_description_match"
REASON_INSUFFICIENT_OVERLAP = "insufficient_overlap_for_lineref_less_quote"
REASON_STRICT_CONFIRMED = "strict_line_ref_plus_description_confirmed"
REASON_DESC_ONLY_CONFIRMED = "description_overlap_confirmed_without_line_refs"
REASON_PARTIAL_CONFIRMATION = "partial_confirmation_below_trust_threshold"

# Deterministic thresholds.
MIN_STRICT_FOR_TRUSTED = 3          # count of same-row line_ref + desc matches
MIN_DESC_OVERLAP_FOR_TRUSTED = 5    # fallback when quote has no line_refs
MIN_DESC_OVERLAP_FOR_WEAK = 3       # fallback weak tier


def analyze_pairing(
    accepted_rows: List[Dict[str, Any]],
    bid_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Run deterministic pairing analysis.

    Returns a pairing_diagnostics dict shaped like:
        {
            pairing_status: "trusted" | "weak" | "rejected",
            pairing_reason: str,
            signals: {
                accepted_rows_count, bid_rows_count,
                rows_with_line_ref_count,
                line_ref_domain_overlap,
                description_exact_overlap,
                strict_confirmation_count,
            },
            warnings: [str, ...],
            allow_mapping: bool,
        }
    """
    accepted_count = len(accepted_rows)
    bid_count = len(bid_rows)

    # Short-circuits for empty inputs.
    if accepted_count == 0:
        return _rejected(
            REASON_EMPTY_QUOTE,
            signals={
                "accepted_rows_count": 0,
                "bid_rows_count": bid_count,
                "rows_with_line_ref_count": 0,
                "line_ref_domain_overlap": 0,
                "description_exact_overlap": 0,
                "strict_confirmation_count": 0,
            },
        )
    if bid_count == 0:
        return _rejected(
            REASON_EMPTY_BID,
            signals={
                "accepted_rows_count": accepted_count,
                "bid_rows_count": 0,
                "rows_with_line_ref_count": 0,
                "line_ref_domain_overlap": 0,
                "description_exact_overlap": 0,
                "strict_confirmation_count": 0,
            },
        )

    # Build deterministic indices over the bid side.
    bid_by_lineno: Dict[str, List[Dict[str, Any]]] = {}
    bid_desc_set: set = set()
    for b in bid_rows:
        ln = b.get("line_number")
        if ln is not None:
            bid_by_lineno.setdefault(_canonical_line_ref(str(ln)), []).append(b)
        desc = b.get("description")
        if desc:
            bid_desc_set.add(_canonical_description(desc))

    rows_with_line_ref = 0
    line_ref_domain_overlap = 0
    description_exact_overlap = 0
    strict_confirmation_count = 0

    for row in accepted_rows:
        lr = row.get("line_ref")
        qd_canonical = _canonical_description(row.get("description") or "")

        if qd_canonical and qd_canonical in bid_desc_set:
            description_exact_overlap += 1

        if not lr:
            continue

        rows_with_line_ref += 1
        canonical_lr = _canonical_line_ref(str(lr))
        candidates = bid_by_lineno.get(canonical_lr, [])
        if candidates:
            line_ref_domain_overlap += 1
            # Strict confirmation: the SAME bid row at that line_number
            # also has a canonically matching description.
            if qd_canonical and any(
                _canonical_description(b.get("description") or "") == qd_canonical
                for b in candidates
            ):
                strict_confirmation_count += 1

    signals = {
        "accepted_rows_count": accepted_count,
        "bid_rows_count": bid_count,
        "rows_with_line_ref_count": rows_with_line_ref,
        "line_ref_domain_overlap": line_ref_domain_overlap,
        "description_exact_overlap": description_exact_overlap,
        "strict_confirmation_count": strict_confirmation_count,
    }

    # Decision: rows_with_line_ref determines which branch.
    if rows_with_line_ref > 0:
        return _decide_with_line_refs(signals)
    return _decide_without_line_refs(signals)


def _decide_with_line_refs(signals: Dict[str, Any]) -> Dict[str, Any]:
    """Quote has at least one line_ref → strict_confirmation is load-bearing."""
    strict = signals["strict_confirmation_count"]

    if strict == 0:
        return {
            "pairing_status": STATUS_REJECTED,
            "pairing_reason": REASON_NO_CROSS_CONFIRMATION,
            "signals": signals,
            "warnings": [
                "No accepted quote row has a line_ref whose bid line_number "
                "also has a matching canonical description. The documents "
                "likely belong to different projects."
            ],
            "allow_mapping": False,
        }

    if strict >= MIN_STRICT_FOR_TRUSTED:
        return {
            "pairing_status": STATUS_TRUSTED,
            "pairing_reason": REASON_STRICT_CONFIRMED,
            "signals": signals,
            "warnings": [],
            "allow_mapping": True,
        }

    return {
        "pairing_status": STATUS_WEAK,
        "pairing_reason": REASON_PARTIAL_CONFIRMATION,
        "signals": signals,
        "warnings": [
            f"Only {strict} strict cross-confirmations found (trusted requires "
            f"{MIN_STRICT_FOR_TRUSTED}). Mapping will run but downstream "
            f"consumers should gate on pairing_status=weak."
        ],
        "allow_mapping": True,
    }


def _decide_without_line_refs(signals: Dict[str, Any]) -> Dict[str, Any]:
    """Quote has no line_refs → fall back to description exact overlap."""
    desc = signals["description_exact_overlap"]

    if desc == 0:
        return {
            "pairing_status": STATUS_REJECTED,
            "pairing_reason": REASON_INSUFFICIENT_OVERLAP,
            "signals": signals,
            "warnings": [
                "Quote rows carry no line_refs and zero canonical "
                "description overlap with the bid side. No deterministic "
                "pairing evidence available."
            ],
            "allow_mapping": False,
        }

    if desc >= MIN_DESC_OVERLAP_FOR_TRUSTED:
        return {
            "pairing_status": STATUS_TRUSTED,
            "pairing_reason": REASON_DESC_ONLY_CONFIRMED,
            "signals": signals,
            "warnings": [],
            "allow_mapping": True,
        }

    if desc >= MIN_DESC_OVERLAP_FOR_WEAK:
        return {
            "pairing_status": STATUS_WEAK,
            "pairing_reason": REASON_PARTIAL_CONFIRMATION,
            "signals": signals,
            "warnings": [
                f"Description-only overlap is {desc} (trusted requires "
                f"{MIN_DESC_OVERLAP_FOR_TRUSTED}). Pairing is weak."
            ],
            "allow_mapping": True,
        }

    return {
        "pairing_status": STATUS_REJECTED,
        "pairing_reason": REASON_INSUFFICIENT_OVERLAP,
        "signals": signals,
        "warnings": [
            f"Description-only overlap is {desc}, below weak threshold "
            f"{MIN_DESC_OVERLAP_FOR_WEAK}."
        ],
        "allow_mapping": False,
    }


def _rejected(reason: str, signals: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "pairing_status": STATUS_REJECTED,
        "pairing_reason": reason,
        "signals": signals,
        "warnings": [reason],
        "allow_mapping": False,
    }
