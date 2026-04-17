"""
C52 — Carry / gap / contingency recommendation layer.

Deterministic handling-posture classification for detected issues.
No pricing. No dollar estimates. Only deterministic posture from
conditions already present in the governed pipeline.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

RECOMMENDATION_VERSION = "carry_gap_recommendation/v1"

POSTURE_CARRY_IN_SUB_QUOTE = "carry_in_sub_quote"
POSTURE_CARRY_INTERNALLY = "carry_internally"
POSTURE_HOLD_AS_CONTINGENCY = "hold_as_contingency"
POSTURE_CLARIFY_BEFORE_RELIANCE = "clarify_before_reliance"
POSTURE_BLOCK_QUOTE_RELIANCE = "block_quote_reliance"

_ALL_POSTURES = (
    POSTURE_CARRY_IN_SUB_QUOTE,
    POSTURE_CARRY_INTERNALLY,
    POSTURE_HOLD_AS_CONTINGENCY,
    POSTURE_CLARIFY_BEFORE_RELIANCE,
    POSTURE_BLOCK_QUOTE_RELIANCE,
)

# Resolution category -> default posture mapping (closed table).
_CATEGORY_TO_POSTURE: Dict[str, str] = {
    "clean_match_no_resolution_needed": POSTURE_CARRY_IN_SUB_QUOTE,
    "non_comparable_missing_quote_source": POSTURE_CLARIFY_BEFORE_RELIANCE,
    "non_comparable_missing_external_source": POSTURE_HOLD_AS_CONTINGENCY,
    "unmapped_scope_review_required": POSTURE_CARRY_INTERNALLY,
    "ambiguous_mapping_review_required": POSTURE_CLARIFY_BEFORE_RELIANCE,
    "source_conflict_review_required": POSTURE_CLARIFY_BEFORE_RELIANCE,
    "quantity_discrepancy_review_required": POSTURE_CLARIFY_BEFORE_RELIANCE,
    "unit_discrepancy_review_required": POSTURE_CLARIFY_BEFORE_RELIANCE,
    "blocked_pairing_resolution_required": POSTURE_BLOCK_QUOTE_RELIANCE,
    "review_required_other": POSTURE_HOLD_AS_CONTINGENCY,
}

# Resolution category -> templated reason (closed).
_CATEGORY_TO_REASON: Dict[str, str] = {
    "clean_match_no_resolution_needed":
        "quote_matches_bid_carry_as_sub_price",
    "non_comparable_missing_quote_source":
        "quote_missing_qty_unit_clarify_with_sub_before_reliance",
    "non_comparable_missing_external_source":
        "no_external_quantity_to_compare_hold_as_contingency",
    "unmapped_scope_review_required":
        "quote_row_not_in_bid_schedule_carry_internally",
    "ambiguous_mapping_review_required":
        "multiple_bid_candidates_clarify_before_reliance",
    "source_conflict_review_required":
        "external_sources_disagree_clarify_before_reliance",
    "quantity_discrepancy_review_required":
        "qty_disagrees_between_quote_and_bid_clarify_before_reliance",
    "unit_discrepancy_review_required":
        "unit_disagrees_between_quote_and_bid_clarify_before_reliance",
    "blocked_pairing_resolution_required":
        "pairing_rejected_cannot_rely_on_quote",
    "review_required_other":
        "unrecognized_state_hold_as_contingency",
}

# Scope class -> posture overrides for unaddressed bid items.
_SCOPE_TO_POSTURE: Dict[str, str] = {
    "not_addressed": POSTURE_CARRY_INTERNALLY,
    "ambiguous_scope": POSTURE_CLARIFY_BEFORE_RELIANCE,
    "explicitly_excluded": POSTURE_CARRY_INTERNALLY,
}


def build_recommendations(
    resolution_output: Optional[Dict[str, Any]] = None,
    scope_interpretation: Optional[Dict[str, Any]] = None,
    risk_output: Optional[Dict[str, Any]] = None,
    office_action_output: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    recs: List[Dict[str, Any]] = []
    ordinal = 0

    # From resolution rows.
    action_lookup = _build_action_lookup(office_action_output)
    for row in (resolution_output or {}).get("resolution_rows") or []:
        rid = row.get("normalized_row_id")
        cat = row.get("resolution_category") or ""
        posture = _CATEGORY_TO_POSTURE.get(cat, POSTURE_HOLD_AS_CONTINGENCY)
        reason = _CATEGORY_TO_REASON.get(cat, "unrecognized_category")

        # Office action override: if office accepted a working basis,
        # upgrade to carry_in_sub_quote.
        if _has_working_basis_action(rid, action_lookup):
            posture = POSTURE_CARRY_IN_SUB_QUOTE
            reason = "office_accepted_working_basis"

        # If office marked lump sum, carry_in_sub_quote.
        if _has_lump_sum_action(rid, action_lookup):
            posture = POSTURE_CARRY_IN_SUB_QUOTE
            reason = "office_marked_lump_sum"

        recs.append({
            "recommendation_id": f"rec-{ordinal}",
            "normalized_row_id": rid,
            "source_type": "resolution_row",
            "resolution_category": cat,
            "handling_posture": posture,
            "posture_reason": reason,
            "row_origin": row.get("row_origin"),
        })
        ordinal += 1

    # From scope topics (unaddressed / ambiguous bid items not already in resolution).
    covered_ids = {r["normalized_row_id"] for r in recs if r.get("normalized_row_id")}
    for topic in (scope_interpretation or {}).get("scope_topics") or []:
        if topic.get("source_type") != "dot_bid_unmatched":
            continue
        ref = (topic.get("source_ref") or {}).get("line_number") or topic.get("topic_id")
        sc = topic.get("scope_class")
        posture = _SCOPE_TO_POSTURE.get(sc, POSTURE_HOLD_AS_CONTINGENCY)
        reason = f"bid_item_{sc}"
        recs.append({
            "recommendation_id": f"rec-{ordinal}",
            "normalized_row_id": None,
            "source_type": "scope_topic_bid_unmatched",
            "resolution_category": None,
            "handling_posture": posture,
            "posture_reason": reason,
            "bid_item_ref": ref,
            "description": topic.get("description"),
        })
        ordinal += 1

    summary = _build_summary(recs)
    return {
        "recommendation_version": RECOMMENDATION_VERSION,
        "recommendations": recs,
        "recommendation_summary": summary,
    }


def _build_action_lookup(office_action_output: Optional[Dict[str, Any]]) -> Dict[str, List]:
    if not office_action_output:
        return {}
    out: Dict[str, List] = {}
    for row in office_action_output.get("resolution_rows") or []:
        rid = row.get("normalized_row_id")
        if rid:
            out[rid] = row.get("office_actions") or []
    return out


def _has_working_basis_action(rid: Optional[str], lookup: Dict[str, List]) -> bool:
    if not rid:
        return False
    for a in lookup.get(rid, []):
        at = a.get("action_type") or ""
        if "working_basis" in at:
            return True
    return False


def _has_lump_sum_action(rid: Optional[str], lookup: Dict[str, List]) -> bool:
    if not rid:
        return False
    for a in lookup.get(rid, []):
        if a.get("action_type") == "mark_lump_sum_non_comparable":
            return True
    return False


def _build_summary(recs: List[Dict[str, Any]]) -> Dict[str, Any]:
    posture_counts = {p: 0 for p in _ALL_POSTURES}
    for r in recs:
        p = r.get("handling_posture")
        if p in posture_counts:
            posture_counts[p] += 1
    return {
        "total_recommendations": len(recs),
        "posture_counts": posture_counts,
        "carry_in_sub_quote_count": posture_counts[POSTURE_CARRY_IN_SUB_QUOTE],
        "carry_internally_count": posture_counts[POSTURE_CARRY_INTERNALLY],
        "hold_as_contingency_count": posture_counts[POSTURE_HOLD_AS_CONTINGENCY],
        "clarify_before_reliance_count": posture_counts[POSTURE_CLARIFY_BEFORE_RELIANCE],
        "block_quote_reliance_count": posture_counts[POSTURE_BLOCK_QUOTE_RELIANCE],
    }
