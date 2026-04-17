"""
C70 — Final bid carry justification record.

Append-only record capturing package confidence, authority posture,
unresolved gaps, internal carry decisions, and acknowledged review items
at bid carry time. Never overwrites prior decisions.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

CARRY_JUSTIFICATION_VERSION = "bid_carry_justification/v1"

CARRY_DECISION_PROCEED = "proceed_to_bid"
CARRY_DECISION_PROCEED_WITH_CAVEATS = "proceed_with_caveats"
CARRY_DECISION_HOLD = "hold_pending_resolution"
CARRY_DECISION_WITHDRAW = "withdraw_from_bid"

_ALL_DECISIONS = frozenset({
    CARRY_DECISION_PROCEED, CARRY_DECISION_PROCEED_WITH_CAVEATS,
    CARRY_DECISION_HOLD, CARRY_DECISION_WITHDRAW,
})


def create_carry_justification(
    bid_id: str,
    carry_decision: str,
    package_gate: Optional[Dict[str, Any]] = None,
    authority_posture: Optional[Dict[str, Any]] = None,
    authority_action_packet: Optional[Dict[str, Any]] = None,
    recommendation_output: Optional[Dict[str, Any]] = None,
    decided_by: Optional[str] = None,
    decided_at: Optional[str] = None,
    decision_note: Optional[str] = None,
    acknowledged_review_items: Optional[List[str]] = None,
) -> Dict[str, Any]:
    pg = package_gate or {}
    ap = authority_posture or {}
    aap = authority_action_packet or {}
    rec = recommendation_output or {}

    valid_decision = carry_decision in _ALL_DECISIONS
    unresolved_authority = _collect_unresolved_authority(aap)
    carry_posture_snap = _carry_snapshot(rec)
    authority_snap = _authority_snapshot(ap)

    return {
        "carry_justification_version": CARRY_JUSTIFICATION_VERSION,
        "record_id": f"carry-{bid_id}",
        "bid_id": bid_id,
        "carry_decision": carry_decision,
        "carry_decision_valid": valid_decision,
        "decided_by": decided_by,
        "decided_at": decided_at,
        "decision_note": decision_note,
        "package_gate_outcome": pg.get("package_gate_outcome"),
        "authority_package_posture": ap.get("authority_package_posture"),
        "unresolved_authority_gaps": unresolved_authority,
        "internal_carry_snapshot": carry_posture_snap,
        "authority_snapshot": authority_snap,
        "acknowledged_review_items": list(acknowledged_review_items or []),
        "package_gate_reasons": deepcopy(pg.get("gate_reasons") or []),
        "authority_posture_reasons": deepcopy(ap.get("posture_reasons") or []),
    }


def append_carry_revision(
    existing_records: List[Dict[str, Any]],
    new_record: Dict[str, Any],
) -> List[Dict[str, Any]]:
    out = [deepcopy(r) for r in existing_records]
    new = deepcopy(new_record)
    new["revision_sequence"] = len(out)
    for prior in out:
        prior.setdefault("superseded_by", None)
    if out:
        out[-1]["superseded_by"] = new.get("record_id")
    out.append(new)
    return out


def get_current_carry_justification(records: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not records:
        return None
    return deepcopy(records[-1])


def get_carry_history(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return deepcopy(records)


def _collect_unresolved_authority(aap: Dict[str, Any]) -> Dict[str, Any]:
    summary = aap.get("action_summary") or {}
    return {
        "total_gaps": int(summary.get("total_gaps") or 0),
        "not_covered_count": int(summary.get("not_covered_count") or 0),
        "weakly_covered_count": int(summary.get("weakly_covered_count") or 0),
        "review_required_count": int(summary.get("review_required_count") or 0),
    }


def _carry_snapshot(rec: Dict[str, Any]) -> Dict[str, Any]:
    s = rec.get("recommendation_summary") or {}
    return {
        "carry_in_sub_quote": int(s.get("carry_in_sub_quote_count") or 0),
        "carry_internally": int(s.get("carry_internally_count") or 0),
        "hold_as_contingency": int(s.get("hold_as_contingency_count") or 0),
        "clarify_before_reliance": int(s.get("clarify_before_reliance_count") or 0),
        "block_quote_reliance": int(s.get("block_quote_reliance_count") or 0),
    }


def _authority_snapshot(ap: Dict[str, Any]) -> Dict[str, Any]:
    s = ap.get("posture_summary") or {}
    return {
        "authority_package_posture": ap.get("authority_package_posture"),
        "total_authority_topics": int(s.get("total_authority_topics") or 0),
        "covered": int(s.get("covered") or 0),
        "not_covered": int(s.get("not_covered") or 0),
        "required_not_covered": int(s.get("required_not_covered") or 0),
        "required_weakly_covered": int(s.get("required_weakly_covered") or 0),
    }
