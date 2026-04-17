"""
C101 — Operator workflow actions.

Deterministic workflow actions for acknowledgement, clarification
progression, carry-decision advancement, and review-item
acknowledgement. All actions append a new revision rather than
mutating the prior record. Closed-vocabulary action/state taxonomy.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

OPERATOR_ACTIONS_VERSION = "operator_workflow_actions/v1"

# Closed action vocabulary ------------------------------------------------
ACTION_ACKNOWLEDGE_REVIEW = "acknowledge_review"
ACTION_CLARIFICATION_ADVANCE = "clarification_advance"
ACTION_CARRY_ADVANCE = "carry_advance"
ACTION_ACKNOWLEDGE_ITEM = "acknowledge_item"

_ALL_ACTIONS = frozenset({
    ACTION_ACKNOWLEDGE_REVIEW, ACTION_CLARIFICATION_ADVANCE,
    ACTION_CARRY_ADVANCE, ACTION_ACKNOWLEDGE_ITEM,
})

# Clarification progression states (closed) -------------------------------
CLARIFICATION_PENDING = "pending"
CLARIFICATION_SENT = "sent"
CLARIFICATION_RESPONDED = "responded"
CLARIFICATION_CLOSED = "closed"

_CLARIFICATION_TRANSITIONS = {
    CLARIFICATION_PENDING: {CLARIFICATION_SENT, CLARIFICATION_CLOSED},
    CLARIFICATION_SENT: {CLARIFICATION_RESPONDED, CLARIFICATION_CLOSED},
    CLARIFICATION_RESPONDED: {CLARIFICATION_CLOSED},
    CLARIFICATION_CLOSED: set(),
}

# Carry decision progression (closed) -------------------------------------
CARRY_PROPOSED = "proposed"
CARRY_UNDER_REVIEW = "under_review"
CARRY_APPROVED = "approved"
CARRY_REJECTED = "rejected"

_CARRY_TRANSITIONS = {
    CARRY_PROPOSED: {CARRY_UNDER_REVIEW, CARRY_REJECTED},
    CARRY_UNDER_REVIEW: {CARRY_APPROVED, CARRY_REJECTED},
    CARRY_APPROVED: set(),
    CARRY_REJECTED: set(),
}

# Status vocabulary -------------------------------------------------------
STATUS_OK = "ok"
STATUS_UNKNOWN_ACTION = "unknown_action"
STATUS_NOT_FOUND = "record_not_found"
STATUS_INVALID_TRANSITION = "invalid_transition"
STATUS_MISSING_FIELD = "missing_field"


def list_actions() -> List[str]:
    return sorted(_ALL_ACTIONS)


def list_clarification_states() -> List[str]:
    return sorted(_CLARIFICATION_TRANSITIONS.keys())


def list_carry_states() -> List[str]:
    return sorted(_CARRY_TRANSITIONS.keys())


def apply_action(repository: Any, action: str,
                  payload: Dict[str, Any]) -> Dict[str, Any]:
    if action not in _ALL_ACTIONS:
        return _fail(STATUS_UNKNOWN_ACTION, action=action)

    if action == ACTION_ACKNOWLEDGE_REVIEW:
        return acknowledge_review(repository, **payload)
    if action == ACTION_CLARIFICATION_ADVANCE:
        return advance_clarification(repository, **payload)
    if action == ACTION_CARRY_ADVANCE:
        return advance_carry_decision(repository, **payload)
    if action == ACTION_ACKNOWLEDGE_ITEM:
        return acknowledge_item(repository, **payload)
    return _fail(STATUS_UNKNOWN_ACTION, action=action)


# ---------------------------------------------------------------------
# Action handlers — all append-only (save a new revision).
# ---------------------------------------------------------------------


def acknowledge_review(repository: Any, *, bid_id: str,
                        acknowledged_by: Optional[str] = None,
                        note: Optional[str] = None,
                        acknowledged_at: Optional[str] = None,
                        **_ignored) -> Dict[str, Any]:
    prior = repository.latest("bid_readiness_snapshot", bid_id=bid_id)
    if prior is None:
        return _fail(STATUS_NOT_FOUND, artifact_type="bid_readiness_snapshot",
                      bid_id=bid_id)

    base = deepcopy((prior.get("envelope") or {}).get("artifact") or {})
    base.setdefault("operator_actions", [])
    base["operator_actions"] = list(base["operator_actions"]) + [{
        "action": ACTION_ACKNOWLEDGE_REVIEW,
        "acknowledged_by": acknowledged_by,
        "acknowledged_at": acknowledged_at,
        "note": note,
    }]
    new_rec = repository.save("bid_readiness_snapshot", base,
                                metadata={"created_by": acknowledged_by
                                            or "operator",
                                            "created_at": acknowledged_at})
    return {
        "operator_actions_version": OPERATOR_ACTIONS_VERSION,
        "status": STATUS_OK,
        "action": ACTION_ACKNOWLEDGE_REVIEW,
        "bid_id": bid_id,
        "prior_record_id": prior.get("record_id"),
        "new_record_id": new_rec.get("record_id"),
        "revision_sequence": new_rec.get("revision_sequence"),
    }


def advance_clarification(repository: Any, *, job_id: str,
                           clarification_id: str,
                           next_state: str,
                           advanced_by: Optional[str] = None,
                           advanced_at: Optional[str] = None,
                           note: Optional[str] = None,
                           **_ignored) -> Dict[str, Any]:
    if not clarification_id:
        return _fail(STATUS_MISSING_FIELD, field="clarification_id")
    if next_state not in _CLARIFICATION_TRANSITIONS:
        return _fail(STATUS_INVALID_TRANSITION, next_state=next_state)

    prior = repository.latest("quote_dossier", job_id=job_id)
    if prior is None:
        return _fail(STATUS_NOT_FOUND, artifact_type="quote_dossier",
                      job_id=job_id)

    base = deepcopy((prior.get("envelope") or {}).get("artifact") or {})
    tracked = list(base.get("clarification_progressions") or [])
    current_state = _current_clarification_state(tracked, clarification_id)
    allowed = _CLARIFICATION_TRANSITIONS.get(current_state or
                                                CLARIFICATION_PENDING)
    if next_state not in allowed:
        return _fail(STATUS_INVALID_TRANSITION,
                      from_state=current_state, to_state=next_state,
                      clarification_id=clarification_id)
    tracked.append({
        "clarification_id": clarification_id,
        "from_state": current_state,
        "to_state": next_state,
        "advanced_by": advanced_by,
        "advanced_at": advanced_at,
        "note": note,
    })
    base["clarification_progressions"] = tracked

    new_rec = repository.save("quote_dossier", base,
                                metadata={"created_by": advanced_by
                                            or "operator",
                                            "created_at": advanced_at})
    return {
        "operator_actions_version": OPERATOR_ACTIONS_VERSION,
        "status": STATUS_OK,
        "action": ACTION_CLARIFICATION_ADVANCE,
        "job_id": job_id,
        "clarification_id": clarification_id,
        "from_state": current_state,
        "to_state": next_state,
        "prior_record_id": prior.get("record_id"),
        "new_record_id": new_rec.get("record_id"),
        "revision_sequence": new_rec.get("revision_sequence"),
    }


def advance_carry_decision(repository: Any, *, bid_id: str,
                             next_state: str,
                             advanced_by: Optional[str] = None,
                             advanced_at: Optional[str] = None,
                             note: Optional[str] = None,
                             **_ignored) -> Dict[str, Any]:
    if next_state not in _CARRY_TRANSITIONS:
        return _fail(STATUS_INVALID_TRANSITION, next_state=next_state)

    prior = repository.latest("bid_carry_justification", bid_id=bid_id)
    if prior is None:
        return _fail(STATUS_NOT_FOUND, artifact_type="bid_carry_justification",
                      bid_id=bid_id)

    base = deepcopy((prior.get("envelope") or {}).get("artifact") or {})
    progressions = list(base.get("carry_progressions") or [])
    current_state = (progressions[-1].get("to_state") if progressions
                      else CARRY_PROPOSED)
    allowed = _CARRY_TRANSITIONS.get(current_state)
    if next_state not in allowed:
        return _fail(STATUS_INVALID_TRANSITION,
                      from_state=current_state, to_state=next_state)
    progressions.append({
        "from_state": current_state,
        "to_state": next_state,
        "advanced_by": advanced_by,
        "advanced_at": advanced_at,
        "note": note,
    })
    base["carry_progressions"] = progressions
    base["carry_progression_state"] = next_state

    new_rec = repository.save("bid_carry_justification", base,
                                metadata={"created_by": advanced_by
                                            or "operator",
                                            "created_at": advanced_at})
    return {
        "operator_actions_version": OPERATOR_ACTIONS_VERSION,
        "status": STATUS_OK,
        "action": ACTION_CARRY_ADVANCE,
        "bid_id": bid_id,
        "from_state": current_state,
        "to_state": next_state,
        "prior_record_id": prior.get("record_id"),
        "new_record_id": new_rec.get("record_id"),
        "revision_sequence": new_rec.get("revision_sequence"),
    }


def acknowledge_item(repository: Any, *, bid_id: str,
                      item_id: str,
                      acknowledged_by: Optional[str] = None,
                      acknowledged_at: Optional[str] = None,
                      note: Optional[str] = None,
                      **_ignored) -> Dict[str, Any]:
    if not item_id:
        return _fail(STATUS_MISSING_FIELD, field="item_id")

    prior = repository.latest("priority_queue", bid_id=bid_id)
    if prior is None:
        return _fail(STATUS_NOT_FOUND, artifact_type="priority_queue",
                      bid_id=bid_id)

    base = deepcopy((prior.get("envelope") or {}).get("artifact") or {})
    acks = list(base.get("item_acknowledgements") or [])
    acks.append({
        "item_id": item_id,
        "acknowledged_by": acknowledged_by,
        "acknowledged_at": acknowledged_at,
        "note": note,
    })
    base["item_acknowledgements"] = acks

    new_rec = repository.save("priority_queue", base,
                                metadata={"created_by": acknowledged_by
                                            or "operator",
                                            "created_at": acknowledged_at})
    return {
        "operator_actions_version": OPERATOR_ACTIONS_VERSION,
        "status": STATUS_OK,
        "action": ACTION_ACKNOWLEDGE_ITEM,
        "bid_id": bid_id,
        "item_id": item_id,
        "prior_record_id": prior.get("record_id"),
        "new_record_id": new_rec.get("record_id"),
        "revision_sequence": new_rec.get("revision_sequence"),
    }


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _current_clarification_state(progressions: List[Dict[str, Any]],
                                   clarification_id: str) -> Optional[str]:
    last: Optional[str] = None
    for p in progressions:
        if p.get("clarification_id") == clarification_id:
            last = p.get("to_state")
    return last


def _fail(status: str, **kwargs) -> Dict[str, Any]:
    return {
        "operator_actions_version": OPERATOR_ACTIONS_VERSION,
        "status": status,
        **kwargs,
    }
