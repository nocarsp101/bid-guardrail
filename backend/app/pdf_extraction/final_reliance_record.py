"""
C61 — Final reliance record.

Append-only record capturing whether and how a quote was relied upon.
Never mutates prior decisions. Historically auditable.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

RELIANCE_VERSION = "final_reliance_record/v1"

RELIANCE_RELIED_UPON = "relied_upon"
RELIANCE_RELIED_WITH_CAVEATS = "relied_upon_with_caveats"
RELIANCE_NOT_RELIED_UPON = "not_relied_upon"
RELIANCE_DEFERRED = "decision_deferred"

_ALL_RELIANCE = frozenset({
    RELIANCE_RELIED_UPON, RELIANCE_RELIED_WITH_CAVEATS,
    RELIANCE_NOT_RELIED_UPON, RELIANCE_DEFERRED,
})


def create_reliance_record(
    job_id: str,
    reliance_decision: str,
    gate_output: Optional[Dict[str, Any]] = None,
    risk_output: Optional[Dict[str, Any]] = None,
    decision_packet: Optional[Dict[str, Any]] = None,
    recommendation_output: Optional[Dict[str, Any]] = None,
    reeval_history: Optional[Dict[str, Any]] = None,
    decided_by: Optional[str] = None,
    decided_at: Optional[str] = None,
    decision_note: Optional[str] = None,
    active_assumptions: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    gate = gate_output or {}
    risk = risk_output or {}
    dp = decision_packet or {}
    rec = recommendation_output or {}
    reeval = reeval_history or {}

    if reliance_decision not in _ALL_RELIANCE:
        reliance_decision_valid = False
    else:
        reliance_decision_valid = True

    unresolved_items = _collect_unresolved(dp, risk)
    carry_posture = _carry_posture_snapshot(rec)
    evidence_snapshot = _evidence_snapshot(dp, gate, risk)

    record: Dict[str, Any] = {
        "reliance_version": RELIANCE_VERSION,
        "record_id": f"rel-{job_id}",
        "job_id": job_id,
        "reliance_decision": reliance_decision,
        "reliance_decision_valid": reliance_decision_valid,
        "decided_by": decided_by,
        "decided_at": decided_at,
        "decision_note": decision_note,
        "final_gate_outcome": gate.get("gate_outcome"),
        "final_risk_level": risk.get("overall_risk_level") or dp.get("overall_risk_level"),
        "final_decision_posture": dp.get("decision_posture"),
        "unresolved_items": unresolved_items,
        "active_assumptions": deepcopy(active_assumptions or []),
        "carry_gap_posture": carry_posture,
        "evidence_snapshot": evidence_snapshot,
        "reeval_cycle_count": int(reeval.get("cycle_count") or 0),
        "reeval_current_cycle_id": reeval.get("current_cycle_id"),
    }
    return record


def append_reliance_revision(
    existing_records: List[Dict[str, Any]],
    new_record: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Append a new reliance record. Never mutates existing list."""
    out = [deepcopy(r) for r in existing_records]
    new = deepcopy(new_record)
    new["revision_sequence"] = len(out)
    for prior in out:
        prior.setdefault("superseded_by", None)
    if out:
        out[-1]["superseded_by"] = new.get("record_id")
    out.append(new)
    return out


def get_current_reliance(records: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not records:
        return None
    return deepcopy(records[-1])


def get_reliance_history(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return deepcopy(records)


def _collect_unresolved(dp, risk) -> Dict[str, Any]:
    blocking = risk.get("blocking_risks") or []
    warnings = risk.get("warning_risks") or []
    sg = dp.get("scope_gaps") or {}
    ev = dp.get("evidence_status") or {}
    return {
        "blocking_count": len(blocking),
        "warning_count": len(warnings),
        "scope_not_addressed": int(sg.get("not_addressed_count") or 0),
        "scope_ambiguous": int(sg.get("ambiguous_count") or 0),
        "unresolved_evidence_blocks": int(ev.get("unresolved_block_count") or 0),
    }


def _carry_posture_snapshot(rec) -> Dict[str, Any]:
    summary = rec.get("recommendation_summary") or {}
    return {
        "carry_in_sub_quote": int(summary.get("carry_in_sub_quote_count") or 0),
        "carry_internally": int(summary.get("carry_internally_count") or 0),
        "hold_as_contingency": int(summary.get("hold_as_contingency_count") or 0),
        "clarify_before_reliance": int(summary.get("clarify_before_reliance_count") or 0),
        "block_quote_reliance": int(summary.get("block_quote_reliance_count") or 0),
    }


def _evidence_snapshot(dp, gate, risk) -> Dict[str, Any]:
    cp = dp.get("comparability_posture") or {}
    return {
        "total_rows": int(cp.get("total_rows") or 0),
        "comparable_matched": int(cp.get("comparable_matched") or 0),
        "non_comparable": int(cp.get("non_comparable") or 0),
        "gate_outcome": gate.get("gate_outcome"),
        "gate_reason_count": int((gate.get("gate_summary") or {}).get("reason_count") or 0),
        "risk_factor_count": int((risk.get("risk_summary") or {}).get("total_factors") or 0),
    }
