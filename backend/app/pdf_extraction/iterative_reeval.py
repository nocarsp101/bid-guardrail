"""
C58 — Iterative re-evaluation engine.

Versioned re-evaluation cycles for the same quote. Produces before/after
comparisons of risk, gate outcome, scope coverage, and comparability
without overwriting prior states.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

REEVAL_VERSION = "iterative_reeval/v1"


def create_evaluation_history(
    initial_snapshot: Dict[str, Any],
    cycle_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a new evaluation history with the initial cycle."""
    snap = _build_snapshot(initial_snapshot, cycle_id or "cycle-0")
    return {
        "reeval_version": REEVAL_VERSION,
        "current_cycle_id": snap["cycle_id"],
        "cycle_count": 1,
        "cycles": [snap],
        "deltas": [],
    }


def append_evaluation_cycle(
    history: Dict[str, Any],
    new_snapshot: Dict[str, Any],
    cycle_id: Optional[str] = None,
    cycle_reason: Optional[str] = None,
) -> Dict[str, Any]:
    """Append a new evaluation cycle and compute delta against prior."""
    out = deepcopy(history)
    cycles = out.get("cycles") or []
    seq = len(cycles)
    effective_id = cycle_id or f"cycle-{seq}"
    snap = _build_snapshot(new_snapshot, effective_id, cycle_reason)
    prev = cycles[-1] if cycles else None

    delta = _compute_delta(prev, snap) if prev else _initial_delta(snap)

    cycles.append(snap)
    out["cycles"] = cycles
    out["current_cycle_id"] = effective_id
    out["cycle_count"] = len(cycles)
    out["deltas"] = list(out.get("deltas") or []) + [delta]
    return out


def get_current_snapshot(history: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    cycles = (history or {}).get("cycles") or []
    return deepcopy(cycles[-1]) if cycles else None


def get_delta_history(history: Dict[str, Any]) -> List[Dict[str, Any]]:
    return deepcopy((history or {}).get("deltas") or [])


# ---------------------------------------------------------------------------
# Snapshot + delta
# ---------------------------------------------------------------------------

def _build_snapshot(data: Dict[str, Any], cycle_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
    risk = data.get("risk_output") or data.get("risk") or {}
    gate = data.get("gate_output") or data.get("gate") or {}
    scope = data.get("scope_interpretation") or data.get("scope") or {}
    dp = data.get("decision_packet") or data.get("decision") or {}
    res = data.get("resolution_output") or data.get("resolution") or {}
    rw = data.get("review_workflow") or {}

    scope_summary = scope.get("scope_summary") or {}
    res_summary = res.get("resolution_summary") or {}
    cats = res_summary.get("category_counts") or {}
    cp = dp.get("comparability_posture") or {}

    return {
        "cycle_id": cycle_id,
        "cycle_reason": reason,
        "metrics": {
            "overall_risk_level": risk.get("overall_risk_level") or dp.get("overall_risk_level"),
            "gate_outcome": gate.get("gate_outcome"),
            "decision_posture": dp.get("decision_posture"),
            "readiness_status": rw.get("readiness_status"),
            "total_rows": int(res_summary.get("rows_total") or cp.get("total_rows") or 0),
            "comparable_matched": int(cp.get("comparable_matched") or cats.get("clean_match_no_resolution_needed") or 0),
            "non_comparable": int(cp.get("non_comparable") or 0),
            "scope_not_addressed": int(scope_summary.get("not_addressed_count") or 0),
            "scope_ambiguous": int(scope_summary.get("ambiguous_count") or 0),
            "risk_factor_count": int((risk.get("risk_summary") or {}).get("total_factors") or 0),
            "gate_reason_count": int((gate.get("gate_summary") or {}).get("reason_count") or 0),
        },
    }


def _compute_delta(prev: Dict[str, Any], curr: Dict[str, Any]) -> Dict[str, Any]:
    pm = prev.get("metrics") or {}
    cm = curr.get("metrics") or {}
    changes: Dict[str, Dict[str, Any]] = {}

    for key in cm:
        pv = pm.get(key)
        cv = cm.get(key)
        if pv != cv:
            changes[key] = {"before": pv, "after": cv}

    return {
        "from_cycle": prev.get("cycle_id"),
        "to_cycle": curr.get("cycle_id"),
        "cycle_reason": curr.get("cycle_reason"),
        "changed_metrics": changes,
        "metrics_changed_count": len(changes),
        "risk_improved": _risk_improved(pm.get("overall_risk_level"), cm.get("overall_risk_level")),
        "gate_improved": _gate_improved(pm.get("gate_outcome"), cm.get("gate_outcome")),
    }


def _initial_delta(snap: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "from_cycle": None,
        "to_cycle": snap.get("cycle_id"),
        "cycle_reason": snap.get("cycle_reason"),
        "changed_metrics": {},
        "metrics_changed_count": 0,
        "risk_improved": None,
        "gate_improved": None,
    }


_RISK_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}
_GATE_ORDER = {"BLOCKED": 0, "HIGH_RISK": 1, "CONDITIONAL": 2, "SAFE": 3}


def _risk_improved(before: Optional[str], after: Optional[str]) -> Optional[bool]:
    if before is None or after is None:
        return None
    return _RISK_ORDER.get(after, 99) > _RISK_ORDER.get(before, 99)


def _gate_improved(before: Optional[str], after: Optional[str]) -> Optional[bool]:
    if before is None or after is None:
        return None
    return _GATE_ORDER.get(after, 99) > _GATE_ORDER.get(before, 99)
