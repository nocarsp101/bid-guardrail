"""
C63 — Quote comparison / vendor choice layer.

Deterministic comparison across multiple vendor quote dossiers for the
same trade/scope. Produces structured comparison and explainable
ordering — never heuristic vendor selection.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

COMPARISON_VERSION = "vendor_comparison/v1"

_GATE_ORDER = {"SAFE": 0, "CONDITIONAL": 1, "HIGH_RISK": 2, "BLOCKED": 3}
_RISK_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3}

RANK_BEST = "best_available"
RANK_ACCEPTABLE = "acceptable"
RANK_CAUTION = "caution"
RANK_NOT_RECOMMENDED = "not_recommended"


def compare_vendors(
    dossiers: List[Dict[str, Any]],
    reliance_records: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> Dict[str, Any]:
    reliance_records = reliance_records or {}
    entries: List[Dict[str, Any]] = []

    for d in dossiers:
        jid = d.get("job_id")
        gate = (d.get("latest_gate") or {}).get("gate_outcome")
        risk = (d.get("latest_risk") or {}).get("overall_risk_level")
        posture = d.get("decision_posture")
        rp = d.get("reliance_posture") or {}
        sg = d.get("scope_gaps") or {}
        cp = d.get("comparability_posture") or {}
        oc = d.get("open_clarifications") or {}
        ev = d.get("evidence_status") or {}
        rec_sum = d.get("recommendation_summary") or {}

        rel_recs = reliance_records.get(jid) or []
        rel_decision = rel_recs[-1].get("reliance_decision") if rel_recs else None

        score = _compute_deterministic_score(gate, risk, sg, cp, oc, ev)
        rank = _derive_rank(gate, risk, score)

        entries.append({
            "job_id": jid,
            "vendor_name": d.get("vendor_name"),
            "gate_outcome": gate,
            "risk_level": risk,
            "decision_posture": posture,
            "reliance_decision": rel_decision,
            "deterministic_score": score,
            "vendor_rank": rank,
            "comparison_details": {
                "scope_not_addressed": int(sg.get("not_addressed_count") or 0),
                "scope_ambiguous": int(sg.get("ambiguous_count") or 0),
                "comparable_matched": int(cp.get("comparable_matched") or 0),
                "non_comparable": int(cp.get("non_comparable") or 0),
                "total_rows": int(cp.get("total_rows") or 0),
                "open_clarifications": int(oc.get("total_open") or 0),
                "unresolved_evidence": int(ev.get("unresolved_block_count") or 0),
                "carry_internally": int(rec_sum.get("carry_internally_count") or 0),
                "block_reliance": int(rec_sum.get("block_quote_reliance_count") or 0),
                "clarify_before_reliance": int(rec_sum.get("clarify_before_reliance_count") or 0),
            },
            "score_breakdown": _score_breakdown(gate, risk, sg, cp, oc, ev),
        })

    entries.sort(key=lambda e: e["deterministic_score"])

    return {
        "comparison_version": COMPARISON_VERSION,
        "vendor_count": len(entries),
        "vendor_entries": entries,
        "comparison_summary": {
            "best_score": entries[0]["deterministic_score"] if entries else None,
            "worst_score": entries[-1]["deterministic_score"] if entries else None,
            "gate_distribution": _dist([e["gate_outcome"] for e in entries]),
            "risk_distribution": _dist([e["risk_level"] for e in entries]),
            "rank_distribution": _dist([e["vendor_rank"] for e in entries]),
        },
    }


def _compute_deterministic_score(gate, risk, sg, cp, oc, ev) -> int:
    """Lower is better. Pure arithmetic from closed fields."""
    score = 0
    score += _GATE_ORDER.get(gate, 3) * 100
    score += _RISK_ORDER.get(risk, 3) * 50
    score += int(sg.get("not_addressed_count") or 0)
    score += int(sg.get("ambiguous_count") or 0) * 2
    score += max(0, int(cp.get("non_comparable") or 0) - int(cp.get("comparable_matched") or 0))
    score += int(oc.get("total_open") or 0) * 3
    score += int(ev.get("unresolved_block_count") or 0) * 10
    return score


def _score_breakdown(gate, risk, sg, cp, oc, ev) -> Dict[str, int]:
    return {
        "gate_penalty": _GATE_ORDER.get(gate, 3) * 100,
        "risk_penalty": _RISK_ORDER.get(risk, 3) * 50,
        "scope_gap_penalty": int(sg.get("not_addressed_count") or 0) + int(sg.get("ambiguous_count") or 0) * 2,
        "comparability_penalty": max(0, int(cp.get("non_comparable") or 0) - int(cp.get("comparable_matched") or 0)),
        "clarification_penalty": int(oc.get("total_open") or 0) * 3,
        "evidence_penalty": int(ev.get("unresolved_block_count") or 0) * 10,
    }


def _derive_rank(gate, risk, score) -> str:
    if gate == "BLOCKED" or risk == "critical":
        return RANK_NOT_RECOMMENDED
    if gate == "HIGH_RISK" or risk == "high":
        return RANK_CAUTION
    if gate == "CONDITIONAL" or risk == "medium":
        return RANK_ACCEPTABLE
    return RANK_BEST


def _dist(values: List) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for v in values:
        k = str(v) if v is not None else "none"
        out[k] = out.get(k, 0) + 1
    return dict(sorted(out.items()))
