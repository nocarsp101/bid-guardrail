"""
C62 — Bid package overview layer.

Package-level rollup across multiple quote dossiers for the same bid.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

PACKAGE_OVERVIEW_VERSION = "bid_package_overview/v1"


def build_package_overview(
    bid_id: str,
    dossiers: List[Dict[str, Any]],
    reliance_records: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> Dict[str, Any]:
    reliance_records = reliance_records or {}
    quote_summaries: List[Dict[str, Any]] = []
    reliance_dist: Dict[str, int] = {}
    total_open_clar = 0
    total_carry_internally = 0
    total_scope_not_addressed = 0
    total_scope_ambiguous = 0
    total_blocking = 0
    total_risk_factors = 0
    total_unresolved_evidence = 0
    gate_dist: Dict[str, int] = {}
    risk_dist: Dict[str, int] = {}

    for d in dossiers:
        jid = d.get("job_id")
        vendor = d.get("vendor_name")
        gate = (d.get("latest_gate") or {}).get("gate_outcome")
        risk_level = (d.get("latest_risk") or {}).get("overall_risk_level")
        posture = d.get("decision_posture")
        open_cl = (d.get("open_clarifications") or {}).get("total_open", 0)
        sg = d.get("scope_gaps") or {}
        ev = d.get("evidence_status") or {}
        rp = d.get("reliance_posture") or {}
        rec_sum = d.get("recommendation_summary") or {}

        rel_recs = reliance_records.get(jid) or []
        current_rel = rel_recs[-1] if rel_recs else None
        rel_decision = (current_rel or {}).get("reliance_decision")

        quote_summaries.append({
            "job_id": jid,
            "vendor_name": vendor,
            "gate_outcome": gate,
            "risk_level": risk_level,
            "decision_posture": posture,
            "reliance_decision": rel_decision,
            "open_clarifications": int(open_cl),
            "scope_not_addressed": int(sg.get("not_addressed_count") or 0),
            "carry_internally_count": int(rec_sum.get("carry_internally_count") or 0),
            "unresolved_evidence": int(ev.get("unresolved_block_count") or 0),
        })

        gate_dist[gate or "none"] = gate_dist.get(gate or "none", 0) + 1
        risk_dist[risk_level or "none"] = risk_dist.get(risk_level or "none", 0) + 1
        if rel_decision:
            reliance_dist[rel_decision] = reliance_dist.get(rel_decision, 0) + 1
        total_open_clar += int(open_cl)
        total_carry_internally += int(rec_sum.get("carry_internally_count") or 0)
        total_scope_not_addressed += int(sg.get("not_addressed_count") or 0)
        total_scope_ambiguous += int(sg.get("ambiguous_count") or 0)
        total_blocking += int((d.get("latest_risk") or {}).get("blocking_count") or 0)
        total_risk_factors += int((d.get("latest_risk") or {}).get("factor_count") or 0)
        total_unresolved_evidence += int(ev.get("unresolved_block_count") or 0)

    return {
        "package_overview_version": PACKAGE_OVERVIEW_VERSION,
        "bid_id": bid_id,
        "quote_count": len(dossiers),
        "quote_summaries": quote_summaries,
        "package_summary": {
            "gate_outcome_distribution": dict(sorted(gate_dist.items())),
            "risk_level_distribution": dict(sorted(risk_dist.items())),
            "reliance_decision_distribution": dict(sorted(reliance_dist.items())),
            "total_open_clarifications": total_open_clar,
            "total_carry_internally": total_carry_internally,
            "total_scope_not_addressed": total_scope_not_addressed,
            "total_scope_ambiguous": total_scope_ambiguous,
            "total_blocking_risks": total_blocking,
            "total_risk_factors": total_risk_factors,
            "total_unresolved_evidence": total_unresolved_evidence,
        },
    }
