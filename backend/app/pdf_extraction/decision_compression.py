"""
C53 — Decision compression layer.

Groups, deduplicates, and prioritizes clarifications, scope gaps, and
risk factors into a condensed high-signal decision summary for rapid
estimator review. Every compressed item traces back to its source rows.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

COMPRESSION_VERSION = "decision_compression/v1"

_PRIORITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3, "informational": 4}


def compress_decision(
    decision_packet: Optional[Dict[str, Any]] = None,
    clarification_output: Optional[Dict[str, Any]] = None,
    recommendation_output: Optional[Dict[str, Any]] = None,
    risk_output: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    dp = decision_packet or {}
    cl = clarification_output or {}
    rec = recommendation_output or {}
    risk = risk_output or {}

    grouped_clarifications = _group_clarifications(cl.get("clarification_items") or [])
    grouped_recommendations = _group_recommendations(rec.get("recommendations") or [])
    top_risks = _top_risks(risk.get("risk_factors") or [])
    key_numbers = _key_numbers(dp, cl, rec, risk)

    return {
        "compression_version": COMPRESSION_VERSION,
        "decision_posture": dp.get("decision_posture"),
        "overall_risk_level": dp.get("overall_risk_level") or risk.get("overall_risk_level"),
        "key_numbers": key_numbers,
        "top_risks": top_risks,
        "grouped_clarifications": grouped_clarifications,
        "grouped_recommendations": grouped_recommendations,
        "blocking_issues": deepcopy(dp.get("blocking_issues") or []),
        "compression_diagnostics": {
            "raw_clarification_count": len(cl.get("clarification_items") or []),
            "compressed_clarification_groups": len(grouped_clarifications),
            "raw_recommendation_count": len(rec.get("recommendations") or []),
            "compressed_recommendation_groups": len(grouped_recommendations),
            "risk_factor_count": len(risk.get("risk_factors") or []),
        },
    }


def _group_clarifications(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[str, Dict[str, Any]] = {}
    for item in items:
        ctype = item.get("clarification_type") or "unknown"
        if ctype not in groups:
            groups[ctype] = {
                "clarification_type": ctype,
                "count": 0,
                "sample_text": item.get("clarification_text", ""),
                "source_refs": [],
            }
        groups[ctype]["count"] += 1
        ref = item.get("source_ref")
        if ref and len(groups[ctype]["source_refs"]) < 5:
            groups[ctype]["source_refs"].append(ref)
    return sorted(groups.values(), key=lambda g: -g["count"])


def _group_recommendations(recs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[str, Dict[str, Any]] = {}
    for r in recs:
        posture = r.get("handling_posture") or "unknown"
        if posture not in groups:
            groups[posture] = {
                "handling_posture": posture,
                "count": 0,
                "sample_reason": r.get("posture_reason", ""),
                "row_ids": [],
            }
        groups[posture]["count"] += 1
        rid = r.get("normalized_row_id") or r.get("bid_item_ref")
        if rid and len(groups[posture]["row_ids"]) < 5:
            groups[posture]["row_ids"].append(rid)
    return sorted(groups.values(), key=lambda g: -g["count"])


def _top_risks(factors: List[Dict[str, Any]], limit: int = 5) -> List[Dict[str, Any]]:
    sorted_f = sorted(factors, key=lambda f: (_PRIORITY_ORDER.get(f.get("severity"), 99), f.get("factor_id", "")))
    return [{"factor_id": f["factor_id"], "severity": f["severity"], "detail": f["detail"]}
            for f in sorted_f[:limit]]


def _key_numbers(dp, cl, rec, risk) -> Dict[str, Any]:
    cp = dp.get("comparability_posture") or {}
    sg = dp.get("scope_gaps") or {}
    rs = risk.get("risk_summary") or {}
    return {
        "total_rows": int(cp.get("total_rows") or 0),
        "comparable_matched": int(cp.get("comparable_matched") or 0),
        "non_comparable": int(cp.get("non_comparable") or 0),
        "scope_not_addressed": int(sg.get("not_addressed_count") or 0),
        "scope_ambiguous": int(sg.get("ambiguous_count") or 0),
        "clarification_count": len(cl.get("clarification_items") or []),
        "recommendation_count": len(rec.get("recommendations") or []),
        "risk_factor_count": int(rs.get("total_factors") or 0),
        "blocking_count": len(dp.get("blocking_issues") or []),
    }
