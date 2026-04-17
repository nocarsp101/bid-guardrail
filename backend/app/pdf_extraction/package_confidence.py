"""
C64 — Package confidence / bid exposure layer.

Deterministic package-level gating from existing quote/package signals.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

PACKAGE_GATE_VERSION = "package_confidence/v1"

PKG_BLOCKED = "PACKAGE_BLOCKED"
PKG_HIGH_RISK = "PACKAGE_HIGH_RISK"
PKG_CONDITIONAL = "PACKAGE_CONDITIONAL"
PKG_READY = "PACKAGE_READY"

_PKG_ORDER = {PKG_BLOCKED: 0, PKG_HIGH_RISK: 1, PKG_CONDITIONAL: 2, PKG_READY: 3}


def evaluate_package_confidence(
    package_overview: Dict[str, Any],
    vendor_comparison: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    po = package_overview or {}
    vc = vendor_comparison or {}
    ps = po.get("package_summary") or {}
    quotes = po.get("quote_summaries") or []

    reasons: List[Dict[str, Any]] = []
    gate = PKG_READY

    # Check 1: any blocked quotes.
    blocked_quotes = [q for q in quotes if q.get("gate_outcome") == "BLOCKED"]
    if blocked_quotes:
        gate = _escalate(gate, PKG_BLOCKED)
        reasons.append({"check": "blocked_quotes", "severity": "critical",
                        "detail": f"{len(blocked_quotes)}_quotes_blocked",
                        "refs": [q["job_id"] for q in blocked_quotes]})

    # Check 2: unresolved reliance decisions.
    unresolved_rel = [q for q in quotes if q.get("reliance_decision") in (None, "decision_deferred")]
    if unresolved_rel:
        gate = _escalate(gate, PKG_HIGH_RISK)
        reasons.append({"check": "unresolved_reliance", "severity": "high",
                        "detail": f"{len(unresolved_rel)}_quotes_without_reliance_decision",
                        "refs": [q["job_id"] for q in unresolved_rel]})

    # Check 3: uncovered scope.
    total_scope_gap = int(ps.get("total_scope_not_addressed") or 0)
    if total_scope_gap > 0:
        gate = _escalate(gate, PKG_CONDITIONAL)
        reasons.append({"check": "uncovered_scope", "severity": "medium",
                        "detail": f"{total_scope_gap}_bid_items_not_addressed_across_package",
                        "refs": []})

    # Check 4: internal carry burden.
    total_carry = int(ps.get("total_carry_internally") or 0)
    if total_carry > 0:
        gate = _escalate(gate, PKG_CONDITIONAL)
        reasons.append({"check": "internal_carry_burden", "severity": "medium",
                        "detail": f"{total_carry}_items_carried_internally",
                        "refs": []})

    # Check 5: open clarifications.
    total_open_clar = int(ps.get("total_open_clarifications") or 0)
    if total_open_clar > 0:
        gate = _escalate(gate, PKG_CONDITIONAL)
        reasons.append({"check": "open_clarifications", "severity": "medium",
                        "detail": f"{total_open_clar}_clarifications_still_open",
                        "refs": []})

    # Check 6: unresolved evidence.
    total_unresolved_ev = int(ps.get("total_unresolved_evidence") or 0)
    if total_unresolved_ev > 0:
        gate = _escalate(gate, PKG_HIGH_RISK)
        reasons.append({"check": "unresolved_evidence", "severity": "high",
                        "detail": f"{total_unresolved_ev}_evidence_blocks_unresolved",
                        "refs": []})

    # Check 7: all quotes high risk.
    high_risk_quotes = [q for q in quotes if q.get("risk_level") in ("high", "critical")]
    if quotes and len(high_risk_quotes) == len(quotes):
        gate = _escalate(gate, PKG_HIGH_RISK)
        reasons.append({"check": "all_quotes_high_risk", "severity": "high",
                        "detail": f"all_{len(quotes)}_quotes_at_high_or_critical_risk",
                        "refs": [q["job_id"] for q in high_risk_quotes]})

    return {
        "package_gate_version": PACKAGE_GATE_VERSION,
        "bid_id": po.get("bid_id"),
        "package_gate_outcome": gate,
        "gate_reasons": reasons,
        "gate_summary": {
            "reason_count": len(reasons),
            "severity_counts": {
                "critical": sum(1 for r in reasons if r["severity"] == "critical"),
                "high": sum(1 for r in reasons if r["severity"] == "high"),
                "medium": sum(1 for r in reasons if r["severity"] == "medium"),
            },
            "quote_count": len(quotes),
            "blocked_quote_count": len(blocked_quotes),
            "unresolved_reliance_count": len(unresolved_rel),
        },
    }


def _escalate(current: str, candidate: str) -> str:
    if _PKG_ORDER.get(candidate, 99) < _PKG_ORDER.get(current, 99):
        return candidate
    return current
