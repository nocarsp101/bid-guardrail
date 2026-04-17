"""
C55 — Bid submission confidence gate.

Deterministic gating output strictly from existing governed signals.
No inference. Explicit reasons for every gate outcome.

Closed gate outcomes:
    BLOCKED     — cannot submit; critical blockers present
    HIGH_RISK   — submission possible but carries material risk
    CONDITIONAL — submission possible with documented caveats
    SAFE        — no actionable blockers, caveats, or gaps
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

GATE_VERSION = "submission_gate/v1"

GATE_BLOCKED = "BLOCKED"
GATE_HIGH_RISK = "HIGH_RISK"
GATE_CONDITIONAL = "CONDITIONAL"
GATE_SAFE = "SAFE"


def evaluate_submission_gate(
    risk_output: Optional[Dict[str, Any]] = None,
    decision_packet: Optional[Dict[str, Any]] = None,
    review_workflow: Optional[Dict[str, Any]] = None,
    scope_interpretation: Optional[Dict[str, Any]] = None,
    resolution_output: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    risk = risk_output or {}
    dp = decision_packet or {}
    rw = review_workflow or {}
    si = scope_interpretation or {}
    res = resolution_output or {}

    reasons: List[Dict[str, Any]] = []
    gate = GATE_SAFE

    # Check 1: blocking risks → BLOCKED.
    blocking = risk.get("blocking_risks") or []
    if blocking:
        gate = GATE_BLOCKED
        for b in blocking:
            reasons.append({"check": "blocking_risk", "severity": "critical",
                            "detail": b.get("detail"), "factor_id": b.get("factor_id")})

    # Check 2: pairing blocked → BLOCKED.
    if (res.get("packet_status") == "blocked" or
            dp.get("decision_posture") == "blocked"):
        if gate != GATE_BLOCKED:
            gate = GATE_BLOCKED
        reasons.append({"check": "pairing_blocked", "severity": "critical",
                        "detail": "pairing_rejected_or_packet_blocked"})

    # Check 3: unresolved evidence → at least HIGH_RISK.
    unresolved = int((dp.get("evidence_status") or {}).get("unresolved_block_count") or 0)
    if unresolved > 0:
        gate = _escalate(gate, GATE_HIGH_RISK)
        reasons.append({"check": "unresolved_evidence", "severity": "high",
                        "detail": f"{unresolved}_evidence_blocks_unresolved"})

    # Check 4: unapproved manual entries → at least HIGH_RISK.
    readiness = rw.get("readiness_status")
    if readiness in ("blocked_pending_approval", "blocked_pending_evidence", "not_started"):
        gate = _escalate(gate, GATE_HIGH_RISK)
        reasons.append({"check": "review_not_ready", "severity": "high",
                        "detail": f"review_readiness={readiness}"})

    # Check 5: high risk level → at least HIGH_RISK.
    overall_risk = risk.get("overall_risk_level") or dp.get("overall_risk_level")
    if overall_risk == "critical":
        gate = _escalate(gate, GATE_BLOCKED)
        reasons.append({"check": "critical_risk_level", "severity": "critical",
                        "detail": "overall_risk_level=critical"})
    elif overall_risk == "high":
        gate = _escalate(gate, GATE_HIGH_RISK)
        reasons.append({"check": "high_risk_level", "severity": "high",
                        "detail": "overall_risk_level=high"})

    # Check 6: low comparability → at least CONDITIONAL.
    cp = dp.get("comparability_posture") or {}
    total = int(cp.get("total_rows") or 0)
    matched = int(cp.get("comparable_matched") or 0)
    if total > 0 and matched == 0:
        gate = _escalate(gate, GATE_CONDITIONAL)
        reasons.append({"check": "zero_comparability", "severity": "medium",
                        "detail": f"0_of_{total}_rows_comparable"})
    elif total > 0 and (matched / total) < 0.5:
        gate = _escalate(gate, GATE_CONDITIONAL)
        reasons.append({"check": "low_comparability", "severity": "medium",
                        "detail": f"{matched}_of_{total}_rows_comparable"})

    # Check 7: large scope gaps → at least CONDITIONAL.
    sg = dp.get("scope_gaps") or si.get("scope_summary") or {}
    not_addressed = int(sg.get("not_addressed_count") or 0)
    ambiguous = int(sg.get("ambiguous_count") or 0)
    if not_addressed > 0:
        gate = _escalate(gate, GATE_CONDITIONAL)
        reasons.append({"check": "scope_not_addressed", "severity": "medium",
                        "detail": f"{not_addressed}_bid_items_not_addressed"})
    if ambiguous > 0:
        gate = _escalate(gate, GATE_CONDITIONAL)
        reasons.append({"check": "scope_ambiguous", "severity": "medium",
                        "detail": f"{ambiguous}_scope_items_ambiguous"})

    # Check 8: source conflicts → at least CONDITIONAL.
    conflicts = int(cp.get("conflicts") or 0)
    if conflicts > 0:
        gate = _escalate(gate, GATE_CONDITIONAL)
        reasons.append({"check": "source_conflicts", "severity": "medium",
                        "detail": f"{conflicts}_source_conflicts"})

    return {
        "gate_version": GATE_VERSION,
        "gate_outcome": gate,
        "gate_reasons": reasons,
        "gate_summary": {
            "reason_count": len(reasons),
            "severity_counts": {
                "critical": sum(1 for r in reasons if r["severity"] == "critical"),
                "high": sum(1 for r in reasons if r["severity"] == "high"),
                "medium": sum(1 for r in reasons if r["severity"] == "medium"),
            },
        },
    }


_GATE_ORDER = {GATE_BLOCKED: 0, GATE_HIGH_RISK: 1, GATE_CONDITIONAL: 2, GATE_SAFE: 3}


def _escalate(current: str, candidate: str) -> str:
    if _GATE_ORDER.get(candidate, 99) < _GATE_ORDER.get(current, 99):
        return candidate
    return current
