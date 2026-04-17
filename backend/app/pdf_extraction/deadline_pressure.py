"""
C71 — Bid countdown / deadline pressure overlay.

Deterministic deadline-pressure overlay for a bid package. Never
downgrades blocked or action-required states.

Closed pressure vocabulary:
    on_track, at_risk_due_to_time, critical_due_to_time, deadline_blocked
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

DEADLINE_PRESSURE_VERSION = "deadline_pressure/v1"

PRESSURE_ON_TRACK = "on_track"
PRESSURE_AT_RISK = "at_risk_due_to_time"
PRESSURE_CRITICAL = "critical_due_to_time"
PRESSURE_DEADLINE_BLOCKED = "deadline_blocked"

_PRESSURE_ORDER = {
    PRESSURE_DEADLINE_BLOCKED: 0, PRESSURE_CRITICAL: 1,
    PRESSURE_AT_RISK: 2, PRESSURE_ON_TRACK: 3,
}


def evaluate_deadline_pressure(
    hours_until_due: Optional[float] = None,
    package_gate: Optional[Dict[str, Any]] = None,
    authority_posture: Optional[Dict[str, Any]] = None,
    package_overview: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    pg = package_gate or {}
    ap = authority_posture or {}
    po = package_overview or {}
    ps = po.get("package_summary") or {}

    pressure = PRESSURE_ON_TRACK
    reasons: List[Dict[str, Any]] = []

    # Pure time check.
    if hours_until_due is not None:
        h = float(hours_until_due)
        if h <= 0:
            pressure = _escalate(pressure, PRESSURE_DEADLINE_BLOCKED)
            reasons.append({"check": "deadline_passed", "severity": "critical",
                            "detail": f"hours_until_due={h}", "source": "timing"})
        elif h <= 4:
            pressure = _escalate(pressure, PRESSURE_CRITICAL)
            reasons.append({"check": "less_than_4_hours_remaining", "severity": "high",
                            "detail": f"hours_until_due={h}", "source": "timing"})
        elif h <= 24:
            pressure = _escalate(pressure, PRESSURE_AT_RISK)
            reasons.append({"check": "less_than_24_hours_remaining", "severity": "medium",
                            "detail": f"hours_until_due={h}", "source": "timing"})

    # Open items under deadline pressure.
    open_clar = int(ps.get("total_open_clarifications") or 0)
    if open_clar > 0 and hours_until_due is not None and hours_until_due <= 24:
        pressure = _escalate(pressure, PRESSURE_CRITICAL)
        reasons.append({"check": "open_clarifications_under_24h", "severity": "high",
                        "detail": f"{open_clar}_clarifications_open",
                        "source": "package_overview"})

    # Deferred reliance decisions.
    quotes = po.get("quote_summaries") or []
    deferred = [q for q in quotes if q.get("reliance_decision") in (None, "decision_deferred")]
    if deferred and hours_until_due is not None and hours_until_due <= 24:
        pressure = _escalate(pressure, PRESSURE_CRITICAL)
        reasons.append({"check": "unresolved_reliance_under_24h", "severity": "high",
                        "detail": f"{len(deferred)}_reliance_decisions_deferred",
                        "refs": [q.get("job_id") for q in deferred]})

    # Authority required-not-covered under pressure.
    ap_summary = ap.get("posture_summary") or {}
    req_nc = int(ap_summary.get("required_not_covered") or 0)
    if req_nc > 0 and hours_until_due is not None and hours_until_due <= 24:
        pressure = _escalate(pressure, PRESSURE_CRITICAL)
        reasons.append({"check": "required_authority_not_covered_under_24h", "severity": "high",
                        "detail": f"{req_nc}_required_authority_topics_not_covered",
                        "source": "authority_posture"})

    # Package gate escalation.
    pkg_outcome = pg.get("package_gate_outcome")
    if pkg_outcome == "PACKAGE_BLOCKED":
        pressure = _escalate(pressure, PRESSURE_DEADLINE_BLOCKED)
        reasons.append({"check": "package_gate_blocked", "severity": "critical",
                        "detail": "package_gate_outcome=PACKAGE_BLOCKED",
                        "source": "package_gate"})
    elif pkg_outcome == "PACKAGE_HIGH_RISK" and hours_until_due is not None and hours_until_due <= 24:
        pressure = _escalate(pressure, PRESSURE_CRITICAL)
        reasons.append({"check": "package_high_risk_under_24h", "severity": "high",
                        "detail": "package_gate=PACKAGE_HIGH_RISK",
                        "source": "package_gate"})

    # Authority blocked escalation.
    if ap.get("authority_package_posture") == "authority_blocked":
        pressure = _escalate(pressure, PRESSURE_DEADLINE_BLOCKED)
        reasons.append({"check": "authority_blocked", "severity": "critical",
                        "detail": "authority_package_posture=authority_blocked",
                        "source": "authority_posture"})

    return {
        "deadline_pressure_version": DEADLINE_PRESSURE_VERSION,
        "deadline_pressure": pressure,
        "hours_until_due": hours_until_due,
        "pressure_reasons": reasons,
        "pressure_summary": {
            "reason_count": len(reasons),
            "severity_counts": {
                "critical": sum(1 for r in reasons if r["severity"] == "critical"),
                "high": sum(1 for r in reasons if r["severity"] == "high"),
                "medium": sum(1 for r in reasons if r["severity"] == "medium"),
            },
            "source_package_gate": pkg_outcome,
            "source_authority_posture": ap.get("authority_package_posture"),
        },
    }


def _escalate(current: str, candidate: str) -> str:
    if _PRESSURE_ORDER.get(candidate, 99) < _PRESSURE_ORDER.get(current, 99):
        return candidate
    return current
