"""
C85 — Demo seed / reference scenario pack.

Stable seeded scenarios for deterministic demos and tests:
    1. straightforward_usable
    2. high_risk_incomplete
    3. blocked_authority
    4. proceed_with_caveats
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict

SCENARIO_STRAIGHTFORWARD = "straightforward_usable"
SCENARIO_HIGH_RISK_INCOMPLETE = "high_risk_incomplete"
SCENARIO_BLOCKED_AUTHORITY = "blocked_authority"
SCENARIO_PROCEED_WITH_CAVEATS = "proceed_with_caveats"

_ALL_SCENARIOS = (
    SCENARIO_STRAIGHTFORWARD, SCENARIO_HIGH_RISK_INCOMPLETE,
    SCENARIO_BLOCKED_AUTHORITY, SCENARIO_PROCEED_WITH_CAVEATS,
)


def list_scenarios():
    return list(_ALL_SCENARIOS)


def build_scenario(scenario_id: str, bid_id: str = None) -> Dict[str, Any]:
    if scenario_id == SCENARIO_STRAIGHTFORWARD:
        return _straightforward(bid_id or "seed-straightforward")
    if scenario_id == SCENARIO_HIGH_RISK_INCOMPLETE:
        return _high_risk(bid_id or "seed-high-risk")
    if scenario_id == SCENARIO_BLOCKED_AUTHORITY:
        return _blocked_authority(bid_id or "seed-blocked-auth")
    if scenario_id == SCENARIO_PROCEED_WITH_CAVEATS:
        return _proceed_caveats(bid_id or "seed-caveats")
    raise ValueError(f"unknown_scenario_id: {scenario_id}")


def run_scenario_e2e(scenario_id: str, bid_id: str = None) -> Dict[str, Any]:
    from .e2e_demo_harness import run_e2e_demo
    fx = build_scenario(scenario_id, bid_id=bid_id)
    decision = _carry_decision_for(scenario_id)
    return run_e2e_demo(fx, carry_decision=decision, decided_by="seed_estimator")


def _carry_decision_for(scenario_id: str) -> str:
    if scenario_id == SCENARIO_STRAIGHTFORWARD:
        return "proceed_to_bid"
    if scenario_id == SCENARIO_HIGH_RISK_INCOMPLETE:
        return "hold_pending_resolution"
    if scenario_id == SCENARIO_BLOCKED_AUTHORITY:
        return "withdraw_from_bid"
    return "proceed_with_caveats"


def _dossier(jid, vendor, gate, risk, posture, reliance, not_addr=0, open_cl=0,
              carry_int=0, unres_ev=0, matched=15, non_comp=0):
    return {
        "dossier_version": "quote_dossier/v1",
        "job_id": jid,
        "vendor_name": vendor,
        "latest_gate": {"gate_outcome": gate, "reason_count": 0},
        "latest_risk": {"overall_risk_level": risk, "factor_count": 0, "blocking_count": 0},
        "decision_posture": posture,
        "readiness_status": "actionable",
        "open_clarifications": {"total_open": open_cl, "pending_send": open_cl, "sent": 0},
        "response_history_summary": {"total_responses": 0},
        "active_assumptions": [],
        "reliance_posture": {"clarify_before_reliance_count": 0, "carry_in_sub_quote_count": matched,
                              "block_quote_reliance_count": 0},
        "recommendation_summary": {"carry_in_sub_quote_count": matched,
                                    "carry_internally_count": carry_int,
                                    "hold_as_contingency_count": 0,
                                    "clarify_before_reliance_count": 0,
                                    "block_quote_reliance_count": 0},
        "comparability_posture": {"total_rows": matched + non_comp,
                                   "comparable_matched": matched, "non_comparable": non_comp},
        "scope_gaps": {"not_addressed_count": not_addr, "ambiguous_count": 0},
        "evidence_status": {"unresolved_block_count": unres_ev},
        "current_cycle": {"cycle_id": "cycle-0", "metrics": {}},
    }


def _straightforward(bid_id: str) -> Dict[str, Any]:
    return {
        "bid_id": bid_id,
        "dossiers": [
            _dossier("s1-j1", "AcmeClean", gate="SAFE", risk="low",
                     posture="ready_for_use", reliance="relied_upon",
                     matched=15, non_comp=0, not_addr=0, open_cl=0, carry_int=0),
        ],
        "authority_entries": [
            {"topic_id": "auth-1", "description": "Clearing and Grubbing",
             "authority_source_type": "dot", "authority_posture": "required",
             "source_ref": {"spec_section": "2101"}},
        ],
        "scope_topics": [
            {"topic_id": "s-1", "description": "Clearing and Grubbing",
             "scope_class": "explicitly_included",
             "source_ref": {"normalized_row_id": "qr-p0-r0"}},
        ],
        "hours_until_due": 48.0,
    }


def _high_risk(bid_id: str) -> Dict[str, Any]:
    return {
        "bid_id": bid_id,
        "dossiers": [
            _dossier("hr-j1", "RiskySub", gate="HIGH_RISK", risk="high",
                     posture="requires_action", reliance="decision_deferred",
                     matched=3, non_comp=12, not_addr=20, open_cl=8, carry_int=5,
                     unres_ev=3),
            _dossier("hr-j2", "WeakSub", gate="HIGH_RISK", risk="high",
                     posture="requires_action", reliance="decision_deferred",
                     matched=5, non_comp=10, not_addr=15, open_cl=5, carry_int=3),
        ],
        "authority_entries": [
            {"topic_id": "auth-1", "description": "Traffic Control",
             "authority_source_type": "dot", "authority_posture": "required",
             "source_ref": {"spec_section": "2528"}},
        ],
        "scope_topics": [],
        "hours_until_due": 12.0,
    }


def _blocked_authority(bid_id: str) -> Dict[str, Any]:
    return {
        "bid_id": bid_id,
        "dossiers": [
            _dossier("ba-j1", "OkSub", gate="CONDITIONAL", risk="medium",
                     posture="usable_with_caveats", reliance="relied_upon_with_caveats",
                     matched=10, non_comp=5, not_addr=3),
        ],
        "authority_entries": [
            {"topic_id": "auth-1", "description": "Erosion Control",
             "authority_source_type": "dot", "authority_posture": "required",
             "source_ref": {"spec_section": "2602"}},
            {"topic_id": "auth-2", "description": "Storm Sewer",
             "authority_source_type": "sudas", "authority_posture": "required",
             "source_ref": {"spec_section": "6010"}},
        ],
        "scope_topics": [],  # nothing addresses required authority topics
        "hours_until_due": 24.0,
    }


def _proceed_caveats(bid_id: str) -> Dict[str, Any]:
    return {
        "bid_id": bid_id,
        "dossiers": [
            _dossier("pc-j1", "GoodishSub", gate="CONDITIONAL", risk="medium",
                     posture="usable_with_caveats", reliance="relied_upon_with_caveats",
                     matched=12, non_comp=3, not_addr=5, open_cl=2, carry_int=1),
        ],
        "authority_entries": [
            {"topic_id": "auth-1", "description": "Clearing and Grubbing",
             "authority_source_type": "dot", "authority_posture": "required",
             "source_ref": {"spec_section": "2101"}},
            {"topic_id": "auth-2", "description": "Traffic Control",
             "authority_source_type": "dot", "authority_posture": "conditional",
             "source_ref": {"spec_section": "2528"}},
        ],
        "scope_topics": [
            {"topic_id": "s-1", "description": "Clearing and Grubbing",
             "scope_class": "explicitly_included",
             "source_ref": {"normalized_row_id": "qr-p0-r0"}},
            {"topic_id": "s-2", "description": "Traffic Control",
             "scope_class": "implicitly_included",
             "source_ref": {"normalized_row_id": "qr-p0-r1"}},
        ],
        "hours_until_due": 36.0,
    }
