"""
C79 — End-to-end demo flow harness.

Deterministic harness that assembles canonical artifacts through
readiness/control-room/export/carry flows. Orchestrates existing
components only; introduces no new business rules.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

DEMO_HARNESS_VERSION = "e2e_demo_harness/v1"


def build_demo_fixture(bid_id: str = "demo-bid-1") -> Dict[str, Any]:
    """Stable deterministic demo fixture built from canonical shapes."""
    return {
        "bid_id": bid_id,
        "dossiers": [
            _fixture_dossier("demo-j1", "AcmeSubs", gate="CONDITIONAL",
                              risk="medium", posture="usable_with_caveats",
                              reliance="relied_upon_with_caveats"),
            _fixture_dossier("demo-j2", "BetaSubs", gate="HIGH_RISK",
                              risk="high", posture="requires_action",
                              reliance="decision_deferred"),
        ],
        "authority_entries": [
            {"topic_id": "auth-1", "description": "Clearing and Grubbing",
             "authority_source_type": "dot", "authority_posture": "required",
             "source_ref": {"spec_section": "2101"}},
            {"topic_id": "auth-2", "description": "Traffic Control",
             "authority_source_type": "sudas", "authority_posture": "conditional",
             "source_ref": {"spec_section": "TC-1"}},
        ],
        "scope_topics": [
            {"topic_id": "s-1", "description": "Clearing and Grubbing",
             "scope_class": "explicitly_included",
             "source_ref": {"normalized_row_id": "qr-p0-r0"}},
        ],
        "hours_until_due": 8.0,
    }


def run_e2e_demo(
    fixture: Optional[Dict[str, Any]] = None,
    carry_decision: str = "proceed_with_caveats",
    decided_by: Optional[str] = "demo_estimator",
) -> Dict[str, Any]:
    """
    Run the canonical end-to-end flow on a fixture. Returns a bundled
    artifact set: package overview, gate, authority reference/comparison/
    exposure/action packet/posture, vendor comparison, deadline pressure,
    priority queue, readiness snapshot, carry justification, and
    exports.
    """
    from .bid_package_overview import build_package_overview
    from .package_confidence import evaluate_package_confidence
    from .vendor_comparison import compare_vendors
    from .scope_authority import build_authority_reference
    from .scope_authority_comparison import compare_scope_vs_authority
    from .authority_exposure import build_authority_exposure
    from .authority_action_packet import build_authority_action_packet
    from .authority_package_posture import evaluate_authority_posture
    from .deadline_pressure import evaluate_deadline_pressure
    from .resolution_priority_queue import build_priority_queue
    from .bid_readiness_snapshot import build_readiness_snapshot
    from .bid_carry_justification import create_carry_justification
    from .export_packet_builder import (
        build_bid_readiness_export, build_final_carry_export,
        build_authority_action_export,
    )
    from .control_room_view_models import (
        build_package_overview_view, build_authority_action_view,
        build_bid_readiness_view,
    )

    fx = fixture or build_demo_fixture()
    bid_id = fx["bid_id"]
    dossiers = fx["dossiers"]

    package_overview = build_package_overview(bid_id, dossiers)
    package_gate = evaluate_package_confidence(package_overview)
    vendor_cmp = compare_vendors(dossiers)

    authority_reference = build_authority_reference(fx.get("authority_entries") or [])
    scope_interp = {"scope_topics": fx.get("scope_topics") or []}
    authority_cmp = compare_scope_vs_authority(authority_reference, scope_interpretation=scope_interp)
    authority_exp = build_authority_exposure(authority_cmp, authority_reference)
    authority_action = build_authority_action_packet(authority_exp,
                                                      authority_reference,
                                                      package_overview)
    authority_posture = evaluate_authority_posture(authority_exp, authority_action)

    deadline = evaluate_deadline_pressure(
        hours_until_due=fx.get("hours_until_due"),
        package_gate=package_gate,
        authority_posture=authority_posture,
        package_overview=package_overview,
    )

    priority_queue = build_priority_queue(
        package_overview=package_overview,
        authority_action_packet=authority_action,
        deadline_pressure=deadline,
        dossiers=dossiers,
    )

    readiness = build_readiness_snapshot(
        bid_id,
        package_overview=package_overview,
        package_gate=package_gate,
        authority_posture=authority_posture,
        deadline_pressure=deadline,
        priority_queue=priority_queue,
        vendor_comparison=vendor_cmp,
        authority_action_packet=authority_action,
    )

    carry = create_carry_justification(
        bid_id, carry_decision,
        package_gate=package_gate,
        authority_posture=authority_posture,
        authority_action_packet=authority_action,
        decided_by=decided_by,
        decided_at="2026-04-16T12:00:00",
    )

    exports = {
        "bid_readiness_export": build_bid_readiness_export(
            readiness, priority_queue, package_gate, authority_posture, deadline),
        "final_carry_export": build_final_carry_export(carry, readiness, authority_action),
        "authority_action_export": build_authority_action_export(
            authority_action, authority_posture, authority_reference),
    }

    view_models = {
        "package_overview_view": build_package_overview_view(package_overview, package_gate, vendor_cmp),
        "authority_action_view": build_authority_action_view(authority_action, authority_posture, authority_reference),
        "bid_readiness_view": build_bid_readiness_view(readiness, priority_queue),
    }

    return {
        "demo_harness_version": DEMO_HARNESS_VERSION,
        "bid_id": bid_id,
        "canonical_artifacts": {
            "package_overview": package_overview,
            "package_gate": package_gate,
            "vendor_comparison": vendor_cmp,
            "authority_reference": authority_reference,
            "authority_comparison": authority_cmp,
            "authority_exposure": authority_exp,
            "authority_action_packet": authority_action,
            "authority_posture": authority_posture,
            "deadline_pressure": deadline,
            "priority_queue": priority_queue,
            "readiness_snapshot": readiness,
            "carry_justification": carry,
        },
        "view_models": view_models,
        "exports": exports,
        "demo_summary": {
            "overall_readiness": readiness.get("overall_readiness"),
            "package_gate_outcome": package_gate.get("package_gate_outcome"),
            "authority_posture": authority_posture.get("authority_package_posture"),
            "deadline_pressure": deadline.get("deadline_pressure"),
            "carry_decision": carry.get("carry_decision"),
            "dossier_count": len(dossiers),
            "authority_topic_count": len(fx.get("authority_entries") or []),
            "priority_queue_items": (priority_queue.get("queue_summary") or {}).get("total_items", 0),
        },
    }


def _fixture_dossier(jid, vendor, gate, risk, posture, reliance):
    return {
        "dossier_version": "quote_dossier/v1",
        "job_id": jid,
        "vendor_name": vendor,
        "latest_gate": {"gate_outcome": gate, "reason_count": 2},
        "latest_risk": {"overall_risk_level": risk, "factor_count": 2, "blocking_count": 0},
        "decision_posture": posture,
        "readiness_status": "actionable",
        "open_clarifications": {"total_open": 2, "pending_send": 2, "sent": 0},
        "response_history_summary": {"total_responses": 0},
        "active_assumptions": [],
        "reliance_posture": {"clarify_before_reliance_count": 3, "carry_in_sub_quote_count": 5,
                              "block_quote_reliance_count": 0},
        "recommendation_summary": {"carry_in_sub_quote_count": 5, "carry_internally_count": 1,
                                    "hold_as_contingency_count": 0,
                                    "clarify_before_reliance_count": 3,
                                    "block_quote_reliance_count": 0},
        "comparability_posture": {"total_rows": 15, "comparable_matched": 5, "non_comparable": 10},
        "scope_gaps": {"not_addressed_count": 8, "ambiguous_count": 1},
        "evidence_status": {"unresolved_block_count": 0},
        "current_cycle": {"cycle_id": "cycle-0", "metrics": {}},
    }
