"""
C103 — End-to-end product demo flow.

Demonstrable end-to-end product flow using seeded scenarios across
readiness, authority action, timeline/diff, reports, and final carry
decision progression. Reuses canonical artifacts and existing
endpoints only. Reproducible, append-only, fully traceable.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

PRODUCT_DEMO_VERSION = "product_demo_flow/v1"


def run_product_demo(
    scenario_id: str = "proceed_with_caveats",
    repository: Any = None,
) -> Dict[str, Any]:
    """Run a full product demo against a seeded scenario."""
    from .artifact_repository import ArtifactRepository
    from .seed_scenarios import run_scenario_e2e
    from .frontend_reference_integration import ControlRoomReferenceClient
    from .control_room_assembly import assemble_timeline_payload
    from .revision_diff import diff_revisions, diff_lineage
    from .render_reports import (
        build_bid_readiness_report, build_authority_action_report,
        build_final_carry_report,
    )
    from .report_delivery import deliver_report, deliver_all_for_bid
    from .operator_workflow_actions import (
        acknowledge_review, advance_carry_decision,
        CARRY_UNDER_REVIEW, CARRY_APPROVED,
    )

    repo = repository or ArtifactRepository()
    e2e = run_scenario_e2e(scenario_id)
    bid_id = e2e["bid_id"]

    # --- Stage 1: seed canonical artifacts --------------------------------
    seed_saves: List[Dict[str, Any]] = []
    for artifact_type, source_key in _PERSIST_ORDER:
        art = e2e["canonical_artifacts"].get(source_key)
        if art:
            rec = repo.save(artifact_type, deepcopy(art),
                             metadata={"created_by": "product_demo",
                                       "created_at": "2026-04-17T00:00:00"})
            seed_saves.append({"artifact_type": artifact_type,
                                "record_id": rec["record_id"],
                                "revision_sequence": rec["revision_sequence"]})

    client = ControlRoomReferenceClient(repo)

    # --- Stage 2: bid overview bundle ------------------------------------
    overview = client.bid_overview_bundle(bid_id)

    # --- Stage 3: operator acknowledges readiness -------------------------
    ack = acknowledge_review(
        repo, bid_id=bid_id,
        acknowledged_by="product_demo_operator",
        acknowledged_at="2026-04-17T00:01:00",
        note="Readiness snapshot reviewed",
    )

    # --- Stage 4: advance carry decision through closed transitions ------
    carry_advance_1 = advance_carry_decision(
        repo, bid_id=bid_id, next_state=CARRY_UNDER_REVIEW,
        advanced_by="product_demo_reviewer",
        advanced_at="2026-04-17T00:02:00",
        note="Under review by estimator",
    )
    carry_advance_2 = advance_carry_decision(
        repo, bid_id=bid_id, next_state=CARRY_APPROVED,
        advanced_by="product_demo_office",
        advanced_at="2026-04-17T00:03:00",
        note="Office approval granted",
    )

    # --- Stage 5: timeline + diffs ----------------------------------------
    timeline = assemble_timeline_payload(repo, bid_id=bid_id)
    readiness_history = repo.history("bid_readiness_snapshot", bid_id=bid_id)
    carry_history = repo.history("bid_carry_justification", bid_id=bid_id)
    readiness_lineage_diffs = diff_lineage(readiness_history)
    carry_lineage_diffs = diff_lineage(carry_history)

    # --- Stage 6: reports + delivery --------------------------------------
    reports = {
        "bid_readiness": build_bid_readiness_report(repo, bid_id),
        "authority_action": build_authority_action_report(repo, bid_id),
        "final_carry": build_final_carry_report(repo, bid_id),
    }
    delivered_json = {
        k: deliver_report(v, output_format="json") for k, v in reports.items()
    }
    delivered_markdown_batch = deliver_all_for_bid(repo, bid_id,
                                                     output_format="markdown")

    flow_steps: List[Dict[str, Any]] = [
        {"step": "seed", "count": len(seed_saves)},
        {"step": "bid_overview", "ok":
            overview["package_overview"]["assembly_diagnostics"]
              ["package_overview_present"]},
        {"step": "acknowledge_review", "status": ack.get("status"),
          "revision": ack.get("revision_sequence")},
        {"step": "carry_advance_under_review",
          "status": carry_advance_1.get("status"),
          "to_state": carry_advance_1.get("to_state")},
        {"step": "carry_advance_approved",
          "status": carry_advance_2.get("status"),
          "to_state": carry_advance_2.get("to_state")},
        {"step": "timeline",
          "kind_count": len(timeline.get("kind_timelines") or [])},
        {"step": "readiness_diffs", "count": len(readiness_lineage_diffs)},
        {"step": "carry_diffs", "count": len(carry_lineage_diffs)},
        {"step": "reports", "count": len(reports)},
        {"step": "deliveries_json",
          "count": sum(1 for d in delivered_json.values()
                        if d.get("delivery_status") == "ok")},
        {"step": "deliveries_markdown_batch",
          "count": delivered_markdown_batch.get("report_count", 0)},
    ]
    all_ok = all(
        s.get("ok") is not False and s.get("status") != "record_not_found"
        for s in flow_steps
    )

    return {
        "product_demo_version": PRODUCT_DEMO_VERSION,
        "scenario_id": scenario_id,
        "bid_id": bid_id,
        "all_stages_ok": all_ok,
        "stages": flow_steps,
        "seed_saves": seed_saves,
        "bid_overview": {
            "package_overview_present":
              overview["package_overview"]["assembly_diagnostics"]
                ["package_overview_present"],
            "bid_readiness_present":
              overview["bid_readiness"]["assembly_diagnostics"]
                ["readiness_present"],
            "authority_action_present":
              overview["authority_action"]["assembly_diagnostics"]
                ["authority_action_present"],
            "timeline_kind_count":
              len(overview["timeline"].get("kind_timelines") or []),
        },
        "operator_actions": {
            "acknowledge_review": ack,
            "carry_advance_under_review": carry_advance_1,
            "carry_advance_approved": carry_advance_2,
        },
        "history_counts": {
            "readiness": len(readiness_history),
            "carry": len(carry_history),
        },
        "diff_counts": {
            "readiness_lineage": len(readiness_lineage_diffs),
            "carry_lineage": len(carry_lineage_diffs),
        },
        "report_kinds": sorted(reports.keys()),
        "delivery_counts": {
            "json": sum(1 for d in delivered_json.values()
                         if d.get("delivery_status") == "ok"),
            "markdown_batch": delivered_markdown_batch.get("report_count", 0),
        },
        "repository_summary": repo.repository_summary(),
    }


_PERSIST_ORDER = (
    ("package_overview", "package_overview"),
    ("vendor_comparison", "vendor_comparison"),
    ("authority_reference", "authority_reference"),
    ("authority_comparison", "authority_comparison"),
    ("authority_exposure", "authority_exposure"),
    ("authority_action_packet", "authority_action_packet"),
    ("authority_posture", "authority_posture"),
    ("deadline_pressure", "deadline_pressure"),
    ("priority_queue", "priority_queue"),
    ("bid_readiness_snapshot", "readiness_snapshot"),
    ("bid_carry_justification", "carry_justification"),
)
