"""
C121 — End-to-end demo acceptance walkthrough.

Deterministic real-UI acceptance walkthrough that composes the actual
UI-facing endpoints and canonical services in the exact order a real
operator would traverse them. Uses only seeded scenarios + existing
adapters, command flows, downloads, and diagnostics.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

WALKTHROUGH_VERSION = "e2e_acceptance_walkthrough/v1"


def run_walkthrough(
    scenario_id: str = "proceed_with_caveats",
    *,
    repository: Any = None,
    operator_id: str = "walkthrough_operator",
) -> Dict[str, Any]:
    """Execute a real UI/backend walkthrough against a seeded scenario."""
    from .artifact_repository import ArtifactRepository
    from .seed_scenarios import run_scenario_e2e
    from .frontend_screen_adapters import (
        adapt_package_overview, adapt_quote_case, adapt_authority_action,
        adapt_bid_readiness, adapt_timeline, adapt_revision_inspection,
    )
    from .operator_command_flow import (
        execute_command, CommandReceiptLog, CMD_ACKNOWLEDGE_REVIEW,
        CMD_CAPTURE_NOTE, CMD_CARRY_ADVANCE, CMD_DOWNLOAD_REPORT,
    )
    from .report_download_flow import build_downloadable_bundle

    steps: List[Dict[str, Any]] = []

    def record(step: str, status: str, detail: Any = None):
        steps.append({"step": step, "status": status, "detail": detail})

    repo = repository or ArtifactRepository()
    e2e = run_scenario_e2e(scenario_id)
    bid_id = e2e["bid_id"]
    record("scenario_load", "ok", {"scenario_id": scenario_id, "bid_id": bid_id})

    # Persist the canonical artifacts the UI screens rely on.
    persist_order = (
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
    for artifact_type, src in persist_order:
        art = e2e["canonical_artifacts"].get(src)
        if not art:
            continue
        art = deepcopy(art)
        if artifact_type == "priority_queue":
            art["bid_id"] = bid_id
        repo.save(artifact_type, art,
                   metadata={"created_by": "walkthrough"})
    record("persist_canonical", "ok",
            {"records_after_seed": repo.repository_summary()["total_records"]})

    log = CommandReceiptLog()

    # --- Walkthrough stage 1: package overview ---------------------------
    po = adapt_package_overview(repo, bid_id)
    record("view.package_overview", "ok" if po["diagnostics"]
            ["package_overview_present"] else "fail",
            {"state_labels": po["state_labels"]})

    # --- Walkthrough stage 2: pick a quote (synthesize one dossier) ------
    dossier_job = "walkthrough-j1"
    dossier = {
        "dossier_version": "quote_dossier/v1",
        "job_id": dossier_job,
        "vendor_name": "WalkthroughVendor",
        "decision_posture": "usable_with_caveats",
        "readiness_status": "actionable",
        "latest_gate": {"gate_outcome": "CONDITIONAL"},
        "latest_risk": {"overall_risk_level": "medium"},
        "comparability_posture": {"total_rows": 12},
        "reliance_posture": {"carry_in_sub_quote_count": 10},
        "scope_gaps": {"not_addressed_count": 3},
        "evidence_status": {},
        "open_clarifications": {"total_open": 1, "pending_send": 1, "sent": 0},
        "response_history_summary": {},
        "active_assumptions": [],
        "recommendation_summary": {},
        "package_ref": {"bid_id": bid_id},
    }
    repo.save("quote_dossier", dossier,
               metadata={"created_by": "walkthrough"})
    qc = adapt_quote_case(repo, dossier_job)
    record("view.quote_case", "ok" if qc["diagnostics"]
            ["dossier_present"] else "fail",
            {"state_labels": qc["state_labels"]})

    # --- Walkthrough stage 3: authority action ---------------------------
    aa = adapt_authority_action(repo, bid_id)
    record("view.authority_action",
            "ok" if aa["diagnostics"]["authority_action_present"] else "fail",
            {"state_labels": aa["state_labels"]})

    # --- Walkthrough stage 4: bid readiness initial view -----------------
    before_rd = adapt_bid_readiness(repo, bid_id)
    record("view.bid_readiness.initial",
            "ok" if before_rd["diagnostics"]["readiness_present"] else "fail",
            {"state_labels": before_rd["state_labels"]})

    # --- Walkthrough stage 5: operator action → ack review ---------------
    ack = execute_command(
        repo, CMD_ACKNOWLEDGE_REVIEW,
        {"bid_id": bid_id, "note": "walkthrough ack"},
        issued_by=operator_id,
        issued_at="2026-04-17T02:00:00",
        log=log,
    )
    record("command.acknowledge_review", ack["status"],
            {"command_id": ack["command_id"],
             "new_record_id": (ack.get("result") or {}).get("new_record_id")})

    # --- Walkthrough stage 6: note capture + carry advance ---------------
    note = execute_command(
        repo, CMD_CAPTURE_NOTE,
        {"bid_id": bid_id, "note": "walkthrough operator note",
          "tag": "walkthrough"},
        issued_by=operator_id,
        issued_at="2026-04-17T02:01:00",
        log=log,
    )
    record("command.capture_note", note["status"])

    carry1 = execute_command(
        repo, CMD_CARRY_ADVANCE,
        {"bid_id": bid_id, "next_state": "under_review",
          "note": "walkthrough under review"},
        issued_by=operator_id,
        issued_at="2026-04-17T02:02:00",
        log=log,
    )
    record("command.carry_advance.under_review", carry1["status"])

    carry2 = execute_command(
        repo, CMD_CARRY_ADVANCE,
        {"bid_id": bid_id, "next_state": "approved",
          "note": "walkthrough approved"},
        issued_by=operator_id,
        issued_at="2026-04-17T02:03:00",
        log=log,
    )
    record("command.carry_advance.approved", carry2["status"])

    # --- Walkthrough stage 7: refresh readiness view after actions -------
    after_rd = adapt_bid_readiness(repo, bid_id)
    ok_refresh = (
        after_rd["state_labels"].get("carry_progression_state") == "approved"
    )
    record("view.bid_readiness.after", "ok" if ok_refresh else "fail",
            {"state_labels": after_rd["state_labels"]})

    # --- Walkthrough stage 8: timeline + diff ----------------------------
    tl = adapt_timeline(repo, bid_id=bid_id)
    record("view.timeline",
            "ok" if (tl["body"].get("kind_timelines")) else "fail",
            {"kind_count": tl["state_labels"].get("kind_count")})

    diff = adapt_revision_inspection(
        repo, "bid_readiness_snapshot", bid_id=bid_id)
    record("view.diff.bid_readiness",
            "ok" if diff["state_labels"]["history_length"] >= 2 else "fail",
            {"history_length": diff["state_labels"]["history_length"]})

    # --- Walkthrough stage 9: report downloads ---------------------------
    bundle = build_downloadable_bundle(repo, bid_id=bid_id,
                                         output_format="markdown")
    record("download.bundle.markdown",
            bundle["download_status"],
            {"count": bundle["download_count"]})

    single = execute_command(
        repo, CMD_DOWNLOAD_REPORT,
        {"report_kind": "bid_readiness_report",
          "bid_id": bid_id, "format": "json"},
        issued_by=operator_id,
        issued_at="2026-04-17T02:04:00",
        log=log,
    )
    record("download.command.bid_readiness_json", single["status"],
            {"filename": (single.get("result") or {}).get("filename")})

    all_ok = all(s["status"] == "ok" for s in steps)

    return {
        "walkthrough_version": WALKTHROUGH_VERSION,
        "scenario_id": scenario_id,
        "bid_id": bid_id,
        "all_stages_ok": all_ok,
        "steps": steps,
        "command_receipt_summary": log.summary(),
        "readiness_state_after":
            after_rd["state_labels"].get("carry_progression_state"),
        "history_counts": {
            "bid_readiness_snapshot": len(
                repo.history("bid_readiness_snapshot", bid_id=bid_id)),
            "bid_carry_justification": len(
                repo.history("bid_carry_justification", bid_id=bid_id)),
        },
        "repository_summary": repo.repository_summary(),
    }


def list_walkthrough_scenarios() -> List[str]:
    from .seed_scenarios import list_scenarios
    return list_scenarios()
