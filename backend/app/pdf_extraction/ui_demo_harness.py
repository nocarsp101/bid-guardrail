"""
C109 — End-to-end UI / operator demo harness.

Demonstrable app-facing flow that loads a seeded scenario, fetches
package overview / readiness / authority / timeline payloads,
performs one or more operator workflow actions, generates and
downloads reports, and confirms resulting revision / state effects.
Canonical artifacts + existing services only — reproducible,
append-only, fully traceable.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

UI_DEMO_VERSION = "ui_demo_harness/v1"


def run_ui_demo(
    scenario_id: str = "proceed_with_caveats",
    *,
    repository: Any = None,
    operator_id: str = "ui_demo_operator",
) -> Dict[str, Any]:
    """Run a full UI-facing operator demo and capture every stage."""
    from .bootstrap_harness import bootstrap
    from .runtime_config import default_config
    from .frontend_screen_adapters import (
        adapt_quote_case, adapt_package_overview, adapt_authority_action,
        adapt_bid_readiness, adapt_timeline, adapt_revision_inspection,
    )
    from .operator_command_flow import (
        execute_command, CommandReceiptLog,
        CMD_ACKNOWLEDGE_REVIEW, CMD_CARRY_ADVANCE, CMD_CAPTURE_NOTE,
        CMD_DOWNLOAD_REPORT,
    )
    from .report_download_flow import (
        build_downloadable_bundle, REPORT_BID_READINESS,
        REPORT_AUTHORITY_ACTION, REPORT_FINAL_CARRY,
    )

    # Bootstrap with demo seeding enabled against a scoped scenario list.
    cfg = default_config()
    cfg["feature_flags"]["seed_scenarios_enabled"] = True
    boot = bootstrap(cfg, seed_scenarios=[scenario_id])
    if boot["readiness"] != "ready":
        return {
            "ui_demo_version": UI_DEMO_VERSION,
            "all_stages_ok": False,
            "reason": "bootstrap_failed",
            "bootstrap": {k: v for k, v in boot.items()
                           if k not in ("repository", "adapter")},
        }
    repo = repository or boot["repository"]
    # Determine the bid_id from the seed saves.
    bid_id = None
    for save in (boot.get("seed_result") or {}).get("scenarios", []):
        pass  # seed_result summary doesn't store bid_id per save
    for rec in repo.all_records():
        art = (rec.get("envelope") or {}).get("artifact") or {}
        b = art.get("bid_id") or (art.get("package_ref") or {}).get("bid_id")
        if b:
            bid_id = b
            break
    if not bid_id:
        return {
            "ui_demo_version": UI_DEMO_VERSION,
            "all_stages_ok": False,
            "reason": "bid_id_not_found",
        }

    # Pick a job_id if any quote_dossier records exist (scenarios don't
    # currently persist quote_dossier, but we still attempt).
    job_id = None
    for rec in repo.all_records():
        if rec.get("artifact_type") == "quote_dossier":
            art = (rec.get("envelope") or {}).get("artifact") or {}
            job_id = art.get("job_id")
            break

    # Screen adapters.
    screens = {
        "package_overview": adapt_package_overview(repo, bid_id),
        "authority_action": adapt_authority_action(repo, bid_id),
        "bid_readiness": adapt_bid_readiness(repo, bid_id),
        "timeline": adapt_timeline(repo, bid_id=bid_id),
    }
    if job_id:
        screens["quote_case"] = adapt_quote_case(repo, job_id)

    # Command flow with its own receipt log for this demo.
    log = CommandReceiptLog()

    ack = execute_command(repo, CMD_ACKNOWLEDGE_REVIEW,
                           {"bid_id": bid_id, "note": "UI demo ack"},
                           issued_by=operator_id,
                           issued_at="2026-04-17T01:00:00",
                           log=log)
    carry_1 = execute_command(repo, CMD_CARRY_ADVANCE,
                                {"bid_id": bid_id,
                                 "next_state": "under_review",
                                 "note": "Under review from UI demo"},
                                issued_by=operator_id,
                                issued_at="2026-04-17T01:01:00",
                                log=log)
    note = execute_command(repo, CMD_CAPTURE_NOTE,
                             {"bid_id": bid_id,
                              "note": "UI demo operator note",
                              "tag": "ui_demo"},
                             issued_by=operator_id,
                             issued_at="2026-04-17T01:02:00",
                             log=log)
    carry_2 = execute_command(repo, CMD_CARRY_ADVANCE,
                                {"bid_id": bid_id,
                                 "next_state": "approved",
                                 "note": "Approved via UI demo"},
                                issued_by=operator_id,
                                issued_at="2026-04-17T01:03:00",
                                log=log)

    # Revision inspection after the commands.
    revision = adapt_revision_inspection(repo, "bid_readiness_snapshot",
                                           bid_id=bid_id)

    # Report downloads.
    downloads = []
    for kind in (REPORT_BID_READINESS, REPORT_AUTHORITY_ACTION,
                  REPORT_FINAL_CARRY):
        d = execute_command(repo, CMD_DOWNLOAD_REPORT,
                              {"report_kind": kind, "bid_id": bid_id,
                               "format": "json"},
                              issued_by=operator_id,
                              issued_at="2026-04-17T01:04:00",
                              log=log)
        downloads.append(d)
    bundle = build_downloadable_bundle(repo, bid_id=bid_id,
                                        output_format="markdown")

    revisions_after = repo.history("bid_readiness_snapshot", bid_id=bid_id)
    carry_after = repo.history("bid_carry_justification", bid_id=bid_id)

    all_ok = (ack["status"] == "ok" and carry_1["status"] == "ok"
               and note["status"] == "ok" and carry_2["status"] == "ok"
               and all(d["status"] == "ok" for d in downloads))

    return {
        "ui_demo_version": UI_DEMO_VERSION,
        "scenario_id": scenario_id,
        "bid_id": bid_id,
        "job_id": job_id,
        "all_stages_ok": all_ok,
        "bootstrap_readiness": boot["readiness"],
        "screens": {name: _screen_snapshot(s) for name, s in screens.items()},
        "command_receipts": [r for r in log.all_receipts()],
        "revision_inspection": revision,
        "download_bundle": {
            "status": bundle.get("download_status"),
            "count": bundle.get("download_count"),
        },
        "history_counts": {
            "bid_readiness_snapshot": len(revisions_after),
            "bid_carry_justification": len(carry_after),
        },
        "final_carry_state":
            (carry_after[-1].get("envelope") or {}).get("artifact", {})
             .get("carry_progression_state") if carry_after else None,
        "receipt_summary": log.summary(),
    }


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _screen_snapshot(screen: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "screen_id": screen.get("screen_id"),
        "identity": deepcopy(screen.get("identity") or {}),
        "state_labels": deepcopy(screen.get("state_labels") or {}),
        "source_ref_count": len(screen.get("source_refs") or []),
        "diagnostics": deepcopy(screen.get("diagnostics") or {}),
    }
