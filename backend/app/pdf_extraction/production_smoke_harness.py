"""
C91 — Production demo / smoke harness.

Deterministic smoke harness that seeds scenarios through the
repository / API layer and exercises latest / history / control-room /
export / timeline flows. Introduces no new business rules; only
orchestrates existing canonical components to prove round-trip
behaviour.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

SMOKE_HARNESS_VERSION = "production_smoke_harness/v1"


def run_smoke(
    scenario_ids: Optional[List[str]] = None,
    repository: Any = None,
) -> Dict[str, Any]:
    """Run the production smoke harness across one or more seed scenarios."""
    from .artifact_repository import get_default_repository
    from .seed_scenarios import list_scenarios, run_scenario_e2e
    from .control_room_assembly import (
        assemble_quote_case_payload, assemble_package_overview_payload,
        assemble_authority_action_payload, assemble_bid_readiness_payload,
        assemble_timeline_payload,
    )
    from .export_orchestration import (
        generate_bid_readiness_export, generate_authority_action_export,
        generate_final_carry_export,
    )
    from .revision_diff import diff_revisions, diff_lineage

    repo = repository or get_default_repository()
    scenarios = scenario_ids or list_scenarios()

    steps: List[Dict[str, Any]] = []
    seed_results: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for sid in scenarios:
        try:
            e2e = run_scenario_e2e(sid)
            bid_id = e2e["bid_id"]
            arts = e2e["canonical_artifacts"]

            # Persist canonical artifacts into the repository.
            saves: List[Dict[str, Any]] = []
            for artifact_type, artifact in _persist_order(arts):
                rec = repo.save(artifact_type, artifact,
                                 metadata={"created_by": "smoke_harness",
                                           "created_at": "2026-04-15T00:00:00"})
                saves.append({"artifact_type": artifact_type,
                              "record_id": rec["record_id"],
                              "revision_sequence": rec["revision_sequence"]})

            # Second revision of readiness + carry for diff coverage.
            readiness_v2 = deepcopy(arts["readiness_snapshot"])
            readiness_v2["smoke_rev"] = 2
            readiness_rec2 = repo.save("bid_readiness_snapshot", readiness_v2,
                                        metadata={"created_by": "smoke_harness",
                                                  "created_at": "2026-04-15T00:01:00"})

            # Assemble control-room payloads.
            po_payload = assemble_package_overview_payload(repo, bid_id)
            auth_payload = assemble_authority_action_payload(repo, bid_id)
            rd_payload = assemble_bid_readiness_payload(repo, bid_id)
            tl_payload = assemble_timeline_payload(repo, bid_id=bid_id)
            qc_payloads = [assemble_quote_case_payload(repo, d["job_id"])
                            for d in _iter_dossier_job_ids(e2e)]

            # Export generation.
            exports = {
                "bid_readiness": generate_bid_readiness_export(repo, bid_id),
                "authority_action": generate_authority_action_export(repo, bid_id),
                "final_carry": generate_final_carry_export(repo, bid_id),
            }

            # Revision diffs.
            readiness_history = repo.history("bid_readiness_snapshot", bid_id=bid_id)
            readiness_diff = None
            if len(readiness_history) >= 2:
                readiness_diff = diff_revisions(readiness_history[-2], readiness_history[-1])
            lineage_diffs = diff_lineage(readiness_history)

            seed_results.append({
                "scenario_id": sid,
                "bid_id": bid_id,
                "seed_saves": saves,
                "second_revision_record_id": readiness_rec2["record_id"],
                "control_room": {
                    "package_overview_present":
                        po_payload["assembly_diagnostics"]["package_overview_present"],
                    "authority_action_present":
                        auth_payload["assembly_diagnostics"]["authority_action_present"],
                    "bid_readiness_present":
                        rd_payload["assembly_diagnostics"]["readiness_present"],
                    "timeline_kind_count": len(tl_payload.get("kind_timelines") or []),
                    "quote_case_count": len(qc_payloads),
                },
                "export_statuses": {
                    k: bool(v.get("export") and v.get("source_records") is not None)
                    for k, v in exports.items()
                },
                "revision_diff": readiness_diff,
                "lineage_diff_count": len(lineage_diffs),
            })
            steps.append({"scenario_id": sid, "status": "ok"})
        except Exception as exc:  # fail-closed: surface, never swallow
            errors.append({"scenario_id": sid, "error": repr(exc)})
            steps.append({"scenario_id": sid, "status": "error",
                           "error": repr(exc)})

    summary = {
        "scenarios_run": len(scenarios),
        "scenarios_ok": sum(1 for s in steps if s["status"] == "ok"),
        "scenarios_failed": sum(1 for s in steps if s["status"] == "error"),
        "total_records_in_repo": repo.repository_summary()["total_records"],
    }
    return {
        "smoke_harness_version": SMOKE_HARNESS_VERSION,
        "steps": steps,
        "scenario_results": seed_results,
        "errors": errors,
        "summary": summary,
    }


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


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


def _persist_order(arts: Dict[str, Any]):
    out: List = []
    for artifact_type, source_key in _PERSIST_ORDER:
        art = arts.get(source_key)
        if art:
            out.append((artifact_type, deepcopy(art)))
    return out


def _iter_dossier_job_ids(e2e: Dict[str, Any]):
    # The demo harness does not persist per-dossier artifacts, so we
    # synthesize job-id keys from the fixture's canonical summary.
    summary = e2e.get("demo_summary") or {}
    job_count = summary.get("dossier_count") or 0
    out = []
    for i in range(job_count):
        out.append({"job_id": f"smoke-j{i}"})
    return out
