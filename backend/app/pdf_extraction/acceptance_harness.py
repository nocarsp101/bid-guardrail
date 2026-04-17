"""
C97 — Full operational acceptance harness.

Broad deterministic acceptance harness that exercises authorization,
scope checks, writes (idempotent), latest/history retrieval,
timeline/diff generation, control-room assembly, exports, readiness /
carry flows, and backup/restore behavior end-to-end. Seeded scenarios
+ canonical artifacts only. Reproducible, append-only, fully
traceable.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

ACCEPTANCE_VERSION = "acceptance_harness/v1"


def run_acceptance(
    scenario_ids: Optional[List[str]] = None,
    repository: Any = None,
) -> Dict[str, Any]:
    from .artifact_repository import ArtifactRepository
    from .seed_scenarios import list_scenarios, run_scenario_e2e
    from .production_smoke_harness import run_smoke
    from .authorization import (
        authorize, ROLE_ESTIMATOR, ROLE_ADMIN, ROLE_GUEST,
        ACTION_SAVE_ARTIFACT, ACTION_READ_ARTIFACT, ACTION_RESET_REPOSITORY,
        ACTION_BACKUP, ACTION_RESTORE, ACTION_ADMIN_DIAGNOSTICS,
        ACTION_UI_INTEGRATION,
    )
    from .scope_guardrails import check_scope
    from .idempotency import IdempotencyStore, idempotent_save_artifact
    from .backup_restore import (
        create_snapshot, validate_snapshot, restore_snapshot,
    )
    from .revision_diff import diff_revisions, diff_lineage
    from .control_room_assembly import (
        assemble_quote_case_payload, assemble_package_overview_payload,
        assemble_authority_action_payload, assemble_bid_readiness_payload,
        assemble_timeline_payload,
    )
    from .export_orchestration import (
        generate_bid_readiness_export, generate_authority_action_export,
        generate_final_carry_export,
    )
    from .render_reports import (
        build_estimator_review_report, build_authority_action_report,
        build_bid_readiness_report, build_final_carry_report,
    )
    from .admin_diagnostics import collect_diagnostics

    repo = repository or ArtifactRepository()
    scenarios = scenario_ids or list_scenarios()
    idem_store = IdempotencyStore()
    steps: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    def _record(step_id: str, status: str, detail: Any = None):
        steps.append({"step": step_id, "status": status,
                       "detail": detail})
        if status != "ok":
            failures.append(steps[-1])

    # --- authorization matrix ---------------------------------------------
    auth_cases = [
        (ROLE_ESTIMATOR, ACTION_SAVE_ARTIFACT, True),
        (ROLE_ESTIMATOR, ACTION_RESET_REPOSITORY, False),
        (ROLE_ADMIN, ACTION_RESET_REPOSITORY, True),
        (ROLE_GUEST, ACTION_READ_ARTIFACT, False),
        (ROLE_GUEST, ACTION_UI_INTEGRATION, True),
    ]
    auth_results = []
    for role, act, expected in auth_cases:
        decision = authorize(role, act)
        auth_results.append({"role": role, "action": act,
                              "allowed": decision["allowed"],
                              "expected": expected})
        if decision["allowed"] != expected:
            _record("authorization_matrix", "mismatch",
                     {"role": role, "action": act,
                      "expected": expected, "allowed": decision["allowed"]})
    else:
        if all(a["allowed"] == a["expected"] for a in auth_results):
            _record("authorization_matrix", "ok",
                     {"case_count": len(auth_results)})

    # --- per-scenario end-to-end ------------------------------------------
    scenario_results: List[Dict[str, Any]] = []
    for sid in scenarios:
        try:
            e2e = run_scenario_e2e(sid)
            bid_id = e2e["bid_id"]
            arts = e2e["canonical_artifacts"]

            # Idempotent save of each canonical artifact.
            idem_keys: List[Dict[str, Any]] = []
            for artifact_type, source_key in _PERSIST_ORDER:
                art = arts.get(source_key)
                if not art:
                    continue
                k = f"accept:{sid}:{artifact_type}"
                env = idempotent_save_artifact(repo, k, artifact_type,
                                                deepcopy(art),
                                                metadata={"created_by": "acceptance"},
                                                store=idem_store)
                idem_keys.append({"key": k, "status": env["status"]})

            # Idempotent replay for one artifact; must return replay_hit.
            replay_key = f"accept:{sid}:bid_readiness_snapshot"
            replay_env = idempotent_save_artifact(
                repo, replay_key, "bid_readiness_snapshot",
                deepcopy(arts["readiness_snapshot"]),
                metadata={"created_by": "acceptance"}, store=idem_store)
            if replay_env["status"] != "replay_hit":
                _record(f"idempotency.replay.{sid}", "not_replay",
                         {"status": replay_env["status"]})

            # Force a new revision of readiness for diff / timeline coverage.
            readiness_v2 = deepcopy(arts["readiness_snapshot"])
            readiness_v2["acceptance_rev"] = 2
            repo.save("bid_readiness_snapshot", readiness_v2,
                       metadata={"created_by": "acceptance"})

            # Scope check against a known record.
            rs_latest = repo.latest("bid_readiness_snapshot", bid_id=bid_id)
            scope = check_scope(rs_latest, bid_id=bid_id)
            if not scope["ok"]:
                _record(f"scope_check.{sid}", "fail", scope)

            # Retrieval.
            latest = repo.latest("bid_readiness_snapshot", bid_id=bid_id)
            history = repo.history("bid_readiness_snapshot", bid_id=bid_id)
            if latest is None or len(history) < 2:
                _record(f"retrieval.{sid}", "missing",
                         {"latest": bool(latest),
                          "history_len": len(history)})

            # Diff + lineage diff.
            diff = diff_revisions(history[-2], history[-1])
            lineage = diff_lineage(history)

            # Control-room + exports.
            po_payload = assemble_package_overview_payload(repo, bid_id)
            auth_payload = assemble_authority_action_payload(repo, bid_id)
            rd_payload = assemble_bid_readiness_payload(repo, bid_id)
            tl_payload = assemble_timeline_payload(repo, bid_id=bid_id)

            exports = {
                "bid_readiness": generate_bid_readiness_export(repo, bid_id),
                "authority_action": generate_authority_action_export(repo, bid_id),
                "final_carry": generate_final_carry_export(repo, bid_id),
            }

            # Render reports.
            reports = {
                "bid_readiness_report": build_bid_readiness_report(repo, bid_id),
                "authority_action_report": build_authority_action_report(repo, bid_id),
                "final_carry_report": build_final_carry_report(repo, bid_id),
            }

            scenario_results.append({
                "scenario_id": sid,
                "bid_id": bid_id,
                "idempotent_saves": idem_keys,
                "replay_status": replay_env["status"],
                "readiness_history_len": len(history),
                "diff_status": diff.get("status"),
                "lineage_diff_count": len(lineage),
                "control_room_ok": all([
                    po_payload["assembly_diagnostics"]["package_overview_present"],
                    rd_payload["assembly_diagnostics"]["readiness_present"],
                    auth_payload["assembly_diagnostics"]["authority_action_present"],
                    bool(tl_payload.get("kind_timelines")),
                ]),
                "exports_ok": all(bool(v.get("export")) for v in exports.values()),
                "reports_ok": all(bool(r.get("sections")) for r in reports.values()),
            })
            _record(f"scenario.{sid}", "ok",
                     {"bid_id": bid_id, "history_len": len(history)})
        except Exception as exc:
            _record(f"scenario.{sid}", "error", {"error": repr(exc)})

    # --- backup + restore round-trip --------------------------------------
    try:
        snap = create_snapshot(repo)
        val = validate_snapshot(snap)
        if not val["ok"]:
            _record("backup.validate", "fail", val)

        # Restore into a scratch repo; ensure counts match.
        from .artifact_repository import ArtifactRepository as _Repo
        scratch = _Repo()
        restore = restore_snapshot(scratch, snap)
        if not restore["restored"]:
            _record("backup.restore", "fail", restore)
        elif scratch.repository_summary()["total_records"] != \
             repo.repository_summary()["total_records"]:
            _record("backup.restore.count_mismatch", "fail",
                     {"source": repo.repository_summary()["total_records"],
                      "restored": scratch.repository_summary()["total_records"]})
        else:
            _record("backup.roundtrip", "ok",
                     {"record_count": restore["restored_count"]})
    except Exception as exc:
        _record("backup", "error", {"error": repr(exc)})

    # --- smoke sanity ------------------------------------------------------
    try:
        smoke = run_smoke(repository=None)  # defaults to default repo
        if smoke["summary"]["scenarios_failed"] != 0:
            _record("smoke", "degraded", smoke["summary"])
        else:
            _record("smoke", "ok", smoke["summary"])
    except Exception as exc:
        _record("smoke", "error", {"error": repr(exc)})

    # --- admin diagnostics (against our acceptance repo) -------------------
    try:
        diag = collect_diagnostics(repo)
        _record("diagnostics", diag.get("overall_health") or "unknown",
                 {"overall_health": diag.get("overall_health")})
    except Exception as exc:
        _record("diagnostics", "error", {"error": repr(exc)})

    ok = all(s.get("status") == "ok" for s in steps if
              not s["step"].startswith("scenario.")) and not failures

    return {
        "acceptance_version": ACCEPTANCE_VERSION,
        "overall_pass": len(failures) == 0,
        "steps": steps,
        "failures": failures,
        "scenario_results": scenario_results,
        "authorization_results": auth_results,
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
