"""
C96 — Admin diagnostics / health pack.

Deterministic admin diagnostics for adapter state, repository counts,
lineage integrity, scoped artifact anomalies, schema validation
failures, smoke-harness status, and endpoint readiness. Explainable,
traceable, and strictly read-only.
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional

DIAGNOSTICS_VERSION = "admin_diagnostics/v1"

# --- status vocabulary ----------------------------------------------------
HEALTH_OK = "ok"
HEALTH_DEGRADED = "degraded"
HEALTH_CRITICAL = "critical"


def collect_diagnostics(
    repository: Any,
    adapter: Any = None,
    run_smoke: bool = False,
) -> Dict[str, Any]:
    """Aggregate all admin diagnostics into a single payload."""
    repo_diag = repository_diagnostics(repository)
    lineage_diag = lineage_integrity(repository)
    scope_diag = scope_anomalies(repository)
    schema_diag = schema_validation(repository)
    adapter_diag = adapter_diagnostics(adapter)
    endpoint_diag = endpoint_readiness()
    smoke_diag = smoke_status(repository) if run_smoke else {
        "diagnostics_version": DIAGNOSTICS_VERSION,
        "skipped": True,
    }

    status = _overall_status([
        repo_diag, lineage_diag, scope_diag, schema_diag,
        adapter_diag, smoke_diag, endpoint_diag,
    ])

    return {
        "diagnostics_version": DIAGNOSTICS_VERSION,
        "overall_health": status,
        "repository": repo_diag,
        "lineage": lineage_diag,
        "scope_anomalies": scope_diag,
        "schema_validation": schema_diag,
        "adapter": adapter_diag,
        "endpoints": endpoint_diag,
        "smoke": smoke_diag,
    }


def repository_diagnostics(repository: Any) -> Dict[str, Any]:
    summary = repository.repository_summary()
    records = repository.all_records()
    latest_count = sum(1 for r in records if not r.get("superseded_by"))
    return {
        "diagnostics_version": DIAGNOSTICS_VERSION,
        "status": HEALTH_OK,
        "total_records": summary.get("total_records"),
        "records_by_type": summary.get("records_by_type") or {},
        "records_by_bid_count": len(summary.get("records_by_bid") or {}),
        "distinct_record_ids": summary.get("distinct_record_ids"),
        "latest_revision_count": latest_count,
    }


def lineage_integrity(repository: Any) -> Dict[str, Any]:
    records = repository.all_records()
    ids = {r.get("record_id") for r in records}
    broken: List[Dict[str, Any]] = []
    dangling_supersedes: List[str] = []
    for r in records:
        sup = r.get("supersedes")
        if sup and sup not in ids:
            dangling_supersedes.append(r.get("record_id"))
            broken.append({"record_id": r.get("record_id"),
                            "reason": "supersedes_missing",
                            "supersedes": sup})
        sb = r.get("superseded_by")
        if sb and sb not in ids:
            broken.append({"record_id": r.get("record_id"),
                            "reason": "superseded_by_missing",
                            "superseded_by": sb})
    status = HEALTH_OK if not broken else HEALTH_DEGRADED
    return {
        "diagnostics_version": DIAGNOSTICS_VERSION,
        "status": status,
        "record_count": len(records),
        "broken_links": broken,
        "dangling_supersedes_count": len(dangling_supersedes),
    }


def scope_anomalies(repository: Any) -> Dict[str, Any]:
    """Records that are missing scope identifiers or carry mixed scope."""
    records = repository.all_records()
    missing_bid: List[str] = []
    missing_org: List[str] = []
    mixed_bid_per_record_id: List[str] = []
    by_rid: Dict[str, set] = {}

    for r in records:
        rid = r.get("record_id")
        env = r.get("envelope") or {}
        art = env.get("artifact") or {}
        bid = art.get("bid_id") or (art.get("package_ref") or {}).get("bid_id")
        if r.get("artifact_type") in ("package_overview", "bid_readiness_snapshot",
                                        "bid_carry_justification",
                                        "authority_action_packet") and not bid:
            missing_bid.append(rid)
        if not r.get("org_id"):
            missing_org.append(rid)
        if rid and bid:
            by_rid.setdefault(rid, set()).add(bid)
    for rid, bids in by_rid.items():
        if len(bids) > 1:
            mixed_bid_per_record_id.append(rid)

    reasons: List[str] = []
    if missing_bid:
        reasons.append("missing_bid_id")
    if mixed_bid_per_record_id:
        reasons.append("mixed_bid_per_record_id")
    status = HEALTH_OK if not reasons else HEALTH_DEGRADED
    return {
        "diagnostics_version": DIAGNOSTICS_VERSION,
        "status": status,
        "missing_bid_count": len(missing_bid),
        "missing_org_count": len(missing_org),
        "mixed_bid_per_record_id_count": len(mixed_bid_per_record_id),
        "reasons": reasons,
    }


def schema_validation(repository: Any) -> Dict[str, Any]:
    """Envelope-level schema checks against canonical contract metadata."""
    from .canonical_api_contracts import (
        list_supported_artifact_types, get_schema_descriptor,
    )
    supported = set(list_supported_artifact_types())
    records = repository.all_records()
    failures: List[Dict[str, Any]] = []
    checked = 0
    for r in records:
        artifact_type = r.get("artifact_type")
        env = r.get("envelope") or {}
        art = env.get("artifact") or {}
        errs: List[str] = []
        if not env:
            errs.append("envelope_missing")
        if not art:
            errs.append("artifact_missing")
        if artifact_type in supported:
            descriptor = get_schema_descriptor(artifact_type) or {}
            version_field = descriptor.get("version_field")
            if version_field and art and version_field not in art:
                errs.append(f"missing_version_field:{version_field}")
        checked += 1
        if errs:
            failures.append({"record_id": r.get("record_id"),
                              "artifact_type": artifact_type,
                              "errors": errs})
    status = HEALTH_OK if not failures else HEALTH_DEGRADED
    return {
        "diagnostics_version": DIAGNOSTICS_VERSION,
        "status": status,
        "records_checked": checked,
        "failure_count": len(failures),
        "failures": failures[:20],
    }


def adapter_diagnostics(adapter: Any) -> Dict[str, Any]:
    if adapter is None:
        return {
            "diagnostics_version": DIAGNOSTICS_VERSION,
            "status": HEALTH_OK,
            "attached": False,
        }
    try:
        summary = adapter.adapter_summary() if hasattr(adapter, "adapter_summary") else {}
        keys = adapter.all_keys() if hasattr(adapter, "all_keys") else []
        return {
            "diagnostics_version": DIAGNOSTICS_VERSION,
            "status": HEALTH_OK,
            "attached": True,
            "adapter_summary": summary,
            "key_count": len(keys),
        }
    except Exception as exc:
        return {
            "diagnostics_version": DIAGNOSTICS_VERSION,
            "status": HEALTH_CRITICAL,
            "attached": True,
            "error": repr(exc),
        }


def endpoint_readiness() -> Dict[str, Any]:
    """Declares which canonical/control-room/export/admin endpoints exist."""
    endpoints = [
        "/canonical/artifacts/{artifact_type}",
        "/canonical/artifacts/{artifact_type}/latest",
        "/canonical/artifacts/{artifact_type}/history",
        "/canonical/artifacts/by-bid/{bid_id}",
        "/canonical/artifacts/by-record-id/{record_id}",
        "/canonical/artifacts/{artifact_type}/revision/{revision_sequence}",
        "/canonical/artifacts/by-record-id/{record_id}/lineage",
        "/canonical/artifacts/{artifact_type}/latest-scoped",
        "/canonical/artifacts/{artifact_type}/history-scoped",
        "/canonical/repository/summary",
        "/canonical/repository/reset",
        "/control-room/quote-case/{job_id}",
        "/control-room/package-overview/{bid_id}",
        "/control-room/authority-action",
        "/control-room/bid-readiness/{bid_id}",
        "/control-room/timeline",
        "/exports/sub-clarification/{job_id}",
        "/exports/estimator-review/{job_id}",
        "/exports/authority-action/{bid_id}",
        "/exports/bid-readiness/{bid_id}",
        "/exports/final-carry/{bid_id}",
        "/demo/scenarios",
        "/demo/run/{scenario_id}",
        "/demo/fixture",
        "/demo/run-e2e",
        "/api/error-codes",
        "/api/scope-check",
        "/api/revision-diff",
        "/api/ui-integration-pack",
        "/api/smoke-harness",
        "/api/authorization/summary",
        "/api/authorization/check",
        "/api/idempotency/summary",
        "/api/backup/snapshot",
        "/api/backup/restore",
        "/api/reports/{report_kind}",
        "/api/diagnostics",
        "/api/acceptance",
    ]
    return {
        "diagnostics_version": DIAGNOSTICS_VERSION,
        "status": HEALTH_OK,
        "endpoint_count": len(endpoints),
        "endpoints": endpoints,
    }


def smoke_status(repository: Any) -> Dict[str, Any]:
    from .production_smoke_harness import run_smoke
    from .artifact_repository import ArtifactRepository
    # Run the smoke harness against a scratch repo to avoid mutating the
    # observed repository state.
    scratch = ArtifactRepository()
    out = run_smoke(repository=scratch)
    summary = out.get("summary") or {}
    status = HEALTH_OK if summary.get("scenarios_failed", 0) == 0 else HEALTH_DEGRADED
    return {
        "diagnostics_version": DIAGNOSTICS_VERSION,
        "status": status,
        "scenarios_run": summary.get("scenarios_run"),
        "scenarios_ok": summary.get("scenarios_ok"),
        "scenarios_failed": summary.get("scenarios_failed"),
    }


def _overall_status(parts: List[Dict[str, Any]]) -> str:
    statuses = [p.get("status") for p in parts if isinstance(p, dict)]
    if HEALTH_CRITICAL in statuses:
        return HEALTH_CRITICAL
    if HEALTH_DEGRADED in statuses:
        return HEALTH_DEGRADED
    return HEALTH_OK
