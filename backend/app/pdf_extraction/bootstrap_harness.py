"""
C108 — Startup / bootstrap harness.

Deterministic bootstrap path that initializes config, storage,
repository/services, safety/diagnostics wiring, and optional seeded
demo mode. Exposes readiness/health clearly and fails closed on
invalid startup conditions. Canonical startup — scattered setup logic
should defer to this entry point.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

BOOTSTRAP_VERSION = "bootstrap_harness/v1"

# Readiness status vocabulary ---------------------------------------------
READY_OK = "ready"
READY_DEGRADED = "degraded"
READY_FAIL = "failed"


def bootstrap(
    config: Optional[Dict[str, Any]] = None,
    *,
    seed_scenarios: Optional[List[str]] = None,
    seed_enabled_override: Optional[bool] = None,
) -> Dict[str, Any]:
    """Canonical startup entry point.

    Returns a bootstrap receipt with components, readiness, and any
    validation failures. If config is invalid, fails closed — no
    storage or demo seeding is performed.
    """
    from .runtime_config import (
        load_config_from_env, validate_config,
    )
    from .storage_adapter import build_adapter
    from .artifact_repository import ArtifactRepository
    from .authorization import authorization_summary
    from .idempotency import (
        get_default_idempotency_store, reset_default_idempotency_store,
    )
    from .admin_safety_controls import safety_summary
    from .admin_diagnostics import collect_diagnostics

    cfg = config if config is not None else load_config_from_env()
    validation = validate_config(cfg)
    steps: List[Dict[str, Any]] = []

    if not validation["ok"]:
        steps.append({"step": "config_validation",
                       "status": READY_FAIL,
                       "reasons": validation["reasons"]})
        return {
            "bootstrap_version": BOOTSTRAP_VERSION,
            "readiness": READY_FAIL,
            "config_summary": _config_summary(cfg),
            "validation": validation,
            "components": {},
            "steps": steps,
        }
    steps.append({"step": "config_validation", "status": READY_OK})

    # Storage adapter.
    adapter_kwargs: Dict[str, Any] = {}
    if cfg["storage_kind"] == "file":
        adapter_kwargs["base_dir"] = cfg["storage_base_dir"]
    try:
        adapter = build_adapter(cfg["storage_kind"], **adapter_kwargs)
        steps.append({"step": "storage_adapter", "status": READY_OK,
                       "adapter_type": adapter.__class__.__name__})
    except Exception as exc:
        steps.append({"step": "storage_adapter", "status": READY_FAIL,
                       "error": repr(exc)})
        return {
            "bootstrap_version": BOOTSTRAP_VERSION,
            "readiness": READY_FAIL,
            "config_summary": _config_summary(cfg),
            "validation": validation,
            "components": {},
            "steps": steps,
        }

    # Fresh repository bound to the adapter.
    repository = ArtifactRepository(storage_adapter=adapter)
    steps.append({"step": "repository",
                   "status": READY_OK,
                   "summary": repository.repository_summary()})

    # Idempotency store reset (clean startup).
    reset_default_idempotency_store()
    idem = get_default_idempotency_store()
    steps.append({"step": "idempotency",
                   "status": READY_OK,
                   "summary": idem.summary()})

    # Authorization / safety snapshots.
    auth_snapshot = authorization_summary()
    safety_snapshot = safety_summary()
    steps.append({"step": "authorization", "status": READY_OK,
                   "role_count": len(auth_snapshot.get("roles") or [])})
    steps.append({"step": "safety_controls", "status": READY_OK,
                   "current_environment":
                     safety_snapshot.get("current_environment")})

    # Optional demo seeding.
    seed_enabled = (seed_enabled_override
                     if seed_enabled_override is not None
                     else (cfg.get("feature_flags") or {})
                            .get("seed_scenarios_enabled", False))
    seeded: Dict[str, Any] = {"seeded": False}
    if seed_enabled:
        seeded = _seed_repository(repository, seed_scenarios)
        steps.append({"step": "seed_scenarios",
                       "status": READY_OK if seeded.get("ok") else READY_DEGRADED,
                       "seed_summary": seeded})
    else:
        steps.append({"step": "seed_scenarios", "status": READY_OK,
                       "seed_summary": {"seeded": False,
                                         "reason": "disabled"}})

    # Diagnostics snapshot (never run smoke during boot to keep it cheap).
    diag = collect_diagnostics(repository, adapter=adapter, run_smoke=False)
    steps.append({"step": "diagnostics_snapshot",
                   "status": READY_OK,
                   "overall_health": diag.get("overall_health")})

    readiness = READY_OK
    if any(s.get("status") == READY_DEGRADED for s in steps):
        readiness = READY_DEGRADED
    if any(s.get("status") == READY_FAIL for s in steps):
        readiness = READY_FAIL

    return {
        "bootstrap_version": BOOTSTRAP_VERSION,
        "readiness": readiness,
        "config_summary": _config_summary(cfg),
        "validation": validation,
        "components": {
            "adapter_type": adapter.__class__.__name__,
            "repository_summary": repository.repository_summary(),
            "authorization_summary": auth_snapshot,
            "safety_summary": safety_snapshot,
            "idempotency_summary": idem.summary(),
            "diagnostics_snapshot": diag,
        },
        "seed_result": seeded,
        "steps": steps,
        "repository": repository,
        "adapter": adapter,
    }


def health_check(bootstrap_receipt: Dict[str, Any]) -> Dict[str, Any]:
    """Deterministic health check against a bootstrap receipt."""
    readiness = bootstrap_receipt.get("readiness")
    reasons: List[str] = []
    if readiness != READY_OK:
        reasons.append(f"readiness:{readiness}")
    val = bootstrap_receipt.get("validation") or {}
    if not val.get("ok", True):
        reasons.append("invalid_config")
    return {
        "bootstrap_version": BOOTSTRAP_VERSION,
        "healthy": readiness == READY_OK,
        "readiness": readiness,
        "reasons": reasons,
    }


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _seed_repository(repository: Any,
                      scenarios: Optional[List[str]] = None) -> Dict[str, Any]:
    from .seed_scenarios import list_scenarios, run_scenario_e2e
    sids = scenarios or list_scenarios()
    saves: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for sid in sids:
        try:
            e2e = run_scenario_e2e(sid)
            bid_id = e2e["bid_id"]
            for artifact_type, source_key in _PERSIST_ORDER:
                art = e2e["canonical_artifacts"].get(source_key)
                if not art:
                    continue
                rec = repository.save(artifact_type, deepcopy(art),
                                        metadata={"created_by":
                                                    "bootstrap_harness"})
                saves.append({"scenario_id": sid,
                               "artifact_type": artifact_type,
                               "bid_id": bid_id,
                               "record_id": rec["record_id"]})
        except Exception as exc:
            errors.append({"scenario_id": sid, "error": repr(exc)})
    return {
        "seeded": True,
        "ok": len(errors) == 0,
        "scenarios": sids,
        "save_count": len(saves),
        "error_count": len(errors),
        "errors": errors,
    }


def _config_summary(cfg: Dict[str, Any]) -> Dict[str, Any]:
    from .runtime_config import summarize_config
    return summarize_config(cfg)


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
