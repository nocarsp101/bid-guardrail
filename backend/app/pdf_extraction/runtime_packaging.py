"""
C120 — Runtime / deployment packaging.

Canonical startup/runtime packaging for the backend (+ frontend handoff
contract). Combines C107 config, C108 bootstrap, C96 diagnostics, and
C102 safety controls into a single packaging surface. Clearly separates
dev/demo conveniences from production-safe behavior and fails closed
on invalid config or missing dependencies.
"""
from __future__ import annotations
import os
from copy import deepcopy
from typing import Any, Dict, List, Optional

RUNTIME_PACKAGING_VERSION = "runtime_packaging/v1"

# Packaging status vocabulary ---------------------------------------------
PACKAGING_OK = "ok"
PACKAGING_DEGRADED = "degraded"
PACKAGING_FAIL = "failed"

# Mode vocabulary ---------------------------------------------------------
MODE_DEV = "dev"
MODE_DEMO = "demo"
MODE_STAGING = "staging"
MODE_PROD = "prod"

_ALL_MODES = frozenset({MODE_DEV, MODE_DEMO, MODE_STAGING, MODE_PROD})


def runtime_profile(mode: Optional[str] = None) -> Dict[str, Any]:
    """Return a deterministic profile that shapes runtime defaults per mode."""
    m = (mode or current_mode()).lower()
    if m not in _ALL_MODES:
        m = MODE_DEV
    feature_flags = {
        "demo_enabled": m in (MODE_DEV, MODE_DEMO),
        "dev_routes_enabled": m in (MODE_DEV, MODE_DEMO),
        "seed_scenarios_enabled": m in (MODE_DEV, MODE_DEMO),
    }
    if m == MODE_PROD:
        dangerous_policy = "admin_token"
        expose_diagnostics = "admin_only"
        environment = "prod"
    elif m == MODE_STAGING:
        dangerous_policy = "admin_token"
        expose_diagnostics = "admin_only"
        environment = "staging"
    elif m == MODE_DEMO:
        dangerous_policy = "allow_all"
        expose_diagnostics = "public"
        environment = "dev"
    else:
        dangerous_policy = "allow_all"
        expose_diagnostics = "public"
        environment = "dev"
    return {
        "runtime_packaging_version": RUNTIME_PACKAGING_VERSION,
        "mode": m,
        "environment": environment,
        "feature_flags": feature_flags,
        "dangerous_action_policy": dangerous_policy,
        "diagnostics_exposure": expose_diagnostics,
    }


def current_mode() -> str:
    mode = os.getenv("BID_GUARDRAIL_MODE")
    if mode:
        return mode.strip().lower()
    env = os.getenv("BID_GUARDRAIL_ENV", MODE_DEV).strip().lower()
    return env if env in _ALL_MODES else MODE_DEV


def build_runtime_config(mode: Optional[str] = None,
                          overrides: Optional[Dict[str, Any]] = None
                          ) -> Dict[str, Any]:
    """Compose a runtime config from mode + overrides.

    The resulting config is what `bootstrap_harness.bootstrap()` expects.
    """
    from .runtime_config import default_config, _merge  # type: ignore
    profile = runtime_profile(mode)
    cfg = default_config()
    cfg["environment"] = profile["environment"]
    cfg["dangerous_action_policy"] = profile["dangerous_action_policy"]
    cfg["feature_flags"].update(profile["feature_flags"])
    cfg["diagnostics"]["exposure"] = profile["diagnostics_exposure"]
    if profile["mode"] in (MODE_STAGING, MODE_PROD):
        cfg.setdefault("expected_admin_token",
                        os.getenv("BID_GUARDRAIL_ADMIN_TOKEN"))
    if overrides:
        cfg = _merge(cfg, overrides)
    return cfg


def build_frontend_handoff(mode: Optional[str] = None) -> Dict[str, Any]:
    """Contract the frontend can consume at startup."""
    profile = runtime_profile(mode)
    frontend_origins = [
        o.strip() for o in
        os.getenv("BID_GUARDRAIL_FRONTEND_ORIGINS",
                   "http://localhost:5173").split(",")
        if o.strip()
    ]
    api_base = os.getenv("BID_GUARDRAIL_API_BASE", "http://localhost:8000")
    return {
        "runtime_packaging_version": RUNTIME_PACKAGING_VERSION,
        "mode": profile["mode"],
        "environment": profile["environment"],
        "api_base": api_base,
        "allowed_frontend_origins": frontend_origins,
        "feature_flags": profile["feature_flags"],
        "show_demo_tabs": profile["feature_flags"]["demo_enabled"],
        "show_legacy_tab": True,
        "dangerous_actions_visible": profile["dangerous_action_policy"]
                                       != "deny_all",
    }


def package_runtime(mode: Optional[str] = None,
                     overrides: Optional[Dict[str, Any]] = None,
                     *,
                     seed_enabled_override: Optional[bool] = None,
                     ) -> Dict[str, Any]:
    """Produce the full packaging receipt for a given mode.

    Runs config validation + bootstrap + diagnostics snapshot, then
    assembles a frontend handoff contract. Dev/demo conveniences are
    kept explicit so production deployment can disable them.
    """
    from .runtime_config import validate_config
    from .bootstrap_harness import bootstrap, health_check

    profile = runtime_profile(mode)
    cfg = build_runtime_config(mode=mode, overrides=overrides)
    validation = validate_config(cfg)

    receipt: Dict[str, Any] = {
        "runtime_packaging_version": RUNTIME_PACKAGING_VERSION,
        "mode": profile["mode"],
        "profile": profile,
        "config": cfg,
        "validation": validation,
    }

    if not validation["ok"]:
        receipt["packaging_status"] = PACKAGING_FAIL
        receipt["reasons"] = validation["reasons"]
        receipt["frontend_handoff"] = None
        receipt["bootstrap"] = None
        return receipt

    effective_seed = (
        seed_enabled_override
        if seed_enabled_override is not None
        else profile["feature_flags"]["seed_scenarios_enabled"]
    )

    boot = bootstrap(cfg, seed_enabled_override=effective_seed)
    boot_plain = {k: v for k, v in boot.items()
                   if k not in ("repository", "adapter")}
    health = health_check(boot_plain)

    status = PACKAGING_OK if health["healthy"] else PACKAGING_DEGRADED
    if boot["readiness"] == "failed":
        status = PACKAGING_FAIL

    receipt["packaging_status"] = status
    receipt["bootstrap"] = boot_plain
    receipt["health"] = health
    receipt["frontend_handoff"] = build_frontend_handoff(mode)
    receipt["dev_conveniences"] = _dev_conveniences(profile)
    receipt["production_safe"] = profile["mode"] in (MODE_STAGING, MODE_PROD)
    return receipt


def _dev_conveniences(profile: Dict[str, Any]) -> Dict[str, Any]:
    flags = profile["feature_flags"]
    return {
        "demo_enabled": flags["demo_enabled"],
        "seed_scenarios_enabled": flags["seed_scenarios_enabled"],
        "dev_routes_enabled": flags["dev_routes_enabled"],
        "dangerous_action_policy": profile["dangerous_action_policy"],
        "diagnostics_exposure": profile["diagnostics_exposure"],
    }


def startup_verification(mode: Optional[str] = None) -> Dict[str, Any]:
    """Lightweight verification used by orchestration/liveness probes."""
    pkg = package_runtime(mode=mode, seed_enabled_override=False)
    reasons: List[str] = []
    if pkg["packaging_status"] != PACKAGING_OK:
        reasons.append(f"packaging:{pkg['packaging_status']}")
    if pkg.get("health") and not pkg["health"]["healthy"]:
        reasons.extend(pkg["health"]["reasons"] or [])
    return {
        "runtime_packaging_version": RUNTIME_PACKAGING_VERSION,
        "healthy": len(reasons) == 0,
        "mode": pkg["mode"],
        "packaging_status": pkg["packaging_status"],
        "reasons": sorted(set(reasons)),
    }
