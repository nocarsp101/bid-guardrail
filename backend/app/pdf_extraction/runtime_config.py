"""
C107 — Runtime config / environment contract.

Closed-vocabulary runtime config layer covering environment mode,
dangerous-action policy, storage adapter selection, demo/dev feature
flags, report delivery options, diagnostics exposure, and admin /
safety rules. Fails closed on invalid or incomplete config. Config
state never carries business truth.
"""
from __future__ import annotations
import os
from copy import deepcopy
from typing import Any, Dict, List, Optional

RUNTIME_CONFIG_VERSION = "runtime_config/v1"

# Closed vocabularies ------------------------------------------------------
ENVIRONMENTS = frozenset({"dev", "test", "staging", "prod"})
STORAGE_KINDS = frozenset({"in_memory", "file"})
DELIVERY_FORMATS = frozenset({"json", "text", "markdown", "structured"})
DIAGNOSTICS_EXPOSURE = frozenset({"public", "admin_only", "disabled"})
DANGEROUS_POLICIES = frozenset({"allow_all", "admin_token", "deny_all"})

# Validation reasons -------------------------------------------------------
REASON_UNKNOWN_ENV = "unknown_environment"
REASON_UNKNOWN_STORAGE = "unknown_storage_kind"
REASON_UNKNOWN_FORMAT = "unknown_delivery_format"
REASON_UNKNOWN_EXPOSURE = "unknown_diagnostics_exposure"
REASON_UNKNOWN_POLICY = "unknown_dangerous_policy"
REASON_FILE_ADAPTER_MISSING_BASE_DIR = "file_adapter_missing_base_dir"
REASON_INCONSISTENT_DEV_FLAGS_IN_PROD = "inconsistent_dev_flags_in_prod"
REASON_PROD_REQUIRES_ADMIN_TOKEN = "prod_requires_admin_token"

# Default config
_DEFAULT_CONFIG: Dict[str, Any] = {
    "environment": "dev",
    "storage_kind": "in_memory",
    "storage_base_dir": None,
    "dangerous_action_policy": "allow_all",
    "expected_admin_token": None,
    "feature_flags": {
        "demo_enabled": True,
        "dev_routes_enabled": True,
        "seed_scenarios_enabled": True,
    },
    "delivery": {
        "default_format": "json",
        "allowed_formats": sorted(DELIVERY_FORMATS),
    },
    "diagnostics": {
        "exposure": "public",
        "include_smoke": False,
    },
}


def default_config() -> Dict[str, Any]:
    return deepcopy(_DEFAULT_CONFIG)


def load_config_from_env(overrides: Optional[Dict[str, Any]] = None
                          ) -> Dict[str, Any]:
    """Load a config from environment variables + explicit overrides.

    Env vars consulted:
      BID_GUARDRAIL_ENV
      BID_GUARDRAIL_STORAGE_KIND
      BID_GUARDRAIL_STORAGE_BASE_DIR
      BID_GUARDRAIL_DANGEROUS_POLICY
      BID_GUARDRAIL_ADMIN_TOKEN
      BID_GUARDRAIL_DEMO_ENABLED (1/0)
      BID_GUARDRAIL_DEV_ROUTES_ENABLED (1/0)
      BID_GUARDRAIL_SEED_ENABLED (1/0)
      BID_GUARDRAIL_DEFAULT_FORMAT
      BID_GUARDRAIL_DIAG_EXPOSURE
    """
    cfg = default_config()
    cfg["environment"] = os.getenv("BID_GUARDRAIL_ENV", cfg["environment"])
    cfg["storage_kind"] = os.getenv("BID_GUARDRAIL_STORAGE_KIND",
                                      cfg["storage_kind"])
    cfg["storage_base_dir"] = os.getenv("BID_GUARDRAIL_STORAGE_BASE_DIR",
                                          cfg["storage_base_dir"])
    cfg["dangerous_action_policy"] = os.getenv(
        "BID_GUARDRAIL_DANGEROUS_POLICY", cfg["dangerous_action_policy"])
    cfg["expected_admin_token"] = os.getenv("BID_GUARDRAIL_ADMIN_TOKEN",
                                              cfg["expected_admin_token"])

    flags = cfg["feature_flags"]
    flags["demo_enabled"] = _bool_env("BID_GUARDRAIL_DEMO_ENABLED",
                                        flags["demo_enabled"])
    flags["dev_routes_enabled"] = _bool_env(
        "BID_GUARDRAIL_DEV_ROUTES_ENABLED", flags["dev_routes_enabled"])
    flags["seed_scenarios_enabled"] = _bool_env(
        "BID_GUARDRAIL_SEED_ENABLED", flags["seed_scenarios_enabled"])

    cfg["delivery"]["default_format"] = os.getenv(
        "BID_GUARDRAIL_DEFAULT_FORMAT",
        cfg["delivery"]["default_format"])
    cfg["diagnostics"]["exposure"] = os.getenv(
        "BID_GUARDRAIL_DIAG_EXPOSURE", cfg["diagnostics"]["exposure"])

    if overrides:
        cfg = _merge(cfg, overrides)
    return cfg


def validate_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Return a deterministic validation result. Fails closed."""
    reasons: List[str] = []
    if cfg.get("environment") not in ENVIRONMENTS:
        reasons.append(REASON_UNKNOWN_ENV)
    if cfg.get("storage_kind") not in STORAGE_KINDS:
        reasons.append(REASON_UNKNOWN_STORAGE)
    if cfg.get("storage_kind") == "file" and not cfg.get("storage_base_dir"):
        reasons.append(REASON_FILE_ADAPTER_MISSING_BASE_DIR)
    if cfg.get("dangerous_action_policy") not in DANGEROUS_POLICIES:
        reasons.append(REASON_UNKNOWN_POLICY)
    default_format = (cfg.get("delivery") or {}).get("default_format")
    if default_format not in DELIVERY_FORMATS:
        reasons.append(REASON_UNKNOWN_FORMAT)
    allowed_formats = (cfg.get("delivery") or {}).get("allowed_formats") or []
    for fmt in allowed_formats:
        if fmt not in DELIVERY_FORMATS:
            reasons.append(REASON_UNKNOWN_FORMAT)
            break
    exposure = (cfg.get("diagnostics") or {}).get("exposure")
    if exposure not in DIAGNOSTICS_EXPOSURE:
        reasons.append(REASON_UNKNOWN_EXPOSURE)

    # Cross-field checks.
    if cfg.get("environment") == "prod":
        flags = cfg.get("feature_flags") or {}
        if flags.get("dev_routes_enabled") or flags.get("seed_scenarios_enabled"):
            reasons.append(REASON_INCONSISTENT_DEV_FLAGS_IN_PROD)
        if cfg.get("dangerous_action_policy") == "allow_all":
            reasons.append(REASON_PROD_REQUIRES_ADMIN_TOKEN)

    ok = len(reasons) == 0
    return {
        "runtime_config_version": RUNTIME_CONFIG_VERSION,
        "ok": ok,
        "reasons": sorted(set(reasons)),
        "config": deepcopy(cfg),
    }


def summarize_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    validation = validate_config(cfg)
    return {
        "runtime_config_version": RUNTIME_CONFIG_VERSION,
        "environment": cfg.get("environment"),
        "storage_kind": cfg.get("storage_kind"),
        "storage_base_dir": cfg.get("storage_base_dir"),
        "dangerous_action_policy": cfg.get("dangerous_action_policy"),
        "feature_flags": deepcopy(cfg.get("feature_flags") or {}),
        "delivery": deepcopy(cfg.get("delivery") or {}),
        "diagnostics": deepcopy(cfg.get("diagnostics") or {}),
        "validation": {
            "ok": validation["ok"],
            "reasons": validation["reasons"],
        },
    }


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _bool_env(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = deepcopy(v)
    return out
