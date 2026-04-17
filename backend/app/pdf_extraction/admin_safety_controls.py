"""
C102 — Admin / ops safety controls.

Environment-aware protections for dangerous actions such as reset,
restore, and admin-only operations. Closed action vocabulary,
fail-closed outside authorized / dev-safe contexts.
"""
from __future__ import annotations
import os
from typing import Any, Dict, List, Optional

SAFETY_VERSION = "admin_safety_controls/v1"

# Environment vocabulary (closed) ----------------------------------------
ENV_DEV = "dev"
ENV_STAGING = "staging"
ENV_PROD = "prod"
ENV_TEST = "test"

_ALL_ENVS = frozenset({ENV_DEV, ENV_STAGING, ENV_PROD, ENV_TEST})

# Dangerous action vocabulary (closed) -----------------------------------
DANGEROUS_RESET_REPOSITORY = "reset_repository"
DANGEROUS_RESTORE_SNAPSHOT = "restore_snapshot"
DANGEROUS_CLEAR_ADAPTER = "clear_adapter"
DANGEROUS_WIPE_IDEMPOTENCY = "wipe_idempotency"
DANGEROUS_BACKFILL = "backfill_records"

_ALL_DANGEROUS = frozenset({
    DANGEROUS_RESET_REPOSITORY, DANGEROUS_RESTORE_SNAPSHOT,
    DANGEROUS_CLEAR_ADAPTER, DANGEROUS_WIPE_IDEMPOTENCY,
    DANGEROUS_BACKFILL,
})

# Denial reason vocabulary -----------------------------------------------
DENY_ENV_DISALLOWED = "env_disallowed"
DENY_UNKNOWN_ENV = "unknown_env"
DENY_UNKNOWN_ACTION = "unknown_dangerous_action"
DENY_MISSING_CONFIRM_TOKEN = "missing_confirmation_token"
DENY_BAD_CONFIRM_TOKEN = "bad_confirmation_token"
DENY_NOT_AUTHORIZED = "not_authorized"

# env → actions allowed --------------------------------------------------
_ENV_ALLOWED: Dict[str, frozenset] = {
    ENV_DEV: frozenset(_ALL_DANGEROUS),
    ENV_TEST: frozenset(_ALL_DANGEROUS),
    ENV_STAGING: frozenset({
        DANGEROUS_RESET_REPOSITORY, DANGEROUS_WIPE_IDEMPOTENCY,
    }),
    ENV_PROD: frozenset(),  # fail-closed by default
}


def list_environments() -> List[str]:
    return sorted(_ALL_ENVS)


def list_dangerous_actions() -> List[str]:
    return sorted(_ALL_DANGEROUS)


def current_environment() -> str:
    env = os.getenv("BID_GUARDRAIL_ENV", ENV_DEV).lower().strip()
    return env if env in _ALL_ENVS else ENV_DEV


def evaluate_safety(
    action: str,
    *,
    role: Optional[str] = None,
    environment: Optional[str] = None,
    confirmation_token: Optional[str] = None,
    expected_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Deterministic safety gate for dangerous operations."""
    env = environment or current_environment()
    reasons: List[str] = []
    if env not in _ALL_ENVS:
        reasons.append(DENY_UNKNOWN_ENV)
    if action not in _ALL_DANGEROUS:
        reasons.append(DENY_UNKNOWN_ACTION)

    if not reasons:
        # Authorization check against C92 (admin required).
        from .authorization import (
            authorize, ROLE_ADMIN, ACTION_RESET_REPOSITORY, ACTION_RESTORE,
            ACTION_ADMIN_DIAGNOSTICS,
        )
        auth_action_map = {
            DANGEROUS_RESET_REPOSITORY: ACTION_RESET_REPOSITORY,
            DANGEROUS_RESTORE_SNAPSHOT: ACTION_RESTORE,
            DANGEROUS_CLEAR_ADAPTER: ACTION_RESET_REPOSITORY,
            DANGEROUS_WIPE_IDEMPOTENCY: ACTION_RESET_REPOSITORY,
            DANGEROUS_BACKFILL: ACTION_ADMIN_DIAGNOSTICS,
        }
        auth_action = auth_action_map.get(action)
        auth = authorize(role or ROLE_ADMIN, auth_action)
        if not auth.get("allowed"):
            reasons.append(DENY_NOT_AUTHORIZED)

        # Environment allowance.
        allowed_for_env = _ENV_ALLOWED.get(env, frozenset())
        if action not in allowed_for_env:
            reasons.append(DENY_ENV_DISALLOWED)

        # Confirmation token (only required outside dev/test).
        if env in (ENV_STAGING, ENV_PROD):
            if not confirmation_token:
                reasons.append(DENY_MISSING_CONFIRM_TOKEN)
            elif expected_token is not None and \
                 confirmation_token != expected_token:
                reasons.append(DENY_BAD_CONFIRM_TOKEN)

    allowed = len(reasons) == 0
    return {
        "admin_safety_version": SAFETY_VERSION,
        "allowed": allowed,
        "action": action,
        "environment": env,
        "role": role,
        "reasons": reasons,
    }


def guarded_reset_repository(role: str,
                              environment: Optional[str] = None,
                              confirmation_token: Optional[str] = None,
                              expected_token: Optional[str] = None) -> Dict[str, Any]:
    decision = evaluate_safety(DANGEROUS_RESET_REPOSITORY,
                                role=role, environment=environment,
                                confirmation_token=confirmation_token,
                                expected_token=expected_token)
    if not decision["allowed"]:
        return {
            "admin_safety_version": SAFETY_VERSION,
            "executed": False,
            "decision": decision,
        }
    from .artifact_repository import reset_default_repository, get_default_repository
    reset_default_repository()
    return {
        "admin_safety_version": SAFETY_VERSION,
        "executed": True,
        "decision": decision,
        "repository_summary": get_default_repository().repository_summary(),
    }


def guarded_restore_snapshot(role: str,
                               snapshot: Dict[str, Any],
                               environment: Optional[str] = None,
                               confirmation_token: Optional[str] = None,
                               expected_token: Optional[str] = None) -> Dict[str, Any]:
    decision = evaluate_safety(DANGEROUS_RESTORE_SNAPSHOT,
                                role=role, environment=environment,
                                confirmation_token=confirmation_token,
                                expected_token=expected_token)
    if not decision["allowed"]:
        return {
            "admin_safety_version": SAFETY_VERSION,
            "executed": False,
            "decision": decision,
        }
    from .backup_restore import restore_snapshot
    from .artifact_repository import get_default_repository
    result = restore_snapshot(get_default_repository(), snapshot)
    return {
        "admin_safety_version": SAFETY_VERSION,
        "executed": True,
        "decision": decision,
        "restore_result": result,
    }


def guarded_wipe_idempotency(role: str,
                              environment: Optional[str] = None,
                              confirmation_token: Optional[str] = None,
                              expected_token: Optional[str] = None) -> Dict[str, Any]:
    decision = evaluate_safety(DANGEROUS_WIPE_IDEMPOTENCY,
                                role=role, environment=environment,
                                confirmation_token=confirmation_token,
                                expected_token=expected_token)
    if not decision["allowed"]:
        return {
            "admin_safety_version": SAFETY_VERSION,
            "executed": False,
            "decision": decision,
        }
    from .idempotency import reset_default_idempotency_store
    reset_default_idempotency_store()
    return {
        "admin_safety_version": SAFETY_VERSION,
        "executed": True,
        "decision": decision,
    }


def safety_summary() -> Dict[str, Any]:
    return {
        "admin_safety_version": SAFETY_VERSION,
        "current_environment": current_environment(),
        "environments": list_environments(),
        "dangerous_actions": list_dangerous_actions(),
        "env_allowed_map": {env: sorted(_ENV_ALLOWED.get(env, frozenset()))
                             for env in list_environments()},
    }
