"""
C92 — Role / action authorization layer.

Deterministic authorization for repository writes, scoped retrieval,
export generation, control-room payload access, smoke/demo/admin
flows, and reset/dev-only actions. Closed role/action vocabulary,
fail-closed on unauthorized access. Separate from business truth and
scope guardrails.
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional

AUTHORIZATION_VERSION = "authorization/v1"

# --- closed role vocabulary ------------------------------------------------
ROLE_ESTIMATOR = "estimator"
ROLE_REVIEWER = "reviewer"
ROLE_OFFICE = "office"
ROLE_ADMIN = "admin"
ROLE_SYSTEM = "system"
ROLE_READ_ONLY = "read_only"
ROLE_GUEST = "guest"

_ALL_ROLES = frozenset({
    ROLE_ESTIMATOR, ROLE_REVIEWER, ROLE_OFFICE, ROLE_ADMIN,
    ROLE_SYSTEM, ROLE_READ_ONLY, ROLE_GUEST,
})

# --- closed action vocabulary ---------------------------------------------
ACTION_SAVE_ARTIFACT = "save_artifact"
ACTION_READ_ARTIFACT = "read_artifact"
ACTION_READ_SCOPED = "read_scoped_artifact"
ACTION_GENERATE_EXPORT = "generate_export"
ACTION_CONTROL_ROOM_VIEW = "control_room_view"
ACTION_RUN_SMOKE = "run_smoke"
ACTION_RUN_DEMO = "run_demo"
ACTION_REVISION_DIFF = "revision_diff"
ACTION_RESET_REPOSITORY = "reset_repository"
ACTION_BACKUP = "backup_repository"
ACTION_RESTORE = "restore_repository"
ACTION_ADMIN_DIAGNOSTICS = "admin_diagnostics"
ACTION_UI_INTEGRATION = "ui_integration"
ACTION_SCOPE_CHECK = "scope_check"
ACTION_ACCEPTANCE_HARNESS = "acceptance_harness"

_ALL_ACTIONS = frozenset({
    ACTION_SAVE_ARTIFACT, ACTION_READ_ARTIFACT, ACTION_READ_SCOPED,
    ACTION_GENERATE_EXPORT, ACTION_CONTROL_ROOM_VIEW, ACTION_RUN_SMOKE,
    ACTION_RUN_DEMO, ACTION_REVISION_DIFF, ACTION_RESET_REPOSITORY,
    ACTION_BACKUP, ACTION_RESTORE, ACTION_ADMIN_DIAGNOSTICS,
    ACTION_UI_INTEGRATION, ACTION_SCOPE_CHECK, ACTION_ACCEPTANCE_HARNESS,
})

# --- role → allowed actions -----------------------------------------------
_READ_ONLY_ACTIONS = frozenset({
    ACTION_READ_ARTIFACT, ACTION_READ_SCOPED, ACTION_CONTROL_ROOM_VIEW,
    ACTION_UI_INTEGRATION, ACTION_REVISION_DIFF, ACTION_SCOPE_CHECK,
})

_ESTIMATOR_ACTIONS = _READ_ONLY_ACTIONS | frozenset({
    ACTION_SAVE_ARTIFACT, ACTION_GENERATE_EXPORT,
})

_REVIEWER_ACTIONS = _READ_ONLY_ACTIONS | frozenset({ACTION_GENERATE_EXPORT})

_OFFICE_ACTIONS = _ESTIMATOR_ACTIONS | frozenset({
    ACTION_RUN_DEMO,
})

_ADMIN_ACTIONS = frozenset(_ALL_ACTIONS)  # admin can do everything

_SYSTEM_ACTIONS = _ADMIN_ACTIONS - frozenset({ACTION_RESET_REPOSITORY})

_GUEST_ACTIONS = frozenset({ACTION_UI_INTEGRATION})

_ROLE_ACTIONS: Dict[str, frozenset] = {
    ROLE_ESTIMATOR: _ESTIMATOR_ACTIONS,
    ROLE_REVIEWER: _REVIEWER_ACTIONS,
    ROLE_OFFICE: _OFFICE_ACTIONS,
    ROLE_ADMIN: _ADMIN_ACTIONS,
    ROLE_SYSTEM: _SYSTEM_ACTIONS,
    ROLE_READ_ONLY: _READ_ONLY_ACTIONS,
    ROLE_GUEST: _GUEST_ACTIONS,
}

# --- denial reason vocabulary ---------------------------------------------
DENY_UNKNOWN_ROLE = "unknown_role"
DENY_UNKNOWN_ACTION = "unknown_action"
DENY_ROLE_MISSING = "role_missing"
DENY_ACTION_MISSING = "action_missing"
DENY_ROLE_NOT_PERMITTED = "role_not_permitted"


def list_roles() -> List[str]:
    return sorted(_ALL_ROLES)


def list_actions() -> List[str]:
    return sorted(_ALL_ACTIONS)


def actions_for_role(role: str) -> List[str]:
    return sorted(_ROLE_ACTIONS.get(role, frozenset()))


def authorize(
    role: Optional[str],
    action: Optional[str],
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Deterministic authorization check. Fails closed."""
    ctx = dict(context or {})
    if role is None or role == "":
        return _deny(DENY_ROLE_MISSING, role, action, ctx)
    if action is None or action == "":
        return _deny(DENY_ACTION_MISSING, role, action, ctx)
    if role not in _ALL_ROLES:
        return _deny(DENY_UNKNOWN_ROLE, role, action, ctx)
    if action not in _ALL_ACTIONS:
        return _deny(DENY_UNKNOWN_ACTION, role, action, ctx)
    allowed = _ROLE_ACTIONS.get(role, frozenset())
    if action not in allowed:
        return _deny(DENY_ROLE_NOT_PERMITTED, role, action, ctx)
    return {
        "authorization_version": AUTHORIZATION_VERSION,
        "allowed": True,
        "role": role,
        "action": action,
        "reasons": [],
        "context": ctx,
    }


def enforce(
    role: Optional[str],
    action: Optional[str],
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Same as authorize but raises on deny (for internal gating)."""
    result = authorize(role, action, context=context)
    if not result["allowed"]:
        raise AuthorizationError(result)
    return result


class AuthorizationError(Exception):
    def __init__(self, decision: Dict[str, Any]):
        super().__init__(f"unauthorized: {decision.get('reasons')}")
        self.decision = decision


def _deny(reason: str, role, action, ctx) -> Dict[str, Any]:
    return {
        "authorization_version": AUTHORIZATION_VERSION,
        "allowed": False,
        "role": role,
        "action": action,
        "reasons": [reason],
        "context": ctx,
    }


def authorization_summary() -> Dict[str, Any]:
    return {
        "authorization_version": AUTHORIZATION_VERSION,
        "roles": list_roles(),
        "actions": list_actions(),
        "role_action_map": {r: actions_for_role(r) for r in list_roles()},
    }
