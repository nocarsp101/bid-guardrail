"""
C90 — UI integration reference pack.

Deterministic integration fixtures mapping control-room screens and
export actions to endpoint payloads and canonical state labels.
No UI is built here — this is a contract/reference surface that
downstream UI layers can bind to without ambiguity.
"""
from __future__ import annotations
from typing import Any, Dict, List

UI_INTEGRATION_VERSION = "ui_integration_pack/v1"

_SCREENS: List[Dict[str, Any]] = [
    {
        "screen_id": "quote_case_view",
        "title": "Quote Case",
        "endpoint": "/control-room/quote-case",
        "method": "POST",
        "request_schema": {
            "bid_id": "string",
            "job_id": "string",
            "vendor": "string",
        },
        "canonical_state_labels": [
            "risk_level", "gate_outcome", "clarification_count",
            "open_clarifications", "resolved_clarifications",
        ],
        "linked_exports": ["sub_clarification", "estimator_review"],
        "artifact_type": "quote_dossier",
    },
    {
        "screen_id": "package_overview",
        "title": "Package Overview",
        "endpoint": "/control-room/package-overview",
        "method": "POST",
        "request_schema": {"bid_id": "string"},
        "canonical_state_labels": [
            "package_confidence", "confidence_level", "gate_outcome",
            "risk_level", "vendor_count", "unresolved_count",
        ],
        "linked_exports": ["bid_readiness"],
        "artifact_type": "package_overview",
    },
    {
        "screen_id": "authority_action",
        "title": "Authority Action",
        "endpoint": "/control-room/authority-action",
        "method": "POST",
        "request_schema": {"bid_id": "string"},
        "canonical_state_labels": [
            "authority_status", "action_count", "exposure_level",
        ],
        "linked_exports": ["authority_action"],
        "artifact_type": "authority_action_packet",
    },
    {
        "screen_id": "bid_readiness",
        "title": "Bid Readiness",
        "endpoint": "/control-room/bid-readiness",
        "method": "POST",
        "request_schema": {"bid_id": "string"},
        "canonical_state_labels": [
            "readiness_state", "readiness_level", "gate_outcome",
            "blocker_count", "caveat_count",
        ],
        "linked_exports": ["bid_readiness", "final_carry"],
        "artifact_type": "bid_readiness_snapshot",
    },
    {
        "screen_id": "timeline",
        "title": "Revision Timeline",
        "endpoint": "/canonical/timeline",
        "method": "POST",
        "request_schema": {
            "artifact_type": "string",
            "bid_id": "string",
        },
        "canonical_state_labels": [
            "revision_sequence", "supersedes", "created_at",
        ],
        "linked_exports": [],
        "artifact_type": "*",
    },
]

_EXPORT_ACTIONS: List[Dict[str, Any]] = [
    {
        "export_id": "sub_clarification",
        "title": "Subcontractor Clarification",
        "endpoint": "/exports/sub-clarification",
        "method": "POST",
        "request_schema": {"bid_id": "string", "vendor": "string"},
    },
    {
        "export_id": "estimator_review",
        "title": "Estimator Review Packet",
        "endpoint": "/exports/estimator-review",
        "method": "POST",
        "request_schema": {"bid_id": "string"},
    },
    {
        "export_id": "authority_action",
        "title": "Authority Action Packet",
        "endpoint": "/exports/authority-action",
        "method": "POST",
        "request_schema": {"bid_id": "string"},
    },
    {
        "export_id": "bid_readiness",
        "title": "Bid Readiness Packet",
        "endpoint": "/exports/bid-readiness",
        "method": "POST",
        "request_schema": {"bid_id": "string"},
    },
    {
        "export_id": "final_carry",
        "title": "Final Carry / Justification",
        "endpoint": "/exports/final-carry",
        "method": "POST",
        "request_schema": {"bid_id": "string"},
    },
]

_STATE_LABEL_VOCAB: Dict[str, List[str]] = {
    "risk_level": ["LOW", "MEDIUM", "HIGH", "BLOCKED"],
    "readiness_state": ["READY", "CONDITIONAL", "HIGH_RISK", "BLOCKED"],
    "readiness_level": ["READY", "CONDITIONAL", "HIGH_RISK", "BLOCKED"],
    "gate_outcome": ["READY", "CONDITIONAL", "HIGH_RISK", "BLOCKED"],
    "package_confidence": ["HIGH", "MEDIUM", "LOW", "BLOCKED"],
    "confidence_level": ["HIGH", "MEDIUM", "LOW", "BLOCKED"],
    "authority_status": ["CLEAR", "ACTION_REQUIRED", "BLOCKED"],
    "exposure_level": ["LOW", "MEDIUM", "HIGH"],
    "carry_decision": ["CARRY", "CONTINGENCY", "DEFER", "NO_CARRY"],
    "posture": ["ALIGNED", "AT_RISK", "MISALIGNED"],
    "pressure_level": ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
}


def get_ui_integration_pack() -> Dict[str, Any]:
    return {
        "ui_integration_version": UI_INTEGRATION_VERSION,
        "screens": [dict(s) for s in _SCREENS],
        "export_actions": [dict(e) for e in _EXPORT_ACTIONS],
        "state_label_vocab": {k: list(v) for k, v in _STATE_LABEL_VOCAB.items()},
    }


def get_screen(screen_id: str) -> Dict[str, Any]:
    for s in _SCREENS:
        if s["screen_id"] == screen_id:
            return dict(s)
    return {
        "ui_integration_version": UI_INTEGRATION_VERSION,
        "error": "unknown_screen_id",
        "screen_id": screen_id,
    }


def get_export_action(export_id: str) -> Dict[str, Any]:
    for e in _EXPORT_ACTIONS:
        if e["export_id"] == export_id:
            return dict(e)
    return {
        "ui_integration_version": UI_INTEGRATION_VERSION,
        "error": "unknown_export_id",
        "export_id": export_id,
    }


def list_screen_ids() -> List[str]:
    return sorted(s["screen_id"] for s in _SCREENS)


def list_export_ids() -> List[str]:
    return sorted(e["export_id"] for e in _EXPORT_ACTIONS)
