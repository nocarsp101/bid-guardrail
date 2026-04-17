"""
C74 — Persistent review state schema.

Canonical persistent schemas and adapters for quote dossiers, package
overviews, authority action packets, bid carry justifications, deadline
pressure, resolution queues, and bid readiness snapshots. Preserves
append-only revision chains and deterministic traceability.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

SCHEMA_VERSION = "persistent_review_schemas/v1"

SCHEMA_QUOTE_DOSSIER = "quote_dossier"
SCHEMA_PACKAGE_OVERVIEW = "package_overview"
SCHEMA_AUTHORITY_ACTION_PACKET = "authority_action_packet"
SCHEMA_BID_CARRY_JUSTIFICATION = "bid_carry_justification"
SCHEMA_DEADLINE_PRESSURE = "deadline_pressure"
SCHEMA_RESOLUTION_QUEUE = "resolution_queue"
SCHEMA_BID_READINESS_SNAPSHOT = "bid_readiness_snapshot"

_ALL_SCHEMA_TYPES = frozenset({
    SCHEMA_QUOTE_DOSSIER, SCHEMA_PACKAGE_OVERVIEW,
    SCHEMA_AUTHORITY_ACTION_PACKET, SCHEMA_BID_CARRY_JUSTIFICATION,
    SCHEMA_DEADLINE_PRESSURE, SCHEMA_RESOLUTION_QUEUE,
    SCHEMA_BID_READINESS_SNAPSHOT,
})

# Required fields per schema type (closed set).
_REQUIRED_FIELDS: Dict[str, List[str]] = {
    SCHEMA_QUOTE_DOSSIER: ["dossier_version", "job_id"],
    SCHEMA_PACKAGE_OVERVIEW: ["package_overview_version", "bid_id"],
    SCHEMA_AUTHORITY_ACTION_PACKET: ["authority_action_version"],
    SCHEMA_BID_CARRY_JUSTIFICATION: ["carry_justification_version", "bid_id", "carry_decision"],
    SCHEMA_DEADLINE_PRESSURE: ["deadline_pressure_version", "deadline_pressure"],
    SCHEMA_RESOLUTION_QUEUE: ["priority_queue_version"],
    SCHEMA_BID_READINESS_SNAPSHOT: ["readiness_snapshot_version", "bid_id"],
}


def persist_artifact(
    schema_type: str,
    artifact: Dict[str, Any],
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Wrap an artifact in a canonical persistent envelope.

    Produces a deterministic record with schema_type, validation status,
    traceability refs, and the artifact itself (deep-copied).
    """
    metadata = metadata or {}
    valid_type = schema_type in _ALL_SCHEMA_TYPES
    required = _REQUIRED_FIELDS.get(schema_type, [])
    missing = [f for f in required if not (artifact or {}).get(f)]

    return {
        "persistent_schema_version": SCHEMA_VERSION,
        "schema_type": schema_type,
        "schema_type_valid": valid_type,
        "record_id": metadata.get("record_id") or _derive_record_id(schema_type, artifact),
        "persisted_at": metadata.get("persisted_at"),
        "persisted_by": metadata.get("persisted_by"),
        "traceability_refs": _traceability_refs(schema_type, artifact),
        "artifact": deepcopy(artifact or {}),
        "schema_diagnostics": {
            "missing_required_fields": missing,
            "fields_present": list((artifact or {}).keys()) if artifact else [],
        },
    }


def append_revision(
    existing_records: List[Dict[str, Any]],
    new_record: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Append-only revision chain with `superseded_by` linkage."""
    out = [deepcopy(r) for r in existing_records]
    new = deepcopy(new_record)
    new["revision_sequence"] = len(out)
    for prior in out:
        prior.setdefault("superseded_by", None)
    if out:
        out[-1]["superseded_by"] = new.get("record_id")
    out.append(new)
    return out


def get_current_record(records: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not records:
        return None
    return deepcopy(records[-1])


def get_revision_history(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return deepcopy(records)


def validate_schema(schema_type: str, artifact: Dict[str, Any]) -> Dict[str, Any]:
    """Deterministic validation — never modifies the artifact."""
    valid_type = schema_type in _ALL_SCHEMA_TYPES
    required = _REQUIRED_FIELDS.get(schema_type, [])
    missing = [f for f in required if not (artifact or {}).get(f)]
    return {
        "schema_type": schema_type,
        "schema_type_valid": valid_type,
        "missing_required_fields": missing,
        "is_valid": valid_type and not missing,
    }


def _derive_record_id(schema_type: str, artifact: Dict[str, Any]) -> str:
    artifact = artifact or {}
    if schema_type == SCHEMA_QUOTE_DOSSIER:
        return f"dossier-{artifact.get('job_id')}"
    if schema_type == SCHEMA_PACKAGE_OVERVIEW:
        return f"package-{artifact.get('bid_id')}"
    if schema_type == SCHEMA_BID_CARRY_JUSTIFICATION:
        return f"carry-{artifact.get('bid_id')}"
    if schema_type == SCHEMA_BID_READINESS_SNAPSHOT:
        return f"readiness-{artifact.get('bid_id')}"
    if schema_type == SCHEMA_AUTHORITY_ACTION_PACKET:
        bid = (artifact.get("package_ref") or {}).get("bid_id")
        return f"authority-action-{bid}"
    if schema_type == SCHEMA_DEADLINE_PRESSURE:
        return "deadline-pressure"
    if schema_type == SCHEMA_RESOLUTION_QUEUE:
        return "priority-queue"
    return f"record-{schema_type}"


def _traceability_refs(schema_type: str, artifact: Dict[str, Any]) -> Dict[str, Any]:
    a = artifact or {}
    refs: Dict[str, Any] = {"schema_type": schema_type}
    if schema_type == SCHEMA_QUOTE_DOSSIER:
        refs["job_id"] = a.get("job_id")
        refs["vendor_name"] = a.get("vendor_name")
    elif schema_type == SCHEMA_PACKAGE_OVERVIEW:
        refs["bid_id"] = a.get("bid_id")
        refs["quote_count"] = a.get("quote_count")
    elif schema_type == SCHEMA_BID_CARRY_JUSTIFICATION:
        refs["bid_id"] = a.get("bid_id")
        refs["carry_decision"] = a.get("carry_decision")
    elif schema_type == SCHEMA_BID_READINESS_SNAPSHOT:
        refs["bid_id"] = a.get("bid_id")
        refs["overall_readiness"] = a.get("overall_readiness")
    elif schema_type == SCHEMA_AUTHORITY_ACTION_PACKET:
        refs["action_item_count"] = a.get("action_item_count")
    elif schema_type == SCHEMA_DEADLINE_PRESSURE:
        refs["deadline_pressure"] = a.get("deadline_pressure")
    elif schema_type == SCHEMA_RESOLUTION_QUEUE:
        refs["total_items"] = (a.get("queue_summary") or {}).get("total_items")
    return refs
