"""
C77 — API contract layer for canonical artifacts.

Deterministic API-facing contracts, serializers, and parsers for
canonical artifacts. Preserves stable schema identity, revision
metadata, source refs, and append-only lineage. Never recomputes
business truth.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

API_CONTRACT_VERSION = "canonical_api_contracts/v1"

ARTIFACT_QUOTE_DOSSIER = "quote_dossier"
ARTIFACT_PACKAGE_OVERVIEW = "package_overview"
ARTIFACT_AUTHORITY_ACTION_PACKET = "authority_action_packet"
ARTIFACT_BID_READINESS_SNAPSHOT = "bid_readiness_snapshot"
ARTIFACT_BID_CARRY_JUSTIFICATION = "bid_carry_justification"
ARTIFACT_EXPORT_PACKET = "export_packet"

_ALL_ARTIFACTS = frozenset({
    ARTIFACT_QUOTE_DOSSIER, ARTIFACT_PACKAGE_OVERVIEW,
    ARTIFACT_AUTHORITY_ACTION_PACKET, ARTIFACT_BID_READINESS_SNAPSHOT,
    ARTIFACT_BID_CARRY_JUSTIFICATION, ARTIFACT_EXPORT_PACKET,
})

# Artifact type -> (version_field, id_fields, stable_schema_id)
_ARTIFACT_META: Dict[str, Dict[str, Any]] = {
    ARTIFACT_QUOTE_DOSSIER: {
        "version_field": "dossier_version",
        "id_fields": ["job_id", "vendor_name"],
        "stable_schema_id": "bid_guardrail.quote_dossier",
    },
    ARTIFACT_PACKAGE_OVERVIEW: {
        "version_field": "package_overview_version",
        "id_fields": ["bid_id"],
        "stable_schema_id": "bid_guardrail.package_overview",
    },
    ARTIFACT_AUTHORITY_ACTION_PACKET: {
        "version_field": "authority_action_version",
        "id_fields": [],
        "stable_schema_id": "bid_guardrail.authority_action_packet",
    },
    ARTIFACT_BID_READINESS_SNAPSHOT: {
        "version_field": "readiness_snapshot_version",
        "id_fields": ["bid_id"],
        "stable_schema_id": "bid_guardrail.bid_readiness_snapshot",
    },
    ARTIFACT_BID_CARRY_JUSTIFICATION: {
        "version_field": "carry_justification_version",
        "id_fields": ["bid_id", "record_id"],
        "stable_schema_id": "bid_guardrail.bid_carry_justification",
    },
    ARTIFACT_EXPORT_PACKET: {
        "version_field": "export_version",
        "id_fields": ["export_type"],
        "stable_schema_id": "bid_guardrail.export_packet",
    },
}


def serialize_artifact(
    artifact_type: str,
    artifact: Dict[str, Any],
    revision_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Wrap a canonical artifact in an API-facing envelope."""
    meta = revision_metadata or {}
    type_valid = artifact_type in _ALL_ARTIFACTS
    atype_meta = _ARTIFACT_META.get(artifact_type, {})

    version_field = atype_meta.get("version_field")
    artifact_version = (artifact or {}).get(version_field) if version_field else None

    identity_refs = {}
    for f in atype_meta.get("id_fields", []):
        identity_refs[f] = (artifact or {}).get(f)

    lineage = {
        "revision_sequence": int(meta.get("revision_sequence") or 0),
        "superseded_by": meta.get("superseded_by"),
        "supersedes": meta.get("supersedes"),
        "created_at": meta.get("created_at"),
        "created_by": meta.get("created_by"),
    }

    return {
        "api_contract_version": API_CONTRACT_VERSION,
        "artifact_type": artifact_type,
        "artifact_type_valid": type_valid,
        "stable_schema_id": atype_meta.get("stable_schema_id"),
        "artifact_version": artifact_version,
        "identity_refs": identity_refs,
        "lineage": lineage,
        "source_refs": _source_refs(artifact_type, artifact),
        "artifact": deepcopy(artifact or {}),
    }


def parse_artifact(envelope: Dict[str, Any]) -> Dict[str, Any]:
    """Return a deep copy of the wrapped artifact; validates envelope."""
    if not envelope:
        return {}
    atype = envelope.get("artifact_type")
    type_valid = atype in _ALL_ARTIFACTS
    inner = envelope.get("artifact") or {}
    return {
        "artifact_type": atype,
        "artifact_type_valid": type_valid,
        "artifact_version": envelope.get("artifact_version"),
        "identity_refs": deepcopy(envelope.get("identity_refs") or {}),
        "lineage": deepcopy(envelope.get("lineage") or {}),
        "artifact": deepcopy(inner),
    }


def list_supported_artifact_types() -> List[str]:
    return sorted(_ALL_ARTIFACTS)


def get_schema_descriptor(artifact_type: str) -> Dict[str, Any]:
    meta = _ARTIFACT_META.get(artifact_type)
    if meta is None:
        return {"artifact_type": artifact_type, "type_valid": False}
    return {
        "artifact_type": artifact_type,
        "type_valid": True,
        "stable_schema_id": meta["stable_schema_id"],
        "version_field": meta["version_field"],
        "id_fields": list(meta["id_fields"]),
    }


def _source_refs(artifact_type: str, artifact: Dict[str, Any]) -> Dict[str, Any]:
    a = artifact or {}
    refs: Dict[str, Any] = {"artifact_type": artifact_type}
    if artifact_type == ARTIFACT_QUOTE_DOSSIER:
        refs["job_id"] = a.get("job_id")
        refs["vendor_name"] = a.get("vendor_name")
        refs["reeval_current_cycle_id"] = (a.get("current_cycle") or {}).get("cycle_id")
    elif artifact_type == ARTIFACT_PACKAGE_OVERVIEW:
        refs["bid_id"] = a.get("bid_id")
        refs["quote_count"] = a.get("quote_count")
    elif artifact_type == ARTIFACT_BID_READINESS_SNAPSHOT:
        refs["bid_id"] = a.get("bid_id")
        refs["overall_readiness"] = a.get("overall_readiness")
        refs["traceability_refs"] = deepcopy(a.get("traceability_refs") or {})
    elif artifact_type == ARTIFACT_BID_CARRY_JUSTIFICATION:
        refs["bid_id"] = a.get("bid_id")
        refs["record_id"] = a.get("record_id")
        refs["carry_decision"] = a.get("carry_decision")
    elif artifact_type == ARTIFACT_AUTHORITY_ACTION_PACKET:
        refs["package_ref"] = deepcopy(a.get("package_ref") or {})
    elif artifact_type == ARTIFACT_EXPORT_PACKET:
        refs["export_type"] = a.get("export_type")
        refs["inner_source_refs"] = deepcopy(a.get("source_refs") or {})
    return refs
