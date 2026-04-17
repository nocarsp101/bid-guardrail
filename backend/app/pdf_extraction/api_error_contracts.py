"""
C88 — API hardening + error contracts.

Canonical API error contracts with deterministic codes/messages.
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional

ERROR_CONTRACT_VERSION = "api_error_contracts/v1"

ERR_INVALID_ARTIFACT_TYPE = "invalid_artifact_type"
ERR_SCHEMA_MISMATCH = "schema_mismatch"
ERR_MALFORMED_PAYLOAD = "malformed_payload"
ERR_MISSING_REVISION = "missing_revision"
ERR_SCOPE_MISMATCH = "scope_mismatch"
ERR_RECORD_NOT_FOUND = "record_not_found"
ERR_UNKNOWN_SCENARIO = "unknown_scenario"
ERR_UNKNOWN_EXPORT_TYPE = "unknown_export_type"
ERR_INVALID_REQUEST = "invalid_request"

_HTTP_MAPPING = {
    ERR_INVALID_ARTIFACT_TYPE: 400,
    ERR_SCHEMA_MISMATCH: 422,
    ERR_MALFORMED_PAYLOAD: 400,
    ERR_MISSING_REVISION: 404,
    ERR_SCOPE_MISMATCH: 403,
    ERR_RECORD_NOT_FOUND: 404,
    ERR_UNKNOWN_SCENARIO: 400,
    ERR_UNKNOWN_EXPORT_TYPE: 400,
    ERR_INVALID_REQUEST: 400,
}

_MESSAGES = {
    ERR_INVALID_ARTIFACT_TYPE: "Artifact type is not supported.",
    ERR_SCHEMA_MISMATCH: "Artifact does not match the required schema fields.",
    ERR_MALFORMED_PAYLOAD: "Request payload is malformed or not a JSON object.",
    ERR_MISSING_REVISION: "No record found for the given revision_sequence.",
    ERR_SCOPE_MISMATCH: "Requested scope does not match record scope.",
    ERR_RECORD_NOT_FOUND: "No record matched the query.",
    ERR_UNKNOWN_SCENARIO: "Unknown scenario id.",
    ERR_UNKNOWN_EXPORT_TYPE: "Unknown export type.",
    ERR_INVALID_REQUEST: "Invalid request parameters.",
}


def build_error(
    error_code: str,
    detail: Optional[Dict[str, Any]] = None,
    hint: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "error_contract_version": ERROR_CONTRACT_VERSION,
        "error_code": error_code,
        "http_status": _HTTP_MAPPING.get(error_code, 400),
        "message": _MESSAGES.get(error_code, "Unspecified error."),
        "hint": hint,
        "detail": detail or {},
    }


def to_http_response(error: Dict[str, Any]):
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=error.get("http_status", 400),
                         content={k: v for k, v in error.items() if k != "http_status"})


def list_error_codes() -> List[str]:
    return sorted(_HTTP_MAPPING.keys())
