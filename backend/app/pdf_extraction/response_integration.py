"""
C57 — Response integration layer.

Structured ingestion of subcontractor responses linked to original
clarifications and underlying evidence. Updates scope, comparability,
and risk via append-only overlays — never mutates original data.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

RESPONSE_VERSION = "response_integration/v1"

RESPONSE_CONFIRMED = "confirmed"
RESPONSE_CORRECTED = "corrected"
RESPONSE_DECLINED = "declined"
RESPONSE_PARTIAL = "partial"
RESPONSE_NO_RESPONSE = "no_response"

_ALL_RESPONSE_TYPES = frozenset({
    RESPONSE_CONFIRMED, RESPONSE_CORRECTED, RESPONSE_DECLINED,
    RESPONSE_PARTIAL, RESPONSE_NO_RESPONSE,
})


def integrate_responses(
    tracking_state: Dict[str, Any],
    responses: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Ingest structured responses and produce an integration result.

    Each response shaped:
        {
            "response_id": "...",
            "clarification_id": "...",
            "response_type": "confirmed" | "corrected" | ...,
            "responded_by": "...",
            "responded_at": "...",
            "response_values": { "qty": ..., "unit": ..., ... },
            "response_note": "...",
        }
    """
    tracking = deepcopy(tracking_state or {})
    tracked = tracking.get("tracked_clarifications") or []
    clar_lookup = {t["clarification_id"]: t for t in tracked}

    integrated: List[Dict[str, Any]] = []
    unknown_clar_ids: List[str] = []
    scope_updates: List[Dict[str, Any]] = []
    comparability_updates: List[Dict[str, Any]] = []
    risk_updates: List[Dict[str, Any]] = []

    for resp in responses:
        if not isinstance(resp, dict):
            continue
        cid = resp.get("clarification_id")
        resp_id = resp.get("response_id") or f"resp-{len(integrated)}"
        resp_type = resp.get("response_type")
        if resp_type not in _ALL_RESPONSE_TYPES:
            resp_type = RESPONSE_NO_RESPONSE

        if cid not in clar_lookup:
            unknown_clar_ids.append(str(cid))
            integrated.append(_build_response_record(resp_id, cid, resp, resp_type, linked=False))
            continue

        clar = clar_lookup[cid]
        record = _build_response_record(resp_id, cid, resp, resp_type, linked=True)
        record["original_clarification_type"] = clar.get("clarification_type")
        record["original_source_ref"] = clar.get("source_ref")
        record["original_evidence_refs"] = deepcopy(clar.get("evidence_refs") or [])
        integrated.append(record)

        # Derive append-only updates from the response.
        if resp_type in (RESPONSE_CONFIRMED, RESPONSE_CORRECTED):
            vals = resp.get("response_values") or {}
            if vals.get("qty") is not None or vals.get("unit") is not None:
                comparability_updates.append({
                    "response_id": resp_id,
                    "clarification_id": cid,
                    "source_ref": clar.get("source_ref"),
                    "update_type": "comparability_improvement",
                    "provided_values": deepcopy(vals),
                })
            clar_type = clar.get("clarification_type") or ""
            if "scope" in clar_type:
                scope_updates.append({
                    "response_id": resp_id,
                    "clarification_id": cid,
                    "source_ref": clar.get("source_ref"),
                    "update_type": "scope_confirmed" if resp_type == RESPONSE_CONFIRMED else "scope_corrected",
                    "response_note": resp.get("response_note"),
                })

        if resp_type == RESPONSE_DECLINED:
            risk_updates.append({
                "response_id": resp_id,
                "clarification_id": cid,
                "source_ref": clar.get("source_ref"),
                "update_type": "risk_escalation_declined_response",
            })

    summary = _build_summary(integrated, scope_updates, comparability_updates, risk_updates)

    return {
        "response_version": RESPONSE_VERSION,
        "integrated_responses": integrated,
        "scope_updates": scope_updates,
        "comparability_updates": comparability_updates,
        "risk_updates": risk_updates,
        "integration_summary": summary,
        "integration_diagnostics": {
            "responses_processed": len(integrated),
            "unknown_clarification_ids": unknown_clar_ids,
        },
    }


def _build_response_record(
    resp_id: str,
    cid: Optional[str],
    resp: Dict[str, Any],
    resp_type: str,
    linked: bool,
) -> Dict[str, Any]:
    return {
        "response_id": resp_id,
        "clarification_id": cid,
        "response_type": resp_type,
        "responded_by": resp.get("responded_by"),
        "responded_at": resp.get("responded_at"),
        "response_values": deepcopy(resp.get("response_values") or {}),
        "response_note": resp.get("response_note"),
        "linked_to_clarification": linked,
    }


def _build_summary(integrated, scope_updates, comparability_updates, risk_updates):
    type_counts: Dict[str, int] = {}
    for r in integrated:
        t = r.get("response_type") or "unknown"
        type_counts[t] = type_counts.get(t, 0) + 1
    return {
        "total_responses": len(integrated),
        "response_type_counts": dict(sorted(type_counts.items())),
        "scope_updates_count": len(scope_updates),
        "comparability_updates_count": len(comparability_updates),
        "risk_updates_count": len(risk_updates),
        "linked_count": sum(1 for r in integrated if r.get("linked_to_clarification")),
        "unlinked_count": sum(1 for r in integrated if not r.get("linked_to_clarification")),
    }
