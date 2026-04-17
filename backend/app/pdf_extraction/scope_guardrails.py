"""
C87 — Tenant / bid scope guardrails.

Deterministic scope guardrails for org_id, bid_id, and artifact
ownership across repository, orchestration, and API layers.
Fail-closed on scope mismatches.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

SCOPE_GUARDRAIL_VERSION = "scope_guardrails/v1"

GUARD_OK = "ok"
GUARD_MISSING_BID = "missing_bid_id"
GUARD_MISSING_JOB = "missing_job_id"
GUARD_MISSING_ORG = "missing_org_id"
GUARD_BID_MISMATCH = "bid_id_mismatch"
GUARD_JOB_MISMATCH = "job_id_mismatch"
GUARD_ORG_MISMATCH = "org_id_mismatch"
GUARD_OWNER_MISMATCH = "owner_id_mismatch"


def check_scope(
    record: Optional[Dict[str, Any]],
    bid_id: Optional[str] = None,
    job_id: Optional[str] = None,
    org_id: Optional[str] = None,
    owner_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Deterministic scope check against a record envelope.

    Returns {"ok": bool, "reasons": [codes], "scope": {...}}.
    If record is None, returns ok=False with missing reasons.
    """
    reasons: List[str] = []
    if record is None:
        return {"ok": False, "reasons": ["record_not_found"], "scope": {
            "bid_id": bid_id, "job_id": job_id, "org_id": org_id, "owner_id": owner_id,
        }}

    rec_bid = _record_bid_id(record)
    rec_job = _record_job_id(record)
    rec_org = record.get("org_id")
    rec_owner = record.get("owner_id")

    if bid_id is not None:
        if rec_bid is None:
            reasons.append(GUARD_MISSING_BID)
        elif rec_bid != bid_id:
            reasons.append(GUARD_BID_MISMATCH)
    if job_id is not None:
        if rec_job is None:
            reasons.append(GUARD_MISSING_JOB)
        elif rec_job != job_id:
            reasons.append(GUARD_JOB_MISMATCH)
    if org_id is not None:
        if rec_org is None:
            reasons.append(GUARD_MISSING_ORG)
        elif rec_org != org_id:
            reasons.append(GUARD_ORG_MISMATCH)
    if owner_id is not None and rec_owner is not None and rec_owner != owner_id:
        reasons.append(GUARD_OWNER_MISMATCH)

    ok = len(reasons) == 0
    return {
        "ok": ok,
        "reasons": reasons,
        "scope": {
            "requested": {"bid_id": bid_id, "job_id": job_id,
                          "org_id": org_id, "owner_id": owner_id},
            "record": {"bid_id": rec_bid, "job_id": rec_job,
                       "org_id": rec_org, "owner_id": rec_owner},
        },
        "guardrail_version": SCOPE_GUARDRAIL_VERSION,
    }


def filter_records_by_scope(
    records: List[Dict[str, Any]],
    bid_id: Optional[str] = None,
    job_id: Optional[str] = None,
    org_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Drop records that fail scope; leak-proof."""
    out: List[Dict[str, Any]] = []
    for rec in records or []:
        check = check_scope(rec, bid_id=bid_id, job_id=job_id, org_id=org_id)
        if check["ok"]:
            out.append(deepcopy(rec))
    return out


def _record_bid_id(record: Dict[str, Any]) -> Optional[str]:
    env = record.get("envelope") or {}
    art = env.get("artifact") or {}
    return art.get("bid_id") or (art.get("package_ref") or {}).get("bid_id")


def _record_job_id(record: Dict[str, Any]) -> Optional[str]:
    env = record.get("envelope") or {}
    art = env.get("artifact") or {}
    return art.get("job_id")
