"""
C94 — Durable backup / restore workflow.

Deterministic snapshot / backup / restore utilities for the artifact
repository and file/dev adapter. Preserves canonical artifacts,
revision lineage, scope metadata, and identity refs across restore.
Includes integrity validation for snapshots.
"""
from __future__ import annotations
import hashlib
import json
import os
from copy import deepcopy
from typing import Any, Dict, List, Optional

BACKUP_VERSION = "backup_restore/v1"

# --- integrity status vocabulary -----------------------------------------
INTEGRITY_OK = "ok"
INTEGRITY_MISSING_SECTION = "missing_section"
INTEGRITY_HASH_MISMATCH = "hash_mismatch"
INTEGRITY_MALFORMED_RECORD = "malformed_record"
INTEGRITY_LINEAGE_BROKEN = "lineage_broken"


def create_snapshot(repository: Any) -> Dict[str, Any]:
    """Deterministic, order-preserving snapshot of the repository."""
    records = repository.all_records()
    payload = {
        "backup_version": BACKUP_VERSION,
        "records": [deepcopy(r) for r in records],
        "summary": repository.repository_summary(),
    }
    payload["integrity_hash"] = _hash_records(payload["records"])
    return payload


def validate_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """Validate that a snapshot is internally consistent."""
    reasons: List[str] = []
    if not isinstance(snapshot, dict):
        return {"backup_version": BACKUP_VERSION,
                "ok": False,
                "reasons": [INTEGRITY_MALFORMED_RECORD]}
    if "records" not in snapshot:
        reasons.append(INTEGRITY_MISSING_SECTION)
    if "integrity_hash" not in snapshot:
        reasons.append(INTEGRITY_MISSING_SECTION)
    records = snapshot.get("records") or []

    for rec in records:
        if not isinstance(rec, dict):
            reasons.append(INTEGRITY_MALFORMED_RECORD)
            continue
        if "record_id" not in rec or "artifact_type" not in rec:
            reasons.append(INTEGRITY_MALFORMED_RECORD)

    # Lineage integrity: every supersedes target should exist in the snapshot.
    record_ids = {r.get("record_id") for r in records if isinstance(r, dict)}
    for rec in records:
        if not isinstance(rec, dict):
            continue
        sup = rec.get("supersedes")
        if sup and sup not in record_ids:
            reasons.append(INTEGRITY_LINEAGE_BROKEN)
            break

    expected = snapshot.get("integrity_hash")
    actual = _hash_records(records)
    if expected and expected != actual:
        reasons.append(INTEGRITY_HASH_MISMATCH)

    return {
        "backup_version": BACKUP_VERSION,
        "ok": len(reasons) == 0,
        "reasons": sorted(set(reasons)),
        "record_count": len(records),
        "computed_hash": actual,
        "declared_hash": expected,
    }


def restore_snapshot(repository: Any, snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """Deterministically restore a snapshot into the given repository.

    Implementation detail: replaces the repository's internal record
    vector and rebuilds indices. Append-only semantics are preserved in
    the sense that restore overwrites *everything* or nothing; partial
    restores are not allowed.
    """
    validation = validate_snapshot(snapshot)
    if not validation["ok"]:
        return {
            "backup_version": BACKUP_VERSION,
            "restored": False,
            "validation": validation,
        }

    records = [deepcopy(r) for r in snapshot.get("records") or []]

    # Clear repository state.
    repository._records = []
    repository._by_type = {}
    repository._by_bid = {}
    repository._by_record_id = {}
    repository._lineage_chain = {}

    for rec in records:
        idx = len(repository._records)
        repository._records.append(rec)
        repository._by_type.setdefault(rec.get("artifact_type"), []).append(idx)
        bid = _record_bid_id(rec)
        if bid:
            repository._by_bid.setdefault(bid, []).append(idx)
        rid = rec.get("record_id")
        if rid:
            repository._by_record_id.setdefault(rid, []).append(idx)
        sup = rec.get("supersedes")
        if sup:
            repository._lineage_chain.setdefault(sup, []).append(rid)

    return {
        "backup_version": BACKUP_VERSION,
        "restored": True,
        "validation": validation,
        "restored_count": len(records),
        "repository_summary": repository.repository_summary(),
    }


def backup_to_adapter(repository: Any, adapter: Any,
                      snapshot_key: str = "snapshot:latest") -> Dict[str, Any]:
    snap = create_snapshot(repository)
    adapter.put(snapshot_key, snap)
    return {
        "backup_version": BACKUP_VERSION,
        "stored_at": snapshot_key,
        "record_count": len(snap["records"]),
        "integrity_hash": snap["integrity_hash"],
    }


def restore_from_adapter(repository: Any, adapter: Any,
                         snapshot_key: str = "snapshot:latest") -> Dict[str, Any]:
    snap = adapter.get(snapshot_key)
    if snap is None:
        return {
            "backup_version": BACKUP_VERSION,
            "restored": False,
            "reason": INTEGRITY_MISSING_SECTION,
            "snapshot_key": snapshot_key,
        }
    return restore_snapshot(repository, snap)


def backup_to_file(repository: Any, path: str) -> Dict[str, Any]:
    snap = create_snapshot(repository)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(snap, f, sort_keys=True)
    return {
        "backup_version": BACKUP_VERSION,
        "stored_at": path,
        "record_count": len(snap["records"]),
        "integrity_hash": snap["integrity_hash"],
    }


def restore_from_file(repository: Any, path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {
            "backup_version": BACKUP_VERSION,
            "restored": False,
            "reason": INTEGRITY_MISSING_SECTION,
            "snapshot_path": path,
        }
    with open(path, "r", encoding="utf-8") as f:
        snap = json.load(f)
    return restore_snapshot(repository, snap)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _hash_records(records: List[Dict[str, Any]]) -> str:
    try:
        blob = json.dumps(records, sort_keys=True, separators=(",", ":"),
                           default=repr)
    except TypeError:
        blob = repr(records)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _record_bid_id(record: Dict[str, Any]) -> Optional[str]:
    env = record.get("envelope") or {}
    art = env.get("artifact") or {}
    return art.get("bid_id") or (art.get("package_ref") or {}).get("bid_id")
