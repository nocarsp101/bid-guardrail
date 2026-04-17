"""
C99 — Production-oriented backing store contract.

Extends the C86 storage-adapter boundary toward a production-ready
persistence contract. Preserves append-only semantics, lineage, scope
metadata, idempotency state, and deterministic retrieval. No business
logic lives in this layer.
"""
from __future__ import annotations
from copy import deepcopy
from threading import RLock
from typing import Any, Dict, List, Optional

PRODUCTION_STORAGE_VERSION = "production_storage_contract/v1"

# Closed store-kind vocabulary --------------------------------------------
STORE_RECORDS = "records"
STORE_SNAPSHOTS = "snapshots"
STORE_IDEMPOTENCY = "idempotency"
STORE_SCOPE_INDEX = "scope_index"

_ALL_STORES = frozenset({
    STORE_RECORDS, STORE_SNAPSHOTS, STORE_IDEMPOTENCY, STORE_SCOPE_INDEX,
})

# Result status vocabulary -------------------------------------------------
STATUS_OK = "ok"
STATUS_APPEND_VIOLATION = "append_violation"
STATUS_UNKNOWN_STORE = "unknown_store"
STATUS_NOT_FOUND = "not_found"
STATUS_INTEGRITY_ERROR = "integrity_error"


class ProductionStorageContract:
    """Append-only, lineage-aware, scope-aware storage facade.

    Wraps an underlying adapter (C86 InMemory/File) and adds contract
    enforcement. Every record written carries:
        - artifact_type, record_id, revision_sequence
        - supersedes / superseded_by for lineage
        - bid_id, job_id, org_id (scope metadata)
        - a stable content_hash for integrity verification
    """

    def __init__(self, adapter: Any) -> None:
        self._adapter = adapter
        self._lock = RLock()
        # In-process index mirrors what the adapter persists.
        self._scope_index: Dict[str, List[str]] = {}
        self._lineage: Dict[str, Dict[str, Any]] = {}
        self._idem_entries: Dict[str, Dict[str, Any]] = {}

    # --- write path --------------------------------------------------------
    def append_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            rid = record.get("record_id")
            if not rid:
                return _fail(STATUS_INTEGRITY_ERROR,
                              reason="missing_record_id")

            # Append-only: reject any attempt to overwrite an existing
            # record_id's content. Revisions must come as *new* records
            # that supersede the prior record_id.
            existing = self._lineage.get(rid)
            if existing is not None:
                return _fail(STATUS_APPEND_VIOLATION,
                              reason="record_id_already_exists",
                              record_id=rid)

            normalized = _normalize_record(record)
            self._adapter.append_to_list(_key(STORE_RECORDS), normalized)
            self._lineage[rid] = {
                "record_id": rid,
                "supersedes": normalized.get("supersedes"),
                "superseded_by": None,
                "revision_sequence": normalized.get("revision_sequence"),
                "artifact_type": normalized.get("artifact_type"),
            }

            # Backfill lineage on the prior record, if any.
            sup = normalized.get("supersedes")
            if sup and sup in self._lineage:
                self._lineage[sup]["superseded_by"] = rid

            # Scope index.
            for scope_key in _scope_keys(normalized):
                self._scope_index.setdefault(scope_key, []).append(rid)

            return {
                "production_storage_version": PRODUCTION_STORAGE_VERSION,
                "status": STATUS_OK,
                "record_id": rid,
                "content_hash": normalized.get("content_hash"),
                "revision_sequence": normalized.get("revision_sequence"),
            }

    def record_idempotency_entry(self, key: str,
                                   payload_hash: str,
                                   record_id: str) -> Dict[str, Any]:
        with self._lock:
            entry = {
                "key": key,
                "payload_hash": payload_hash,
                "record_id": record_id,
            }
            self._adapter.append_to_list(_key(STORE_IDEMPOTENCY), entry)
            self._idem_entries[key] = deepcopy(entry)
            return {
                "production_storage_version": PRODUCTION_STORAGE_VERSION,
                "status": STATUS_OK,
                "key": key,
            }

    def save_snapshot(self, snapshot: Dict[str, Any],
                       snapshot_key: str) -> Dict[str, Any]:
        with self._lock:
            self._adapter.put(snapshot_key, snapshot)
            self._adapter.append_to_list(_key(STORE_SNAPSHOTS),
                                           {"snapshot_key": snapshot_key,
                                            "integrity_hash":
                                              snapshot.get("integrity_hash"),
                                            "record_count":
                                              len(snapshot.get("records") or [])})
            return {
                "production_storage_version": PRODUCTION_STORAGE_VERSION,
                "status": STATUS_OK,
                "snapshot_key": snapshot_key,
            }

    # --- read path ---------------------------------------------------------
    def list_records(self) -> List[Dict[str, Any]]:
        return self._adapter.list_items(_key(STORE_RECORDS))

    def records_for_scope(self, bid_id: Optional[str] = None,
                          job_id: Optional[str] = None,
                          org_id: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock:
            rids: Optional[set] = None
            for label, value in (("bid_id", bid_id), ("job_id", job_id),
                                  ("org_id", org_id)):
                if value is None:
                    continue
                key = f"{label}:{value}"
                ids = set(self._scope_index.get(key, []))
                rids = ids if rids is None else (rids & ids)
            all_records = self.list_records()
            if rids is None:
                return [deepcopy(r) for r in all_records]
            return [deepcopy(r) for r in all_records
                    if r.get("record_id") in rids]

    def lineage_for(self, record_id: str) -> List[Dict[str, Any]]:
        chain: List[Dict[str, Any]] = []
        seen: set = set()
        cur = self._lineage.get(record_id)
        while cur and cur["record_id"] not in seen:
            seen.add(cur["record_id"])
            chain.append(deepcopy(cur))
            nxt = cur.get("superseded_by")
            if not nxt:
                break
            cur = self._lineage.get(nxt)
        return chain

    def idempotency_entry(self, key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            entry = self._idem_entries.get(key)
            return deepcopy(entry) if entry else None

    def get_snapshot(self, snapshot_key: str) -> Optional[Dict[str, Any]]:
        return self._adapter.get(snapshot_key)

    def list_snapshot_keys(self) -> List[str]:
        return [item.get("snapshot_key")
                for item in self._adapter.list_items(_key(STORE_SNAPSHOTS))]

    def store_summary(self) -> Dict[str, Any]:
        records = self.list_records()
        by_type: Dict[str, int] = {}
        for r in records:
            t = r.get("artifact_type")
            by_type[t] = by_type.get(t, 0) + 1
        return {
            "production_storage_version": PRODUCTION_STORAGE_VERSION,
            "record_count": len(records),
            "records_by_type": dict(sorted(by_type.items())),
            "idempotency_count": len(self._idem_entries),
            "scope_index_size": len(self._scope_index),
            "lineage_size": len(self._lineage),
            "snapshot_keys": self.list_snapshot_keys(),
        }


def mirror_repository(repository: Any,
                      contract: ProductionStorageContract) -> Dict[str, Any]:
    """Deterministically mirror an ArtifactRepository into the contract.

    Used during cut-over: every existing record is appended once, in
    order. Append-only invariants are preserved by append_record().
    """
    appended: List[str] = []
    violations: List[Dict[str, Any]] = []
    for rec in repository.all_records():
        out = contract.append_record(rec)
        if out.get("status") == STATUS_OK:
            appended.append(out["record_id"])
        else:
            violations.append(out)
    return {
        "production_storage_version": PRODUCTION_STORAGE_VERSION,
        "mirrored_count": len(appended),
        "violation_count": len(violations),
        "violations": violations[:20],
    }


def restore_repository(contract: ProductionStorageContract,
                       repository: Any) -> Dict[str, Any]:
    """Load contract records into a fresh repository instance."""
    repository._records = []
    repository._by_type = {}
    repository._by_bid = {}
    repository._by_record_id = {}
    repository._lineage_chain = {}
    count = 0
    for r in contract.list_records():
        idx = len(repository._records)
        repository._records.append(deepcopy(r))
        repository._by_type.setdefault(r.get("artifact_type"), []).append(idx)
        env = (r.get("envelope") or {}).get("artifact") or {}
        bid = env.get("bid_id") or (env.get("package_ref") or {}).get("bid_id")
        if bid:
            repository._by_bid.setdefault(bid, []).append(idx)
        rid = r.get("record_id")
        if rid:
            repository._by_record_id.setdefault(rid, []).append(idx)
        sup = r.get("supersedes")
        if sup:
            repository._lineage_chain.setdefault(sup, []).append(rid)
        count += 1
    return {
        "production_storage_version": PRODUCTION_STORAGE_VERSION,
        "restored_count": count,
        "repository_summary": repository.repository_summary(),
    }


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _key(store_kind: str) -> str:
    if store_kind not in _ALL_STORES:
        raise ValueError(f"unknown_store_kind:{store_kind}")
    return f"production:{store_kind}"


def _fail(status: str, **kwargs) -> Dict[str, Any]:
    return {
        "production_storage_version": PRODUCTION_STORAGE_VERSION,
        "status": status,
        **kwargs,
    }


def _normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    rec = deepcopy(record)
    if "content_hash" not in rec:
        rec["content_hash"] = _content_hash(rec)
    return rec


def _content_hash(record: Dict[str, Any]) -> str:
    import hashlib
    import json
    try:
        blob = json.dumps(record, sort_keys=True, separators=(",", ":"),
                           default=repr)
    except TypeError:
        blob = repr(record)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _scope_keys(record: Dict[str, Any]) -> List[str]:
    env = record.get("envelope") or {}
    art = env.get("artifact") or {}
    keys: List[str] = []
    bid = art.get("bid_id") or (art.get("package_ref") or {}).get("bid_id")
    job = art.get("job_id")
    org = record.get("org_id")
    if bid:
        keys.append(f"bid_id:{bid}")
    if job:
        keys.append(f"job_id:{job}")
    if org:
        keys.append(f"org_id:{org}")
    return keys
