"""
C93 — Idempotency + write safety layer.

Deterministic idempotency protection for artifact save flows,
revision creation flows, and API write endpoints. Prevents duplicate
writes from retries/replays while preserving append-only semantics.
Never mutates prior revisions.
"""
from __future__ import annotations
import hashlib
import json
from copy import deepcopy
from threading import RLock
from typing import Any, Callable, Dict, Optional, Tuple

IDEMPOTENCY_VERSION = "idempotency/v1"

# --- status vocabulary ----------------------------------------------------
STATUS_NEW = "new_write"
STATUS_REPLAY = "replay_hit"
STATUS_CONFLICT = "conflict"
STATUS_MISSING_KEY = "missing_idempotency_key"


class IdempotencyStore:
    """Stable key -> (payload_hash, response) store.

    Replays return the cached response; conflicting payloads under the
    same key surface as explicit conflicts without mutating the record.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._by_key: Dict[str, Dict[str, Any]] = {}

    def lookup(self, key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            rec = self._by_key.get(key)
            return deepcopy(rec) if rec else None

    def commit(self, key: str, payload_hash: str, response: Dict[str, Any]) -> None:
        with self._lock:
            self._by_key[key] = {
                "key": key,
                "payload_hash": payload_hash,
                "response": deepcopy(response),
            }

    def clear(self) -> None:
        with self._lock:
            self._by_key.clear()

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "idempotency_version": IDEMPOTENCY_VERSION,
                "entry_count": len(self._by_key),
                "keys": sorted(self._by_key.keys()),
            }


# Module-level singleton reused by the API layer.
_DEFAULT_STORE = IdempotencyStore()


def get_default_idempotency_store() -> IdempotencyStore:
    return _DEFAULT_STORE


def reset_default_idempotency_store() -> None:
    global _DEFAULT_STORE
    _DEFAULT_STORE = IdempotencyStore()


def compute_payload_hash(payload: Any) -> str:
    """Canonical, deterministic payload hash."""
    try:
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                           default=_json_default)
    except TypeError:
        blob = repr(payload)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (set, frozenset)):
        return sorted(list(obj))
    return repr(obj)


def perform_idempotent(
    key: Optional[str],
    payload: Any,
    writer: Callable[[], Dict[str, Any]],
    store: Optional[IdempotencyStore] = None,
) -> Dict[str, Any]:
    """Execute `writer()` at most once per key.

    Returns a diagnostic envelope:
        {"status": one of STATUS_*, "response": {...}, ...}
    Conflict: same key, different payload hash -> does not call writer.
    Replay: same key, same payload hash -> returns cached response.
    New: no prior key -> calls writer, caches response.
    Missing key: calls writer (no caching), marks status as missing_key.
    """
    store = store or _DEFAULT_STORE
    payload_hash = compute_payload_hash(payload)
    if not key:
        resp = writer()
        return _envelope(STATUS_MISSING_KEY, None, payload_hash, resp,
                          duplicate=False)

    prior = store.lookup(key)
    if prior is None:
        resp = writer()
        store.commit(key, payload_hash, resp)
        return _envelope(STATUS_NEW, key, payload_hash, resp, duplicate=False)

    if prior["payload_hash"] == payload_hash:
        # Replay: return cached response, do not re-invoke writer.
        return _envelope(STATUS_REPLAY, key, payload_hash,
                          prior["response"], duplicate=True)

    # Same key, different payload — conflict.
    return _envelope(STATUS_CONFLICT, key, payload_hash, None,
                      duplicate=True, prior_hash=prior["payload_hash"],
                      prior_response=prior["response"])


def _envelope(status: str,
              key: Optional[str],
              payload_hash: str,
              response: Optional[Dict[str, Any]],
              duplicate: bool,
              prior_hash: Optional[str] = None,
              prior_response: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "idempotency_version": IDEMPOTENCY_VERSION,
        "status": status,
        "idempotency_key": key,
        "payload_hash": payload_hash,
        "duplicate": duplicate,
        "response": deepcopy(response) if response is not None else None,
    }
    if prior_hash is not None:
        out["prior_payload_hash"] = prior_hash
    if prior_response is not None:
        out["prior_response"] = deepcopy(prior_response)
    return out


def idempotent_save_artifact(
    repository: Any,
    idempotency_key: Optional[str],
    artifact_type: str,
    artifact: Dict[str, Any],
    metadata: Optional[Dict[str, Any]] = None,
    store: Optional[IdempotencyStore] = None,
) -> Dict[str, Any]:
    """Idempotent wrapper around repository.save()."""
    payload = {
        "artifact_type": artifact_type,
        "artifact": artifact,
        "metadata": metadata or {},
    }

    def _writer() -> Dict[str, Any]:
        return repository.save(artifact_type, artifact, metadata)

    return perform_idempotent(idempotency_key, payload, _writer, store=store)
