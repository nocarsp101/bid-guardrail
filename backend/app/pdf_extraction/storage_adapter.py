"""
C86 — Backing store adapter layer.

Storage adapter boundary beneath the artifact repository. In-memory +
file/dev adapters. Append-only semantics, deep-copy isolation, stable
record identity. Storage never enforces business truth.
"""
from __future__ import annotations
import json
import os
from copy import deepcopy
from threading import RLock
from typing import Any, Dict, List, Optional

STORAGE_ADAPTER_VERSION = "storage_adapter/v1"


class StorageAdapter:
    """Abstract adapter contract."""

    def put(self, key: str, value: Dict[str, Any]) -> None:
        raise NotImplementedError

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def append_to_list(self, key: str, value: Dict[str, Any]) -> int:
        raise NotImplementedError

    def list_items(self, key: str) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def all_keys(self) -> List[str]:
        raise NotImplementedError

    def clear(self) -> None:
        raise NotImplementedError

    def adapter_summary(self) -> Dict[str, Any]:
        return {"storage_adapter_version": STORAGE_ADAPTER_VERSION,
                "adapter_type": self.__class__.__name__}


class InMemoryStorageAdapter(StorageAdapter):
    def __init__(self) -> None:
        self._lock = RLock()
        self._kv: Dict[str, Dict[str, Any]] = {}
        self._lists: Dict[str, List[Dict[str, Any]]] = {}

    def put(self, key: str, value: Dict[str, Any]) -> None:
        with self._lock:
            self._kv[key] = deepcopy(value)

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            v = self._kv.get(key)
            return deepcopy(v) if v is not None else None

    def append_to_list(self, key: str, value: Dict[str, Any]) -> int:
        with self._lock:
            self._lists.setdefault(key, []).append(deepcopy(value))
            return len(self._lists[key]) - 1

    def list_items(self, key: str) -> List[Dict[str, Any]]:
        with self._lock:
            return [deepcopy(item) for item in self._lists.get(key, [])]

    def all_keys(self) -> List[str]:
        with self._lock:
            return sorted(set(self._kv.keys()) | set(self._lists.keys()))

    def clear(self) -> None:
        with self._lock:
            self._kv.clear()
            self._lists.clear()


class FileStorageAdapter(StorageAdapter):
    """Append-only JSON file adapter for dev/demo.

    Each list key maps to `<base_dir>/<slug(key)>.jsonl` (append-only).
    Each kv key maps to `<base_dir>/kv/<slug(key)>.json`.
    """

    def __init__(self, base_dir: str) -> None:
        self._base = base_dir
        self._lock = RLock()
        os.makedirs(os.path.join(self._base, "kv"), exist_ok=True)
        os.makedirs(os.path.join(self._base, "lists"), exist_ok=True)

    def _kv_path(self, key: str) -> str:
        return os.path.join(self._base, "kv", self._slug(key) + ".json")

    def _list_path(self, key: str) -> str:
        return os.path.join(self._base, "lists", self._slug(key) + ".jsonl")

    @staticmethod
    def _slug(key: str) -> str:
        return "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in key)

    def put(self, key: str, value: Dict[str, Any]) -> None:
        with self._lock:
            with open(self._kv_path(key), "w", encoding="utf-8") as f:
                json.dump(value, f)

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            path = self._kv_path(key)
            if not os.path.exists(path):
                return None
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)

    def append_to_list(self, key: str, value: Dict[str, Any]) -> int:
        with self._lock:
            path = self._list_path(key)
            # Count existing lines for the index.
            existing = 0
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    for _ in f:
                        existing += 1
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(value) + "\n")
            return existing

    def list_items(self, key: str) -> List[Dict[str, Any]]:
        with self._lock:
            path = self._list_path(key)
            if not os.path.exists(path):
                return []
            out: List[Dict[str, Any]] = []
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        out.append(json.loads(line))
            return out

    def all_keys(self) -> List[str]:
        with self._lock:
            keys = set()
            kv_dir = os.path.join(self._base, "kv")
            list_dir = os.path.join(self._base, "lists")
            if os.path.isdir(kv_dir):
                for name in os.listdir(kv_dir):
                    if name.endswith(".json"):
                        keys.add(name[:-5])
            if os.path.isdir(list_dir):
                for name in os.listdir(list_dir):
                    if name.endswith(".jsonl"):
                        keys.add(name[:-6])
            return sorted(keys)

    def clear(self) -> None:
        import shutil
        with self._lock:
            if os.path.isdir(self._base):
                shutil.rmtree(self._base)
            os.makedirs(os.path.join(self._base, "kv"), exist_ok=True)
            os.makedirs(os.path.join(self._base, "lists"), exist_ok=True)


def build_adapter(kind: str, **kwargs) -> StorageAdapter:
    if kind == "in_memory":
        return InMemoryStorageAdapter()
    if kind == "file":
        base = kwargs.get("base_dir")
        if not base:
            raise ValueError("file adapter requires base_dir")
        return FileStorageAdapter(base)
    raise ValueError(f"unknown_adapter_kind: {kind}")
