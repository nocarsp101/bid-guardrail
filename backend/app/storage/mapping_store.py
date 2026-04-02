# backend/app/storage/mapping_store.py
"""
Named mapping storage for line-number-to-DOT-item mappings.

Stores mappings as JSON files under {data_dir}/mappings/{name}.json.
Each file is a self-describing record with metadata (who saved it, when, entry count).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List


class MappingStore:
    def __init__(self, data_dir: str):
        self.mappings_dir = os.path.join(data_dir, "mappings")
        os.makedirs(self.mappings_dir, exist_ok=True)

    def _path(self, name: str) -> str:
        return os.path.join(self.mappings_dir, f"{name}.json")

    def save(self, name: str, mapping: Dict[str, str], actor: str) -> Dict[str, Any]:
        """Save a mapping. Returns the stored record (with metadata)."""
        record = {
            "name": name,
            "mapping": mapping,
            "saved_by": actor,
            "saved_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "entry_count": len(mapping),
        }
        with open(self._path(name), "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2)
        return record

    def load(self, name: str) -> Dict[str, str]:
        """Load the mapping dict by name. Raises KeyError if not found."""
        path = self._path(name)
        if not os.path.isfile(path):
            raise KeyError(f"Mapping not found: {name}")
        with open(path, "r", encoding="utf-8") as f:
            record = json.load(f)
        return record["mapping"]

    def load_record(self, name: str) -> Dict[str, Any]:
        """Load the full record (mapping + metadata). Raises KeyError if not found."""
        path = self._path(name)
        if not os.path.isfile(path):
            raise KeyError(f"Mapping not found: {name}")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def list_names(self) -> List[str]:
        """Return sorted list of saved mapping names."""
        names = []
        for fn in sorted(os.listdir(self.mappings_dir)):
            if fn.endswith(".json"):
                names.append(fn[:-5])
        return names

    def exists(self, name: str) -> bool:
        return os.path.isfile(self._path(name))
