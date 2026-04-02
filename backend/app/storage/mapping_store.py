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
from typing import Any, Dict, List, Optional


class MappingStore:
    def __init__(self, data_dir: str):
        self.mappings_dir = os.path.join(data_dir, "mappings")
        os.makedirs(self.mappings_dir, exist_ok=True)

    def _path(self, name: str) -> str:
        return os.path.join(self.mappings_dir, f"{name}.json")

    def save(
        self,
        name: str,
        mapping: Dict[str, str],
        actor: str,
        project: Optional[str] = None,
        vendor: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Save a mapping with optional project/vendor context. Returns the stored record."""
        record = {
            "name": name,
            "mapping": mapping,
            "saved_by": actor,
            "saved_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "entry_count": len(mapping),
            "project": project,
            "vendor": vendor,
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

    def list_records(self) -> List[Dict[str, Any]]:
        """Return summary records (without the full mapping dict) for all saved mappings."""
        records = []
        for name in self.list_names():
            rec = self.load_record(name)
            records.append({
                "name": rec["name"],
                "project": rec.get("project"),
                "vendor": rec.get("vendor"),
                "entry_count": rec.get("entry_count", 0),
                "saved_by": rec.get("saved_by"),
                "saved_utc": rec.get("saved_utc"),
            })
        return records

    def find_by_context(
        self,
        project: Optional[str] = None,
        vendor: Optional[str] = None,
    ) -> List[str]:
        """
        Find mapping names matching project/vendor criteria.

        Deterministic exact match (case-insensitive).
        AND logic: all provided criteria must match.
        Returns empty list if no criteria provided.
        """
        if not project and not vendor:
            return []
        results = []
        for name in self.list_names():
            rec = self.load_record(name)
            if project:
                rec_project = (rec.get("project") or "").strip().lower()
                if rec_project != project.strip().lower():
                    continue
            if vendor:
                rec_vendor = (rec.get("vendor") or "").strip().lower()
                if rec_vendor != vendor.strip().lower():
                    continue
            results.append(name)
        return results

    def exists(self, name: str) -> bool:
        return os.path.isfile(self._path(name))
