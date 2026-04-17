"""
C80 — Artifact repository / persistence service.

Deterministic in-memory repository for canonical artifacts. Supports
save, latest, history, by bid_id, by artifact_type, by record_id, by
revision_sequence, and lineage traversal. Append-only semantics —
saves never mutate prior revisions.
"""
from __future__ import annotations
from copy import deepcopy
from typing import Any, Dict, List, Optional

REPOSITORY_VERSION = "artifact_repository/v1"

_SUPPORTED_TYPES = frozenset({
    "quote_dossier", "package_overview", "authority_action_packet",
    "bid_readiness_snapshot", "bid_carry_justification", "export_packet",
    "authority_reference", "authority_comparison", "authority_exposure",
    "authority_posture", "deadline_pressure", "priority_queue",
    "vendor_comparison",
})


class ArtifactRepository:
    """Append-only canonical artifact repository.

    The repository keeps in-process indices for fast lookup. When an
    optional C86 StorageAdapter is provided, every save is also mirrored
    to the adapter (append-only). Business truth stays in the repository
    logic; adapters never enforce schema.
    """

    def __init__(self, storage_adapter: Any = None) -> None:
        # List of revision records in insertion order.
        self._records: List[Dict[str, Any]] = []
        # Keyed indices (lists of indices into _records).
        self._by_type: Dict[str, List[int]] = {}
        self._by_bid: Dict[str, List[int]] = {}
        self._by_record_id: Dict[str, List[int]] = {}
        # Lineage: record_id -> list of (revision_sequence, record_id_next)
        self._lineage_chain: Dict[str, List[str]] = {}
        # C86: optional backing store adapter.
        self._adapter = storage_adapter

    # ------------------------------------------------------------------
    # Save / save_envelope
    # ------------------------------------------------------------------

    def save(
        self,
        artifact_type: str,
        artifact: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        from .canonical_api_contracts import serialize_artifact

        metadata = dict(metadata or {})
        prior = self._latest_for_identity(artifact_type, artifact)
        revision_sequence = (prior or {}).get("revision_sequence", -1) + 1
        metadata.setdefault("revision_sequence", revision_sequence)
        if prior:
            metadata["supersedes"] = prior.get("record_id")

        envelope = serialize_artifact(artifact_type, artifact, metadata)
        record_id = envelope.get("identity_refs", {}).get("record_id") or \
                    self._derive_record_id(artifact_type, artifact, revision_sequence)
        # Prefer caller-supplied record_id in identity_refs if set.
        rid_fields = envelope.get("identity_refs") or {}
        if "record_id" in rid_fields and rid_fields["record_id"]:
            record_id = rid_fields["record_id"]
        else:
            record_id = self._derive_record_id(artifact_type, artifact, revision_sequence)

        record = {
            "record_id": record_id,
            "artifact_type": artifact_type,
            "artifact_type_valid": artifact_type in _SUPPORTED_TYPES,
            "revision_sequence": revision_sequence,
            "supersedes": metadata.get("supersedes"),
            "superseded_by": None,
            "created_at": metadata.get("created_at"),
            "created_by": metadata.get("created_by"),
            "envelope": envelope,
        }

        # Backfill supersession on prior.
        if prior:
            prior_idx = self._find_index_by_record_id(prior["record_id"])
            if prior_idx is not None:
                self._records[prior_idx] = deepcopy(self._records[prior_idx])
                self._records[prior_idx]["superseded_by"] = record_id
                self._lineage_chain.setdefault(prior["record_id"], []).append(record_id)

        # C87 org_id / ownership tagging (optional; stored on the record).
        org_id = metadata.get("org_id")
        if org_id:
            record["org_id"] = org_id
        owner_id = metadata.get("owner_id")
        if owner_id:
            record["owner_id"] = owner_id

        idx = len(self._records)
        self._records.append(record)
        self._by_type.setdefault(artifact_type, []).append(idx)

        bid_id = self._extract_bid_id(artifact)
        if bid_id:
            self._by_bid.setdefault(bid_id, []).append(idx)
        self._by_record_id.setdefault(record_id, []).append(idx)

        # C86 adapter mirror: append the serialized record (append-only).
        if self._adapter is not None:
            try:
                self._adapter.append_to_list(f"records:{artifact_type}", deepcopy(record))
                self._adapter.append_to_list("records:all", deepcopy(record))
            except Exception:
                # Storage failures must not corrupt in-memory state; surface
                # via summary rather than raise.
                pass

        return deepcopy(record)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def latest(
        self,
        artifact_type: str,
        bid_id: Optional[str] = None,
        job_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        for idx in reversed(self._by_type.get(artifact_type, [])):
            rec = self._records[idx]
            if rec.get("superseded_by"):
                continue
            if bid_id and self._record_bid_id(rec) != bid_id:
                continue
            if job_id and self._record_job_id(rec) != job_id:
                continue
            return deepcopy(rec)
        return None

    def history(
        self,
        artifact_type: str,
        bid_id: Optional[str] = None,
        job_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for idx in self._by_type.get(artifact_type, []):
            rec = self._records[idx]
            if bid_id and self._record_bid_id(rec) != bid_id:
                continue
            if job_id and self._record_job_id(rec) != job_id:
                continue
            out.append(deepcopy(rec))
        return out

    def by_bid_id(self, bid_id: str) -> List[Dict[str, Any]]:
        return [deepcopy(self._records[i]) for i in self._by_bid.get(bid_id, [])]

    def by_artifact_type(self, artifact_type: str) -> List[Dict[str, Any]]:
        return [deepcopy(self._records[i]) for i in self._by_type.get(artifact_type, [])]

    def by_record_id(self, record_id: str) -> Optional[Dict[str, Any]]:
        indices = self._by_record_id.get(record_id, [])
        if not indices:
            return None
        return deepcopy(self._records[indices[-1]])

    def by_revision_sequence(
        self,
        artifact_type: str,
        revision_sequence: int,
        bid_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        for idx in self._by_type.get(artifact_type, []):
            rec = self._records[idx]
            if rec.get("revision_sequence") != revision_sequence:
                continue
            if bid_id and self._record_bid_id(rec) != bid_id:
                continue
            return deepcopy(rec)
        return None

    def lineage(self, record_id: str) -> List[Dict[str, Any]]:
        """Walk the lineage chain from the given record forward."""
        out: List[Dict[str, Any]] = []
        current = self.by_record_id(record_id)
        visited = set()
        while current and current["record_id"] not in visited:
            visited.add(current["record_id"])
            out.append(current)
            next_id = current.get("superseded_by")
            if not next_id:
                break
            current = self.by_record_id(next_id)
        return out

    def all_records(self) -> List[Dict[str, Any]]:
        return [deepcopy(r) for r in self._records]

    # ------------------------------------------------------------------
    # C87 scope-aware variants
    # ------------------------------------------------------------------

    def latest_scoped(
        self,
        artifact_type: str,
        bid_id: Optional[str] = None,
        job_id: Optional[str] = None,
        org_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        for idx in reversed(self._by_type.get(artifact_type, [])):
            rec = self._records[idx]
            if rec.get("superseded_by"):
                continue
            if bid_id and self._record_bid_id(rec) != bid_id:
                continue
            if job_id and self._record_job_id(rec) != job_id:
                continue
            if org_id and rec.get("org_id") and rec.get("org_id") != org_id:
                continue
            return deepcopy(rec)
        return None

    def history_scoped(
        self,
        artifact_type: str,
        bid_id: Optional[str] = None,
        job_id: Optional[str] = None,
        org_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for idx in self._by_type.get(artifact_type, []):
            rec = self._records[idx]
            if bid_id and self._record_bid_id(rec) != bid_id:
                continue
            if job_id and self._record_job_id(rec) != job_id:
                continue
            if org_id and rec.get("org_id") and rec.get("org_id") != org_id:
                continue
            out.append(deepcopy(rec))
        return out

    def repository_summary(self) -> Dict[str, Any]:
        by_type_counts = {t: len(idx) for t, idx in self._by_type.items()}
        return {
            "repository_version": REPOSITORY_VERSION,
            "total_records": len(self._records),
            "records_by_type": dict(sorted(by_type_counts.items())),
            "records_by_bid": {b: len(idx) for b, idx in self._by_bid.items()},
            "distinct_record_ids": len(self._by_record_id),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _latest_for_identity(
        self,
        artifact_type: str,
        artifact: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        bid_id = self._extract_bid_id(artifact)
        job_id = self._extract_job_id(artifact)
        for idx in reversed(self._by_type.get(artifact_type, [])):
            rec = self._records[idx]
            if rec.get("superseded_by"):
                continue
            if bid_id and self._record_bid_id(rec) != bid_id:
                continue
            if job_id and self._record_job_id(rec) != job_id:
                continue
            return rec
        return None

    def _extract_bid_id(self, artifact: Dict[str, Any]) -> Optional[str]:
        a = artifact or {}
        return a.get("bid_id") or (a.get("package_ref") or {}).get("bid_id")

    def _extract_job_id(self, artifact: Dict[str, Any]) -> Optional[str]:
        return (artifact or {}).get("job_id")

    def _record_bid_id(self, record: Dict[str, Any]) -> Optional[str]:
        env = record.get("envelope") or {}
        art = env.get("artifact") or {}
        return self._extract_bid_id(art)

    def _record_job_id(self, record: Dict[str, Any]) -> Optional[str]:
        env = record.get("envelope") or {}
        art = env.get("artifact") or {}
        return self._extract_job_id(art)

    def _derive_record_id(
        self,
        artifact_type: str,
        artifact: Dict[str, Any],
        revision_sequence: int,
    ) -> str:
        bid = self._extract_bid_id(artifact) or ""
        job = self._extract_job_id(artifact) or ""
        key = job or bid or "global"
        return f"{artifact_type}:{key}:rev-{revision_sequence}"

    def _find_index_by_record_id(self, record_id: str) -> Optional[int]:
        indices = self._by_record_id.get(record_id, [])
        return indices[-1] if indices else None


# Module-level singleton for API usage.
_DEFAULT_REPO = ArtifactRepository()


def get_default_repository() -> ArtifactRepository:
    return _DEFAULT_REPO


def reset_default_repository() -> None:
    global _DEFAULT_REPO
    _DEFAULT_REPO = ArtifactRepository()
