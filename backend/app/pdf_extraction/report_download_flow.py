"""
C105 — Downloadable report artifact flow.

Stable downloadable artifact output flow for estimator review,
authority action, bid readiness, and final carry justification
reports. Uses the render-ready payloads from C95 and the delivery
layer from C100. Preserves bid/package identity, revision metadata,
source refs, and deterministic structure. No inference or
presentation-side truth is introduced here.
"""
from __future__ import annotations
import hashlib
import json
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional

REPORT_DOWNLOAD_VERSION = "report_download_flow/v1"

# Closed report-kind vocabulary -------------------------------------------
REPORT_ESTIMATOR_REVIEW = "estimator_review_report"
REPORT_AUTHORITY_ACTION = "authority_action_report"
REPORT_BID_READINESS = "bid_readiness_report"
REPORT_FINAL_CARRY = "final_carry_report"

_ALL_REPORT_KINDS = frozenset({
    REPORT_ESTIMATOR_REVIEW, REPORT_AUTHORITY_ACTION,
    REPORT_BID_READINESS, REPORT_FINAL_CARRY,
})

# Closed extension vocabulary -> format mapping ---------------------------
_EXTENSIONS_BY_FORMAT = {
    "json": "json",
    "text": "txt",
    "markdown": "md",
    "structured": "json",
}


def list_report_kinds() -> List[str]:
    return sorted(_ALL_REPORT_KINDS)


def build_downloadable(
    repository: Any,
    report_kind: str,
    *,
    bid_id: Optional[str] = None,
    job_id: Optional[str] = None,
    revision_sequence: Optional[int] = None,
    output_format: str = "json",
) -> Dict[str, Any]:
    """Assemble a single deterministic downloadable artifact."""
    if report_kind not in _ALL_REPORT_KINDS:
        return _fail("unknown_report_kind", report_kind=report_kind)

    report = _build_report(repository, report_kind,
                            bid_id=bid_id, job_id=job_id,
                            revision_sequence=revision_sequence)
    if report is None:
        return _fail("report_build_failed", report_kind=report_kind)

    from .report_delivery import deliver_report, list_formats
    if output_format not in list_formats():
        return _fail("unknown_format", format=output_format)
    delivery = deliver_report(report, output_format=output_format)
    if delivery.get("delivery_status") != "ok":
        return _fail("delivery_error", delivery=delivery)

    body = delivery.get("body")
    if isinstance(body, (dict, list)):
        payload_bytes = json.dumps(body, sort_keys=True,
                                     separators=(",", ":"),
                                     default=repr).encode("utf-8")
    elif body is None:
        payload_bytes = b""
    else:
        payload_bytes = str(body).encode("utf-8")
    content_hash = hashlib.sha256(payload_bytes).hexdigest()

    identity = report.get("identity") or {}
    filename = _deterministic_filename(report_kind, identity, revision_sequence,
                                         output_format)

    return {
        "report_download_version": REPORT_DOWNLOAD_VERSION,
        "download_status": "ok",
        "report_kind": report_kind,
        "identity": deepcopy(identity),
        "state_labels": deepcopy(report.get("state_labels") or {}),
        "format": output_format,
        "content_type": delivery.get("content_type"),
        "filename": filename,
        "content_hash": content_hash,
        "byte_length": len(payload_bytes),
        "body": delivery.get("body"),
        "source_refs": deepcopy(report.get("source_refs") or []),
        "revision_metadata": _revision_metadata(report),
    }


def build_downloadable_bundle(
    repository: Any,
    *,
    bid_id: Optional[str] = None,
    job_id: Optional[str] = None,
    output_format: str = "json",
) -> Dict[str, Any]:
    """Bundle every applicable downloadable report for an identity."""
    downloads: List[Dict[str, Any]] = []
    kinds: List[str] = []
    if bid_id is not None:
        kinds.extend([REPORT_BID_READINESS, REPORT_AUTHORITY_ACTION,
                      REPORT_FINAL_CARRY])
    if job_id is not None:
        kinds.append(REPORT_ESTIMATOR_REVIEW)

    for kind in kinds:
        d = build_downloadable(repository, kind,
                                 bid_id=bid_id, job_id=job_id,
                                 output_format=output_format)
        downloads.append(d)

    ok = all(d.get("download_status") == "ok" for d in downloads)
    return {
        "report_download_version": REPORT_DOWNLOAD_VERSION,
        "download_status": "ok" if ok else "partial",
        "bid_id": bid_id,
        "job_id": job_id,
        "format": output_format,
        "download_count": len(downloads),
        "downloads": downloads,
    }


def persist_downloadable(download: Dict[str, Any], base_dir: str) -> Dict[str, Any]:
    """Persist a downloadable to disk, using the deterministic filename."""
    import os
    if download.get("download_status") != "ok":
        return {
            "report_download_version": REPORT_DOWNLOAD_VERSION,
            "persisted": False,
            "reason": download.get("reason") or "upstream_failure",
        }
    os.makedirs(base_dir, exist_ok=True)
    path = os.path.join(base_dir, download["filename"])
    body = download.get("body")
    if isinstance(body, (dict, list)):
        data = json.dumps(body, sort_keys=True, separators=(",", ":"),
                           default=repr)
    elif body is None:
        data = ""
    else:
        data = str(body)
    with open(path, "w", encoding="utf-8") as f:
        f.write(data)
    return {
        "report_download_version": REPORT_DOWNLOAD_VERSION,
        "persisted": True,
        "path": path,
        "bytes_written": len(data),
        "content_hash": download.get("content_hash"),
    }


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _build_report(repository: Any, report_kind: str, *,
                   bid_id: Optional[str], job_id: Optional[str],
                   revision_sequence: Optional[int]) -> Optional[Dict[str, Any]]:
    from .render_reports import (
        build_estimator_review_report, build_authority_action_report,
        build_bid_readiness_report, build_final_carry_report,
    )
    if report_kind == REPORT_ESTIMATOR_REVIEW:
        if not job_id:
            return None
        return build_estimator_review_report(repository, job_id,
                                              revision_sequence=revision_sequence)
    if report_kind == REPORT_AUTHORITY_ACTION:
        if not bid_id:
            return None
        return build_authority_action_report(repository, bid_id,
                                              revision_sequence=revision_sequence)
    if report_kind == REPORT_BID_READINESS:
        if not bid_id:
            return None
        return build_bid_readiness_report(repository, bid_id,
                                           revision_sequence=revision_sequence)
    if report_kind == REPORT_FINAL_CARRY:
        if not bid_id:
            return None
        return build_final_carry_report(repository, bid_id,
                                         revision_sequence=revision_sequence)
    return None


def _deterministic_filename(report_kind: str,
                              identity: Dict[str, Any],
                              revision_sequence: Optional[int],
                              output_format: str) -> str:
    parts: List[str] = [report_kind]
    bid = identity.get("bid_id")
    job = identity.get("job_id")
    if bid:
        parts.append(f"bid-{_slug(bid)}")
    if job:
        parts.append(f"job-{_slug(job)}")
    if revision_sequence is not None:
        parts.append(f"rev-{revision_sequence}")
    ext = _EXTENSIONS_BY_FORMAT.get(output_format, "bin")
    return "__".join(parts) + f".{ext}"


def _slug(s: Any) -> str:
    text = str(s or "unknown")
    return "".join(c if c.isalnum() or c in ("-", "_", ".") else "-"
                    for c in text)


def _revision_metadata(report: Dict[str, Any]) -> Dict[str, Any]:
    refs = report.get("source_refs") or []
    if not refs:
        return {"revision_sequence": None, "source_record_count": 0}
    primary = refs[0]
    return {
        "revision_sequence": primary.get("revision_sequence"),
        "primary_artifact_type": primary.get("artifact_type"),
        "primary_record_id": primary.get("record_id"),
        "source_record_count": len(refs),
    }


def _fail(reason: str, **kwargs) -> Dict[str, Any]:
    return {
        "report_download_version": REPORT_DOWNLOAD_VERSION,
        "download_status": "error",
        "reason": reason,
        **kwargs,
    }
