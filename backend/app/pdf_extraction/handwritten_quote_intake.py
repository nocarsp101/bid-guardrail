"""
C41 — Handwritten quote intake hardening.

Detects when a quote PDF cannot be reliably extracted by the machine
pipeline and captures explicit evidence blocks for manual interpretation.
The output is a deterministic intake object that downstream layers
(C42 manual interpretation, C43 hybrid evaluation) can consume.

Hard rules:
    - Never guesses handwritten content.
    - Never drops unreadable regions silently; every non-empty OCR
      region is preserved as an evidence block.
    - Never overwrites machine-extracted accepted_rows.
    - Deterministic classification thresholds. Same input → same status.
    - Status vocabulary is a CLOSED set.
    - Limitation reason vocabulary is a CLOSED set.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

HANDWRITTEN_INTAKE_VERSION = "handwritten_quote_intake/v1"

# Closed intake-status vocabulary.
STATUS_MACHINE_READABLE = "machine_readable"
STATUS_MACHINE_PARTIAL_HUMAN_REQUIRED = "machine_partial_human_required"
STATUS_MACHINE_UNREADABLE_HUMAN_REQUIRED = "machine_unreadable_human_required"

# Closed limitation-reason vocabulary.
REASON_LOW_OCR_RELIABILITY = "low_ocr_reliability"
REASON_FRAGMENTED_HANDWRITING = "fragmented_handwriting"
REASON_UNREADABLE_NUMERIC_FIELDS = "unreadable_numeric_fields"
REASON_NO_STABLE_ROW_BOUNDARIES = "no_stable_row_boundaries"
REASON_PARTIAL_ROW_DETECTION_ONLY = "partial_row_detection_only"
REASON_QUOTE_STRUCTURE_INSUFFICIENT = "quote_structure_insufficient_for_machine_read"
REASON_EVIDENCE_BLOCKS_CAPTURED = "evidence_blocks_captured_for_manual_review"

# Closed block-type vocabulary.
BLOCK_TYPE_LINE = "line"
BLOCK_TYPE_REGION = "region"
BLOCK_TYPE_ROW_LIKE = "row_like"
BLOCK_TYPE_UNREADABLE_REGION = "unreadable_region"

# Closed machine_readability vocabulary (per-block).
BLOCK_READABILITY_READABLE = "readable"
BLOCK_READABILITY_PARTIAL = "partial"
BLOCK_READABILITY_UNREADABLE = "unreadable"

# Thresholds (deterministic; not heuristic).
_PARTIAL_READ_RATIO = 2.0   # rejected_count >= accepted_count * this → partial
_MIN_CHARS_PER_PAGE_OCR = 80  # below → low OCR reliability


def run_intake(pdf_path: str) -> Dict[str, Any]:
    """
    Run the C41 intake pipeline and classify machine readability.

    Returns an intake dict with:
        - machine_intake_status (closed)
        - intake_limitation_reasons (closed list)
        - accepted_rows (from staging, untouched if any)
        - evidence_blocks (captured for manual interpretation)
        - intake_summary (counts)
        - staging_diagnostics (pass-through from C10 staging)
        - document_class_detected / extraction_source
    """
    # Lazy imports so callers can unit-test without running the full stack.
    from .quote_normalization import normalize_quote_from_pdf
    from .extractor import ExtractionError
    from . import service as _service

    staging: Dict[str, Any]
    pages: List[Dict[str, Any]] = []
    extraction_source: str = "unknown"
    ocr_used: bool = False

    # Stage 1: staging pipeline (may raise or return failure dict).
    try:
        staging = normalize_quote_from_pdf(pdf_path)
    except ExtractionError as e:
        meta = e.meta or {}
        # Build a synthetic failed staging dict so the intake logic below
        # can still capture evidence.
        staging = {
            "document_class_detected": meta.get("document_class_detected", "unknown"),
            "extraction_source": meta.get("extraction_source", "unknown"),
            "accepted_rows": [],
            "rejected_candidates": list(meta.get("rejected_candidates") or []),
            "document_diagnostics": {
                "status": "extraction_failed",
                "failure_reason": meta.get("failure_reason"),
                "ocr_used": meta.get("ocr_used", False),
                "extraction_source": meta.get("extraction_source", "unknown"),
                "classification_signals": meta.get("classification_signals"),
            },
        }

    # Acquire the raw pages for evidence block capture. We re-run the
    # text acquisition layer with the same permissive rules the staging
    # pipeline used. This never re-runs OCR on a file that was already
    # parsed as native PDF.
    try:
        pages, ocr_used, extraction_source = _service._acquire_text(pdf_path)
    except ExtractionError:
        pages = []
        ocr_used = False
        extraction_source = "unknown"

    accepted_rows = list(staging.get("accepted_rows") or [])
    rejected_candidates = list(staging.get("rejected_candidates") or [])

    evidence_blocks = _capture_evidence_blocks(
        pages=pages,
        accepted_rows=accepted_rows,
        rejected_candidates=rejected_candidates,
        staging=staging,
    )

    machine_intake_status, limitation_reasons = _classify_intake_status(
        accepted_rows=accepted_rows,
        rejected_candidates=rejected_candidates,
        staging=staging,
        pages=pages,
        ocr_used=ocr_used,
        evidence_blocks=evidence_blocks,
    )

    return {
        "handwritten_intake_version": HANDWRITTEN_INTAKE_VERSION,
        "pdf_path": pdf_path,
        "document_class_detected": staging.get("document_class_detected"),
        "extraction_source": extraction_source or staging.get("extraction_source"),
        "ocr_used": ocr_used,
        "machine_intake_status": machine_intake_status,
        "intake_limitation_reasons": limitation_reasons,
        "accepted_rows": deepcopy(accepted_rows),
        "rejected_candidates": deepcopy(rejected_candidates),
        "evidence_blocks": evidence_blocks,
        "intake_summary": {
            "accepted_rows_count": len(accepted_rows),
            "rejected_candidates_count": len(rejected_candidates),
            "evidence_blocks_count": len(evidence_blocks),
            "partial_blocks_count": sum(
                1 for b in evidence_blocks
                if b["machine_readability"] == BLOCK_READABILITY_PARTIAL
            ),
            "unreadable_blocks_count": sum(
                1 for b in evidence_blocks
                if b["machine_readability"] == BLOCK_READABILITY_UNREADABLE
            ),
            "readable_blocks_count": sum(
                1 for b in evidence_blocks
                if b["machine_readability"] == BLOCK_READABILITY_READABLE
            ),
            "page_count": len(pages),
        },
        "staging_diagnostics": staging.get("document_diagnostics") or {},
    }


# ---------------------------------------------------------------------------
# Evidence block capture
# ---------------------------------------------------------------------------

def _capture_evidence_blocks(
    pages: List[Dict[str, Any]],
    accepted_rows: List[Dict[str, Any]],
    rejected_candidates: List[Dict[str, Any]],
    staging: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Build the deterministic evidence-block list for manual review.

    Every page becomes at least one block, so callers can always pin
    manual entries to a page reference. Rejected candidates from the
    C9 parser are preserved as row_like blocks. Fully unreadable pages
    become unreadable_region blocks.
    """
    blocks: List[Dict[str, Any]] = []
    ordinal = 0

    # Readable_row_ids are the `source_page` indices of accepted rows
    # on each page — used to decide whether a page is fully unreadable.
    accepted_by_page: Dict[int, int] = {}
    for r in accepted_rows:
        p = int(r.get("source_page") or 0)
        accepted_by_page[p] = accepted_by_page.get(p, 0) + 1

    for page in pages:
        page_idx = int(page.get("page_index") or 0)
        page_text = (page.get("text") or "").strip()
        char_count = int(page.get("char_count") or 0)
        ocr_used = bool(page.get("ocr_used"))

        if not page_text:
            blocks.append(_make_block(
                ordinal=ordinal, page_idx=page_idx, raw_text="",
                ocr_text="", block_type=BLOCK_TYPE_UNREADABLE_REGION,
                readability=BLOCK_READABILITY_UNREADABLE,
                capture_reason=REASON_UNREADABLE_NUMERIC_FIELDS,
                trace_extra={"page_empty": True},
            ))
            ordinal += 1
            continue

        has_accepted = accepted_by_page.get(page_idx, 0) > 0

        # Decide readability at page granularity first.
        if has_accepted:
            readability = BLOCK_READABILITY_READABLE
            block_type = BLOCK_TYPE_REGION
            capture_reason = REASON_EVIDENCE_BLOCKS_CAPTURED
        elif char_count >= _MIN_CHARS_PER_PAGE_OCR:
            readability = BLOCK_READABILITY_PARTIAL
            block_type = BLOCK_TYPE_REGION
            capture_reason = REASON_PARTIAL_ROW_DETECTION_ONLY
        else:
            readability = BLOCK_READABILITY_UNREADABLE
            block_type = BLOCK_TYPE_UNREADABLE_REGION
            capture_reason = REASON_LOW_OCR_RELIABILITY

        blocks.append(_make_block(
            ordinal=ordinal, page_idx=page_idx,
            raw_text=page_text[:4000],  # bounded for storage safety
            ocr_text=page_text[:4000] if ocr_used else "",
            block_type=block_type,
            readability=readability,
            capture_reason=capture_reason,
            trace_extra={
                "page_char_count": char_count,
                "has_accepted_rows_on_page": has_accepted,
            },
        ))
        ordinal += 1

    # Preserve parser-rejected candidates as row_like blocks. These are
    # extra signal for manual review on top of the page-level region.
    for cand in rejected_candidates:
        raw_text = (cand.get("raw_text") or "")[:2000]
        if not raw_text.strip():
            continue
        blocks.append({
            "block_id": f"blk-{ordinal}",
            "source_page": int(cand.get("source_page") or 0),
            "block_type": BLOCK_TYPE_ROW_LIKE,
            "raw_text": raw_text,
            "ocr_text": raw_text,
            "machine_readability": BLOCK_READABILITY_PARTIAL,
            "capture_reason": cand.get("rejection_reason") or REASON_PARTIAL_ROW_DETECTION_ONLY,
            "capture_trace": {
                "origin": "parser_rejected_candidate",
                "candidate_id": cand.get("candidate_id"),
                "candidate_type": cand.get("candidate_type"),
            },
        })
        ordinal += 1

    return blocks


def _make_block(
    ordinal: int,
    page_idx: int,
    raw_text: str,
    ocr_text: str,
    block_type: str,
    readability: str,
    capture_reason: str,
    trace_extra: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "block_id": f"blk-{ordinal}",
        "source_page": page_idx,
        "block_type": block_type,
        "raw_text": raw_text,
        "ocr_text": ocr_text,
        "machine_readability": readability,
        "capture_reason": capture_reason,
        "capture_trace": {
            "origin": "page_scan",
            **trace_extra,
        },
    }


# ---------------------------------------------------------------------------
# Intake status classification
# ---------------------------------------------------------------------------

def _classify_intake_status(
    accepted_rows: List[Dict[str, Any]],
    rejected_candidates: List[Dict[str, Any]],
    staging: Dict[str, Any],
    pages: List[Dict[str, Any]],
    ocr_used: bool,
    evidence_blocks: List[Dict[str, Any]],
) -> tuple:
    """Return (status, limitation_reasons).

    Thresholds are fixed:
      - accepted_rows == 0             → unreadable_human_required
      - rejected >= accepted * 2       → partial_human_required
      - otherwise                      → machine_readable
    """
    reasons: List[str] = []
    doc_class = staging.get("document_class_detected")

    if doc_class != "quote":
        reasons.append(REASON_QUOTE_STRUCTURE_INSUFFICIENT)
    if ocr_used and _low_ocr_chars(pages):
        reasons.append(REASON_LOW_OCR_RELIABILITY)

    if len(accepted_rows) == 0:
        reasons.append(REASON_NO_STABLE_ROW_BOUNDARIES)
        reasons.append(REASON_EVIDENCE_BLOCKS_CAPTURED)
        return STATUS_MACHINE_UNREADABLE_HUMAN_REQUIRED, sorted(set(reasons))

    # At least one accepted row. Check for partial state.
    rejected_count = len(rejected_candidates)
    accepted_count = len(accepted_rows)
    if rejected_count >= accepted_count * _PARTIAL_READ_RATIO:
        reasons.append(REASON_PARTIAL_ROW_DETECTION_ONLY)
        reasons.append(REASON_EVIDENCE_BLOCKS_CAPTURED)
        return STATUS_MACHINE_PARTIAL_HUMAN_REQUIRED, sorted(set(reasons))

    if reasons:
        # Readable but with some soft reasons surfaced — still readable.
        reasons.append(REASON_EVIDENCE_BLOCKS_CAPTURED)
    return STATUS_MACHINE_READABLE, sorted(set(reasons))


def _low_ocr_chars(pages: List[Dict[str, Any]]) -> bool:
    if not pages:
        return True
    total = sum(int(p.get("char_count") or 0) for p in pages)
    avg = total / len(pages)
    return avg < _MIN_CHARS_PER_PAGE_OCR
