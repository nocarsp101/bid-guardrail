# backend/app/pdf_extraction/quote_normalization.py
"""
C10 — Governed quote normalization staging layer.

Sits between the C9 quote parser/validator and any future mapping /
reconciliation work. Produces an evidence-grade intermediate representation
with THREE clearly separated buckets:

    1. accepted_rows       — deterministic rows that passed parse+validate
    2. rejected_candidates — row-like material rejected, with traceability
    3. document_diagnostics — classification, extraction metadata, counters

Design intent:
    - classify document BEFORE any parser selection (inherited from C9)
    - DOT documents never flow through this layer
    - unknown documents never get normalized as quote
    - accepted_rows and rejected_candidates are never mingled
    - rejected candidates preserve raw_text + source_page + reason so the
      result is audit-friendly and debuggable
    - success requires at least one accepted_row; otherwise fail-closed
      with an explicit failure_reason (evidence is still returned)

This module does NOT:
    - modify C8A/C8B DOT parser behavior
    - perform mapping
    - guess qty/unit/price/item number
    - promote rejected candidates into accepted rows
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from .document_router import classify_document, collect_classification_signals
from .extractor import ExtractionError
from .quote_parser import parse_quote_rows
from .quote_validator import validate_quote_rows
from .quote_row_contract import build_accepted_row
from .quote_enrichment import enrich_quote_rows
from .quote_table_extraction import enrich_quote_rows_with_tables, detect_table_metadata
from .quote_multi_row_aggregation import aggregate_block_candidates
from .pattern_library import enrich_quote_rows_with_pattern_library
from . import service as _service
from . import quote_validator as _qv


STATUS_SUCCESS = "success"
STATUS_FAILED = "extraction_failed"
# Reserved for future partial-accept flows; C10 uses success/failed only.
STATUS_PARTIAL = "partial"


def normalize_quote_from_pdf(pdf_path: str) -> Dict[str, Any]:
    """
    Build the governed staging object for a quote-lane input.

    This entry point never routes a dot_schedule document through the quote
    lane. If classification returns dot_schedule, it raises an ExtractionError
    with failure_reason=unsupported_document_class — callers should use the
    DOT lane instead. If classification returns unknown, it returns a staged
    failure object with zero accepted_rows and preserved diagnostics.

    Returns a dict shaped per the C10 contract:
        {
            "document_class_detected": ...,
            "extraction_source": ...,
            "accepted_rows": [ ... ],
            "rejected_candidates": [ ... ],
            "document_diagnostics": { ... },
        }
    """
    pages, ocr_used, extraction_source = _service._acquire_text(pdf_path)
    signals = collect_classification_signals(pages)
    doc_class = classify_document(pages)

    # DOT documents are protected: the quote staging layer must never touch
    # them. Raise so the caller can route to the DOT lane explicitly.
    if doc_class == "dot_schedule":
        raise ExtractionError(
            "Document classified as dot_schedule. "
            "Quote normalization staging does not process DOT schedules.",
            meta={
                "failure_reason": _service.FAIL_UNSUPPORTED_CLASS,
                "document_class_detected": "dot_schedule",
                "extraction_source": extraction_source,
                "ocr_used": ocr_used,
                "classification_signals": signals,
            },
        )

    # Unknown documents: build a staged failure object. Still surface
    # diagnostics and any row-like evidence we can extract without guessing.
    if doc_class == "unknown":
        accepted, rejected, parse_meta, parse_failed = _try_parse(pages)
        diagnostics = _build_diagnostics(
            status=STATUS_FAILED,
            failure_reason=_service.FAIL_UNKNOWN_CLASS,
            signals=signals,
            parse_meta=parse_meta,
            validation_meta=None,
            ocr_used=ocr_used,
            extraction_source=extraction_source,
        )
        diagnostics["table_metadata"] = detect_table_metadata(pages) if pages else {}
        diagnostics["aggregation_meta"] = {
            "aggregator_version": "quote_multi_row_aggregation/v1",
            "blocks_attempted": 0, "blocks_promoted": 0,
        }
        return {
            "document_class_detected": "unknown",
            "extraction_source": extraction_source,
            "accepted_rows": [],
            "rejected_candidates": rejected,
            "document_diagnostics": diagnostics,
        }

    # doc_class == "quote": run the parse → aggregate → enrich → validate
    # pipeline and split accepted rows from rejected candidates.
    accepted, parser_rejected, parse_meta, parse_failed = _try_parse(pages)

    # C24 deterministic multi-row aggregation. Promotes explicit two-line
    # block candidates (line_ref + description on line 1, price-only on
    # line 2) into accepted rows. Unpromoted blocks stay in rejected
    # with a refined reason. Runs BEFORE enrichment so promoted rows go
    # through the same E1/E2 pipeline.
    aggregation_meta: Dict[str, Any] = {
        "aggregator_version": "quote_multi_row_aggregation/v1",
        "blocks_attempted": 0, "blocks_promoted": 0,
    }
    if accepted or parser_rejected:
        accepted, parser_rejected, aggregation_meta = aggregate_block_candidates(
            accepted, parser_rejected
        )
        # If the single-line parser failed to accept any rows but C24
        # promoted at least one grouped row, the pipeline is no longer in
        # a parse-failure state. Clear the flag so downstream success
        # handling runs on the promoted rows.
        if parse_failed and aggregation_meta["blocks_promoted"] > 0:
            parse_failed = False

    # C20 deterministic enrichment pass. Runs BEFORE validation so the
    # validator's qty*unit_price≈amount consistency rule acts as the
    # final safety net. Enrichment never mutates its inputs.
    if accepted:
        accepted = enrich_quote_rows(accepted)
        # C23 header-gated table enrichment. Only fires on rows that
        # C20 did not already enrich and whose page carries an explicit
        # table header.
        accepted = enrich_quote_rows_with_tables(accepted, pages)
        # C27 pattern library expansion — currently rule C27-U1 (dotted
        # unit normalization). Only fires on rows still lacking qty/unit.
        accepted = enrich_quote_rows_with_pattern_library(accepted)

    # C23 table metadata is captured for diagnostics regardless of whether
    # enrichment fired, so downstream callers can see what the layout
    # detector observed.
    table_metadata_snapshot = detect_table_metadata(pages) if pages else {}

    if parse_failed:
        # No accepted rows at all. Parse meta already carries the failure
        # reason and preserved rejected candidates.
        diagnostics = _build_diagnostics(
            status=STATUS_FAILED,
            failure_reason=parse_meta.get(
                "failure_reason", _service.FAIL_NO_CANDIDATE_QUOTE_ROWS
            ),
            signals=signals,
            parse_meta=parse_meta,
            validation_meta=None,
            ocr_used=ocr_used,
            extraction_source=extraction_source,
        )
        diagnostics["table_metadata"] = table_metadata_snapshot
        diagnostics["aggregation_meta"] = aggregation_meta
        return {
            "document_class_detected": "quote",
            "extraction_source": extraction_source,
            "accepted_rows": [],
            "rejected_candidates": parser_rejected,
            "document_diagnostics": diagnostics,
        }

    # Validator pass: move validator-rejects into rejected_candidates with
    # reason tags, keep only fully-valid rows as accepted.
    accepted_rows_normalized: List[Dict[str, Any]] = []
    validator_rejected: List[Dict[str, Any]] = []
    validation_meta: Dict[str, Any] = {
        "rows_input": 0,
        "rows_valid": 0,
        "rows_rejected": 0,
    }

    try:
        valid_rows, rejected_rows_raw, validation_meta = validate_quote_rows(accepted)
        for row in valid_rows:
            accepted_rows_normalized.append(
                build_accepted_row(row, extraction_source, ocr_used)
            )
        # Convert validator-rejects into the standard rejected_candidate shape.
        for row in rejected_rows_raw:
            validator_rejected.append(
                _validator_row_to_candidate(row, extraction_source)
            )
    except ExtractionError as e:
        # All rows failed validation → no accepted rows; convert every
        # parser-candidate row into a rejected_candidate with its validator
        # issue list.
        validation_meta = dict(e.meta or {})
        for row in accepted:
            issues = _qv._check_quote_row(row)
            reason = issues[0] if issues else _qv.V_NO_MONETARY
            validator_rejected.append({
                "candidate_id": f"v{row.get('source_page', 0)}-{row.get('row_id', 0)}",
                "raw_text": _row_to_raw(row),
                "source_page": row.get("source_page", 0),
                "rejection_reason": reason,
                "candidate_type": "row_like",
                "extraction_source": extraction_source,
            })

    all_rejected = parser_rejected + validator_rejected
    for candidate in all_rejected:
        candidate.setdefault("extraction_source", extraction_source)

    if not accepted_rows_normalized:
        diagnostics = _build_diagnostics(
            status=STATUS_FAILED,
            failure_reason=validation_meta.get(
                "failure_reason", _qv.REASON_STRUCTURE_INSUFFICIENT
            ),
            signals=signals,
            parse_meta=parse_meta,
            validation_meta=validation_meta,
            ocr_used=ocr_used,
            extraction_source=extraction_source,
        )
        diagnostics["table_metadata"] = table_metadata_snapshot
        diagnostics["aggregation_meta"] = aggregation_meta
        return {
            "document_class_detected": "quote",
            "extraction_source": extraction_source,
            "accepted_rows": [],
            "rejected_candidates": all_rejected,
            "document_diagnostics": diagnostics,
        }

    diagnostics = _build_diagnostics(
        status=STATUS_SUCCESS,
        failure_reason=None,
        signals=signals,
        parse_meta=parse_meta,
        validation_meta=validation_meta,
        ocr_used=ocr_used,
        extraction_source=extraction_source,
    )
    diagnostics["table_metadata"] = table_metadata_snapshot
    diagnostics["aggregation_meta"] = aggregation_meta
    return {
        "document_class_detected": "quote",
        "extraction_source": extraction_source,
        "accepted_rows": accepted_rows_normalized,
        "rejected_candidates": all_rejected,
        "document_diagnostics": diagnostics,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _try_parse(
    pages: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any], bool]:
    """
    Run the quote parser, catching its fail-closed ExtractionError and
    returning the preserved rejected_candidates so the staging layer can
    still surface evidence on failure.
    """
    try:
        accepted, rejected, parse_meta = parse_quote_rows(pages)
        return accepted, rejected, parse_meta, False
    except ExtractionError as e:
        meta = dict(e.meta or {})
        rejected = list(meta.get("rejected_candidates") or [])
        meta.pop("rejected_candidates", None)  # keep in top-level bucket only
        return [], rejected, meta, True


def _validator_row_to_candidate(
    row: Dict[str, Any],
    extraction_source: str,
) -> Dict[str, Any]:
    """Project a validator-rejected row into the C10 rejected_candidate schema."""
    issues = row.get("_rejection_reasons") or []
    reason = issues[0] if issues else _qv.V_NO_MONETARY
    return {
        "candidate_id": f"v{row.get('source_page', 0)}-{row.get('row_id', 0)}",
        "raw_text": _row_to_raw(row),
        "source_page": row.get("source_page", 0),
        "rejection_reason": reason,
        "candidate_type": "row_like",
        "extraction_source": extraction_source,
    }


def _row_to_raw(row: Dict[str, Any]) -> str:
    """
    Reconstruct a readable raw-text approximation from a parsed row when the
    original line isn't carried on the row dict. Used only for validator
    rejects (parser-level rejects carry the true raw_text).
    """
    parts: List[str] = []
    if row.get("line_ref"):
        parts.append(str(row["line_ref"]))
    if row.get("description"):
        parts.append(str(row["description"]))
    if row.get("unit_price") is not None:
        parts.append(f"${row['unit_price']:,.2f}")
    if row.get("amount") is not None:
        parts.append(f"${row['amount']:,.2f}")
    return " ".join(parts)


def _build_diagnostics(
    status: str,
    failure_reason: str | None,
    signals: Dict[str, Any],
    parse_meta: Dict[str, Any] | None,
    validation_meta: Dict[str, Any] | None,
    ocr_used: bool,
    extraction_source: str,
) -> Dict[str, Any]:
    """Assemble the document_diagnostics bucket."""
    parse_meta = parse_meta or {}
    validation_meta = validation_meta or {}
    candidate_counts = {
        "accepted_rows": parse_meta.get("rows_detected", 0)
        if status == STATUS_SUCCESS
        else 0,
        "parser_candidates_rejected": parse_meta.get("candidates_rejected", 0),
        "validator_candidates_rejected": validation_meta.get("rows_rejected", 0),
        "block_candidates": parse_meta.get("block_candidates", 0),
    }
    if status != STATUS_SUCCESS:
        candidate_counts["accepted_rows"] = 0
    return {
        "status": status,
        "failure_reason": failure_reason,
        "classification_signals": signals,
        "candidate_counts": candidate_counts,
        "rejection_counts": parse_meta.get("rejection_counts", {}),
        "parse_format": parse_meta.get("format"),
        "ocr_used": ocr_used,
        "extraction_source": extraction_source,
    }
