# backend/app/pdf_extraction/service.py
"""
PDF Extraction Service — orchestrator.

Two separate extraction lanes:
    A. DOT schedule (C8A/C8B, LOCKED):
         PDF -> native/OCR text -> schedule detector -> DOT row parser -> validator
    B. Quote (C9):
         PDF -> native/OCR text -> quote parser -> quote validator

Document routing determines which lane is used. OCR is ONLY an upstream
text-acquisition layer for both lanes. Classification happens BEFORE parser
selection.

All failure paths emit an explicit `failure_reason` code so callers can
distinguish between unsupported document classes, ambiguous classifications,
insufficient text, and lane-specific extraction failures.
"""
from __future__ import annotations

from typing import Dict, Any, List, Tuple

from .extractor import extract_pages_text_permissive, ExtractionError
from .schedule_detector import detect_schedule_pages
from .row_parser import parse_schedule_rows
from .validator import validate_extracted_rows
from .document_router import classify_document, collect_classification_signals
from .quote_parser import parse_quote_rows
from .quote_validator import validate_quote_rows

# Minimum total chars across all pages to consider native text "sufficient".
# Below this threshold, OCR fallback is triggered.
_MIN_NATIVE_TEXT_CHARS = 200

# Minimum total chars after OCR to even attempt classification.
_MIN_CLASSIFIABLE_CHARS = 40


# ---------------------------------------------------------------------------
# Explicit failure reason codes — exposed on every fail-closed path.
# ---------------------------------------------------------------------------
FAIL_UNSUPPORTED_CLASS = "unsupported_document_class"
FAIL_UNKNOWN_CLASS = "unknown_document_class"
FAIL_INSUFFICIENT_TEXT = "insufficient_text_for_classification"
FAIL_QUOTE_STRUCTURE_INSUFFICIENT = "quote_structure_insufficient"
FAIL_QUOTE_ROWS_NOT_DETERMINISTIC = "quote_rows_not_deterministic"
FAIL_NO_CANDIDATE_QUOTE_ROWS = "no_candidate_quote_rows"
FAIL_DOT_SCHEDULE_NOT_PARSEABLE = "dot_schedule_not_parseable"
FAIL_OCR_UNAVAILABLE = "ocr_unavailable"


def extract_bid_items_from_pdf(
    pdf_path: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Full C8 extraction pipeline: PDF -> structured DOT bid rows.

    C8A/C8B LOCKED: parser behavior must not change.
    This function is the fail-closed entry for the DOT lane.

    Raises ExtractionError on any failure with a `failure_reason` in meta.
    """
    pages, ocr_used, extraction_source = _acquire_text(pdf_path)

    try:
        schedule_page_indices = detect_schedule_pages(pages)
        raw_rows, parse_meta = parse_schedule_rows(pages, schedule_page_indices)
        valid_rows, rejected_rows, validation_meta = validate_extracted_rows(raw_rows)
    except ExtractionError as e:
        meta = dict(e.meta or {})
        meta.setdefault("failure_reason", FAIL_DOT_SCHEDULE_NOT_PARSEABLE)
        meta["extraction_source"] = extraction_source
        meta["ocr_used"] = ocr_used
        meta["document_class_detected"] = "dot_schedule"
        raise ExtractionError(str(e), meta=meta)

    normalized = _normalize_rows(valid_rows, extraction_source)

    summary: Dict[str, Any] = {
        "pages_scanned": len(pages),
        "schedule_pages_detected": schedule_page_indices,
        "native_text_detected": not ocr_used,
        "ocr_used": ocr_used,
        "ocr_pages": len(pages) if ocr_used else 0,
        "rows_detected": parse_meta["rows_detected"],
        "rows_extracted": len(normalized),
        "rows_rejected": validation_meta["rows_rejected"],
        "rejected_samples": parse_meta.get("rejected_samples", []),
        "extraction_source": extraction_source,
        "format_detected": parse_meta.get("format_detected", "unknown"),
        "document_class": "dot_schedule",
        "document_class_detected": "dot_schedule",
        "failure_reason": None,
        "status": "success",
    }

    return normalized, summary


def extract_quote_from_pdf(
    pdf_path: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Quote extraction pipeline: PDF -> structured quote rows.

    Completely separate from the DOT schedule lane. Uses the quote-specific
    parser and validator. Classification is performed for diagnostic
    reporting but does NOT gate this entry point — a caller that explicitly
    posts to /extract/quote/pdf is asserting the document is a quote.

    Raises ExtractionError on any failure with a `failure_reason` in meta.
    """
    pages, ocr_used, extraction_source = _acquire_text(pdf_path)

    # Classification is informational for the explicit quote entry.
    doc_class_detected = classify_document(pages)
    signals = collect_classification_signals(pages)

    # Step: parse quote rows
    try:
        raw_rows, _rejected_candidates, parse_meta = parse_quote_rows(pages)
    except ExtractionError as e:
        meta = dict(e.meta or {})
        reason = meta.get("failure_reason") or FAIL_NO_CANDIDATE_QUOTE_ROWS
        meta["failure_reason"] = reason
        meta["extraction_source"] = extraction_source
        meta["ocr_used"] = ocr_used
        meta["document_class_detected"] = doc_class_detected
        meta["classification_signals"] = signals
        raise ExtractionError(str(e), meta=meta)

    # Step: validate quote rows
    try:
        valid_rows, rejected_rows, validation_meta = validate_quote_rows(raw_rows)
    except ExtractionError as e:
        meta = dict(e.meta or {})
        reason = meta.get("failure_reason") or FAIL_QUOTE_STRUCTURE_INSUFFICIENT
        meta["failure_reason"] = reason
        meta["extraction_source"] = extraction_source
        meta["ocr_used"] = ocr_used
        meta["document_class_detected"] = doc_class_detected
        raise ExtractionError(str(e), meta=meta)

    normalized = _normalize_quote_rows(valid_rows, extraction_source)

    summary: Dict[str, Any] = {
        "document_class": "quote",
        "document_class_detected": doc_class_detected,
        "pages_scanned": len(pages),
        "native_text_detected": not ocr_used,
        "ocr_used": ocr_used,
        "ocr_pages": len(pages) if ocr_used else 0,
        "rows_detected": parse_meta["rows_detected"],
        "rows_extracted": len(normalized),
        "rows_rejected": validation_meta["rows_rejected"],
        "extraction_source": extraction_source,
        "failure_reason": None,
        "classification_signals": signals,
        "status": "success",
    }

    return normalized, summary


def extract_pdf_auto(
    pdf_path: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Auto-routing extraction: classify document BEFORE parser selection.

    Contract:
        - dot_schedule -> delegate to C8 DOT lane
        - quote        -> delegate to C9 quote lane
        - unknown      -> fail-closed with FAIL_UNKNOWN_CLASS

    Never forces a document through the wrong lane. Classification is the
    sole gate — the DOT parser is never invoked on a quote or unknown input.
    """
    pages, ocr_used, extraction_source = _acquire_text(pdf_path)

    signals = collect_classification_signals(pages)

    if signals["non_ws_chars"] < _MIN_CLASSIFIABLE_CHARS:
        raise ExtractionError(
            "Insufficient text for classification. "
            "Document has no readable content after native-text and OCR passes.",
            meta={
                "failure_reason": FAIL_INSUFFICIENT_TEXT,
                "document_class_detected": "unknown",
                "extraction_source": extraction_source,
                "ocr_used": ocr_used,
                "classification_signals": signals,
            },
        )

    doc_class = classify_document(pages)

    if doc_class == "dot_schedule":
        rows, summary = extract_bid_items_from_pdf(pdf_path)
        summary["document_class"] = "dot_schedule"
        summary["document_class_detected"] = "dot_schedule"
        summary["classification_signals"] = signals
        return rows, summary

    if doc_class == "quote":
        rows, summary = extract_quote_from_pdf(pdf_path)
        summary["document_class"] = "quote"
        summary["document_class_detected"] = "quote"
        summary["classification_signals"] = signals
        return rows, summary

    # doc_class == "unknown" — fail closed, never guess.
    raise ExtractionError(
        "Document type could not be determined. "
        "Not recognized as a DOT proposal schedule or a structured quote.",
        meta={
            "failure_reason": FAIL_UNKNOWN_CLASS,
            "document_class_detected": "unknown",
            "extraction_source": extraction_source,
            "ocr_used": ocr_used,
            "classification_signals": signals,
        },
    )


def _acquire_text(pdf_path: str) -> Tuple[List[Dict[str, Any]], bool, str]:
    """
    Shared text acquisition: native text first, OCR fallback below threshold.

    Returns (pages, ocr_used, extraction_source).
    Raises ExtractionError with FAIL_OCR_UNAVAILABLE if OCR is needed but
    not installed.
    """
    pages = extract_pages_text_permissive(pdf_path)
    total_native_chars = sum(p["char_count"] for p in pages)

    if total_native_chars >= _MIN_NATIVE_TEXT_CHARS:
        return pages, False, "native_pdf"

    try:
        from .ocr import ocr_pages as _ocr_pages
        pages = _ocr_pages(pdf_path)
    except ExtractionError:
        raise
    except ImportError as e:
        raise ExtractionError(
            f"OCR fallback requires pytesseract: {e}",
            meta={
                "failure_reason": FAIL_OCR_UNAVAILABLE,
                "native_chars": total_native_chars,
                "component": "ocr",
            },
        )
    return pages, True, "ocr_pdf"


def _normalize_rows(
    rows: List[Dict[str, Any]],
    extraction_source: str,
) -> List[Dict[str, Any]]:
    """Normalize DOT rows into the canonical bid row output shape."""
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        normalized.append({
            "line_number": row["line_number"],
            "item": row["item"],
            "description": row["description"],
            "qty": row["qty"],
            "unit": row["unit"],
            "source_page": row.get("source_page"),
            "extraction_source": extraction_source,
        })
    return normalized


def _normalize_quote_rows(
    rows: List[Dict[str, Any]],
    extraction_source: str,
) -> List[Dict[str, Any]]:
    """Normalize quote rows into the quote output shape."""
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        normalized.append({
            "row_id": row["row_id"],
            "line_ref": row.get("line_ref"),
            "description": row["description"],
            "qty": row.get("qty"),
            "unit": row.get("unit"),
            "unit_price": row.get("unit_price"),
            "amount": row.get("amount"),
            "source_page": row.get("source_page"),
            "extraction_source": extraction_source,
        })
    return normalized
