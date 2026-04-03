# backend/app/pdf_extraction/service.py
"""
PDF Extraction Service — orchestrator.

Two separate extraction lanes:
    A. DOT schedule: PDF → native/OCR text → schedule detector → DOT row parser → validator
    B. Quote: PDF → native/OCR text → quote parser → quote validator

Document routing determines which lane is used.
OCR is ONLY an upstream text acquisition layer for both lanes.
"""
from __future__ import annotations

from typing import Dict, Any, List, Tuple

from .extractor import extract_pages_text_permissive, ExtractionError
from .schedule_detector import detect_schedule_pages
from .row_parser import parse_schedule_rows
from .validator import validate_extracted_rows
from .document_router import classify_document
from .quote_parser import parse_quote_rows
from .quote_validator import validate_quote_rows

# Minimum total chars across all pages to consider native text "sufficient".
# Below this threshold, OCR fallback is triggered.
_MIN_NATIVE_TEXT_CHARS = 200


def extract_bid_items_from_pdf(
    pdf_path: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Full extraction pipeline: PDF → structured bid rows.

    Automatically detects whether native text is sufficient.
    If not, attempts OCR fallback (requires Tesseract).

    Returns:
        (rows, summary)

    Raises ExtractionError on any failure (fail-closed).
    """
    # Step 1: Attempt native text extraction (permissive — does not fail on empty)
    pages = extract_pages_text_permissive(pdf_path)
    total_native_chars = sum(p["char_count"] for p in pages)
    native_text_sufficient = total_native_chars >= _MIN_NATIVE_TEXT_CHARS

    ocr_used = False
    ocr_pages_count = 0

    if native_text_sufficient:
        # C8A path: use native text directly
        extraction_source = "native_pdf"
    else:
        # OCR fallback: re-extract text via Tesseract
        extraction_source = "ocr_pdf"
        ocr_used = True
        try:
            from .ocr import ocr_pages as _ocr_pages
            pages = _ocr_pages(pdf_path)
            ocr_pages_count = len(pages)
        except ExtractionError:
            raise  # OCR failure is explicit — propagate
        except ImportError as e:
            raise ExtractionError(
                f"OCR fallback requires pytesseract: {e}",
                meta={"native_chars": total_native_chars, "component": "ocr"},
            )

    # Step 2: Detect schedule pages (same logic for both paths)
    schedule_page_indices = detect_schedule_pages(pages)

    # Step 3: Parse rows from schedule pages (same parser for both paths)
    raw_rows, parse_meta = parse_schedule_rows(pages, schedule_page_indices)

    # Step 4: Validate rows (same validator for both paths)
    valid_rows, rejected_rows, validation_meta = validate_extracted_rows(raw_rows)

    # Step 5: Normalize output
    normalized = _normalize_rows(valid_rows, extraction_source)

    # Build summary with full provenance
    summary: Dict[str, Any] = {
        "pages_scanned": len(pages),
        "schedule_pages_detected": schedule_page_indices,
        "native_text_detected": native_text_sufficient,
        "ocr_used": ocr_used,
        "ocr_pages": ocr_pages_count,
        "rows_detected": parse_meta["rows_detected"],
        "rows_extracted": len(normalized),
        "rows_rejected": validation_meta["rows_rejected"],
        "rejected_samples": parse_meta.get("rejected_samples", []),
        "extraction_source": extraction_source,
        "format_detected": parse_meta.get("format_detected", "unknown"),
        "status": "success",
    }

    return normalized, summary


def extract_quote_from_pdf(
    pdf_path: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Quote extraction pipeline: PDF → structured quote rows.

    Separate from DOT schedule extraction. Uses quote-specific parser/validator.

    Returns:
        (rows, summary)

    Raises ExtractionError on any failure (fail-closed).
    """
    # Step 1: Get text (native or OCR)
    pages = extract_pages_text_permissive(pdf_path)
    total_native_chars = sum(p["char_count"] for p in pages)
    native_text_sufficient = total_native_chars >= _MIN_NATIVE_TEXT_CHARS

    ocr_used = False
    ocr_pages_count = 0

    if native_text_sufficient:
        extraction_source = "native_pdf"
    else:
        extraction_source = "ocr_pdf"
        ocr_used = True
        try:
            from .ocr import ocr_pages as _ocr_pages
            pages = _ocr_pages(pdf_path)
            ocr_pages_count = len(pages)
        except ExtractionError:
            raise
        except ImportError as e:
            raise ExtractionError(
                f"OCR fallback requires pytesseract: {e}",
                meta={"native_chars": total_native_chars, "component": "ocr"},
            )

    # Step 2: Classify document for metadata (informational, not a gate).
    # The explicit quote endpoint always attempts quote parsing.
    # The auto endpoint handles classification-based routing separately.
    doc_class_detected = classify_document(pages)

    # Step 3: Parse quote rows
    raw_rows, parse_meta = parse_quote_rows(pages)

    # Step 4: Validate quote rows
    valid_rows, rejected_rows, validation_meta = validate_quote_rows(raw_rows)

    # Step 5: Normalize output with provenance
    normalized = _normalize_quote_rows(valid_rows, extraction_source)

    summary: Dict[str, Any] = {
        "document_class": "quote",
        "document_class_detected": doc_class_detected,
        "pages_scanned": len(pages),
        "native_text_detected": native_text_sufficient,
        "ocr_used": ocr_used,
        "ocr_pages": ocr_pages_count,
        "rows_detected": parse_meta["rows_detected"],
        "rows_extracted": len(normalized),
        "rows_rejected": validation_meta["rows_rejected"],
        "extraction_source": extraction_source,
        "status": "success",
    }

    return normalized, summary


def extract_pdf_auto(
    pdf_path: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Auto-routing extraction: classify document, then route to correct pipeline.

    Returns (rows, summary) where summary includes document_class.
    """
    # Step 1: Get text
    pages = extract_pages_text_permissive(pdf_path)
    total_native_chars = sum(p["char_count"] for p in pages)

    if total_native_chars < _MIN_NATIVE_TEXT_CHARS:
        try:
            from .ocr import ocr_pages as _ocr_pages
            pages = _ocr_pages(pdf_path)
        except ExtractionError:
            raise
        except ImportError as e:
            raise ExtractionError(
                f"OCR fallback requires pytesseract: {e}",
                meta={"native_chars": total_native_chars},
            )

    # Step 2: Classify
    doc_class = classify_document(pages)

    if doc_class == "dot_schedule":
        rows, summary = extract_bid_items_from_pdf(pdf_path)
        summary["document_class"] = "dot_schedule"
        return rows, summary
    elif doc_class == "quote":
        return extract_quote_from_pdf(pdf_path)
    else:
        raise ExtractionError(
            "Document type could not be determined. "
            "Not recognized as a DOT proposal schedule or a structured quote.",
            meta={"document_class": "unknown"},
        )


def _acquire_text(pdf_path: str) -> tuple:
    """Shared text acquisition: native text or OCR fallback. Returns (pages, ocr_used, extraction_source)."""
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
            meta={"native_chars": total_native_chars},
        )
    return pages, True, "ocr_pdf"


def _normalize_rows(
    rows: List[Dict[str, Any]],
    extraction_source: str,
) -> List[Dict[str, Any]]:
    """Normalize parsed rows into the canonical bid row output shape."""
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
