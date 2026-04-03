# backend/app/pdf_extraction/service.py
"""
PDF Schedule Extraction Service — orchestrator.

Pipeline:
    PDF → native-text attempt
        → if sufficient text → C8A parse/validate path
        → if text absent/insufficient → OCR fallback → same parse/validate path
    → normalized output OR explicit failure

OCR is ONLY an upstream text acquisition layer.
The deterministic parser/validator decides whether text is usable.
"""
from __future__ import annotations

from typing import Dict, Any, List, Tuple

from .extractor import extract_pages_text_permissive, ExtractionError
from .schedule_detector import detect_schedule_pages
from .row_parser import parse_schedule_rows
from .validator import validate_extracted_rows

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
