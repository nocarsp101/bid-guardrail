# backend/app/pdf_extraction/service.py
"""
PDF Schedule Extraction Service — orchestrator.

Pipeline: extract text → detect schedule → parse rows → validate → normalize output

Produces structured bid rows matching the canonical schema
and a detailed extraction summary for audit.
"""
from __future__ import annotations

from typing import Dict, Any, List, Tuple

from .extractor import extract_pages_text, ExtractionError
from .schedule_detector import detect_schedule_pages
from .row_parser import parse_schedule_rows
from .validator import validate_extracted_rows


def extract_bid_items_from_pdf(
    pdf_path: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Full extraction pipeline: PDF → structured bid rows.

    Returns:
        (rows, summary)

    rows: list of normalized bid item dicts:
        {
            "line_number": "0520",
            "item": "2524-6765010",
            "description": "REMOVE AND REINSTALL SIGN AS PER PLAN",
            "qty": 1.0,
            "unit": "EACH",
            "source_page": 0,
            "extraction_source": "native_pdf",
        }

    summary: extraction diagnostics dict

    Raises ExtractionError on any failure (fail-closed).
    """
    # Step 1: Extract raw text from all pages
    pages = extract_pages_text(pdf_path)

    # Step 2: Detect schedule pages
    schedule_page_indices = detect_schedule_pages(pages)

    # Step 3: Parse rows from schedule pages
    raw_rows, parse_meta = parse_schedule_rows(pages, schedule_page_indices)

    # Step 4: Validate rows
    valid_rows, rejected_rows, validation_meta = validate_extracted_rows(raw_rows)

    # Step 5: Normalize output into canonical shape
    normalized = _normalize_rows(valid_rows)

    # Build summary
    summary: Dict[str, Any] = {
        "pages_scanned": len(pages),
        "schedule_pages_detected": schedule_page_indices,
        "native_text_detected": True,
        "rows_detected": parse_meta["rows_detected"],
        "rows_extracted": len(normalized),
        "rows_rejected": validation_meta["rows_rejected"],
        "rejected_samples": parse_meta.get("rejected_samples", []),
        "extraction_source": "native_pdf",
        "format_detected": parse_meta.get("format_detected", "unknown"),
        "status": "success",
    }

    return normalized, summary


def _normalize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
            "extraction_source": "native_pdf",
        })
    return normalized
