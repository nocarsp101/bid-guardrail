# backend/app/pdf_extraction/extractor.py
"""
Raw text extraction from native-text PDFs using PyMuPDF.
No OCR fallback. Fails if no native text is found.
"""
from __future__ import annotations

from typing import List, Dict, Any

import fitz  # PyMuPDF


class ExtractionError(ValueError):
    """Raised when PDF text extraction fails deterministically."""

    def __init__(self, message: str, meta: Dict[str, Any] | None = None):
        super().__init__(message)
        self.meta = meta or {}


def extract_pages_text(pdf_path: str) -> List[Dict[str, Any]]:
    """
    Extract raw text from each page of a native-text PDF.

    Returns a list of dicts, one per page:
        {"page_index": int, "text": str, "char_count": int}

    Raises ExtractionError if:
        - PDF cannot be opened
        - PDF has zero pages
        - No native text detected on any page
    """
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        raise ExtractionError(
            f"Cannot open PDF: {e}",
            meta={"pdf_path": pdf_path},
        )

    if doc.page_count == 0:
        doc.close()
        raise ExtractionError(
            "PDF has zero pages.",
            meta={"pdf_path": pdf_path},
        )

    pages: List[Dict[str, Any]] = []
    total_chars = 0

    for i in range(doc.page_count):
        page = doc.load_page(i)
        text = page.get_text("text") or ""
        char_count = len(text.strip())
        total_chars += char_count
        pages.append({
            "page_index": i,
            "text": text,
            "char_count": char_count,
        })

    doc.close()

    if total_chars == 0:
        raise ExtractionError(
            "No native text detected in PDF. OCR may be required (out of scope for C8A).",
            meta={"pdf_path": pdf_path, "page_count": len(pages)},
        )

    return pages
