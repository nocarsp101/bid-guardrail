# backend/app/pdf_extraction/ocr.py
"""
OCR text acquisition layer for scanned/image-based PDFs.

Uses Tesseract via pytesseract to extract text from page images.
This module is ONLY a text acquisition step — it does NOT parse, validate,
or interpret the text. The deterministic parser/validator pipeline decides
whether OCR-produced text is usable.

Isolation: OCR ≠ parser ≠ validator ≠ reconciliation.
"""
from __future__ import annotations

import io
import os
import re
from typing import List, Dict, Any

import fitz  # PyMuPDF

from .extractor import ExtractionError

# Tesseract binary path — configurable via env var or auto-detected
_TESSERACT_CMD = os.environ.get(
    "TESSERACT_CMD",
    r"C:\Program Files\Tesseract-OCR\tesseract.exe",
)


def _get_pytesseract():
    """Lazy-import pytesseract and configure Tesseract path."""
    try:
        import pytesseract
    except ImportError:
        raise ExtractionError(
            "OCR requires pytesseract package. Install with: pip install pytesseract",
            meta={"component": "ocr"},
        )
    pytesseract.pytesseract.tesseract_cmd = _TESSERACT_CMD
    return pytesseract


def ocr_pages(pdf_path: str) -> List[Dict[str, Any]]:
    """
    OCR all pages of a PDF by rendering to images and running Tesseract.

    Returns list of dicts matching the same shape as extract_pages_text():
        {"page_index": int, "text": str, "char_count": int}

    Raises ExtractionError if:
        - PDF cannot be opened
        - Tesseract is not available
        - OCR produces no usable text on any page
    """
    pytesseract = _get_pytesseract()
    from PIL import Image

    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        raise ExtractionError(
            f"Cannot open PDF for OCR: {e}",
            meta={"pdf_path": pdf_path, "component": "ocr"},
        )

    if doc.page_count == 0:
        doc.close()
        raise ExtractionError(
            "PDF has zero pages.",
            meta={"pdf_path": pdf_path, "component": "ocr"},
        )

    pages: List[Dict[str, Any]] = []
    total_chars = 0

    for i in range(doc.page_count):
        page = doc.load_page(i)

        # Render page to image at 300 DPI for OCR quality
        pix = page.get_pixmap(dpi=300)
        img_bytes = pix.tobytes("png")
        img = Image.open(io.BytesIO(img_bytes))

        # Run Tesseract OCR
        try:
            raw_text = pytesseract.image_to_string(img, lang="eng")
        except Exception as e:
            raise ExtractionError(
                f"Tesseract OCR failed on page {i}: {e}",
                meta={"pdf_path": pdf_path, "page": i, "component": "ocr"},
            )

        # Deterministic normalization — safe transformations only
        text = _normalize_ocr_text(raw_text)

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
            "OCR produced no usable text from any page.",
            meta={"pdf_path": pdf_path, "page_count": len(pages), "component": "ocr"},
        )

    return pages


def _normalize_ocr_text(raw: str) -> str:
    """
    Deterministic OCR text normalization — safe transformations only.

    Acceptable:
        - Collapse multiple blank lines to single
        - Normalize dashes (em-dash, en-dash → hyphen)
        - Strip trailing whitespace per line
        - Normalize common OCR whitespace artifacts

    NOT acceptable:
        - Guessing missing digits
        - Replacing ambiguous characters
        - Inventing missing fields
    """
    text = raw

    # Normalize dash variants to ASCII hyphen (safe for DOT item numbers)
    text = text.replace("\u2013", "-")  # en-dash
    text = text.replace("\u2014", "-")  # em-dash
    text = text.replace("\u2212", "-")  # minus sign

    # Strip trailing whitespace per line
    lines = [line.rstrip() for line in text.splitlines()]

    # Collapse runs of 3+ blank lines to 1
    cleaned: List[str] = []
    blank_count = 0
    for line in lines:
        if not line.strip():
            blank_count += 1
            if blank_count <= 1:
                cleaned.append("")
        else:
            blank_count = 0
            cleaned.append(line)

    return "\n".join(cleaned)
