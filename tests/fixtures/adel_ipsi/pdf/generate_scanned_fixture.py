#!/usr/bin/env python3
"""
Generate a scanned (image-only) DOT schedule PDF for OCR testing.

Renders rows in STACKED format (one field per line, row by row) — matching
the real Iowa DOT proposal layout that the stacked parser handles.
The result is an image-only PDF with zero native text, forcing OCR fallback.
"""
from __future__ import annotations

import json
from pathlib import Path

import fitz  # PyMuPDF


OUTPUT_DIR = Path(__file__).parent
TRUTH_PATH = OUTPUT_DIR / "extraction_truth.json"


def _format_qty(qty):
    if qty == int(qty):
        return f"{int(qty)}.000"
    return f"{qty:.3f}"


def generate_scanned_pdf(output_path: Path, row_count: int = 15):
    """
    Generate an image-only PDF with schedule rows in stacked DOT format.

    Each row is rendered as separate lines:
        line_number
        item_number
        description (may be multi-line)
        unit
        quantity
        _________._____  (price placeholder)
        _________._____  (price placeholder)
    """
    with open(TRUTH_PATH) as f:
        truth = json.load(f)

    rows = truth[:row_count]

    # Step 1: Create a text-based PDF with stacked layout
    src_doc = fitz.open()
    PAGE_W, PAGE_H = 612, 792  # Portrait (like real DOT proposals)
    FONT = "Courier"
    FSIZE = 9
    LINE_H = 12
    MARGIN_L = 50

    page = src_doc.new_page(width=PAGE_W, height=PAGE_H)
    y = 40

    # Page header (matching real DOT format)
    page.insert_text((MARGIN_L, y), "Contracts and Specifications Bureau", fontname=FONT, fontsize=8)
    y += LINE_H
    page.insert_text((MARGIN_L, y), "Proposal Schedule of Items", fontname=FONT, fontsize=8)
    y += LINE_H
    page.insert_text((MARGIN_L, y), "Proposal ID: TEST-SCAN-001", fontname=FONT, fontsize=7)
    y += LINE_H
    page.insert_text((MARGIN_L, y), "SECTION: 0001", fontname=FONT, fontsize=7)
    y += LINE_H
    page.insert_text((MARGIN_L, y), "Roadway Items", fontname=FONT, fontsize=7)
    y += LINE_H * 2

    # Data rows in stacked format
    for row in rows:
        # Check if we need a new page
        if y > PAGE_H - 100:
            page = src_doc.new_page(width=PAGE_W, height=PAGE_H)
            y = 40
            page.insert_text((MARGIN_L, y), "Contracts and Specifications Bureau", fontname=FONT, fontsize=8)
            y += LINE_H
            page.insert_text((MARGIN_L, y), "Proposal Schedule of Items", fontname=FONT, fontsize=8)
            y += LINE_H * 2

        page.insert_text((MARGIN_L, y), row["line_number"], fontname=FONT, fontsize=FSIZE)
        y += LINE_H
        page.insert_text((MARGIN_L, y), row["item"], fontname=FONT, fontsize=FSIZE)
        y += LINE_H
        page.insert_text((MARGIN_L, y), row["description"], fontname=FONT, fontsize=FSIZE)
        y += LINE_H
        page.insert_text((MARGIN_L, y), row["unit"], fontname=FONT, fontsize=FSIZE)
        y += LINE_H
        page.insert_text((MARGIN_L, y), _format_qty(row["qty"]), fontname=FONT, fontsize=FSIZE)
        y += LINE_H
        page.insert_text((MARGIN_L, y), "_________._____", fontname=FONT, fontsize=FSIZE)
        y += LINE_H
        page.insert_text((MARGIN_L, y), "_________._____", fontname=FONT, fontsize=FSIZE)
        y += LINE_H + 4  # small gap between rows

    # Section total
    y += LINE_H
    page.insert_text((MARGIN_L, y), "Section Total", fontname=FONT, fontsize=FSIZE)

    # Step 2: Render ALL pages to images
    page_pixmaps = []
    for i in range(src_doc.page_count):
        pix = src_doc.load_page(i).get_pixmap(dpi=250)
        page_pixmaps.append(pix)
    src_doc.close()

    # Step 3: Create image-only PDF (no native text)
    img_doc = fitz.open()
    for pix in page_pixmaps:
        img_page = img_doc.new_page(width=PAGE_W, height=PAGE_H)
        img_rect = fitz.Rect(0, 0, PAGE_W, PAGE_H)
        img_page.insert_image(img_rect, pixmap=pix)

    img_doc.save(str(output_path))
    img_doc.close()

    return len(rows)


def main():
    out = OUTPUT_DIR / "dot_schedule_scanned.pdf"
    n = generate_scanned_pdf(out)
    print(f"Generated {out}")
    print(f"  Rows: {n} (image-only, no native text)")

    # Verify no native text
    doc = fitz.open(str(out))
    total_native = 0
    pc = doc.page_count
    for i in range(pc):
        native = doc.load_page(i).get_text("text").strip()
        total_native += len(native)
    doc.close()
    print(f"  Pages: {pc}")
    print(f"  Native text chars: {total_native} (should be 0)")


if __name__ == "__main__":
    main()
