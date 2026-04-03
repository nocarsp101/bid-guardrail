#!/usr/bin/env python3
"""
Generate a synthetic DOT-style schedule-of-items PDF for testing.

This creates a native-text PDF that mimics an Iowa DOT bid proposal layout.
All data is derived from bid_truth.json and line_to_item_mapping.json.

Each data row is written as a SINGLE pre-formatted text string per line,
using a monospace font (Courier) so column alignment is deterministic.

Usage:
    python generate_fixture.py

Output:
    dot_schedule_fixture.pdf  (in the same directory)
"""
from __future__ import annotations

import json
from pathlib import Path

import fitz  # PyMuPDF


FIXTURES_DIR = Path(__file__).parent.parent / "structured"
OUTPUT_DIR = Path(__file__).parent


def load_truth_data():
    """Load bid truth and line-to-item mapping."""
    with open(FIXTURES_DIR / "bid_truth.json", "r") as f:
        bid_truth = json.load(f)

    with open(FIXTURES_DIR / "line_to_item_mapping.json", "r") as f:
        mapping_data = json.load(f)

    return bid_truth, mapping_data["full_mapping"]


def build_schedule_rows(bid_truth, full_mapping):
    """Build ordered schedule rows from truth data."""
    rows = []
    used_items = set()

    for line_str in sorted(full_mapping.keys(), key=lambda x: int(x)):
        item_no = full_mapping[line_str]
        line_num = line_str.zfill(4)

        for truth_row in bid_truth:
            if truth_row["item_no"] == item_no:
                row_key = (item_no, truth_row["description"])
                if row_key in used_items:
                    continue
                used_items.add(row_key)

                rows.append({
                    "line_number": line_num,
                    "item": item_no,
                    "description": truth_row["description"],
                    "unit": truth_row["unit"],
                    "qty": truth_row["qty"],
                })
                break

    return rows


def _format_qty(qty):
    """Format quantity as DOT-style string."""
    if qty == int(qty):
        return f"{int(qty)}.000"
    return f"{qty:.3f}"


def _format_row(row):
    """Format a single schedule row as a fixed-width string."""
    # Field widths (Courier chars):
    #   Line: 4 chars + 2 spaces
    #   Item: 12 chars + 2 spaces
    #   Description: 52 chars + 2 spaces
    #   Unit: 10 chars + 2 spaces
    #   Qty: right-aligned
    line_num = row["line_number"]
    item = row["item"]
    desc = row["description"]
    unit = row["unit"]
    qty_str = _format_qty(row["qty"])

    return f"{line_num}  {item}  {desc:<52s}  {unit:<10s}  {qty_str}"


def _format_header():
    """Format the column header line."""
    return f"{'Line':<4s}  {'Item Number':<12s}  {'Description':<52s}  {'Unit':<10s}  {'Quantity'}"


def generate_pdf(rows, output_path):
    """Generate a multi-page DOT-style schedule PDF."""
    doc = fitz.open()

    PAGE_W, PAGE_H = 792, 612  # Landscape letter for wider columns
    MARGIN_LEFT = 36
    LINE_HEIGHT = 11
    FONT_SIZE = 7
    FONT_NAME = "Courier"
    ROWS_PER_PAGE = 40

    total_pages = (len(rows) + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE
    row_idx = 0

    for page_num in range(1, total_pages + 1):
        page = doc.new_page(width=PAGE_W, height=PAGE_H)
        y = 36

        # Page header
        page.insert_text(
            (MARGIN_LEFT, y),
            "IOWA DEPARTMENT OF TRANSPORTATION",
            fontname=FONT_NAME, fontsize=8,
        )
        y += 12
        page.insert_text(
            (MARGIN_LEFT, y),
            "SCHEDULE OF ITEMS",
            fontname=FONT_NAME, fontsize=8,
        )
        y += 12
        page.insert_text(
            (MARGIN_LEFT, y),
            f"Proposal ID: IM-006-1(28)--13-25    Project: Adel    Page {page_num} of {total_pages}",
            fontname=FONT_NAME, fontsize=7,
        )
        y += 16

        # Column headers (single line)
        page.insert_text(
            (MARGIN_LEFT, y),
            _format_header(),
            fontname=FONT_NAME, fontsize=FONT_SIZE,
        )
        y += LINE_HEIGHT + 3

        # Data rows (each as a single formatted string)
        rows_on_page = 0
        while row_idx < len(rows) and rows_on_page < ROWS_PER_PAGE:
            row_text = _format_row(rows[row_idx])
            page.insert_text(
                (MARGIN_LEFT, y),
                row_text,
                fontname=FONT_NAME, fontsize=FONT_SIZE,
            )
            y += LINE_HEIGHT
            row_idx += 1
            rows_on_page += 1

        # Section total at bottom of last page
        if row_idx >= len(rows):
            y += LINE_HEIGHT
            page.insert_text(
                (MARGIN_LEFT, y),
                f"{'':6s}{'':14s}Section Total",
                fontname=FONT_NAME, fontsize=FONT_SIZE,
            )

    doc.save(str(output_path))
    doc.close()
    return total_pages


def main():
    bid_truth, full_mapping = load_truth_data()
    rows = build_schedule_rows(bid_truth, full_mapping)

    output_path = OUTPUT_DIR / "dot_schedule_fixture.pdf"
    total_pages = generate_pdf(rows, output_path)

    print(f"Generated {output_path}")
    print(f"  Pages: {total_pages}")
    print(f"  Rows: {len(rows)}")

    # Save extraction truth
    extraction_truth = []
    for row in rows:
        extraction_truth.append({
            "line_number": row["line_number"],
            "item": row["item"],
            "description": row["description"],
            "unit": row["unit"],
            "qty": row["qty"],
        })

    truth_path = OUTPUT_DIR / "extraction_truth.json"
    with open(truth_path, "w") as f:
        json.dump(extraction_truth, f, indent=2)
    print(f"Generated {truth_path}")
    print(f"  Truth rows: {len(extraction_truth)}")


if __name__ == "__main__":
    main()
