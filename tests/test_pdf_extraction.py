"""
C8A / C8A.1 / C8B — PDF Schedule Extraction Tests

Tests the native-text and OCR PDF schedule-of-items extraction pipeline against:
- Synthetic single-line fixture (C8A — Adel canonical data)
- Real Iowa DOT estprop121.pdf stacked format (C8A.1 hardening)
- Scanned/image-only PDF via OCR fallback (C8B)

Validates:
- Text extraction from native PDF
- Schedule page detection (single-line, stacked, OCR)
- Deterministic row parsing (both single-line and stacked formats)
- OCR fallback routing when native text is absent
- Anchor row validation
- Header/total/placeholder filtering
- Multi-line description assembly
- LUMP SUM handling
- Fail-closed behavior
- Extraction endpoint
- OCR provenance (extraction_source = "ocr_pdf")
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import fitz  # PyMuPDF
import pytest
from fastapi.testclient import TestClient

from app.pdf_extraction.extractor import extract_pages_text, ExtractionError
from app.pdf_extraction.schedule_detector import detect_schedule_pages
from app.pdf_extraction.row_parser import parse_schedule_rows, KNOWN_UNITS
from app.pdf_extraction.validator import validate_extracted_rows
from app.pdf_extraction.service import extract_bid_items_from_pdf
from app.main import app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

PDF_FIXTURES_DIR = Path(__file__).parent / "fixtures" / "adel_ipsi" / "pdf"


@pytest.fixture
def dot_pdf_path() -> Path:
    """Path to the generated DOT schedule PDF fixture."""
    p = PDF_FIXTURES_DIR / "dot_schedule_fixture.pdf"
    assert p.exists(), f"PDF fixture not found: {p}. Run generate_fixture.py first."
    return p


@pytest.fixture
def extraction_truth() -> list:
    """Known truth for PDF extraction validation."""
    p = PDF_FIXTURES_DIR / "extraction_truth.json"
    with open(p, "r") as f:
        return json.load(f)


@pytest.fixture
def empty_pdf_path(tmp_path) -> Path:
    """A PDF with no text content (simulates scanned/image-only)."""
    p = tmp_path / "empty.pdf"
    doc = fitz.open()
    doc.new_page()  # blank page
    doc.save(str(p))
    doc.close()
    return p


@pytest.fixture
def non_schedule_pdf_path(tmp_path) -> Path:
    """A PDF with text but no schedule-of-items content."""
    p = tmp_path / "non_schedule.pdf"
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), "This is a general specification document.", fontsize=12)
    page.insert_text((72, 100), "It contains no schedule of items.", fontsize=12)
    page.insert_text((72, 128), "Section 1: General Requirements", fontsize=12)
    doc.save(str(p))
    doc.close()
    return p


@pytest.fixture
def client():
    return TestClient(app)


# ---------------------------------------------------------------------------
# 1. Text Extraction Tests
# ---------------------------------------------------------------------------

class TestTextExtraction:

    def test_extraction_succeeds(self, dot_pdf_path):
        """Native-text PDF should extract text successfully."""
        pages = extract_pages_text(str(dot_pdf_path))
        assert len(pages) == 3
        for p in pages:
            assert p["char_count"] > 0

    def test_native_text_detected(self, dot_pdf_path):
        """All pages should have substantial native text."""
        pages = extract_pages_text(str(dot_pdf_path))
        for p in pages:
            assert p["char_count"] > 100, f"Page {p['page_index']} has too little text"

    def test_empty_pdf_fails(self, empty_pdf_path):
        """PDF with no native text should fail with ExtractionError."""
        with pytest.raises(ExtractionError, match="No native text detected"):
            extract_pages_text(str(empty_pdf_path))

    def test_invalid_file_fails(self, tmp_path):
        """Non-PDF file should fail."""
        bad = tmp_path / "bad.pdf"
        bad.write_text("not a pdf")
        with pytest.raises(ExtractionError, match="Cannot open PDF"):
            extract_pages_text(str(bad))


# ---------------------------------------------------------------------------
# 2. Schedule Detection Tests
# ---------------------------------------------------------------------------

class TestScheduleDetection:

    def test_schedule_pages_detected(self, dot_pdf_path):
        """All 3 pages should be detected as schedule pages."""
        pages = extract_pages_text(str(dot_pdf_path))
        schedule_pages = detect_schedule_pages(pages)
        assert schedule_pages == [0, 1, 2]

    def test_non_schedule_fails(self, non_schedule_pdf_path):
        """Non-schedule PDF should fail detection."""
        pages = extract_pages_text(str(non_schedule_pdf_path))
        with pytest.raises(ExtractionError, match="No schedule-of-items pages detected"):
            detect_schedule_pages(pages)


# ---------------------------------------------------------------------------
# 3. Row Parsing Tests
# ---------------------------------------------------------------------------

class TestRowParsing:

    def test_row_count(self, dot_pdf_path, extraction_truth):
        """Should extract exactly 93 rows (matching truth data)."""
        pages = extract_pages_text(str(dot_pdf_path))
        schedule_pages = detect_schedule_pages(pages)
        rows, meta = parse_schedule_rows(pages, schedule_pages)
        assert len(rows) == len(extraction_truth)

    def test_anchor_row_0520(self, dot_pdf_path):
        """Anchor: 0520 -> 2524-6765010."""
        rows = self._extract_rows(dot_pdf_path)
        row = self._find_row(rows, "0520")
        assert row is not None, "Row 0520 not found"
        assert row["item"] == "2524-6765010"
        assert row["description"] == "REMOVE AND REINSTALL SIGN AS PER PLAN"
        assert row["qty"] == 1.0
        assert row["unit"] == "EACH"

    def test_anchor_row_0580(self, dot_pdf_path):
        """Anchor: 0580 -> 2524-9325001."""
        rows = self._extract_rows(dot_pdf_path)
        row = self._find_row(rows, "0580")
        assert row is not None, "Row 0580 not found"
        assert row["item"] == "2524-9325001"
        assert row["description"] == "TYPE A SIGNS, SHEET ALUMINUM"
        assert row["qty"] == 884.25
        assert row["unit"] == "SF"

    def test_anchor_row_0600(self, dot_pdf_path):
        """Anchor: 0600 -> 2527-9263217."""
        rows = self._extract_rows(dot_pdf_path)
        row = self._find_row(rows, "0600")
        assert row is not None, "Row 0600 not found"
        assert row["item"] == "2527-9263217"
        assert row["description"] == "PAINTED PAVEMENT MARKINGS, DURABLE"
        assert row["qty"] == 86.0
        assert row["unit"] == "STA"

    def test_all_rows_match_truth(self, dot_pdf_path, extraction_truth):
        """Every extracted row must match its truth counterpart exactly."""
        rows = self._extract_rows(dot_pdf_path)
        truth_by_line = {t["line_number"]: t for t in extraction_truth}

        for row in rows:
            ln = row["line_number"]
            assert ln in truth_by_line, f"Extracted row {ln} not in truth"
            truth = truth_by_line[ln]
            assert row["item"] == truth["item"], f"Row {ln}: item mismatch"
            assert row["description"] == truth["description"], f"Row {ln}: desc mismatch"
            assert row["unit"] == truth["unit"], f"Row {ln}: unit mismatch"
            assert abs(row["qty"] - truth["qty"]) < 0.001, f"Row {ln}: qty mismatch"

    def test_repeated_headers_ignored(self, dot_pdf_path):
        """Page headers ('Line  Item Number...') should not appear as data rows."""
        rows = self._extract_rows(dot_pdf_path)
        for row in rows:
            assert row["line_number"] != "Line"
            assert row["item"] != "Item Number"

    def test_section_totals_ignored(self, dot_pdf_path):
        """'Section Total' should not appear as a data row."""
        rows = self._extract_rows(dot_pdf_path)
        for row in rows:
            assert "Section Total" not in row.get("description", "")

    def test_rows_in_order(self, dot_pdf_path):
        """Rows should be in ascending line number order."""
        rows = self._extract_rows(dot_pdf_path)
        line_nums = [int(r["line_number"]) for r in rows]
        assert line_nums == sorted(line_nums)

    def test_lump_sum_unit_parsed(self, dot_pdf_path):
        """Multi-word unit 'LUMP SUM' should be parsed correctly."""
        rows = self._extract_rows(dot_pdf_path)
        lump_sum_rows = [r for r in rows if r["unit"] == "LUMP SUM"]
        assert len(lump_sum_rows) >= 1, "Expected at least one LUMP SUM row"
        for r in lump_sum_rows:
            assert r["qty"] > 0

    def _extract_rows(self, pdf_path):
        pages = extract_pages_text(str(pdf_path))
        schedule_pages = detect_schedule_pages(pages)
        rows, _ = parse_schedule_rows(pages, schedule_pages)
        return rows

    def _find_row(self, rows, line_number):
        for r in rows:
            if r["line_number"] == line_number:
                return r
        return None


# ---------------------------------------------------------------------------
# 4. Validator Tests
# ---------------------------------------------------------------------------

class TestValidator:

    def test_valid_rows_pass(self, dot_pdf_path):
        """All extracted rows from fixture should pass validation."""
        pages = extract_pages_text(str(dot_pdf_path))
        schedule_pages = detect_schedule_pages(pages)
        raw_rows, _ = parse_schedule_rows(pages, schedule_pages)
        valid, rejected, meta = validate_extracted_rows(raw_rows)
        assert len(valid) == 93
        assert len(rejected) == 0

    def test_missing_field_rejected(self):
        """Row with missing required field should fail-closed (all rows invalid)."""
        rows = [{"line_number": "0010", "item": "2101-0850001", "description": "", "qty": 1.0, "unit": "EACH"}]
        with pytest.raises(ExtractionError, match="All extracted rows failed validation"):
            validate_extracted_rows(rows)

    def test_bad_item_format_rejected(self):
        """Row with non-DOT item format should fail-closed (all rows invalid)."""
        rows = [{"line_number": "0010", "item": "BAD-ITEM", "description": "TEST", "qty": 1.0, "unit": "EACH"}]
        with pytest.raises(ExtractionError, match="All extracted rows failed validation"):
            validate_extracted_rows(rows)


# ---------------------------------------------------------------------------
# 5. Full Service (end-to-end) Tests
# ---------------------------------------------------------------------------

class TestExtractionService:

    def test_full_extraction(self, dot_pdf_path, extraction_truth):
        """Full pipeline: PDF -> normalized rows matching truth."""
        rows, summary = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == len(extraction_truth)
        assert summary["status"] == "success"
        assert summary["native_text_detected"] is True
        assert summary["extraction_source"] == "native_pdf"
        assert summary["rows_extracted"] == 93

    def test_extraction_summary_shape(self, dot_pdf_path):
        """Summary must contain required diagnostic fields."""
        _, summary = extract_bid_items_from_pdf(str(dot_pdf_path))
        required_keys = [
            "pages_scanned", "schedule_pages_detected", "native_text_detected",
            "rows_detected", "rows_extracted", "rows_rejected",
            "extraction_source", "status",
        ]
        for key in required_keys:
            assert key in summary, f"Missing summary key: {key}"

    def test_output_row_shape(self, dot_pdf_path):
        """Each output row must have the canonical fields."""
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        required_fields = ["line_number", "item", "description", "qty", "unit", "extraction_source"]
        for row in rows:
            for field in required_fields:
                assert field in row, f"Missing field '{field}' in row {row.get('line_number')}"

    def test_empty_pdf_fails_closed(self, empty_pdf_path):
        """Empty PDF should fail with clear error."""
        with pytest.raises(ExtractionError):
            extract_bid_items_from_pdf(str(empty_pdf_path))

    def test_non_schedule_fails_closed(self, non_schedule_pdf_path):
        """Non-schedule PDF should fail with clear error."""
        with pytest.raises(ExtractionError):
            extract_bid_items_from_pdf(str(non_schedule_pdf_path))


# ---------------------------------------------------------------------------
# 6. Endpoint Tests
# ---------------------------------------------------------------------------

class TestExtractionEndpoint:

    def test_endpoint_success(self, client, dot_pdf_path):
        """POST /extract/bid-items/pdf returns extracted rows."""
        with open(dot_pdf_path, "rb") as f:
            resp = client.post(
                "/extract/bid-items/pdf",
                files={"pdf": ("schedule.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["row_count"] == 93
        assert len(data["rows"]) == 93

    def test_endpoint_anchor_rows(self, client, dot_pdf_path):
        """Endpoint response contains correct anchor rows."""
        with open(dot_pdf_path, "rb") as f:
            resp = client.post(
                "/extract/bid-items/pdf",
                files={"pdf": ("schedule.pdf", f, "application/pdf")},
            )
        rows = resp.json()["rows"]
        anchors = {"0520": "2524-6765010", "0580": "2524-9325001", "0600": "2527-9263217"}
        for row in rows:
            if row["line_number"] in anchors:
                assert row["item"] == anchors[row["line_number"]]

    def test_endpoint_empty_pdf_422(self, client, empty_pdf_path):
        """Empty PDF should return 422."""
        with open(empty_pdf_path, "rb") as f:
            resp = client.post(
                "/extract/bid-items/pdf",
                files={"pdf": ("empty.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 422
        data = resp.json()
        assert data["status"] == "extraction_failed"

    def test_endpoint_non_pdf_rejected(self, client, tmp_path):
        """Non-PDF file should be rejected."""
        txt = tmp_path / "test.txt"
        txt.write_text("not a pdf")
        with open(txt, "rb") as f:
            resp = client.post(
                "/extract/bid-items/pdf",
                files={"pdf": ("test.txt", f, "text/plain")},
            )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# 7. Anchor Validation (CRITICAL — Mission Requirement)
# ---------------------------------------------------------------------------

class TestAnchorValidation:
    """
    MISSION C8A CRITICAL VALIDATION:
    The following MUST match exactly:
        0520 -> 2524-6765010
        0580 -> 2524-9325001
        0600 -> 2527-9263217
    If ANY mismatch: extraction is invalid -> STOP.
    """

    ANCHORS = {
        "0520": {"item": "2524-6765010", "description": "REMOVE AND REINSTALL SIGN AS PER PLAN", "qty": 1.0, "unit": "EACH"},
        "0580": {"item": "2524-9325001", "description": "TYPE A SIGNS, SHEET ALUMINUM", "qty": 884.25, "unit": "SF"},
        "0600": {"item": "2527-9263217", "description": "PAINTED PAVEMENT MARKINGS, DURABLE", "qty": 86.0, "unit": "STA"},
    }

    def test_all_anchors_present_and_match(self, dot_pdf_path):
        """All 3 anchor rows must be present and match exactly."""
        rows, summary = extract_bid_items_from_pdf(str(dot_pdf_path))
        rows_by_line = {r["line_number"]: r for r in rows}

        for line_num, expected in self.ANCHORS.items():
            assert line_num in rows_by_line, f"Anchor row {line_num} MISSING from extraction"
            actual = rows_by_line[line_num]
            assert actual["item"] == expected["item"], (
                f"ANCHOR MISMATCH {line_num}: item={actual['item']}, expected={expected['item']}"
            )
            assert actual["description"] == expected["description"], (
                f"ANCHOR MISMATCH {line_num}: description mismatch"
            )
            assert abs(actual["qty"] - expected["qty"]) < 0.001, (
                f"ANCHOR MISMATCH {line_num}: qty={actual['qty']}, expected={expected['qty']}"
            )
            assert actual["unit"] == expected["unit"], (
                f"ANCHOR MISMATCH {line_num}: unit={actual['unit']}, expected={expected['unit']}"
            )


# ===========================================================================
# C8A.1 — Real DOT PDF (estprop121.pdf) Stacked-Format Tests
# ===========================================================================

@pytest.fixture
def estprop_pdf_path() -> Path:
    """Path to the real Iowa DOT proposal PDF."""
    p = PDF_FIXTURES_DIR / "estprop121.pdf"
    assert p.exists(), f"estprop121.pdf not found: {p}"
    return p


# ---------------------------------------------------------------------------
# 8. Stacked Format Detection & Parsing
# ---------------------------------------------------------------------------

class TestStackedFormatDetection:

    def test_estprop_detected_as_stacked(self, estprop_pdf_path):
        """Real DOT PDF must be detected as stacked format."""
        rows, summary = extract_bid_items_from_pdf(str(estprop_pdf_path))
        assert summary["format_detected"] == "stacked"

    def test_synthetic_detected_as_single_line(self, dot_pdf_path):
        """Synthetic fixture must be detected as single_line format."""
        _, summary = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert summary["format_detected"] == "single_line"

    def test_estprop_schedule_pages_detected(self, estprop_pdf_path):
        """Schedule pages (6-18) should be detected, non-schedule pages (0-5) excluded."""
        pages = extract_pages_text(str(estprop_pdf_path))
        schedule_pages = detect_schedule_pages(pages)
        # Pages 0-5 are cover/specs, pages 6-18 are schedule
        assert 0 not in schedule_pages
        assert 1 not in schedule_pages
        assert 6 in schedule_pages
        assert 18 in schedule_pages


class TestStackedRowParsing:

    def test_row_count(self, estprop_pdf_path):
        """estprop121 has 140 schedule rows (lines 0010-1400)."""
        rows, summary = extract_bid_items_from_pdf(str(estprop_pdf_path))
        assert len(rows) == 140

    def test_rows_in_order(self, estprop_pdf_path):
        """All rows must be in ascending line number order."""
        rows, _ = extract_bid_items_from_pdf(str(estprop_pdf_path))
        line_nums = [int(r["line_number"]) for r in rows]
        assert line_nums == sorted(line_nums)

    def test_first_and_last_row(self, estprop_pdf_path):
        """First row is 0010, last is 1400."""
        rows, _ = extract_bid_items_from_pdf(str(estprop_pdf_path))
        assert rows[0]["line_number"] == "0010"
        assert rows[-1]["line_number"] == "1400"

    def test_multiline_description_assembled(self, estprop_pdf_path):
        """Multi-line descriptions must be joined into a single string."""
        rows, _ = extract_bid_items_from_pdf(str(estprop_pdf_path))
        rows_by_line = {r["line_number"]: r for r in rows}

        # 0580 has a 4-line description in the PDF
        row_0580 = rows_by_line["0580"]
        assert "STORM SEWER GRAVITY MAIN" in row_0580["description"]
        assert "(CLASS III), 15 IN." in row_0580["description"]
        # Should be one joined string, not contain newlines
        assert "\n" not in row_0580["description"]

    def test_lump_sum_items(self, estprop_pdf_path):
        """LUMP SUM items must have qty=1.0 and unit='LUMP SUM'."""
        rows, _ = extract_bid_items_from_pdf(str(estprop_pdf_path))
        ls_rows = [r for r in rows if r["unit"] == "LUMP SUM"]
        assert len(ls_rows) >= 5, f"Expected >= 5 LUMP SUM rows, got {len(ls_rows)}"
        for r in ls_rows:
            assert r["qty"] == 1.0, f"LUMP SUM row {r['line_number']} has qty={r['qty']}, expected 1.0"

    def test_page_headers_not_in_rows(self, estprop_pdf_path):
        """Page header text must not appear as data rows."""
        rows, _ = extract_bid_items_from_pdf(str(estprop_pdf_path))
        for row in rows:
            assert "Contracts and Specifications" not in row["description"]
            assert "Proposal Schedule" not in row["description"]

    def test_section_totals_not_in_rows(self, estprop_pdf_path):
        """Section total lines must not appear as data rows."""
        rows, _ = extract_bid_items_from_pdf(str(estprop_pdf_path))
        for row in rows:
            desc_lower = row["description"].lower()
            assert "total:" not in desc_lower
            assert "total bid" not in desc_lower

    def test_price_placeholders_not_in_descriptions(self, estprop_pdf_path):
        """Price placeholder strings must not leak into descriptions."""
        rows, _ = extract_bid_items_from_pdf(str(estprop_pdf_path))
        for row in rows:
            assert "_________" not in row["description"]

    def test_all_rows_have_required_fields(self, estprop_pdf_path):
        """Every row must have all required fields populated."""
        rows, _ = extract_bid_items_from_pdf(str(estprop_pdf_path))
        for row in rows:
            assert row["line_number"], f"Empty line_number"
            assert row["item"], f"Empty item on {row['line_number']}"
            assert row["description"], f"Empty description on {row['line_number']}"
            assert row["qty"] is not None and row["qty"] > 0, f"Bad qty on {row['line_number']}"
            assert row["unit"], f"Empty unit on {row['line_number']}"

    def test_commas_stripped_from_quantities(self, estprop_pdf_path):
        """Quantities like '2,635.000' must be parsed as 2635.0."""
        rows, _ = extract_bid_items_from_pdf(str(estprop_pdf_path))
        rows_by_line = {r["line_number"]: r for r in rows}
        # Line 0040: EMBANKMENT-IN-PLACE, CONTRACTOR FURNISHED, qty=2635
        row = rows_by_line.get("0040")
        assert row is not None
        assert row["qty"] == 2635.0


# ---------------------------------------------------------------------------
# 9. estprop121 Anchor Validation (CRITICAL — C8A.1 Mission Requirement)
# ---------------------------------------------------------------------------

class TestEstpropAnchorValidation:
    """
    C8A.1 CRITICAL ANCHOR VALIDATION:
    The following MUST match exactly:
        0520 → 2435-0600020
        0580 → 2503-0114215
        0600 → 2506-4984000
        0740 → 2524-6765010
        0780 → 2524-9325001
        0840 → 2527-9263217
    If ANY mismatch → extraction is invalid → STOP.
    """

    ANCHORS = {
        "0520": {"item": "2435-0600020", "description": "MANHOLE ADJUSTMENT, MAJOR", "unit": "EACH", "qty": 1.0},
        "0580": {"item": "2503-0114215", "unit": "LF", "qty": 91.0},
        "0600": {"item": "2506-4984000", "description": "FLOWABLE MORTAR", "unit": "CY", "qty": 33.0},
        "0740": {"item": "2524-6765010", "unit": "EACH", "qty": 26.0},
        "0780": {"item": "2524-9325001", "description": "TYPE A SIGNS, SHEET ALUMINUM", "unit": "SF", "qty": 241.0},
        "0840": {"item": "2527-9263217", "unit": "STA", "qty": 61.0},
    }

    def test_all_anchors_present(self, estprop_pdf_path):
        """All 6 anchor rows must be present in extraction."""
        rows, _ = extract_bid_items_from_pdf(str(estprop_pdf_path))
        rows_by_line = {r["line_number"]: r for r in rows}
        for line_num in self.ANCHORS:
            assert line_num in rows_by_line, f"Anchor row {line_num} MISSING"

    def test_anchor_0520(self, estprop_pdf_path):
        """0520 → 2435-0600020 MANHOLE ADJUSTMENT, MAJOR."""
        self._check_anchor(estprop_pdf_path, "0520")

    def test_anchor_0580(self, estprop_pdf_path):
        """0580 → 2503-0114215 STORM SEWER GRAVITY MAIN..."""
        self._check_anchor(estprop_pdf_path, "0580")

    def test_anchor_0600(self, estprop_pdf_path):
        """0600 → 2506-4984000 FLOWABLE MORTAR."""
        self._check_anchor(estprop_pdf_path, "0600")

    def test_anchor_0740(self, estprop_pdf_path):
        """0740 → 2524-6765010 REMOVE AND REINSTALL SIGN AS PER PLAN."""
        self._check_anchor(estprop_pdf_path, "0740")

    def test_anchor_0780(self, estprop_pdf_path):
        """0780 → 2524-9325001 TYPE A SIGNS, SHEET ALUMINUM."""
        self._check_anchor(estprop_pdf_path, "0780")

    def test_anchor_0840(self, estprop_pdf_path):
        """0840 → 2527-9263217 PAINTED PAVEMENT MARKINGS, DURABLE."""
        self._check_anchor(estprop_pdf_path, "0840")

    def _check_anchor(self, pdf_path, line_num):
        rows, _ = extract_bid_items_from_pdf(str(pdf_path))
        rows_by_line = {r["line_number"]: r for r in rows}
        actual = rows_by_line[line_num]
        expected = self.ANCHORS[line_num]

        assert actual["item"] == expected["item"], (
            f"ANCHOR {line_num}: item={actual['item']}, expected={expected['item']}"
        )
        assert actual["unit"] == expected["unit"], (
            f"ANCHOR {line_num}: unit={actual['unit']}, expected={expected['unit']}"
        )
        assert abs(actual["qty"] - expected["qty"]) < 0.001, (
            f"ANCHOR {line_num}: qty={actual['qty']}, expected={expected['qty']}"
        )
        if "description" in expected:
            assert actual["description"] == expected["description"], (
                f"ANCHOR {line_num}: desc mismatch"
            )


# ---------------------------------------------------------------------------
# 10. estprop121 Endpoint Test
# ---------------------------------------------------------------------------

class TestEstpropEndpoint:

    def test_endpoint_estprop_success(self, client, estprop_pdf_path):
        """POST /extract/bid-items/pdf with real DOT PDF returns 140 rows."""
        with open(estprop_pdf_path, "rb") as f:
            resp = client.post(
                "/extract/bid-items/pdf",
                files={"pdf": ("estprop121.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["row_count"] == 140

    def test_endpoint_estprop_anchors(self, client, estprop_pdf_path):
        """Endpoint response contains correct estprop121 anchor rows."""
        with open(estprop_pdf_path, "rb") as f:
            resp = client.post(
                "/extract/bid-items/pdf",
                files={"pdf": ("estprop121.pdf", f, "application/pdf")},
            )
        rows = resp.json()["rows"]
        anchors = {
            "0520": "2435-0600020", "0580": "2503-0114215", "0600": "2506-4984000",
            "0740": "2524-6765010", "0780": "2524-9325001", "0840": "2527-9263217",
        }
        rows_by_line = {r["line_number"]: r for r in rows}
        for line_num, expected_item in anchors.items():
            assert rows_by_line[line_num]["item"] == expected_item


# ===========================================================================
# C8B — OCR Fallback Tests
# ===========================================================================

@pytest.fixture
def scanned_pdf_path() -> Path:
    """Path to the image-only (scanned) DOT schedule PDF fixture."""
    p = PDF_FIXTURES_DIR / "dot_schedule_scanned.pdf"
    assert p.exists(), f"Scanned PDF fixture not found: {p}. Run generate_scanned_fixture.py first."
    return p


# ---------------------------------------------------------------------------
# 11. OCR Routing / Detection
# ---------------------------------------------------------------------------

class TestOcrRouting:

    def test_native_pdf_does_not_trigger_ocr(self, dot_pdf_path):
        """Native-text PDF must NOT trigger OCR."""
        _, summary = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert summary["ocr_used"] is False
        assert summary["extraction_source"] == "native_pdf"
        assert summary["native_text_detected"] is True

    def test_estprop_does_not_trigger_ocr(self, estprop_pdf_path):
        """Real DOT PDF with native text must NOT trigger OCR."""
        _, summary = extract_bid_items_from_pdf(str(estprop_pdf_path))
        assert summary["ocr_used"] is False
        assert summary["extraction_source"] == "native_pdf"

    def test_scanned_pdf_triggers_ocr(self, scanned_pdf_path):
        """Image-only PDF must trigger OCR fallback."""
        _, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert summary["ocr_used"] is True
        assert summary["extraction_source"] == "ocr_pdf"
        assert summary["ocr_pages"] > 0


# ---------------------------------------------------------------------------
# 12. OCR Extraction Quality
# ---------------------------------------------------------------------------

class TestOcrExtraction:

    def test_ocr_extracts_all_fixture_rows(self, scanned_pdf_path):
        """OCR should extract all 15 rows from the scanned fixture."""
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) == 15

    def test_ocr_rows_have_correct_schema(self, scanned_pdf_path):
        """Every OCR row must have all required fields."""
        rows, _ = extract_bid_items_from_pdf(str(scanned_pdf_path))
        for row in rows:
            assert row["line_number"], f"Empty line_number"
            assert row["item"], f"Empty item"
            assert row["description"], f"Empty description"
            assert row["qty"] is not None and row["qty"] > 0, f"Bad qty on {row['line_number']}"
            assert row["unit"], f"Empty unit"
            assert row["extraction_source"] == "ocr_pdf"

    def test_ocr_rows_match_truth(self, scanned_pdf_path, extraction_truth):
        """OCR rows must match extraction truth for first 15 items."""
        rows, _ = extract_bid_items_from_pdf(str(scanned_pdf_path))
        rows_by_line = {r["line_number"]: r for r in rows}
        truth_subset = extraction_truth[:15]

        for truth in truth_subset:
            ln = truth["line_number"]
            assert ln in rows_by_line, f"OCR missing row {ln}"
            actual = rows_by_line[ln]
            assert actual["item"] == truth["item"], f"Row {ln}: item mismatch"
            assert actual["unit"] == truth["unit"], f"Row {ln}: unit mismatch"
            assert abs(actual["qty"] - truth["qty"]) < 0.01, f"Row {ln}: qty mismatch"

    def test_ocr_first_row_exact(self, scanned_pdf_path):
        """First OCR row: 0010 → 2101-0850001."""
        rows, _ = extract_bid_items_from_pdf(str(scanned_pdf_path))
        r = rows[0]
        assert r["line_number"] == "0010"
        assert r["item"] == "2101-0850001"
        assert r["unit"] == "ACRE"
        assert abs(r["qty"] - 0.2) < 0.01

    def test_ocr_rows_in_order(self, scanned_pdf_path):
        """OCR rows must be in ascending line number order."""
        rows, _ = extract_bid_items_from_pdf(str(scanned_pdf_path))
        nums = [int(r["line_number"]) for r in rows]
        assert nums == sorted(nums)

    def test_ocr_provenance_on_every_row(self, scanned_pdf_path):
        """Every OCR row must be tagged extraction_source='ocr_pdf'."""
        rows, _ = extract_bid_items_from_pdf(str(scanned_pdf_path))
        for row in rows:
            assert row["extraction_source"] == "ocr_pdf"


# ---------------------------------------------------------------------------
# 13. OCR Fail-Closed Behavior
# ---------------------------------------------------------------------------

class TestOcrFailClosed:

    def test_blank_image_pdf_fails(self, tmp_path):
        """A blank image-only PDF should fail extraction."""
        # Create a white-page image PDF
        p = tmp_path / "blank_image.pdf"
        doc = fitz.open()
        page = doc.new_page()
        # Insert a blank white image
        from PIL import Image
        import io
        img = Image.new("RGB", (612, 792), "white")
        buf = io.BytesIO()
        img.save(buf, "PNG")
        page.insert_image(fitz.Rect(0, 0, 612, 792), stream=buf.getvalue())
        doc.save(str(p))
        doc.close()

        with pytest.raises(ExtractionError):
            extract_bid_items_from_pdf(str(p))

    def test_garbage_image_pdf_fails(self, tmp_path):
        """PDF with random noise image should fail — no parseable schedule rows."""
        p = tmp_path / "noise.pdf"
        doc = fitz.open()
        page = doc.new_page()
        from PIL import Image
        import io
        import random
        # Create a noisy image
        img = Image.new("RGB", (200, 200))
        pixels = [(random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)) for _ in range(200 * 200)]
        img.putdata(pixels)
        buf = io.BytesIO()
        img.save(buf, "PNG")
        page.insert_image(fitz.Rect(0, 0, 200, 200), stream=buf.getvalue())
        doc.save(str(p))
        doc.close()

        with pytest.raises(ExtractionError):
            extract_bid_items_from_pdf(str(p))


# ---------------------------------------------------------------------------
# 14. OCR Summary / Provenance
# ---------------------------------------------------------------------------

class TestOcrSummary:

    def test_summary_discloses_ocr(self, scanned_pdf_path):
        """Summary must explicitly state OCR was used."""
        _, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert summary["ocr_used"] is True
        assert summary["extraction_source"] == "ocr_pdf"
        assert summary["ocr_pages"] >= 1

    def test_summary_has_required_fields(self, scanned_pdf_path):
        """Summary must contain all required diagnostic fields."""
        _, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        for key in [
            "pages_scanned", "schedule_pages_detected", "native_text_detected",
            "ocr_used", "ocr_pages", "rows_detected", "rows_extracted",
            "rows_rejected", "extraction_source", "format_detected", "status",
        ]:
            assert key in summary, f"Missing summary key: {key}"


# ---------------------------------------------------------------------------
# 15. OCR Endpoint Tests
# ---------------------------------------------------------------------------

class TestOcrEndpoint:

    def test_endpoint_ocr_success(self, client, scanned_pdf_path):
        """POST scanned PDF → 200 with OCR-extracted rows."""
        with open(scanned_pdf_path, "rb") as f:
            resp = client.post(
                "/extract/bid-items/pdf",
                files={"pdf": ("scanned.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["row_count"] == 15
        assert data["summary"]["ocr_used"] is True
        assert data["summary"]["extraction_source"] == "ocr_pdf"

    def test_endpoint_ocr_row_provenance(self, client, scanned_pdf_path):
        """Every row in endpoint response must have extraction_source='ocr_pdf'."""
        with open(scanned_pdf_path, "rb") as f:
            resp = client.post(
                "/extract/bid-items/pdf",
                files={"pdf": ("scanned.pdf", f, "application/pdf")},
            )
        for row in resp.json()["rows"]:
            assert row["extraction_source"] == "ocr_pdf"


# ---------------------------------------------------------------------------
# 16. C8A Native-Text Regression (must still pass after C8B)
# ---------------------------------------------------------------------------

class TestNativeTextRegression:
    """Ensure C8B changes did not break C8A native-text extraction."""

    def test_synthetic_still_93_rows(self, dot_pdf_path):
        rows, summary = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93
        assert summary["ocr_used"] is False

    def test_estprop_still_140_rows(self, estprop_pdf_path):
        rows, summary = extract_bid_items_from_pdf(str(estprop_pdf_path))
        assert len(rows) == 140
        assert summary["ocr_used"] is False

    def test_synthetic_anchors_still_match(self, dot_pdf_path):
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        by_line = {r["line_number"]: r for r in rows}
        assert by_line["0520"]["item"] == "2524-6765010"
        assert by_line["0580"]["item"] == "2524-9325001"
        assert by_line["0600"]["item"] == "2527-9263217"

    def test_estprop_anchors_still_match(self, estprop_pdf_path):
        rows, _ = extract_bid_items_from_pdf(str(estprop_pdf_path))
        by_line = {r["line_number"]: r for r in rows}
        assert by_line["0520"]["item"] == "2435-0600020"
        assert by_line["0740"]["item"] == "2524-6765010"
        assert by_line["0840"]["item"] == "2527-9263217"
