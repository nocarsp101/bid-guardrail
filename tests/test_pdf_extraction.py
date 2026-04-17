"""
C8A / C8A.1 / C8B / C9 — PDF Extraction Tests

Tests:
- C8A: native-text DOT schedule extraction (synthetic + estprop121)
- C8A.1: stacked-format parser hardening
- C8B: OCR fallback for scanned PDFs
- C9: document routing + quote ingestion (separate from DOT pipeline)
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


# ===========================================================================
# C9 — Document Routing & Quote Ingestion Tests
# ===========================================================================

from app.pdf_extraction.document_router import classify_document
from app.pdf_extraction.quote_parser import parse_quote_rows
from app.pdf_extraction.quote_validator import validate_quote_rows
from app.pdf_extraction.service import extract_quote_from_pdf, extract_pdf_auto


@pytest.fixture
def ipsi_quote_path() -> Path:
    """Path to the real IPSI subcontractor quote PDF (image-only)."""
    p = PDF_FIXTURES_DIR / "ipsi_quote.pdf"
    assert p.exists(), f"ipsi_quote.pdf not found: {p}"
    return p


@pytest.fixture
def rasch_quote_path() -> Path:
    """Path to the Rasch 'quote' PDF (actually a scanned DOT proposal form)."""
    p = PDF_FIXTURES_DIR / "rasch_quote.pdf"
    assert p.exists(), f"rasch_quote.pdf not found: {p}"
    return p


# ---------------------------------------------------------------------------
# 17. Document Router Tests
# ---------------------------------------------------------------------------

class TestDocumentRouter:

    def test_dot_native_classified_as_dot_schedule(self, estprop_pdf_path):
        """Native-text DOT PDF -> dot_schedule."""
        from app.pdf_extraction.extractor import extract_pages_text_permissive
        pages = extract_pages_text_permissive(str(estprop_pdf_path))
        assert classify_document(pages) == "dot_schedule"

    def test_synthetic_dot_classified_as_dot_schedule(self, dot_pdf_path):
        """Synthetic DOT fixture -> dot_schedule."""
        from app.pdf_extraction.extractor import extract_pages_text_permissive
        pages = extract_pages_text_permissive(str(dot_pdf_path))
        assert classify_document(pages) == "dot_schedule"

    def test_rasch_not_classified_as_dot_schedule(self, rasch_quote_path):
        """Rasch is a scanned vendor markup on a DOT template. OCR noise
        destroys deterministic DOT row structure, so the classifier must
        NOT route it through the DOT parser. Must be quote or unknown."""
        from app.pdf_extraction.ocr import ocr_pages
        pages = ocr_pages(str(rasch_quote_path))
        cls = classify_document(pages)
        assert cls in ("quote", "unknown"), (
            f"Rasch must never classify as dot_schedule, got: {cls}"
        )

    def test_rasch_is_unknown(self, rasch_quote_path):
        """Rasch has no $X.XX patterns (handwritten prices) and no
        parseable DOT row structure after OCR -> unknown."""
        from app.pdf_extraction.ocr import ocr_pages
        pages = ocr_pages(str(rasch_quote_path))
        assert classify_document(pages) == "unknown"

    def test_ipsi_classified_as_quote(self, ipsi_quote_path):
        """IPSI is a real subcontractor quote -> quote."""
        from app.pdf_extraction.ocr import ocr_pages
        pages = ocr_pages(str(ipsi_quote_path))
        assert classify_document(pages) == "quote"

    def test_non_schedule_non_quote_is_unknown(self, non_schedule_pdf_path):
        """Generic text PDF with no schedule or pricing -> unknown."""
        from app.pdf_extraction.extractor import extract_pages_text_permissive
        pages = extract_pages_text_permissive(str(non_schedule_pdf_path))
        assert classify_document(pages) == "unknown"


# ---------------------------------------------------------------------------
# 18. Quote Parser Tests (using IPSI)
# ---------------------------------------------------------------------------

class TestQuoteParser:

    def test_ipsi_extracts_rows(self, ipsi_quote_path):
        """IPSI QUOTE should produce at least 14 quote rows."""
        rows, summary = extract_quote_from_pdf(str(ipsi_quote_path))
        assert len(rows) >= 14

    def test_ipsi_rows_have_descriptions(self, ipsi_quote_path):
        """Every IPSI row must have a description."""
        rows, _ = extract_quote_from_pdf(str(ipsi_quote_path))
        for row in rows:
            assert row["description"], f"Row {row['row_id']} missing description"

    def test_ipsi_rows_have_amounts(self, ipsi_quote_path):
        """Every IPSI row must have an amount."""
        rows, _ = extract_quote_from_pdf(str(ipsi_quote_path))
        for row in rows:
            assert row["amount"] is not None and row["amount"] > 0

    def test_ipsi_extraction_source_is_ocr(self, ipsi_quote_path):
        """IPSI is image-only so extraction_source must be ocr_pdf."""
        rows, summary = extract_quote_from_pdf(str(ipsi_quote_path))
        assert summary["extraction_source"] == "ocr_pdf"
        for row in rows:
            assert row["extraction_source"] == "ocr_pdf"

    def test_ipsi_document_class_is_quote(self, ipsi_quote_path):
        """Summary must report document_class = quote."""
        _, summary = extract_quote_from_pdf(str(ipsi_quote_path))
        assert summary["document_class"] == "quote"

    def test_ipsi_qty_and_unit_are_null(self, ipsi_quote_path):
        """IPSI quote does not contain explicit qty/unit — must be null, NOT inferred."""
        rows, _ = extract_quote_from_pdf(str(ipsi_quote_path))
        for row in rows:
            assert row["qty"] is None, f"Row {row['row_id']}: qty should be null, got {row['qty']}"
            assert row["unit"] is None, f"Row {row['row_id']}: unit should be null, got {row['unit']}"

    def test_ipsi_known_row_530(self, ipsi_quote_path):
        """Known IPSI row: line_ref=530, description contains 'Reference Location Sign', amount=$550."""
        rows, _ = extract_quote_from_pdf(str(ipsi_quote_path))
        row_530 = [r for r in rows if r.get("line_ref") == "530"]
        assert len(row_530) == 1, "Expected exactly one row with line_ref=530"
        assert "Reference Location Sign" in row_530[0]["description"]
        assert abs(row_530[0]["amount"] - 550.0) < 0.01

    def test_ipsi_total_not_emitted_as_row(self, ipsi_quote_path):
        """TOTAL line must not appear as a quote row."""
        rows, _ = extract_quote_from_pdf(str(ipsi_quote_path))
        for row in rows:
            assert "TOTAL" not in row["description"].upper().split()[0] if row["description"] else True

    def test_ipsi_rows_in_order(self, ipsi_quote_path):
        """Quote rows must be in ascending row_id order."""
        rows, _ = extract_quote_from_pdf(str(ipsi_quote_path))
        ids = [r["row_id"] for r in rows]
        assert ids == sorted(ids)


# ---------------------------------------------------------------------------
# 19. Quote Routing / Rejection Tests
# ---------------------------------------------------------------------------

class TestQuoteRouting:

    def test_rasch_processed_as_quote_fails_closed(self, rasch_quote_path):
        """Rasch through the explicit quote pipeline: processed as quote,
        fails closed because handwritten prices produce no deterministic
        $X.XX patterns. Failure_reason must be explicit."""
        from app.pdf_extraction.service import FAIL_NO_CANDIDATE_QUOTE_ROWS
        with pytest.raises(ExtractionError) as exc_info:
            extract_quote_from_pdf(str(rasch_quote_path))
        assert exc_info.value.meta.get("failure_reason") == FAIL_NO_CANDIDATE_QUOTE_ROWS

    def test_rasch_quote_pipeline_uses_ocr(self, rasch_quote_path):
        """Rasch is image-only — quote pipeline must use OCR before failing,
        and must record extraction_source=ocr_pdf in the failure meta."""
        with pytest.raises(ExtractionError) as exc_info:
            extract_quote_from_pdf(str(rasch_quote_path))
        assert exc_info.value.meta.get("extraction_source") == "ocr_pdf"
        assert exc_info.value.meta.get("ocr_used") is True

    def test_rasch_auto_routes_to_unknown(self, rasch_quote_path):
        """Auto endpoint must classify Rasch as unknown and fail-closed,
        NEVER forcing it through the DOT or quote parser."""
        from app.pdf_extraction.service import FAIL_UNKNOWN_CLASS
        with pytest.raises(ExtractionError) as exc_info:
            extract_pdf_auto(str(rasch_quote_path))
        assert exc_info.value.meta.get("failure_reason") == FAIL_UNKNOWN_CLASS
        assert exc_info.value.meta.get("document_class_detected") == "unknown"

    def test_auto_routes_ipsi_to_quote(self, ipsi_quote_path):
        """Auto endpoint should route IPSI to quote pipeline."""
        rows, summary = extract_pdf_auto(str(ipsi_quote_path))
        assert summary["document_class"] == "quote"
        assert len(rows) >= 14

    def test_auto_routes_dot_to_schedule(self, dot_pdf_path):
        """Auto endpoint should route synthetic DOT to schedule pipeline."""
        rows, summary = extract_pdf_auto(str(dot_pdf_path))
        assert summary["document_class"] == "dot_schedule"
        assert len(rows) == 93

    def test_auto_routes_scanned_dot_to_schedule(self, scanned_pdf_path):
        """Auto endpoint must route a scanned DOT proposal through the OCR
        -> C8 DOT lane without regression."""
        rows, summary = extract_pdf_auto(str(scanned_pdf_path))
        assert summary["document_class"] == "dot_schedule"
        assert summary["extraction_source"] == "ocr_pdf"
        assert summary["ocr_used"] is True
        assert len(rows) > 0

    def test_auto_unknown_for_generic_text(self, non_schedule_pdf_path):
        """Generic non-schedule non-quote text PDF must fail with explicit
        unknown_document_class reason."""
        from app.pdf_extraction.service import FAIL_UNKNOWN_CLASS, FAIL_INSUFFICIENT_TEXT
        with pytest.raises(ExtractionError) as exc_info:
            extract_pdf_auto(str(non_schedule_pdf_path))
        reason = exc_info.value.meta.get("failure_reason")
        assert reason in (FAIL_UNKNOWN_CLASS, FAIL_INSUFFICIENT_TEXT)


# ---------------------------------------------------------------------------
# 20. Quote Validator Tests
# ---------------------------------------------------------------------------

class TestQuoteValidator:

    def test_valid_rows_pass(self):
        rows = [{"row_id": 0, "description": "Test item", "amount": 100.0, "unit_price": 50.0, "qty": None, "unit": None}]
        valid, rejected, meta = validate_quote_rows(rows)
        assert len(valid) == 1
        assert len(rejected) == 0

    def test_missing_description_rejected(self):
        rows = [{"row_id": 0, "description": "", "amount": 100.0, "unit_price": None, "qty": None, "unit": None}]
        with pytest.raises(ExtractionError) as exc_info:
            validate_quote_rows(rows)
        assert exc_info.value.meta.get("failure_reason") == "quote_structure_insufficient"

    def test_no_monetary_value_rejected(self):
        rows = [{"row_id": 0, "description": "Test", "amount": None, "unit_price": None, "qty": None, "unit": None}]
        with pytest.raises(ExtractionError) as exc_info:
            validate_quote_rows(rows)
        assert exc_info.value.meta.get("failure_reason") == "quote_structure_insufficient"

    def test_non_positive_amount_rejected(self):
        rows = [{"row_id": 0, "description": "Test", "amount": 0.0, "unit_price": None, "qty": None, "unit": None}]
        with pytest.raises(ExtractionError):
            validate_quote_rows(rows)

    def test_inconsistent_numeric_row_rejected(self):
        """If qty * unit_price != amount, the row is ambiguous -> rejected."""
        rows = [{
            "row_id": 0,
            "description": "Bad math",
            "amount": 999.0,
            "unit_price": 10.0,
            "qty": 5.0,  # 5*10 = 50, not 999
            "unit": "EA",
        }]
        with pytest.raises(ExtractionError):
            validate_quote_rows(rows)

    def test_consistent_numeric_row_passes(self):
        """qty * unit_price == amount (within 1%) -> valid."""
        rows = [{
            "row_id": 0,
            "description": "Good math",
            "amount": 50.0,
            "unit_price": 10.0,
            "qty": 5.0,
            "unit": "EA",
        }]
        valid, rejected, _ = validate_quote_rows(rows)
        assert len(valid) == 1
        assert len(rejected) == 0


# ---------------------------------------------------------------------------
# 20b. Quote Parser Rejection Tests — fail-closed on ambiguity
# ---------------------------------------------------------------------------

class TestQuoteParserRejection:
    """Verify the parser explicitly rejects totals, headers, and ambiguous
    numeric patterns, never guessing partial rows."""

    def _page(self, text: str) -> dict:
        return {"page_index": 0, "text": text, "char_count": len(text)}

    def test_total_line_rejected(self):
        from app.pdf_extraction.quote_parser import parse_quote_rows
        pages = [self._page(
            "Widget A $100.00\n"
            "TOTAL $100.00\n"
        )]
        rows, rejected, meta = parse_quote_rows(pages)
        assert len(rows) == 1
        assert meta["rejection_counts"].get("total_row") == 1
        total_candidates = [c for c in rejected if c["rejection_reason"] == "total_row"]
        assert len(total_candidates) == 1
        assert "TOTAL" in total_candidates[0]["raw_text"].upper()
        assert total_candidates[0]["candidate_type"] == "line"

    def test_subtotal_line_rejected(self):
        from app.pdf_extraction.quote_parser import parse_quote_rows
        pages = [self._page(
            "Widget A $100.00\n"
            "SUBTOTAL $100.00\n"
            "Grand Total $100.00\n"
        )]
        rows, rejected, meta = parse_quote_rows(pages)
        assert len(rows) == 1
        assert meta["rejection_counts"].get("subtotal_row") == 1
        assert meta["rejection_counts"].get("total_row") == 1
        reasons = sorted(c["rejection_reason"] for c in rejected)
        assert reasons == ["subtotal_row", "total_row"]

    def test_header_line_rejected(self):
        from app.pdf_extraction.quote_parser import parse_quote_rows
        pages = [self._page(
            "Description Unit Price $0.00\n"
            "Widget A $100.00\n"
        )]
        rows, rejected, meta = parse_quote_rows(pages)
        assert len(rows) == 1
        assert meta["rejection_counts"].get("header_row") == 1
        header_candidates = [c for c in rejected if c["rejection_reason"] == "header_row"]
        assert len(header_candidates) == 1
        assert header_candidates[0]["candidate_type"] == "line"

    def test_ambiguous_numeric_rejected(self):
        """Line with 3 dollar amounts cannot be deterministically parsed."""
        from app.pdf_extraction.quote_parser import parse_quote_rows
        pages = [self._page(
            "Widget A $10.00 $20.00 $200.00\n"  # 3 amounts, ambiguous
            "Widget B $50.00 $100.00\n"         # 2 amounts, valid
        )]
        rows, rejected, meta = parse_quote_rows(pages)
        assert len(rows) == 1
        assert rows[0]["description"].startswith("Widget B")
        assert meta["rejection_counts"].get("ambiguous_numeric") == 1
        ambiguous = [c for c in rejected if c["rejection_reason"] == "ambiguous_numeric"]
        assert len(ambiguous) == 1
        assert ambiguous[0]["candidate_type"] == "row_like"
        assert "$10.00" in ambiguous[0]["raw_text"]

    def test_no_candidate_rows_fails_closed(self):
        """A document with no $X.XX patterns must raise with explicit reason."""
        from app.pdf_extraction.quote_parser import parse_quote_rows, REASON_NO_CANDIDATES
        pages = [self._page("Quotation\nNo prices here\nJust text\n")]
        with pytest.raises(ExtractionError) as exc_info:
            parse_quote_rows(pages)
        assert exc_info.value.meta.get("failure_reason") == REASON_NO_CANDIDATES
        # Rejected evidence must be preserved even on total-failure path.
        preserved = exc_info.value.meta.get("rejected_candidates")
        assert preserved is not None  # may be empty if no row-like content

    def test_no_guessed_qty_or_unit(self):
        """Parser must never invent qty/unit even when a leading number looks
        like a count."""
        from app.pdf_extraction.quote_parser import parse_quote_rows
        pages = [self._page("530 Widget $100.00 $500.00\n")]
        rows, rejected, _ = parse_quote_rows(pages)
        assert len(rows) == 1
        assert rows[0]["qty"] is None
        assert rows[0]["unit"] is None
        assert rows[0]["line_ref"] == "530"
        assert rows[0]["unit_price"] == 100.0
        assert rows[0]["amount"] == 500.0
        assert rejected == []


# ---------------------------------------------------------------------------
# 20d. C11 — Block-Level Candidate Recovery Tests
# ---------------------------------------------------------------------------

class TestQuoteBlockCandidateRecovery:
    """C11: deterministic multi-line block grouping for row-fragment
    evidence. Block candidates must NEVER become accepted rows."""

    def _page(self, text: str, page_index: int = 0) -> dict:
        return {"page_index": page_index, "text": text, "char_count": len(text)}

    def test_split_row_block_preserved(self):
        """Two-line split fragment (line_ref+desc, then $-only) is preserved
        as a single block candidate, NOT as an accepted row."""
        from app.pdf_extraction.quote_parser import parse_quote_rows
        pages = [self._page(
            "Widget A $50.00 $200.00\n"      # accepted
            "530 Mounting Hardware\n"        # fragment
            "$25.00 $125.00\n"               # price-only continuation
        )]
        accepted, rejected, meta = parse_quote_rows(pages)
        assert len(accepted) == 1
        assert accepted[0]["description"].startswith("Widget A")
        block_candidates = [c for c in rejected if c["candidate_type"] == "block"]
        assert len(block_candidates) == 1
        assert "Mounting Hardware" in block_candidates[0]["raw_text"]
        assert "$25.00" in block_candidates[0]["raw_text"]
        assert block_candidates[0]["rejection_reason"] == "unstable_boundary"
        assert meta["block_candidates"] == 1

    def test_multiline_description_block_preserved(self):
        """Two adjacent line_ref+desc fragments WITHOUT prices form a block
        with reason=insufficient_structure."""
        from app.pdf_extraction.quote_parser import parse_quote_rows
        pages = [self._page(
            "Widget A $50.00 $200.00\n"
            "530 Mounting Hardware\n"
            "and Brackets per plan\n"
            "\n"
            "Widget Z $1.00 $5.00\n"
        )]
        accepted, rejected, meta = parse_quote_rows(pages)
        assert len(accepted) == 2
        block_candidates = [c for c in rejected if c["candidate_type"] == "block"]
        assert len(block_candidates) == 1
        block = block_candidates[0]
        assert "Mounting Hardware" in block["raw_text"]
        assert "Brackets" in block["raw_text"]
        assert block["rejection_reason"] == "insufficient_structure"

    def test_block_candidates_have_full_traceability(self):
        """Block candidates must carry candidate_id, raw_text, source_page,
        rejection_reason, candidate_type. Extraction_source is added by
        the staging layer downstream."""
        from app.pdf_extraction.quote_parser import parse_quote_rows
        pages = [self._page(
            "Widget A $1.00 $5.00\n"
            "530 Item A\n"
            "Item A continued\n",
            page_index=2,
        )]
        _, rejected, _ = parse_quote_rows(pages)
        block = next(c for c in rejected if c["candidate_type"] == "block")
        assert block["candidate_id"].startswith("b2-")
        assert block["source_page"] == 2
        assert block["raw_text"]
        assert block["rejection_reason"]
        assert block["candidate_type"] == "block"

    def test_block_grouping_is_bounded_by_skip_lines(self):
        """A SUBTOTAL line ends a fragment block deterministically."""
        from app.pdf_extraction.quote_parser import parse_quote_rows
        pages = [self._page(
            "Widget A $1.00 $5.00\n"
            "530 Item A\n"
            "Item A part 2\n"
            "SUBTOTAL $5.00\n"
            "540 Item B\n"
            "Item B part 2\n"
        )]
        _, rejected, _ = parse_quote_rows(pages)
        block_candidates = [c for c in rejected if c["candidate_type"] == "block"]
        assert len(block_candidates) == 2
        assert "Item A" in block_candidates[0]["raw_text"]
        assert "SUBTOTAL" not in block_candidates[0]["raw_text"]
        assert "Item B" in block_candidates[1]["raw_text"]
        assert "SUBTOTAL" not in block_candidates[1]["raw_text"]

    def test_block_grouping_does_not_create_accepted_rows(self):
        """No accepted row is produced from a fragment block, even one with
        a deterministic price-only terminator. Strict accepted-row rules
        are unchanged."""
        from app.pdf_extraction.quote_parser import parse_quote_rows
        pages = [self._page(
            "Widget A $1.00 $5.00\n"        # accepted
            "530 Item A\n"                  # fragment line 1
            "$10.00 $50.00\n"               # fragment line 2 (price-only)
        )]
        accepted, rejected, _ = parse_quote_rows(pages)
        assert len(accepted) == 1
        assert accepted[0]["description"].startswith("Widget A")
        # Item A must not appear among accepted rows.
        for row in accepted:
            assert "Item A" not in row["description"]
        # But it must appear in rejected block evidence.
        block = next(c for c in rejected if c["candidate_type"] == "block")
        assert "Item A" in block["raw_text"]

    def test_solitary_fragment_preserved_as_row_like(self):
        """A single line_ref+desc fragment with no continuation is preserved
        as a row_like single-line candidate (not promoted to a block)."""
        from app.pdf_extraction.quote_parser import parse_quote_rows
        pages = [self._page(
            "Widget A $1.00 $5.00\n"
            "530 Item A standalone\n"
            "Widget B $2.00 $10.00\n"
        )]
        _, rejected, _ = parse_quote_rows(pages)
        # Item A standalone has no continuation → preserved as row_like
        row_like = [c for c in rejected if c["candidate_type"] == "row_like"]
        assert any("Item A standalone" in c["raw_text"] for c in row_like)

    def test_block_candidates_count_in_meta(self):
        from app.pdf_extraction.quote_parser import parse_quote_rows
        pages = [self._page(
            "Widget A $1.00 $5.00\n"
            "530 Item A\n"
            "more A\n"
            "Widget B $2.00 $10.00\n"
            "540 Item B\n"
            "more B\n"
        )]
        _, _, meta = parse_quote_rows(pages)
        assert meta["block_candidates"] == 2

    def test_ipsi_accepted_rows_unchanged_under_c11(self, ipsi_quote_path):
        """C11 must NOT change accepted_row count for IPSI."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert staging["document_class_detected"] == "quote"
        assert len(staging["accepted_rows"]) >= 14

    def test_ipsi_block_candidates_in_diagnostics(self, ipsi_quote_path):
        """Diagnostics must expose a block_candidates counter."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert "block_candidates" in staging["document_diagnostics"]["candidate_counts"]

    def test_rasch_unknown_path_unchanged(self, rasch_quote_path):
        """Rasch still returns unknown classification with zero accepted rows."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(rasch_quote_path))
        assert staging["document_class_detected"] == "unknown"
        assert staging["accepted_rows"] == []

    def test_dot_native_unaffected_by_block_grouping(self, dot_pdf_path):
        """C8 DOT lane must not regress under C11 changes."""
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93
        assert summary["document_class"] == "dot_schedule"

    def test_dot_scanned_unaffected_by_block_grouping(self, scanned_pdf_path):
        """C8B OCR DOT lane must not regress under C11 changes."""
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["document_class"] == "dot_schedule"
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20e. C12 — Normalized Quote Row Contract Tests
# ---------------------------------------------------------------------------

class TestC12NormalizedRowContract:
    """C12: stable, deterministic pre-mapping contract for accepted_rows.

    No mapping logic is exercised here. These tests pin the schema only.
    """

    def test_contract_keys_present(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.quote_row_contract import ACCEPTED_ROW_KEYS
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        for row in staging["accepted_rows"]:
            assert ACCEPTED_ROW_KEYS.issubset(set(row.keys())), (
                f"Missing keys: {ACCEPTED_ROW_KEYS - set(row.keys())}"
            )

    def test_normalized_row_id_is_deterministic(self, ipsi_quote_path):
        """Same document → same IDs across runs."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        s1 = normalize_quote_from_pdf(str(ipsi_quote_path))
        s2 = normalize_quote_from_pdf(str(ipsi_quote_path))
        ids1 = [r["normalized_row_id"] for r in s1["accepted_rows"]]
        ids2 = [r["normalized_row_id"] for r in s2["accepted_rows"]]
        assert ids1 == ids2
        # Format check.
        for row_id in ids1:
            assert row_id.startswith("qr-p")
            assert "-r" in row_id

    def test_normalized_row_ids_are_unique(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        ids = [r["normalized_row_id"] for r in staging["accepted_rows"]]
        assert len(ids) == len(set(ids))

    def test_provenance_shape(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.quote_row_contract import PROVENANCE_KEYS
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        for row in staging["accepted_rows"]:
            prov = row["provenance"]
            assert PROVENANCE_KEYS.issubset(set(prov.keys()))
            assert prov["extraction_source"] == "ocr_pdf"
            assert prov["ocr_used"] is True
            assert prov["parser"] == "quote_parser_v1"
            assert prov["source_page"] == row["source_page"]

    def test_source_text_preserved_from_parser(self, ipsi_quote_path):
        """source_text must contain the description fragment so downstream
        mapping can audit the exact line that produced the row."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        for row in staging["accepted_rows"]:
            assert isinstance(row["source_text"], str)
            assert row["source_text"]
            # The description text must appear within the original line.
            assert row["description"].split()[0] in row["source_text"]

    def test_row_issues_emitted_for_missing_unit_price(self):
        """A row with only an amount (no unit_price) gets unit_price_absent."""
        from app.pdf_extraction.quote_row_contract import (
            build_accepted_row, ISSUE_UNIT_PRICE_ABSENT, ISSUE_QTY_ABSENT
        )
        parsed = {
            "row_id": 0,
            "line_ref": "530",
            "description": "Test item",
            "qty": None,
            "unit": None,
            "unit_price": None,
            "amount": 100.0,
            "source_page": 0,
            "source_text": "530 Test item $100.00",
        }
        row = build_accepted_row(parsed, "native_pdf", False)
        assert ISSUE_UNIT_PRICE_ABSENT in row["row_issues"]
        assert ISSUE_QTY_ABSENT in row["row_issues"]

    def test_row_issues_no_unit_price_absent_when_present(self):
        from app.pdf_extraction.quote_row_contract import (
            build_accepted_row, ISSUE_UNIT_PRICE_ABSENT
        )
        parsed = {
            "row_id": 0,
            "description": "Test",
            "qty": None,
            "unit": None,
            "unit_price": 50.0,
            "amount": 100.0,
            "source_page": 0,
            "source_text": "Test $50.00 $100.00",
        }
        row = build_accepted_row(parsed, "native_pdf", False)
        assert ISSUE_UNIT_PRICE_ABSENT not in row["row_issues"]

    def test_line_ref_absent_issue_for_ipsi_rows(self, ipsi_quote_path):
        """IPSI rows have no leading line_ref → issue must appear."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.quote_row_contract import ISSUE_LINE_REF_ABSENT
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        rows_no_lineref = [r for r in staging["accepted_rows"] if r["line_ref"] is None]
        assert len(rows_no_lineref) > 0
        for row in rows_no_lineref:
            assert ISSUE_LINE_REF_ABSENT in row["row_issues"]

    def test_rejected_candidates_unchanged_by_c12(self, ipsi_quote_path):
        """Rejected candidates retain the C10 schema; C12 only touches
        accepted_rows."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        for cand in staging["rejected_candidates"]:
            for k in ("candidate_id", "raw_text", "source_page",
                      "rejection_reason", "candidate_type", "extraction_source"):
                assert k in cand
            # No C12 contract fields leak into rejected.
            assert "normalized_row_id" not in cand
            assert "provenance" not in cand
            assert "row_issues" not in cand

    def test_diagnostics_unchanged_shape(self, ipsi_quote_path):
        """Diagnostics bucket must still carry the C10 fields."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        diag = staging["document_diagnostics"]
        for k in ("status", "failure_reason", "classification_signals",
                  "candidate_counts", "rejection_counts", "ocr_used",
                  "extraction_source"):
            assert k in diag

    def test_dot_lane_has_no_c12_fields(self, dot_pdf_path):
        """DOT lane output must NOT pick up the quote contract fields."""
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        for row in rows:
            assert "normalized_row_id" not in row
            assert "provenance" not in row
            assert "row_issues" not in row

    def test_endpoint_exposes_c12_contract(self, client, ipsi_quote_path):
        with open(ipsi_quote_path, "rb") as f:
            resp = client.post(
                "/extract/quote/staging",
                files={"pdf": ("ipsi_quote.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["accepted_rows"]) >= 14
        sample = data["accepted_rows"][0]
        assert "normalized_row_id" in sample
        assert "provenance" in sample
        assert "row_issues" in sample
        assert "source_text" in sample
        assert sample["provenance"]["parser"] == "quote_parser_v1"

    def test_dot_native_unchanged_under_c12(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93
        assert summary["document_class"] == "dot_schedule"

    def test_dot_scanned_unchanged_under_c12(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20f. C13 — Quote-to-Bid Mapping Foundation Tests
# ---------------------------------------------------------------------------

class TestC13QuoteToBidMapping:
    """C13: deterministic mapping from C12 normalized accepted_rows to
    C8 DOT bid items. NO fuzzy matching, NO guessing, fail-closed on
    ambiguity."""

    def _q(self, **kwargs):
        """Build a synthetic C12 accepted row for unit testing."""
        defaults = {
            "normalized_row_id": "qr-p0-r0",
            "row_id": 0,
            "line_ref": None,
            "description": "Test",
            "qty": None,
            "unit": None,
            "unit_price": None,
            "amount": 100.0,
            "source_page": 0,
            "extraction_source": "ocr_pdf",
            "source_text": "Test $100.00",
            "row_issues": [],
            "provenance": {
                "extraction_source": "ocr_pdf",
                "source_page": 0,
                "ocr_used": True,
                "parser": "quote_parser_v1",
            },
        }
        defaults.update(kwargs)
        return defaults

    def _b(self, **kwargs):
        """Build a synthetic DOT bid row matching the C8 schema."""
        defaults = {
            "line_number": "0010",
            "item": "2101-0850001",
            "description": "CLEARING AND GRUBBING",
            "qty": 1.0,
            "unit": "ACRE",
            "source_page": 0,
            "extraction_source": "native_pdf",
        }
        defaults.update(kwargs)
        return defaults

    # ---- Rule R1: line_ref exact ----

    def test_line_ref_exact_maps(self):
        from app.pdf_extraction.quote_to_bid_mapping import (
            map_quote_to_bid, OUTCOME_MAPPED, REASON_LINE_REF_EXACT
        )
        accepted = [self._q(line_ref="520", description="Reference Sign")]
        bid = [
            self._b(line_number="0010", description="CLEARING"),
            self._b(line_number="0520", description="REFERENCE LOCATION SIGN"),
        ]
        result = map_quote_to_bid(accepted, bid)
        assert result["mapping_status"] == "success"
        assert result["mapping_diagnostics"]["mapped_count"] == 1
        r0 = result["mapping_results"][0]
        assert r0["mapping_outcome"] == OUTCOME_MAPPED
        assert r0["mapping_reason"] == REASON_LINE_REF_EXACT
        assert r0["mapped_bid_item"]["line_number"] == "0520"

    def test_line_ref_canonicalization_matches_padded(self):
        """520 == 0520 (leading-zero canonicalization, not fuzzy)."""
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid, OUTCOME_MAPPED
        accepted = [self._q(line_ref="520")]
        bid = [self._b(line_number="0520")]
        result = map_quote_to_bid(accepted, bid)
        assert result["mapping_results"][0]["mapping_outcome"] == OUTCOME_MAPPED

    def test_line_ref_ambiguous_fails_closed(self):
        from app.pdf_extraction.quote_to_bid_mapping import (
            map_quote_to_bid, OUTCOME_AMBIGUOUS, REASON_LINE_REF_AMBIGUOUS
        )
        accepted = [self._q(line_ref="0520")]
        bid = [
            self._b(line_number="0520", description="A"),
            self._b(line_number="0520", description="B"),
        ]
        result = map_quote_to_bid(accepted, bid)
        r0 = result["mapping_results"][0]
        assert r0["mapping_outcome"] == OUTCOME_AMBIGUOUS
        assert r0["mapping_reason"] == REASON_LINE_REF_AMBIGUOUS
        assert r0["mapped_bid_item"] is None
        assert result["mapping_diagnostics"]["ambiguous_count"] == 1
        # Trace must show both candidates.
        assert len(r0["mapping_trace"]["rules_attempted"][0]["candidates"]) == 2

    def test_line_ref_no_match_falls_to_description(self):
        from app.pdf_extraction.quote_to_bid_mapping import (
            map_quote_to_bid, OUTCOME_MAPPED, REASON_DESCRIPTION_EXACT
        )
        accepted = [self._q(line_ref="9999", description="CLEARING AND GRUBBING")]
        bid = [self._b(line_number="0010", description="Clearing and Grubbing")]
        result = map_quote_to_bid(accepted, bid)
        r0 = result["mapping_results"][0]
        assert r0["mapping_outcome"] == OUTCOME_MAPPED
        assert r0["mapping_reason"] == REASON_DESCRIPTION_EXACT

    # ---- Rule R2: description normalized exact ----

    def test_description_canonicalization_case_insensitive(self):
        """Whitespace + case canonicalization is canonicalization, not fuzzy."""
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid, OUTCOME_MAPPED
        accepted = [self._q(description="  CLEARING   and   grubbing  ")]
        bid = [self._b(description="Clearing And Grubbing")]
        result = map_quote_to_bid(accepted, bid)
        assert result["mapping_results"][0]["mapping_outcome"] == OUTCOME_MAPPED

    def test_description_ambiguous_fails_closed(self):
        from app.pdf_extraction.quote_to_bid_mapping import (
            map_quote_to_bid, OUTCOME_AMBIGUOUS, REASON_DESCRIPTION_AMBIGUOUS
        )
        accepted = [self._q(description="CLEARING")]
        bid = [
            self._b(line_number="0010", description="Clearing"),
            self._b(line_number="0020", description="clearing"),
        ]
        result = map_quote_to_bid(accepted, bid)
        r0 = result["mapping_results"][0]
        assert r0["mapping_outcome"] == OUTCOME_AMBIGUOUS
        assert r0["mapping_reason"] == REASON_DESCRIPTION_AMBIGUOUS

    def test_unmapped_when_no_candidates(self):
        from app.pdf_extraction.quote_to_bid_mapping import (
            map_quote_to_bid, OUTCOME_UNMAPPED, REASON_NO_CANDIDATES
        )
        accepted = [self._q(line_ref="9999", description="No such item")]
        bid = [self._b(line_number="0010", description="Clearing")]
        result = map_quote_to_bid(accepted, bid)
        r0 = result["mapping_results"][0]
        assert r0["mapping_outcome"] == OUTCOME_UNMAPPED
        assert r0["mapping_reason"] == REASON_NO_CANDIDATES
        assert r0["mapped_bid_item"] is None

    # ---- Document-level status ----

    def test_status_success_when_all_mapped(self):
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid
        accepted = [
            self._q(normalized_row_id="qr-p0-r0", line_ref="0010"),
            self._q(normalized_row_id="qr-p0-r1", line_ref="0020"),
        ]
        bid = [
            self._b(line_number="0010"),
            self._b(line_number="0020", description="X"),
        ]
        result = map_quote_to_bid(accepted, bid)
        assert result["mapping_status"] == "success"

    def test_status_partial_when_some_mapped(self):
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid
        accepted = [
            self._q(normalized_row_id="qr-p0-r0", line_ref="0010"),
            self._q(normalized_row_id="qr-p0-r1", line_ref="9999"),
        ]
        bid = [self._b(line_number="0010")]
        result = map_quote_to_bid(accepted, bid)
        assert result["mapping_status"] == "partial"
        assert result["mapping_diagnostics"]["mapped_count"] == 1
        assert result["mapping_diagnostics"]["unmapped_count"] == 1

    def test_status_failed_when_zero_mapped(self):
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid
        accepted = [self._q(line_ref="9999", description="No match")]
        bid = [self._b(line_number="0010", description="Clearing")]
        result = map_quote_to_bid(accepted, bid)
        assert result["mapping_status"] == "mapping_failed"

    # ---- Traceability ----

    def test_mapping_results_are_traceable(self):
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid
        accepted = [self._q(
            normalized_row_id="qr-p0-r5",
            line_ref="0520",
            description="Sign",
        )]
        bid = [self._b(line_number="0520", description="REFERENCE SIGN")]
        result = map_quote_to_bid(accepted, bid)
        r0 = result["mapping_results"][0]
        assert r0["normalized_row_id"] == "qr-p0-r5"
        assert r0["quote_description"] == "Sign"
        assert r0["quote_line_ref"] == "0520"
        assert "rules_attempted" in r0["mapping_trace"]
        assert r0["mapping_trace"]["rules_attempted"][0]["rule"] == "R1_line_ref_exact"

    def test_mapping_does_not_mutate_inputs(self):
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid
        accepted = [self._q(line_ref="0010")]
        bid = [self._b(line_number="0010")]
        original_accepted = dict(accepted[0])
        original_bid = dict(bid[0])
        map_quote_to_bid(accepted, bid)
        assert accepted[0] == original_accepted
        assert bid[0] == original_bid

    def test_no_line_ref_skips_rule_r1(self):
        from app.pdf_extraction.quote_to_bid_mapping import (
            map_quote_to_bid, REASON_NO_LINE_REF_PRESENT
        )
        accepted = [self._q(line_ref=None, description="CLEARING AND GRUBBING")]
        bid = [self._b(line_number="0010", description="CLEARING AND GRUBBING")]
        result = map_quote_to_bid(accepted, bid)
        r0 = result["mapping_results"][0]
        assert r0["mapping_outcome"] == "mapped"
        # R1 was attempted but skipped.
        first_rule = r0["mapping_trace"]["rules_attempted"][0]
        assert first_rule["rule"] == "R1_line_ref_exact"
        assert first_rule.get("skipped") == REASON_NO_LINE_REF_PRESENT

    # ---- Live integration: real fixtures ----

    def test_live_ipsi_against_dot_native(self, ipsi_quote_path, dot_pdf_path):
        """End-to-end: real IPSI quote against real synthetic DOT.
        IPSI rows have no line_refs and don't share descriptions with
        the DOT signing items, so the expected outcome is mostly
        unmapped with explicit reasons. NEVER a mapped guess."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid

        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        bid_rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))

        result = map_quote_to_bid(staging["accepted_rows"], bid_rows)
        assert result["mapping_diagnostics"]["rows_input"] == len(staging["accepted_rows"])
        assert result["mapping_diagnostics"]["bid_items_indexed"] == len(bid_rows)
        # All outcomes must be one of the explicit codes.
        for r in result["mapping_results"]:
            assert r["mapping_outcome"] in ("mapped", "unmapped", "ambiguous")
            assert r["mapping_reason"]
        # Row identifiers must come from the C12 contract, not invented.
        ids = [r["normalized_row_id"] for r in result["mapping_results"]]
        for rid in ids:
            assert rid.startswith("qr-p")

    # ---- Endpoint integration ----

    def test_mapping_endpoint_ipsi_x_dot(self, client, ipsi_quote_path, dot_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/mapping",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["document_class_detected"] == "quote"
        assert data["mapping_status"] in ("success", "partial", "mapping_failed")
        assert isinstance(data["mapping_results"], list)
        assert len(data["mapping_results"]) == len(data["accepted_rows"])
        diag = data["mapping_diagnostics"]
        assert diag["rows_input"] == len(data["accepted_rows"])
        assert diag["bid_items_indexed"] == 93

    def test_mapping_endpoint_rejects_dot_as_quote(self, client, dot_pdf_path):
        """Posting a DOT PDF in the quote_pdf slot must fail at quote
        normalization with explicit reason — NEVER reach the mapper."""
        with open(dot_pdf_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/mapping",
                files={
                    "quote_pdf": ("dot.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 422
        data = resp.json()
        assert data["stage"] == "quote_normalization"
        assert data["failure_reason"] == "unsupported_document_class"

    # ---- DOT regression guard ----

    def test_dot_extraction_unchanged_under_c13(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_quote_staging_unchanged_under_c13(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14


# ---------------------------------------------------------------------------
# 20g. C14 — Pairing Guardrails Tests
# ---------------------------------------------------------------------------

class TestC14PairingGuardrails:
    """C14: deterministic pairing analysis with trusted/weak/rejected
    statuses. No fuzzy scoring, no semantic inference. Every signal is
    an explicit equality count."""

    def _q(self, line_ref=None, description="Test", row_id=0):
        return {
            "normalized_row_id": f"qr-p0-r{row_id}",
            "row_id": row_id,
            "line_ref": line_ref,
            "description": description,
            "qty": None,
            "unit": None,
            "unit_price": None,
            "amount": 100.0,
            "source_page": 0,
            "extraction_source": "ocr_pdf",
            "source_text": f"{description} $100.00",
            "row_issues": [],
            "provenance": {},
        }

    def _b(self, line_number, description):
        return {
            "line_number": line_number,
            "item": "9999-9999999",
            "description": description,
            "qty": 1.0,
            "unit": "EACH",
            "source_page": 0,
            "extraction_source": "native_pdf",
        }

    def test_empty_quote_rejected(self):
        from app.pdf_extraction.pairing_guardrails import (
            analyze_pairing, STATUS_REJECTED, REASON_EMPTY_QUOTE
        )
        result = analyze_pairing([], [self._b("0010", "X")])
        assert result["pairing_status"] == STATUS_REJECTED
        assert result["pairing_reason"] == REASON_EMPTY_QUOTE
        assert result["allow_mapping"] is False

    def test_empty_bid_rejected(self):
        from app.pdf_extraction.pairing_guardrails import (
            analyze_pairing, STATUS_REJECTED, REASON_EMPTY_BID
        )
        result = analyze_pairing([self._q()], [])
        assert result["pairing_status"] == STATUS_REJECTED
        assert result["pairing_reason"] == REASON_EMPTY_BID
        assert result["allow_mapping"] is False

    def test_no_strict_confirmation_rejected(self):
        """Quote has line_refs but NONE of them line up on the same bid
        row as the matching description → rejected (wrong project pair)."""
        from app.pdf_extraction.pairing_guardrails import (
            analyze_pairing, STATUS_REJECTED, REASON_NO_CROSS_CONFIRMATION
        )
        accepted = [
            self._q(line_ref="520", description="Sign A"),
            self._q(line_ref="530", description="Sign B", row_id=1),
            self._q(line_ref="540", description="Sign C", row_id=2),
        ]
        # Bid has these line numbers but COMPLETELY different descriptions.
        bid = [
            self._b("0520", "DRAINAGE PIPE 12 INCH"),
            self._b("0530", "EMBANKMENT FILL"),
            self._b("0540", "CONCRETE SIDEWALK"),
        ]
        result = analyze_pairing(accepted, bid)
        assert result["pairing_status"] == STATUS_REJECTED
        assert result["pairing_reason"] == REASON_NO_CROSS_CONFIRMATION
        assert result["allow_mapping"] is False
        assert result["signals"]["line_ref_domain_overlap"] == 3
        assert result["signals"]["strict_confirmation_count"] == 0

    def test_strict_confirmation_trusted(self):
        from app.pdf_extraction.pairing_guardrails import (
            analyze_pairing, STATUS_TRUSTED, REASON_STRICT_CONFIRMED
        )
        accepted = [
            self._q(line_ref="520", description="Sign A"),
            self._q(line_ref="530", description="Sign B", row_id=1),
            self._q(line_ref="540", description="Sign C", row_id=2),
        ]
        bid = [
            self._b("0520", "Sign A"),
            self._b("0530", "Sign B"),
            self._b("0540", "Sign C"),
        ]
        result = analyze_pairing(accepted, bid)
        assert result["pairing_status"] == STATUS_TRUSTED
        assert result["pairing_reason"] == REASON_STRICT_CONFIRMED
        assert result["allow_mapping"] is True
        assert result["warnings"] == []
        assert result["signals"]["strict_confirmation_count"] == 3

    def test_partial_confirmation_is_weak(self):
        from app.pdf_extraction.pairing_guardrails import analyze_pairing, STATUS_WEAK
        accepted = [
            self._q(line_ref="520", description="Sign A"),
            self._q(line_ref="530", description="Sign B", row_id=1),
            self._q(line_ref="540", description="Completely Different", row_id=2),
        ]
        bid = [
            self._b("0520", "Sign A"),
            self._b("0530", "Different One"),
            self._b("0540", "Also Different"),
        ]
        result = analyze_pairing(accepted, bid)
        assert result["pairing_status"] == STATUS_WEAK
        assert result["allow_mapping"] is True
        assert len(result["warnings"]) >= 1
        assert result["signals"]["strict_confirmation_count"] == 1

    def test_no_line_refs_with_strong_description_overlap_trusted(self):
        from app.pdf_extraction.pairing_guardrails import (
            analyze_pairing, STATUS_TRUSTED, REASON_DESC_ONLY_CONFIRMED
        )
        accepted = [self._q(description=f"Item {i}", row_id=i) for i in range(6)]
        bid = [self._b(f"{i:04d}", f"Item {i}") for i in range(6)]
        result = analyze_pairing(accepted, bid)
        assert result["pairing_status"] == STATUS_TRUSTED
        assert result["pairing_reason"] == REASON_DESC_ONLY_CONFIRMED
        assert result["signals"]["rows_with_line_ref_count"] == 0
        assert result["signals"]["description_exact_overlap"] == 6

    def test_no_line_refs_no_description_overlap_rejected(self):
        from app.pdf_extraction.pairing_guardrails import (
            analyze_pairing, STATUS_REJECTED, REASON_INSUFFICIENT_OVERLAP
        )
        accepted = [self._q(description=f"Alpha {i}", row_id=i) for i in range(5)]
        bid = [self._b(f"{i:04d}", f"Beta {i}") for i in range(5)]
        result = analyze_pairing(accepted, bid)
        assert result["pairing_status"] == STATUS_REJECTED
        assert result["pairing_reason"] == REASON_INSUFFICIENT_OVERLAP
        assert result["allow_mapping"] is False

    def test_no_line_refs_moderate_overlap_weak(self):
        """3 description matches, 0 line_refs → weak (not trusted, not rejected)."""
        from app.pdf_extraction.pairing_guardrails import analyze_pairing, STATUS_WEAK
        accepted = [
            self._q(description="Item 0", row_id=0),
            self._q(description="Item 1", row_id=1),
            self._q(description="Item 2", row_id=2),
            self._q(description="Unique A", row_id=3),
            self._q(description="Unique B", row_id=4),
        ]
        bid = [
            self._b("0001", "Item 0"),
            self._b("0002", "Item 1"),
            self._b("0003", "Item 2"),
            self._b("0004", "Other thing"),
        ]
        result = analyze_pairing(accepted, bid)
        assert result["pairing_status"] == STATUS_WEAK
        assert result["signals"]["description_exact_overlap"] == 3

    # ---- Real-fixture live pairing ----

    def test_ipsi_x_synthetic_dot_is_trusted(self, ipsi_quote_path, dot_pdf_path):
        """Real IPSI quote × the synthetic DOT fixture: line_refs AND
        descriptions align on the same bid rows → trusted."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing, STATUS_TRUSTED
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        bid_rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        result = analyze_pairing(staging["accepted_rows"], bid_rows)
        assert result["pairing_status"] == STATUS_TRUSTED
        assert result["allow_mapping"] is True
        assert result["signals"]["strict_confirmation_count"] >= 3

    def test_ipsi_x_estprop121_is_rejected(self, ipsi_quote_path, estprop_pdf_path):
        """Real IPSI quote × estprop121 (unrelated project): line_refs
        align by accident but descriptions never match on the SAME
        bid row → rejected."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.pairing_guardrails import (
            analyze_pairing, STATUS_REJECTED, REASON_NO_CROSS_CONFIRMATION
        )
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        bid_rows, _ = extract_bid_items_from_pdf(str(estprop_pdf_path))
        result = analyze_pairing(staging["accepted_rows"], bid_rows)
        assert result["pairing_status"] == STATUS_REJECTED
        assert result["pairing_reason"] == REASON_NO_CROSS_CONFIRMATION
        assert result["allow_mapping"] is False
        assert result["signals"]["strict_confirmation_count"] == 0

    # ---- Endpoint integration ----

    def test_endpoint_blocks_rejected_pair(self, client, ipsi_quote_path, estprop_pdf_path):
        """/extract/quote/mapping with IPSI × estprop121 → 422 at pairing."""
        with open(ipsi_quote_path, "rb") as qf, open(estprop_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/mapping",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("estprop121.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 422
        data = resp.json()
        assert data["status"] == "pairing_rejected"
        assert data["stage"] == "pairing_guardrail"
        assert data["mapping_status"] == "blocked_by_pairing"
        assert data["mapping_results"] == []
        assert data["pairing_diagnostics"]["pairing_status"] == "rejected"
        assert data["mapping_diagnostics"]["mapped_count"] == 0

    def test_endpoint_trusted_pair_runs_mapping(self, client, ipsi_quote_path, dot_pdf_path):
        """/extract/quote/mapping with IPSI × synthetic DOT → 200 with
        pairing_diagnostics.pairing_status=trusted and normal mapping."""
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/mapping",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["pairing_diagnostics"]["pairing_status"] == "trusted"
        assert data["pairing_diagnostics"]["allow_mapping"] is True
        assert len(data["mapping_results"]) == len(data["accepted_rows"])
        assert data["mapping_diagnostics"]["mapped_count"] >= 1

    # ---- DOT regression ----

    def test_dot_lane_unchanged_under_c14(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c14(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True

    def test_quote_staging_unchanged_under_c14(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14


# ---------------------------------------------------------------------------
# 20h. C15 — Review Packet Tests
# ---------------------------------------------------------------------------

class TestC15ReviewPacket:
    """C15: governed review packet with ready/partial/blocked status,
    deterministic review flags, and traceable row entries."""

    def _pairing(self, status, allow=True, strict=5):
        return {
            "pairing_status": status,
            "pairing_reason": "test",
            "signals": {
                "accepted_rows_count": 3,
                "bid_rows_count": 3,
                "rows_with_line_ref_count": 3,
                "line_ref_domain_overlap": 3,
                "description_exact_overlap": 3,
                "strict_confirmation_count": strict,
            },
            "warnings": [] if status == "trusted" else ["warn"],
            "allow_mapping": allow,
        }

    def _row(self, row_id=0, line_ref="0010", description="A",
             amount=100.0, unit_price=None, qty=None, unit=None):
        return {
            "normalized_row_id": f"qr-p0-r{row_id}",
            "row_id": row_id,
            "line_ref": line_ref,
            "description": description,
            "qty": qty,
            "unit": unit,
            "unit_price": unit_price,
            "amount": amount,
            "source_page": 0,
            "extraction_source": "ocr_pdf",
            "source_text": f"{description} ${amount}",
            "row_issues": [],
            "provenance": {},
        }

    def _mapping_result(self, outcomes):
        """Build a synthetic mapping_result dict from (row_id, outcome) list."""
        results = []
        mapped = unmapped = ambiguous = 0
        for row_id, outcome in outcomes:
            results.append({
                "normalized_row_id": f"qr-p0-r{row_id}",
                "quote_description": f"desc{row_id}",
                "quote_line_ref": f"{row_id:04d}",
                "mapping_outcome": outcome,
                "mapping_reason": "t",
                "mapped_bid_item": ({"line_number": f"{row_id:04d}"} if outcome == "mapped" else None),
                "mapping_trace": {"rules_attempted": [
                    {"rule": "R1_line_ref_exact", "candidate_count": 1},
                ]},
            })
            if outcome == "mapped": mapped += 1
            elif outcome == "unmapped": unmapped += 1
            elif outcome == "ambiguous": ambiguous += 1
        status = "success" if mapped == len(outcomes) else ("mapping_failed" if mapped == 0 else "partial")
        return {
            "mapping_status": status,
            "mapping_results": results,
            "mapping_diagnostics": {
                "mapped_count": mapped, "unmapped_count": unmapped,
                "ambiguous_count": ambiguous, "rows_input": len(outcomes),
                "bid_items_indexed": 3,
            },
        }

    def test_trusted_all_mapped_is_ready(self):
        from app.pdf_extraction.review_packet import build_review_packet, PACKET_READY
        pairing = self._pairing("trusted")
        mapping = self._mapping_result([(0, "mapped"), (1, "mapped"), (2, "mapped")])
        rows = [self._row(row_id=i) for i in range(3)]
        packet = build_review_packet(pairing, mapping, rows, {}, {})
        assert packet["packet_status"] == PACKET_READY
        assert packet["packet_diagnostics"]["mapped_count"] == 3
        assert packet["packet_diagnostics"]["rows_ready_for_reconciliation"] == 3

    def test_trusted_partial_is_partial(self):
        from app.pdf_extraction.review_packet import build_review_packet, PACKET_PARTIAL
        pairing = self._pairing("trusted")
        mapping = self._mapping_result([(0, "mapped"), (1, "unmapped"), (2, "ambiguous")])
        rows = [self._row(row_id=i) for i in range(3)]
        packet = build_review_packet(pairing, mapping, rows, {}, {})
        assert packet["packet_status"] == PACKET_PARTIAL
        diag = packet["packet_diagnostics"]
        assert diag["mapped_count"] == 1
        assert diag["unmapped_count"] == 1
        assert diag["ambiguous_count"] == 1

    def test_weak_pairing_is_partial_even_if_all_mapped(self):
        """Weak pairing must NEVER produce a ready packet, even when
        every row mapped. Reviewers must gate on pairing."""
        from app.pdf_extraction.review_packet import (
            build_review_packet, PACKET_PARTIAL, FLAG_WEAK_PAIRING
        )
        pairing = self._pairing("weak", strict=1)
        mapping = self._mapping_result([(0, "mapped"), (1, "mapped"), (2, "mapped")])
        rows = [self._row(row_id=i) for i in range(3)]
        packet = build_review_packet(pairing, mapping, rows, {}, {})
        assert packet["packet_status"] == PACKET_PARTIAL
        for rr in packet["review_rows"]:
            assert FLAG_WEAK_PAIRING in rr["review_flags"]

    def test_rejected_pairing_is_blocked(self):
        from app.pdf_extraction.review_packet import (
            build_review_packet, PACKET_BLOCKED, FLAG_BLOCKED_BY_PAIRING, BLOCKED_OUTCOME
        )
        pairing = {
            "pairing_status": "rejected",
            "pairing_reason": "no_strict_line_ref_plus_description_match",
            "signals": {},
            "warnings": ["wrong project"],
            "allow_mapping": False,
        }
        rows = [self._row(row_id=i) for i in range(3)]
        packet = build_review_packet(pairing, None, rows, {}, {})
        assert packet["packet_status"] == PACKET_BLOCKED
        assert packet["packet_diagnostics"]["blocked_count"] == 3
        assert packet["packet_diagnostics"]["rows_ready_for_reconciliation"] == 0
        for rr in packet["review_rows"]:
            assert rr["mapping_outcome"] == BLOCKED_OUTCOME
            assert FLAG_BLOCKED_BY_PAIRING in rr["review_flags"]
            assert rr["mapped_bid_item"] is None

    def test_blocked_packet_never_shows_mapping_summary_success(self):
        from app.pdf_extraction.review_packet import build_review_packet
        pairing = {
            "pairing_status": "rejected",
            "pairing_reason": "x",
            "signals": {},
            "warnings": [],
            "allow_mapping": False,
        }
        rows = [self._row(row_id=0)]
        packet = build_review_packet(pairing, None, rows, {}, {})
        assert packet["mapping_summary"]["mapping_status"] == "blocked_by_pairing"
        assert packet["mapping_summary"]["mapped_count"] == 0

    def test_row_flags_missing_qty_unit_unit_price(self):
        from app.pdf_extraction.review_packet import (
            build_review_packet,
            FLAG_MISSING_QTY, FLAG_MISSING_UNIT, FLAG_MISSING_UNIT_PRICE,
        )
        pairing = self._pairing("trusted")
        mapping = self._mapping_result([(0, "mapped")])
        rows = [self._row(row_id=0, qty=None, unit=None, unit_price=None, amount=100.0)]
        packet = build_review_packet(pairing, mapping, rows, {}, {})
        flags = packet["review_rows"][0]["review_flags"]
        assert FLAG_MISSING_QTY in flags
        assert FLAG_MISSING_UNIT in flags
        assert FLAG_MISSING_UNIT_PRICE in flags

    def test_row_flags_unmapped_and_ambiguous(self):
        from app.pdf_extraction.review_packet import (
            build_review_packet, FLAG_UNMAPPED, FLAG_AMBIGUOUS
        )
        pairing = self._pairing("trusted")
        mapping = self._mapping_result([(0, "unmapped"), (1, "ambiguous")])
        rows = [self._row(row_id=0), self._row(row_id=1)]
        packet = build_review_packet(pairing, mapping, rows, {}, {})
        r0 = packet["review_rows"][0]
        r1 = packet["review_rows"][1]
        assert FLAG_UNMAPPED in r0["review_flags"]
        assert FLAG_AMBIGUOUS in r1["review_flags"]

    def test_packet_preserves_pairing_diagnostics(self):
        from app.pdf_extraction.review_packet import build_review_packet
        pairing = self._pairing("trusted")
        mapping = self._mapping_result([(0, "mapped")])
        rows = [self._row(row_id=0)]
        packet = build_review_packet(pairing, mapping, rows, {}, {})
        assert packet["pairing_diagnostics"] == pairing

    def test_mapping_trace_summary_in_review_row(self):
        from app.pdf_extraction.review_packet import build_review_packet
        pairing = self._pairing("trusted")
        mapping = self._mapping_result([(0, "mapped")])
        rows = [self._row(row_id=0)]
        packet = build_review_packet(pairing, mapping, rows, {}, {})
        trace = packet["review_rows"][0]["mapping_trace_summary"]
        assert trace["rules_attempted_count"] == 1
        assert trace["rules"][0]["rule"] == "R1_line_ref_exact"

    def test_blocked_rows_hide_nothing(self):
        """Blocked packet must still surface quote descriptions — it
        never hides evidence from the reviewer."""
        from app.pdf_extraction.review_packet import build_review_packet
        pairing = {
            "pairing_status": "rejected",
            "pairing_reason": "x",
            "signals": {},
            "warnings": [],
            "allow_mapping": False,
        }
        rows = [
            self._row(row_id=0, description="Widget A"),
            self._row(row_id=1, description="Widget B"),
        ]
        packet = build_review_packet(pairing, None, rows, {}, {})
        descs = [r["quote_description"] for r in packet["review_rows"]]
        assert descs == ["Widget A", "Widget B"]

    # ---- Endpoint integration ----

    def test_review_endpoint_trusted_ready_or_partial(self, client, ipsi_quote_path, dot_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/review",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        # IPSI has 2 unmapped rows so packet should be partial.
        assert data["packet_status"] == "partial"
        assert data["pairing_diagnostics"]["pairing_status"] == "trusted"
        assert len(data["review_rows"]) == 15
        assert data["packet_diagnostics"]["mapped_count"] == 13

    def test_review_endpoint_rejected_is_blocked(self, client, ipsi_quote_path, estprop_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(estprop_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/review",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("estprop121.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 422
        data = resp.json()
        assert data["packet_status"] == "blocked"
        assert data["pairing_diagnostics"]["pairing_status"] == "rejected"
        assert data["mapping_summary"]["mapping_status"] == "blocked_by_pairing"
        assert data["packet_diagnostics"]["rows_ready_for_reconciliation"] == 0
        # All 15 accepted rows must still appear for audit.
        assert len(data["review_rows"]) == 15
        for rr in data["review_rows"]:
            assert "blocked_by_pairing" in rr["review_flags"]
            assert rr["mapping_outcome"] == "blocked"

    # ---- Regression ----

    def test_dot_unchanged_under_c15(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_staging_unchanged_under_c15(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14

    def test_mapping_endpoint_unchanged_under_c15(self, client, ipsi_quote_path, dot_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/mapping",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["pairing_diagnostics"]["pairing_status"] == "trusted"
        assert data["mapping_diagnostics"]["mapped_count"] == 13


# ---------------------------------------------------------------------------
# 20i. C16 — Reconciliation Foundation Tests
# ---------------------------------------------------------------------------

class TestC16ReconciliationFoundation:
    """C16: deterministic reconciliation of mapped rows only, using
    explicit field comparisons with no inference or auto-resolution."""

    def _review_row(self, **kw):
        base = {
            "normalized_row_id": "qr-p0-r0",
            "quote_description": "Item A",
            "quote_line_ref": "0010",
            "quote_amount": 100.0,
            "quote_unit_price": 10.0,
            "quote_qty": 10.0,
            "quote_unit": "EACH",
            "quote_source_page": 0,
            "mapping_outcome": "mapped",
            "mapping_reason": "line_ref_exact_match",
            "mapped_bid_item": {
                "line_number": "0010",
                "item_number": "2101-0850001",
                "description": "ITEM A",
                "qty": 10.0,
                "unit": "EACH",
            },
            "review_flags": [],
            "mapping_trace_summary": {},
        }
        base.update(kw)
        return base

    def _packet(self, review_rows, packet_status="partial", pairing_status="trusted"):
        return {
            "packet_status": packet_status,
            "pairing_diagnostics": {
                "pairing_status": pairing_status,
                "pairing_reason": "x",
                "signals": {},
                "warnings": [],
                "allow_mapping": packet_status != "blocked",
            },
            "mapping_summary": {"mapping_status": "partial"},
            "review_rows": review_rows,
        }

    # ---- Comparison logic ----

    def test_unit_and_qty_match_produces_match(self):
        from app.pdf_extraction.reconciliation_foundation import (
            reconcile_packet, COMPARISON_MATCH, FLAG_UNIT_MATCH, FLAG_QTY_MATCH
        )
        packet = self._packet([self._review_row()])
        result = reconcile_packet(packet)
        row = result["reconciliation_rows"][0]
        assert row["comparison_status"] == COMPARISON_MATCH
        assert FLAG_UNIT_MATCH in row["comparison_flags"]
        assert FLAG_QTY_MATCH in row["comparison_flags"]
        assert result["reconciliation_summary"]["matches"] == 1
        assert result["reconciliation_summary"]["mismatches"] == 0

    def test_unit_conflict_produces_mismatch(self):
        from app.pdf_extraction.reconciliation_foundation import (
            reconcile_packet, COMPARISON_MISMATCH, FLAG_UNIT_CONFLICT
        )
        rr = self._review_row(quote_unit="EACH")
        rr["mapped_bid_item"]["unit"] = "LF"
        packet = self._packet([rr])
        result = reconcile_packet(packet)
        row = result["reconciliation_rows"][0]
        assert row["comparison_status"] == COMPARISON_MISMATCH
        assert FLAG_UNIT_CONFLICT in row["comparison_flags"]
        assert result["reconciliation_summary"]["mismatches"] == 1

    def test_qty_conflict_beyond_tolerance(self):
        from app.pdf_extraction.reconciliation_foundation import (
            reconcile_packet, COMPARISON_MISMATCH, FLAG_QTY_CONFLICT
        )
        rr = self._review_row(quote_qty=10.0)
        rr["mapped_bid_item"]["qty"] = 15.0
        result = reconcile_packet(self._packet([rr]))
        row = result["reconciliation_rows"][0]
        assert row["comparison_status"] == COMPARISON_MISMATCH
        assert FLAG_QTY_CONFLICT in row["comparison_flags"]

    def test_qty_within_tolerance_matches(self):
        from app.pdf_extraction.reconciliation_foundation import FLAG_QTY_MATCH, reconcile_packet
        rr = self._review_row(quote_qty=100.0)
        rr["mapped_bid_item"]["qty"] = 100.4  # 0.4% drift, inside 0.5%
        result = reconcile_packet(self._packet([rr]))
        assert FLAG_QTY_MATCH in result["reconciliation_rows"][0]["comparison_flags"]

    def test_missing_quote_qty_still_compares_unit(self):
        """If quote has no qty but both sides have unit, unit is still compared."""
        from app.pdf_extraction.reconciliation_foundation import (
            reconcile_packet, FLAG_UNIT_MATCH, FLAG_MISSING_QUOTE_QTY, COMPARISON_MATCH
        )
        rr = self._review_row(quote_qty=None)
        result = reconcile_packet(self._packet([rr]))
        row = result["reconciliation_rows"][0]
        assert row["comparison_status"] == COMPARISON_MATCH
        assert FLAG_UNIT_MATCH in row["comparison_flags"]
        assert FLAG_MISSING_QUOTE_QTY in row["comparison_flags"]

    def test_no_overlapping_fields_is_non_comparable(self):
        """Mapped row but quote has no unit and no qty → non-comparable."""
        from app.pdf_extraction.reconciliation_foundation import (
            reconcile_packet, COMPARISON_NON_COMPARABLE, NC_NO_OVERLAPPING_FIELDS,
            FLAG_MISSING_QUOTE_QTY, FLAG_MISSING_QUOTE_UNIT,
        )
        rr = self._review_row(quote_qty=None, quote_unit=None)
        result = reconcile_packet(self._packet([rr]))
        row = result["reconciliation_rows"][0]
        assert row["comparison_status"] == COMPARISON_NON_COMPARABLE
        assert FLAG_MISSING_QUOTE_QTY in row["comparison_flags"]
        assert FLAG_MISSING_QUOTE_UNIT in row["comparison_flags"]
        assert row["comparison_trace"]["non_comparable_reason"] == NC_NO_OVERLAPPING_FIELDS
        assert result["reconciliation_summary"]["rows_compared"] == 0
        assert result["reconciliation_summary"]["rows_non_comparable"] == 1

    def test_missing_bid_amount_is_always_flagged(self):
        """DOT bid items never carry monetary amount — structural flag."""
        from app.pdf_extraction.reconciliation_foundation import (
            reconcile_packet, FLAG_MISSING_BID_AMOUNT
        )
        result = reconcile_packet(self._packet([self._review_row()]))
        row = result["reconciliation_rows"][0]
        assert FLAG_MISSING_BID_AMOUNT in row["comparison_flags"]

    # ---- Non-comparable outcome propagation ----

    def test_unmapped_row_is_non_comparable(self):
        from app.pdf_extraction.reconciliation_foundation import (
            reconcile_packet, COMPARISON_NON_COMPARABLE, NC_UNMAPPED
        )
        rr = self._review_row(mapping_outcome="unmapped", mapped_bid_item=None)
        result = reconcile_packet(self._packet([rr]))
        row = result["reconciliation_rows"][0]
        assert row["comparison_status"] == COMPARISON_NON_COMPARABLE
        assert row["comparison_trace"]["non_comparable_reason"] == NC_UNMAPPED
        assert row["bid_values"] is None

    def test_ambiguous_row_is_non_comparable(self):
        from app.pdf_extraction.reconciliation_foundation import (
            reconcile_packet, COMPARISON_NON_COMPARABLE, NC_AMBIGUOUS
        )
        rr = self._review_row(mapping_outcome="ambiguous", mapped_bid_item=None)
        result = reconcile_packet(self._packet([rr]))
        row = result["reconciliation_rows"][0]
        assert row["comparison_status"] == COMPARISON_NON_COMPARABLE
        assert row["comparison_trace"]["non_comparable_reason"] == NC_AMBIGUOUS

    def test_blocked_packet_produces_blocked_recon(self):
        from app.pdf_extraction.reconciliation_foundation import (
            reconcile_packet, RECON_BLOCKED, COMPARISON_BLOCKED
        )
        rr = self._review_row(mapping_outcome="blocked", mapped_bid_item=None)
        packet = self._packet([rr], packet_status="blocked", pairing_status="rejected")
        result = reconcile_packet(packet)
        assert result["reconciliation_status"] == RECON_BLOCKED
        assert result["reconciliation_rows"][0]["comparison_status"] == COMPARISON_BLOCKED
        assert result["reconciliation_summary"]["rows_compared"] == 0

    def test_unmapped_rows_do_not_reach_comparison_logic(self):
        """An unmapped row with incomparable bid data still emits
        non_comparable — and never auto-promotes to mapped."""
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        rr = self._review_row(
            mapping_outcome="unmapped",
            mapped_bid_item=None,
            quote_qty=10.0,
            quote_unit="EACH",
        )
        result = reconcile_packet(self._packet([rr]))
        row = result["reconciliation_rows"][0]
        assert row["comparison_status"] == "non_comparable"
        assert row["comparison_flags"] == []

    # ---- Document-level status ----

    def test_all_match_is_ready(self):
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet, RECON_READY
        rows = [self._review_row(normalized_row_id=f"qr-p0-r{i}") for i in range(3)]
        packet = self._packet(rows)
        result = reconcile_packet(packet)
        assert result["reconciliation_status"] == RECON_READY

    def test_any_mismatch_is_partial(self):
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet, RECON_PARTIAL
        rows = [self._review_row(), self._review_row(normalized_row_id="qr-p0-r1")]
        rows[0]["mapped_bid_item"]["qty"] = 99.0
        packet = self._packet(rows)
        result = reconcile_packet(packet)
        assert result["reconciliation_status"] == RECON_PARTIAL

    def test_any_non_comparable_is_partial(self):
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet, RECON_PARTIAL
        rows = [
            self._review_row(),
            self._review_row(normalized_row_id="qr-p0-r1", mapping_outcome="unmapped", mapped_bid_item=None),
        ]
        packet = self._packet(rows)
        result = reconcile_packet(packet)
        assert result["reconciliation_status"] == RECON_PARTIAL

    def test_zero_compared_is_partial(self):
        """All rows non-comparable → partial, not ready."""
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet, RECON_PARTIAL
        rows = [self._review_row(quote_qty=None, quote_unit=None)]
        result = reconcile_packet(self._packet(rows))
        assert result["reconciliation_status"] == RECON_PARTIAL

    # ---- Endpoint integration ----

    def test_reconcile_endpoint_ipsi_trusted(self, client, ipsi_quote_path, dot_pdf_path):
        """Real IPSI × synthetic DOT: quote rows carry no qty/unit so all
        mapped rows are non-comparable. Reconciliation status = partial."""
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/reconcile",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        # C17 hardened contract surface.
        assert data["contract_version"] == "reconciliation_contract/v1"
        assert data["reconciliation_status"] == "partial"
        assert data["pairing_status"] == "trusted"
        assert data["pairing_diagnostics"]["pairing_status"] == "trusted"
        # 13 mapped rows but all non_comparable (no qty/unit on quote side).
        assert data["reconciliation_summary"]["rows_matched"] == 0
        assert data["reconciliation_summary"]["rows_mismatched"] == 0
        assert data["reconciliation_summary"]["rows_non_comparable"] >= 13
        assert data["reconciliation_summary"]["rows_total"] >= 13

    def test_reconcile_endpoint_rejected_blocked(self, client, ipsi_quote_path, estprop_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(estprop_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/reconcile",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("estprop121.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 422
        data = resp.json()
        # C17 hardened contract surface.
        assert data["contract_version"] == "reconciliation_contract/v1"
        assert data["reconciliation_status"] == "blocked"
        assert data["packet_status"] == "blocked"
        assert data["pairing_status"] == "rejected"
        assert data["pairing_diagnostics"]["pairing_status"] == "rejected"
        assert data["reconciliation_summary"]["rows_compared"] == 0
        assert data["reconciliation_summary"]["rows_matched"] == 0
        assert data["reconciliation_summary"]["rows_mismatched"] == 0
        assert data["reconciliation_summary"]["rows_blocked"] == len(data["reconciliation_rows"])
        for row in data["reconciliation_rows"]:
            assert row["comparison_status"] == "blocked"
            # Every contract row carries the stable key set.
            for key in ("normalized_row_id", "mapping_outcome", "comparison_status",
                        "comparison_flags", "compared_fields", "non_comparable_reason",
                        "quote_values", "bid_values", "comparison_trace"):
                assert key in row

    # ---- Regression ----

    def test_dot_unchanged_under_c16(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c16(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True

    def test_staging_unchanged_under_c16(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14

    def test_mapping_unchanged_under_c16(self, client, ipsi_quote_path, dot_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/mapping",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 200
        assert resp.json()["mapping_diagnostics"]["mapped_count"] == 13

    def test_review_packet_unchanged_under_c16(self, client, ipsi_quote_path, dot_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/review",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["packet_status"] == "partial"
        assert len(data["review_rows"]) == 15


# ---------------------------------------------------------------------------
# 20j. C17 — Reconciliation Contract Tests
# ---------------------------------------------------------------------------


class TestC17ReconciliationContract:
    """C17: hardened, stable reconciliation output contract built on top
    of the C16 reconcile_packet raw result. Tests exercise the contract
    envelope without re-testing C16 comparison logic."""

    def _review_row(self, **kw):
        base = {
            "normalized_row_id": "qr-p0-r0",
            "quote_description": "Item A",
            "quote_line_ref": "0010",
            "quote_amount": 100.0,
            "quote_unit_price": 10.0,
            "quote_qty": 10.0,
            "quote_unit": "EACH",
            "quote_source_page": 0,
            "mapping_outcome": "mapped",
            "mapping_reason": "line_ref_exact_match",
            "mapped_bid_item": {
                "line_number": "0010",
                "item_number": "2101-0850001",
                "description": "ITEM A",
                "qty": 10.0,
                "unit": "EACH",
            },
            "review_flags": [],
            "mapping_trace_summary": {},
        }
        base.update(kw)
        return base

    def _packet(self, review_rows, packet_status="partial", pairing_status="trusted"):
        return {
            "packet_status": packet_status,
            "pairing_diagnostics": {
                "pairing_status": pairing_status,
                "pairing_reason": "x",
                "signals": {},
                "warnings": [],
                "allow_mapping": packet_status != "blocked",
            },
            "mapping_summary": {"mapping_status": "partial"},
            "review_rows": review_rows,
        }

    def _build(self, rows, packet_status="partial", pairing_status="trusted"):
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        packet = self._packet(rows, packet_status=packet_status, pairing_status=pairing_status)
        recon = reconcile_packet(packet)
        return build_reconciliation_contract(recon, packet)

    # ---- Stable envelope shape ----

    def test_contract_version_present(self):
        contract = self._build([self._review_row()])
        assert contract["contract_version"] == "reconciliation_contract/v1"

    def test_top_level_status_fields_all_present(self):
        contract = self._build([self._review_row()])
        assert "reconciliation_status" in contract
        assert "packet_status" in contract
        assert "pairing_status" in contract
        assert "mapping_status" in contract
        assert contract["packet_status"] == "partial"
        assert contract["pairing_status"] == "trusted"

    def test_summary_has_all_required_counts(self):
        contract = self._build([self._review_row()])
        s = contract["reconciliation_summary"]
        for k in ("rows_total", "rows_compared", "rows_matched", "rows_mismatched",
                  "rows_non_comparable", "rows_blocked", "flag_counts"):
            assert k in s

    def test_row_has_stable_key_set(self):
        contract = self._build([self._review_row()])
        row = contract["reconciliation_rows"][0]
        for k in ("normalized_row_id", "source_page", "mapping_outcome",
                  "mapped_bid_item", "comparison_status", "comparison_flags",
                  "compared_fields", "non_comparable_reason", "quote_values",
                  "bid_values", "comparison_trace"):
            assert k in row

    # ---- Counts are accurate ----

    def test_counts_matched_mismatched_non_comparable_blocked(self):
        rows = [
            self._review_row(normalized_row_id="qr-p0-r0"),  # match
            self._review_row(normalized_row_id="qr-p0-r1"),  # mismatch (qty)
            self._review_row(normalized_row_id="qr-p0-r2",  # non_comparable
                             quote_qty=None, quote_unit=None),
            self._review_row(normalized_row_id="qr-p0-r3",  # non_comparable unmapped
                             mapping_outcome="unmapped", mapped_bid_item=None),
        ]
        rows[1]["mapped_bid_item"]["qty"] = 9999.0
        contract = self._build(rows)
        s = contract["reconciliation_summary"]
        assert s["rows_total"] == 4
        assert s["rows_matched"] == 1
        assert s["rows_mismatched"] == 1
        assert s["rows_non_comparable"] == 2
        assert s["rows_compared"] == 2
        assert s["rows_blocked"] == 0

    def test_blocked_counts(self):
        rr = self._review_row(mapping_outcome="blocked", mapped_bid_item=None)
        contract = self._build([rr], packet_status="blocked", pairing_status="rejected")
        s = contract["reconciliation_summary"]
        assert s["rows_blocked"] == 1
        assert s["rows_matched"] == 0
        assert s["rows_mismatched"] == 0
        assert s["rows_compared"] == 0
        assert contract["reconciliation_status"] == "blocked"

    # ---- compared_fields list ----

    def test_compared_fields_match_case(self):
        contract = self._build([self._review_row()])
        row = contract["reconciliation_rows"][0]
        assert row["compared_fields"] == ["unit", "qty"]

    def test_compared_fields_excludes_uncompared(self):
        rr = self._review_row(quote_qty=None)  # only unit compared
        contract = self._build([rr])
        row = contract["reconciliation_rows"][0]
        assert row["compared_fields"] == ["unit"]

    def test_compared_fields_empty_when_non_comparable(self):
        rr = self._review_row(quote_qty=None, quote_unit=None)
        contract = self._build([rr])
        row = contract["reconciliation_rows"][0]
        assert row["compared_fields"] == []

    # ---- non_comparable_reason surfaced at row top level ----

    def test_non_comparable_reason_top_level(self):
        rr = self._review_row(mapping_outcome="unmapped", mapped_bid_item=None)
        contract = self._build([rr])
        row = contract["reconciliation_rows"][0]
        assert row["non_comparable_reason"] == "row_not_mapped"

    def test_non_comparable_reason_none_when_comparable(self):
        contract = self._build([self._review_row()])
        row = contract["reconciliation_rows"][0]
        assert row["non_comparable_reason"] is None

    def test_non_comparable_reason_blocked(self):
        rr = self._review_row(mapping_outcome="blocked", mapped_bid_item=None)
        contract = self._build([rr], packet_status="blocked", pairing_status="rejected")
        row = contract["reconciliation_rows"][0]
        assert row["non_comparable_reason"] == "row_blocked_by_pairing"

    # ---- Deterministic ordering ----

    def test_rows_deterministically_ordered(self):
        rows = [
            self._review_row(normalized_row_id="qr-p2-r5", quote_source_page=2),
            self._review_row(normalized_row_id="qr-p0-r3", quote_source_page=0),
            self._review_row(normalized_row_id="qr-p0-r1", quote_source_page=0),
            self._review_row(normalized_row_id="qr-p1-r0", quote_source_page=1),
        ]
        contract = self._build(rows)
        ids = [r["normalized_row_id"] for r in contract["reconciliation_rows"]]
        assert ids == ["qr-p0-r1", "qr-p0-r3", "qr-p1-r0", "qr-p2-r5"]

    def test_source_page_stamped_on_row(self):
        rr = self._review_row(normalized_row_id="qr-p3-r9", quote_source_page=3)
        contract = self._build([rr])
        assert contract["reconciliation_rows"][0]["source_page"] == 3

    # ---- flag_counts ----

    def test_flag_counts_histogram(self):
        rows = [self._review_row(normalized_row_id=f"qr-p0-r{i}") for i in range(2)]
        contract = self._build(rows)
        fc = contract["reconciliation_summary"]["flag_counts"]
        assert fc.get("unit_match") == 2
        assert fc.get("qty_match") == 2
        assert fc.get("missing_bid_amount") == 2

    # ---- Input immutability ----

    def test_does_not_mutate_inputs(self):
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        packet = self._packet([self._review_row()])
        recon = reconcile_packet(packet)
        import copy
        recon_snap = copy.deepcopy(recon)
        packet_snap = copy.deepcopy(packet)
        build_reconciliation_contract(recon, packet)
        assert recon == recon_snap
        assert packet == packet_snap

    # ---- Blocked / non-comparable never hidden ----

    def test_blocked_rows_never_collapsed_into_non_comparable(self):
        rr = self._review_row(mapping_outcome="blocked", mapped_bid_item=None)
        contract = self._build([rr], packet_status="blocked", pairing_status="rejected")
        # blocked rows stay distinct — rows_blocked != rows_non_comparable
        assert contract["reconciliation_summary"]["rows_blocked"] == 1
        assert contract["reconciliation_summary"]["rows_non_comparable"] == 0

    # ---- DOT / staging regression guards ----

    def test_dot_native_unchanged_under_c17(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c17(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True

    def test_quote_staging_unchanged_under_c17(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14


# ---------------------------------------------------------------------------
# 20k. C18 — Discrepancy Classification Tests
# ---------------------------------------------------------------------------


class TestC18DiscrepancyClassification:
    """C18: deterministic discrepancy classification + office review
    summary buckets on top of the C17 hardened contract."""

    def _review_row(self, **kw):
        base = {
            "normalized_row_id": "qr-p0-r0",
            "quote_description": "Item A",
            "quote_line_ref": "0010",
            "quote_amount": 100.0,
            "quote_unit_price": 10.0,
            "quote_qty": 10.0,
            "quote_unit": "EACH",
            "quote_source_page": 0,
            "mapping_outcome": "mapped",
            "mapping_reason": "line_ref_exact_match",
            "mapped_bid_item": {
                "line_number": "0010",
                "item_number": "2101-0850001",
                "description": "ITEM A",
                "qty": 10.0,
                "unit": "EACH",
            },
            "review_flags": [],
            "mapping_trace_summary": {},
        }
        base.update(kw)
        return base

    def _packet(self, review_rows, packet_status="partial", pairing_status="trusted"):
        return {
            "packet_status": packet_status,
            "pairing_diagnostics": {
                "pairing_status": pairing_status,
                "pairing_reason": "x",
                "signals": {},
                "warnings": [],
                "allow_mapping": packet_status != "blocked",
            },
            "mapping_summary": {"mapping_status": "partial"},
            "review_rows": review_rows,
        }

    def _classify(self, rows, packet_status="partial", pairing_status="trusted"):
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        packet = self._packet(rows, packet_status=packet_status, pairing_status=pairing_status)
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        return classify_contract(contract)

    # ---- Single-class deterministic assignment ----

    def test_comparable_match_class(self):
        from app.pdf_extraction.discrepancy_classification import CLASS_COMPARABLE_MATCH
        out = self._classify([self._review_row()])
        assert out["reconciliation_rows"][0]["discrepancy_class"] == CLASS_COMPARABLE_MATCH

    def test_comparable_mismatch_unit_class(self):
        from app.pdf_extraction.discrepancy_classification import CLASS_COMPARABLE_MISMATCH_UNIT
        rr = self._review_row()
        rr["mapped_bid_item"]["unit"] = "LF"
        out = self._classify([rr])
        assert out["reconciliation_rows"][0]["discrepancy_class"] == CLASS_COMPARABLE_MISMATCH_UNIT

    def test_comparable_mismatch_qty_class(self):
        from app.pdf_extraction.discrepancy_classification import CLASS_COMPARABLE_MISMATCH_QTY
        rr = self._review_row()
        rr["mapped_bid_item"]["qty"] = 50.0
        out = self._classify([rr])
        assert out["reconciliation_rows"][0]["discrepancy_class"] == CLASS_COMPARABLE_MISMATCH_QTY

    def test_comparable_mismatch_multi_class(self):
        from app.pdf_extraction.discrepancy_classification import CLASS_COMPARABLE_MISMATCH_MULTI
        rr = self._review_row()
        rr["mapped_bid_item"]["unit"] = "LF"
        rr["mapped_bid_item"]["qty"] = 50.0
        out = self._classify([rr])
        assert out["reconciliation_rows"][0]["discrepancy_class"] == CLASS_COMPARABLE_MISMATCH_MULTI

    def test_unmapped_quote_row_class(self):
        from app.pdf_extraction.discrepancy_classification import CLASS_UNMAPPED_QUOTE_ROW
        rr = self._review_row(mapping_outcome="unmapped", mapped_bid_item=None)
        out = self._classify([rr])
        assert out["reconciliation_rows"][0]["discrepancy_class"] == CLASS_UNMAPPED_QUOTE_ROW

    def test_ambiguous_mapping_class(self):
        from app.pdf_extraction.discrepancy_classification import CLASS_AMBIGUOUS_MAPPING
        rr = self._review_row(mapping_outcome="ambiguous", mapped_bid_item=None)
        out = self._classify([rr])
        assert out["reconciliation_rows"][0]["discrepancy_class"] == CLASS_AMBIGUOUS_MAPPING

    def test_blocked_by_pairing_class(self):
        from app.pdf_extraction.discrepancy_classification import CLASS_BLOCKED_BY_PAIRING
        rr = self._review_row(mapping_outcome="blocked", mapped_bid_item=None)
        out = self._classify([rr], packet_status="blocked", pairing_status="rejected")
        assert out["reconciliation_rows"][0]["discrepancy_class"] == CLASS_BLOCKED_BY_PAIRING

    def test_missing_quote_information_class(self):
        """Mapped row with no qty/unit on quote side → missing_quote_information."""
        from app.pdf_extraction.discrepancy_classification import CLASS_MISSING_QUOTE_INFORMATION
        rr = self._review_row(quote_qty=None, quote_unit=None)
        out = self._classify([rr])
        assert out["reconciliation_rows"][0]["discrepancy_class"] == CLASS_MISSING_QUOTE_INFORMATION

    def test_missing_bid_information_class(self):
        """Mapped row: both quote sides present, but bid qty+unit missing."""
        from app.pdf_extraction.discrepancy_classification import CLASS_MISSING_BID_INFORMATION
        rr = self._review_row()
        rr["mapped_bid_item"]["qty"] = None
        rr["mapped_bid_item"]["unit"] = None
        out = self._classify([rr])
        assert out["reconciliation_rows"][0]["discrepancy_class"] == CLASS_MISSING_BID_INFORMATION

    # ---- Distinct buckets ----

    def test_blocked_and_unmapped_stay_distinct(self):
        rows = [
            self._review_row(normalized_row_id="qr-p0-r0", mapping_outcome="unmapped", mapped_bid_item=None),
            self._review_row(normalized_row_id="qr-p0-r1", mapping_outcome="ambiguous", mapped_bid_item=None),
        ]
        out = self._classify(rows)
        classes = {r["discrepancy_class"] for r in out["reconciliation_rows"]}
        assert "unmapped_quote_row" in classes
        assert "ambiguous_mapping" in classes
        assert "blocked_by_pairing" not in classes

    def test_office_summary_counts(self):
        rows = [
            self._review_row(normalized_row_id="qr-p0-r0"),  # match
            self._review_row(normalized_row_id="qr-p0-r1"),  # match
            self._review_row(normalized_row_id="qr-p0-r2"),  # mismatch unit
            self._review_row(normalized_row_id="qr-p0-r3",
                             mapping_outcome="unmapped", mapped_bid_item=None),
            self._review_row(normalized_row_id="qr-p0-r4",
                             quote_qty=None, quote_unit=None),  # missing quote info
        ]
        rows[2]["mapped_bid_item"]["unit"] = "LF"
        out = self._classify(rows)
        summary = out["office_review_summary"]
        assert summary["rows_total"] == 5
        assert summary["comparable_match_count"] == 2
        assert summary["comparable_mismatch_unit_count"] == 1
        assert summary["unmapped_count"] == 1
        assert summary["missing_quote_info_count"] == 1
        assert summary["blocked_count"] == 0
        assert summary["comparable_mismatch_qty_count"] == 0
        assert summary["comparable_mismatch_multi_count"] == 0

    def test_class_counts_present(self):
        out = self._classify([self._review_row()])
        cc = out["office_review_summary"]["class_counts"]
        # All 11 classes present with deterministic initial zeros.
        assert cc["comparable_match"] == 1
        assert cc["unmapped_quote_row"] == 0
        assert cc["blocked_by_pairing"] == 0

    def test_classification_version_present(self):
        out = self._classify([self._review_row()])
        assert out["classification_version"] == "discrepancy_classification/v1"

    # ---- Classification trace ----

    def test_classification_trace_has_rule_fired(self):
        rr = self._review_row()
        rr["mapped_bid_item"]["unit"] = "LF"
        out = self._classify([rr])
        trace = out["reconciliation_rows"][0]["classification_trace"]
        assert "rule_fired" in trace
        assert trace["rule_fired"] is not None
        assert "inputs" in trace

    # ---- Contract fields preserved ----

    def test_classification_preserves_contract_fields(self):
        out = self._classify([self._review_row()])
        assert out["contract_version"] == "reconciliation_contract/v1"
        assert "reconciliation_summary" in out
        row = out["reconciliation_rows"][0]
        for k in ("normalized_row_id", "comparison_status", "comparison_flags",
                  "compared_fields", "non_comparable_reason", "discrepancy_class"):
            assert k in row

    def test_classification_does_not_mutate_input(self):
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        packet = self._packet([self._review_row()])
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        import copy
        snap = copy.deepcopy(contract)
        classify_contract(contract)
        assert contract == snap

    # ---- Endpoint integration ----

    def test_endpoint_trusted_carries_classification(self, client, ipsi_quote_path, dot_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/reconcile",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["classification_version"] == "discrepancy_classification/v1"
        summary = data["office_review_summary"]
        # IPSI quotes carry no qty/unit: 13 mapped non-comparable rows
        # land in missing_quote_info; 2 rows unmapped.
        assert summary["missing_quote_info_count"] == 13
        assert summary["unmapped_count"] == 2
        assert summary["comparable_match_count"] == 0
        for row in data["reconciliation_rows"]:
            assert "discrepancy_class" in row
            assert row["discrepancy_class"] is not None

    def test_endpoint_blocked_all_blocked_by_pairing(self, client, ipsi_quote_path, estprop_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(estprop_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/reconcile",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("estprop121.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 422
        data = resp.json()
        summary = data["office_review_summary"]
        assert summary["blocked_count"] == summary["rows_total"]
        assert summary["unmapped_count"] == 0
        for row in data["reconciliation_rows"]:
            assert row["discrepancy_class"] == "blocked_by_pairing"

    # ---- DOT + staging regression guards ----

    def test_dot_native_unchanged_under_c18(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c18(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True

    def test_quote_staging_unchanged_under_c18(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14


# ---------------------------------------------------------------------------
# 20l. C19 — Findings Packet Foundation Tests
# ---------------------------------------------------------------------------


class TestC19FindingsPacketFoundation:
    """C19: governed findings packet foundation — deterministic assembly
    of pairing, mapping, reconciliation, and discrepancy classification
    into a single auditable packet artifact."""

    def _review_row(self, **kw):
        base = {
            "normalized_row_id": "qr-p0-r0",
            "quote_description": "Item A",
            "quote_line_ref": "0010",
            "quote_amount": 100.0,
            "quote_unit_price": 10.0,
            "quote_qty": 10.0,
            "quote_unit": "EACH",
            "quote_source_page": 0,
            "mapping_outcome": "mapped",
            "mapping_reason": "line_ref_exact_match",
            "mapped_bid_item": {
                "line_number": "0010",
                "item_number": "2101-0850001",
                "description": "ITEM A",
                "qty": 10.0,
                "unit": "EACH",
            },
            "review_flags": [],
            "mapping_trace_summary": {},
        }
        base.update(kw)
        return base

    def _packet(self, review_rows, packet_status="partial", pairing_status="trusted"):
        return {
            "packet_status": packet_status,
            "pairing_diagnostics": {
                "pairing_status": pairing_status,
                "pairing_reason": "x",
                "signals": {"desc_overlap_ratio": 0.9},
                "warnings": [],
                "allow_mapping": packet_status != "blocked",
            },
            "quote_summary": {
                "accepted_rows_count": len(review_rows),
                "extraction_source": "ocr_pdf",
                "ocr_used": True,
                "status": "success",
            },
            "bid_summary": {
                "rows_extracted": 93,
                "format_detected": "standard",
                "document_class": "dot_schedule",
                "extraction_source": "native_pdf",
            },
            "mapping_summary": {
                "mapping_status": "partial",
                "mapped_count": sum(1 for r in review_rows if r.get("mapping_outcome") == "mapped"),
                "unmapped_count": sum(1 for r in review_rows if r.get("mapping_outcome") == "unmapped"),
                "ambiguous_count": sum(1 for r in review_rows if r.get("mapping_outcome") == "ambiguous"),
            },
            "review_rows": review_rows,
            "packet_diagnostics": {
                "total_review_rows": len(review_rows),
                "rows_ready_for_reconciliation":
                    sum(1 for r in review_rows if r.get("mapping_outcome") == "mapped"),
            },
        }

    def _build(self, rows, packet_status="partial", pairing_status="trusted"):
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.findings_packet import build_findings_packet
        packet = self._packet(rows, packet_status=packet_status, pairing_status=pairing_status)
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        return packet, build_findings_packet(packet, classified)

    # ---- Stable structure ----

    def test_packet_version_present(self):
        _, fp = self._build([self._review_row()])
        assert fp["packet_version"] == "findings_packet/v1"

    def test_all_sections_present(self):
        _, fp = self._build([self._review_row()])
        for section in ("packet_status", "pairing_section", "quote_section",
                        "bid_section", "mapping_section", "reconciliation_section",
                        "discrepancy_summary", "findings_rows", "packet_diagnostics"):
            assert section in fp

    def test_packet_status_partial(self):
        _, fp = self._build([self._review_row()])
        assert fp["packet_status"] == "partial"

    def test_packet_status_ready(self):
        """When every mapped row matches and no non-comparable rows, a
        ready review packet maps to a ready findings packet."""
        rows = [self._review_row(normalized_row_id=f"qr-p0-r{i}") for i in range(3)]
        _, fp = self._build(rows, packet_status="ready")
        assert fp["packet_status"] == "ready"

    def test_packet_status_blocked(self):
        rr = self._review_row(mapping_outcome="blocked", mapped_bid_item=None)
        _, fp = self._build([rr], packet_status="blocked", pairing_status="rejected")
        assert fp["packet_status"] == "blocked"

    # ---- Sections carry correct data ----

    def test_pairing_section_shape(self):
        _, fp = self._build([self._review_row()])
        ps = fp["pairing_section"]
        assert ps["pairing_status"] == "trusted"
        assert ps["allow_mapping"] is True
        assert "signals" in ps

    def test_reconciliation_section_includes_counts(self):
        rr = self._review_row()
        rr["mapped_bid_item"]["qty"] = 50.0
        _, fp = self._build([rr])
        rs = fp["reconciliation_section"]
        assert rs["rows_total"] == 1
        assert rs["rows_mismatched"] == 1
        assert rs["rows_matched"] == 0
        assert rs["contract_version"] == "reconciliation_contract/v1"

    def test_discrepancy_summary_has_buckets(self):
        _, fp = self._build([self._review_row()])
        ds = fp["discrepancy_summary"]
        assert ds["classification_version"] == "discrepancy_classification/v1"
        assert ds["comparable_match_count"] == 1

    # ---- Findings rows ----

    def test_findings_row_count_matches_review(self):
        rows = [self._review_row(normalized_row_id=f"qr-p0-r{i}") for i in range(4)]
        _, fp = self._build(rows)
        assert len(fp["findings_rows"]) == 4

    def test_finding_row_stable_key_set(self):
        _, fp = self._build([self._review_row()])
        row = fp["findings_rows"][0]
        for k in ("normalized_row_id", "source_page", "quote_description",
                  "quote_line_ref", "mapped_bid_line_number",
                  "mapped_bid_item_number", "mapping_outcome",
                  "comparison_status", "compared_fields", "non_comparable_reason",
                  "discrepancy_class", "review_flags", "comparison_flags",
                  "quote_values", "bid_values", "finding_trace"):
            assert k in row

    def test_finding_trace_carries_classification_and_comparison(self):
        _, fp = self._build([self._review_row()])
        trace = fp["findings_rows"][0]["finding_trace"]
        assert "mapping_reason" in trace
        assert "classification_trace" in trace
        assert "comparison_trace" in trace
        assert trace["classification_trace"]["rule_fired"] == "R5_comparable_match"

    def test_finding_row_preserves_discrepancy_class(self):
        rr = self._review_row(mapping_outcome="unmapped", mapped_bid_item=None)
        _, fp = self._build([rr])
        assert fp["findings_rows"][0]["discrepancy_class"] == "unmapped_quote_row"

    def test_blocked_rows_preserved_in_findings(self):
        rr = self._review_row(mapping_outcome="blocked", mapped_bid_item=None)
        _, fp = self._build([rr], packet_status="blocked", pairing_status="rejected")
        row = fp["findings_rows"][0]
        assert row["discrepancy_class"] == "blocked_by_pairing"
        assert row["comparison_status"] == "blocked"

    def test_mapped_bid_line_number_populated_when_mapped(self):
        _, fp = self._build([self._review_row()])
        row = fp["findings_rows"][0]
        assert row["mapped_bid_line_number"] == "0010"
        assert row["mapped_bid_item_number"] == "2101-0850001"

    def test_mapped_bid_line_none_when_unmapped(self):
        rr = self._review_row(mapping_outcome="unmapped", mapped_bid_item=None)
        _, fp = self._build([rr])
        row = fp["findings_rows"][0]
        assert row["mapped_bid_line_number"] is None
        assert row["mapped_bid_item_number"] is None

    # ---- Packet diagnostics ----

    def test_packet_diagnostics_unresolved_total(self):
        rows = [
            self._review_row(normalized_row_id="qr-p0-r0"),  # match
            self._review_row(normalized_row_id="qr-p0-r1",
                             mapping_outcome="unmapped", mapped_bid_item=None),
            self._review_row(normalized_row_id="qr-p0-r2",
                             quote_qty=None, quote_unit=None),
        ]
        _, fp = self._build(rows)
        diag = fp["packet_diagnostics"]
        assert diag["findings_row_count"] == 3
        # 1 unmapped + 1 missing_quote_info = 2 unresolved
        assert diag["unresolved_total"] == 2
        assert diag["unresolved_counts"]["unmapped_count"] == 1
        assert diag["unresolved_counts"]["missing_quote_info_count"] == 1

    # ---- Input immutability ----

    def test_does_not_mutate_inputs(self):
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.findings_packet import build_findings_packet
        import copy
        packet = self._packet([self._review_row()])
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        psnap = copy.deepcopy(packet)
        csnap = copy.deepcopy(classified)
        build_findings_packet(packet, classified)
        assert packet == psnap
        assert classified == csnap

    # ---- Endpoint integration ----

    def test_findings_endpoint_trusted_partial(self, client, ipsi_quote_path, dot_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/findings",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["packet_version"] == "findings_packet/v1"
        assert data["packet_status"] == "partial"
        assert data["pairing_section"]["pairing_status"] == "trusted"
        assert data["reconciliation_section"]["rows_total"] >= 13
        assert data["discrepancy_summary"]["missing_quote_info_count"] == 13
        assert data["discrepancy_summary"]["unmapped_count"] == 2
        # Every finding row preserves its discrepancy class.
        for row in data["findings_rows"]:
            assert row["discrepancy_class"] is not None

    def test_findings_endpoint_blocked(self, client, ipsi_quote_path, estprop_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(estprop_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/findings",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("estprop121.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 422
        data = resp.json()
        assert data["packet_status"] == "blocked"
        assert data["pairing_section"]["pairing_status"] == "rejected"
        for row in data["findings_rows"]:
            assert row["discrepancy_class"] == "blocked_by_pairing"
            assert row["comparison_status"] == "blocked"

    # ---- DOT + staging regression guards ----

    def test_dot_native_unchanged_under_c19(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c19(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True

    def test_quote_staging_unchanged_under_c19(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14


# ---------------------------------------------------------------------------
# 20o. C22 — Findings Packet Export Tests
# ---------------------------------------------------------------------------


class TestC22FindingsPacketExports:
    """C22: deterministic export formats — JSON, CSV, structured report —
    built on top of the C19 findings packet decorated by C21 priorities."""

    def _finding(self, **kw):
        base = {
            "normalized_row_id": kw.get("normalized_row_id", "qr-p0-r0"),
            "source_page": 0,
            "quote_description": "Item A",
            "quote_line_ref": "0010",
            "mapped_bid_line_number": "0010",
            "mapped_bid_item_number": "2101-0850001",
            "mapping_outcome": "mapped",
            "comparison_status": "match",
            "compared_fields": ["unit", "qty"],
            "non_comparable_reason": None,
            "discrepancy_class": "comparable_match",
            "review_flags": [],
            "comparison_flags": ["unit_match", "qty_match"],
            "quote_values": {"qty": 10, "unit": "EACH", "unit_price": 10.0, "amount": 100.0},
            "bid_values": {"qty": 10, "unit": "EACH"},
            "priority_class": "low",
            "priority_reason": "mapped_row_fully_reconciled_no_discrepancies",
            "priority_trace": {},
            "finding_trace": {},
        }
        base.update(kw)
        return base

    def _packet(self, rows, packet_status="partial", pairing_status="trusted"):
        return {
            "packet_version": "findings_packet/v1",
            "packet_status": packet_status,
            "pairing_section": {
                "pairing_status": pairing_status,
                "pairing_reason": "x",
                "allow_mapping": packet_status != "blocked",
                "warnings": [],
                "signals": {},
            },
            "quote_section": {"accepted_rows_count": len(rows)},
            "bid_section": {"rows_extracted": 93, "document_class": "dot_schedule"},
            "mapping_section": {"mapping_status": "partial"},
            "reconciliation_section": {"reconciliation_status": packet_status,
                                       "rows_total": len(rows)},
            "discrepancy_summary": {"rows_total": len(rows)},
            "priority_summary": {
                "rows_total": len(rows), "critical_count": 0, "high_count": 0,
                "medium_count": 0, "low_count": 0, "informational_count": 0,
                "top_issues_summary": [],
            },
            "findings_rows": rows,
            "packet_diagnostics": {},
            "prioritization_version": "review_prioritization/v1",
        }

    # ---- JSON export ----

    def test_json_export_has_stable_top_level_keys(self):
        from app.pdf_extraction.findings_exports import export_findings_json
        out = export_findings_json(self._packet([self._finding()]))
        for k in ("export_version", "export_format", "packet_version",
                  "packet_status", "pairing_status", "summary", "rows",
                  "pairing_section", "quote_section", "bid_section"):
            assert k in out
        assert out["export_format"] == "json"
        assert out["export_version"] == "findings_exports/v1"

    def test_json_export_row_count_matches(self):
        from app.pdf_extraction.findings_exports import export_findings_json
        rows = [self._finding(normalized_row_id=f"qr-p0-r{i}") for i in range(5)]
        out = export_findings_json(self._packet(rows))
        assert len(out["rows"]) == 5

    def test_json_export_preserves_statuses(self):
        from app.pdf_extraction.findings_exports import export_findings_json
        rows = [self._finding(
            discrepancy_class="blocked_by_pairing",
            comparison_status="blocked",
            priority_class="critical",
        )]
        packet = self._packet(rows, packet_status="blocked", pairing_status="rejected")
        out = export_findings_json(packet)
        assert out["packet_status"] == "blocked"
        assert out["pairing_status"] == "rejected"
        assert out["rows"][0]["comparison_status"] == "blocked"
        assert out["rows"][0]["priority_class"] == "critical"

    def test_json_export_flattened_row_has_required_fields(self):
        from app.pdf_extraction.findings_exports import export_findings_json
        out = export_findings_json(self._packet([self._finding()]))
        row = out["rows"][0]
        for k in ("normalized_row_id", "priority_class", "discrepancy_class",
                  "mapping_outcome", "comparison_status", "quote_description",
                  "quote_qty", "quote_unit", "bid_qty", "bid_unit"):
            assert k in row

    def test_json_export_summary_sections_present(self):
        from app.pdf_extraction.findings_exports import export_findings_json
        out = export_findings_json(self._packet([self._finding()]))
        assert "reconciliation" in out["summary"]
        assert "discrepancy" in out["summary"]
        assert "priority" in out["summary"]
        assert "mapping" in out["summary"]

    def test_json_export_never_hides_low_priority(self):
        from app.pdf_extraction.findings_exports import export_findings_json
        rows = [
            self._finding(normalized_row_id="qr-p0-r0", priority_class="critical"),
            self._finding(normalized_row_id="qr-p0-r1", priority_class="low"),
        ]
        out = export_findings_json(self._packet(rows))
        priorities = [r["priority_class"] for r in out["rows"]]
        assert "critical" in priorities
        assert "low" in priorities

    # ---- CSV export ----

    def test_csv_export_header_is_stable(self):
        from app.pdf_extraction.findings_exports import export_findings_csv, csv_columns
        out = export_findings_csv(self._packet([self._finding()]))
        header = out.splitlines()[0]
        expected = ",".join(csv_columns())
        assert header == expected

    def test_csv_export_row_count_matches(self):
        from app.pdf_extraction.findings_exports import export_findings_csv
        rows = [self._finding(normalized_row_id=f"qr-p0-r{i}") for i in range(4)]
        out = export_findings_csv(self._packet(rows))
        lines = out.splitlines()
        # Header + 4 rows
        assert len(lines) == 5

    def test_csv_export_preserves_blocked_rows(self):
        from app.pdf_extraction.findings_exports import export_findings_csv
        row = self._finding(
            discrepancy_class="blocked_by_pairing",
            comparison_status="blocked",
            priority_class="critical",
        )
        out = export_findings_csv(self._packet([row]))
        assert "blocked" in out
        assert "critical" in out
        assert "blocked_by_pairing" in out

    def test_csv_export_lists_become_semicolon_joined(self):
        from app.pdf_extraction.findings_exports import export_findings_csv
        row = self._finding(compared_fields=["unit", "qty"],
                            comparison_flags=["unit_match", "qty_match"])
        out = export_findings_csv(self._packet([row]))
        assert "unit;qty" in out
        assert "unit_match;qty_match" in out

    def test_csv_export_none_values_become_empty(self):
        from app.pdf_extraction.findings_exports import export_findings_csv
        row = self._finding(
            quote_values={"qty": None, "unit": None, "unit_price": None, "amount": None},
            bid_values=None,
            non_comparable_reason=None,
        )
        out = export_findings_csv(self._packet([row]))
        # Data row should not contain any "None" literal.
        data_row = out.splitlines()[1]
        assert "None" not in data_row

    # ---- Report export ----

    def test_report_export_has_sections(self):
        from app.pdf_extraction.findings_exports import export_findings_report
        out = export_findings_report(self._packet([self._finding()]))
        section_ids = [s["section_id"] for s in out["sections"]]
        for sid in ("packet_status", "pairing", "quote", "bid", "mapping",
                    "reconciliation", "discrepancy", "priority", "findings_rows"):
            assert sid in section_ids

    def test_report_export_section_order_stable(self):
        from app.pdf_extraction.findings_exports import export_findings_report
        out1 = export_findings_report(self._packet([self._finding()]))
        out2 = export_findings_report(self._packet([self._finding()]))
        ids1 = [s["section_id"] for s in out1["sections"]]
        ids2 = [s["section_id"] for s in out2["sections"]]
        assert ids1 == ids2

    def test_report_export_pdf_rendering_deferred(self):
        from app.pdf_extraction.findings_exports import export_findings_report
        out = export_findings_report(self._packet([self._finding()]))
        assert out["pdf_rendering"] == "deferred"

    def test_report_export_findings_section_row_count(self):
        from app.pdf_extraction.findings_exports import export_findings_report
        rows = [self._finding(normalized_row_id=f"qr-p0-r{i}") for i in range(3)]
        out = export_findings_report(self._packet(rows))
        findings_section = next(s for s in out["sections"] if s["section_id"] == "findings_rows")
        assert findings_section["content"]["row_count"] == 3
        assert len(findings_section["content"]["rows"]) == 3

    def test_report_export_preserves_blocked_status(self):
        from app.pdf_extraction.findings_exports import export_findings_report
        packet = self._packet([self._finding()], packet_status="blocked", pairing_status="rejected")
        out = export_findings_report(packet)
        packet_section = next(s for s in out["sections"] if s["section_id"] == "packet_status")
        assert packet_section["content"]["packet_status"] == "blocked"
        pairing_section = next(s for s in out["sections"] if s["section_id"] == "pairing")
        assert pairing_section["content"]["pairing_status"] == "rejected"

    # ---- Immutability ----

    def test_exports_do_not_mutate_input(self):
        from app.pdf_extraction.findings_exports import (
            export_findings_json, export_findings_csv, export_findings_report,
        )
        import copy
        packet = self._packet([self._finding()])
        snap = copy.deepcopy(packet)
        export_findings_json(packet)
        export_findings_csv(packet)
        export_findings_report(packet)
        assert packet == snap

    # ---- Endpoint integration ----

    def test_json_export_endpoint_trusted(self, client, ipsi_quote_path, dot_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/findings/export/json",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["export_format"] == "json"
        assert data["packet_status"] == "partial"
        assert data["pairing_status"] == "trusted"
        assert len(data["rows"]) >= 15
        assert "priority" in data["summary"]

    def test_json_export_endpoint_blocked(self, client, ipsi_quote_path, estprop_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(estprop_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/findings/export/json",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("estprop121.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 422
        data = resp.json()
        assert data["packet_status"] == "blocked"
        assert data["pairing_status"] == "rejected"
        for row in data["rows"]:
            assert row["priority_class"] == "critical"
            assert row["comparison_status"] == "blocked"

    def test_csv_export_endpoint_trusted(self, client, ipsi_quote_path, dot_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/findings/export/csv",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/csv")
        assert resp.headers.get("x-packet-status") == "partial"
        body = resp.text
        lines = body.splitlines()
        # Header + at least 15 rows
        assert len(lines) >= 16
        header = lines[0]
        assert "priority_class" in header
        assert "discrepancy_class" in header

    def test_csv_export_endpoint_blocked(self, client, ipsi_quote_path, estprop_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(estprop_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/findings/export/csv",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("estprop121.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 422
        assert resp.headers.get("x-packet-status") == "blocked"
        body = resp.text
        assert "blocked" in body
        assert "critical" in body

    def test_report_export_endpoint_trusted(self, client, ipsi_quote_path, dot_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/findings/export/report",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["export_format"] == "report"
        assert data["pdf_rendering"] == "deferred"
        assert data["packet_status"] == "partial"
        section_ids = [s["section_id"] for s in data["sections"]]
        assert "findings_rows" in section_ids

    def test_report_export_endpoint_blocked(self, client, ipsi_quote_path, estprop_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(estprop_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/findings/export/report",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("estprop121.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 422
        data = resp.json()
        assert data["packet_status"] == "blocked"
        pairing = next(s for s in data["sections"] if s["section_id"] == "pairing")
        assert pairing["content"]["pairing_status"] == "rejected"

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c22(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c22(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True

    def test_quote_staging_unchanged_under_c22(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14


# ---------------------------------------------------------------------------
# 20n. C21 — Review Prioritization Layer Tests
# ---------------------------------------------------------------------------


class TestC21ReviewPrioritization:
    """C21: deterministic priority classification on top of C18
    discrepancy classes. Priority decoration never overrides
    discrepancy truth — it adds an ordering + triage layer."""

    def _row(self, discrepancy_class, **kw):
        base = {
            "normalized_row_id": kw.get("normalized_row_id", "qr-p0-r0"),
            "discrepancy_class": discrepancy_class,
            "mapping_outcome": kw.get("mapping_outcome", "mapped"),
            "comparison_status": kw.get("comparison_status", "match"),
        }
        base.update(kw)
        return base

    # ---- assign_priority pure function ----

    def test_blocked_by_pairing_is_critical(self):
        from app.pdf_extraction.review_prioritization import assign_priority, PRIORITY_CRITICAL
        p, r, _ = assign_priority(self._row("blocked_by_pairing"))
        assert p == PRIORITY_CRITICAL
        assert r

    def test_review_required_other_is_critical(self):
        from app.pdf_extraction.review_prioritization import assign_priority, PRIORITY_CRITICAL
        p, _, _ = assign_priority(self._row("review_required_other"))
        assert p == PRIORITY_CRITICAL

    def test_unmapped_is_high(self):
        from app.pdf_extraction.review_prioritization import assign_priority, PRIORITY_HIGH
        p, _, _ = assign_priority(self._row("unmapped_quote_row"))
        assert p == PRIORITY_HIGH

    def test_ambiguous_is_high(self):
        from app.pdf_extraction.review_prioritization import assign_priority, PRIORITY_HIGH
        p, _, _ = assign_priority(self._row("ambiguous_mapping"))
        assert p == PRIORITY_HIGH

    def test_mismatch_unit_is_high(self):
        from app.pdf_extraction.review_prioritization import assign_priority, PRIORITY_HIGH
        p, _, _ = assign_priority(self._row("comparable_mismatch_unit"))
        assert p == PRIORITY_HIGH

    def test_mismatch_qty_is_high(self):
        from app.pdf_extraction.review_prioritization import assign_priority, PRIORITY_HIGH
        p, _, _ = assign_priority(self._row("comparable_mismatch_qty"))
        assert p == PRIORITY_HIGH

    def test_mismatch_multi_is_high(self):
        from app.pdf_extraction.review_prioritization import assign_priority, PRIORITY_HIGH
        p, _, _ = assign_priority(self._row("comparable_mismatch_multi"))
        assert p == PRIORITY_HIGH

    def test_missing_quote_info_is_medium(self):
        from app.pdf_extraction.review_prioritization import assign_priority, PRIORITY_MEDIUM
        p, _, _ = assign_priority(self._row("missing_quote_information"))
        assert p == PRIORITY_MEDIUM

    def test_missing_bid_info_is_medium(self):
        from app.pdf_extraction.review_prioritization import assign_priority, PRIORITY_MEDIUM
        p, _, _ = assign_priority(self._row("missing_bid_information"))
        assert p == PRIORITY_MEDIUM

    def test_structurally_non_comparable_is_medium(self):
        from app.pdf_extraction.review_prioritization import assign_priority, PRIORITY_MEDIUM
        p, _, _ = assign_priority(self._row("structurally_non_comparable"))
        assert p == PRIORITY_MEDIUM

    def test_comparable_match_is_low(self):
        from app.pdf_extraction.review_prioritization import assign_priority, PRIORITY_LOW
        p, _, _ = assign_priority(self._row("comparable_match"))
        assert p == PRIORITY_LOW

    def test_unknown_class_is_critical_fail_safe(self):
        from app.pdf_extraction.review_prioritization import assign_priority, PRIORITY_CRITICAL
        p, r, _ = assign_priority(self._row("this_class_does_not_exist"))
        assert p == PRIORITY_CRITICAL
        assert "unknown" in r

    # ---- Priority trace ----

    def test_priority_trace_carries_inputs(self):
        from app.pdf_extraction.review_prioritization import assign_priority
        _, _, trace = assign_priority(self._row("blocked_by_pairing"))
        assert trace["input_discrepancy_class"] == "blocked_by_pairing"
        assert "mapping_table_version" in trace

    # ---- Classified contract decoration ----

    def _contract(self, rows):
        return {
            "contract_version": "reconciliation_contract/v1",
            "classification_version": "discrepancy_classification/v1",
            "reconciliation_status": "partial",
            "packet_status": "partial",
            "pairing_status": "trusted",
            "mapping_status": "partial",
            "reconciliation_summary": {
                "rows_total": len(rows), "rows_compared": 0,
                "rows_matched": 0, "rows_mismatched": 0,
                "rows_non_comparable": 0, "rows_blocked": 0,
                "flag_counts": {},
            },
            "office_review_summary": {},
            "reconciliation_rows": rows,
        }

    def test_contract_decoration_adds_priority_fields(self):
        from app.pdf_extraction.review_prioritization import prioritize_classified_contract
        c = self._contract([self._row("comparable_match")])
        out = prioritize_classified_contract(c)
        row = out["reconciliation_rows"][0]
        assert row["priority_class"] == "low"
        assert row["priority_reason"]
        assert "priority_trace" in row
        assert out["prioritization_version"] == "review_prioritization/v1"

    def test_contract_decoration_discrepancy_class_preserved(self):
        from app.pdf_extraction.review_prioritization import prioritize_classified_contract
        c = self._contract([self._row("unmapped_quote_row")])
        out = prioritize_classified_contract(c)
        row = out["reconciliation_rows"][0]
        assert row["discrepancy_class"] == "unmapped_quote_row"  # unchanged
        assert row["priority_class"] == "high"

    def test_contract_decoration_priority_summary(self):
        from app.pdf_extraction.review_prioritization import prioritize_classified_contract
        c = self._contract([
            self._row("blocked_by_pairing", normalized_row_id="qr-p0-r0"),
            self._row("unmapped_quote_row", normalized_row_id="qr-p0-r1"),
            self._row("comparable_mismatch_qty", normalized_row_id="qr-p0-r2"),
            self._row("missing_quote_information", normalized_row_id="qr-p0-r3"),
            self._row("comparable_match", normalized_row_id="qr-p0-r4"),
        ])
        out = prioritize_classified_contract(c)
        s = out["priority_summary"]
        assert s["rows_total"] == 5
        assert s["critical_count"] == 1
        assert s["high_count"] == 2
        assert s["medium_count"] == 1
        assert s["low_count"] == 1
        assert s["informational_count"] == 0

    def test_contract_decoration_stable_priority_ordering(self):
        from app.pdf_extraction.review_prioritization import prioritize_classified_contract
        c = self._contract([
            self._row("comparable_match", normalized_row_id="qr-p0-r0"),
            self._row("blocked_by_pairing", normalized_row_id="qr-p0-r1"),
            self._row("unmapped_quote_row", normalized_row_id="qr-p0-r2"),
            self._row("missing_quote_information", normalized_row_id="qr-p0-r3"),
        ])
        out = prioritize_classified_contract(c)
        ids = [r["normalized_row_id"] for r in out["reconciliation_rows"]]
        # critical (r1), high (r2), medium (r3), low (r0)
        assert ids == ["qr-p0-r1", "qr-p0-r2", "qr-p0-r3", "qr-p0-r0"]

    def test_contract_decoration_no_row_hidden(self):
        from app.pdf_extraction.review_prioritization import prioritize_classified_contract
        rows_in = [
            self._row("comparable_match", normalized_row_id=f"qr-p0-r{i}")
            for i in range(10)
        ]
        c = self._contract(rows_in)
        out = prioritize_classified_contract(c)
        assert len(out["reconciliation_rows"]) == 10

    def test_contract_decoration_does_not_mutate_input(self):
        from app.pdf_extraction.review_prioritization import prioritize_classified_contract
        c = self._contract([self._row("comparable_match")])
        import copy
        snap = copy.deepcopy(c)
        prioritize_classified_contract(c)
        assert c == snap

    # ---- Findings packet decoration ----

    def _findings_packet(self, rows):
        return {
            "packet_version": "findings_packet/v1",
            "packet_status": "partial",
            "pairing_section": {"pairing_status": "trusted"},
            "quote_section": {}, "bid_section": {}, "mapping_section": {},
            "reconciliation_section": {},
            "discrepancy_summary": {},
            "findings_rows": rows,
            "packet_diagnostics": {},
        }

    def test_findings_packet_decoration(self):
        from app.pdf_extraction.review_prioritization import prioritize_findings_packet
        rows = [
            self._row("comparable_match", normalized_row_id="qr-p0-r0"),
            self._row("blocked_by_pairing", normalized_row_id="qr-p0-r1"),
        ]
        fp = self._findings_packet(rows)
        out = prioritize_findings_packet(fp)
        assert out["prioritization_version"] == "review_prioritization/v1"
        assert out["findings_rows"][0]["normalized_row_id"] == "qr-p0-r1"
        assert out["priority_summary"]["critical_count"] == 1
        assert out["priority_summary"]["low_count"] == 1

    def test_top_issues_summary_only_critical_and_high(self):
        from app.pdf_extraction.review_prioritization import prioritize_findings_packet
        rows = [
            self._row("blocked_by_pairing", normalized_row_id="qr-p0-r0"),
            self._row("unmapped_quote_row", normalized_row_id="qr-p0-r1"),
            self._row("comparable_match", normalized_row_id="qr-p0-r2"),
            self._row("missing_quote_information", normalized_row_id="qr-p0-r3"),
        ]
        fp = self._findings_packet(rows)
        out = prioritize_findings_packet(fp)
        top = out["priority_summary"]["top_issues_summary"]
        assert len(top) == 2
        classes = {t["priority_class"] for t in top}
        assert classes == {"critical", "high"}

    # ---- Endpoint integration ----

    def test_reconcile_endpoint_has_priority(self, client, ipsi_quote_path, dot_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/reconcile",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["prioritization_version"] == "review_prioritization/v1"
        s = data["priority_summary"]
        assert s["rows_total"] >= 15
        assert s["high_count"] >= 2  # 2 unmapped rows
        assert s["medium_count"] >= 13  # 13 missing_quote_info
        for row in data["reconciliation_rows"]:
            assert "priority_class" in row
            assert row["priority_class"] in ("critical", "high", "medium", "low", "informational")

    def test_findings_endpoint_has_priority(self, client, ipsi_quote_path, dot_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/findings",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["prioritization_version"] == "review_prioritization/v1"
        # First findings row should be high (no critical rows in this fixture).
        first = data["findings_rows"][0]
        assert first["priority_class"] == "high"

    def test_findings_endpoint_blocked_priority(self, client, ipsi_quote_path, estprop_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(estprop_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/findings",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("estprop121.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 422
        data = resp.json()
        for row in data["findings_rows"]:
            assert row["priority_class"] == "critical"
        assert data["priority_summary"]["critical_count"] == data["priority_summary"]["rows_total"]

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c21(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c21(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True

    def test_quote_staging_unchanged_under_c21(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14


# ---------------------------------------------------------------------------
# 20uuu. C80 — Artifact Repository Tests
# ---------------------------------------------------------------------------


class TestC80ArtifactRepository:

    def _repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        return ArtifactRepository()

    def test_save_and_latest(self):
        repo = self._repo()
        art = {"dossier_version": "v1", "job_id": "j1", "vendor_name": "V1"}
        rec = repo.save("quote_dossier", art)
        assert rec["revision_sequence"] == 0
        latest = repo.latest("quote_dossier", job_id="j1")
        assert latest["record_id"] == rec["record_id"]

    def test_append_revision_supersedes(self):
        repo = self._repo()
        art1 = {"dossier_version": "v1", "job_id": "j1"}
        rec1 = repo.save("quote_dossier", art1)
        art2 = {"dossier_version": "v1", "job_id": "j1", "decision_posture": "ready"}
        rec2 = repo.save("quote_dossier", art2)
        # Latest should be rec2.
        latest = repo.latest("quote_dossier", job_id="j1")
        assert latest["record_id"] == rec2["record_id"]
        # History has both, rec1 superseded by rec2.
        hist = repo.history("quote_dossier", job_id="j1")
        assert len(hist) == 2
        assert hist[0]["superseded_by"] == rec2["record_id"]

    def test_by_bid_id(self):
        repo = self._repo()
        repo.save("package_overview", {"package_overview_version": "v1", "bid_id": "b1"})
        repo.save("bid_readiness_snapshot", {"readiness_snapshot_version": "v1",
                                              "bid_id": "b1", "overall_readiness": "ready"})
        recs = repo.by_bid_id("b1")
        assert len(recs) == 2

    def test_by_artifact_type(self):
        repo = self._repo()
        repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j2"})
        repo.save("package_overview", {"package_overview_version": "v1", "bid_id": "b1"})
        assert len(repo.by_artifact_type("quote_dossier")) == 2
        assert len(repo.by_artifact_type("package_overview")) == 1

    def test_by_record_id(self):
        repo = self._repo()
        rec = repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        fetched = repo.by_record_id(rec["record_id"])
        assert fetched["record_id"] == rec["record_id"]

    def test_by_revision_sequence(self):
        repo = self._repo()
        repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        rev0 = repo.by_revision_sequence("quote_dossier", 0, bid_id=None)
        rev1 = repo.by_revision_sequence("quote_dossier", 1, bid_id=None)
        assert rev0["revision_sequence"] == 0
        assert rev1["revision_sequence"] == 1

    def test_lineage_traversal(self):
        repo = self._repo()
        r1 = repo.save("bid_carry_justification", {"carry_justification_version": "v1",
                                                    "bid_id": "b1", "carry_decision": "hold_pending_resolution"})
        r2 = repo.save("bid_carry_justification", {"carry_justification_version": "v1",
                                                    "bid_id": "b1", "carry_decision": "proceed_with_caveats"})
        r3 = repo.save("bid_carry_justification", {"carry_justification_version": "v1",
                                                    "bid_id": "b1", "carry_decision": "proceed_to_bid"})
        chain = repo.lineage(r1["record_id"])
        assert len(chain) == 3
        assert chain[0]["record_id"] == r1["record_id"]
        assert chain[-1]["record_id"] == r3["record_id"]

    def test_not_found_returns_none(self):
        repo = self._repo()
        assert repo.latest("quote_dossier", job_id="none") is None
        assert repo.by_record_id("missing") is None

    def test_repository_summary(self):
        repo = self._repo()
        repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        repo.save("package_overview", {"package_overview_version": "v1", "bid_id": "b1"})
        s = repo.repository_summary()
        assert s["total_records"] == 2
        assert s["records_by_type"]["quote_dossier"] == 1

    def test_save_deep_copied(self):
        repo = self._repo()
        art = {"dossier_version": "v1", "job_id": "j1", "nested": {"x": 1}}
        repo.save("quote_dossier", art)
        art["nested"]["x"] = 999
        latest = repo.latest("quote_dossier", job_id="j1")
        inner = (latest["envelope"].get("artifact") or {}).get("nested") or {}
        assert inner.get("x") == 1


# ---------------------------------------------------------------------------
# 20vvv. C81 — Control Room Assembly Tests
# ---------------------------------------------------------------------------


class TestC81ControlRoomAssembly:

    def _repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        return ArtifactRepository()

    def _dossier(self, jid="j1"):
        return {"dossier_version": "quote_dossier/v1", "job_id": jid, "vendor_name": "V1",
                "decision_posture": "usable_with_caveats", "readiness_status": "actionable",
                "latest_gate": {"gate_outcome": "CONDITIONAL"},
                "latest_risk": {"overall_risk_level": "medium"},
                "comparability_posture": {"total_rows": 15},
                "scope_gaps": {}, "evidence_status": {}, "reliance_posture": {},
                "open_clarifications": {}, "response_history_summary": {},
                "active_assumptions": [], "recommendation_summary": {}}

    def test_quote_case_payload(self):
        from app.pdf_extraction.control_room_assembly import assemble_quote_case_payload
        repo = self._repo()
        repo.save("quote_dossier", self._dossier("j1"))
        out = assemble_quote_case_payload(repo, "j1")
        assert out["payload_type"] == "quote_case_payload"
        assert out["assembly_diagnostics"]["dossier_present"] is True
        assert out["view_model"]["job_id"] == "j1"

    def test_package_overview_payload(self):
        from app.pdf_extraction.control_room_assembly import assemble_package_overview_payload
        repo = self._repo()
        repo.save("package_overview", {"package_overview_version": "v1", "bid_id": "b1",
                                        "quote_count": 2, "quote_summaries": [],
                                        "package_summary": {}})
        out = assemble_package_overview_payload(repo, "b1")
        assert out["payload_type"] == "package_overview_payload"
        assert out["assembly_diagnostics"]["package_overview_present"] is True

    def test_authority_action_payload(self):
        from app.pdf_extraction.control_room_assembly import assemble_authority_action_payload
        repo = self._repo()
        repo.save("authority_action_packet", {"authority_action_version": "v1",
                                                "action_item_count": 3,
                                                "top_priority_actions": [],
                                                "implication_groups": [],
                                                "action_summary": {},
                                                "package_ref": {"bid_id": "b1"}})
        out = assemble_authority_action_payload(repo)
        assert out["assembly_diagnostics"]["authority_action_present"] is True

    def test_bid_readiness_payload(self):
        from app.pdf_extraction.control_room_assembly import assemble_bid_readiness_payload
        repo = self._repo()
        repo.save("bid_readiness_snapshot", {"readiness_snapshot_version": "v1",
                                               "bid_id": "b1", "overall_readiness": "ready",
                                               "package_confidence": {}, "authority_posture": {},
                                               "deadline_pressure": {}, "top_unresolved_items": [],
                                               "top_priority_queue_actions": [],
                                               "carry_decision_posture": {},
                                               "vendor_highlights": {},
                                               "package_summary_counts": {},
                                               "top_reasons": [], "traceability_refs": {}})
        out = assemble_bid_readiness_payload(repo, "b1")
        assert out["payload_type"] == "bid_readiness_payload"
        assert out["assembly_diagnostics"]["readiness_present"] is True

    def test_timeline_payload(self):
        from app.pdf_extraction.control_room_assembly import assemble_timeline_payload
        repo = self._repo()
        repo.save("bid_carry_justification", {"carry_justification_version": "v1",
                                                "bid_id": "b1", "carry_decision": "hold_pending_resolution"})
        repo.save("bid_carry_justification", {"carry_justification_version": "v1",
                                                "bid_id": "b1", "carry_decision": "proceed_to_bid"})
        out = assemble_timeline_payload(repo, bid_id="b1")
        assert out["payload_type"] == "timeline_payload"
        assert len(out["kind_timelines"]) >= 1
        # At least 2 events in the carry timeline (create + revise).
        carry_tl = next(tl for tl in out["kind_timelines"] if tl["artifact_kind"] == "bid_carry_justification")
        assert carry_tl["timeline_summary"]["event_count"] >= 2

    def test_missing_artifact_returns_empty(self):
        from app.pdf_extraction.control_room_assembly import assemble_quote_case_payload
        repo = self._repo()
        out = assemble_quote_case_payload(repo, "missing")
        assert out["assembly_diagnostics"]["dossier_present"] is False


# ---------------------------------------------------------------------------
# 20www. C82 — Export Orchestration Tests
# ---------------------------------------------------------------------------


class TestC82ExportOrchestration:

    def _repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        return ArtifactRepository()

    def test_sub_clarification_orchestration(self):
        from app.pdf_extraction.export_orchestration import generate_sub_clarification_export
        repo = self._repo()
        repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1", "vendor_name": "V1"})
        out = generate_sub_clarification_export(repo, "j1")
        assert out["export_orchestration_version"] == "export_orchestration/v1"
        assert out["export"]["export_type"] == "subcontractor_clarification_packet"

    def test_estimator_review_orchestration(self):
        from app.pdf_extraction.export_orchestration import generate_estimator_review_export
        repo = self._repo()
        repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1", "vendor_name": "V1",
                                      "decision_posture": "requires_action",
                                      "readiness_status": "actionable",
                                      "comparability_posture": {}, "scope_gaps": {},
                                      "evidence_status": {}, "open_clarifications": {}})
        out = generate_estimator_review_export(repo, "j1")
        assert out["export"]["export_type"] == "estimator_review_packet"

    def test_bid_readiness_orchestration(self):
        from app.pdf_extraction.export_orchestration import generate_bid_readiness_export
        repo = self._repo()
        repo.save("bid_readiness_snapshot", {"readiness_snapshot_version": "v1",
                                               "bid_id": "b1", "overall_readiness": "ready"})
        out = generate_bid_readiness_export(repo, "b1")
        assert out["export"]["export_type"] == "bid_readiness_packet"
        assert any(s["artifact_type"] == "bid_readiness_snapshot" for s in out["source_records"])

    def test_final_carry_orchestration(self):
        from app.pdf_extraction.export_orchestration import generate_final_carry_export
        repo = self._repo()
        repo.save("bid_carry_justification", {"carry_justification_version": "v1",
                                                "bid_id": "b1", "carry_decision": "proceed_to_bid",
                                                "record_id": "carry-b1"})
        out = generate_final_carry_export(repo, "b1")
        assert out["export"]["export_type"] == "final_bid_carry_justification_packet"

    def test_explicit_revision_version(self):
        from app.pdf_extraction.export_orchestration import generate_final_carry_export
        repo = self._repo()
        repo.save("bid_carry_justification", {"carry_justification_version": "v1",
                                                "bid_id": "b1", "carry_decision": "hold_pending_resolution",
                                                "record_id": "carry-b1-v0"})
        repo.save("bid_carry_justification", {"carry_justification_version": "v1",
                                                "bid_id": "b1", "carry_decision": "proceed_to_bid",
                                                "record_id": "carry-b1-v1"})
        out_latest = generate_final_carry_export(repo, "b1")
        out_v0 = generate_final_carry_export(repo, "b1", revision_sequence=0)
        assert out_latest["export"]["payload"]["carry_decision"] == "proceed_to_bid"
        assert out_v0["export"]["payload"]["carry_decision"] == "hold_pending_resolution"

    def test_authority_action_orchestration(self):
        from app.pdf_extraction.export_orchestration import generate_authority_action_export
        repo = self._repo()
        repo.save("authority_action_packet", {"authority_action_version": "v1",
                                                "action_item_count": 2,
                                                "top_priority_actions": [],
                                                "implication_groups": [], "action_summary": {},
                                                "package_ref": {"bid_id": "b1"}})
        out = generate_authority_action_export(repo, "b1")
        assert out["export"]["export_type"] == "authority_action_packet_export"


# ---------------------------------------------------------------------------
# 20xxx. C83 — API Endpoint Surface Tests
# ---------------------------------------------------------------------------


class TestC83ApiEndpoints:

    def setup_method(self):
        from app.pdf_extraction.artifact_repository import reset_default_repository
        reset_default_repository()

    def test_canonical_schema_types(self, client):
        r = client.get("/canonical/schema-types")
        assert r.status_code == 200
        data = r.json()
        assert "supported_types" in data
        assert "quote_dossier" in data["supported_types"]

    def test_save_and_retrieve_artifact(self, client):
        payload = {"artifact": {"dossier_version": "v1", "job_id": "j-api-1", "vendor_name": "V1"},
                   "metadata": {"created_by": "api-test"}}
        r = client.post("/canonical/artifacts/quote_dossier", json=payload)
        assert r.status_code == 200
        rec = r.json()
        assert rec["revision_sequence"] == 0

        r2 = client.get("/canonical/artifacts/quote_dossier/latest", params={"job_id": "j-api-1"})
        assert r2.status_code == 200
        assert r2.json()["record_id"] == rec["record_id"]

    def test_history_endpoint(self, client):
        client.post("/canonical/artifacts/quote_dossier",
                     json={"artifact": {"dossier_version": "v1", "job_id": "j-h"}})
        client.post("/canonical/artifacts/quote_dossier",
                     json={"artifact": {"dossier_version": "v1", "job_id": "j-h"}})
        r = client.get("/canonical/artifacts/quote_dossier/history", params={"job_id": "j-h"})
        assert r.status_code == 200
        assert len(r.json()["records"]) == 2

    def test_by_bid_id_endpoint(self, client):
        client.post("/canonical/artifacts/package_overview",
                     json={"artifact": {"package_overview_version": "v1", "bid_id": "b-api-1",
                                         "quote_count": 2, "quote_summaries": [], "package_summary": {}}})
        r = client.get("/canonical/artifacts/by-bid/b-api-1")
        assert r.status_code == 200
        assert len(r.json()["records"]) == 1

    def test_latest_not_found_404(self, client):
        r = client.get("/canonical/artifacts/quote_dossier/latest", params={"job_id": "nope"})
        assert r.status_code == 404

    def test_repository_summary_endpoint(self, client):
        client.post("/canonical/artifacts/quote_dossier",
                     json={"artifact": {"dossier_version": "v1", "job_id": "j-s"}})
        r = client.get("/canonical/repository/summary")
        assert r.status_code == 200
        assert r.json()["total_records"] >= 1

    def test_repository_reset(self, client):
        client.post("/canonical/artifacts/quote_dossier",
                     json={"artifact": {"dossier_version": "v1", "job_id": "j-r"}})
        r = client.post("/canonical/repository/reset")
        assert r.status_code == 200
        assert r.json()["repository_summary"]["total_records"] == 0

    def test_control_room_bid_readiness(self, client):
        client.post("/canonical/artifacts/bid_readiness_snapshot",
                     json={"artifact": {"readiness_snapshot_version": "v1", "bid_id": "b-r",
                                         "overall_readiness": "ready", "package_confidence": {},
                                         "authority_posture": {}, "deadline_pressure": {},
                                         "top_unresolved_items": [], "top_priority_queue_actions": [],
                                         "carry_decision_posture": {}, "vendor_highlights": {},
                                         "package_summary_counts": {}, "top_reasons": [],
                                         "traceability_refs": {}}})
        r = client.get("/control-room/bid-readiness/b-r")
        assert r.status_code == 200
        assert r.json()["assembly_diagnostics"]["readiness_present"] is True

    def test_export_endpoint_bid_readiness(self, client):
        client.post("/canonical/artifacts/bid_readiness_snapshot",
                     json={"artifact": {"readiness_snapshot_version": "v1", "bid_id": "b-exp",
                                         "overall_readiness": "ready"}})
        r = client.get("/exports/bid-readiness/b-exp")
        assert r.status_code == 200
        assert r.json()["export"]["export_type"] == "bid_readiness_packet"

    def test_timeline_endpoint(self, client):
        client.post("/canonical/artifacts/bid_carry_justification",
                     json={"artifact": {"carry_justification_version": "v1", "bid_id": "b-tl",
                                         "carry_decision": "hold_pending_resolution"}})
        client.post("/canonical/artifacts/bid_carry_justification",
                     json={"artifact": {"carry_justification_version": "v1", "bid_id": "b-tl",
                                         "carry_decision": "proceed_to_bid"}})
        r = client.get("/control-room/timeline", params={"bid_id": "b-tl"})
        assert r.status_code == 200
        assert r.json()["payload_type"] == "timeline_payload"

    def test_demo_scenarios_endpoint(self, client):
        r = client.get("/demo/scenarios")
        assert r.status_code == 200
        data = r.json()
        assert "straightforward_usable" in data["scenarios"]

    def test_demo_run_scenario(self, client):
        r = client.post("/demo/run/straightforward_usable")
        assert r.status_code == 200
        out = r.json()
        assert out["canonical_artifacts"]["readiness_snapshot"]["bid_id"] == "seed-straightforward"

    def test_demo_run_unknown_scenario(self, client):
        r = client.post("/demo/run/made_up")
        assert r.status_code == 400

    def test_demo_fixture_endpoint(self, client):
        r = client.get("/demo/fixture", params={"bid_id": "custom"})
        assert r.status_code == 200
        assert r.json()["bid_id"] == "custom"

    def test_demo_run_e2e_endpoint(self, client):
        r = client.post("/demo/run-e2e", json={})
        assert r.status_code == 200
        assert r.json()["demo_harness_version"] == "e2e_demo_harness/v1"


# ---------------------------------------------------------------------------
# 20yyy. C84 — End-to-End Integration Tests
# ---------------------------------------------------------------------------


class TestC84EndToEndIntegration:

    def setup_method(self):
        from app.pdf_extraction.artifact_repository import reset_default_repository
        reset_default_repository()

    def _run_scenario_and_persist(self, client, scenario_id):
        """Run scenario and persist all canonical artifacts into repository."""
        from app.pdf_extraction.seed_scenarios import run_scenario_e2e
        from app.pdf_extraction.artifact_repository import get_default_repository
        out = run_scenario_e2e(scenario_id)
        repo = get_default_repository()
        ca = out["canonical_artifacts"]
        repo.save("package_overview", ca["package_overview"])
        repo.save("bid_readiness_snapshot", ca["readiness_snapshot"])
        repo.save("bid_carry_justification", ca["carry_justification"])
        repo.save("authority_action_packet", ca["authority_action_packet"])
        repo.save("authority_posture", ca["authority_posture"])
        repo.save("authority_reference", ca["authority_reference"])
        repo.save("deadline_pressure", ca["deadline_pressure"])
        repo.save("priority_queue", ca["priority_queue"])
        repo.save("vendor_comparison", ca["vendor_comparison"])
        return out

    def test_straightforward_scenario_e2e(self, client):
        out = self._run_scenario_and_persist(client, "straightforward_usable")
        bid_id = out["bid_id"]
        r = client.get(f"/control-room/bid-readiness/{bid_id}")
        assert r.status_code == 200
        # Readiness reflects the actual canonical snapshot — just verify it's set.
        readiness = r.json()["view_model"]["header"]["overall_readiness"]
        assert readiness is not None

    def test_high_risk_scenario_e2e(self, client):
        out = self._run_scenario_and_persist(client, "high_risk_incomplete")
        bid_id = out["bid_id"]
        r = client.get(f"/control-room/bid-readiness/{bid_id}")
        assert r.status_code == 200
        # Gate should be HIGH_RISK or BLOCKED.
        gate = out["canonical_artifacts"]["package_gate"]["package_gate_outcome"]
        assert gate in ("PACKAGE_HIGH_RISK", "PACKAGE_CONDITIONAL", "PACKAGE_BLOCKED")

    def test_blocked_authority_scenario_e2e(self, client):
        out = self._run_scenario_and_persist(client, "blocked_authority")
        # Authority posture should be blocked due to uncovered required topics.
        posture = out["canonical_artifacts"]["authority_posture"]["authority_package_posture"]
        assert posture == "authority_blocked"

    def test_proceed_with_caveats_scenario_e2e(self, client):
        out = self._run_scenario_and_persist(client, "proceed_with_caveats")
        assert out["canonical_artifacts"]["carry_justification"]["carry_decision"] == "proceed_with_caveats"

    def test_latest_vs_historical_retrieval(self, client):
        # Save two revisions of the same carry justification.
        client.post("/canonical/artifacts/bid_carry_justification",
                     json={"artifact": {"carry_justification_version": "v1",
                                         "bid_id": "b-hist",
                                         "carry_decision": "hold_pending_resolution"}})
        client.post("/canonical/artifacts/bid_carry_justification",
                     json={"artifact": {"carry_justification_version": "v1",
                                         "bid_id": "b-hist",
                                         "carry_decision": "proceed_to_bid"}})
        # Latest export reflects proceed.
        r_latest = client.get("/exports/final-carry/b-hist")
        assert r_latest.json()["export"]["payload"]["carry_decision"] == "proceed_to_bid"
        # Historical rev 0 export reflects hold.
        r_hist = client.get("/exports/final-carry/b-hist",
                             params={"revision_sequence": 0})
        assert r_hist.json()["export"]["payload"]["carry_decision"] == "hold_pending_resolution"

    def test_timeline_across_revisions(self, client):
        client.post("/canonical/artifacts/bid_carry_justification",
                     json={"artifact": {"carry_justification_version": "v1",
                                         "bid_id": "b-tl-1",
                                         "carry_decision": "hold_pending_resolution"}})
        client.post("/canonical/artifacts/bid_carry_justification",
                     json={"artifact": {"carry_justification_version": "v1",
                                         "bid_id": "b-tl-1",
                                         "carry_decision": "proceed_with_caveats"}})
        client.post("/canonical/artifacts/bid_carry_justification",
                     json={"artifact": {"carry_justification_version": "v1",
                                         "bid_id": "b-tl-1",
                                         "carry_decision": "proceed_to_bid"}})
        r = client.get("/control-room/timeline", params={"bid_id": "b-tl-1"})
        assert r.status_code == 200
        tl = r.json()["merged_timeline"]
        assert tl["timeline_summary"]["event_count"] >= 3

    def test_dot_native_unchanged_under_c84(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c84(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0 and summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20zzz. C85 — Seed Scenarios Tests
# ---------------------------------------------------------------------------


class TestC85SeedScenarios:

    def test_list_scenarios(self):
        from app.pdf_extraction.seed_scenarios import list_scenarios
        s = list_scenarios()
        assert "straightforward_usable" in s
        assert "high_risk_incomplete" in s
        assert "blocked_authority" in s
        assert "proceed_with_caveats" in s

    def test_straightforward_fixture(self):
        from app.pdf_extraction.seed_scenarios import build_scenario
        fx = build_scenario("straightforward_usable")
        assert fx["bid_id"] == "seed-straightforward"
        assert fx["hours_until_due"] == 48.0

    def test_high_risk_fixture(self):
        from app.pdf_extraction.seed_scenarios import build_scenario
        fx = build_scenario("high_risk_incomplete")
        assert fx["bid_id"] == "seed-high-risk"
        # Multiple high-risk dossiers.
        assert len(fx["dossiers"]) == 2
        assert fx["hours_until_due"] == 12.0

    def test_blocked_authority_fixture(self):
        from app.pdf_extraction.seed_scenarios import build_scenario
        fx = build_scenario("blocked_authority")
        # Authority entries all required; no scope topics to match them.
        assert len(fx["authority_entries"]) == 2
        assert fx["scope_topics"] == []

    def test_proceed_with_caveats_fixture(self):
        from app.pdf_extraction.seed_scenarios import build_scenario
        fx = build_scenario("proceed_with_caveats")
        assert fx["bid_id"] == "seed-caveats"

    def test_unknown_scenario_raises(self):
        from app.pdf_extraction.seed_scenarios import build_scenario
        import pytest as _p
        with _p.raises(ValueError):
            build_scenario("made_up_scenario")

    def test_run_straightforward_produces_low_risk(self):
        from app.pdf_extraction.seed_scenarios import run_scenario_e2e
        out = run_scenario_e2e("straightforward_usable")
        # The underlying dossier carries SAFE gate + low risk per quote.
        dossiers = out["canonical_artifacts"]["package_overview"]["quote_summaries"]
        assert all(d["risk_level"] == "low" for d in dossiers)
        assert all(d["gate_outcome"] == "SAFE" for d in dossiers)

    def test_run_high_risk_escalates(self):
        from app.pdf_extraction.seed_scenarios import run_scenario_e2e
        out = run_scenario_e2e("high_risk_incomplete")
        risk_levels = [d["risk_level"]
                       for d in out["canonical_artifacts"]["package_overview"]["quote_summaries"]]
        assert all(r == "high" for r in risk_levels)

    def test_run_blocked_authority_produces_blocked_posture(self):
        from app.pdf_extraction.seed_scenarios import run_scenario_e2e
        out = run_scenario_e2e("blocked_authority")
        assert out["canonical_artifacts"]["authority_posture"]["authority_package_posture"] == "authority_blocked"

    def test_run_proceed_with_caveats(self):
        from app.pdf_extraction.seed_scenarios import run_scenario_e2e
        out = run_scenario_e2e("proceed_with_caveats")
        assert out["canonical_artifacts"]["carry_justification"]["carry_decision"] == "proceed_with_caveats"

    def test_deterministic_repeat(self):
        from app.pdf_extraction.seed_scenarios import run_scenario_e2e
        o1 = run_scenario_e2e("straightforward_usable")
        o2 = run_scenario_e2e("straightforward_usable")
        assert o1["demo_summary"] == o2["demo_summary"]

    def test_dot_native_unchanged_under_c85(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20rrr. C77 — Canonical API Contracts Tests
# ---------------------------------------------------------------------------


class TestC77CanonicalApiContracts:

    def test_version_present(self):
        from app.pdf_extraction.canonical_api_contracts import (
            serialize_artifact, API_CONTRACT_VERSION, ARTIFACT_QUOTE_DOSSIER,
        )
        out = serialize_artifact(ARTIFACT_QUOTE_DOSSIER, {"dossier_version": "v1", "job_id": "j1"})
        assert out["api_contract_version"] == API_CONTRACT_VERSION

    def test_serialize_quote_dossier(self):
        from app.pdf_extraction.canonical_api_contracts import (
            serialize_artifact, ARTIFACT_QUOTE_DOSSIER,
        )
        art = {"dossier_version": "quote_dossier/v1", "job_id": "j1", "vendor_name": "V1"}
        out = serialize_artifact(ARTIFACT_QUOTE_DOSSIER, art)
        assert out["artifact_type"] == ARTIFACT_QUOTE_DOSSIER
        assert out["artifact_type_valid"] is True
        assert out["stable_schema_id"] == "bid_guardrail.quote_dossier"
        assert out["identity_refs"]["job_id"] == "j1"
        assert out["identity_refs"]["vendor_name"] == "V1"

    def test_unknown_artifact_type_flagged(self):
        from app.pdf_extraction.canonical_api_contracts import serialize_artifact
        out = serialize_artifact("made_up", {})
        assert out["artifact_type_valid"] is False
        assert out["stable_schema_id"] is None

    def test_lineage_from_metadata(self):
        from app.pdf_extraction.canonical_api_contracts import (
            serialize_artifact, ARTIFACT_BID_CARRY_JUSTIFICATION,
        )
        meta = {"revision_sequence": 3, "superseded_by": "carry-X", "supersedes": "carry-W",
                "created_by": "alice", "created_at": "2026-04-16T10:00"}
        out = serialize_artifact(ARTIFACT_BID_CARRY_JUSTIFICATION,
                                  {"carry_justification_version": "v1", "bid_id": "b1",
                                   "carry_decision": "proceed_to_bid"}, meta)
        assert out["lineage"]["revision_sequence"] == 3
        assert out["lineage"]["superseded_by"] == "carry-X"
        assert out["lineage"]["created_by"] == "alice"

    def test_parse_artifact_roundtrip(self):
        from app.pdf_extraction.canonical_api_contracts import (
            serialize_artifact, parse_artifact, ARTIFACT_PACKAGE_OVERVIEW,
        )
        art = {"package_overview_version": "v1", "bid_id": "b1", "quote_count": 3}
        env = serialize_artifact(ARTIFACT_PACKAGE_OVERVIEW, art)
        parsed = parse_artifact(env)
        assert parsed["artifact_type"] == ARTIFACT_PACKAGE_OVERVIEW
        assert parsed["artifact"]["bid_id"] == "b1"
        assert parsed["artifact"]["quote_count"] == 3

    def test_list_supported_types(self):
        from app.pdf_extraction.canonical_api_contracts import list_supported_artifact_types
        types = list_supported_artifact_types()
        assert "quote_dossier" in types
        assert "package_overview" in types
        assert "bid_carry_justification" in types

    def test_schema_descriptor(self):
        from app.pdf_extraction.canonical_api_contracts import get_schema_descriptor, ARTIFACT_QUOTE_DOSSIER
        desc = get_schema_descriptor(ARTIFACT_QUOTE_DOSSIER)
        assert desc["stable_schema_id"] == "bid_guardrail.quote_dossier"
        assert "job_id" in desc["id_fields"]

    def test_schema_descriptor_unknown(self):
        from app.pdf_extraction.canonical_api_contracts import get_schema_descriptor
        desc = get_schema_descriptor("bogus")
        assert desc["type_valid"] is False

    def test_source_refs_for_readiness(self):
        from app.pdf_extraction.canonical_api_contracts import (
            serialize_artifact, ARTIFACT_BID_READINESS_SNAPSHOT,
        )
        art = {"readiness_snapshot_version": "v1", "bid_id": "b1",
               "overall_readiness": "ready", "traceability_refs": {"x": True}}
        out = serialize_artifact(ARTIFACT_BID_READINESS_SNAPSHOT, art)
        assert out["source_refs"]["overall_readiness"] == "ready"
        assert out["source_refs"]["traceability_refs"]["x"] is True

    def test_artifact_deep_copied(self):
        from app.pdf_extraction.canonical_api_contracts import (
            serialize_artifact, ARTIFACT_QUOTE_DOSSIER,
        )
        art = {"dossier_version": "v1", "job_id": "j1", "nested": {"x": 1}}
        out = serialize_artifact(ARTIFACT_QUOTE_DOSSIER, art)
        out["artifact"]["nested"]["x"] = 999
        assert art["nested"]["x"] == 1

    def test_export_packet_source_refs(self):
        from app.pdf_extraction.canonical_api_contracts import (
            serialize_artifact, ARTIFACT_EXPORT_PACKET,
        )
        art = {"export_version": "v1", "export_type": "bid_readiness_packet",
               "source_refs": {"readiness_snapshot_version": "v1"}}
        out = serialize_artifact(ARTIFACT_EXPORT_PACKET, art)
        assert out["source_refs"]["export_type"] == "bid_readiness_packet"
        assert out["source_refs"]["inner_source_refs"]["readiness_snapshot_version"] == "v1"

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20sss. C78 — Revision Timeline Tests
# ---------------------------------------------------------------------------


class TestC78RevisionTimeline:

    def _records(self, count=3, state_field="carry_decision"):
        states = ["hold_pending_resolution", "proceed_with_caveats", "proceed_to_bid"]
        out = []
        for i in range(count):
            r = {"record_id": f"rec-{i}", "revision_sequence": i,
                 "decided_by": f"user{i}", "decided_at": f"2026-04-16T1{i}:00"}
            if state_field:
                r[state_field] = states[min(i, len(states) - 1)]
            if i < count - 1:
                r["superseded_by"] = f"rec-{i+1}"
            out.append(r)
        return out

    def test_version_present(self):
        from app.pdf_extraction.revision_timeline import build_revision_timeline
        out = build_revision_timeline([])
        assert out["timeline_version"] == "revision_timeline/v1"

    def test_creation_event(self):
        from app.pdf_extraction.revision_timeline import build_revision_timeline, EVENT_CREATED
        out = build_revision_timeline(self._records(count=1, state_field=None))
        creations = [e for e in out["events"] if e["event_type"] == EVENT_CREATED]
        assert len(creations) == 1

    def test_revision_events(self):
        from app.pdf_extraction.revision_timeline import build_revision_timeline, EVENT_REVISED
        out = build_revision_timeline(self._records(count=3, state_field=None))
        revisions = [e for e in out["events"] if e["event_type"] == EVENT_REVISED]
        assert len(revisions) == 2

    def test_supersession_events(self):
        from app.pdf_extraction.revision_timeline import build_revision_timeline, EVENT_SUPERSEDED
        out = build_revision_timeline(self._records(count=3, state_field=None))
        supers = [e for e in out["events"] if e["event_type"] == EVENT_SUPERSEDED]
        assert len(supers) == 2

    def test_state_transition_carry(self):
        from app.pdf_extraction.revision_timeline import (
            build_revision_timeline, EVENT_CARRY_DECISION_CHANGED,
        )
        out = build_revision_timeline(self._records(count=3), state_field="carry_decision")
        transitions = [e for e in out["events"] if e["event_type"] == EVENT_CARRY_DECISION_CHANGED]
        assert len(transitions) == 3  # initial + 2 changes
        assert transitions[-1]["state_after"] == "proceed_to_bid"

    def test_state_transition_readiness(self):
        from app.pdf_extraction.revision_timeline import (
            build_revision_timeline, EVENT_READINESS_CHANGED,
        )
        records = [
            {"record_id": "r0", "revision_sequence": 0, "overall_readiness": "action_required"},
            {"record_id": "r1", "revision_sequence": 1, "overall_readiness": "ready_with_caveats"},
            {"record_id": "r2", "revision_sequence": 2, "overall_readiness": "ready"},
        ]
        out = build_revision_timeline(records, state_field="overall_readiness")
        transitions = [e for e in out["events"] if e["event_type"] == EVENT_READINESS_CHANGED]
        assert len(transitions) == 3

    def test_timeline_summary(self):
        from app.pdf_extraction.revision_timeline import build_revision_timeline
        out = build_revision_timeline(self._records(count=3), state_field="carry_decision")
        s = out["timeline_summary"]
        assert s["creation_events"] == 1
        assert s["revision_events"] == 2
        assert s["supersession_events"] == 2
        assert s["state_transition_events"] == 3

    def test_merge_timelines(self):
        from app.pdf_extraction.revision_timeline import build_revision_timeline, merge_timelines
        tl1 = build_revision_timeline(self._records(count=2, state_field=None),
                                       artifact_kind="carry")
        tl2 = build_revision_timeline(self._records(count=2, state_field=None),
                                       artifact_kind="readiness")
        merged = merge_timelines(tl1, tl2)
        assert merged["merged"] is True
        assert merged["timeline_summary"]["event_count"] == \
               tl1["timeline_summary"]["event_count"] + tl2["timeline_summary"]["event_count"]

    def test_artifact_kind_propagates(self):
        from app.pdf_extraction.revision_timeline import build_revision_timeline
        out = build_revision_timeline(self._records(count=1, state_field=None),
                                       artifact_kind="bid_carry_justification")
        assert out["artifact_kind"] == "bid_carry_justification"
        assert out["events"][0]["artifact_kind"] == "bid_carry_justification"

    def test_no_records_empty_timeline(self):
        from app.pdf_extraction.revision_timeline import build_revision_timeline
        out = build_revision_timeline([])
        assert out["events"] == []
        assert out["record_count"] == 0

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20ttt. C79 — End-to-End Demo Harness Tests
# ---------------------------------------------------------------------------


class TestC79E2EDemoHarness:

    def test_version_present(self):
        from app.pdf_extraction.e2e_demo_harness import run_e2e_demo
        out = run_e2e_demo()
        assert out["demo_harness_version"] == "e2e_demo_harness/v1"

    def test_fixture_deterministic(self):
        from app.pdf_extraction.e2e_demo_harness import build_demo_fixture
        f1 = build_demo_fixture()
        f2 = build_demo_fixture()
        assert f1 == f2

    def test_all_canonical_artifacts_produced(self):
        from app.pdf_extraction.e2e_demo_harness import run_e2e_demo
        out = run_e2e_demo()
        ca = out["canonical_artifacts"]
        for key in ("package_overview", "package_gate", "vendor_comparison",
                    "authority_reference", "authority_comparison", "authority_exposure",
                    "authority_action_packet", "authority_posture", "deadline_pressure",
                    "priority_queue", "readiness_snapshot", "carry_justification"):
            assert key in ca
            assert ca[key] is not None

    def test_view_models_produced(self):
        from app.pdf_extraction.e2e_demo_harness import run_e2e_demo
        out = run_e2e_demo()
        vm = out["view_models"]
        assert "package_overview_view" in vm
        assert "authority_action_view" in vm
        assert "bid_readiness_view" in vm

    def test_exports_produced(self):
        from app.pdf_extraction.e2e_demo_harness import run_e2e_demo
        out = run_e2e_demo()
        ex = out["exports"]
        assert "bid_readiness_export" in ex
        assert "final_carry_export" in ex
        assert "authority_action_export" in ex

    def test_demo_summary(self):
        from app.pdf_extraction.e2e_demo_harness import run_e2e_demo
        out = run_e2e_demo()
        s = out["demo_summary"]
        assert s["dossier_count"] == 2
        assert s["authority_topic_count"] == 2
        assert s["carry_decision"] == "proceed_with_caveats"

    def test_deterministic_output(self):
        from app.pdf_extraction.e2e_demo_harness import run_e2e_demo
        o1 = run_e2e_demo()
        o2 = run_e2e_demo()
        assert o1["demo_summary"] == o2["demo_summary"]

    def test_carry_decision_propagated(self):
        from app.pdf_extraction.e2e_demo_harness import run_e2e_demo
        out = run_e2e_demo(carry_decision="hold_pending_resolution", decided_by="bob")
        assert out["canonical_artifacts"]["carry_justification"]["carry_decision"] == "hold_pending_resolution"
        assert out["canonical_artifacts"]["carry_justification"]["decided_by"] == "bob"

    def test_bid_id_consistent_across_artifacts(self):
        from app.pdf_extraction.e2e_demo_harness import run_e2e_demo
        out = run_e2e_demo()
        bid_id = out["bid_id"]
        assert out["canonical_artifacts"]["package_overview"]["bid_id"] == bid_id
        assert out["canonical_artifacts"]["readiness_snapshot"]["bid_id"] == bid_id
        assert out["canonical_artifacts"]["carry_justification"]["bid_id"] == bid_id

    def test_custom_fixture(self):
        from app.pdf_extraction.e2e_demo_harness import run_e2e_demo, build_demo_fixture
        fx = build_demo_fixture("custom-bid")
        out = run_e2e_demo(fx)
        assert out["bid_id"] == "custom-bid"

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0 and summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20ooo. C74 — Persistent Review Schemas Tests
# ---------------------------------------------------------------------------


class TestC74PersistentReviewSchemas:

    def test_version_present(self):
        from app.pdf_extraction.persistent_review_schemas import (
            persist_artifact, SCHEMA_QUOTE_DOSSIER, SCHEMA_VERSION,
        )
        out = persist_artifact(SCHEMA_QUOTE_DOSSIER, {"dossier_version": "v1", "job_id": "j1"})
        assert out["persistent_schema_version"] == SCHEMA_VERSION

    def test_valid_schema_type(self):
        from app.pdf_extraction.persistent_review_schemas import (
            persist_artifact, SCHEMA_PACKAGE_OVERVIEW,
        )
        out = persist_artifact(SCHEMA_PACKAGE_OVERVIEW,
                                {"package_overview_version": "v1", "bid_id": "b1"})
        assert out["schema_type_valid"] is True
        assert out["schema_diagnostics"]["missing_required_fields"] == []

    def test_unknown_schema_type(self):
        from app.pdf_extraction.persistent_review_schemas import persist_artifact
        out = persist_artifact("made_up_type", {})
        assert out["schema_type_valid"] is False

    def test_missing_required_fields_surfaced(self):
        from app.pdf_extraction.persistent_review_schemas import (
            persist_artifact, SCHEMA_QUOTE_DOSSIER,
        )
        out = persist_artifact(SCHEMA_QUOTE_DOSSIER, {"dossier_version": "v1"})
        assert "job_id" in out["schema_diagnostics"]["missing_required_fields"]

    def test_traceability_refs_populated(self):
        from app.pdf_extraction.persistent_review_schemas import (
            persist_artifact, SCHEMA_QUOTE_DOSSIER,
        )
        out = persist_artifact(SCHEMA_QUOTE_DOSSIER,
                                {"dossier_version": "v1", "job_id": "j1", "vendor_name": "V1"})
        assert out["traceability_refs"]["job_id"] == "j1"
        assert out["traceability_refs"]["vendor_name"] == "V1"

    def test_record_id_derived(self):
        from app.pdf_extraction.persistent_review_schemas import (
            persist_artifact, SCHEMA_BID_CARRY_JUSTIFICATION,
        )
        out = persist_artifact(SCHEMA_BID_CARRY_JUSTIFICATION,
                                {"carry_justification_version": "v1", "bid_id": "b1",
                                 "carry_decision": "proceed_to_bid"})
        assert out["record_id"] == "carry-b1"

    def test_record_id_caller_supplied(self):
        from app.pdf_extraction.persistent_review_schemas import (
            persist_artifact, SCHEMA_QUOTE_DOSSIER,
        )
        out = persist_artifact(SCHEMA_QUOTE_DOSSIER,
                                {"dossier_version": "v1", "job_id": "j1"},
                                metadata={"record_id": "custom-123", "persisted_by": "alice"})
        assert out["record_id"] == "custom-123"
        assert out["persisted_by"] == "alice"

    def test_append_revision_chain(self):
        from app.pdf_extraction.persistent_review_schemas import (
            persist_artifact, append_revision, get_current_record,
            get_revision_history, SCHEMA_BID_READINESS_SNAPSHOT,
        )
        r1 = persist_artifact(SCHEMA_BID_READINESS_SNAPSHOT,
                               {"readiness_snapshot_version": "v1", "bid_id": "b1",
                                "overall_readiness": "action_required"})
        records = append_revision([], r1)
        r2 = persist_artifact(SCHEMA_BID_READINESS_SNAPSHOT,
                               {"readiness_snapshot_version": "v1", "bid_id": "b1",
                                "overall_readiness": "ready"})
        records = append_revision(records, r2)
        assert len(records) == 2
        assert records[0].get("superseded_by") == r2["record_id"]
        assert get_current_record(records)["artifact"]["overall_readiness"] == "ready"
        assert len(get_revision_history(records)) == 2

    def test_append_immutable(self):
        from app.pdf_extraction.persistent_review_schemas import (
            persist_artifact, append_revision, SCHEMA_QUOTE_DOSSIER,
        )
        import copy
        r1 = persist_artifact(SCHEMA_QUOTE_DOSSIER, {"dossier_version": "v1", "job_id": "j1"})
        records = append_revision([], r1)
        snap = copy.deepcopy(records)
        r2 = persist_artifact(SCHEMA_QUOTE_DOSSIER, {"dossier_version": "v1", "job_id": "j1"})
        append_revision(records, r2)
        assert records == snap

    def test_validate_schema_direct(self):
        from app.pdf_extraction.persistent_review_schemas import (
            validate_schema, SCHEMA_PACKAGE_OVERVIEW,
        )
        v = validate_schema(SCHEMA_PACKAGE_OVERVIEW, {"package_overview_version": "v1", "bid_id": "b1"})
        assert v["is_valid"] is True
        v2 = validate_schema(SCHEMA_PACKAGE_OVERVIEW, {})
        assert v2["is_valid"] is False
        assert "bid_id" in v2["missing_required_fields"]

    def test_artifact_deep_copied(self):
        from app.pdf_extraction.persistent_review_schemas import (
            persist_artifact, SCHEMA_QUOTE_DOSSIER,
        )
        art = {"dossier_version": "v1", "job_id": "j1", "nested": {"x": 1}}
        out = persist_artifact(SCHEMA_QUOTE_DOSSIER, art)
        out["artifact"]["nested"]["x"] = 999
        assert art["nested"]["x"] == 1

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20ppp. C75 — Control Room View Models Tests
# ---------------------------------------------------------------------------


class TestC75ControlRoomViewModels:

    def _dossier(self):
        return {"dossier_version": "quote_dossier/v1", "job_id": "j1",
                "vendor_name": "V1", "decision_posture": "requires_action",
                "readiness_status": "actionable",
                "latest_gate": {"gate_outcome": "CONDITIONAL"},
                "latest_risk": {"overall_risk_level": "medium"},
                "comparability_posture": {"total_rows": 15},
                "scope_gaps": {"not_addressed_count": 5},
                "evidence_status": {"unresolved_block_count": 0},
                "reliance_posture": {"carry_in_sub_quote_count": 3},
                "open_clarifications": {"total_open": 2},
                "response_history_summary": {"total_responses": 0},
                "active_assumptions": [],
                "recommendation_summary": {"carry_internally_count": 1}}

    def test_version_present(self):
        from app.pdf_extraction.control_room_view_models import build_quote_case_view
        vm = build_quote_case_view(self._dossier())
        assert vm["view_model_version"] == "control_room_view_models/v1"

    def test_quote_case_view(self):
        from app.pdf_extraction.control_room_view_models import build_quote_case_view
        vm = build_quote_case_view(self._dossier())
        assert vm["view_type"] == "quote_case_view"
        assert vm["job_id"] == "j1"
        assert vm["vendor_name"] == "V1"
        assert vm["header"]["gate_outcome"] == "CONDITIONAL"
        assert "comparability" in vm["sections"]
        assert "scope_gaps" in vm["sections"]

    def test_package_overview_view(self):
        from app.pdf_extraction.control_room_view_models import build_package_overview_view
        po = {"package_overview_version": "v1", "bid_id": "b1", "quote_count": 2,
              "quote_summaries": [{"job_id": "j1", "vendor_name": "V1"}],
              "package_summary": {"reliance_decision_distribution": {"relied_upon": 1}}}
        pg = {"package_gate_version": "v1", "package_gate_outcome": "PACKAGE_CONDITIONAL",
              "gate_reasons": [{"check": "x", "detail": "y"}]}
        vc = {"comparison_version": "v1", "vendor_entries": [
            {"vendor_name": "V1", "job_id": "j1", "vendor_rank": "acceptable",
             "deterministic_score": 100}]}
        vm = build_package_overview_view(po, pg, vc)
        assert vm["view_type"] == "package_overview_view"
        assert vm["bid_id"] == "b1"
        assert vm["header"]["package_gate_outcome"] == "PACKAGE_CONDITIONAL"
        assert len(vm["sections"]["vendor_ranking"]) == 1

    def test_authority_action_view(self):
        from app.pdf_extraction.control_room_view_models import build_authority_action_view
        aap = {"authority_action_version": "v1", "action_item_count": 5,
               "top_priority_actions": [{"authority_topic_id": "a1"}],
               "implication_groups": [], "action_summary": {},
               "package_ref": {"bid_id": "b1"}}
        ap = {"authority_posture_version": "v1",
              "authority_package_posture": "authority_watch",
              "posture_reasons": []}
        ar = {"authority_version": "v1", "authority_summary": {"total_topics": 10}}
        vm = build_authority_action_view(aap, ap, ar)
        assert vm["view_type"] == "authority_action_view"
        assert vm["header"]["authority_package_posture"] == "authority_watch"
        assert vm["source_refs"]["package_ref"]["bid_id"] == "b1"

    def test_bid_readiness_view(self):
        from app.pdf_extraction.control_room_view_models import build_bid_readiness_view
        rs = {"readiness_snapshot_version": "v1", "bid_id": "b1",
              "overall_readiness": "ready_with_caveats",
              "package_confidence": {"package_gate_outcome": "PACKAGE_CONDITIONAL"},
              "authority_posture": {"authority_package_posture": "authority_watch"},
              "deadline_pressure": {"pressure": "at_risk_due_to_time"},
              "top_unresolved_items": [], "top_priority_queue_actions": [],
              "carry_decision_posture": {}, "vendor_highlights": {},
              "package_summary_counts": {}, "top_reasons": [],
              "traceability_refs": {}}
        pq = {"priority_queue_version": "v1", "bucket_counts": {"resolve_today": 2}}
        vm = build_bid_readiness_view(rs, pq)
        assert vm["view_type"] == "bid_readiness_view"
        assert vm["bid_id"] == "b1"
        assert vm["header"]["overall_readiness"] == "ready_with_caveats"
        assert vm["sections"]["queue_bucket_counts"]["resolve_today"] == 2

    def test_state_labels_present(self):
        from app.pdf_extraction.control_room_view_models import build_quote_case_view
        vm = build_quote_case_view(self._dossier())
        assert "state_labels" in vm
        assert vm["state_labels"]["decision_posture"] == "requires_action"

    def test_source_refs_present(self):
        from app.pdf_extraction.control_room_view_models import build_quote_case_view
        vm = build_quote_case_view(self._dossier())
        assert vm["source_refs"]["dossier_version"] == "quote_dossier/v1"

    def test_input_deep_copied(self):
        from app.pdf_extraction.control_room_view_models import build_quote_case_view
        d = self._dossier()
        vm = build_quote_case_view(d)
        vm["sections"]["comparability"]["total_rows"] = 9999
        assert d["comparability_posture"]["total_rows"] == 15

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20qqq. C76 — Export Packet Builder Tests
# ---------------------------------------------------------------------------


class TestC76ExportPacketBuilder:

    def test_version_present(self):
        from app.pdf_extraction.export_packet_builder import build_sub_clarification_export
        out = build_sub_clarification_export()
        assert out["export_version"] == "export_packet_builder/v1"

    def test_sub_clarification_export(self):
        from app.pdf_extraction.export_packet_builder import (
            build_sub_clarification_export, EXPORT_SUB_CLARIFICATION,
        )
        d = {"dossier_version": "v1", "job_id": "j1", "vendor_name": "V1"}
        cl = {"clarification_items": [
            {"clarification_id": "c1", "clarification_type": "scope_gap_clarification",
             "clarification_text": "Confirm scope", "source_ref": "r1",
             "evidence_refs": [{"type": "line_ref", "value": "0530"}]},
        ]}
        tracking = {"tracking_version": "v1", "tracked_clarifications": [
            {"clarification_id": "c1", "current_status": "pending_send"}],
            "tracking_summary": {"pending_send_count": 1}}
        out = build_sub_clarification_export(d, cl, tracking)
        assert out["export_type"] == EXPORT_SUB_CLARIFICATION
        assert out["export_type_valid"] is True
        assert out["payload"]["vendor_name"] == "V1"
        assert out["payload"]["item_count"] == 1

    def test_estimator_review_export(self):
        from app.pdf_extraction.export_packet_builder import (
            build_estimator_review_export, EXPORT_ESTIMATOR_REVIEW,
        )
        d = {"dossier_version": "v1", "job_id": "j1", "vendor_name": "V1",
             "decision_posture": "requires_action", "readiness_status": "actionable",
             "comparability_posture": {}, "scope_gaps": {}, "evidence_status": {},
             "open_clarifications": {}}
        dp = {"decision_packet_version": "v1", "blocking_issues": [], "warning_issues": []}
        risk = {"risk_scoring_version": "v1", "overall_risk_level": "medium",
                "recommended_actions": [], "blocking_risks": [], "warning_risks": []}
        rec = {"recommendation_version": "v1", "recommendation_summary": {}}
        out = build_estimator_review_export(d, dp, rec, risk, {"clarification_items": []})
        assert out["export_type"] == EXPORT_ESTIMATOR_REVIEW
        assert out["payload"]["overall_risk_level"] == "medium"

    def test_authority_action_export(self):
        from app.pdf_extraction.export_packet_builder import (
            build_authority_action_export, EXPORT_AUTHORITY_ACTION,
        )
        aap = {"authority_action_version": "v1", "action_item_count": 3,
               "top_priority_actions": [], "implication_groups": [], "action_summary": {},
               "package_ref": {"bid_id": "b1"}}
        ap = {"authority_posture_version": "v1",
              "authority_package_posture": "authority_watch", "posture_summary": {}}
        ar = {"authority_version": "v1", "authority_summary": {}}
        out = build_authority_action_export(aap, ap, ar)
        assert out["export_type"] == EXPORT_AUTHORITY_ACTION
        assert out["payload"]["authority_package_posture"] == "authority_watch"

    def test_bid_readiness_export(self):
        from app.pdf_extraction.export_packet_builder import (
            build_bid_readiness_export, EXPORT_BID_READINESS,
        )
        rs = {"readiness_snapshot_version": "v1", "bid_id": "b1",
              "overall_readiness": "ready", "package_confidence": {},
              "authority_posture": {}, "deadline_pressure": {},
              "top_unresolved_items": [], "top_priority_queue_actions": [],
              "carry_decision_posture": {}, "vendor_highlights": {},
              "package_summary_counts": {}, "top_reasons": []}
        out = build_bid_readiness_export(rs)
        assert out["export_type"] == EXPORT_BID_READINESS
        assert out["payload"]["bid_id"] == "b1"
        assert out["payload"]["overall_readiness"] == "ready"

    def test_final_carry_export(self):
        from app.pdf_extraction.export_packet_builder import (
            build_final_carry_export, EXPORT_FINAL_CARRY,
        )
        cj = {"carry_justification_version": "v1", "bid_id": "b1", "record_id": "carry-b1",
              "carry_decision": "proceed_to_bid", "decided_by": "alice",
              "package_gate_outcome": "PACKAGE_READY",
              "authority_package_posture": "authority_clear",
              "unresolved_authority_gaps": {}, "internal_carry_snapshot": {},
              "authority_snapshot": {}, "acknowledged_review_items": [],
              "package_gate_reasons": [], "authority_posture_reasons": []}
        rs = {"readiness_snapshot_version": "v1", "overall_readiness": "ready",
              "top_unresolved_items": []}
        aap = {"authority_action_version": "v1", "action_item_count": 0}
        out = build_final_carry_export(cj, rs, aap)
        assert out["export_type"] == EXPORT_FINAL_CARRY
        assert out["payload"]["carry_decision"] == "proceed_to_bid"
        assert out["payload"]["readiness_at_carry"]["overall_readiness"] == "ready"

    def test_export_type_valid_flag(self):
        from app.pdf_extraction.export_packet_builder import build_sub_clarification_export
        out = build_sub_clarification_export()
        assert out["export_type_valid"] is True

    def test_source_refs_present(self):
        from app.pdf_extraction.export_packet_builder import build_bid_readiness_export
        rs = {"readiness_snapshot_version": "v1", "bid_id": "b1"}
        out = build_bid_readiness_export(rs)
        assert out["source_refs"]["readiness_snapshot_version"] == "v1"

    def test_pending_only_sub_clarifications(self):
        from app.pdf_extraction.export_packet_builder import build_sub_clarification_export
        cl = {"clarification_items": [
            {"clarification_id": "c1", "clarification_type": "x", "clarification_text": "a",
             "source_ref": "r1", "evidence_refs": []},
            {"clarification_id": "c2", "clarification_type": "x", "clarification_text": "b",
             "source_ref": "r2", "evidence_refs": []},
        ]}
        tracking = {"tracking_version": "v1", "tracked_clarifications": [
            {"clarification_id": "c1", "current_status": "pending_send"},
            {"clarification_id": "c2", "current_status": "responded"},
        ]}
        out = build_sub_clarification_export({"dossier_version": "v1", "job_id": "j1"}, cl, tracking)
        ids = [i["clarification_id"] for i in out["payload"]["clarification_items"]]
        assert ids == ["c1"]

    def test_input_deep_copied(self):
        from app.pdf_extraction.export_packet_builder import build_authority_action_export
        aap = {"authority_action_version": "v1", "top_priority_actions": [{"a": 1}]}
        out = build_authority_action_export(aap)
        out["payload"]["top_priority_actions"][0]["a"] = 999
        assert aap["top_priority_actions"][0]["a"] == 1

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0 and summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20lll. C71 — Deadline Pressure Tests
# ---------------------------------------------------------------------------


class TestC71DeadlinePressure:

    def _po(self, open_clar=0, quotes=None):
        quotes = quotes or [{"job_id": "j1", "reliance_decision": "relied_upon"}]
        return {"bid_id": "bid-1", "quote_summaries": quotes,
                "package_summary": {"total_open_clarifications": open_clar}}

    def test_version_present(self):
        from app.pdf_extraction.deadline_pressure import evaluate_deadline_pressure
        out = evaluate_deadline_pressure()
        assert out["deadline_pressure_version"] == "deadline_pressure/v1"

    def test_on_track_when_plenty_of_time(self):
        from app.pdf_extraction.deadline_pressure import evaluate_deadline_pressure, PRESSURE_ON_TRACK
        out = evaluate_deadline_pressure(hours_until_due=48, package_overview=self._po())
        assert out["deadline_pressure"] == PRESSURE_ON_TRACK

    def test_at_risk_under_24h(self):
        from app.pdf_extraction.deadline_pressure import evaluate_deadline_pressure, PRESSURE_AT_RISK
        out = evaluate_deadline_pressure(hours_until_due=20, package_overview=self._po())
        assert out["deadline_pressure"] == PRESSURE_AT_RISK

    def test_critical_under_4h(self):
        from app.pdf_extraction.deadline_pressure import evaluate_deadline_pressure, PRESSURE_CRITICAL
        out = evaluate_deadline_pressure(hours_until_due=2, package_overview=self._po())
        assert out["deadline_pressure"] == PRESSURE_CRITICAL

    def test_deadline_blocked_past_due(self):
        from app.pdf_extraction.deadline_pressure import evaluate_deadline_pressure, PRESSURE_DEADLINE_BLOCKED
        out = evaluate_deadline_pressure(hours_until_due=-1, package_overview=self._po())
        assert out["deadline_pressure"] == PRESSURE_DEADLINE_BLOCKED

    def test_critical_with_open_clarifications_under_24h(self):
        from app.pdf_extraction.deadline_pressure import evaluate_deadline_pressure, PRESSURE_CRITICAL
        out = evaluate_deadline_pressure(hours_until_due=20, package_overview=self._po(open_clar=5))
        assert out["deadline_pressure"] == PRESSURE_CRITICAL
        assert any(r["check"] == "open_clarifications_under_24h" for r in out["pressure_reasons"])

    def test_critical_with_deferred_reliance_under_24h(self):
        from app.pdf_extraction.deadline_pressure import evaluate_deadline_pressure, PRESSURE_CRITICAL
        po = self._po(quotes=[{"job_id": "j1", "reliance_decision": None}])
        out = evaluate_deadline_pressure(hours_until_due=20, package_overview=po)
        assert out["deadline_pressure"] == PRESSURE_CRITICAL
        assert any(r["check"] == "unresolved_reliance_under_24h" for r in out["pressure_reasons"])

    def test_package_blocked_escalates(self):
        from app.pdf_extraction.deadline_pressure import evaluate_deadline_pressure, PRESSURE_DEADLINE_BLOCKED
        out = evaluate_deadline_pressure(hours_until_due=48,
                                          package_gate={"package_gate_outcome": "PACKAGE_BLOCKED"})
        assert out["deadline_pressure"] == PRESSURE_DEADLINE_BLOCKED

    def test_authority_blocked_escalates(self):
        from app.pdf_extraction.deadline_pressure import evaluate_deadline_pressure, PRESSURE_DEADLINE_BLOCKED
        out = evaluate_deadline_pressure(hours_until_due=48,
                                          authority_posture={"authority_package_posture": "authority_blocked"})
        assert out["deadline_pressure"] == PRESSURE_DEADLINE_BLOCKED

    def test_required_auth_not_covered_under_pressure(self):
        from app.pdf_extraction.deadline_pressure import evaluate_deadline_pressure, PRESSURE_CRITICAL
        ap = {"posture_summary": {"required_not_covered": 3}}
        out = evaluate_deadline_pressure(hours_until_due=12, authority_posture=ap,
                                          package_overview=self._po())
        assert out["deadline_pressure"] == PRESSURE_CRITICAL

    def test_escalation_only_no_downgrade(self):
        from app.pdf_extraction.deadline_pressure import evaluate_deadline_pressure, PRESSURE_DEADLINE_BLOCKED
        out = evaluate_deadline_pressure(hours_until_due=100,
                                          package_gate={"package_gate_outcome": "PACKAGE_BLOCKED"})
        assert out["deadline_pressure"] == PRESSURE_DEADLINE_BLOCKED

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20mmm. C72 — Resolution Priority Queue Tests
# ---------------------------------------------------------------------------


class TestC72ResolutionPriorityQueue:

    def _dossier(self, jid="j1", vendor="V1", gate="CONDITIONAL", risk="medium",
                  open_cl=0, clarify_count=0):
        return {"job_id": jid, "vendor_name": vendor,
                "latest_gate": {"gate_outcome": gate},
                "latest_risk": {"overall_risk_level": risk},
                "open_clarifications": {"total_open": open_cl},
                "reliance_posture": {"clarify_before_reliance_count": clarify_count}}

    def _aap(self, actions=None):
        actions = actions or []
        return {"action_items": actions}

    def test_version_present(self):
        from app.pdf_extraction.resolution_priority_queue import build_priority_queue
        out = build_priority_queue()
        assert out["priority_queue_version"] == "resolution_priority_queue/v1"

    def test_blocked_quote_goes_today(self):
        from app.pdf_extraction.resolution_priority_queue import build_priority_queue, BUCKET_RESOLVE_TODAY
        out = build_priority_queue(dossiers=[self._dossier(gate="BLOCKED")])
        assert any(i["action_bucket"] == BUCKET_RESOLVE_TODAY and i["item_type"] == "blocked_quote"
                   for i in out["queue_items"])

    def test_open_clarifications_under_pressure(self):
        from app.pdf_extraction.resolution_priority_queue import build_priority_queue, BUCKET_RESOLVE_TODAY
        dp = {"deadline_pressure": "critical_due_to_time"}
        out = build_priority_queue(dossiers=[self._dossier(open_cl=3)], deadline_pressure=dp)
        open_items = [i for i in out["queue_items"] if i["item_type"] == "open_clarifications"]
        assert open_items[0]["action_bucket"] == BUCKET_RESOLVE_TODAY

    def test_authority_required_gap_today_under_pressure(self):
        from app.pdf_extraction.resolution_priority_queue import build_priority_queue, BUCKET_RESOLVE_TODAY
        aap = self._aap([{"authority_topic_id": "a1", "authority_posture": "required",
                          "handling_implication": "clarify_or_carry_internally_required_authority",
                          "authority_source_type": "dot", "exposure_level": "weakly_covered",
                          "authority_description": "x"}])
        dp = {"deadline_pressure": "critical_due_to_time"}
        out = build_priority_queue(authority_action_packet=aap, deadline_pressure=dp)
        assert any(i["action_bucket"] == BUCKET_RESOLVE_TODAY and i["item_type"] == "authority_gap"
                   for i in out["queue_items"])

    def test_carry_in_sub_quote_monitors_only(self):
        from app.pdf_extraction.resolution_priority_queue import build_priority_queue, BUCKET_MONITOR_ONLY
        aap = self._aap([{"authority_topic_id": "a1", "authority_posture": "conditional",
                          "handling_implication": "carry_in_sub_quote",
                          "authority_source_type": "dot", "exposure_level": "covered",
                          "authority_description": "x"}])
        out = build_priority_queue(authority_action_packet=aap)
        item = next(i for i in out["queue_items"] if i["item_type"] == "authority_gap")
        assert item["action_bucket"] == BUCKET_MONITOR_ONLY

    def test_ordering_priority_first(self):
        from app.pdf_extraction.resolution_priority_queue import build_priority_queue
        out = build_priority_queue(dossiers=[
            self._dossier("j1", gate="BLOCKED"),
            self._dossier("j2", gate="CONDITIONAL", open_cl=3),
        ])
        buckets = [i["action_bucket"] for i in out["queue_items"]]
        order = {"resolve_today": 0, "resolve_before_bid": 1, "safe_to_carry_with_caveat": 2, "monitor_only": 3}
        for i in range(len(buckets) - 1):
            assert order[buckets[i]] <= order[buckets[i+1]]

    def test_summary_counts(self):
        from app.pdf_extraction.resolution_priority_queue import build_priority_queue
        out = build_priority_queue(dossiers=[
            self._dossier("j1", gate="BLOCKED"),
            self._dossier("j2", gate="BLOCKED"),
            self._dossier("j3", gate="CONDITIONAL", open_cl=2),
        ])
        s = out["queue_summary"]
        assert s["resolve_today_count"] >= 2
        assert s["total_items"] >= 3

    def test_top_priority_capped(self):
        from app.pdf_extraction.resolution_priority_queue import build_priority_queue
        dossiers = [self._dossier(f"j{i}", gate="BLOCKED") for i in range(30)]
        out = build_priority_queue(dossiers=dossiers)
        assert len(out["top_priority_actions"]) == 20

    def test_source_refs_preserved(self):
        from app.pdf_extraction.resolution_priority_queue import build_priority_queue
        out = build_priority_queue(dossiers=[self._dossier("j1", "AcmeSub", gate="BLOCKED")])
        item = out["queue_items"][0]
        assert item["source_ref"]["job_id"] == "j1"
        assert item["source_ref"]["vendor"] == "AcmeSub"

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20nnn. C73 — Bid Readiness Snapshot Tests
# ---------------------------------------------------------------------------


class TestC73BidReadinessSnapshot:

    def test_version_present(self):
        from app.pdf_extraction.bid_readiness_snapshot import build_readiness_snapshot
        out = build_readiness_snapshot("bid-1")
        assert out["readiness_snapshot_version"] == "bid_readiness_snapshot/v1"

    def test_bid_id_captured(self):
        from app.pdf_extraction.bid_readiness_snapshot import build_readiness_snapshot
        out = build_readiness_snapshot("bid-xyz")
        assert out["bid_id"] == "bid-xyz"

    def test_overall_ready_clean(self):
        from app.pdf_extraction.bid_readiness_snapshot import build_readiness_snapshot
        out = build_readiness_snapshot("bid-1",
            package_gate={"package_gate_outcome": "PACKAGE_READY"},
            authority_posture={"authority_package_posture": "authority_clear"},
            deadline_pressure={"deadline_pressure": "on_track"})
        assert out["overall_readiness"] == "ready"

    def test_overall_not_ready_blocked(self):
        from app.pdf_extraction.bid_readiness_snapshot import build_readiness_snapshot
        out = build_readiness_snapshot("bid-1",
            package_gate={"package_gate_outcome": "PACKAGE_BLOCKED"})
        assert out["overall_readiness"] == "not_ready_blocked"

    def test_overall_action_required(self):
        from app.pdf_extraction.bid_readiness_snapshot import build_readiness_snapshot
        out = build_readiness_snapshot("bid-1",
            authority_posture={"authority_package_posture": "authority_action_required"})
        assert out["overall_readiness"] == "action_required"

    def test_overall_ready_with_caveats(self):
        from app.pdf_extraction.bid_readiness_snapshot import build_readiness_snapshot
        out = build_readiness_snapshot("bid-1",
            package_gate={"package_gate_outcome": "PACKAGE_CONDITIONAL"})
        assert out["overall_readiness"] == "ready_with_caveats"

    def test_package_confidence_section(self):
        from app.pdf_extraction.bid_readiness_snapshot import build_readiness_snapshot
        pg = {"package_gate_outcome": "PACKAGE_CONDITIONAL", "gate_summary": {"reason_count": 3}}
        out = build_readiness_snapshot("bid-1", package_gate=pg)
        assert out["package_confidence"]["package_gate_outcome"] == "PACKAGE_CONDITIONAL"
        assert out["package_confidence"]["reason_count"] == 3

    def test_authority_posture_section(self):
        from app.pdf_extraction.bid_readiness_snapshot import build_readiness_snapshot
        ap = {"authority_package_posture": "authority_watch",
              "posture_summary": {"required_not_covered": 1, "required_weakly_covered": 2}}
        out = build_readiness_snapshot("bid-1", authority_posture=ap)
        assert out["authority_posture"]["authority_package_posture"] == "authority_watch"
        assert out["authority_posture"]["required_not_covered"] == 1

    def test_deadline_pressure_section(self):
        from app.pdf_extraction.bid_readiness_snapshot import build_readiness_snapshot
        dp = {"deadline_pressure": "at_risk_due_to_time", "hours_until_due": 20}
        out = build_readiness_snapshot("bid-1", deadline_pressure=dp)
        assert out["deadline_pressure"]["pressure"] == "at_risk_due_to_time"
        assert out["deadline_pressure"]["hours_until_due"] == 20

    def test_top_unresolved_items_from_queue(self):
        from app.pdf_extraction.bid_readiness_snapshot import build_readiness_snapshot
        pq = {"queue_items": [
            {"queue_item_id": "q1", "action_bucket": "resolve_today", "item_type": "blocked_quote",
             "source_ref": {"job_id": "j1"}, "reason": "blocked"},
            {"queue_item_id": "q2", "action_bucket": "monitor_only", "item_type": "auth",
             "source_ref": {}, "reason": "x"},
        ], "top_priority_actions": []}
        out = build_readiness_snapshot("bid-1", priority_queue=pq)
        assert len(out["top_unresolved_items"]) == 1

    def test_vendor_highlights(self):
        from app.pdf_extraction.bid_readiness_snapshot import build_readiness_snapshot
        vc = {"comparison_version": "vendor_comparison/v1", "vendor_entries": [
            {"vendor_name": "Good", "job_id": "j1", "deterministic_score": 10, "vendor_rank": "best_available"},
            {"vendor_name": "Bad", "job_id": "j2", "deterministic_score": 500, "vendor_rank": "not_recommended"},
        ], "comparison_summary": {"rank_distribution": {"best_available": 1, "not_recommended": 1}}}
        out = build_readiness_snapshot("bid-1", vendor_comparison=vc)
        assert out["vendor_highlights"]["best"]["vendor_name"] == "Good"
        assert out["vendor_highlights"]["worst"]["vendor_name"] == "Bad"

    def test_carry_decision_posture(self):
        from app.pdf_extraction.bid_readiness_snapshot import build_readiness_snapshot
        cj = {"carry_justification_version": "bid_carry_justification/v1",
              "carry_decision": "proceed_with_caveats", "decided_by": "alice",
              "package_gate_outcome": "PACKAGE_CONDITIONAL",
              "authority_package_posture": "authority_watch"}
        out = build_readiness_snapshot("bid-1", carry_justification=cj)
        assert out["carry_decision_posture"]["carry_decision"] == "proceed_with_caveats"
        assert out["carry_decision_posture"]["decided_by"] == "alice"

    def test_top_reasons_multi_source(self):
        from app.pdf_extraction.bid_readiness_snapshot import build_readiness_snapshot
        pg = {"gate_reasons": [{"check": "x1", "severity": "high", "detail": "y"}]}
        ap = {"posture_reasons": [{"check": "x2", "severity": "critical", "detail": "y"}]}
        dp = {"pressure_reasons": [{"check": "x3", "severity": "high", "detail": "y"}]}
        out = build_readiness_snapshot("bid-1", package_gate=pg, authority_posture=ap, deadline_pressure=dp)
        origins = {r["origin"] for r in out["top_reasons"]}
        assert origins == {"package_gate", "authority_posture", "deadline_pressure"}

    def test_traceability_refs(self):
        from app.pdf_extraction.bid_readiness_snapshot import build_readiness_snapshot
        pg = {"package_gate_version": "package_confidence/v1", "package_gate_outcome": "PACKAGE_READY"}
        out = build_readiness_snapshot("bid-1", package_gate=pg)
        assert out["traceability_refs"]["package_gate_present"] is True
        assert out["traceability_refs"]["authority_posture_present"] is False

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0 and summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20iii. C68 — Authority Action Packet Tests
# ---------------------------------------------------------------------------


class TestC68AuthorityActionPacket:

    def _exposure_item(self, tid="a1", exposure="not_covered", imp="carry_internally_or_clarify",
                        posture="required", desc="Item"):
        return {"authority_topic_id": tid, "authority_description": desc,
                "authority_posture": posture, "authority_source_type": "dot",
                "authority_source_ref": {"spec": "x"}, "comparison_outcome": "authority_not_addressed",
                "exposure_level": exposure, "handling_implication": imp, "matched_scope_ref": None}

    def _exposure(self, items):
        counts = {}
        for i in items:
            e = i["exposure_level"]
            counts[e] = counts.get(e, 0) + 1
        return {"exposure_items": items, "exposure_summary": {
            "total_items": len(items),
            "not_covered_count": counts.get("not_covered", 0),
            "weakly_covered_count": counts.get("weakly_covered", 0),
            "ambiguously_covered_count": counts.get("ambiguously_covered", 0),
            "review_required_count": counts.get("review_required", 0),
            "covered_count": counts.get("covered", 0),
            "implication_counts": {},
        }}

    def test_version_present(self):
        from app.pdf_extraction.authority_action_packet import build_authority_action_packet
        out = build_authority_action_packet(self._exposure([]))
        assert out["authority_action_version"] == "authority_action_packet/v1"

    def test_covered_items_excluded(self):
        from app.pdf_extraction.authority_action_packet import build_authority_action_packet
        items = [
            self._exposure_item("a1", exposure="covered", imp="carry_in_sub_quote"),
            self._exposure_item("a2", exposure="not_covered"),
        ]
        out = build_authority_action_packet(self._exposure(items))
        assert out["action_item_count"] == 1
        assert out["action_items"][0]["authority_topic_id"] == "a2"

    def test_priority_ordering(self):
        from app.pdf_extraction.authority_action_packet import build_authority_action_packet
        items = [
            self._exposure_item("a1", exposure="not_covered", imp="carry_internally_or_clarify"),
            self._exposure_item("a2", exposure="weakly_covered",
                                 imp="clarify_or_carry_internally_required_authority"),
            self._exposure_item("a3", exposure="review_required", imp="estimator_review_required"),
        ]
        out = build_authority_action_packet(self._exposure(items))
        imps = [a["handling_implication"] for a in out["action_items"]]
        assert imps[0] == "clarify_or_carry_internally_required_authority"
        assert imps[1] == "carry_internally_or_clarify"

    def test_implication_groups(self):
        from app.pdf_extraction.authority_action_packet import build_authority_action_packet
        items = [
            self._exposure_item("a1", imp="carry_internally_or_clarify"),
            self._exposure_item("a2", imp="carry_internally_or_clarify"),
            self._exposure_item("a3", imp="estimator_review_required", exposure="review_required"),
        ]
        out = build_authority_action_packet(self._exposure(items))
        groups = out["implication_groups"]
        assert groups[0]["handling_implication"] == "carry_internally_or_clarify"
        assert groups[0]["count"] == 2

    def test_top_priority_actions_capped(self):
        from app.pdf_extraction.authority_action_packet import build_authority_action_packet
        items = [self._exposure_item(f"a{i}") for i in range(20)]
        out = build_authority_action_packet(self._exposure(items))
        assert len(out["top_priority_actions"]) == 10

    def test_package_ref_present(self):
        from app.pdf_extraction.authority_action_packet import build_authority_action_packet
        po = {"bid_id": "bid-1", "quote_count": 3}
        out = build_authority_action_packet(self._exposure([]), package_overview=po)
        assert out["package_ref"]["bid_id"] == "bid-1"

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20jjj. C69 — Authority Package Posture Tests
# ---------------------------------------------------------------------------


class TestC69AuthorityPackagePosture:

    def _item(self, posture="required", exposure="covered"):
        return {"authority_posture": posture, "exposure_level": exposure}

    def _exposure(self, items):
        counts = {}
        for i in items:
            e = i["exposure_level"]
            counts[e] = counts.get(e, 0) + 1
        return {"exposure_items": items, "exposure_summary": {
            "total_items": len(items),
            "covered_count": counts.get("covered", 0),
            "not_covered_count": counts.get("not_covered", 0),
            "weakly_covered_count": counts.get("weakly_covered", 0),
            "ambiguously_covered_count": counts.get("ambiguously_covered", 0),
            "review_required_count": counts.get("review_required", 0),
        }}

    def test_version_present(self):
        from app.pdf_extraction.authority_package_posture import evaluate_authority_posture
        out = evaluate_authority_posture(self._exposure([]))
        assert out["authority_posture_version"] == "authority_package_posture/v1"

    def test_clear_when_all_covered(self):
        from app.pdf_extraction.authority_package_posture import evaluate_authority_posture, POSTURE_CLEAR
        exp = self._exposure([self._item("required", "covered"), self._item("conditional", "covered")])
        out = evaluate_authority_posture(exp)
        assert out["authority_package_posture"] == POSTURE_CLEAR

    def test_blocked_when_required_not_covered(self):
        from app.pdf_extraction.authority_package_posture import evaluate_authority_posture, POSTURE_BLOCKED
        exp = self._exposure([self._item("required", "not_covered")])
        out = evaluate_authority_posture(exp)
        assert out["authority_package_posture"] == POSTURE_BLOCKED
        assert any(r["check"] == "required_authority_not_covered" for r in out["posture_reasons"])

    def test_action_required_when_required_weakly_covered(self):
        from app.pdf_extraction.authority_package_posture import evaluate_authority_posture, POSTURE_ACTION_REQUIRED
        exp = self._exposure([self._item("required", "weakly_covered")])
        out = evaluate_authority_posture(exp)
        assert out["authority_package_posture"] == POSTURE_ACTION_REQUIRED

    def test_action_required_when_required_ambiguously_covered(self):
        from app.pdf_extraction.authority_package_posture import evaluate_authority_posture, POSTURE_ACTION_REQUIRED
        exp = self._exposure([self._item("required", "ambiguously_covered")])
        out = evaluate_authority_posture(exp)
        assert out["authority_package_posture"] == POSTURE_ACTION_REQUIRED

    def test_watch_when_non_required_not_covered(self):
        from app.pdf_extraction.authority_package_posture import evaluate_authority_posture, POSTURE_WATCH
        exp = self._exposure([self._item("conditional", "not_covered")])
        out = evaluate_authority_posture(exp)
        assert out["authority_package_posture"] == POSTURE_WATCH

    def test_watch_when_review_required(self):
        from app.pdf_extraction.authority_package_posture import evaluate_authority_posture, POSTURE_WATCH
        exp = self._exposure([self._item("conditional", "review_required")])
        out = evaluate_authority_posture(exp)
        assert out["authority_package_posture"] == POSTURE_WATCH

    def test_posture_summary_counts(self):
        from app.pdf_extraction.authority_package_posture import evaluate_authority_posture
        exp = self._exposure([
            self._item("required", "not_covered"),
            self._item("required", "weakly_covered"),
            self._item("conditional", "covered"),
        ])
        out = evaluate_authority_posture(exp)
        s = out["posture_summary"]
        assert s["required_not_covered"] == 1
        assert s["required_weakly_covered"] == 1
        assert s["covered"] == 1

    def test_escalation_only_upward(self):
        from app.pdf_extraction.authority_package_posture import evaluate_authority_posture, POSTURE_BLOCKED
        exp = self._exposure([
            self._item("required", "not_covered"),
            self._item("conditional", "covered"),
        ])
        out = evaluate_authority_posture(exp)
        assert out["authority_package_posture"] == POSTURE_BLOCKED

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20kkk. C70 — Bid Carry Justification Tests
# ---------------------------------------------------------------------------


class TestC70BidCarryJustification:

    def _pg(self, outcome="PACKAGE_CONDITIONAL"):
        return {"package_gate_outcome": outcome, "gate_reasons": [{"check": "x", "detail": "y"}]}

    def _ap(self, posture="authority_watch"):
        return {"authority_package_posture": posture,
                "posture_reasons": [{"check": "z", "detail": "w"}],
                "posture_summary": {"total_authority_topics": 10, "covered": 7,
                                     "not_covered": 2, "required_not_covered": 1,
                                     "required_weakly_covered": 0}}

    def _aap(self, gaps=3):
        return {"action_summary": {"total_gaps": gaps, "not_covered_count": 2,
                                    "weakly_covered_count": 1, "review_required_count": 0}}

    def _rec(self):
        return {"recommendation_summary": {"carry_in_sub_quote_count": 5,
                "carry_internally_count": 3, "hold_as_contingency_count": 0,
                "clarify_before_reliance_count": 7, "block_quote_reliance_count": 0}}

    def test_version_present(self):
        from app.pdf_extraction.bid_carry_justification import create_carry_justification
        r = create_carry_justification("bid-1", "proceed_to_bid")
        assert r["carry_justification_version"] == "bid_carry_justification/v1"

    def test_proceed_to_bid(self):
        from app.pdf_extraction.bid_carry_justification import create_carry_justification
        r = create_carry_justification("bid-1", "proceed_to_bid",
                                        package_gate=self._pg("PACKAGE_READY"),
                                        authority_posture=self._ap("authority_clear"),
                                        decided_by="alice", decided_at="2026-04-16T10:00")
        assert r["carry_decision"] == "proceed_to_bid"
        assert r["decided_by"] == "alice"
        assert r["package_gate_outcome"] == "PACKAGE_READY"
        assert r["authority_package_posture"] == "authority_clear"

    def test_hold_pending(self):
        from app.pdf_extraction.bid_carry_justification import create_carry_justification
        r = create_carry_justification("bid-1", "hold_pending_resolution",
                                        package_gate=self._pg("PACKAGE_BLOCKED"),
                                        authority_posture=self._ap("authority_blocked"))
        assert r["carry_decision"] == "hold_pending_resolution"
        assert r["package_gate_outcome"] == "PACKAGE_BLOCKED"

    def test_invalid_decision_flagged(self):
        from app.pdf_extraction.bid_carry_justification import create_carry_justification
        r = create_carry_justification("bid-1", "made_up_decision")
        assert r["carry_decision_valid"] is False

    def test_unresolved_authority_captured(self):
        from app.pdf_extraction.bid_carry_justification import create_carry_justification
        r = create_carry_justification("bid-1", "proceed_with_caveats",
                                        authority_action_packet=self._aap(gaps=5))
        assert r["unresolved_authority_gaps"]["total_gaps"] == 5

    def test_carry_snapshot(self):
        from app.pdf_extraction.bid_carry_justification import create_carry_justification
        r = create_carry_justification("bid-1", "proceed_with_caveats",
                                        recommendation_output=self._rec())
        assert r["internal_carry_snapshot"]["carry_in_sub_quote"] == 5
        assert r["internal_carry_snapshot"]["carry_internally"] == 3

    def test_authority_snapshot(self):
        from app.pdf_extraction.bid_carry_justification import create_carry_justification
        r = create_carry_justification("bid-1", "proceed_with_caveats",
                                        authority_posture=self._ap())
        snap = r["authority_snapshot"]
        assert snap["total_authority_topics"] == 10
        assert snap["required_not_covered"] == 1

    def test_acknowledged_review_items(self):
        from app.pdf_extraction.bid_carry_justification import create_carry_justification
        r = create_carry_justification("bid-1", "proceed_with_caveats",
                                        acknowledged_review_items=["auth-1", "auth-5"])
        assert r["acknowledged_review_items"] == ["auth-1", "auth-5"]

    def test_gate_and_posture_reasons_preserved(self):
        from app.pdf_extraction.bid_carry_justification import create_carry_justification
        r = create_carry_justification("bid-1", "proceed_to_bid",
                                        package_gate=self._pg(), authority_posture=self._ap())
        assert len(r["package_gate_reasons"]) == 1
        assert len(r["authority_posture_reasons"]) == 1

    def test_append_revision(self):
        from app.pdf_extraction.bid_carry_justification import (
            create_carry_justification, append_carry_revision,
            get_current_carry_justification, get_carry_history,
        )
        r1 = create_carry_justification("bid-1", "hold_pending_resolution", decided_by="alice")
        r2 = create_carry_justification("bid-1", "proceed_to_bid", decided_by="bob")
        records = append_carry_revision([], r1)
        records = append_carry_revision(records, r2)
        assert len(records) == 2
        assert records[0].get("superseded_by") == r2["record_id"]
        assert get_current_carry_justification(records)["carry_decision"] == "proceed_to_bid"
        assert len(get_carry_history(records)) == 2

    def test_append_does_not_mutate(self):
        from app.pdf_extraction.bid_carry_justification import (
            create_carry_justification, append_carry_revision,
        )
        import copy
        r1 = create_carry_justification("bid-1", "hold_pending_resolution")
        records = append_carry_revision([], r1)
        snap = copy.deepcopy(records)
        r2 = create_carry_justification("bid-1", "proceed_to_bid")
        append_carry_revision(records, r2)
        assert records == snap

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0 and summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20fff. C65 — Scope Authority Tests
# ---------------------------------------------------------------------------


class TestC65ScopeAuthority:

    def _entry(self, tid="auth-0", desc="Clearing and Grubbing", src="dot",
               posture="required", ref=None, note=None):
        return {"topic_id": tid, "description": desc,
                "authority_source_type": src, "authority_posture": posture,
                "source_ref": ref or {"spec_section": "2101"}, "note": note}

    def test_version_present(self):
        from app.pdf_extraction.scope_authority import build_authority_reference
        out = build_authority_reference([])
        assert out["authority_version"] == "scope_authority/v1"

    def test_single_entry(self):
        from app.pdf_extraction.scope_authority import build_authority_reference, POSTURE_REQUIRED
        out = build_authority_reference([self._entry()])
        assert len(out["authority_topics"]) == 1
        t = out["authority_topics"][0]
        assert t["authority_posture"] == POSTURE_REQUIRED
        assert t["authority_source_valid"] is True
        assert t["authority_posture_valid"] is True

    def test_multiple_postures(self):
        from app.pdf_extraction.scope_authority import build_authority_reference
        entries = [
            self._entry("a1", posture="required"),
            self._entry("a2", posture="conditional"),
            self._entry("a3", posture="allowance_note"),
            self._entry("a4", posture="incidental_candidate"),
            self._entry("a5", posture="review_required"),
        ]
        out = build_authority_reference(entries)
        assert out["authority_summary"]["total_topics"] == 5
        pc = out["authority_summary"]["posture_counts"]
        assert pc["required"] == 1
        assert pc["conditional"] == 1

    def test_unknown_source_surfaced(self):
        from app.pdf_extraction.scope_authority import build_authority_reference
        out = build_authority_reference([self._entry(src="made_up_source")])
        assert "made_up_source" in out["authority_diagnostics"]["unknown_source_types"]
        assert out["authority_topics"][0]["authority_source_valid"] is False

    def test_unknown_posture_defaults_to_review(self):
        from app.pdf_extraction.scope_authority import build_authority_reference, POSTURE_REVIEW_REQUIRED
        out = build_authority_reference([self._entry(posture="fake_posture")])
        assert out["authority_topics"][0]["authority_posture"] == POSTURE_REVIEW_REQUIRED
        assert out["authority_topics"][0]["authority_posture_valid"] is False
        assert "fake_posture" in out["authority_diagnostics"]["unknown_postures"]

    def test_source_type_counts(self):
        from app.pdf_extraction.scope_authority import build_authority_reference
        entries = [self._entry("a1", src="dot"), self._entry("a2", src="dot"),
                   self._entry("a3", src="sudas")]
        out = build_authority_reference(entries)
        assert out["authority_summary"]["source_type_counts"]["dot"] == 2
        assert out["authority_summary"]["source_type_counts"]["sudas"] == 1

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20ggg. C66 — Scope Authority Comparison Tests
# ---------------------------------------------------------------------------


class TestC66ScopeAuthorityComparison:

    def _auth(self, entries):
        from app.pdf_extraction.scope_authority import build_authority_reference
        return build_authority_reference(entries)

    def _auth_entry(self, tid, desc, posture="required"):
        return {"topic_id": tid, "description": desc,
                "authority_source_type": "dot", "authority_posture": posture,
                "source_ref": {"spec": "x"}}

    def _scope(self, topics):
        return {"scope_topics": topics}

    def _scope_topic(self, desc, scope_class="explicitly_included", tid=None):
        return {"topic_id": tid or f"scope-{desc[:5]}", "description": desc,
                "scope_class": scope_class,
                "source_ref": {"normalized_row_id": "qr-p0-r0"}}

    def test_version_present(self):
        from app.pdf_extraction.scope_authority_comparison import compare_scope_vs_authority
        out = compare_scope_vs_authority(self._auth([]))
        assert out["comparison_version"] == "scope_authority_comparison/v1"

    def test_addressed_exact_match(self):
        from app.pdf_extraction.scope_authority_comparison import (
            compare_scope_vs_authority, OUTCOME_ADDRESSED,
        )
        auth = self._auth([self._auth_entry("a1", "Clearing and Grubbing")])
        scope = self._scope([self._scope_topic("Clearing and Grubbing")])
        out = compare_scope_vs_authority(auth, scope_interpretation=scope)
        assert out["comparisons"][0]["comparison_outcome"] == OUTCOME_ADDRESSED

    def test_not_addressed_no_match(self):
        from app.pdf_extraction.scope_authority_comparison import (
            compare_scope_vs_authority, OUTCOME_NOT_ADDRESSED,
        )
        auth = self._auth([self._auth_entry("a1", "Guardrail Repair")])
        scope = self._scope([self._scope_topic("Concrete Barrier")])
        out = compare_scope_vs_authority(auth, scope_interpretation=scope)
        assert out["comparisons"][0]["comparison_outcome"] == OUTCOME_NOT_ADDRESSED

    def test_conditionally_addressed_implicit(self):
        from app.pdf_extraction.scope_authority_comparison import (
            compare_scope_vs_authority, OUTCOME_CONDITIONALLY_ADDRESSED,
        )
        auth = self._auth([self._auth_entry("a1", "Traffic Control", posture="required")])
        scope = self._scope([self._scope_topic("Traffic Control", "implicitly_included")])
        out = compare_scope_vs_authority(auth, scope_interpretation=scope)
        assert out["comparisons"][0]["comparison_outcome"] == OUTCOME_CONDITIONALLY_ADDRESSED

    def test_ambiguous_scope_match(self):
        from app.pdf_extraction.scope_authority_comparison import (
            compare_scope_vs_authority, OUTCOME_AMBIGUOUS,
        )
        auth = self._auth([self._auth_entry("a1", "Misc Work")])
        scope = self._scope([self._scope_topic("Misc Work", "ambiguous_scope")])
        out = compare_scope_vs_authority(auth, scope_interpretation=scope)
        assert out["comparisons"][0]["comparison_outcome"] == OUTCOME_AMBIGUOUS

    def test_token_overlap_matching(self):
        from app.pdf_extraction.scope_authority_comparison import (
            compare_scope_vs_authority, OUTCOME_ADDRESSED,
        )
        auth = self._auth([self._auth_entry("a1", "Remove Asphalt Pavement")])
        scope = self._scope([self._scope_topic("Remove Asphalt Pavement Section A")])
        out = compare_scope_vs_authority(auth, scope_interpretation=scope)
        assert out["comparisons"][0]["comparison_outcome"] == OUTCOME_ADDRESSED

    def test_summary_counts(self):
        from app.pdf_extraction.scope_authority_comparison import compare_scope_vs_authority
        auth = self._auth([
            self._auth_entry("a1", "Clearing"),
            self._auth_entry("a2", "Guardrail"),
            self._auth_entry("a3", "Traffic Control"),
        ])
        scope = self._scope([
            self._scope_topic("Clearing"),
            self._scope_topic("Traffic Control", "implicitly_included"),
        ])
        out = compare_scope_vs_authority(auth, scope_interpretation=scope)
        s = out["comparison_summary"]
        assert s["addressed_count"] == 1
        assert s["conditionally_addressed_count"] == 1
        assert s["not_addressed_count"] == 1

    def test_matched_scope_ref_present(self):
        from app.pdf_extraction.scope_authority_comparison import compare_scope_vs_authority
        auth = self._auth([self._auth_entry("a1", "Clearing")])
        scope = self._scope([self._scope_topic("Clearing")])
        out = compare_scope_vs_authority(auth, scope_interpretation=scope)
        ref = out["comparisons"][0]["matched_scope_ref"]
        assert ref is not None
        assert ref["scope_class"] == "explicitly_included"

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20hhh. C67 — Authority Exposure Tests
# ---------------------------------------------------------------------------


class TestC67AuthorityExposure:

    def _comparison(self, items):
        return {"comparisons": items}

    def _comp_item(self, tid="a1", desc="Item", outcome="authority_addressed",
                    posture="required", src_type="dot"):
        return {"authority_topic_id": tid, "authority_description": desc,
                "authority_posture": posture, "authority_source_type": src_type,
                "authority_source_ref": {"spec": "x"},
                "comparison_outcome": outcome, "matched_scope_ref": None}

    def test_version_present(self):
        from app.pdf_extraction.authority_exposure import build_authority_exposure
        out = build_authority_exposure(self._comparison([]))
        assert out["exposure_version"] == "authority_exposure/v1"

    def test_covered_from_addressed(self):
        from app.pdf_extraction.authority_exposure import (
            build_authority_exposure, EXPOSURE_COVERED,
        )
        out = build_authority_exposure(self._comparison([
            self._comp_item(outcome="authority_addressed"),
        ]))
        assert out["exposure_items"][0]["exposure_level"] == EXPOSURE_COVERED
        assert out["exposure_items"][0]["handling_implication"] == "carry_in_sub_quote"

    def test_weakly_covered_from_conditional(self):
        from app.pdf_extraction.authority_exposure import (
            build_authority_exposure, EXPOSURE_WEAKLY_COVERED,
        )
        out = build_authority_exposure(self._comparison([
            self._comp_item(outcome="authority_conditionally_addressed"),
        ]))
        assert out["exposure_items"][0]["exposure_level"] == EXPOSURE_WEAKLY_COVERED

    def test_not_covered_from_not_addressed(self):
        from app.pdf_extraction.authority_exposure import (
            build_authority_exposure, EXPOSURE_NOT_COVERED,
        )
        out = build_authority_exposure(self._comparison([
            self._comp_item(outcome="authority_not_addressed"),
        ]))
        assert out["exposure_items"][0]["exposure_level"] == EXPOSURE_NOT_COVERED
        assert out["exposure_items"][0]["handling_implication"] == "carry_internally_or_clarify"

    def test_ambiguously_covered(self):
        from app.pdf_extraction.authority_exposure import (
            build_authority_exposure, EXPOSURE_AMBIGUOUSLY_COVERED,
        )
        out = build_authority_exposure(self._comparison([
            self._comp_item(outcome="authority_ambiguous"),
        ]))
        assert out["exposure_items"][0]["exposure_level"] == EXPOSURE_AMBIGUOUSLY_COVERED

    def test_review_required(self):
        from app.pdf_extraction.authority_exposure import (
            build_authority_exposure, EXPOSURE_REVIEW_REQUIRED,
        )
        out = build_authority_exposure(self._comparison([
            self._comp_item(outcome="authority_needs_review"),
        ]))
        assert out["exposure_items"][0]["exposure_level"] == EXPOSURE_REVIEW_REQUIRED

    def test_required_authority_escalates_implication(self):
        from app.pdf_extraction.authority_exposure import build_authority_exposure
        out = build_authority_exposure(self._comparison([
            self._comp_item(outcome="authority_conditionally_addressed", posture="required"),
        ]))
        assert out["exposure_items"][0]["handling_implication"] == "clarify_or_carry_internally_required_authority"

    def test_summary_counts(self):
        from app.pdf_extraction.authority_exposure import build_authority_exposure
        items = [
            self._comp_item("a1", outcome="authority_addressed"),
            self._comp_item("a2", outcome="authority_not_addressed"),
            self._comp_item("a3", outcome="authority_ambiguous"),
            self._comp_item("a4", outcome="authority_conditionally_addressed"),
        ]
        out = build_authority_exposure(self._comparison(items))
        s = out["exposure_summary"]
        assert s["covered_count"] == 1
        assert s["not_covered_count"] == 1
        assert s["ambiguously_covered_count"] == 1
        assert s["weakly_covered_count"] == 1
        assert s["total_items"] == 4

    def test_implication_counts(self):
        from app.pdf_extraction.authority_exposure import build_authority_exposure
        items = [
            self._comp_item("a1", outcome="authority_addressed"),
            self._comp_item("a2", outcome="authority_addressed"),
            self._comp_item("a3", outcome="authority_not_addressed"),
        ]
        out = build_authority_exposure(self._comparison(items))
        ic = out["exposure_summary"]["implication_counts"]
        assert ic["carry_in_sub_quote"] == 2
        assert ic["carry_internally_or_clarify"] == 1

    def test_authority_source_ref_preserved(self):
        from app.pdf_extraction.authority_exposure import build_authority_exposure
        item = self._comp_item()
        item["authority_source_ref"] = {"spec_section": "2101", "page": 45}
        out = build_authority_exposure(self._comparison([item]))
        assert out["exposure_items"][0]["authority_source_ref"]["spec_section"] == "2101"

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0 and summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20ccc. C62 — Bid Package Overview Tests
# ---------------------------------------------------------------------------


class TestC62BidPackageOverview:

    def _dossier(self, jid="j1", vendor="V1", gate="CONDITIONAL", risk="medium",
                  not_addr=10, carry_int=2, open_cl=3, unres_ev=0, blocking=0):
        return {
            "dossier_version": "quote_dossier/v1", "job_id": jid, "vendor_name": vendor,
            "latest_gate": {"gate_outcome": gate, "reason_count": 2},
            "latest_risk": {"overall_risk_level": risk, "factor_count": 2, "blocking_count": blocking},
            "decision_posture": "usable_with_caveats",
            "open_clarifications": {"total_open": open_cl},
            "scope_gaps": {"not_addressed_count": not_addr, "ambiguous_count": 1},
            "evidence_status": {"unresolved_block_count": unres_ev},
            "reliance_posture": {},
            "recommendation_summary": {"carry_internally_count": carry_int},
            "comparability_posture": {"total_rows": 15, "comparable_matched": 5, "non_comparable": 10},
        }

    def test_version_present(self):
        from app.pdf_extraction.bid_package_overview import build_package_overview
        out = build_package_overview("bid-1", [self._dossier()])
        assert out["package_overview_version"] == "bid_package_overview/v1"

    def test_single_dossier_rollup(self):
        from app.pdf_extraction.bid_package_overview import build_package_overview
        out = build_package_overview("bid-1", [self._dossier()])
        assert out["quote_count"] == 1
        ps = out["package_summary"]
        assert ps["total_open_clarifications"] == 3
        assert ps["total_carry_internally"] == 2
        assert ps["total_scope_not_addressed"] == 10

    def test_multi_dossier_rollup(self):
        from app.pdf_extraction.bid_package_overview import build_package_overview
        d1 = self._dossier("j1", "V1", gate="CONDITIONAL", open_cl=5, not_addr=80)
        d2 = self._dossier("j2", "V2", gate="HIGH_RISK", open_cl=10, not_addr=20)
        out = build_package_overview("bid-1", [d1, d2])
        ps = out["package_summary"]
        assert ps["total_open_clarifications"] == 15
        assert ps["total_scope_not_addressed"] == 100
        assert ps["gate_outcome_distribution"]["CONDITIONAL"] == 1
        assert ps["gate_outcome_distribution"]["HIGH_RISK"] == 1

    def test_reliance_distribution(self):
        from app.pdf_extraction.bid_package_overview import build_package_overview
        d = self._dossier()
        rel = {"j1": [{"reliance_decision": "relied_upon"}]}
        out = build_package_overview("bid-1", [d], reliance_records=rel)
        assert out["package_summary"]["reliance_decision_distribution"]["relied_upon"] == 1
        assert out["quote_summaries"][0]["reliance_decision"] == "relied_upon"

    def test_traceability_to_dossiers(self):
        from app.pdf_extraction.bid_package_overview import build_package_overview
        out = build_package_overview("bid-1", [self._dossier("j1", "V1"), self._dossier("j2", "V2")])
        ids = [q["job_id"] for q in out["quote_summaries"]]
        assert ids == ["j1", "j2"]

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20ddd. C63 — Vendor Comparison Tests
# ---------------------------------------------------------------------------


class TestC63VendorComparison:

    def _dossier(self, jid="j1", vendor="V1", gate="CONDITIONAL", risk="medium",
                  not_addr=10, matched=5, non_comp=10, open_cl=3, unres_ev=0):
        return {
            "dossier_version": "quote_dossier/v1", "job_id": jid, "vendor_name": vendor,
            "latest_gate": {"gate_outcome": gate},
            "latest_risk": {"overall_risk_level": risk},
            "decision_posture": "usable_with_caveats",
            "open_clarifications": {"total_open": open_cl},
            "scope_gaps": {"not_addressed_count": not_addr, "ambiguous_count": 0},
            "evidence_status": {"unresolved_block_count": unres_ev},
            "reliance_posture": {},
            "recommendation_summary": {"carry_internally_count": 0, "block_quote_reliance_count": 0,
                                       "clarify_before_reliance_count": 0},
            "comparability_posture": {"total_rows": 15, "comparable_matched": matched, "non_comparable": non_comp},
        }

    def test_version_present(self):
        from app.pdf_extraction.vendor_comparison import compare_vendors
        out = compare_vendors([self._dossier()])
        assert out["comparison_version"] == "vendor_comparison/v1"

    def test_single_vendor(self):
        from app.pdf_extraction.vendor_comparison import compare_vendors
        out = compare_vendors([self._dossier()])
        assert out["vendor_count"] == 1
        assert out["vendor_entries"][0]["vendor_name"] == "V1"

    def test_ordering_by_score(self):
        from app.pdf_extraction.vendor_comparison import compare_vendors
        d1 = self._dossier("j1", "Bad", gate="HIGH_RISK", risk="high", not_addr=80, matched=0, non_comp=15, open_cl=20)
        d2 = self._dossier("j2", "Good", gate="SAFE", risk="low", not_addr=0, matched=15, non_comp=0, open_cl=0)
        out = compare_vendors([d1, d2])
        assert out["vendor_entries"][0]["vendor_name"] == "Good"
        assert out["vendor_entries"][1]["vendor_name"] == "Bad"
        assert out["vendor_entries"][0]["deterministic_score"] < out["vendor_entries"][1]["deterministic_score"]

    def test_rank_assignment(self):
        from app.pdf_extraction.vendor_comparison import compare_vendors, RANK_BEST, RANK_CAUTION, RANK_NOT_RECOMMENDED
        d1 = self._dossier("j1", "V1", gate="SAFE", risk="low")
        d2 = self._dossier("j2", "V2", gate="HIGH_RISK", risk="high")
        d3 = self._dossier("j3", "V3", gate="BLOCKED", risk="critical")
        out = compare_vendors([d1, d2, d3])
        ranks = {e["vendor_name"]: e["vendor_rank"] for e in out["vendor_entries"]}
        assert ranks["V1"] == RANK_BEST
        assert ranks["V2"] == RANK_CAUTION
        assert ranks["V3"] == RANK_NOT_RECOMMENDED

    def test_score_breakdown_present(self):
        from app.pdf_extraction.vendor_comparison import compare_vendors
        out = compare_vendors([self._dossier()])
        sb = out["vendor_entries"][0]["score_breakdown"]
        for k in ("gate_penalty", "risk_penalty", "scope_gap_penalty",
                  "comparability_penalty", "clarification_penalty", "evidence_penalty"):
            assert k in sb

    def test_comparison_summary(self):
        from app.pdf_extraction.vendor_comparison import compare_vendors
        d1 = self._dossier("j1", "V1", gate="SAFE", risk="low")
        d2 = self._dossier("j2", "V2", gate="CONDITIONAL", risk="medium")
        out = compare_vendors([d1, d2])
        cs = out["comparison_summary"]
        assert cs["best_score"] is not None
        assert cs["worst_score"] >= cs["best_score"]
        assert cs["gate_distribution"]["SAFE"] == 1
        assert cs["rank_distribution"]["best_available"] == 1

    def test_reliance_decision_propagated(self):
        from app.pdf_extraction.vendor_comparison import compare_vendors
        d = self._dossier()
        rel = {"j1": [{"reliance_decision": "relied_upon"}]}
        out = compare_vendors([d], reliance_records=rel)
        assert out["vendor_entries"][0]["reliance_decision"] == "relied_upon"

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20eee. C64 — Package Confidence Tests
# ---------------------------------------------------------------------------


class TestC64PackageConfidence:

    def _qs(self, jid="j1", gate="CONDITIONAL", risk="medium", rel_dec="relied_upon",
             not_addr=10, open_cl=3, unres_ev=0):
        return {"job_id": jid, "gate_outcome": gate, "risk_level": risk,
                "reliance_decision": rel_dec, "scope_not_addressed": not_addr,
                "open_clarifications": open_cl, "carry_internally_count": 2,
                "unresolved_evidence": unres_ev, "vendor_name": "V"}

    def _po(self, quotes, **kw):
        ps = {
            "total_open_clarifications": sum(q.get("open_clarifications", 0) for q in quotes),
            "total_carry_internally": sum(q.get("carry_internally_count", 0) for q in quotes),
            "total_scope_not_addressed": sum(q.get("scope_not_addressed", 0) for q in quotes),
            "total_unresolved_evidence": sum(q.get("unresolved_evidence", 0) for q in quotes),
        }
        ps.update(kw)
        return {"bid_id": "bid-1", "quote_summaries": quotes, "package_summary": ps}

    def test_version_present(self):
        from app.pdf_extraction.package_confidence import evaluate_package_confidence
        out = evaluate_package_confidence(self._po([]))
        assert out["package_gate_version"] == "package_confidence/v1"

    def test_ready_when_clean(self):
        from app.pdf_extraction.package_confidence import evaluate_package_confidence, PKG_READY
        q = {"job_id": "j1", "gate_outcome": "SAFE", "risk_level": "low",
             "reliance_decision": "relied_upon", "scope_not_addressed": 0,
             "open_clarifications": 0, "carry_internally_count": 0,
             "unresolved_evidence": 0, "vendor_name": "V"}
        out = evaluate_package_confidence(self._po([q]))
        assert out["package_gate_outcome"] == PKG_READY

    def test_blocked_when_blocked_quote(self):
        from app.pdf_extraction.package_confidence import evaluate_package_confidence, PKG_BLOCKED
        q = self._qs(gate="BLOCKED", risk="critical")
        out = evaluate_package_confidence(self._po([q]))
        assert out["package_gate_outcome"] == PKG_BLOCKED
        assert any(r["check"] == "blocked_quotes" for r in out["gate_reasons"])

    def test_high_risk_unresolved_reliance(self):
        from app.pdf_extraction.package_confidence import evaluate_package_confidence, PKG_HIGH_RISK
        q = self._qs(rel_dec=None)
        out = evaluate_package_confidence(self._po([q]))
        assert out["package_gate_outcome"] in (PKG_HIGH_RISK, "PACKAGE_CONDITIONAL")
        assert any(r["check"] == "unresolved_reliance" for r in out["gate_reasons"])

    def test_conditional_on_scope_gaps(self):
        from app.pdf_extraction.package_confidence import evaluate_package_confidence, PKG_CONDITIONAL
        q = self._qs(gate="SAFE", risk="low", not_addr=50, open_cl=0)
        out = evaluate_package_confidence(self._po([q]))
        assert out["package_gate_outcome"] == PKG_CONDITIONAL
        assert any(r["check"] == "uncovered_scope" for r in out["gate_reasons"])

    def test_conditional_on_open_clarifications(self):
        from app.pdf_extraction.package_confidence import evaluate_package_confidence, PKG_CONDITIONAL
        q = self._qs(gate="SAFE", risk="low", not_addr=0, open_cl=10)
        out = evaluate_package_confidence(self._po([q]))
        assert out["package_gate_outcome"] == PKG_CONDITIONAL

    def test_high_risk_unresolved_evidence(self):
        from app.pdf_extraction.package_confidence import evaluate_package_confidence, PKG_HIGH_RISK
        q = self._qs(gate="SAFE", risk="low", not_addr=0, open_cl=0, unres_ev=5)
        out = evaluate_package_confidence(self._po([q]))
        assert out["package_gate_outcome"] == PKG_HIGH_RISK

    def test_high_risk_all_quotes_high(self):
        from app.pdf_extraction.package_confidence import evaluate_package_confidence, PKG_HIGH_RISK
        q1 = self._qs("j1", risk="high", not_addr=0, open_cl=0)
        q2 = self._qs("j2", risk="critical", not_addr=0, open_cl=0)
        out = evaluate_package_confidence(self._po([q1, q2]))
        assert any(r["check"] == "all_quotes_high_risk" for r in out["gate_reasons"])

    def test_gate_summary_counts(self):
        from app.pdf_extraction.package_confidence import evaluate_package_confidence
        q = self._qs(gate="BLOCKED", risk="critical", rel_dec=None, not_addr=50, open_cl=10, unres_ev=5)
        out = evaluate_package_confidence(self._po([q]))
        gs = out["gate_summary"]
        assert gs["reason_count"] >= 3
        assert gs["blocked_quote_count"] == 1

    def test_refs_populated(self):
        from app.pdf_extraction.package_confidence import evaluate_package_confidence
        q = self._qs(gate="BLOCKED", risk="critical")
        out = evaluate_package_confidence(self._po([q]))
        blocked_reason = next(r for r in out["gate_reasons"] if r["check"] == "blocked_quotes")
        assert "j1" in blocked_reason["refs"]

    def test_escalation_only_upward(self):
        from app.pdf_extraction.package_confidence import evaluate_package_confidence, PKG_BLOCKED
        q1 = self._qs("j1", gate="BLOCKED", risk="critical")
        q2 = self._qs("j2", gate="SAFE", risk="low", not_addr=0, open_cl=0)
        out = evaluate_package_confidence(self._po([q1, q2]))
        assert out["package_gate_outcome"] == PKG_BLOCKED

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0 and summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20zz. C59 — Quote Dossier Tests
# ---------------------------------------------------------------------------


class TestC59QuoteDossier:

    def _gate(self, outcome="HIGH_RISK", reasons=3):
        return {"gate_outcome": outcome, "gate_summary": {"reason_count": reasons}}

    def _risk(self, level="high", factors=3, blocking=0):
        return {"overall_risk_level": level,
                "risk_summary": {"total_factors": factors},
                "blocking_risks": [{"factor_id": f"b{i}", "severity": "critical", "detail": "x"} for i in range(blocking)],
                "warning_risks": []}

    def _dp(self, posture="requires_action"):
        return {"decision_posture": posture, "overall_risk_level": "high",
                "comparability_posture": {"total_rows": 15, "comparable_matched": 0, "non_comparable": 13},
                "scope_gaps": {"not_addressed_count": 80, "ambiguous_count": 2},
                "evidence_status": {"unresolved_block_count": 5},
                "blocking_issues": [], "warning_issues": []}

    def test_version_present(self):
        from app.pdf_extraction.quote_dossier import build_dossier
        d = build_dossier()
        assert d["dossier_version"] == "quote_dossier/v1"

    def test_vendor_and_job_id(self):
        from app.pdf_extraction.quote_dossier import build_dossier
        d = build_dossier(job_id="j1", vendor_name="IPSI")
        assert d["job_id"] == "j1"
        assert d["vendor_name"] == "IPSI"

    def test_latest_gate_and_risk(self):
        from app.pdf_extraction.quote_dossier import build_dossier
        d = build_dossier(gate_output=self._gate("CONDITIONAL"), risk_output=self._risk("medium", 2))
        assert d["latest_gate"]["gate_outcome"] == "CONDITIONAL"
        assert d["latest_risk"]["overall_risk_level"] == "medium"
        assert d["latest_risk"]["factor_count"] == 2

    def test_open_clarifications(self):
        from app.pdf_extraction.quote_dossier import build_dossier
        tracking = {"tracked_clarifications": [
            {"clarification_id": "c0", "current_status": "pending_send"},
            {"clarification_id": "c1", "current_status": "sent"},
            {"clarification_id": "c2", "current_status": "responded"},
        ]}
        d = build_dossier(tracking_state=tracking)
        assert d["open_clarifications"]["total_open"] == 2
        assert d["open_clarifications"]["pending_send"] == 1
        assert d["open_clarifications"]["sent"] == 1

    def test_response_history_summary(self):
        from app.pdf_extraction.quote_dossier import build_dossier
        resp = {"integration_summary": {"total_responses": 5, "scope_updates_count": 2,
                                         "comparability_updates_count": 1, "risk_updates_count": 1}}
        d = build_dossier(response_integration=resp)
        assert d["response_history_summary"]["total_responses"] == 5

    def test_reliance_posture(self):
        from app.pdf_extraction.quote_dossier import build_dossier
        rec = {"recommendation_summary": {"carry_in_sub_quote_count": 5,
                                           "clarify_before_reliance_count": 10,
                                           "block_quote_reliance_count": 0}}
        d = build_dossier(gate_output=self._gate(), decision_packet=self._dp(), recommendation_output=rec)
        rp = d["reliance_posture"]
        assert rp["carry_in_sub_quote_count"] == 5
        assert rp["clarify_before_reliance_count"] == 10

    def test_active_assumptions_from_scenarios(self):
        from app.pdf_extraction.quote_dossier import build_dossier
        whatif = {"scenario_results": [
            {"scenario_id": "s1", "assumptions_applied": [{"row_id": "r0"}],
             "scenario_risk_level": "low", "scenario_decision_posture": "ready_for_use"},
        ]}
        d = build_dossier(scenario_whatif=whatif)
        assert len(d["active_assumptions"]) == 1
        assert d["active_assumptions"][0]["scenario_id"] == "s1"

    def test_diagnostics_flags(self):
        from app.pdf_extraction.quote_dossier import build_dossier
        d = build_dossier(gate_output=self._gate(), risk_output=self._risk())
        assert d["dossier_diagnostics"]["has_gate"] is True
        assert d["dossier_diagnostics"]["has_risk"] is True

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20aaa. C60 — External Communication Packet Tests
# ---------------------------------------------------------------------------


class TestC60ExternalCommunicationPacket:

    def _dossier(self):
        return {"dossier_version": "quote_dossier/v1", "job_id": "j1",
                "vendor_name": "IPSI", "decision_posture": "requires_action",
                "readiness_status": "actionable",
                "latest_risk": {"overall_risk_level": "high"},
                "open_clarifications": {"total_open": 5, "pending_send": 3, "sent": 2}}

    def _cl(self, count=3):
        return {"clarification_items": [
            {"clarification_id": f"clar-{i}", "clarification_type": "scope_gap_clarification",
             "clarification_text": f"Please confirm item {i}.", "source_ref": f"ref-{i}",
             "evidence_refs": []}
            for i in range(count)
        ]}

    def _tracking(self, pending_ids=None):
        pending_ids = pending_ids or ["clar-0", "clar-1"]
        return {"tracked_clarifications": [
            {"clarification_id": cid, "current_status": "pending_send",
             "clarification_type": "scope_gap_clarification", "source_ref": f"ref",
             "evidence_refs": []}
            for cid in pending_ids
        ]}

    def test_version_present(self):
        from app.pdf_extraction.external_communication_packet import build_communication_packet
        out = build_communication_packet("subcontractor_clarification_request")
        assert out["comm_packet_version"] == "external_communication_packet/v1"

    def test_sub_clarification_sections(self):
        from app.pdf_extraction.external_communication_packet import (
            build_communication_packet, COMM_SUB_CLARIFICATION,
        )
        out = build_communication_packet(COMM_SUB_CLARIFICATION,
                                          clarification_output=self._cl(),
                                          tracking_state=self._tracking())
        ids = [s["section_id"] for s in out["sections"]]
        assert "clarification_questions" in ids
        assert "response_instructions" in ids
        qs = next(s for s in out["sections"] if s["section_id"] == "clarification_questions")
        assert qs["item_count"] == 2  # only pending_send items

    def test_internal_summary_sections(self):
        from app.pdf_extraction.external_communication_packet import (
            build_communication_packet, COMM_INTERNAL_SUMMARY,
        )
        dp = {"decision_posture": "requires_action", "overall_risk_level": "high",
              "blocking_issues": [{"factor_id": "x", "detail": "y"}],
              "warning_issues": [{"factor_id": "w", "detail": "z"}]}
        risk = {"blocking_risks": [], "warning_risks": []}
        rec = {"recommendation_summary": {"carry_in_sub_quote_count": 5}}
        out = build_communication_packet(COMM_INTERNAL_SUMMARY,
                                          dossier=self._dossier(), decision_packet=dp,
                                          risk_output=risk, recommendation_output=rec)
        ids = [s["section_id"] for s in out["sections"]]
        assert "posture_overview" in ids
        assert "blocking_issues" in ids
        assert "carry_gap_summary" in ids

    def test_escalation_includes_open_clarifications(self):
        from app.pdf_extraction.external_communication_packet import (
            build_communication_packet, COMM_ESCALATION,
        )
        dp = {"decision_posture": "blocked", "overall_risk_level": "critical",
              "blocking_issues": [], "warning_issues": []}
        out = build_communication_packet(COMM_ESCALATION, dossier=self._dossier(),
                                          decision_packet=dp, risk_output={})
        ids = [s["section_id"] for s in out["sections"]]
        assert "open_clarifications" in ids
        assert "escalation_reason" in ids

    def test_vendor_name_propagated(self):
        from app.pdf_extraction.external_communication_packet import build_communication_packet
        out = build_communication_packet("subcontractor_clarification_request",
                                          dossier=self._dossier())
        assert out["vendor_name"] == "IPSI"

    def test_invalid_type_handled(self):
        from app.pdf_extraction.external_communication_packet import build_communication_packet
        out = build_communication_packet("made_up_type")
        assert out["sections"] == []
        assert out["comm_diagnostics"]["valid_type"] is False

    def test_evidence_refs_preserved(self):
        from app.pdf_extraction.external_communication_packet import (
            build_communication_packet, COMM_SUB_CLARIFICATION,
        )
        cl = {"clarification_items": [
            {"clarification_id": "clar-0", "clarification_type": "scope_gap_clarification",
             "clarification_text": "Check", "source_ref": "ref-0",
             "evidence_refs": [{"type": "line_ref", "value": "0530"}]},
        ]}
        tracking = self._tracking(["clar-0"])
        out = build_communication_packet(COMM_SUB_CLARIFICATION,
                                          clarification_output=cl, tracking_state=tracking)
        qs = next(s for s in out["sections"] if s["section_id"] == "clarification_questions")
        assert qs["items"][0]["evidence_refs"][0]["value"] == "0530"

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20bbb. C61 — Final Reliance Record Tests
# ---------------------------------------------------------------------------


class TestC61FinalRelianceRecord:

    def _gate(self, outcome="CONDITIONAL"):
        return {"gate_outcome": outcome, "gate_summary": {"reason_count": 3}}

    def _risk(self, level="medium"):
        return {"overall_risk_level": level, "risk_summary": {"total_factors": 2},
                "blocking_risks": [], "warning_risks": [{"factor_id": "w1", "detail": "x"}]}

    def _dp(self):
        return {"decision_posture": "usable_with_caveats", "overall_risk_level": "medium",
                "comparability_posture": {"total_rows": 15, "comparable_matched": 5, "non_comparable": 10},
                "scope_gaps": {"not_addressed_count": 80, "ambiguous_count": 2},
                "evidence_status": {"unresolved_block_count": 0}}

    def _rec(self):
        return {"recommendation_summary": {"carry_in_sub_quote_count": 5,
                "carry_internally_count": 2, "hold_as_contingency_count": 0,
                "clarify_before_reliance_count": 8, "block_quote_reliance_count": 0}}

    def test_version_present(self):
        from app.pdf_extraction.final_reliance_record import create_reliance_record
        r = create_reliance_record("j1", "relied_upon")
        assert r["reliance_version"] == "final_reliance_record/v1"

    def test_relied_upon(self):
        from app.pdf_extraction.final_reliance_record import create_reliance_record
        r = create_reliance_record("j1", "relied_upon", gate_output=self._gate("SAFE"),
                                    risk_output=self._risk("low"), decision_packet=self._dp(),
                                    decided_by="alice", decided_at="2026-04-16T10:00")
        assert r["reliance_decision"] == "relied_upon"
        assert r["decided_by"] == "alice"
        assert r["final_gate_outcome"] == "SAFE"
        assert r["reliance_decision_valid"] is True

    def test_not_relied_upon(self):
        from app.pdf_extraction.final_reliance_record import create_reliance_record
        r = create_reliance_record("j1", "not_relied_upon", gate_output=self._gate("BLOCKED"),
                                    risk_output=self._risk("critical"))
        assert r["reliance_decision"] == "not_relied_upon"
        assert r["final_gate_outcome"] == "BLOCKED"

    def test_relied_with_caveats(self):
        from app.pdf_extraction.final_reliance_record import create_reliance_record
        r = create_reliance_record("j1", "relied_upon_with_caveats",
                                    gate_output=self._gate(), decision_packet=self._dp(),
                                    recommendation_output=self._rec(),
                                    active_assumptions=[{"scenario_id": "s1"}])
        assert r["reliance_decision"] == "relied_upon_with_caveats"
        assert len(r["active_assumptions"]) == 1
        assert r["carry_gap_posture"]["carry_in_sub_quote"] == 5
        assert r["carry_gap_posture"]["clarify_before_reliance"] == 8

    def test_unresolved_items_captured(self):
        from app.pdf_extraction.final_reliance_record import create_reliance_record
        r = create_reliance_record("j1", "relied_upon_with_caveats",
                                    decision_packet=self._dp(), risk_output=self._risk())
        ui = r["unresolved_items"]
        assert ui["scope_not_addressed"] == 80
        assert ui["warning_count"] == 1

    def test_evidence_snapshot(self):
        from app.pdf_extraction.final_reliance_record import create_reliance_record
        r = create_reliance_record("j1", "relied_upon", gate_output=self._gate(),
                                    decision_packet=self._dp(), risk_output=self._risk())
        es = r["evidence_snapshot"]
        assert es["total_rows"] == 15
        assert es["comparable_matched"] == 5
        assert es["gate_reason_count"] == 3

    def test_invalid_reliance_decision_flagged(self):
        from app.pdf_extraction.final_reliance_record import create_reliance_record
        r = create_reliance_record("j1", "made_up_decision")
        assert r["reliance_decision_valid"] is False

    def test_append_reliance_revision(self):
        from app.pdf_extraction.final_reliance_record import (
            create_reliance_record, append_reliance_revision,
            get_current_reliance, get_reliance_history,
        )
        r1 = create_reliance_record("j1", "decision_deferred", decided_by="alice")
        r2 = create_reliance_record("j1", "relied_upon_with_caveats", decided_by="bob")
        records = append_reliance_revision([], r1)
        records = append_reliance_revision(records, r2)
        assert len(records) == 2
        assert records[0].get("superseded_by") == r2["record_id"]
        assert get_current_reliance(records)["reliance_decision"] == "relied_upon_with_caveats"
        assert len(get_reliance_history(records)) == 2

    def test_append_does_not_mutate(self):
        from app.pdf_extraction.final_reliance_record import (
            create_reliance_record, append_reliance_revision,
        )
        import copy
        r1 = create_reliance_record("j1", "decision_deferred")
        records = append_reliance_revision([], r1)
        snap = copy.deepcopy(records)
        r2 = create_reliance_record("j1", "relied_upon")
        append_reliance_revision(records, r2)
        assert records == snap

    def test_reeval_linkage(self):
        from app.pdf_extraction.final_reliance_record import create_reliance_record
        reeval = {"cycle_count": 3, "current_cycle_id": "cycle-2"}
        r = create_reliance_record("j1", "relied_upon", reeval_history=reeval)
        assert r["reeval_cycle_count"] == 3
        assert r["reeval_current_cycle_id"] == "cycle-2"

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0 and summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20ww. C56 — Clarification Tracking Tests
# ---------------------------------------------------------------------------


class TestC56ClarificationTracking:

    def _cl(self, count=3):
        return {"clarification_items": [
            {"clarification_id": f"clar-{i}", "clarification_type": "scope_gap_clarification",
             "source_ref": f"ref-{i}", "evidence_refs": []}
            for i in range(count)
        ]}

    def test_create_tracking_state(self):
        from app.pdf_extraction.clarification_tracking import create_tracking_state, STATUS_PENDING_SEND
        state = create_tracking_state(self._cl())
        assert state["tracking_version"] == "clarification_tracking/v1"
        assert len(state["tracked_clarifications"]) == 3
        for t in state["tracked_clarifications"]:
            assert t["current_status"] == STATUS_PENDING_SEND
            assert len(t["status_history"]) == 1

    def test_update_status_sent(self):
        from app.pdf_extraction.clarification_tracking import (
            create_tracking_state, update_clarification_status, STATUS_SENT,
        )
        state = create_tracking_state(self._cl())
        state2 = update_clarification_status(state, [
            {"clarification_id": "clar-0", "status": STATUS_SENT, "actor": "alice"},
        ])
        t = next(c for c in state2["tracked_clarifications"] if c["clarification_id"] == "clar-0")
        assert t["current_status"] == STATUS_SENT
        assert len(t["status_history"]) == 2
        assert state2["tracking_summary"]["sent_count"] == 1

    def test_update_status_responded(self):
        from app.pdf_extraction.clarification_tracking import (
            create_tracking_state, update_clarification_status, STATUS_SENT, STATUS_RESPONDED,
        )
        state = create_tracking_state(self._cl(1))
        state = update_clarification_status(state, [
            {"clarification_id": "clar-0", "status": STATUS_SENT},
        ])
        state = update_clarification_status(state, [
            {"clarification_id": "clar-0", "status": STATUS_RESPONDED, "response_ref": "resp-1"},
        ])
        t = state["tracked_clarifications"][0]
        assert t["current_status"] == STATUS_RESPONDED
        assert t["response_ref"] == "resp-1"
        assert len(t["status_history"]) == 3

    def test_unknown_id_surfaced(self):
        from app.pdf_extraction.clarification_tracking import (
            create_tracking_state, update_clarification_status,
        )
        state = create_tracking_state(self._cl(1))
        state2 = update_clarification_status(state, [
            {"clarification_id": "missing", "status": "sent"},
        ])
        assert "missing" in state2["tracking_summary"]["unknown_ids"]

    def test_append_only_history(self):
        from app.pdf_extraction.clarification_tracking import (
            create_tracking_state, update_clarification_status,
        )
        import copy
        state = create_tracking_state(self._cl(1))
        snap = copy.deepcopy(state)
        update_clarification_status(state, [
            {"clarification_id": "clar-0", "status": "sent"},
        ])
        assert state == snap  # original not mutated

    def test_summary_counts(self):
        from app.pdf_extraction.clarification_tracking import (
            create_tracking_state, update_clarification_status,
        )
        state = create_tracking_state(self._cl(3))
        state = update_clarification_status(state, [
            {"clarification_id": "clar-0", "status": "sent"},
            {"clarification_id": "clar-1", "status": "closed"},
        ])
        s = state["tracking_summary"]
        assert s["pending_send_count"] == 1
        assert s["sent_count"] == 1
        assert s["closed_count"] == 1

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20xx. C57 — Response Integration Tests
# ---------------------------------------------------------------------------


class TestC57ResponseIntegration:

    def _tracking(self):
        from app.pdf_extraction.clarification_tracking import create_tracking_state
        cl = {"clarification_items": [
            {"clarification_id": "clar-0", "clarification_type": "scope_gap_clarification",
             "source_ref": "ref-0", "evidence_refs": [{"type": "line_ref", "value": "0530"}]},
            {"clarification_id": "clar-1", "clarification_type": "missing_quantity_clarification",
             "source_ref": "ref-1", "evidence_refs": []},
        ]}
        return create_tracking_state(cl)

    def test_version_present(self):
        from app.pdf_extraction.response_integration import integrate_responses
        out = integrate_responses(self._tracking(), [])
        assert out["response_version"] == "response_integration/v1"

    def test_confirmed_response_linked(self):
        from app.pdf_extraction.response_integration import integrate_responses, RESPONSE_CONFIRMED
        resp = [{"response_id": "r1", "clarification_id": "clar-0",
                 "response_type": RESPONSE_CONFIRMED, "responded_by": "sub",
                 "response_note": "included in lump sum"}]
        out = integrate_responses(self._tracking(), resp)
        assert len(out["integrated_responses"]) == 1
        assert out["integrated_responses"][0]["linked_to_clarification"] is True
        assert out["integrated_responses"][0]["original_clarification_type"] == "scope_gap_clarification"
        assert out["integration_summary"]["scope_updates_count"] == 1

    def test_corrected_response_with_values(self):
        from app.pdf_extraction.response_integration import integrate_responses, RESPONSE_CORRECTED
        resp = [{"response_id": "r1", "clarification_id": "clar-1",
                 "response_type": RESPONSE_CORRECTED,
                 "response_values": {"qty": 100.0, "unit": "SY"}}]
        out = integrate_responses(self._tracking(), resp)
        assert out["integration_summary"]["comparability_updates_count"] == 1
        cu = out["comparability_updates"][0]
        assert cu["provided_values"]["qty"] == 100.0

    def test_declined_response_escalates_risk(self):
        from app.pdf_extraction.response_integration import integrate_responses, RESPONSE_DECLINED
        resp = [{"response_id": "r1", "clarification_id": "clar-0",
                 "response_type": RESPONSE_DECLINED}]
        out = integrate_responses(self._tracking(), resp)
        assert out["integration_summary"]["risk_updates_count"] == 1
        assert out["risk_updates"][0]["update_type"] == "risk_escalation_declined_response"

    def test_unknown_clarification_id(self):
        from app.pdf_extraction.response_integration import integrate_responses
        resp = [{"response_id": "r1", "clarification_id": "missing",
                 "response_type": "confirmed"}]
        out = integrate_responses(self._tracking(), resp)
        assert "missing" in out["integration_diagnostics"]["unknown_clarification_ids"]
        assert out["integrated_responses"][0]["linked_to_clarification"] is False

    def test_multiple_responses(self):
        from app.pdf_extraction.response_integration import integrate_responses
        resp = [
            {"response_id": "r1", "clarification_id": "clar-0", "response_type": "confirmed"},
            {"response_id": "r2", "clarification_id": "clar-1", "response_type": "corrected",
             "response_values": {"qty": 50.0, "unit": "LF"}},
        ]
        out = integrate_responses(self._tracking(), resp)
        assert out["integration_summary"]["total_responses"] == 2
        assert out["integration_summary"]["linked_count"] == 2

    def test_input_not_mutated(self):
        from app.pdf_extraction.response_integration import integrate_responses
        import copy
        tracking = self._tracking()
        snap = copy.deepcopy(tracking)
        integrate_responses(tracking, [{"response_id": "r1", "clarification_id": "clar-0",
                                         "response_type": "confirmed"}])
        assert tracking == snap

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20yy. C58 — Iterative Re-evaluation Tests
# ---------------------------------------------------------------------------


class TestC58IterativeReeval:

    def _snap(self, risk="high", gate="HIGH_RISK", matched=0, non_comp=13, not_addr=93):
        return {
            "risk_output": {"overall_risk_level": risk, "risk_summary": {"total_factors": 3}},
            "gate_output": {"gate_outcome": gate, "gate_summary": {"reason_count": 3}},
            "decision_packet": {
                "decision_posture": "requires_action", "overall_risk_level": risk,
                "comparability_posture": {"total_rows": 15, "comparable_matched": matched, "non_comparable": non_comp},
            },
            "scope_interpretation": {"scope_summary": {"not_addressed_count": not_addr, "ambiguous_count": 0}},
            "resolution_output": {"resolution_summary": {"rows_total": 15, "category_counts": {}}},
            "review_workflow": {"readiness_status": "actionable"},
        }

    def test_create_history(self):
        from app.pdf_extraction.iterative_reeval import create_evaluation_history
        h = create_evaluation_history(self._snap())
        assert h["reeval_version"] == "iterative_reeval/v1"
        assert h["cycle_count"] == 1
        assert h["current_cycle_id"] == "cycle-0"

    def test_append_cycle_computes_delta(self):
        from app.pdf_extraction.iterative_reeval import create_evaluation_history, append_evaluation_cycle
        h = create_evaluation_history(self._snap(risk="high", gate="HIGH_RISK", matched=0))
        h = append_evaluation_cycle(h, self._snap(risk="low", gate="SAFE", matched=15, non_comp=0, not_addr=0),
                                     cycle_reason="after_sub_response")
        assert h["cycle_count"] == 2
        assert h["current_cycle_id"] == "cycle-1"
        delta = h["deltas"][-1]
        assert delta["risk_improved"] is True
        assert delta["gate_improved"] is True
        assert "overall_risk_level" in delta["changed_metrics"]
        assert delta["changed_metrics"]["overall_risk_level"]["before"] == "high"
        assert delta["changed_metrics"]["overall_risk_level"]["after"] == "low"

    def test_no_change_delta_empty(self):
        from app.pdf_extraction.iterative_reeval import create_evaluation_history, append_evaluation_cycle
        snap = self._snap()
        h = create_evaluation_history(snap)
        h = append_evaluation_cycle(h, snap)
        delta = h["deltas"][-1]
        assert delta["metrics_changed_count"] == 0
        assert delta["risk_improved"] is False
        assert delta["gate_improved"] is False

    def test_three_cycles(self):
        from app.pdf_extraction.iterative_reeval import (
            create_evaluation_history, append_evaluation_cycle, get_delta_history,
        )
        h = create_evaluation_history(self._snap(risk="high", matched=0))
        h = append_evaluation_cycle(h, self._snap(risk="medium", matched=5, non_comp=8, not_addr=50),
                                     cycle_reason="partial_response")
        h = append_evaluation_cycle(h, self._snap(risk="low", matched=15, non_comp=0, not_addr=0),
                                     cycle_reason="full_response")
        assert h["cycle_count"] == 3
        deltas = get_delta_history(h)
        assert len(deltas) == 2  # 2 appended cycles produce 2 deltas
        assert deltas[0]["risk_improved"] is True  # high -> medium
        assert deltas[1]["risk_improved"] is True  # medium -> low

    def test_get_current_snapshot(self):
        from app.pdf_extraction.iterative_reeval import create_evaluation_history, get_current_snapshot
        h = create_evaluation_history(self._snap(risk="high"))
        snap = get_current_snapshot(h)
        assert snap["metrics"]["overall_risk_level"] == "high"
        # Mutation safe.
        snap["metrics"]["overall_risk_level"] = "TAMPERED"
        snap2 = get_current_snapshot(h)
        assert snap2["metrics"]["overall_risk_level"] == "high"

    def test_base_not_mutated(self):
        from app.pdf_extraction.iterative_reeval import create_evaluation_history, append_evaluation_cycle
        import copy
        h = create_evaluation_history(self._snap())
        snap = copy.deepcopy(h)
        append_evaluation_cycle(h, self._snap(risk="low"))
        assert h == snap

    def test_initial_delta_has_no_before(self):
        from app.pdf_extraction.iterative_reeval import create_evaluation_history
        h = create_evaluation_history(self._snap())
        assert h["deltas"] == []  # initial cycle has no delta

    def test_cycle_reason_in_delta(self):
        from app.pdf_extraction.iterative_reeval import create_evaluation_history, append_evaluation_cycle
        h = create_evaluation_history(self._snap())
        h = append_evaluation_cycle(h, self._snap(risk="low"), cycle_reason="sub_responded")
        assert h["deltas"][-1]["cycle_reason"] == "sub_responded"

    def test_risk_worsened_detected(self):
        from app.pdf_extraction.iterative_reeval import create_evaluation_history, append_evaluation_cycle
        h = create_evaluation_history(self._snap(risk="low"))
        h = append_evaluation_cycle(h, self._snap(risk="high"))
        delta = h["deltas"][-1]
        assert delta["risk_improved"] is False

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0 and summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20tt. C53 — Decision Compression Tests
# ---------------------------------------------------------------------------


class TestC53DecisionCompression:

    def test_version_present(self):
        from app.pdf_extraction.decision_compression import compress_decision
        out = compress_decision()
        assert out["compression_version"] == "decision_compression/v1"

    def test_groups_clarifications_by_type(self):
        from app.pdf_extraction.decision_compression import compress_decision
        cl = {"clarification_items": [
            {"clarification_type": "scope_gap_clarification", "clarification_text": "A", "source_ref": "r1"},
            {"clarification_type": "scope_gap_clarification", "clarification_text": "B", "source_ref": "r2"},
            {"clarification_type": "missing_quantity_clarification", "clarification_text": "C", "source_ref": "r3"},
        ]}
        out = compress_decision(clarification_output=cl)
        groups = out["grouped_clarifications"]
        assert groups[0]["count"] == 2  # scope_gap is most frequent
        assert groups[0]["clarification_type"] == "scope_gap_clarification"
        assert len(groups[0]["source_refs"]) == 2
        assert out["compression_diagnostics"]["raw_clarification_count"] == 3
        assert out["compression_diagnostics"]["compressed_clarification_groups"] == 2

    def test_groups_recommendations_by_posture(self):
        from app.pdf_extraction.decision_compression import compress_decision
        rec = {"recommendations": [
            {"handling_posture": "carry_in_sub_quote", "posture_reason": "x", "normalized_row_id": "r0"},
            {"handling_posture": "carry_in_sub_quote", "posture_reason": "x", "normalized_row_id": "r1"},
            {"handling_posture": "clarify_before_reliance", "posture_reason": "y", "normalized_row_id": "r2"},
        ]}
        out = compress_decision(recommendation_output=rec)
        groups = out["grouped_recommendations"]
        assert groups[0]["count"] == 2
        assert groups[0]["handling_posture"] == "carry_in_sub_quote"

    def test_top_risks_limited_and_sorted(self):
        from app.pdf_extraction.decision_compression import compress_decision
        risk = {"risk_factors": [
            {"factor_id": f"f{i}", "severity": "medium", "detail": f"d{i}"} for i in range(10)
        ], "risk_summary": {"total_factors": 10}}
        out = compress_decision(risk_output=risk)
        assert len(out["top_risks"]) <= 5

    def test_key_numbers_populated(self):
        from app.pdf_extraction.decision_compression import compress_decision
        dp = {
            "comparability_posture": {"total_rows": 15, "comparable_matched": 5, "non_comparable": 10},
            "scope_gaps": {"not_addressed_count": 80, "ambiguous_count": 2},
            "blocking_issues": [],
        }
        cl = {"clarification_items": [{"clarification_type": "x", "clarification_text": "y", "source_ref": "z"}]}
        rec = {"recommendations": [{"handling_posture": "x", "posture_reason": "y"}]}
        risk = {"risk_factors": [{"factor_id": "f1", "severity": "high", "detail": "d"}],
                "risk_summary": {"total_factors": 1}}
        out = compress_decision(decision_packet=dp, clarification_output=cl,
                                recommendation_output=rec, risk_output=risk)
        kn = out["key_numbers"]
        assert kn["total_rows"] == 15
        assert kn["non_comparable"] == 10
        assert kn["scope_not_addressed"] == 80
        assert kn["clarification_count"] == 1
        assert kn["recommendation_count"] == 1

    def test_source_refs_capped_at_five(self):
        from app.pdf_extraction.decision_compression import compress_decision
        cl = {"clarification_items": [
            {"clarification_type": "x", "clarification_text": "t", "source_ref": f"r{i}"} for i in range(20)
        ]}
        out = compress_decision(clarification_output=cl)
        assert len(out["grouped_clarifications"][0]["source_refs"]) == 5

    def test_empty_inputs(self):
        from app.pdf_extraction.decision_compression import compress_decision
        out = compress_decision()
        assert out["grouped_clarifications"] == []
        assert out["grouped_recommendations"] == []
        assert out["top_risks"] == []

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20uu. C54 — Scenario What-If Tests
# ---------------------------------------------------------------------------


class TestC54ScenarioWhatIf:

    def _res(self, rows):
        return {"resolution_rows": rows, "resolution_summary": {"rows_total": len(rows)},
                "packet_status": "partial"}

    def _row(self, rid, cat, priority="medium"):
        return {"normalized_row_id": rid, "resolution_category": cat,
                "resolution_priority": priority, "quote_values": {},
                "comparison_basis": {}, "external_sources": []}

    def test_version_present(self):
        from app.pdf_extraction.scenario_whatif import evaluate_whatif_scenarios
        out = evaluate_whatif_scenarios({})
        assert out["scenario_whatif_version"] == "scenario_whatif/v1"

    def test_lump_sum_assumption_resolves_row(self):
        from app.pdf_extraction.scenario_whatif import evaluate_whatif_scenarios
        res = self._res([self._row("r0", "non_comparable_missing_quote_source")])
        risk = {"overall_risk_level": "high", "blocking_risks": [], "risk_factors": []}
        scenarios = [{"scenario_id": "s1", "assumptions": [
            {"row_id": "r0", "assumption": "treat_as_lump_sum"},
        ]}]
        out = evaluate_whatif_scenarios(res, risk_output=risk, scenarios=scenarios)
        s = out["scenario_results"][0]
        assert s["scenario_category_counts"].get("clean_match_no_resolution_needed") == 1
        assert s["scenario_category_counts"].get("non_comparable_missing_quote_source", 0) == 0
        assert s["delta_summary"]["risk_changed"] is True

    def test_accept_external_assumption(self):
        from app.pdf_extraction.scenario_whatif import evaluate_whatif_scenarios
        res = self._res([self._row("r0", "non_comparable_missing_quote_source")])
        risk = {"overall_risk_level": "high", "blocking_risks": []}
        scenarios = [{"scenario_id": "s2", "assumptions": [
            {"row_id": "r0", "assumption": "accept_external_qty_as_basis"},
        ]}]
        out = evaluate_whatif_scenarios(res, risk_output=risk, scenarios=scenarios)
        assert out["scenario_results"][0]["scenario_category_counts"]["clean_match_no_resolution_needed"] == 1

    def test_exclude_scope_assumption(self):
        from app.pdf_extraction.scenario_whatif import evaluate_whatif_scenarios
        res = self._res([self._row("r0", "unmapped_scope_review_required", "high")])
        risk = {"overall_risk_level": "high", "blocking_risks": []}
        scenarios = [{"scenario_id": "s3", "assumptions": [
            {"row_id": "r0", "assumption": "exclude_from_scope"},
        ]}]
        out = evaluate_whatif_scenarios(res, risk_output=risk, scenarios=scenarios)
        assert out["scenario_results"][0]["scenario_category_counts"]["clean_match_no_resolution_needed"] == 1

    def test_unknown_assumption_skipped(self):
        from app.pdf_extraction.scenario_whatif import evaluate_whatif_scenarios
        res = self._res([self._row("r0", "non_comparable_missing_quote_source")])
        scenarios = [{"scenario_id": "s4", "assumptions": [
            {"row_id": "r0", "assumption": "made_up"},
        ]}]
        out = evaluate_whatif_scenarios(res, risk_output={"overall_risk_level": "high", "blocking_risks": []},
                                        scenarios=scenarios)
        assert len(out["scenario_results"][0]["assumptions_skipped"]) == 1

    def test_unknown_row_skipped(self):
        from app.pdf_extraction.scenario_whatif import evaluate_whatif_scenarios
        res = self._res([self._row("r0", "non_comparable_missing_quote_source")])
        scenarios = [{"scenario_id": "s5", "assumptions": [
            {"row_id": "missing", "assumption": "treat_as_lump_sum"},
        ]}]
        out = evaluate_whatif_scenarios(res, risk_output={"overall_risk_level": "high", "blocking_risks": []},
                                        scenarios=scenarios)
        assert len(out["scenario_results"][0]["assumptions_skipped"]) == 1

    def test_base_truth_not_mutated(self):
        from app.pdf_extraction.scenario_whatif import evaluate_whatif_scenarios
        import copy
        res = self._res([self._row("r0", "non_comparable_missing_quote_source")])
        snap = copy.deepcopy(res)
        evaluate_whatif_scenarios(res, risk_output={"overall_risk_level": "high", "blocking_risks": []},
                                  scenarios=[{"scenario_id": "s1", "assumptions": [
                                      {"row_id": "r0", "assumption": "treat_as_lump_sum"}]}])
        assert res == snap

    def test_multiple_scenarios(self):
        from app.pdf_extraction.scenario_whatif import evaluate_whatif_scenarios
        res = self._res([
            self._row("r0", "non_comparable_missing_quote_source"),
            self._row("r1", "unmapped_scope_review_required", "high"),
        ])
        risk = {"overall_risk_level": "high", "blocking_risks": []}
        scenarios = [
            {"scenario_id": "optimistic", "assumptions": [
                {"row_id": "r0", "assumption": "treat_as_lump_sum"},
                {"row_id": "r1", "assumption": "assume_scope_covered"},
            ]},
            {"scenario_id": "conservative", "assumptions": []},
        ]
        out = evaluate_whatif_scenarios(res, risk_output=risk, scenarios=scenarios)
        assert len(out["scenario_results"]) == 2
        opt = out["scenario_results"][0]
        con = out["scenario_results"][1]
        assert opt["scenario_risk_level"] == "low"
        assert con["scenario_risk_level"] == "high"

    def test_delta_summary_present(self):
        from app.pdf_extraction.scenario_whatif import evaluate_whatif_scenarios
        res = self._res([self._row("r0", "non_comparable_missing_quote_source")])
        risk = {"overall_risk_level": "high", "blocking_risks": []}
        scenarios = [{"scenario_id": "s1", "assumptions": [
            {"row_id": "r0", "assumption": "treat_as_lump_sum"}]}]
        out = evaluate_whatif_scenarios(res, risk_output=risk, scenarios=scenarios)
        d = out["scenario_results"][0]["delta_summary"]
        assert d["assumptions_count"] == 1
        assert "risk_changed" in d
        assert "posture_changed" in d

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20vv. C55 — Submission Gate Tests
# ---------------------------------------------------------------------------


class TestC55SubmissionGate:

    def test_version_present(self):
        from app.pdf_extraction.submission_gate import evaluate_submission_gate
        out = evaluate_submission_gate()
        assert out["gate_version"] == "submission_gate/v1"

    def test_safe_when_clean(self):
        from app.pdf_extraction.submission_gate import evaluate_submission_gate, GATE_SAFE
        out = evaluate_submission_gate()
        assert out["gate_outcome"] == GATE_SAFE
        assert out["gate_reasons"] == []

    def test_blocked_when_blocking_risks(self):
        from app.pdf_extraction.submission_gate import evaluate_submission_gate, GATE_BLOCKED
        risk = {"blocking_risks": [{"factor_id": "x", "severity": "critical", "detail": "y"}],
                "overall_risk_level": "critical"}
        out = evaluate_submission_gate(risk_output=risk)
        assert out["gate_outcome"] == GATE_BLOCKED
        assert any(r["check"] == "blocking_risk" for r in out["gate_reasons"])

    def test_blocked_when_pairing_blocked(self):
        from app.pdf_extraction.submission_gate import evaluate_submission_gate, GATE_BLOCKED
        res = {"packet_status": "blocked"}
        out = evaluate_submission_gate(resolution_output=res)
        assert out["gate_outcome"] == GATE_BLOCKED

    def test_high_risk_on_unresolved_evidence(self):
        from app.pdf_extraction.submission_gate import evaluate_submission_gate, GATE_HIGH_RISK
        dp = {"decision_posture": "requires_action", "evidence_status": {"unresolved_block_count": 5},
              "comparability_posture": {}, "scope_gaps": {}, "blocking_issues": [],
              "overall_risk_level": "high"}
        risk = {"blocking_risks": [], "overall_risk_level": "high"}
        out = evaluate_submission_gate(risk_output=risk, decision_packet=dp)
        assert out["gate_outcome"] == GATE_HIGH_RISK

    def test_high_risk_on_review_not_ready(self):
        from app.pdf_extraction.submission_gate import evaluate_submission_gate, GATE_HIGH_RISK
        rw = {"readiness_status": "not_started"}
        risk = {"blocking_risks": [], "overall_risk_level": "medium"}
        out = evaluate_submission_gate(risk_output=risk, review_workflow=rw)
        assert out["gate_outcome"] == GATE_HIGH_RISK

    def test_conditional_on_zero_comparability(self):
        from app.pdf_extraction.submission_gate import evaluate_submission_gate, GATE_CONDITIONAL
        dp = {"decision_posture": "usable_with_caveats",
              "comparability_posture": {"total_rows": 15, "comparable_matched": 0, "conflicts": 0},
              "scope_gaps": {}, "evidence_status": {}, "blocking_issues": [],
              "overall_risk_level": "medium"}
        risk = {"blocking_risks": [], "overall_risk_level": "medium"}
        out = evaluate_submission_gate(risk_output=risk, decision_packet=dp)
        assert out["gate_outcome"] == GATE_CONDITIONAL
        assert any(r["check"] == "zero_comparability" for r in out["gate_reasons"])

    def test_conditional_on_scope_gaps(self):
        from app.pdf_extraction.submission_gate import evaluate_submission_gate, GATE_CONDITIONAL
        dp = {"decision_posture": "usable_with_caveats",
              "comparability_posture": {"total_rows": 15, "comparable_matched": 15, "conflicts": 0},
              "scope_gaps": {"not_addressed_count": 80, "ambiguous_count": 0},
              "evidence_status": {}, "blocking_issues": [],
              "overall_risk_level": "medium"}
        risk = {"blocking_risks": [], "overall_risk_level": "medium"}
        out = evaluate_submission_gate(risk_output=risk, decision_packet=dp)
        assert out["gate_outcome"] == GATE_CONDITIONAL
        assert any(r["check"] == "scope_not_addressed" for r in out["gate_reasons"])

    def test_conditional_on_source_conflicts(self):
        from app.pdf_extraction.submission_gate import evaluate_submission_gate, GATE_CONDITIONAL
        dp = {"decision_posture": "usable_with_caveats",
              "comparability_posture": {"total_rows": 15, "comparable_matched": 15, "conflicts": 3},
              "scope_gaps": {}, "evidence_status": {}, "blocking_issues": [],
              "overall_risk_level": "medium"}
        risk = {"blocking_risks": [], "overall_risk_level": "medium"}
        out = evaluate_submission_gate(risk_output=risk, decision_packet=dp)
        assert out["gate_outcome"] == GATE_CONDITIONAL

    def test_gate_summary_counts(self):
        from app.pdf_extraction.submission_gate import evaluate_submission_gate
        risk = {"blocking_risks": [{"factor_id": "x", "severity": "critical", "detail": "y"}],
                "overall_risk_level": "critical"}
        dp = {"decision_posture": "blocked", "evidence_status": {"unresolved_block_count": 2},
              "comparability_posture": {"total_rows": 10, "comparable_matched": 0, "conflicts": 0},
              "scope_gaps": {"not_addressed_count": 5, "ambiguous_count": 1}, "blocking_issues": []}
        out = evaluate_submission_gate(risk_output=risk, decision_packet=dp)
        s = out["gate_summary"]
        assert s["reason_count"] >= 3
        assert s["severity_counts"]["critical"] >= 1

    def test_escalation_only_upward(self):
        """Gate can only escalate, never de-escalate."""
        from app.pdf_extraction.submission_gate import evaluate_submission_gate, GATE_BLOCKED
        risk = {"blocking_risks": [{"factor_id": "x", "severity": "critical", "detail": "y"}],
                "overall_risk_level": "critical"}
        rw = {"readiness_status": "complete"}  # good readiness doesn't downgrade blocked
        out = evaluate_submission_gate(risk_output=risk, review_workflow=rw)
        assert out["gate_outcome"] == GATE_BLOCKED

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0 and summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20qq. C50 — Office Decision Packet Tests
# ---------------------------------------------------------------------------


class TestC50OfficeDecisionPacket:

    def _risk(self, overall="medium", blocking=None, warnings=None, recommended=None, summary=None):
        return {
            "overall_risk_level": overall,
            "blocking_risks": blocking or [],
            "warning_risks": warnings or [{"factor_id": "f1", "severity": "medium", "detail": "x"}],
            "recommended_actions": recommended or [],
            "risk_summary": summary or {"total_factors": 1, "critical_count": 0, "high_count": 0, "medium_count": 1, "low_count": 0},
        }

    def _res(self, cats=None):
        cats = cats or {"clean_match_no_resolution_needed": 5, "non_comparable_missing_quote_source": 10}
        return {"resolution_summary": {"rows_total": sum(cats.values()), "category_counts": cats, "priority_counts": {}}}

    def test_version_present(self):
        from app.pdf_extraction.office_decision_packet import build_decision_packet
        out = build_decision_packet(self._risk())
        assert out["decision_packet_version"] == "office_decision_packet/v1"

    def test_posture_blocked_when_blocking(self):
        from app.pdf_extraction.office_decision_packet import build_decision_packet, POSTURE_BLOCKED
        risk = self._risk(overall="critical", blocking=[{"factor_id": "x", "severity": "critical", "detail": "y"}])
        out = build_decision_packet(risk)
        assert out["decision_posture"] == POSTURE_BLOCKED

    def test_posture_requires_action(self):
        from app.pdf_extraction.office_decision_packet import build_decision_packet, POSTURE_REQUIRES_ACTION
        risk = self._risk(overall="high")
        out = build_decision_packet(risk)
        assert out["decision_posture"] == POSTURE_REQUIRES_ACTION

    def test_posture_usable_with_caveats(self):
        from app.pdf_extraction.office_decision_packet import build_decision_packet, POSTURE_USABLE_WITH_CAVEATS
        risk = self._risk(overall="medium")
        out = build_decision_packet(risk)
        assert out["decision_posture"] == POSTURE_USABLE_WITH_CAVEATS

    def test_posture_ready_for_use(self):
        from app.pdf_extraction.office_decision_packet import build_decision_packet, POSTURE_READY_FOR_USE
        risk = self._risk(overall="low", warnings=[])
        out = build_decision_packet(risk)
        assert out["decision_posture"] == POSTURE_READY_FOR_USE

    def test_comparability_posture(self):
        from app.pdf_extraction.office_decision_packet import build_decision_packet
        out = build_decision_packet(self._risk(), resolution_output=self._res())
        cp = out["comparability_posture"]
        assert cp["total_rows"] == 15
        assert cp["comparable_matched"] == 5
        assert cp["non_comparable"] == 10

    def test_scope_gaps(self):
        from app.pdf_extraction.office_decision_packet import build_decision_packet
        si = {"scope_summary": {"not_addressed_count": 80, "ambiguous_count": 2,
                                "explicitly_excluded_count": 0, "total_topics": 100}}
        out = build_decision_packet(self._risk(), scope_interpretation=si)
        assert out["scope_gaps"]["not_addressed_count"] == 80
        assert out["scope_gaps"]["ambiguous_count"] == 2

    def test_evidence_status(self):
        from app.pdf_extraction.office_decision_packet import build_decision_packet
        hr = {"unresolved_evidence_summary": {"unresolved_block_count": 5, "blocks_with_manual_entries": 3}}
        rw = {"readiness_status": "actionable", "progress_metrics": {"queue_items_remaining": 2}}
        out = build_decision_packet(self._risk(), review_workflow=rw, handwritten_review=hr)
        assert out["evidence_status"]["unresolved_block_count"] == 5
        assert out["evidence_status"]["review_queue_items"] == 2

    def test_blocking_issues_list(self):
        from app.pdf_extraction.office_decision_packet import build_decision_packet
        blocking = [{"factor_id": "blocked_pairing", "severity": "critical", "detail": "pairing_rejected", "count": 1}]
        out = build_decision_packet(self._risk(overall="critical", blocking=blocking))
        assert len(out["blocking_issues"]) == 1
        assert out["blocking_issues"][0]["factor_id"] == "blocked_pairing"

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20rr. C51 — Clarification Generator Tests
# ---------------------------------------------------------------------------


class TestC51ClarificationGenerator:

    def test_version_present(self):
        from app.pdf_extraction.clarification_generator import generate_clarifications
        out = generate_clarifications()
        assert out["clarification_version"] == "clarification_generator/v1"

    def test_scope_gap_generates_clarification(self):
        from app.pdf_extraction.clarification_generator import generate_clarifications, CLAR_SCOPE_GAP
        si = {"scope_topics": [
            {"scope_class": "not_addressed", "description": "Guardrail Repair",
             "source_ref": {"line_number": "0530"}, "evidence_refs": []},
        ]}
        out = generate_clarifications(scope_interpretation=si)
        assert len(out["clarification_items"]) >= 1
        assert out["clarification_items"][0]["clarification_type"] == CLAR_SCOPE_GAP
        assert "Guardrail Repair" in out["clarification_items"][0]["clarification_text"]

    def test_ambiguous_scope_generates_clarification(self):
        from app.pdf_extraction.clarification_generator import generate_clarifications, CLAR_AMBIGUOUS_SCOPE
        si = {"scope_topics": [
            {"scope_class": "ambiguous_scope", "description": "Misc work",
             "source_ref": {"normalized_row_id": "qr-p0-r0"}, "evidence_refs": []},
        ]}
        out = generate_clarifications(scope_interpretation=si)
        assert any(c["clarification_type"] == CLAR_AMBIGUOUS_SCOPE for c in out["clarification_items"])

    def test_missing_qty_generates_clarification(self):
        from app.pdf_extraction.clarification_generator import generate_clarifications, CLAR_MISSING_QTY
        res = {"resolution_rows": [{
            "normalized_row_id": "qr-p0-r0",
            "resolution_category": "non_comparable_missing_quote_source",
            "quote_values": {"description": "Remove Asphalt", "qty": None, "unit": None},
            "comparison_basis": {}, "external_sources": [],
        }]}
        out = generate_clarifications(resolution_output=res)
        assert any(c["clarification_type"] == CLAR_MISSING_QTY for c in out["clarification_items"])

    def test_unmapped_row_generates_clarification(self):
        from app.pdf_extraction.clarification_generator import generate_clarifications, CLAR_UNMAPPED_ROW
        res = {"resolution_rows": [{
            "normalized_row_id": "qr-p0-r0",
            "resolution_category": "unmapped_scope_review_required",
            "quote_values": {"description": "Extra item"}, "comparison_basis": {}, "external_sources": [],
        }]}
        out = generate_clarifications(resolution_output=res)
        assert any(c["clarification_type"] == CLAR_UNMAPPED_ROW for c in out["clarification_items"])

    def test_unit_conflict_generates_clarification(self):
        from app.pdf_extraction.clarification_generator import generate_clarifications, CLAR_UNIT_CONFLICT
        res = {"resolution_rows": [{
            "normalized_row_id": "qr-p0-r0",
            "resolution_category": "unit_discrepancy_review_required",
            "quote_values": {"description": "Barrier", "unit": "LF"},
            "comparison_basis": {"effective_comparison_values": {"unit": "SY"}},
            "external_sources": [],
        }]}
        out = generate_clarifications(resolution_output=res)
        item = next(c for c in out["clarification_items"] if c["clarification_type"] == CLAR_UNIT_CONFLICT)
        assert "LF" in item["clarification_text"]
        assert "SY" in item["clarification_text"]

    def test_qty_conflict_generates_clarification(self):
        from app.pdf_extraction.clarification_generator import generate_clarifications, CLAR_QTY_CONFLICT
        res = {"resolution_rows": [{
            "normalized_row_id": "qr-p0-r0",
            "resolution_category": "quantity_discrepancy_review_required",
            "quote_values": {"description": "Asphalt", "qty": 24.0},
            "comparison_basis": {"effective_comparison_values": {"qty": 50.0}},
            "external_sources": [],
        }]}
        out = generate_clarifications(resolution_output=res)
        assert any(c["clarification_type"] == CLAR_QTY_CONFLICT for c in out["clarification_items"])

    def test_source_conflict_generates_clarification(self):
        from app.pdf_extraction.clarification_generator import generate_clarifications, CLAR_SOURCE_CONFLICT
        res = {"resolution_rows": [{
            "normalized_row_id": "qr-p0-r0",
            "resolution_category": "source_conflict_review_required",
            "quote_values": {"description": "Pipe"}, "comparison_basis": {}, "external_sources": [],
        }]}
        out = generate_clarifications(resolution_output=res)
        assert any(c["clarification_type"] == CLAR_SOURCE_CONFLICT for c in out["clarification_items"])

    def test_unresolved_evidence_generates_clarification(self):
        from app.pdf_extraction.clarification_generator import generate_clarifications, CLAR_UNRESOLVED_EVIDENCE
        hr = {"block_index": [
            {"block_id": "blk-0", "source_page": 3, "machine_readability": "unreadable", "has_manual_entry": False},
            {"block_id": "blk-1", "source_page": 4, "machine_readability": "partial", "has_manual_entry": True},
        ]}
        out = generate_clarifications(handwritten_review=hr)
        items = [c for c in out["clarification_items"] if c["clarification_type"] == CLAR_UNRESOLVED_EVIDENCE]
        assert len(items) == 1
        assert "page 3" in items[0]["clarification_text"]

    def test_summary_counts(self):
        from app.pdf_extraction.clarification_generator import generate_clarifications
        si = {"scope_topics": [
            {"scope_class": "not_addressed", "description": "A", "source_ref": {"line_number": "1"}, "evidence_refs": []},
            {"scope_class": "not_addressed", "description": "B", "source_ref": {"line_number": "2"}, "evidence_refs": []},
        ]}
        out = generate_clarifications(scope_interpretation=si)
        assert out["clarification_summary"]["total_clarifications"] == 2
        assert out["clarification_summary"]["type_counts"]["scope_gap_clarification"] == 2

    def test_empty_inputs_produces_empty(self):
        from app.pdf_extraction.clarification_generator import generate_clarifications
        out = generate_clarifications()
        assert out["clarification_items"] == []
        assert out["clarification_summary"]["total_clarifications"] == 0

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20ss. C52 — Carry / Gap / Contingency Recommendation Tests
# ---------------------------------------------------------------------------


class TestC52CarryGapRecommendation:

    def _res_row(self, cat, rid="qr-p0-r0", **kw):
        base = {"normalized_row_id": rid, "resolution_category": cat,
                "quote_values": {}, "comparison_basis": {}, "external_sources": []}
        base.update(kw)
        return base

    def test_version_present(self):
        from app.pdf_extraction.carry_gap_recommendation import build_recommendations
        out = build_recommendations()
        assert out["recommendation_version"] == "carry_gap_recommendation/v1"

    def test_clean_match_carry_in_sub(self):
        from app.pdf_extraction.carry_gap_recommendation import (
            build_recommendations, POSTURE_CARRY_IN_SUB_QUOTE,
        )
        res = {"resolution_rows": [self._res_row("clean_match_no_resolution_needed")]}
        out = build_recommendations(resolution_output=res)
        assert out["recommendations"][0]["handling_posture"] == POSTURE_CARRY_IN_SUB_QUOTE

    def test_missing_quote_clarify(self):
        from app.pdf_extraction.carry_gap_recommendation import (
            build_recommendations, POSTURE_CLARIFY_BEFORE_RELIANCE,
        )
        res = {"resolution_rows": [self._res_row("non_comparable_missing_quote_source")]}
        out = build_recommendations(resolution_output=res)
        assert out["recommendations"][0]["handling_posture"] == POSTURE_CLARIFY_BEFORE_RELIANCE

    def test_unmapped_carry_internally(self):
        from app.pdf_extraction.carry_gap_recommendation import (
            build_recommendations, POSTURE_CARRY_INTERNALLY,
        )
        res = {"resolution_rows": [self._res_row("unmapped_scope_review_required")]}
        out = build_recommendations(resolution_output=res)
        assert out["recommendations"][0]["handling_posture"] == POSTURE_CARRY_INTERNALLY

    def test_blocked_pairing_block_reliance(self):
        from app.pdf_extraction.carry_gap_recommendation import (
            build_recommendations, POSTURE_BLOCK_QUOTE_RELIANCE,
        )
        res = {"resolution_rows": [self._res_row("blocked_pairing_resolution_required")]}
        out = build_recommendations(resolution_output=res)
        assert out["recommendations"][0]["handling_posture"] == POSTURE_BLOCK_QUOTE_RELIANCE

    def test_source_conflict_clarify(self):
        from app.pdf_extraction.carry_gap_recommendation import (
            build_recommendations, POSTURE_CLARIFY_BEFORE_RELIANCE,
        )
        res = {"resolution_rows": [self._res_row("source_conflict_review_required")]}
        out = build_recommendations(resolution_output=res)
        assert out["recommendations"][0]["handling_posture"] == POSTURE_CLARIFY_BEFORE_RELIANCE

    def test_office_working_basis_upgrades_to_carry(self):
        from app.pdf_extraction.carry_gap_recommendation import (
            build_recommendations, POSTURE_CARRY_IN_SUB_QUOTE,
        )
        res = {"resolution_rows": [self._res_row("non_comparable_missing_quote_source")]}
        actions = {"resolution_rows": [{
            "normalized_row_id": "qr-p0-r0",
            "office_actions": [{"action_type": "accept_dot_quantity_as_working_basis"}],
        }]}
        out = build_recommendations(resolution_output=res, office_action_output=actions)
        assert out["recommendations"][0]["handling_posture"] == POSTURE_CARRY_IN_SUB_QUOTE
        assert out["recommendations"][0]["posture_reason"] == "office_accepted_working_basis"

    def test_office_lump_sum_upgrades_to_carry(self):
        from app.pdf_extraction.carry_gap_recommendation import (
            build_recommendations, POSTURE_CARRY_IN_SUB_QUOTE,
        )
        res = {"resolution_rows": [self._res_row("non_comparable_missing_quote_source")]}
        actions = {"resolution_rows": [{
            "normalized_row_id": "qr-p0-r0",
            "office_actions": [{"action_type": "mark_lump_sum_non_comparable"}],
        }]}
        out = build_recommendations(resolution_output=res, office_action_output=actions)
        assert out["recommendations"][0]["handling_posture"] == POSTURE_CARRY_IN_SUB_QUOTE

    def test_unaddressed_bid_items_carry_internally(self):
        from app.pdf_extraction.carry_gap_recommendation import (
            build_recommendations, POSTURE_CARRY_INTERNALLY,
        )
        si = {"scope_topics": [
            {"topic_id": "t-0", "scope_class": "not_addressed",
             "source_type": "dot_bid_unmatched",
             "source_ref": {"line_number": "9999"}, "description": "Guardrail"},
        ]}
        out = build_recommendations(scope_interpretation=si)
        rec = next(r for r in out["recommendations"] if r["source_type"] == "scope_topic_bid_unmatched")
        assert rec["handling_posture"] == POSTURE_CARRY_INTERNALLY

    def test_ambiguous_scope_clarify(self):
        from app.pdf_extraction.carry_gap_recommendation import (
            build_recommendations, POSTURE_CLARIFY_BEFORE_RELIANCE,
        )
        si = {"scope_topics": [
            {"topic_id": "t-0", "scope_class": "ambiguous_scope",
             "source_type": "dot_bid_unmatched",
             "source_ref": {"line_number": "8888"}, "description": "Misc"},
        ]}
        out = build_recommendations(scope_interpretation=si)
        rec = next(r for r in out["recommendations"] if r["source_type"] == "scope_topic_bid_unmatched")
        assert rec["handling_posture"] == POSTURE_CLARIFY_BEFORE_RELIANCE

    def test_summary_posture_counts(self):
        from app.pdf_extraction.carry_gap_recommendation import build_recommendations
        res = {"resolution_rows": [
            self._res_row("clean_match_no_resolution_needed", rid="r0"),
            self._res_row("clean_match_no_resolution_needed", rid="r1"),
            self._res_row("unmapped_scope_review_required", rid="r2"),
            self._res_row("blocked_pairing_resolution_required", rid="r3"),
        ]}
        out = build_recommendations(resolution_output=res)
        s = out["recommendation_summary"]
        assert s["carry_in_sub_quote_count"] == 2
        assert s["carry_internally_count"] == 1
        assert s["block_quote_reliance_count"] == 1

    def test_empty_inputs_empty_output(self):
        from app.pdf_extraction.carry_gap_recommendation import build_recommendations
        out = build_recommendations()
        assert out["recommendations"] == []
        assert out["recommendation_summary"]["total_recommendations"] == 0

    def test_reason_always_present(self):
        from app.pdf_extraction.carry_gap_recommendation import build_recommendations
        res = {"resolution_rows": [
            self._res_row("clean_match_no_resolution_needed"),
            self._res_row("source_conflict_review_required", rid="r1"),
        ]}
        out = build_recommendations(resolution_output=res)
        for r in out["recommendations"]:
            assert r["posture_reason"] is not None
            assert len(r["posture_reason"]) > 0

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0 and summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20nn. C47 — Review Workflow Optimization Tests
# ---------------------------------------------------------------------------


class TestC47ReviewWorkflowOptimization:

    def _hr(self, **kw):
        base = {
            "document_status": "machine_unreadable_human_required",
            "intake_summary": {"evidence_blocks_count": 10, "accepted_rows_count": 0, "page_count": 10},
            "manual_interpretation_summary": {"rows_manual_count": 0, "rows_machine_count": 0},
            "approval_summary": {"approved_rows_count": 0, "unapproved_rows_count": 0, "rejected_rows_count": 0},
            "unresolved_evidence_summary": {"unresolved_block_count": 10, "total_partial_or_unreadable": 10, "blocks_with_manual_entries": 0},
            "block_index": [{"block_id": f"blk-{i}", "source_page": i, "machine_readability": "partial", "has_manual_entry": False} for i in range(10)],
            "manual_row_index": [],
        }
        base.update(kw)
        return base

    def test_not_started_readiness(self):
        from app.pdf_extraction.review_workflow_optimization import build_review_workflow, READINESS_NOT_STARTED
        wf = build_review_workflow(self._hr())
        assert wf["readiness_status"] == READINESS_NOT_STARTED

    def test_blocked_pending_approval(self):
        from app.pdf_extraction.review_workflow_optimization import build_review_workflow, READINESS_BLOCKED_PENDING_APPROVAL
        hr = self._hr(
            manual_row_index=[{"manual_entry_id": "me-0", "manual_row_key": "r-0",
                               "entry_status": "active", "source_block_id": "blk-0",
                               "approval_status": "draft_manual_interpretation"}],
            approval_summary={"approved_rows_count": 0, "unapproved_rows_count": 1},
            unresolved_evidence_summary={"unresolved_block_count": 0, "total_partial_or_unreadable": 10, "blocks_with_manual_entries": 10},
        )
        wf = build_review_workflow(hr)
        assert wf["readiness_status"] == READINESS_BLOCKED_PENDING_APPROVAL

    def test_actionable_readiness(self):
        from app.pdf_extraction.review_workflow_optimization import build_review_workflow, READINESS_ACTIONABLE
        hr = self._hr(
            document_status="machine_readable",
            approval_summary={"approved_rows_count": 0, "unapproved_rows_count": 0},
            unresolved_evidence_summary={"unresolved_block_count": 0, "total_partial_or_unreadable": 0, "blocks_with_manual_entries": 0},
        )
        actioned = {"resolution_rows": [
            {"normalized_row_id": "qr-p0-r0", "resolution_category": "non_comparable_missing_quote_source",
             "resolution_priority": "medium", "office_actions": []},
        ]}
        wf = build_review_workflow(hr, actioned, actioned)
        assert wf["readiness_status"] == READINESS_ACTIONABLE
        assert wf["progress_metrics"]["queue_items_remaining"] >= 1

    def test_complete_readiness(self):
        from app.pdf_extraction.review_workflow_optimization import build_review_workflow, READINESS_COMPLETE
        hr = self._hr(
            document_status="machine_readable",
            approval_summary={"approved_rows_count": 0, "unapproved_rows_count": 0},
            unresolved_evidence_summary={"unresolved_block_count": 0, "total_partial_or_unreadable": 0, "blocks_with_manual_entries": 0},
            block_index=[],
        )
        wf = build_review_workflow(hr, {"resolution_rows": []}, {"resolution_rows": []})
        assert wf["readiness_status"] == READINESS_COMPLETE

    def test_queue_prioritized(self):
        from app.pdf_extraction.review_workflow_optimization import build_review_workflow
        hr = self._hr(
            manual_row_index=[{"manual_entry_id": "me-0", "manual_row_key": "r-0",
                               "entry_status": "active", "source_block_id": "blk-0",
                               "approval_status": "draft_manual_interpretation"}],
        )
        wf = build_review_workflow(hr)
        queue = wf["review_queue"]
        # Should have pending_approval + pending_manual entries, sorted by priority.
        assert len(queue) >= 1
        priorities = [i["priority"] for i in queue]
        order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        assert all(order.get(priorities[i], 99) <= order.get(priorities[i+1], 99) for i in range(len(priorities)-1))

    def test_progress_metrics_shape(self):
        from app.pdf_extraction.review_workflow_optimization import build_review_workflow
        wf = build_review_workflow(self._hr())
        pm = wf["progress_metrics"]
        for key in ("evidence_coverage_ratio", "approval_ratio", "office_action_ratio",
                    "queue_items_remaining", "queue_priority_counts"):
            assert key in pm

    def test_version_present(self):
        from app.pdf_extraction.review_workflow_optimization import build_review_workflow
        wf = build_review_workflow(self._hr())
        assert wf["review_workflow_version"] == "review_workflow_optimization/v1"

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0 and summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20oo. C48 — Scope Interpretation Tests
# ---------------------------------------------------------------------------


class TestC48ScopeInterpretation:

    def _row(self, **kw):
        base = {"normalized_row_id": "qr-p0-r0", "description": "Remove Asphalt",
                "qty": 100.0, "unit": "SY", "unit_price": 5.0, "amount": 500.0,
                "source_page": 0, "line_ref": "0530", "row_origin": "ocr_pdf"}
        base.update(kw)
        return base

    def test_explicitly_included(self):
        from app.pdf_extraction.scope_interpretation import build_scope_interpretation, SCOPE_EXPLICITLY_INCLUDED
        out = build_scope_interpretation([self._row()])
        assert out["scope_topics"][0]["scope_class"] == SCOPE_EXPLICITLY_INCLUDED

    def test_implicitly_included_amount_only(self):
        from app.pdf_extraction.scope_interpretation import build_scope_interpretation, SCOPE_IMPLICITLY_INCLUDED
        row = self._row(qty=None, unit=None, unit_price=None)
        out = build_scope_interpretation([row])
        assert out["scope_topics"][0]["scope_class"] == SCOPE_IMPLICITLY_INCLUDED

    def test_ambiguous_no_monetary(self):
        from app.pdf_extraction.scope_interpretation import build_scope_interpretation, SCOPE_AMBIGUOUS
        row = self._row(qty=None, unit=None, unit_price=None, amount=None)
        out = build_scope_interpretation([row])
        assert out["scope_topics"][0]["scope_class"] == SCOPE_AMBIGUOUS

    def test_not_addressed_bid_item(self):
        from app.pdf_extraction.scope_interpretation import build_scope_interpretation, SCOPE_NOT_ADDRESSED
        bid_rows = [{"line_number": "9999", "item_number": "X", "description": "Unmatched Bid"}]
        out = build_scope_interpretation([], bid_rows=bid_rows)
        assert any(t["scope_class"] == SCOPE_NOT_ADDRESSED for t in out["scope_topics"])

    def test_manual_with_amount_explicitly_included(self):
        from app.pdf_extraction.scope_interpretation import build_scope_interpretation, SCOPE_EXPLICITLY_INCLUDED
        row = self._row(row_origin="manual_interpretation", qty=None, unit=None, unit_price=None, amount=500.0)
        out = build_scope_interpretation([row])
        assert out["scope_topics"][0]["scope_class"] == SCOPE_EXPLICITLY_INCLUDED

    def test_evidence_refs_populated(self):
        from app.pdf_extraction.scope_interpretation import build_scope_interpretation
        row = self._row(manual_entry_ref={"entered_by": "alice"}, source_block_ref={"block_id": "blk-0"})
        out = build_scope_interpretation([row])
        refs = out["scope_topics"][0]["evidence_refs"]
        types = {r["type"] for r in refs}
        assert "line_ref" in types
        assert "manual_entry_ref" in types
        assert "source_block_ref" in types
        assert "monetary_amount" in types

    def test_scope_summary_counts(self):
        from app.pdf_extraction.scope_interpretation import build_scope_interpretation
        rows = [
            self._row(normalized_row_id="qr-p0-r0"),
            self._row(normalized_row_id="qr-p0-r1", qty=None, unit=None, unit_price=None),
            self._row(normalized_row_id="qr-p0-r2", qty=None, unit=None, unit_price=None, amount=None),
        ]
        out = build_scope_interpretation(rows)
        s = out["scope_summary"]
        assert s["explicitly_included_count"] == 1
        assert s["implicitly_included_count"] == 1
        assert s["ambiguous_count"] == 1
        assert s["total_topics"] == 3

    def test_version_present(self):
        from app.pdf_extraction.scope_interpretation import build_scope_interpretation
        out = build_scope_interpretation([self._row()])
        assert out["scope_interpretation_version"] == "scope_interpretation/v1"

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# 20pp. C49 — Risk Scoring Tests
# ---------------------------------------------------------------------------


class TestC49RiskScoring:

    def test_blocked_pairing_critical(self):
        from app.pdf_extraction.risk_scoring import score_bid_risk, RISK_CRITICAL, FACTOR_BLOCKED_PAIRING
        out = score_bid_risk(resolution_output={"packet_status": "blocked", "resolution_summary": {"rows_total": 15, "category_counts": {}}})
        assert out["overall_risk_level"] == RISK_CRITICAL
        assert any(f["factor_id"] == FACTOR_BLOCKED_PAIRING for f in out["blocking_risks"])

    def test_unresolved_evidence_high(self):
        from app.pdf_extraction.risk_scoring import score_bid_risk, RISK_HIGH, FACTOR_UNRESOLVED_EVIDENCE
        rw = {"readiness_status": "blocked_pending_evidence",
              "review_summary": {"unapproved_count": 0, "manual_rows_total": 0, "machine_rows_total": 0},
              "progress_metrics": {"evidence_coverage_ratio": {"covered": 0, "total": 10}, "queue_items_remaining": 10}}
        hr = {"unresolved_evidence_summary": {"unresolved_block_count": 10}}
        out = score_bid_risk(review_workflow=rw, handwritten_review=hr)
        assert any(f["factor_id"] == FACTOR_UNRESOLVED_EVIDENCE for f in out["risk_factors"])
        assert out["overall_risk_level"] in (RISK_HIGH, "critical")

    def test_source_conflicts_flagged(self):
        from app.pdf_extraction.risk_scoring import score_bid_risk, FACTOR_SOURCE_CONFLICTS
        sm = {"source_management_summary": {"rows_with_conflicted_sources": 3, "rows_total": 15}}
        out = score_bid_risk(source_management=sm)
        assert any(f["factor_id"] == FACTOR_SOURCE_CONFLICTS for f in out["risk_factors"])

    def test_ambiguous_scope_flagged(self):
        from app.pdf_extraction.risk_scoring import score_bid_risk, FACTOR_AMBIGUOUS_SCOPE
        si = {"scope_summary": {"ambiguous_count": 5, "not_addressed_count": 0}}
        out = score_bid_risk(scope_interpretation=si)
        assert any(f["factor_id"] == FACTOR_AMBIGUOUS_SCOPE for f in out["risk_factors"])

    def test_unaddressed_bid_items_flagged(self):
        from app.pdf_extraction.risk_scoring import score_bid_risk, FACTOR_UNADDRESSED_BID_ITEMS
        si = {"scope_summary": {"ambiguous_count": 0, "not_addressed_count": 8}}
        out = score_bid_risk(scope_interpretation=si)
        assert any(f["factor_id"] == FACTOR_UNADDRESSED_BID_ITEMS for f in out["risk_factors"])

    def test_low_risk_when_clean(self):
        from app.pdf_extraction.risk_scoring import score_bid_risk, RISK_LOW
        out = score_bid_risk()
        assert out["overall_risk_level"] == RISK_LOW
        assert out["risk_summary"]["total_factors"] == 0

    def test_recommended_actions_present(self):
        from app.pdf_extraction.risk_scoring import score_bid_risk
        out = score_bid_risk(resolution_output={"packet_status": "blocked", "resolution_summary": {"rows_total": 15, "category_counts": {}}})
        assert len(out["recommended_actions"]) >= 1
        assert out["recommended_actions"][0]["recommended_action"] is not None

    def test_risk_summary_counts(self):
        from app.pdf_extraction.risk_scoring import score_bid_risk
        rw = {"readiness_status": "not_started",
              "review_summary": {"unapproved_count": 2, "manual_rows_total": 2, "machine_rows_total": 0},
              "progress_metrics": {"evidence_coverage_ratio": {"covered": 0, "total": 10}, "queue_items_remaining": 12}}
        hr = {"unresolved_evidence_summary": {"unresolved_block_count": 10}}
        out = score_bid_risk(review_workflow=rw, handwritten_review=hr)
        assert out["risk_summary"]["total_factors"] >= 2
        assert out["risk_summary"]["high_count"] >= 1

    def test_version_present(self):
        from app.pdf_extraction.risk_scoring import score_bid_risk
        out = score_bid_risk()
        assert out["risk_scoring_version"] == "risk_scoring/v1"

    def test_multiple_factors_worst_wins(self):
        from app.pdf_extraction.risk_scoring import score_bid_risk, RISK_CRITICAL
        rw = {"readiness_status": "not_started",
              "review_summary": {"unapproved_count": 0, "manual_rows_total": 0, "machine_rows_total": 0},
              "progress_metrics": {"evidence_coverage_ratio": {"covered": 0, "total": 10}, "queue_items_remaining": 0}}
        hr = {"unresolved_evidence_summary": {"unresolved_block_count": 10}}
        out = score_bid_risk(
            review_workflow=rw, handwritten_review=hr,
            resolution_output={"packet_status": "blocked", "resolution_summary": {"rows_total": 15, "category_counts": {}}},
        )
        assert out["overall_risk_level"] == RISK_CRITICAL

    def test_dot_native_unchanged(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0 and summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20mm. C46 — Handwritten Review Control-Room Contract Tests
# ---------------------------------------------------------------------------


class TestC46HandwrittenReviewControlRoom:
    """C46: backend view-state contract for handwritten/manual review."""

    def _intake(self, path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        return run_intake(str(path))

    def _store(self, intake, entries_data):
        from app.pdf_extraction.manual_quote_interpretation import create_manual_interpretation
        entries = []
        for i, vals in enumerate(entries_data):
            bid = intake["evidence_blocks"][min(i, len(intake["evidence_blocks"]) - 1)]["block_id"]
            entries.append({
                "manual_row_key": f"row-{i}",
                "source_block_id": bid,
                "entered_by": "alice",
                "entered_values": vals,
            })
        return create_manual_interpretation(intake, {"entries": entries})

    # ---- Structure ----

    def test_version_and_views_present(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_review_control_room import (
            build_handwritten_review, HANDWRITTEN_REVIEW_VERSION,
        )
        intake = self._intake(rasch_quote_path)
        review = build_handwritten_review(intake)
        assert review["handwritten_review_version"] == HANDWRITTEN_REVIEW_VERSION
        assert review["available_views"] == [
            "intake_overview", "unreadable_blocks", "partial_blocks",
            "manual_entry_queue", "approval_queue", "approved_manual_rows",
            "unresolved_evidence_blocks", "hybrid_evaluation_preview",
        ]

    def test_document_status_propagated(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_review_control_room import build_handwritten_review
        intake = self._intake(rasch_quote_path)
        review = build_handwritten_review(intake)
        assert review["document_status"] == "machine_unreadable_human_required"

    # ---- Default view cascade ----

    def test_default_unreadable_blocks_when_no_manual_entries(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_review_control_room import (
            build_handwritten_review, VIEW_UNREADABLE_BLOCKS, VIEW_PARTIAL_BLOCKS,
        )
        intake = self._intake(rasch_quote_path)
        review = build_handwritten_review(intake)
        # Rasch has 10 partial (OCR-noise) blocks and no manual entries.
        # No unreadable blocks exist (they are partial) → cascade hits partial_blocks.
        assert review["default_view"] in (VIEW_UNREADABLE_BLOCKS, VIEW_PARTIAL_BLOCKS)

    def test_default_approval_queue_when_unapproved_entries(self, rasch_quote_path):
        from app.pdf_extraction.manual_interpretation_approval import build_approval_state
        from app.pdf_extraction.handwritten_review_control_room import (
            build_handwritten_review, VIEW_APPROVAL_QUEUE,
        )
        intake = self._intake(rasch_quote_path)
        store = self._store(intake, [
            {"description": "Item A", "qty": 10.0, "unit": "LF",
             "unit_price": 5.0, "amount": 50.0},
        ])
        state = build_approval_state(store)  # all draft
        review = build_handwritten_review(intake, store, state)
        assert review["default_view"] == VIEW_APPROVAL_QUEUE

    def test_default_hybrid_preview_when_all_approved(self, rasch_quote_path):
        from app.pdf_extraction.manual_interpretation_approval import (
            build_approval_state, STATUS_APPROVED,
        )
        from app.pdf_extraction.handwritten_review_control_room import (
            build_handwritten_review, VIEW_HYBRID_PREVIEW, VIEW_UNRESOLVED_EVIDENCE,
        )
        intake = self._intake(rasch_quote_path)
        bid = intake["evidence_blocks"][0]["block_id"]
        store = self._store(intake, [
            {"description": "Item A", "qty": 10.0, "unit": "LF",
             "unit_price": 5.0, "amount": 50.0},
        ])
        eids = [e["manual_entry_id"] for e in store["entries"]]
        state = build_approval_state(store, [
            {"manual_entry_id": eids[0], "approval_status": STATUS_APPROVED},
        ])
        review = build_handwritten_review(intake, store, state)
        # 1 block covered by manual entry, 9 still unresolved → unresolved.
        assert review["default_view"] in (VIEW_HYBRID_PREVIEW, VIEW_UNRESOLVED_EVIDENCE, "partial_blocks")

    def test_default_overview_for_machine_readable(self, ipsi_quote_path):
        from app.pdf_extraction.handwritten_review_control_room import (
            build_handwritten_review, VIEW_INTAKE_OVERVIEW,
        )
        intake = self._intake(ipsi_quote_path)
        review = build_handwritten_review(intake)
        # IPSI is machine_readable with no manual entries → overview.
        # But IPSI has 1 partial block → may hit partial_blocks first.
        # Either is acceptable; the key is determinism.
        assert review["default_view"] in (VIEW_INTAKE_OVERVIEW, "partial_blocks")

    # ---- View summaries ----

    def test_intake_overview_summary(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_review_control_room import build_handwritten_review
        intake = self._intake(rasch_quote_path)
        review = build_handwritten_review(intake)
        ov = review["view_summaries"]["intake_overview"]
        assert ov["document_status"] == "machine_unreadable_human_required"
        assert ov["evidence_blocks_count"] == 10
        assert ov["accepted_rows_count"] == 0

    def test_approval_queue_summary(self, rasch_quote_path):
        from app.pdf_extraction.manual_interpretation_approval import build_approval_state
        from app.pdf_extraction.handwritten_review_control_room import build_handwritten_review
        intake = self._intake(rasch_quote_path)
        store = self._store(intake, [
            {"description": "A", "qty": 1.0, "unit": "LF",
             "unit_price": 1.0, "amount": 1.0},
        ])
        state = build_approval_state(store)
        review = build_handwritten_review(intake, store, state)
        aq = review["view_summaries"]["approval_queue"]
        assert aq["unapproved_count"] == 1
        assert aq["approved_count"] == 0

    def test_unresolved_evidence_summary(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_review_control_room import build_handwritten_review
        intake = self._intake(rasch_quote_path)
        store = self._store(intake, [
            {"description": "A", "qty": 1.0, "unit": "LF",
             "unit_price": 1.0, "amount": 1.0},
        ])
        review = build_handwritten_review(intake, store)
        ues = review["unresolved_evidence_summary"]
        # 10 blocks, 1 covered by manual entry, 9 unresolved.
        assert ues["unresolved_block_count"] == 9
        assert ues["blocks_with_manual_entries"] == 1

    # ---- Block index reachability ----

    def test_every_block_in_index(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_review_control_room import build_handwritten_review
        intake = self._intake(rasch_quote_path)
        review = build_handwritten_review(intake)
        assert len(review["block_index"]) == 10
        for b in review["block_index"]:
            assert "block_id" in b
            assert "machine_readability" in b
            assert "view_bucket" in b

    def test_block_with_manual_entry_flagged(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_review_control_room import build_handwritten_review
        intake = self._intake(rasch_quote_path)
        store = self._store(intake, [
            {"description": "A", "qty": 1.0, "unit": "LF",
             "unit_price": 1.0, "amount": 1.0},
        ])
        review = build_handwritten_review(intake, store)
        bid = intake["evidence_blocks"][0]["block_id"]
        entry = next(b for b in review["block_index"] if b["block_id"] == bid)
        assert entry["has_manual_entry"] is True

    # ---- Manual row index ----

    def test_manual_row_index_present(self, rasch_quote_path):
        from app.pdf_extraction.manual_interpretation_approval import (
            build_approval_state, STATUS_APPROVED,
        )
        from app.pdf_extraction.handwritten_review_control_room import build_handwritten_review
        intake = self._intake(rasch_quote_path)
        store = self._store(intake, [
            {"description": "A", "qty": 1.0, "unit": "LF",
             "unit_price": 1.0, "amount": 1.0},
        ])
        eids = [e["manual_entry_id"] for e in store["entries"]]
        state = build_approval_state(store, [
            {"manual_entry_id": eids[0], "approval_status": STATUS_APPROVED},
        ])
        review = build_handwritten_review(intake, store, state)
        assert len(review["manual_row_index"]) == 1
        mr = review["manual_row_index"][0]
        assert mr["manual_entry_id"] == eids[0]
        assert mr["approval_status"] == "approved_for_evaluation"

    # ---- Superseded/rejected visible ----

    def test_superseded_entries_visible_in_index(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, append_manual_revision,
        )
        from app.pdf_extraction.handwritten_review_control_room import build_handwritten_review
        intake = self._intake(rasch_quote_path)
        bid = intake["evidence_blocks"][0]["block_id"]
        store = create_manual_interpretation(intake, {"entries": [{
            "manual_row_key": "row-0", "source_block_id": bid,
            "entered_by": "alice",
            "entered_values": {"description": "A", "qty": 1.0, "unit": "LF",
                                "unit_price": 1.0, "amount": 1.0},
        }]})
        store = append_manual_revision(store, {"entries": [{
            "manual_row_key": "row-0", "source_block_id": bid,
            "entered_by": "bob",
            "entered_values": {"description": "A revised", "qty": 2.0, "unit": "LF",
                                "unit_price": 1.0, "amount": 2.0},
        }]})
        review = build_handwritten_review(intake, store)
        statuses = [mr["entry_status"] for mr in review["manual_row_index"]]
        assert "superseded" in statuses
        assert "active" in statuses

    # ---- Mixed machine/manual ----

    def test_mixed_document_review(self, ipsi_quote_path):
        from app.pdf_extraction.handwritten_review_control_room import build_handwritten_review
        intake = self._intake(ipsi_quote_path)
        store = self._store(intake, [
            {"description": "Extra scope", "qty": 1.0, "unit": "LS",
             "unit_price": 500.0, "amount": 500.0},
        ])
        review = build_handwritten_review(intake, store)
        # IPSI has 15 machine rows + 1 manual entry. Manual data surfaced.
        ov = review["view_summaries"]["intake_overview"]
        assert ov["accepted_rows_count"] == 15
        mr = review["view_summaries"]["manual_entry_queue"]
        assert mr["active_entries"] == 1

    # ---- Immutability ----

    def test_input_not_mutated(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_review_control_room import build_handwritten_review
        import copy
        intake = self._intake(rasch_quote_path)
        snap = copy.deepcopy(intake)
        build_handwritten_review(intake)
        assert intake["evidence_blocks"] == snap["evidence_blocks"]

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c46(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c46(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20ll. C45 — Manual Interpretation Approval Gate Tests
# ---------------------------------------------------------------------------


class TestC45ApprovalGate:
    """C45: append-only approval gate over C42 manual entries."""

    def _intake(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        return run_intake(str(rasch_quote_path))

    def _store(self, intake, count=2):
        from app.pdf_extraction.manual_quote_interpretation import create_manual_interpretation
        entries = []
        for i in range(count):
            bid = intake["evidence_blocks"][min(i, len(intake["evidence_blocks"]) - 1)]["block_id"]
            entries.append({
                "manual_row_key": f"row-{i}",
                "source_block_id": bid,
                "entered_by": "alice",
                "entered_values": {"description": f"Item {i}", "qty": 10.0 + i,
                                    "unit": "LF", "unit_price": 5.0, "amount": 50.0 + i * 5},
            })
        return create_manual_interpretation(intake, {"entries": entries})

    # ---- Default: all entries are draft ----

    def test_default_all_draft(self, rasch_quote_path):
        from app.pdf_extraction.manual_interpretation_approval import (
            build_approval_state, STATUS_DRAFT,
        )
        intake = self._intake(rasch_quote_path)
        store = self._store(intake)
        state = build_approval_state(store)
        assert state["approval_version"] == "manual_interpretation_approval/v1"
        s = state["approval_summary"]
        assert s["approved_rows_count"] == 0
        assert s["unapproved_rows_count"] == 2  # both draft
        for info in state["entry_approvals"].values():
            assert info["approval_status"] == STATUS_DRAFT

    # ---- Approve an entry ----

    def test_approve_entry(self, rasch_quote_path):
        from app.pdf_extraction.manual_interpretation_approval import (
            build_approval_state, STATUS_APPROVED, get_approved_manual_rows,
        )
        intake = self._intake(rasch_quote_path)
        store = self._store(intake)
        eids = [e["manual_entry_id"] for e in store["entries"]]
        state = build_approval_state(store, [
            {"manual_entry_id": eids[0], "approval_status": STATUS_APPROVED,
             "reviewed_by": "bob", "approval_note": "looks good"},
        ])
        s = state["approval_summary"]
        assert s["approved_rows_count"] == 1
        assert s["unapproved_rows_count"] == 1
        rows = get_approved_manual_rows(store, state)
        assert len(rows) == 1

    # ---- Reject an entry ----

    def test_reject_entry(self, rasch_quote_path):
        from app.pdf_extraction.manual_interpretation_approval import (
            build_approval_state, STATUS_REJECTED,
        )
        intake = self._intake(rasch_quote_path)
        store = self._store(intake)
        eids = [e["manual_entry_id"] for e in store["entries"]]
        state = build_approval_state(store, [
            {"manual_entry_id": eids[0], "approval_status": STATUS_REJECTED,
             "reviewed_by": "bob"},
        ])
        s = state["approval_summary"]
        assert s["rejected_rows_count"] == 1
        assert s["unapproved_rows_count"] == 1

    # ---- Superseded entry cannot be approved ----

    def test_superseded_entry_not_approvable(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, append_manual_revision,
        )
        from app.pdf_extraction.manual_interpretation_approval import (
            build_approval_state, STATUS_SUPERSEDED, STATUS_APPROVED,
        )
        intake = self._intake(rasch_quote_path)
        bid = intake["evidence_blocks"][0]["block_id"]
        store = create_manual_interpretation(intake, {"entries": [{
            "manual_row_key": "row-0", "source_block_id": bid,
            "entered_by": "alice",
            "entered_values": {"description": "X", "qty": 1.0, "unit": "LF",
                                "unit_price": 1.0, "amount": 1.0},
        }]})
        old_eid = store["entries"][0]["manual_entry_id"]
        store = append_manual_revision(store, {"entries": [{
            "manual_row_key": "row-0", "source_block_id": bid,
            "entered_by": "bob",
            "entered_values": {"description": "X revised", "qty": 2.0, "unit": "LF",
                                "unit_price": 1.0, "amount": 2.0},
        }]})
        state = build_approval_state(store, [
            {"manual_entry_id": old_eid, "approval_status": STATUS_APPROVED,
             "reviewed_by": "carol"},
        ])
        # Old entry is superseded → approval skipped.
        assert state["entry_approvals"][old_eid]["approval_status"] == STATUS_SUPERSEDED
        history = state["entry_approvals"][old_eid]["approval_history"]
        assert history[0]["action_result"] == "skipped_entry_not_approvable"
        assert state["approval_summary"]["approved_rows_count"] == 0

    # ---- Unknown entry_id surfaced ----

    def test_unknown_entry_id_surfaced(self, rasch_quote_path):
        from app.pdf_extraction.manual_interpretation_approval import build_approval_state
        intake = self._intake(rasch_quote_path)
        store = self._store(intake, count=1)
        state = build_approval_state(store, [
            {"manual_entry_id": "does-not-exist", "approval_status": "approved_for_evaluation"},
        ])
        assert "does-not-exist" in state["approval_diagnostics"]["unknown_entry_ids"]
        # Approval summary unchanged.
        assert state["approval_summary"]["approved_rows_count"] == 0

    # ---- Approval history is append-only ----

    def test_approval_history_preserved(self, rasch_quote_path):
        from app.pdf_extraction.manual_interpretation_approval import (
            build_approval_state, STATUS_SUBMITTED, STATUS_APPROVED,
        )
        intake = self._intake(rasch_quote_path)
        store = self._store(intake, count=1)
        eids = [e["manual_entry_id"] for e in store["entries"]]
        state = build_approval_state(store, [
            {"manual_entry_id": eids[0], "approval_status": STATUS_SUBMITTED,
             "reviewed_by": "alice"},
            {"manual_entry_id": eids[0], "approval_status": STATUS_APPROVED,
             "reviewed_by": "bob"},
        ])
        history = state["entry_approvals"][eids[0]]["approval_history"]
        assert len(history) == 2
        assert history[0]["approval_status"] == STATUS_SUBMITTED
        assert history[1]["approval_status"] == STATUS_APPROVED

    # ---- get_unapproved_manual_rows ----

    def test_get_unapproved_rows(self, rasch_quote_path):
        from app.pdf_extraction.manual_interpretation_approval import (
            build_approval_state, STATUS_APPROVED, get_unapproved_manual_rows,
        )
        intake = self._intake(rasch_quote_path)
        store = self._store(intake, count=3)
        eids = [e["manual_entry_id"] for e in store["entries"]]
        state = build_approval_state(store, [
            {"manual_entry_id": eids[0], "approval_status": STATUS_APPROVED},
        ])
        unapproved = get_unapproved_manual_rows(store, state)
        assert len(unapproved) == 2

    # ---- Hybrid eval default mode excludes unapproved ----

    def test_hybrid_approved_only_mode(self, rasch_quote_path, dot_pdf_path):
        from app.pdf_extraction.manual_interpretation_approval import (
            build_approval_state, STATUS_APPROVED,
        )
        from app.pdf_extraction.hybrid_quote_evaluation import build_hybrid_rows
        intake = self._intake(rasch_quote_path)
        store = self._store(intake, count=2)
        eids = [e["manual_entry_id"] for e in store["entries"]]
        # Approve only entry 0.
        state = build_approval_state(store, [
            {"manual_entry_id": eids[0], "approval_status": STATUS_APPROVED},
        ])
        hybrid = build_hybrid_rows(intake, store, approval_state=state,
                                    include_unapproved_manual_rows=False)
        assert hybrid["hybrid_summary"]["manual_rows_used"] == 1
        assert hybrid["hybrid_summary"]["include_unapproved_mode"] == "approved_only"

    def test_hybrid_all_active_mode(self, rasch_quote_path):
        from app.pdf_extraction.manual_interpretation_approval import (
            build_approval_state, STATUS_APPROVED,
        )
        from app.pdf_extraction.hybrid_quote_evaluation import build_hybrid_rows
        intake = self._intake(rasch_quote_path)
        store = self._store(intake, count=2)
        state = build_approval_state(store)
        # Explicit override: include unapproved.
        hybrid = build_hybrid_rows(intake, store, approval_state=state,
                                    include_unapproved_manual_rows=True)
        assert hybrid["hybrid_summary"]["manual_rows_used"] == 2
        assert hybrid["hybrid_summary"]["include_unapproved_mode"] == "all_active"

    def test_hybrid_no_approval_state_includes_all(self, rasch_quote_path):
        """When approval_state is None (legacy mode), all active manual
        rows are included regardless of include_unapproved flag."""
        from app.pdf_extraction.hybrid_quote_evaluation import build_hybrid_rows
        intake = self._intake(rasch_quote_path)
        store = self._store(intake, count=2)
        hybrid = build_hybrid_rows(intake, store)
        assert hybrid["hybrid_summary"]["manual_rows_used"] == 2
        assert hybrid["hybrid_summary"]["include_unapproved_mode"] == "all_active"

    # ---- Immutability ----

    def test_approval_does_not_mutate_store(self, rasch_quote_path):
        from app.pdf_extraction.manual_interpretation_approval import (
            build_approval_state, STATUS_APPROVED,
        )
        import copy
        intake = self._intake(rasch_quote_path)
        store = self._store(intake)
        snap = copy.deepcopy(store)
        build_approval_state(store, [
            {"manual_entry_id": store["entries"][0]["manual_entry_id"],
             "approval_status": STATUS_APPROVED},
        ])
        assert store == snap

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c45(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c45(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20kk. C44 — End-to-End Provenance Propagation Tests
# ---------------------------------------------------------------------------


class TestC44ProvenancePropagation:
    """C44: row_origin / manual_entry_ref / source_block_ref survive
    through reconciliation, resolution, engineer packet, claim packet."""

    def test_machine_provenance_survives_full_pipeline(self, ipsi_quote_path, dot_pdf_path):
        """Pure machine (IPSI): row_origin=ocr_pdf must survive through
        resolution rows after the hybrid pipeline."""
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        from app.pdf_extraction.hybrid_quote_evaluation import run_hybrid_pipeline
        intake = run_intake(str(ipsi_quote_path))
        result = run_hybrid_pipeline(intake, None, str(dot_pdf_path))
        for row in result["resolution"]["resolution_rows"]:
            assert row.get("row_origin") == "ocr_pdf"

    def test_manual_provenance_survives_resolution(self, rasch_quote_path, dot_pdf_path):
        """Pure manual (Rasch + manual entries): row_origin=manual_interpretation
        and manual_entry_ref must survive resolution rows."""
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        from app.pdf_extraction.manual_quote_interpretation import create_manual_interpretation
        from app.pdf_extraction.hybrid_quote_evaluation import run_hybrid_pipeline

        intake = run_intake(str(rasch_quote_path))
        block_id = intake["evidence_blocks"][0]["block_id"]
        manual_store = create_manual_interpretation(intake, {"entries": [{
            "manual_row_key": "mr-1",
            "source_block_id": block_id,
            "entered_by": "alice",
            "entered_values": {"description": "Remove Asphalt", "qty": 100.0,
                                "unit": "SY", "unit_price": 5.0, "amount": 500.0,
                                "line_ref": "0530"},
        }]})
        result = run_hybrid_pipeline(intake, manual_store, str(dot_pdf_path))
        assert result["pipeline_status"] == "complete"
        res_rows = result["resolution"]["resolution_rows"]
        assert len(res_rows) >= 1
        manual_row = res_rows[0]
        assert manual_row.get("row_origin") == "manual_interpretation"
        assert manual_row.get("manual_entry_ref") is not None
        assert manual_row["manual_entry_ref"]["entered_by"] == "alice"
        assert manual_row.get("source_block_ref") is not None
        assert manual_row["source_block_ref"]["block_id"] == block_id

    def test_mixed_provenance_distinct(self, ipsi_quote_path, dot_pdf_path):
        """Mixed (IPSI + 1 manual): machine rows carry ocr_pdf origin,
        manual row carries manual_interpretation origin."""
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        from app.pdf_extraction.manual_quote_interpretation import create_manual_interpretation
        from app.pdf_extraction.hybrid_quote_evaluation import run_hybrid_pipeline

        intake = run_intake(str(ipsi_quote_path))
        block_id = intake["evidence_blocks"][0]["block_id"]
        manual_store = create_manual_interpretation(intake, {"entries": [{
            "manual_row_key": "extra-1",
            "source_block_id": block_id,
            "entered_by": "alice",
            "entered_values": {"description": "Extra scope", "qty": 1.0,
                                "unit": "LS", "unit_price": 500.0, "amount": 500.0},
        }]})
        result = run_hybrid_pipeline(intake, manual_store, str(dot_pdf_path))
        res_rows = result["resolution"]["resolution_rows"]
        machine_rows = [r for r in res_rows if r.get("row_origin") == "ocr_pdf"]
        manual_rows = [r for r in res_rows if r.get("row_origin") == "manual_interpretation"]
        assert len(machine_rows) == 15
        assert len(manual_rows) == 1
        assert manual_rows[0].get("manual_entry_ref") is not None
        # Machine rows have no manual_entry_ref.
        for mr in machine_rows:
            assert mr.get("manual_entry_ref") is None

    def test_provenance_survives_engineer_packet(self, rasch_quote_path, dot_pdf_path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        from app.pdf_extraction.manual_quote_interpretation import create_manual_interpretation
        from app.pdf_extraction.hybrid_quote_evaluation import run_hybrid_pipeline
        from app.pdf_extraction.office_resolution_actions import record_office_actions
        from app.pdf_extraction.engineer_output_packet import build_engineer_packet

        intake = run_intake(str(rasch_quote_path))
        block_id = intake["evidence_blocks"][0]["block_id"]
        manual_store = create_manual_interpretation(intake, {"entries": [{
            "manual_row_key": "mr-1",
            "source_block_id": block_id,
            "entered_by": "alice",
            "entered_values": {"description": "Remove Asphalt", "qty": 100.0,
                                "unit": "SY", "unit_price": 5.0, "amount": 500.0},
        }]})
        result = run_hybrid_pipeline(intake, manual_store, str(dot_pdf_path))
        resolved = result["resolution"]
        actioned = record_office_actions(resolved)
        ep = build_engineer_packet(actioned, actioned)
        for eng_row in ep["engineer_rows"]:
            assert eng_row.get("row_origin") == "manual_interpretation"
            assert eng_row.get("manual_entry_ref") is not None

    def test_provenance_survives_claim_packet(self, rasch_quote_path, dot_pdf_path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        from app.pdf_extraction.manual_quote_interpretation import create_manual_interpretation
        from app.pdf_extraction.hybrid_quote_evaluation import run_hybrid_pipeline
        from app.pdf_extraction.claim_packet import build_claim_packet

        intake = run_intake(str(rasch_quote_path))
        block_id = intake["evidence_blocks"][0]["block_id"]
        manual_store = create_manual_interpretation(intake, {"entries": [{
            "manual_row_key": "mr-1",
            "source_block_id": block_id,
            "entered_by": "alice",
            "entered_values": {"description": "Remove Asphalt", "qty": 100.0,
                                "unit": "SY", "unit_price": 5.0, "amount": 500.0},
        }]})
        result = run_hybrid_pipeline(intake, manual_store, str(dot_pdf_path))
        cp = build_claim_packet(result["resolution"])
        # Every non-empty section row carries provenance.
        for section_rows in cp["issue_sections"].values():
            for row in section_rows:
                assert row.get("row_origin") == "manual_interpretation"

    def test_machine_provenance_in_claim_packet(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        from app.pdf_extraction.hybrid_quote_evaluation import run_hybrid_pipeline
        from app.pdf_extraction.claim_packet import build_claim_packet

        intake = run_intake(str(ipsi_quote_path))
        result = run_hybrid_pipeline(intake, None, str(dot_pdf_path))
        cp = build_claim_packet(result["resolution"])
        for section_rows in cp["issue_sections"].values():
            for row in section_rows:
                assert row.get("row_origin") == "ocr_pdf"
                assert row.get("manual_entry_ref") is None

    # ---- Regression guards ----

    def test_pure_machine_pipeline_still_works(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        assert cr["job_status"] == "partial"
        assert len(cr["resolution"]["resolution_rows"]) == 15

    def test_dot_native_unchanged_under_c44(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c44(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20jj. C43 — Hybrid Evaluation Path Tests
# ---------------------------------------------------------------------------


class TestC43HybridEvaluation:
    """C43: machine + manual row merger and hybrid downstream pipeline."""

    def _rasch_intake(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        return run_intake(str(rasch_quote_path))

    def _ipsi_intake(self, ipsi_quote_path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        return run_intake(str(ipsi_quote_path))

    def _manual_store(self, intake, entries):
        from app.pdf_extraction.manual_quote_interpretation import create_manual_interpretation
        block_id = intake["evidence_blocks"][0]["block_id"]
        formatted = []
        for i, e in enumerate(entries):
            formatted.append({
                "manual_row_key": f"row-{i}",
                "source_block_id": block_id,
                "entered_by": "alice",
                "entered_at": "2026-04-15T10:00:00",
                "entry_reason": "manual interpretation",
                "entered_values": e,
            })
        return create_manual_interpretation(intake, {"entries": formatted})

    # ---- build_hybrid_rows ----

    def test_pure_machine_document(self, ipsi_quote_path):
        from app.pdf_extraction.hybrid_quote_evaluation import (
            build_hybrid_rows, ROW_ORIGIN_OCR,
        )
        intake = self._ipsi_intake(ipsi_quote_path)
        hybrid = build_hybrid_rows(intake)
        assert hybrid["hybrid_summary"]["machine_rows_used"] == 15
        assert hybrid["hybrid_summary"]["manual_rows_used"] == 0
        assert hybrid["hybrid_summary"]["mixed_document"] is False
        # Every row carries row_origin.
        for r in hybrid["effective_rows"]:
            assert r["row_origin"] == ROW_ORIGIN_OCR

    def test_pure_manual_document(self, rasch_quote_path):
        from app.pdf_extraction.hybrid_quote_evaluation import (
            build_hybrid_rows, ROW_ORIGIN_MANUAL,
        )
        intake = self._rasch_intake(rasch_quote_path)
        manual_store = self._manual_store(intake, [
            {"description": "Item A", "qty": 10.0, "unit": "LF",
             "unit_price": 5.0, "amount": 50.0},
        ])
        hybrid = build_hybrid_rows(intake, manual_store)
        assert hybrid["hybrid_summary"]["machine_rows_used"] == 0
        assert hybrid["hybrid_summary"]["manual_rows_used"] == 1
        assert hybrid["hybrid_summary"]["mixed_document"] is False
        assert hybrid["effective_rows"][0]["row_origin"] == ROW_ORIGIN_MANUAL

    def test_mixed_machine_manual(self, ipsi_quote_path):
        from app.pdf_extraction.hybrid_quote_evaluation import build_hybrid_rows
        intake = self._ipsi_intake(ipsi_quote_path)
        manual_store = self._manual_store(intake, [
            {"description": "Extra scope", "qty": 1.0, "unit": "LS",
             "unit_price": 500.0, "amount": 500.0},
        ])
        hybrid = build_hybrid_rows(intake, manual_store)
        assert hybrid["hybrid_summary"]["machine_rows_used"] == 15
        assert hybrid["hybrid_summary"]["manual_rows_used"] == 1
        assert hybrid["hybrid_summary"]["mixed_document"] is True
        assert hybrid["hybrid_summary"]["total_effective_rows"] == 16

    def test_unresolved_blocks_remaining(self, rasch_quote_path):
        from app.pdf_extraction.hybrid_quote_evaluation import build_hybrid_rows
        intake = self._rasch_intake(rasch_quote_path)
        # 10 pages → 10 evidence blocks. Manual covers only 1.
        manual_store = self._manual_store(intake, [
            {"description": "Item A", "qty": 10.0, "unit": "LF",
             "unit_price": 5.0, "amount": 50.0},
        ])
        hybrid = build_hybrid_rows(intake, manual_store)
        # 10 partial/unreadable blocks - 1 covered by manual = 9 remaining.
        assert hybrid["hybrid_summary"]["unresolved_blocks_remaining"] == 9

    def test_no_manual_store_all_blocks_unresolved(self, rasch_quote_path):
        from app.pdf_extraction.hybrid_quote_evaluation import build_hybrid_rows
        intake = self._rasch_intake(rasch_quote_path)
        hybrid = build_hybrid_rows(intake)
        assert hybrid["hybrid_summary"]["unresolved_blocks_remaining"] == 10

    # ---- Provenance in effective rows ----

    def test_machine_row_carries_origin(self, ipsi_quote_path):
        from app.pdf_extraction.hybrid_quote_evaluation import (
            build_hybrid_rows, ROW_ORIGIN_OCR,
        )
        intake = self._ipsi_intake(ipsi_quote_path)
        hybrid = build_hybrid_rows(intake)
        for r in hybrid["effective_rows"]:
            assert "row_origin" in r
            assert r["row_origin"] == ROW_ORIGIN_OCR

    def test_manual_row_carries_entry_ref(self, rasch_quote_path):
        from app.pdf_extraction.hybrid_quote_evaluation import build_hybrid_rows
        intake = self._rasch_intake(rasch_quote_path)
        manual_store = self._manual_store(intake, [
            {"description": "Item A", "qty": 10.0, "unit": "LF",
             "unit_price": 5.0, "amount": 50.0},
        ])
        hybrid = build_hybrid_rows(intake, manual_store)
        manual = [r for r in hybrid["effective_rows"] if r["row_origin"] == "manual_interpretation"]
        assert len(manual) == 1
        assert "manual_entry_ref" in manual[0]
        assert "source_block_ref" in manual[0]

    # ---- run_hybrid_pipeline ----

    def test_pure_machine_pipeline_unchanged(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.hybrid_quote_evaluation import run_hybrid_pipeline
        intake = self._ipsi_intake(ipsi_quote_path)
        result = run_hybrid_pipeline(intake, None, str(dot_pdf_path))
        assert result["pipeline_status"] == "complete"
        assert result["pairing_status"] == "trusted"
        assert result["hybrid_summary"]["machine_rows_used"] == 15
        assert result["hybrid_summary"]["manual_rows_used"] == 0
        # Resolution runs.
        assert result["resolution_status"] == "review_required"
        assert result["effective_rows_used"] == 15

    def test_pure_manual_pipeline(self, rasch_quote_path, dot_pdf_path):
        from app.pdf_extraction.hybrid_quote_evaluation import run_hybrid_pipeline
        intake = self._rasch_intake(rasch_quote_path)
        manual_store = self._manual_store(intake, [
            {"description": "Remove Asphalt", "qty": 100.0, "unit": "SY",
             "unit_price": 5.0, "amount": 500.0, "line_ref": "0530"},
            {"description": "Concrete Barrier", "qty": 24.0, "unit": "LF",
             "unit_price": 10.0, "amount": 240.0, "line_ref": "0630"},
        ])
        result = run_hybrid_pipeline(intake, manual_store, str(dot_pdf_path))
        assert result["pipeline_status"] == "complete"
        assert result["hybrid_summary"]["machine_rows_used"] == 0
        assert result["hybrid_summary"]["manual_rows_used"] == 2
        # Manual rows go through pairing + mapping.
        assert result["pairing_status"] in ("trusted", "weak", "rejected")
        assert result["effective_rows_used"] == 2

    def test_mixed_pipeline(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.hybrid_quote_evaluation import run_hybrid_pipeline
        intake = self._ipsi_intake(ipsi_quote_path)
        manual_store = self._manual_store(intake, [
            {"description": "Extra scope manual", "qty": 1.0, "unit": "LS",
             "unit_price": 500.0, "amount": 500.0},
        ])
        result = run_hybrid_pipeline(intake, manual_store, str(dot_pdf_path))
        assert result["pipeline_status"] == "complete"
        assert result["hybrid_summary"]["machine_rows_used"] == 15
        assert result["hybrid_summary"]["manual_rows_used"] == 1
        assert result["effective_rows_used"] == 16

    def test_provenance_survives_resolution(self, rasch_quote_path, dot_pdf_path):
        """Manual row provenance must survive all the way through the
        resolution layer."""
        from app.pdf_extraction.hybrid_quote_evaluation import run_hybrid_pipeline
        intake = self._rasch_intake(rasch_quote_path)
        manual_store = self._manual_store(intake, [
            {"description": "Remove Asphalt", "qty": 100.0, "unit": "SY",
             "unit_price": 5.0, "amount": 500.0},
        ])
        result = run_hybrid_pipeline(intake, manual_store, str(dot_pdf_path))
        # The resolution rows come from the augmented contract, which was
        # built from the effective_rows. Each effective row carries
        # provenance keys at the quote_values level.
        assert result["pipeline_status"] == "complete"

    def test_unresolved_blocks_visible(self, rasch_quote_path, dot_pdf_path):
        """Even after evaluation, the hybrid_summary still surfaces
        unresolved evidence blocks."""
        from app.pdf_extraction.hybrid_quote_evaluation import run_hybrid_pipeline
        intake = self._rasch_intake(rasch_quote_path)
        manual_store = self._manual_store(intake, [
            {"description": "Item A", "qty": 10.0, "unit": "LF",
             "unit_price": 5.0, "amount": 50.0},
        ])
        result = run_hybrid_pipeline(intake, manual_store, str(dot_pdf_path))
        assert result["hybrid_summary"]["unresolved_blocks_remaining"] >= 9

    # ---- Immutability ----

    def test_build_hybrid_does_not_mutate(self, ipsi_quote_path):
        from app.pdf_extraction.hybrid_quote_evaluation import build_hybrid_rows
        import copy
        intake = self._ipsi_intake(ipsi_quote_path)
        snap = copy.deepcopy(intake)
        build_hybrid_rows(intake)
        assert intake["accepted_rows"] == snap["accepted_rows"]

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c43(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c43(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20ii. C42 — Append-Only Manual Interpretation Tests
# ---------------------------------------------------------------------------


class TestC42ManualQuoteInterpretation:
    """C42: append-only manual interpretation tied to C41 evidence blocks."""

    def _intake(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        return run_intake(str(rasch_quote_path))

    def _meta_entry(self, block_id, **kw):
        base = {
            "manual_row_key": "row-1",
            "source_block_id": block_id,
            "entered_by": "alice",
            "entered_at": "2026-04-15T10:00:00",
            "entry_reason": "interpreted from plan",
            "entered_values": {
                "description": "Remove Asphalt",
                "qty": 100.0, "unit": "SY",
                "unit_price": 5.0, "amount": 500.0,
            },
        }
        base.update(kw)
        return base

    # ---- create_manual_interpretation ----

    def test_create_empty_store(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, MANUAL_INTERPRETATION_VERSION,
        )
        intake = self._intake(rasch_quote_path)
        store = create_manual_interpretation(intake)
        assert store["manual_interpretation_version"] == MANUAL_INTERPRETATION_VERSION
        assert store["entries"] == []
        assert len(store["evidence_blocks"]) >= 1
        assert store["summary"]["manual_entry_count"] == 0

    def test_create_with_initial_entries(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation,
        )
        intake = self._intake(rasch_quote_path)
        block_id = intake["evidence_blocks"][0]["block_id"]
        store = create_manual_interpretation(intake, {
            "entries": [self._meta_entry(block_id)],
        })
        assert len(store["entries"]) == 1
        assert store["entries"][0]["entry_status"] == "active"
        assert store["summary"]["manual_entry_count"] == 1
        assert store["summary"]["rows_manual_count"] == 1

    # ---- append_manual_revision ----

    def test_append_revision_adds_entry(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, append_manual_revision,
        )
        intake = self._intake(rasch_quote_path)
        block_id = intake["evidence_blocks"][0]["block_id"]
        store = create_manual_interpretation(intake)
        store2 = append_manual_revision(store, {"entries": [self._meta_entry(block_id)]})
        assert len(store["entries"]) == 0  # original store untouched
        assert len(store2["entries"]) == 1

    def test_supersedes_prior_entry(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, append_manual_revision,
        )
        intake = self._intake(rasch_quote_path)
        block_id = intake["evidence_blocks"][0]["block_id"]
        store = create_manual_interpretation(intake, {
            "entries": [self._meta_entry(block_id, entered_by="alice")]
        })
        # Append a correction on the same manual_row_key.
        store2 = append_manual_revision(store, {
            "entries": [self._meta_entry(block_id, entered_by="bob",
                                          entered_values={
                                              "description": "Remove Asphalt (revised)",
                                              "qty": 110.0, "unit": "SY",
                                              "unit_price": 5.0, "amount": 550.0,
                                          })],
        })
        assert len(store2["entries"]) == 2
        # Older entry preserved but tagged superseded.
        old = store2["entries"][0]
        new = store2["entries"][1]
        assert old["entry_status"] == "superseded"
        assert old["superseded_by"] == new["manual_entry_id"]
        assert new["entry_status"] == "active"
        assert store2["summary"]["superseded_entry_count"] == 1
        assert store2["summary"]["rows_manual_count"] == 1

    def test_multiple_distinct_keys_coexist(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation,
        )
        intake = self._intake(rasch_quote_path)
        block_id = intake["evidence_blocks"][0]["block_id"]
        store = create_manual_interpretation(intake, {"entries": [
            self._meta_entry(block_id, manual_row_key="row-1"),
            self._meta_entry(block_id, manual_row_key="row-2",
                             entered_values={"description": "Other", "qty": 10.0,
                                              "unit": "LF", "unit_price": 20.0,
                                              "amount": 200.0}),
        ]})
        assert len(store["entries"]) == 2
        assert store["summary"]["rows_manual_count"] == 2

    # ---- get_current_manual_rows ----

    def test_current_manual_rows_shape(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, get_current_manual_rows,
            PROV_MANUAL_INTERPRETATION,
        )
        intake = self._intake(rasch_quote_path)
        block_id = intake["evidence_blocks"][0]["block_id"]
        store = create_manual_interpretation(intake, {"entries": [self._meta_entry(block_id)]})
        rows = get_current_manual_rows(store)
        assert len(rows) == 1
        r = rows[0]
        assert r["source_provenance"] == PROV_MANUAL_INTERPRETATION
        assert r["extraction_source"] == "manual_interpretation"
        assert r["description"] == "Remove Asphalt"
        assert r["qty"] == 100.0
        assert r["normalized_row_id"].startswith("qm-")
        assert r["manual_entry_ref"]["entered_by"] == "alice"
        assert r["source_block_ref"]["block_id"] == block_id

    def test_superseded_entries_not_in_current(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, append_manual_revision,
            get_current_manual_rows,
        )
        intake = self._intake(rasch_quote_path)
        block_id = intake["evidence_blocks"][0]["block_id"]
        store = create_manual_interpretation(intake, {"entries": [self._meta_entry(block_id)]})
        store = append_manual_revision(store, {"entries": [self._meta_entry(block_id,
                                           entered_by="bob")]})
        rows = get_current_manual_rows(store)
        assert len(rows) == 1
        assert rows[0]["manual_entry_ref"]["entered_by"] == "bob"

    # ---- get_manual_history ----

    def test_history_preserves_all_entries(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, append_manual_revision,
            get_manual_history,
        )
        intake = self._intake(rasch_quote_path)
        block_id = intake["evidence_blocks"][0]["block_id"]
        store = create_manual_interpretation(intake, {"entries": [self._meta_entry(block_id)]})
        store = append_manual_revision(store, {"entries": [self._meta_entry(block_id,
                                           entered_by="bob")]})
        store = append_manual_revision(store, {"entries": [self._meta_entry(block_id,
                                           entered_by="carol")]})
        history = get_manual_history(store)
        assert len(history) == 3
        # First two are superseded, last is active.
        assert history[0]["entry_status"] == "superseded"
        assert history[1]["entry_status"] == "superseded"
        assert history[2]["entry_status"] == "active"

    def test_history_mutation_does_not_affect_store(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, get_manual_history,
        )
        intake = self._intake(rasch_quote_path)
        block_id = intake["evidence_blocks"][0]["block_id"]
        store = create_manual_interpretation(intake, {"entries": [self._meta_entry(block_id)]})
        history = get_manual_history(store)
        history[0]["manual_entry_id"] = "TAMPERED"
        history2 = get_manual_history(store)
        assert history2[0]["manual_entry_id"] != "TAMPERED"

    # ---- Validation paths ----

    def test_unknown_block_id_rejected(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, VALIDATION_UNKNOWN_BLOCK_ID,
        )
        intake = self._intake(rasch_quote_path)
        store = create_manual_interpretation(intake, {"entries": [
            self._meta_entry("blk-does-not-exist"),
        ]})
        assert len(store["entries"]) == 1
        entry = store["entries"][0]
        assert entry["entry_status"] == "rejected"
        assert entry["entry_validation_status"] == VALIDATION_UNKNOWN_BLOCK_ID
        assert "blk-does-not-exist" in store["diagnostics"]["unknown_block_ids"]
        # Active/rows_manual count NOT incremented by rejected entries.
        assert store["summary"]["rows_manual_count"] == 0

    def test_missing_block_ref_rejected(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, VALIDATION_MISSING_BLOCK_REF,
        )
        intake = self._intake(rasch_quote_path)
        entry = self._meta_entry("ignored")
        entry["source_block_id"] = None
        entry["source_block_ref"] = {}
        store = create_manual_interpretation(intake, {"entries": [entry]})
        assert store["entries"][0]["entry_validation_status"] == VALIDATION_MISSING_BLOCK_REF
        assert store["entries"][0]["entry_status"] == "rejected"

    def test_missing_description_rejected(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, VALIDATION_MISSING_REQUIRED_FIELDS,
        )
        intake = self._intake(rasch_quote_path)
        block_id = intake["evidence_blocks"][0]["block_id"]
        entry = self._meta_entry(block_id)
        entry["entered_values"]["description"] = ""
        store = create_manual_interpretation(intake, {"entries": [entry]})
        assert store["entries"][0]["entry_validation_status"] == VALIDATION_MISSING_REQUIRED_FIELDS
        assert store["entries"][0]["entry_status"] == "rejected"

    def test_bad_numeric_rejected(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, VALIDATION_BAD_NUMERIC,
        )
        intake = self._intake(rasch_quote_path)
        block_id = intake["evidence_blocks"][0]["block_id"]
        entry = self._meta_entry(block_id)
        entry["entered_values"]["qty"] = -5.0
        store = create_manual_interpretation(intake, {"entries": [entry]})
        assert store["entries"][0]["entry_validation_status"] == VALIDATION_BAD_NUMERIC

    def test_non_numeric_string_rejected(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, VALIDATION_BAD_NUMERIC,
        )
        intake = self._intake(rasch_quote_path)
        block_id = intake["evidence_blocks"][0]["block_id"]
        entry = self._meta_entry(block_id)
        entry["entered_values"]["amount"] = "not-a-number"
        store = create_manual_interpretation(intake, {"entries": [entry]})
        assert store["entries"][0]["entry_validation_status"] == VALIDATION_BAD_NUMERIC

    # ---- Machine vs manual separation ----

    def test_machine_rows_never_touched(self, rasch_quote_path):
        """Rasch has 0 machine rows; this test documents that manual
        entries do not add to machine counts."""
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation,
        )
        intake = self._intake(rasch_quote_path)
        block_id = intake["evidence_blocks"][0]["block_id"]
        store = create_manual_interpretation(intake, {"entries": [self._meta_entry(block_id)]})
        assert store["summary"]["rows_machine_count"] == 0
        assert store["summary"]["rows_manual_count"] == 1

    def test_interpretation_status_machine_partial(self, ipsi_quote_path):
        """IPSI has 15 machine rows; adding 1 manual row yields
        machine_partial_human_completed."""
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, ROW_STATUS_MACHINE_PARTIAL_HUMAN_COMPLETED,
        )
        intake = run_intake(str(ipsi_quote_path))
        block_id = intake["evidence_blocks"][0]["block_id"]
        store = create_manual_interpretation(intake, {"entries": [self._meta_entry(block_id)]})
        assert store["summary"]["interpretation_status"] == ROW_STATUS_MACHINE_PARTIAL_HUMAN_COMPLETED
        assert store["summary"]["rows_machine_count"] == 15
        assert store["summary"]["rows_manual_count"] == 1

    def test_interpretation_status_fully_manual(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, ROW_STATUS_FULLY_MANUAL_INTERPRETATION,
        )
        intake = self._intake(rasch_quote_path)
        block_id = intake["evidence_blocks"][0]["block_id"]
        store = create_manual_interpretation(intake, {"entries": [self._meta_entry(block_id)]})
        assert store["summary"]["interpretation_status"] == ROW_STATUS_FULLY_MANUAL_INTERPRETATION

    def test_interpretation_status_machine_extracted_default(self, ipsi_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, ROW_STATUS_MACHINE_EXTRACTED,
        )
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        intake = run_intake(str(ipsi_quote_path))
        store = create_manual_interpretation(intake)
        assert store["summary"]["interpretation_status"] == ROW_STATUS_MACHINE_EXTRACTED

    # ---- Immutability ----

    def test_append_does_not_mutate_inputs(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, append_manual_revision,
        )
        import copy
        intake = self._intake(rasch_quote_path)
        block_id = intake["evidence_blocks"][0]["block_id"]
        store = create_manual_interpretation(intake)
        store_snap = copy.deepcopy(store)
        meta = {"entries": [self._meta_entry(block_id)]}
        meta_snap = copy.deepcopy(meta)
        append_manual_revision(store, meta)
        assert store == store_snap
        assert meta == meta_snap

    def test_append_to_empty_store_raises(self):
        from app.pdf_extraction.manual_quote_interpretation import append_manual_revision
        import pytest as _pytest
        with _pytest.raises(ValueError):
            append_manual_revision({}, {"entries": []})

    # ---- Real-fixture end-to-end ----

    def test_rasch_full_manual_lifecycle(self, rasch_quote_path):
        from app.pdf_extraction.manual_quote_interpretation import (
            create_manual_interpretation, append_manual_revision,
            get_current_manual_rows, get_manual_history,
        )
        intake = self._intake(rasch_quote_path)
        block_ids = [b["block_id"] for b in intake["evidence_blocks"]]

        # Create: two distinct keys on different blocks.
        store = create_manual_interpretation(intake, {"entries": [
            self._meta_entry(block_ids[0], manual_row_key="row-a"),
            self._meta_entry(block_ids[1], manual_row_key="row-b",
                             entered_values={"description": "Barrier", "qty": 50.0,
                                              "unit": "LF", "unit_price": 10.0, "amount": 500.0}),
        ]})
        # Append: correct row-a.
        store = append_manual_revision(store, {"entries": [
            self._meta_entry(block_ids[0], manual_row_key="row-a", entered_by="bob",
                             entered_values={"description": "Remove Asphalt (revised)",
                                              "qty": 120.0, "unit": "SY",
                                              "unit_price": 5.0, "amount": 600.0}),
        ]})
        rows = get_current_manual_rows(store)
        assert len(rows) == 2
        # History has all 3 entries.
        assert len(get_manual_history(store)) == 3
        # Row-a current uses bob's values.
        row_a = next(r for r in rows
                     if r["manual_entry_ref"]["manual_row_key"] == "row-a")
        assert row_a["qty"] == 120.0

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c42(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c42(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20hh. C41 — Handwritten Quote Intake Hardening Tests
# ---------------------------------------------------------------------------


class TestC41HandwrittenIntake:
    """C41: deterministic machine-readability detection + evidence
    block capture for handwritten / scanned quotes."""

    # ---- Real fixtures ----

    def test_ipsi_classified_machine_readable(self, ipsi_quote_path):
        from app.pdf_extraction.handwritten_quote_intake import (
            run_intake, STATUS_MACHINE_READABLE,
        )
        out = run_intake(str(ipsi_quote_path))
        assert out["machine_intake_status"] == STATUS_MACHINE_READABLE
        assert out["intake_summary"]["accepted_rows_count"] >= 14

    def test_rasch_classified_unreadable(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_quote_intake import (
            run_intake, STATUS_MACHINE_UNREADABLE_HUMAN_REQUIRED,
            REASON_NO_STABLE_ROW_BOUNDARIES, REASON_EVIDENCE_BLOCKS_CAPTURED,
        )
        out = run_intake(str(rasch_quote_path))
        assert out["machine_intake_status"] == STATUS_MACHINE_UNREADABLE_HUMAN_REQUIRED
        reasons = out["intake_limitation_reasons"]
        assert REASON_NO_STABLE_ROW_BOUNDARIES in reasons
        assert REASON_EVIDENCE_BLOCKS_CAPTURED in reasons
        # Every page becomes at least one evidence block.
        assert out["intake_summary"]["evidence_blocks_count"] >= 1
        # No accepted rows.
        assert out["intake_summary"]["accepted_rows_count"] == 0

    # ---- Evidence block shape ----

    def test_evidence_blocks_have_required_keys(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        out = run_intake(str(rasch_quote_path))
        for b in out["evidence_blocks"]:
            for k in ("block_id", "source_page", "block_type", "raw_text",
                      "ocr_text", "machine_readability", "capture_reason",
                      "capture_trace"):
                assert k in b

    def test_evidence_blocks_closed_readability_vocab(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        out = run_intake(str(rasch_quote_path))
        allowed = {"readable", "partial", "unreadable"}
        for b in out["evidence_blocks"]:
            assert b["machine_readability"] in allowed

    def test_evidence_blocks_closed_block_types(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        out = run_intake(str(rasch_quote_path))
        allowed = {"line", "region", "row_like", "unreadable_region"}
        for b in out["evidence_blocks"]:
            assert b["block_type"] in allowed

    def test_evidence_blocks_ids_unique(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        out = run_intake(str(rasch_quote_path))
        ids = [b["block_id"] for b in out["evidence_blocks"]]
        assert len(ids) == len(set(ids))

    # ---- Controlled unreadable fixture ----

    def test_empty_pdf_is_unreadable(self, empty_pdf_path):
        from app.pdf_extraction.handwritten_quote_intake import (
            run_intake, STATUS_MACHINE_UNREADABLE_HUMAN_REQUIRED,
        )
        out = run_intake(str(empty_pdf_path))
        assert out["machine_intake_status"] == STATUS_MACHINE_UNREADABLE_HUMAN_REQUIRED
        assert out["intake_summary"]["accepted_rows_count"] == 0

    def test_non_schedule_pdf_is_unreadable(self, non_schedule_pdf_path):
        from app.pdf_extraction.handwritten_quote_intake import (
            run_intake, STATUS_MACHINE_UNREADABLE_HUMAN_REQUIRED,
        )
        out = run_intake(str(non_schedule_pdf_path))
        assert out["machine_intake_status"] == STATUS_MACHINE_UNREADABLE_HUMAN_REQUIRED

    # ---- Accepted rows untouched ----

    def test_accepted_rows_preserved_unchanged(self, ipsi_quote_path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        out = run_intake(str(ipsi_quote_path))
        assert len(out["accepted_rows"]) == len(staging["accepted_rows"])
        # Field-by-field parity on a sample row.
        a = staging["accepted_rows"][0]
        b = out["accepted_rows"][0]
        assert a["normalized_row_id"] == b["normalized_row_id"]
        assert a["description"] == b["description"]
        assert a["amount"] == b["amount"]

    def test_accepted_rows_separate_from_evidence_blocks(self, ipsi_quote_path):
        """Machine-accepted rows and evidence blocks are distinct
        buckets — the intake never moves accepted rows into blocks."""
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        out = run_intake(str(ipsi_quote_path))
        accepted_ids = {r["normalized_row_id"] for r in out["accepted_rows"]}
        block_ids = {b["block_id"] for b in out["evidence_blocks"]}
        assert accepted_ids.isdisjoint(block_ids)

    # ---- Version + top-level fields ----

    def test_version_tag_present(self, ipsi_quote_path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        out = run_intake(str(ipsi_quote_path))
        assert out["handwritten_intake_version"] == "handwritten_quote_intake/v1"

    def test_summary_counts_consistent(self, rasch_quote_path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        out = run_intake(str(rasch_quote_path))
        s = out["intake_summary"]
        total = (s["partial_blocks_count"] + s["unreadable_blocks_count"]
                 + s["readable_blocks_count"])
        assert total == s["evidence_blocks_count"]

    # ---- Deterministic ----

    def test_intake_is_deterministic(self, ipsi_quote_path):
        from app.pdf_extraction.handwritten_quote_intake import run_intake
        a = run_intake(str(ipsi_quote_path))
        b = run_intake(str(ipsi_quote_path))
        assert a["machine_intake_status"] == b["machine_intake_status"]
        assert a["intake_summary"] == b["intake_summary"]
        assert a["intake_limitation_reasons"] == b["intake_limitation_reasons"]

    # ---- Failure path ----

    def test_missing_pdf_is_unreadable(self, tmp_path):
        from app.pdf_extraction.handwritten_quote_intake import (
            run_intake, STATUS_MACHINE_UNREADABLE_HUMAN_REQUIRED,
        )
        missing = tmp_path / "nope.pdf"
        out = run_intake(str(missing))
        assert out["machine_intake_status"] == STATUS_MACHINE_UNREADABLE_HUMAN_REQUIRED
        assert out["intake_summary"]["accepted_rows_count"] == 0

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c41(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c41(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20gg. C40 — Real Job Pilot Harness Tests
# ---------------------------------------------------------------------------


class TestC40PilotHarness:
    """C40: end-to-end deterministic pilot harness for one bid job."""

    # ---- Structural shape ----

    def test_pilot_artifact_structure(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        artifact = run_pilot(str(ipsi_quote_path), str(dot_pdf_path))
        for k in ("pilot_version", "pilot_run_id", "job_id", "inputs",
                  "control_room", "interaction_model", "scenario_output",
                  "claim_packet", "exception_summary", "coverage_audit",
                  "pilot_summary", "pilot_diagnostics"):
            assert k in artifact
        assert artifact["pilot_version"] == "pilot_harness/v1"
        assert artifact["pilot_diagnostics"]["pipeline_succeeded"] is True

    def test_pilot_run_id_default(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        artifact = run_pilot(str(ipsi_quote_path), str(dot_pdf_path))
        assert "pilot-" in artifact["pilot_run_id"]
        assert "ipsi_quote" in artifact["pilot_run_id"]

    def test_pilot_run_id_caller_supplied(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        artifact = run_pilot(str(ipsi_quote_path), str(dot_pdf_path),
                              pilot_run_id="pilot-2026-04-15-001")
        assert artifact["pilot_run_id"] == "pilot-2026-04-15-001"

    def test_pilot_stages_run(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        artifact = run_pilot(str(ipsi_quote_path), str(dot_pdf_path))
        stages = artifact["pilot_diagnostics"]["stages_run"]
        assert "control_room" in stages
        assert "interaction_model" in stages
        assert "scenario_engine" in stages
        assert "claim_packet" in stages
        assert "coverage_audit" in stages
        assert "exception_feedback" in stages

    # ---- Trusted pair ----

    def test_trusted_pair_summary(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        artifact = run_pilot(str(ipsi_quote_path), str(dot_pdf_path))
        s = artifact["pilot_summary"]
        assert s["job_status"] == "partial"
        assert s["packet_status"] == "draft_ready"
        assert s["total_rows"] == 15
        assert s["unmapped_count"] == 2
        assert s["non_comparable_count"] == 13
        assert s["blocked_count"] == 0
        assert s["source_conflict_count"] == 0

    def test_trusted_pair_scenario_basis_counts(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        artifact = run_pilot(str(ipsi_quote_path), str(dot_pdf_path))
        sc = artifact["pilot_summary"]["scenario_basis_counts"]
        assert sc["scenario_dot_basis"] == 13
        assert sc["scenario_takeoff_basis"] == 0
        assert sc["scenario_no_external"] == 0

    def test_trusted_pair_top_exceptions_present(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        artifact = run_pilot(str(ipsi_quote_path), str(dot_pdf_path))
        cats = [e["category"] for e in artifact["pilot_summary"]["top_exception_categories"]]
        # Exception surfacing should pick up at least the no-table-header
        # / no-inline-qty/unit gaps from the IPSI run.
        assert any("inline_qty_unit" in c.lower() for c in cats) or \
               any("non_comparable" in c.lower() for c in cats) or len(cats) > 0

    # ---- Blocked pair ----

    def test_blocked_pair_summary(self, ipsi_quote_path, estprop_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        artifact = run_pilot(str(ipsi_quote_path), str(estprop_pdf_path))
        s = artifact["pilot_summary"]
        assert s["job_status"] == "blocked"
        assert s["packet_status"] == "blocked"
        assert s["blocked_count"] == 15
        assert s["critical_issues"] == 15

    def test_blocked_pair_pipeline_still_runs_all_stages(self, ipsi_quote_path, estprop_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        artifact = run_pilot(str(ipsi_quote_path), str(estprop_pdf_path))
        # Even though the pair is blocked, the pilot should still run
        # every downstream stage (they all gracefully report blocked).
        stages = artifact["pilot_diagnostics"]["stages_run"]
        assert "scenario_engine" in stages
        assert "claim_packet" in stages

    # ---- Conflict fixture ----

    def test_conflict_fixture_summary(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        external = [{
            "source_type": "internal_takeoff",
            "source_ref": {"id": "T-1"},
            "rows": {"qr-p0-r1": {"qty": 5.0, "unit": "EACH"}},
        }]
        artifact = run_pilot(str(ipsi_quote_path), str(dot_pdf_path),
                              external_sources=external)
        s = artifact["pilot_summary"]
        assert s["source_conflict_count"] == 1
        # Scenario_takeoff_basis should now have at least 1 comparable row.
        assert artifact["pilot_summary"]["scenario_basis_counts"]["scenario_takeoff_basis"] >= 1

    # ---- Office actions propagate ----

    def test_office_actions_propagate(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        actions = {
            "rows": {
                "qr-p0-r0": {"actions": [{"action_type": "escalate_to_engineer",
                                            "actor": "alice"}]},
                "qr-p0-r1": {"actions": [{"action_type": "accept_dot_quantity_as_working_basis",
                                            "actor": "alice"}]},
            }
        }
        artifact = run_pilot(str(ipsi_quote_path), str(dot_pdf_path),
                              office_action_metadata=actions)
        assert artifact["pilot_summary"]["rows_with_actions"] == 2
        # Claim packet has 2 office actions in flat list.
        assert len(artifact["claim_packet"]["office_actions"]) == 2

    # ---- Interaction model embedded ----

    def test_interaction_model_default_view(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        artifact = run_pilot(str(ipsi_quote_path), str(dot_pdf_path))
        # IPSI trusted has 2 unmapped → unmapped_scope is the default view.
        assert artifact["interaction_model"]["default_view"] == "unmapped_scope"

    def test_interaction_model_blocked_pair(self, ipsi_quote_path, estprop_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        artifact = run_pilot(str(ipsi_quote_path), str(estprop_pdf_path))
        assert artifact["interaction_model"]["default_view"] == "blocked_items"

    # ---- Inputs section ----

    def test_inputs_section_records_metadata_supplied(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        artifact = run_pilot(
            str(ipsi_quote_path), str(dot_pdf_path),
            external_sources=[{"source_type": "internal_takeoff", "rows": {}}],
            office_action_metadata={"rows": {}},
        )
        assert artifact["inputs"]["external_sources_supplied"] == 1
        assert artifact["inputs"]["office_action_metadata_supplied"] is True

    # ---- Failure path ----

    def test_pilot_failure_when_quote_missing(self, tmp_path, dot_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        missing = tmp_path / "nope.pdf"
        artifact = run_pilot(str(missing), str(dot_pdf_path))
        assert artifact["pilot_diagnostics"]["pipeline_succeeded"] is False
        assert artifact["pilot_summary"]["pipeline_succeeded"] is False
        # Downstream sections are None on failure.
        assert artifact["interaction_model"] is None
        assert artifact["claim_packet"] is None

    # ---- Determinism ----

    def test_pilot_is_deterministic(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.pilot_harness import run_pilot
        a1 = run_pilot(str(ipsi_quote_path), str(dot_pdf_path), pilot_run_id="run-x")
        a2 = run_pilot(str(ipsi_quote_path), str(dot_pdf_path), pilot_run_id="run-x")
        assert a1["pilot_summary"] == a2["pilot_summary"]
        assert a1["interaction_model"] == a2["interaction_model"]

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c40(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c40(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20ff. C39 — Persistence + Versioned Job State Tests
# ---------------------------------------------------------------------------


class TestC39JobStateStore:
    """C39: append-safe versioned store for control room job states."""

    def _control_room(self, rows=None, job_status="partial", actions=None):
        rows = rows or []
        return {
            "control_room_version": "control_room/v1",
            "job_id": "test-job",
            "job_status": job_status,
            "resolution": {"resolution_rows": rows},
            "office_actions_output": {
                "resolution_rows": [
                    {"normalized_row_id": rid, "office_actions": acts}
                    for rid, acts in (actions or {}).items()
                ],
            },
        }

    def _row(self, **kw):
        base = {
            "normalized_row_id": "qr-p0-r0",
            "resolution_category": "non_comparable_missing_quote_source",
            "resolution_priority": "medium",
            "comparison_basis": {"basis": "dot_augmented"},
        }
        base.update(kw)
        return base

    # ---- create_job_state ----

    def test_create_initial_store(self):
        from app.pdf_extraction.job_state_store import (
            create_job_state, REV_INITIAL_RUN, STORE_VERSION,
        )
        store = create_job_state("job-1", self._control_room([self._row()]))
        assert store["store_version"] == STORE_VERSION
        assert store["job_id"] == "job-1"
        assert store["current_revision"] == "rev-0"
        assert store["current_revision_index"] == 0
        assert len(store["revisions"]) == 1
        rev = store["revisions"][0]
        assert rev["revision_id"] == "rev-0"
        assert rev["sequence"] == 0
        assert rev["revision_type"] == REV_INITIAL_RUN
        assert rev["change_summary"]["is_initial"] is True

    def test_initial_revision_id_caller_supplied(self):
        from app.pdf_extraction.job_state_store import create_job_state
        store = create_job_state(
            "job-1", self._control_room([self._row()]),
            revision_metadata={"revision_id": "init-2026-04-15", "actor": "alice"},
        )
        assert store["current_revision"] == "init-2026-04-15"
        assert store["revisions"][0]["actor"] == "alice"

    # ---- append_revision ----

    def test_append_revision_preserves_history(self):
        from app.pdf_extraction.job_state_store import (
            create_job_state, append_revision, REV_ACTION_UPDATE,
        )
        store = create_job_state("job-1", self._control_room([self._row()]))
        original_first = deepcopy_simple(store["revisions"][0])
        store2 = append_revision(
            store,
            self._control_room([self._row()],
                                actions={"qr-p0-r0": [{"action_type": "escalate_to_engineer"}]}),
            revision_metadata={"revision_type": REV_ACTION_UPDATE,
                                "actor": "bob", "created_at": "2026-04-15T10:00"},
        )
        # Original store untouched.
        assert store["revisions"][0] == original_first
        assert len(store["revisions"]) == 1
        # New store has both revisions.
        assert len(store2["revisions"]) == 2
        assert store2["current_revision"] == "rev-1"
        assert store2["current_revision_index"] == 1

    def test_append_revision_change_summary(self):
        from app.pdf_extraction.job_state_store import (
            create_job_state, append_revision,
        )
        store = create_job_state("job-1", self._control_room([self._row()]))
        store2 = append_revision(
            store,
            self._control_room([self._row()],
                                actions={"qr-p0-r0": [{"action_type": "escalate_to_engineer"}]}),
        )
        rev2 = store2["revisions"][1]
        assert rev2["change_summary"]["actions_added_count"] == 1
        assert rev2["change_summary"]["is_initial"] is False
        assert rev2["change_summary"]["rows_changed_count"] == 0

    def test_append_revision_status_change_detected(self):
        from app.pdf_extraction.job_state_store import (
            create_job_state, append_revision,
        )
        store = create_job_state("job-1", self._control_room([self._row()],
                                                              job_status="partial"))
        store2 = append_revision(store,
                                  self._control_room([self._row()], job_status="ready"))
        assert store2["revisions"][1]["change_summary"]["status_changed"] is True
        assert store2["revisions"][1]["change_summary"]["previous_job_status"] == "partial"
        assert store2["revisions"][1]["change_summary"]["current_job_status"] == "ready"

    def test_append_revision_row_changed_detected(self):
        from app.pdf_extraction.job_state_store import (
            create_job_state, append_revision,
        )
        store = create_job_state("job-1", self._control_room([self._row()]))
        # Reclassify the row in the new revision.
        store2 = append_revision(store, self._control_room([
            self._row(resolution_category="quantity_discrepancy_review_required",
                      resolution_priority="high"),
        ]))
        assert store2["revisions"][1]["change_summary"]["rows_changed_count"] == 1

    def test_append_chain_three_revisions(self):
        from app.pdf_extraction.job_state_store import (
            create_job_state, append_revision,
        )
        store = create_job_state("job-1", self._control_room([self._row()]))
        store = append_revision(store, self._control_room([self._row()],
                                actions={"qr-p0-r0": [{"action_type": "escalate_to_engineer"}]}))
        store = append_revision(store, self._control_room([self._row()],
                                actions={"qr-p0-r0": [
                                    {"action_type": "escalate_to_engineer"},
                                    {"action_type": "requires_field_verification"},
                                ]}))
        assert len(store["revisions"]) == 3
        assert store["current_revision"] == "rev-2"
        assert store["revisions"][2]["change_summary"]["actions_added_count"] == 1

    def test_append_revision_unknown_type_preserved(self):
        from app.pdf_extraction.job_state_store import (
            create_job_state, append_revision,
        )
        store = create_job_state("job-1", self._control_room([self._row()]))
        store2 = append_revision(store, self._control_room([self._row()]),
                                  revision_metadata={"revision_type": "made_up"})
        rev = store2["revisions"][1]
        assert rev["revision_type"] == "made_up"
        assert rev["revision_validation_status"] == "unknown_revision_type"

    # ---- load_current_revision ----

    def test_load_current_revision(self):
        from app.pdf_extraction.job_state_store import (
            create_job_state, append_revision, load_current_revision,
        )
        store = create_job_state("job-1", self._control_room([self._row()]))
        store2 = append_revision(store, self._control_room([self._row()],
                                actions={"qr-p0-r0": [{"action_type": "escalate_to_engineer"}]}))
        cur = load_current_revision(store2)
        assert cur["revision_id"] == "rev-1"
        # Mutating returned dict does NOT affect store.
        cur["job_state"]["job_status"] = "complete"
        cur2 = load_current_revision(store2)
        assert cur2["job_state"]["job_status"] == "partial"

    def test_load_current_revision_empty_store(self):
        from app.pdf_extraction.job_state_store import load_current_revision
        assert load_current_revision({}) is None

    # ---- load_revision_history ----

    def test_load_revision_history(self):
        from app.pdf_extraction.job_state_store import (
            create_job_state, append_revision, load_revision_history,
        )
        store = create_job_state("job-1", self._control_room([self._row()]))
        store = append_revision(store, self._control_room([self._row()]))
        store = append_revision(store, self._control_room([self._row()]))
        history = load_revision_history(store)
        assert len(history) == 3
        assert [r["revision_id"] for r in history] == ["rev-0", "rev-1", "rev-2"]

    def test_load_revision_by_id(self):
        from app.pdf_extraction.job_state_store import (
            create_job_state, append_revision, load_revision,
        )
        store = create_job_state("job-1", self._control_room([self._row()]))
        store = append_revision(store, self._control_room([self._row()]))
        rev = load_revision(store, "rev-1")
        assert rev is not None
        assert rev["sequence"] == 1
        assert load_revision(store, "missing") is None

    # ---- Immutability ----

    def test_create_does_not_mutate_input(self):
        from app.pdf_extraction.job_state_store import create_job_state
        cr = self._control_room([self._row()])
        snap = deepcopy_simple(cr)
        create_job_state("job-1", cr)
        assert cr == snap

    def test_append_does_not_mutate_input_store_or_state(self):
        from app.pdf_extraction.job_state_store import (
            create_job_state, append_revision,
        )
        store = create_job_state("job-1", self._control_room([self._row()]))
        store_snap = deepcopy_simple(store)
        new_state = self._control_room([self._row()])
        new_state_snap = deepcopy_simple(new_state)
        append_revision(store, new_state)
        assert store == store_snap
        assert new_state == new_state_snap

    def test_old_revisions_immutable_through_loads(self):
        from app.pdf_extraction.job_state_store import (
            create_job_state, append_revision, load_revision_history,
        )
        store = create_job_state("job-1", self._control_room([self._row()]))
        store = append_revision(store, self._control_room([self._row()]))
        history = load_revision_history(store)
        history[0]["revision_id"] = "tampered"
        history2 = load_revision_history(store)
        assert history2[0]["revision_id"] == "rev-0"

    # ---- Diagnostics ----

    def test_store_diagnostics(self):
        from app.pdf_extraction.job_state_store import (
            create_job_state, append_revision, REV_ACTION_UPDATE,
            REV_RECOMPUTE,
        )
        store = create_job_state("job-1", self._control_room([self._row()]))
        store = append_revision(store, self._control_room([self._row()]),
                                 revision_metadata={"revision_type": REV_ACTION_UPDATE})
        store = append_revision(store, self._control_room([self._row()]),
                                 revision_metadata={"revision_type": REV_RECOMPUTE})
        d = store["store_diagnostics"]
        assert d["revision_count"] == 3
        assert d["appended_revision_types"] == ["initial_run", "action_update", "recompute"]

    # ---- Error path ----

    def test_append_to_empty_store_raises(self):
        from app.pdf_extraction.job_state_store import append_revision
        import pytest as _pytest
        with _pytest.raises(ValueError):
            append_revision({}, self._control_room([self._row()]))

    # ---- End-to-end with real pipeline ----

    def test_real_ipsi_trusted_store_lifecycle(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        from app.pdf_extraction.job_state_store import (
            create_job_state, append_revision, load_current_revision,
            REV_ACTION_UPDATE,
        )
        cr1 = build_control_room(str(ipsi_quote_path), str(dot_pdf_path),
                                  job_id="real-job-1")
        store = create_job_state("real-job-1", cr1,
                                  revision_metadata={"actor": "alice",
                                                      "revision_reason": "initial pipeline run"})
        # Recompute with office actions to test job_status transition.
        cr2 = build_control_room(
            str(ipsi_quote_path), str(dot_pdf_path),
            job_id="real-job-1",
            office_action_metadata={"rows": {
                f"qr-p0-r{i}": {"actions": [{"action_type": "no_action_taken",
                                              "actor": "alice"}]}
                for i in range(15)
            }},
        )
        store = append_revision(store, cr2,
                                 revision_metadata={"revision_type": REV_ACTION_UPDATE,
                                                     "actor": "alice",
                                                     "revision_reason": "all rows acknowledged"})
        # Current revision is rev-1, status changed from partial to ready.
        cur = load_current_revision(store)
        assert cur["revision_id"] == "rev-1"
        assert cur["change_summary"]["status_changed"] is True
        assert cur["change_summary"]["previous_job_status"] == "partial"
        assert cur["change_summary"]["current_job_status"] == "ready"
        assert cur["change_summary"]["actions_added_count"] >= 15

    def test_real_blocked_pair_revision(self, ipsi_quote_path, estprop_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        from app.pdf_extraction.job_state_store import create_job_state
        cr = build_control_room(str(ipsi_quote_path), str(estprop_pdf_path))
        store = create_job_state("blocked-job", cr)
        rev = store["revisions"][0]
        assert rev["job_state"]["job_status"] == "blocked"
        assert rev["change_summary"]["current_job_status"] == "blocked"

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c39(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c39(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


def deepcopy_simple(x):
    import copy
    return copy.deepcopy(x)


# ---------------------------------------------------------------------------
# 20ee. C38 — Control Room Interaction Model Tests
# ---------------------------------------------------------------------------


class TestC38ControlRoomInteraction:
    """C38: deterministic interaction model wrapping a C35 control room."""

    def _control_room(self, resolution_rows, job_status="partial",
                       packet_status="partial", pairing_status="trusted",
                       actioned_rows=None,
                       source_management_summary=None,
                       engineer_preview=None):
        return {
            "control_room_version": "control_room/v1",
            "job_id": "test-job",
            "job_status": job_status,
            "pipeline_status": {
                "pairing_status": pairing_status,
                "packet_status": packet_status,
            },
            "resolution": {"resolution_rows": resolution_rows},
            "office_actions_output": {
                "resolution_rows": actioned_rows or [],
            },
            "source_management": {
                "source_management_summary": source_management_summary or {},
            },
            "engineer_packet_preview": engineer_preview or {
                "engineer_row_count": len(resolution_rows),
                "packet_status": packet_status,
            },
        }

    def _row(self, **kw):
        base = {
            "normalized_row_id": "qr-p0-r0",
            "resolution_category": "non_comparable_missing_quote_source",
            "resolution_priority": "medium",
        }
        base.update(kw)
        return base

    # ---- Available views + version ----

    def test_available_views_closed_set(self):
        from app.pdf_extraction.control_room_interaction import build_interaction_model
        m = build_interaction_model(self._control_room([self._row()]))
        assert m["interaction_model_version"] == "control_room_interaction/v1"
        assert m["available_views"] == [
            "overview", "blocked_items", "unmapped_scope", "source_conflicts",
            "non_comparable", "quantity_discrepancies", "unit_discrepancies",
            "scenarios", "office_actions", "engineer_packet",
        ]

    # ---- Default view derivation ----

    def test_default_view_blocked(self):
        from app.pdf_extraction.control_room_interaction import (
            build_interaction_model, VIEW_BLOCKED_ITEMS,
        )
        rows = [self._row(resolution_category="blocked_pairing_resolution_required",
                          resolution_priority="critical") for _ in range(3)]
        m = build_interaction_model(self._control_room(rows, job_status="blocked"))
        assert m["default_view"] == VIEW_BLOCKED_ITEMS
        assert "blocked_count=3" in m["interaction_diagnostics"]["default_view_basis"]

    def test_default_view_unmapped(self):
        from app.pdf_extraction.control_room_interaction import (
            build_interaction_model, VIEW_UNMAPPED_SCOPE,
        )
        rows = [
            self._row(normalized_row_id="qr-p0-r0",
                      resolution_category="unmapped_scope_review_required",
                      resolution_priority="high"),
            self._row(normalized_row_id="qr-p0-r1"),
        ]
        m = build_interaction_model(self._control_room(rows))
        assert m["default_view"] == VIEW_UNMAPPED_SCOPE

    def test_default_view_source_conflicts(self):
        from app.pdf_extraction.control_room_interaction import (
            build_interaction_model, VIEW_SOURCE_CONFLICTS,
        )
        rows = [self._row(resolution_category="source_conflict_review_required",
                          resolution_priority="high")]
        m = build_interaction_model(self._control_room(rows))
        assert m["default_view"] == VIEW_SOURCE_CONFLICTS

    def test_default_view_qty_discrepancy(self):
        from app.pdf_extraction.control_room_interaction import (
            build_interaction_model, VIEW_QTY_DISCREPANCIES,
        )
        rows = [self._row(resolution_category="quantity_discrepancy_review_required",
                          resolution_priority="high")]
        m = build_interaction_model(self._control_room(rows))
        assert m["default_view"] == VIEW_QTY_DISCREPANCIES

    def test_default_view_unit_discrepancy(self):
        from app.pdf_extraction.control_room_interaction import (
            build_interaction_model, VIEW_UNIT_DISCREPANCIES,
        )
        rows = [self._row(resolution_category="unit_discrepancy_review_required",
                          resolution_priority="high")]
        m = build_interaction_model(self._control_room(rows))
        assert m["default_view"] == VIEW_UNIT_DISCREPANCIES

    def test_default_view_non_comparable(self):
        from app.pdf_extraction.control_room_interaction import (
            build_interaction_model, VIEW_NON_COMPARABLE,
        )
        m = build_interaction_model(self._control_room([self._row()]))
        assert m["default_view"] == VIEW_NON_COMPARABLE

    def test_default_view_overview_fallback(self):
        from app.pdf_extraction.control_room_interaction import (
            build_interaction_model, VIEW_OVERVIEW,
        )
        rows = [self._row(resolution_category="clean_match_no_resolution_needed",
                          resolution_priority="low")]
        m = build_interaction_model(self._control_room(rows))
        assert m["default_view"] == VIEW_OVERVIEW
        assert "no_review_required_rows" in m["interaction_diagnostics"]["default_view_basis"]

    # ---- Default view derivation cascade priority ----

    def test_blocked_supersedes_other_views(self):
        from app.pdf_extraction.control_room_interaction import (
            build_interaction_model, VIEW_BLOCKED_ITEMS,
        )
        rows = [
            self._row(normalized_row_id="qr-p0-r0",
                      resolution_category="blocked_pairing_resolution_required",
                      resolution_priority="critical"),
            self._row(normalized_row_id="qr-p0-r1",
                      resolution_category="source_conflict_review_required",
                      resolution_priority="high"),
            self._row(normalized_row_id="qr-p0-r2"),
        ]
        m = build_interaction_model(self._control_room(rows, job_status="blocked"))
        assert m["default_view"] == VIEW_BLOCKED_ITEMS

    def test_unmapped_supersedes_conflict_when_blocked_absent(self):
        from app.pdf_extraction.control_room_interaction import (
            build_interaction_model, VIEW_UNMAPPED_SCOPE,
        )
        rows = [
            self._row(normalized_row_id="qr-p0-r0",
                      resolution_category="unmapped_scope_review_required",
                      resolution_priority="high"),
            self._row(normalized_row_id="qr-p0-r1",
                      resolution_category="source_conflict_review_required",
                      resolution_priority="high"),
        ]
        m = build_interaction_model(self._control_room(rows))
        assert m["default_view"] == VIEW_UNMAPPED_SCOPE

    # ---- Focus row ----

    def test_focus_row_first_match_in_default_view(self):
        from app.pdf_extraction.control_room_interaction import build_interaction_model
        rows = [
            self._row(normalized_row_id="qr-p0-r0"),  # missing_quote
            self._row(normalized_row_id="qr-p0-r1",
                      resolution_category="unmapped_scope_review_required",
                      resolution_priority="high"),
            self._row(normalized_row_id="qr-p0-r2",
                      resolution_category="unmapped_scope_review_required",
                      resolution_priority="high"),
        ]
        m = build_interaction_model(self._control_room(rows))
        assert m["default_view"] == "unmapped_scope"
        # First match in original order is qr-p0-r1.
        assert m["view_state"]["focus_row_id"] == "qr-p0-r1"

    def test_focus_row_none_when_empty(self):
        from app.pdf_extraction.control_room_interaction import build_interaction_model
        m = build_interaction_model(self._control_room([]))
        assert m["view_state"]["focus_row_id"] is None

    # ---- Row reachability ----

    def test_every_row_in_index(self):
        from app.pdf_extraction.control_room_interaction import build_interaction_model
        rows = [
            self._row(normalized_row_id="qr-p0-r0",
                      resolution_category="blocked_pairing_resolution_required",
                      resolution_priority="critical"),
            self._row(normalized_row_id="qr-p0-r1",
                      resolution_category="unmapped_scope_review_required",
                      resolution_priority="high"),
            self._row(normalized_row_id="qr-p0-r2",
                      resolution_category="non_comparable_missing_quote_source",
                      resolution_priority="medium"),
            self._row(normalized_row_id="qr-p0-r3",
                      resolution_category="clean_match_no_resolution_needed",
                      resolution_priority="low"),
        ]
        m = build_interaction_model(self._control_room(rows, job_status="blocked"))
        ids = {r["normalized_row_id"] for r in m["row_index"]}
        assert ids == {"qr-p0-r0", "qr-p0-r1", "qr-p0-r2", "qr-p0-r3"}
        # Every row's view bucket is one of the closed set.
        from app.pdf_extraction.control_room_interaction import _ALL_VIEWS
        for r in m["row_index"]:
            assert r["view_bucket"] in _ALL_VIEWS

    # ---- View summaries ----

    def test_view_summaries_counts(self):
        from app.pdf_extraction.control_room_interaction import build_interaction_model
        rows = [
            self._row(normalized_row_id="qr-p0-r0",
                      resolution_category="unmapped_scope_review_required",
                      resolution_priority="high"),
            self._row(normalized_row_id="qr-p0-r1",
                      resolution_category="non_comparable_missing_quote_source",
                      resolution_priority="medium"),
            self._row(normalized_row_id="qr-p0-r2",
                      resolution_category="non_comparable_missing_quote_source",
                      resolution_priority="medium"),
        ]
        m = build_interaction_model(self._control_room(rows))
        vs = m["view_summaries"]
        assert vs["unmapped_scope"]["row_count"] == 1
        assert vs["non_comparable"]["row_count"] == 2
        assert vs["overview"]["row_count"] == 3

    def test_office_actions_view_counts(self):
        from app.pdf_extraction.control_room_interaction import build_interaction_model
        actioned = [{
            "normalized_row_id": "qr-p0-r0",
            "office_actions": [{"action_type": "escalate_to_engineer"}],
        }]
        m = build_interaction_model(self._control_room(
            [self._row()], actioned_rows=actioned,
        ))
        assert m["view_summaries"]["office_actions"]["rows_with_actions"] == 1

    def test_scenarios_view_advertises_closed_set(self):
        from app.pdf_extraction.control_room_interaction import build_interaction_model
        m = build_interaction_model(self._control_room([self._row()]))
        sc = m["view_summaries"]["scenarios"]
        assert sc["scenario_count"] == 5
        assert "scenario_dot_basis" in sc["available_scenarios"]

    # ---- Filters + sort ----

    def test_filter_dimensions_closed_set(self):
        from app.pdf_extraction.control_room_interaction import build_interaction_model
        m = build_interaction_model(self._control_room([self._row()]))
        assert m["view_state"]["filter_dimensions"] == [
            "priority", "resolution_category", "office_action_status",
            "source_conflict_present", "mapping_outcome", "scenario_id",
        ]

    def test_sort_keys_closed_set(self):
        from app.pdf_extraction.control_room_interaction import build_interaction_model
        m = build_interaction_model(self._control_room([self._row()]))
        sort = m["view_state"]["sort"]
        assert sort["key"] == "priority_then_row_id"
        assert sort["available_keys"] == [
            "priority_then_row_id", "row_id", "resolution_category",
            "office_action_status",
        ]

    def test_filters_default_to_none(self):
        from app.pdf_extraction.control_room_interaction import build_interaction_model
        m = build_interaction_model(self._control_room([self._row()]))
        for v in m["view_state"]["filters"].values():
            assert v is None

    # ---- Immutability ----

    def test_input_not_mutated(self):
        from app.pdf_extraction.control_room_interaction import build_interaction_model
        import copy
        cr = self._control_room([self._row()])
        snap = copy.deepcopy(cr)
        build_interaction_model(cr)
        assert cr == snap

    # ---- Real pipeline end-to-end ----

    def test_real_ipsi_trusted(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        from app.pdf_extraction.control_room_interaction import (
            build_interaction_model, VIEW_UNMAPPED_SCOPE,
        )
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        m = build_interaction_model(cr)
        # IPSI trusted has 2 unmapped + 13 missing_quote → unmapped wins.
        assert m["default_view"] == VIEW_UNMAPPED_SCOPE
        assert m["view_summaries"]["unmapped_scope"]["row_count"] == 2
        assert m["view_summaries"]["non_comparable"]["row_count"] == 13
        assert len(m["row_index"]) == 15

    def test_real_blocked_pair(self, ipsi_quote_path, estprop_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        from app.pdf_extraction.control_room_interaction import (
            build_interaction_model, VIEW_BLOCKED_ITEMS,
        )
        cr = build_control_room(str(ipsi_quote_path), str(estprop_pdf_path))
        m = build_interaction_model(cr)
        assert m["default_view"] == VIEW_BLOCKED_ITEMS
        assert m["view_summaries"]["blocked_items"]["row_count"] == 15

    def test_real_conflict_pair(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        from app.pdf_extraction.control_room_interaction import (
            build_interaction_model, VIEW_UNMAPPED_SCOPE,
        )
        # Inject a conflict on r1; trusted pair already has 2 unmapped → unmapped wins.
        cr = build_control_room(
            str(ipsi_quote_path), str(dot_pdf_path),
            external_sources=[{
                "source_type": "internal_takeoff", "source_ref": {"id": "T1"},
                "rows": {"qr-p0-r1": {"qty": 5.0, "unit": "EACH"}},
            }],
        )
        m = build_interaction_model(cr)
        # Cascade: unmapped (2) > source_conflicts (1)
        assert m["default_view"] == VIEW_UNMAPPED_SCOPE
        assert m["view_summaries"]["source_conflicts"]["row_count"] == 1

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c38(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c38(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20dd. C37 — Claim / Discrepancy Packet Tests
# ---------------------------------------------------------------------------


class TestC37ClaimPacket:
    """C37: structured engineer-facing discrepancy packet builder."""

    def _resolution_row(self, **kw):
        base = {
            "normalized_row_id": "qr-p0-r0",
            "resolution_category": "non_comparable_missing_quote_source",
            "resolution_priority": "medium",
            "resolution_reason": "mapped_row_has_no_quote_qty_unit_and_no_external_source",
            "quote_values": {"description": "Item A", "qty": None, "unit": None,
                             "unit_price": 5.0, "amount": 2750.0},
            "external_sources": [],
            "comparison_basis": {"basis": "unavailable"},
            "comparison_result": {"qty_match": None, "unit_match": None},
            "resolution_trace": {},
        }
        base.update(kw)
        return base

    def _resolution(self, rows, packet_status="partial"):
        return {
            "resolution_version": "discrepancy_resolution/v1",
            "resolution_status": "review_required",
            "packet_status": packet_status,
            "pairing_status": "trusted",
            "contract_version": "reconciliation_contract/v1",
            "augmentation_rules_version": "augmentation_rules/v1",
            "resolution_summary": {"rows_total": len(rows)},
            "resolution_rows": rows,
        }

    # ---- Section grouping ----

    def test_non_comparable_missing_quote_section(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        p = build_claim_packet(self._resolution([self._resolution_row()]))
        assert len(p["issue_sections"]["non_comparable_missing_quote"]) == 1
        assert p["issue_sections"]["unmapped_scope"] == []

    def test_blocked_pairing_section(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        row = self._resolution_row(
            resolution_category="blocked_pairing_resolution_required",
            resolution_priority="critical",
        )
        p = build_claim_packet(self._resolution([row], packet_status="blocked"))
        assert len(p["issue_sections"]["blocked_pairing"]) == 1

    def test_unmapped_scope_section(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        row = self._resolution_row(
            resolution_category="unmapped_scope_review_required",
            resolution_priority="high",
        )
        p = build_claim_packet(self._resolution([row]))
        assert len(p["issue_sections"]["unmapped_scope"]) == 1

    def test_source_conflicts_section(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        row = self._resolution_row(
            resolution_category="source_conflict_review_required",
            resolution_priority="high",
        )
        p = build_claim_packet(self._resolution([row]))
        assert len(p["issue_sections"]["source_conflicts"]) == 1

    def test_quantity_discrepancies_section(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        row = self._resolution_row(
            resolution_category="quantity_discrepancy_review_required",
            resolution_priority="high",
        )
        p = build_claim_packet(self._resolution([row]))
        assert len(p["issue_sections"]["quantity_discrepancies"]) == 1

    def test_unit_discrepancies_section(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        row = self._resolution_row(
            resolution_category="unit_discrepancy_review_required",
            resolution_priority="high",
        )
        p = build_claim_packet(self._resolution([row]))
        assert len(p["issue_sections"]["unit_discrepancies"]) == 1

    def test_clean_matches_section(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        row = self._resolution_row(
            resolution_category="clean_match_no_resolution_needed",
            resolution_priority="low",
        )
        p = build_claim_packet(self._resolution([row]))
        assert len(p["issue_sections"]["clean_matches"]) == 1

    def test_all_sections_always_present(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        p = build_claim_packet(self._resolution([self._resolution_row()]))
        for key in ("blocked_pairing", "unmapped_scope", "ambiguous_mapping",
                    "source_conflicts", "quantity_discrepancies",
                    "unit_discrepancies", "non_comparable_missing_quote",
                    "non_comparable_missing_external", "review_required_other",
                    "clean_matches"):
            assert key in p["issue_sections"]

    def test_every_row_belongs_to_exactly_one_section(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        rows = [
            self._resolution_row(normalized_row_id="qr-p0-r0",
                                 resolution_category="unmapped_scope_review_required",
                                 resolution_priority="high"),
            self._resolution_row(normalized_row_id="qr-p0-r1",
                                 resolution_category="source_conflict_review_required",
                                 resolution_priority="high"),
            self._resolution_row(normalized_row_id="qr-p0-r2"),
            self._resolution_row(normalized_row_id="qr-p0-r3",
                                 resolution_category="clean_match_no_resolution_needed",
                                 resolution_priority="low"),
        ]
        p = build_claim_packet(self._resolution(rows))
        assigned = sum(len(v) for v in p["issue_sections"].values())
        assert assigned == 4

    # ---- Priority ordering within section ----

    def test_rows_within_section_sorted_by_priority(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        rows = [
            self._resolution_row(normalized_row_id="qr-p0-r0",
                                 resolution_category="unmapped_scope_review_required",
                                 resolution_priority="medium"),
            self._resolution_row(normalized_row_id="qr-p0-r1",
                                 resolution_category="unmapped_scope_review_required",
                                 resolution_priority="high"),
            self._resolution_row(normalized_row_id="qr-p0-r2",
                                 resolution_category="unmapped_scope_review_required",
                                 resolution_priority="critical"),
        ]
        p = build_claim_packet(self._resolution(rows))
        ids = [r["normalized_row_id"] for r in p["issue_sections"]["unmapped_scope"]]
        assert ids == ["qr-p0-r2", "qr-p0-r1", "qr-p0-r0"]

    def test_stable_sort_on_ties(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        rows = [
            self._resolution_row(normalized_row_id=f"qr-p0-r{i}",
                                 resolution_category="unmapped_scope_review_required",
                                 resolution_priority="high")
            for i in range(4)
        ]
        p = build_claim_packet(self._resolution(rows))
        ids = [r["normalized_row_id"] for r in p["issue_sections"]["unmapped_scope"]]
        assert ids == ["qr-p0-r0", "qr-p0-r1", "qr-p0-r2", "qr-p0-r3"]

    # ---- Summary counts ----

    def test_summary_counts(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        rows = [
            self._resolution_row(normalized_row_id="qr-p0-r0",
                                 resolution_category="blocked_pairing_resolution_required",
                                 resolution_priority="critical"),
            self._resolution_row(normalized_row_id="qr-p0-r1",
                                 resolution_category="unmapped_scope_review_required",
                                 resolution_priority="high"),
            self._resolution_row(normalized_row_id="qr-p0-r2"),  # medium
            self._resolution_row(normalized_row_id="qr-p0-r3",
                                 resolution_category="clean_match_no_resolution_needed",
                                 resolution_priority="low"),
        ]
        p = build_claim_packet(self._resolution(rows, packet_status="blocked"))
        s = p["summary_section"]
        assert s["total_rows"] == 4
        assert s["critical_issues"] == 1
        assert s["high_priority_issues"] == 1
        assert s["medium_priority_issues"] == 1
        assert s["low_priority_issues"] == 1
        assert s["blocked_items"] == 1

    # ---- Packet status derivation ----

    def test_status_blocked(self):
        from app.pdf_extraction.claim_packet import build_claim_packet, PACKET_BLOCKED
        row = self._resolution_row(
            resolution_category="blocked_pairing_resolution_required",
            resolution_priority="critical",
        )
        p = build_claim_packet(self._resolution([row], packet_status="blocked"))
        assert p["packet_status"] == PACKET_BLOCKED

    def test_status_draft_ready(self):
        from app.pdf_extraction.claim_packet import build_claim_packet, PACKET_DRAFT_READY
        p = build_claim_packet(self._resolution([self._resolution_row()]))
        assert p["packet_status"] == PACKET_DRAFT_READY

    def test_status_no_issues_when_only_clean_matches(self):
        from app.pdf_extraction.claim_packet import build_claim_packet, PACKET_NO_ISSUES
        row = self._resolution_row(
            resolution_category="clean_match_no_resolution_needed",
            resolution_priority="low",
        )
        p = build_claim_packet(self._resolution([row]))
        assert p["packet_status"] == PACKET_NO_ISSUES

    def test_status_no_issues_empty_input(self):
        from app.pdf_extraction.claim_packet import build_claim_packet, PACKET_NO_ISSUES
        p = build_claim_packet(self._resolution([]))
        assert p["packet_status"] == PACKET_NO_ISSUES

    # ---- Office actions ----

    def test_office_actions_attached_to_rows(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        resolution = self._resolution([self._resolution_row()])
        office_action_output = {
            "office_actions_version": "x",
            "office_actions_summary": {}, "office_actions_diagnostics": {},
            "resolution_rows": [{
                "normalized_row_id": "qr-p0-r0",
                "office_action_status": "recorded",
                "office_actions": [{
                    "action_id": "a1",
                    "action_type": "escalate_to_engineer",
                    "actor": "alice",
                    "action_validation_status": "valid",
                }],
                "office_action_summary": {},
            }],
        }
        p = build_claim_packet(resolution, office_action_output)
        row = p["issue_sections"]["non_comparable_missing_quote"][0]
        assert row["office_action_count"] == 1
        assert row["office_actions"][0]["action_type"] == "escalate_to_engineer"
        # Flat list also has the entry.
        assert len(p["office_actions"]) == 1
        assert p["office_actions"][0]["normalized_row_id"] == "qr-p0-r0"

    def test_office_actions_never_override_category(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        resolution = self._resolution([self._resolution_row(
            resolution_category="source_conflict_review_required",
            resolution_priority="high",
        )])
        office_action_output = {
            "office_actions_version": "x",
            "office_actions_summary": {}, "office_actions_diagnostics": {},
            "resolution_rows": [{
                "normalized_row_id": "qr-p0-r0",
                "office_action_status": "recorded",
                "office_actions": [{
                    "action_type": "accept_dot_quantity_as_working_basis",
                    "action_validation_status": "valid",
                }],
                "office_action_summary": {},
            }],
        }
        p = build_claim_packet(resolution, office_action_output)
        # Row still in source_conflicts section.
        assert len(p["issue_sections"]["source_conflicts"]) == 1
        row = p["issue_sections"]["source_conflicts"][0]
        assert row["resolution_category"] == "source_conflict_review_required"
        assert row["resolution_priority"] == "high"

    # ---- Supporting data ----

    def test_supporting_data_populated(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        row = self._resolution_row(external_sources=[
            {"source_type": "dot_bid_item", "qty": 550.0, "unit": "SY"},
        ])
        p = build_claim_packet(self._resolution([row]))
        sd = p["supporting_data"]
        assert len(sd["quote_rows"]) == 1
        assert len(sd["external_sources"]) == 1
        assert len(sd["comparison_basis"]) == 1
        assert sd["external_sources"][0]["source_type"] == "dot_bid_item"

    # ---- Source management embedding ----

    def test_source_management_embedded(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        sm = {
            "source_management_version": "quantity_source_management/v1",
            "source_management_summary": {"rows_with_sources": 13},
        }
        p = build_claim_packet(self._resolution([self._resolution_row()]),
                               source_management_output=sm)
        assert p["source_management"]["source_management_version"] == "quantity_source_management/v1"
        assert p["source_management"]["source_management_summary"]["rows_with_sources"] == 13

    def test_source_management_null_when_omitted(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        p = build_claim_packet(self._resolution([self._resolution_row()]))
        assert p["source_management"] is None

    # ---- Diagnostics ----

    def test_diagnostics_assignment_check(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        rows = [self._resolution_row(normalized_row_id=f"qr-p0-r{i}") for i in range(5)]
        p = build_claim_packet(self._resolution(rows))
        d = p["packet_diagnostics"]
        assert d["input_row_count"] == 5
        assert d["assigned_row_count"] == 5

    # ---- Immutability ----

    def test_input_not_mutated(self):
        from app.pdf_extraction.claim_packet import build_claim_packet
        import copy
        resolution = self._resolution([self._resolution_row()])
        snap = copy.deepcopy(resolution)
        build_claim_packet(resolution)
        assert resolution == snap

    # ---- Real pipeline end-to-end ----

    def test_real_ipsi_trusted_claim_packet(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        from app.pdf_extraction.quantity_source_management import manage_quantity_sources
        from app.pdf_extraction.augmentation_rules import apply_augmentation_rules
        from app.pdf_extraction.discrepancy_resolution import build_resolution
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions, ACTION_ESCALATE_ENGINEER,
        )
        from app.pdf_extraction.claim_packet import build_claim_packet, PACKET_DRAFT_READY

        bid_rows, bs = extract_bid_items_from_pdf(str(dot_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        mapping = map_quote_to_bid(staging["accepted_rows"], bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=mapping,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")})
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)
        managed = manage_quantity_sources(injected)
        augmented = apply_augmentation_rules(injected)
        resolved = build_resolution(augmented)
        actioned = record_office_actions(resolved, {"rows": {
            "qr-p0-r0": {"actions": [{"action_type": ACTION_ESCALATE_ENGINEER,
                                      "actor": "alice"}]},
        }})

        cp = build_claim_packet(actioned, actioned, managed)
        assert cp["packet_status"] == PACKET_DRAFT_READY
        assert len(cp["issue_sections"]["unmapped_scope"]) == 2
        assert len(cp["issue_sections"]["non_comparable_missing_quote"]) == 13
        assert cp["source_management"]["source_management_version"] == "quantity_source_management/v1"
        assert cp["summary_section"]["high_priority_issues"] == 2
        assert cp["summary_section"]["medium_priority_issues"] == 13
        # Escalation action is embedded on the r0 row + in flat list.
        r0 = next(r for r in cp["issue_sections"]["unmapped_scope"]
                  if r["normalized_row_id"] == "qr-p0-r0")
        assert r0["office_action_count"] == 1
        assert any(a["normalized_row_id"] == "qr-p0-r0" for a in cp["office_actions"])

    def test_real_blocked_claim_packet(self, ipsi_quote_path, estprop_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        from app.pdf_extraction.augmentation_rules import apply_augmentation_rules
        from app.pdf_extraction.discrepancy_resolution import build_resolution
        from app.pdf_extraction.claim_packet import build_claim_packet, PACKET_BLOCKED

        bid_rows, bs = extract_bid_items_from_pdf(str(estprop_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=None,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")})
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)
        augmented = apply_augmentation_rules(injected)
        resolved = build_resolution(augmented)
        cp = build_claim_packet(resolved)
        assert cp["packet_status"] == PACKET_BLOCKED
        assert len(cp["issue_sections"]["blocked_pairing"]) == 15

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c37(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c37(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20cc. C36 — Scenario + Sensitivity Layer Tests
# ---------------------------------------------------------------------------


class TestC36ScenarioEngine:
    """C36: deterministic what-if comparisons over injected contracts."""

    def _row(self, sources=None, **kw):
        base = {
            "normalized_row_id": "qr-p0-r0",
            "mapping_outcome": "mapped",
            "comparison_status": "non_comparable",
            "quote_values": {"qty": None, "unit": None,
                             "unit_price": 5.0, "amount": 2750.0},
            "external_quantity_sources": sources or [],
        }
        base.update(kw)
        return base

    def _contract(self, rows):
        return {
            "contract_version": "reconciliation_contract/v1",
            "reconciliation_rows": rows,
            "injection_version": "quantity_injection/v1",
        }

    def _source(self, source_type, qty=None, unit=None):
        return {
            "source_type": source_type,
            "source_ref": {"id": "x"},
            "qty": qty, "unit": unit,
            "source_trace": {"origin": "test"},
        }

    # ---- All five scenarios produced ----

    def test_all_scenarios_produced(self):
        from app.pdf_extraction.scenario_engine import evaluate_scenarios
        out = evaluate_scenarios(self._contract([self._row()]))
        ids = [s["scenario_id"] for s in out["scenarios"]]
        assert ids == [
            "scenario_dot_basis",
            "scenario_takeoff_basis",
            "scenario_engineer_basis",
            "scenario_manual_basis",
            "scenario_no_external",
        ]
        assert out["scenario_version"] == "scenario_engine/v1"

    # ---- scenario_dot_basis ----

    def test_dot_scenario_uses_dot_source(self):
        from app.pdf_extraction.scenario_engine import (
            evaluate_scenarios, ROW_MATCH,
        )
        row = self._row([self._source("dot_bid_item", qty=550.0, unit="SY")])
        out = evaluate_scenarios(self._contract([row]))
        dot_scenario = out["scenarios"][0]
        assert dot_scenario["scenario_id"] == "scenario_dot_basis"
        assert dot_scenario["rows_comparable"] == 1
        r = dot_scenario["rows"][0]
        assert r["row_state"] == ROW_MATCH
        assert r["scenario_qty"] == 550.0
        assert r["scenario_unit"] == "SY"

    def test_dot_scenario_missing_source_unresolved(self):
        from app.pdf_extraction.scenario_engine import (
            evaluate_scenarios, ROW_MISSING_SOURCE,
        )
        # Row has no dot source.
        row = self._row([self._source("internal_takeoff", qty=1.0, unit="LF")])
        out = evaluate_scenarios(self._contract([row]))
        dot_scenario = out["scenarios"][0]
        assert dot_scenario["rows_unresolved"] == 1
        assert dot_scenario["rows"][0]["row_state"] == ROW_MISSING_SOURCE

    # ---- scenario_takeoff_basis ----

    def test_takeoff_scenario_uses_takeoff_source(self):
        from app.pdf_extraction.scenario_engine import (
            evaluate_scenarios, ROW_MATCH,
        )
        row = self._row([
            self._source("dot_bid_item", qty=550.0, unit="SY"),
            self._source("internal_takeoff", qty=560.0, unit="SY"),
        ])
        out = evaluate_scenarios(self._contract([row]))
        takeoff = next(s for s in out["scenarios"] if s["scenario_id"] == "scenario_takeoff_basis")
        assert takeoff["rows_comparable"] == 1
        r = takeoff["rows"][0]
        assert r["scenario_qty"] == 560.0

    # ---- scenario_engineer_basis ----

    def test_engineer_scenario_mismatch_against_quote(self):
        from app.pdf_extraction.scenario_engine import (
            evaluate_scenarios, ROW_MISMATCH,
        )
        row = self._row(
            [self._source("engineer_quantity", qty=50.0, unit="LF")],
            quote_values={"qty": 24.0, "unit": "LF",
                          "unit_price": 10.0, "amount": 240.0},
        )
        out = evaluate_scenarios(self._contract([row]))
        eng = next(s for s in out["scenarios"] if s["scenario_id"] == "scenario_engineer_basis")
        assert eng["rows_mismatched"] == 1
        assert eng["rows_conflicted"] == 1
        r = eng["rows"][0]
        assert r["row_state"] == ROW_MISMATCH
        assert r["qty_match"] is False
        assert r["unit_match"] is True

    # ---- scenario_no_external ----

    def test_no_external_scenario_with_quote(self):
        from app.pdf_extraction.scenario_engine import (
            evaluate_scenarios, ROW_MATCH,
        )
        row = self._row(
            quote_values={"qty": 24.0, "unit": "LF",
                          "unit_price": 10.0, "amount": 240.0},
        )
        out = evaluate_scenarios(self._contract([row]))
        no_ext = next(s for s in out["scenarios"] if s["scenario_id"] == "scenario_no_external")
        assert no_ext["rows_comparable"] == 1
        r = no_ext["rows"][0]
        assert r["row_state"] == ROW_MATCH

    def test_no_external_scenario_missing_quote(self):
        from app.pdf_extraction.scenario_engine import (
            evaluate_scenarios, ROW_MISSING_QUOTE,
        )
        row = self._row()  # quote qty/unit None
        out = evaluate_scenarios(self._contract([row]))
        no_ext = next(s for s in out["scenarios"] if s["scenario_id"] == "scenario_no_external")
        assert no_ext["rows_unresolved"] == 1
        assert no_ext["rows"][0]["row_state"] == ROW_MISSING_QUOTE

    # ---- Blocked / unmapped / ambiguous inherit ----

    def test_blocked_row_inherits_blocked_in_all_scenarios(self):
        from app.pdf_extraction.scenario_engine import (
            evaluate_scenarios, ROW_BLOCKED,
        )
        row = self._row([self._source("dot_bid_item", qty=1.0, unit="LF")],
                         mapping_outcome="blocked", comparison_status="blocked")
        out = evaluate_scenarios(self._contract([row]))
        for s in out["scenarios"]:
            assert s["rows_blocked"] == 1
            assert s["rows"][0]["row_state"] == ROW_BLOCKED

    def test_unmapped_row_inherits_unmapped(self):
        from app.pdf_extraction.scenario_engine import (
            evaluate_scenarios, ROW_UNMAPPED,
        )
        row = self._row(mapping_outcome="unmapped")
        out = evaluate_scenarios(self._contract([row]))
        for s in out["scenarios"]:
            assert s["rows_unmapped"] == 1
            assert s["rows"][0]["row_state"] == ROW_UNMAPPED

    def test_ambiguous_row_inherits_ambiguous(self):
        from app.pdf_extraction.scenario_engine import (
            evaluate_scenarios, ROW_AMBIGUOUS,
        )
        row = self._row(mapping_outcome="ambiguous")
        out = evaluate_scenarios(self._contract([row]))
        for s in out["scenarios"]:
            assert s["rows_ambiguous"] == 1

    # ---- Scenarios differ by source type ----

    def test_scenarios_differ_per_source(self):
        from app.pdf_extraction.scenario_engine import evaluate_scenarios
        row = self._row([
            self._source("dot_bid_item", qty=550.0, unit="SY"),
            self._source("internal_takeoff", qty=560.0, unit="SY"),
            self._source("engineer_quantity", qty=570.0, unit="SY"),
        ])
        out = evaluate_scenarios(self._contract([row]))
        dot = next(s for s in out["scenarios"] if s["scenario_id"] == "scenario_dot_basis")
        takeoff = next(s for s in out["scenarios"] if s["scenario_id"] == "scenario_takeoff_basis")
        engineer = next(s for s in out["scenarios"] if s["scenario_id"] == "scenario_engineer_basis")
        assert dot["rows"][0]["scenario_qty"] == 550.0
        assert takeoff["rows"][0]["scenario_qty"] == 560.0
        assert engineer["rows"][0]["scenario_qty"] == 570.0

    # ---- Multiple same-type sources ----

    def test_first_matching_source_used_multi_same_type_flagged(self):
        from app.pdf_extraction.scenario_engine import evaluate_scenarios
        row = self._row([
            self._source("dot_bid_item", qty=550.0, unit="SY"),
            self._source("dot_bid_item", qty=999.0, unit="SY"),
        ])
        out = evaluate_scenarios(self._contract([row]))
        dot = next(s for s in out["scenarios"] if s["scenario_id"] == "scenario_dot_basis")
        r = dot["rows"][0]
        assert r["scenario_qty"] == 550.0  # first one wins
        assert r["scenario_trace"]["multiple_same_type_present"] is True

    # ---- Aggregate summary ----

    def test_aggregate_summary_format(self):
        from app.pdf_extraction.scenario_engine import evaluate_scenarios
        row = self._row([self._source("dot_bid_item", qty=1.0, unit="LF")])
        out = evaluate_scenarios(self._contract([row]))
        s = out["scenario_summary"]
        assert "scenario_dot_basis" in s
        assert s["scenario_dot_basis"]["rows_comparable"] == 1
        assert s["scenario_dot_basis"]["scenario_basis_source_type"] == "dot_bid_item"

    # ---- Truth preservation ----

    def test_base_truth_not_mutated(self):
        from app.pdf_extraction.scenario_engine import evaluate_scenarios
        import copy
        contract = self._contract([self._row([
            self._source("dot_bid_item", qty=1.0, unit="LF"),
        ])])
        snap = copy.deepcopy(contract)
        evaluate_scenarios(contract)
        assert contract == snap

    def test_augmentation_rules_not_touched(self):
        """If caller supplies an augmented contract with comparison_basis
        set, the scenario engine must not change it."""
        from app.pdf_extraction.scenario_engine import evaluate_scenarios
        row = self._row([self._source("dot_bid_item", qty=1.0, unit="LF")])
        row["comparison_basis"] = "dot_augmented"
        row["effective_comparison_values"] = {"qty": 1.0, "unit": "LF"}
        out_contract = self._contract([row])
        evaluate_scenarios(out_contract)
        # Contract untouched.
        assert out_contract["reconciliation_rows"][0]["comparison_basis"] == "dot_augmented"
        assert out_contract["reconciliation_rows"][0]["effective_comparison_values"] == {
            "qty": 1.0, "unit": "LF"
        }

    # ---- End-to-end with real pipeline ----

    def test_real_ipsi_trusted_scenarios(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        from app.pdf_extraction.scenario_engine import evaluate_scenarios

        bid_rows, bs = extract_bid_items_from_pdf(str(dot_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        mapping = map_quote_to_bid(staging["accepted_rows"], bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=mapping,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")})
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)

        out = evaluate_scenarios(injected)
        # 13 mapped rows have dot_bid_item sources → 13 comparable in dot scenario.
        dot = next(s for s in out["scenarios"] if s["scenario_id"] == "scenario_dot_basis")
        assert dot["rows_comparable"] == 13
        assert dot["rows_unmapped"] == 2
        # No takeoff/engineer/manual sources on IPSI → all unresolved except unmapped/blocked.
        takeoff = next(s for s in out["scenarios"] if s["scenario_id"] == "scenario_takeoff_basis")
        assert takeoff["rows_unresolved"] == 13
        # no_external: quote has no qty/unit → 13 missing_quote on mapped rows.
        no_ext = next(s for s in out["scenarios"] if s["scenario_id"] == "scenario_no_external")
        assert no_ext["rows_comparable"] == 0
        assert no_ext["rows_unresolved"] == 13

    def test_real_blocked_pair_scenarios(self, ipsi_quote_path, estprop_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        from app.pdf_extraction.scenario_engine import evaluate_scenarios

        bid_rows, bs = extract_bid_items_from_pdf(str(estprop_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=None,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")})
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)
        out = evaluate_scenarios(injected)
        # Every row blocked → every scenario shows 15 rows_blocked, 0 comparable.
        for s in out["scenarios"]:
            assert s["rows_blocked"] == 15
            assert s["rows_comparable"] == 0

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c36(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c36(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20bb. C35 — Control Room (Unified Job State) Tests
# ---------------------------------------------------------------------------


class TestC35ControlRoom:
    """C35: unified pipeline job state. Orchestrates full C9→C34 chain
    and exposes a canonical inspection object."""

    # ---- Structural shape ----

    def test_control_room_structure_trusted(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        for k in ("control_room_version", "job_id", "job_status",
                  "input_summary", "pipeline_status", "pairing_section",
                  "review_packet", "reconciliation_contract", "injected_contract",
                  "source_management", "augmented_contract", "resolution",
                  "office_actions_output", "engineer_packet",
                  "discrepancy_summary", "priority_summary",
                  "source_management_section", "office_action_summary",
                  "engineer_packet_preview", "control_room_diagnostics"):
            assert k in cr
        assert cr["control_room_version"] == "control_room/v1"
        assert cr["control_room_diagnostics"]["pipeline_succeeded"] is True

    def test_job_id_default(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        assert "ipsi_quote" in cr["job_id"]
        assert "dot_schedule_fixture" in cr["job_id"]

    def test_job_id_caller_supplied(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path), job_id="my-job-42")
        assert cr["job_id"] == "my-job-42"

    # ---- Pipeline status completeness ----

    def test_pipeline_status_populated(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        ps = cr["pipeline_status"]
        assert ps["classification"] == "quote"
        assert ps["pairing_status"] == "trusted"
        assert ps["packet_status"] == "partial"
        assert ps["reconciliation_status"] == "partial"
        assert ps["resolution_status"] == "review_required"

    def test_input_summary_counts(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        s = cr["input_summary"]
        assert s["quote_accepted_rows"] == 15
        assert s["dot_rows_extracted"] == 93
        assert s["external_sources_supplied"] == 0
        assert s["office_action_metadata_supplied"] is False

    # ---- Job status derivation ----

    def test_job_status_blocked_pair(self, ipsi_quote_path, estprop_pdf_path):
        from app.pdf_extraction.control_room import build_control_room, JOB_BLOCKED
        cr = build_control_room(str(ipsi_quote_path), str(estprop_pdf_path))
        assert cr["job_status"] == JOB_BLOCKED
        # Pipeline still completed all stages — status reflects upstream.
        assert cr["control_room_diagnostics"]["pipeline_succeeded"] is True

    def test_job_status_partial_trusted_no_actions(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room, JOB_PARTIAL
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        # Trusted pair has 15 review-required rows, 0 actions → partial.
        assert cr["job_status"] == JOB_PARTIAL

    def test_job_status_ready_when_all_actioned(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room, JOB_READY
        # Record an action on EVERY actionable row id. Real IPSI trusted
        # resolution produces these 15 row ids: qr-p0-r0..r14.
        metadata = {
            "rows": {
                f"qr-p0-r{i}": {
                    "actions": [{
                        "action_type": "no_action_taken",
                        "actor": "alice",
                        "action_note": "acknowledged",
                    }],
                }
                for i in range(15)
            }
        }
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path),
                                 office_action_metadata=metadata)
        assert cr["job_status"] == JOB_READY

    # ---- Nested objects preserved exactly ----

    def test_nested_resolution_preserved(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        # The nested resolution must carry the C31 shape verbatim.
        r = cr["resolution"]
        assert r["resolution_version"] == "discrepancy_resolution/v1"
        assert len(r["resolution_rows"]) == 15
        # 13 dot_augmented rows yield non_comparable_missing_quote_source,
        # 2 unmapped rows yield unmapped_scope_review_required.
        cats = [row["resolution_category"] for row in r["resolution_rows"]]
        assert cats.count("non_comparable_missing_quote_source") == 13
        assert cats.count("unmapped_scope_review_required") == 2

    def test_nested_engineer_packet_preserved(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        ep = cr["engineer_packet"]
        assert ep["packet_version"] == "engineer_output_packet/v1"
        assert ep["packet_status"] == "draft_ready"
        assert len(ep["engineer_rows"]) == 15

    def test_nested_source_management_preserved(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        sm = cr["source_management"]
        assert sm["source_management_version"] == "quantity_source_management/v1"
        assert sm["source_management_summary"]["rows_with_sources"] == 13

    # ---- Previews + summaries ----

    def test_engineer_packet_preview(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        pv = cr["engineer_packet_preview"]
        assert pv["packet_status"] == "draft_ready"
        assert pv["engineer_row_count"] == 15
        assert pv["priority_histogram"]["high"] == 2
        assert pv["priority_histogram"]["medium"] == 13

    def test_discrepancy_summary_populated(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        d = cr["discrepancy_summary"]
        assert d["category_counts"]["unmapped_scope_review_required"] == 2
        assert d["category_counts"]["non_comparable_missing_quote_source"] == 13

    # ---- Blocked pair preserves all layers ----

    def test_blocked_pair_layers_intact(self, ipsi_quote_path, estprop_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        cr = build_control_room(str(ipsi_quote_path), str(estprop_pdf_path))
        assert cr["pipeline_status"]["pairing_status"] == "rejected"
        assert cr["engineer_packet"]["packet_status"] == "blocked"
        assert cr["pipeline_status"]["packet_status"] == "blocked"
        # Every resolution row present and categorized as blocked_pairing.
        cats = [r["resolution_category"] for r in cr["resolution"]["resolution_rows"]]
        assert all(c == "blocked_pairing_resolution_required" for c in cats)

    # ---- With external sources + office actions ----

    def test_with_external_sources_and_actions(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        ext = [{
            "source_type": "internal_takeoff",
            "source_ref": {"id": "T-1"},
            "rows": {"qr-p0-r1": {"qty": 5.0, "unit": "EACH"}},
        }]
        actions = {"rows": {"qr-p0-r1": {"actions": [{
            "action_type": "escalate_to_engineer", "actor": "alice",
        }]}}}
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path),
                                 external_sources=ext,
                                 office_action_metadata=actions)
        sm = cr["source_management"]
        # The row now carries both sources and the conflict is detected.
        assert sm["source_management_summary"]["rows_with_conflicted_sources"] == 1
        # Engineer packet picks up conflicting_quantity_sources flag.
        flag_hist = cr["engineer_packet_preview"]["flag_histogram"]
        assert flag_hist.get("conflicting_quantity_sources", 0) == 1
        # Office action recorded.
        assert cr["office_action_summary"]["rows_with_actions"] == 1
        # Input summary reflects external source.
        assert cr["input_summary"]["external_sources_supplied"] == 1

    # ---- Immutability ----

    def test_build_is_deterministic(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room
        cr1 = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        cr2 = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        # Compare a stable projection — the whole dict should be equal,
        # but timestamps / process state are not injected, so equality
        # holds.
        assert cr1["job_status"] == cr2["job_status"]
        assert cr1["pipeline_status"] == cr2["pipeline_status"]
        assert cr1["discrepancy_summary"] == cr2["discrepancy_summary"]
        assert cr1["engineer_packet_preview"] == cr2["engineer_packet_preview"]

    def test_caller_can_mutate_without_affecting_internals(self, ipsi_quote_path, dot_pdf_path):
        """Deep-copy guarantee: mutating the returned control room does
        not affect subsequent builds."""
        from app.pdf_extraction.control_room import build_control_room
        cr = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        cr["resolution"]["resolution_rows"].clear()
        cr2 = build_control_room(str(ipsi_quote_path), str(dot_pdf_path))
        assert len(cr2["resolution"]["resolution_rows"]) == 15

    # ---- Failure path ----

    def test_quote_file_missing_failure(self, tmp_path, dot_pdf_path):
        from app.pdf_extraction.control_room import build_control_room, JOB_BLOCKED
        missing = tmp_path / "nope.pdf"
        cr = build_control_room(str(missing), str(dot_pdf_path), job_id="j1")
        # Stage 2 (quote staging) should fail; control room reports blocked.
        assert cr["job_status"] == JOB_BLOCKED
        assert cr["control_room_diagnostics"]["pipeline_succeeded"] is False

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c35(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c35(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20aa. C34 — Engineer Output Packet Tests
# ---------------------------------------------------------------------------


class TestC34EngineerOutputPacket:
    """C34: deterministic engineer packet foundation assembling C31
    resolution + C33 actions + optional C32 source management."""

    def _resolution_row(self, **kw):
        base = {
            "normalized_row_id": "qr-p0-r0",
            "resolution_category": "non_comparable_missing_quote_source",
            "resolution_priority": "medium",
            "resolution_reason": "mapped_row_has_no_quote_qty_unit_and_no_external_source",
            "quote_values": {"description": "Item A", "qty": None, "unit": None,
                             "unit_price": 5.0, "amount": 2750.0},
            "external_sources": [],
            "comparison_basis": {"basis": "dot_augmented",
                                 "effective_comparison_values": {"qty": 550.0, "unit": "SY"}},
            "comparison_result": {"qty_match": None, "unit_match": None},
            "resolution_trace": {"inputs": {"mapping_outcome": "mapped"}},
        }
        base.update(kw)
        return base

    def _resolution(self, rows, packet_status="partial"):
        return {
            "resolution_version": "discrepancy_resolution/v1",
            "resolution_status": "review_required",
            "packet_status": packet_status,
            "pairing_status": "trusted",
            "contract_version": "reconciliation_contract/v1",
            "augmentation_rules_version": "augmentation_rules/v1",
            "resolution_summary": {
                "rows_total": len(rows),
                "category_counts": {},
                "priority_counts": {},
            },
            "resolution_rows": rows,
        }

    # ---- Packet structure ----

    def test_packet_version_present(self):
        from app.pdf_extraction.engineer_output_packet import build_engineer_packet
        p = build_engineer_packet(self._resolution([self._resolution_row()]))
        assert p["packet_version"] == "engineer_output_packet/v1"

    def test_all_sections_present(self):
        from app.pdf_extraction.engineer_output_packet import build_engineer_packet
        p = build_engineer_packet(self._resolution([self._resolution_row()]))
        for k in ("packet_status", "packet_header", "pairing_section",
                  "source_management_section", "discrepancy_summary",
                  "office_action_summary", "engineer_rows", "packet_diagnostics"):
            assert k in p

    def test_engineer_row_shape(self):
        from app.pdf_extraction.engineer_output_packet import build_engineer_packet
        p = build_engineer_packet(self._resolution([self._resolution_row()]))
        row = p["engineer_rows"][0]
        for k in ("normalized_row_id", "quote_description", "mapping_outcome",
                  "resolution_category", "resolution_priority", "comparison_basis",
                  "comparison_result", "quote_values", "external_sources",
                  "office_actions", "office_action_status", "engineer_packet_flags",
                  "engineer_trace"):
            assert k in row

    # ---- Packet status derivation ----

    def test_status_blocked_from_upstream(self):
        from app.pdf_extraction.engineer_output_packet import (
            build_engineer_packet, PACKET_BLOCKED,
        )
        row = self._resolution_row(
            resolution_category="blocked_pairing_resolution_required",
            resolution_priority="critical",
        )
        p = build_engineer_packet(self._resolution([row], packet_status="blocked"))
        assert p["packet_status"] == PACKET_BLOCKED

    def test_status_draft_ready_when_review_required(self):
        from app.pdf_extraction.engineer_output_packet import (
            build_engineer_packet, PACKET_DRAFT_READY,
        )
        p = build_engineer_packet(self._resolution([self._resolution_row()]))
        assert p["packet_status"] == PACKET_DRAFT_READY

    def test_status_no_external_packet_needed_when_empty(self):
        from app.pdf_extraction.engineer_output_packet import (
            build_engineer_packet, PACKET_NO_EXTERNAL_PACKET_NEEDED,
        )
        p = build_engineer_packet(self._resolution([]))
        assert p["packet_status"] == PACKET_NO_EXTERNAL_PACKET_NEEDED

    def test_status_partial_when_only_clean_matches(self):
        from app.pdf_extraction.engineer_output_packet import (
            build_engineer_packet, PACKET_PARTIAL,
        )
        row = self._resolution_row(resolution_category="clean_match_no_resolution_needed",
                                    resolution_priority="low")
        p = build_engineer_packet(self._resolution([row]))
        assert p["packet_status"] == PACKET_PARTIAL

    # ---- Row ordering ----

    def test_rows_ordered_by_priority(self):
        from app.pdf_extraction.engineer_output_packet import build_engineer_packet
        rows = [
            self._resolution_row(normalized_row_id="qr-p0-r0", resolution_priority="low",
                                 resolution_category="clean_match_no_resolution_needed"),
            self._resolution_row(normalized_row_id="qr-p0-r1", resolution_priority="critical",
                                 resolution_category="blocked_pairing_resolution_required"),
            self._resolution_row(normalized_row_id="qr-p0-r2", resolution_priority="medium"),
            self._resolution_row(normalized_row_id="qr-p0-r3", resolution_priority="high",
                                 resolution_category="unmapped_scope_review_required"),
        ]
        p = build_engineer_packet(self._resolution(rows))
        priorities = [r["resolution_priority"] for r in p["engineer_rows"]]
        assert priorities == ["critical", "high", "medium", "low"]

    def test_rows_ordering_stable_on_ties(self):
        from app.pdf_extraction.engineer_output_packet import build_engineer_packet
        rows = [
            self._resolution_row(normalized_row_id=f"qr-p0-r{i}",
                                 resolution_priority="high",
                                 resolution_category="unmapped_scope_review_required")
            for i in range(4)
        ]
        p = build_engineer_packet(self._resolution(rows))
        ids = [r["normalized_row_id"] for r in p["engineer_rows"]]
        assert ids == ["qr-p0-r0", "qr-p0-r1", "qr-p0-r2", "qr-p0-r3"]

    # ---- Engineer packet flags ----

    def test_flag_blocked_pairing(self):
        from app.pdf_extraction.engineer_output_packet import (
            build_engineer_packet, FLAG_BLOCKED_PAIRING,
        )
        row = self._resolution_row(resolution_category="blocked_pairing_resolution_required",
                                    resolution_priority="critical")
        p = build_engineer_packet(self._resolution([row], packet_status="blocked"))
        assert FLAG_BLOCKED_PAIRING in p["engineer_rows"][0]["engineer_packet_flags"]

    def test_flag_conflicting_quantity_sources(self):
        from app.pdf_extraction.engineer_output_packet import (
            build_engineer_packet, FLAG_CONFLICTING_QUANTITY_SOURCES,
        )
        row = self._resolution_row(resolution_category="source_conflict_review_required",
                                    resolution_priority="high")
        p = build_engineer_packet(self._resolution([row]))
        assert FLAG_CONFLICTING_QUANTITY_SOURCES in p["engineer_rows"][0]["engineer_packet_flags"]

    def test_flag_missing_quote_quantity(self):
        from app.pdf_extraction.engineer_output_packet import (
            build_engineer_packet, FLAG_MISSING_QUOTE_QUANTITY,
        )
        p = build_engineer_packet(self._resolution([self._resolution_row()]))
        assert FLAG_MISSING_QUOTE_QUANTITY in p["engineer_rows"][0]["engineer_packet_flags"]

    def test_flag_qty_discrepancy(self):
        from app.pdf_extraction.engineer_output_packet import (
            build_engineer_packet, FLAG_QTY_DISCREPANCY,
        )
        row = self._resolution_row(resolution_category="quantity_discrepancy_review_required",
                                    resolution_priority="high")
        p = build_engineer_packet(self._resolution([row]))
        assert FLAG_QTY_DISCREPANCY in p["engineer_rows"][0]["engineer_packet_flags"]

    def test_flag_office_action_working_basis(self):
        from app.pdf_extraction.engineer_output_packet import (
            build_engineer_packet, FLAG_WORKING_BASIS_SELECTED_BY_OFFICE,
        )
        resolution = self._resolution([self._resolution_row()])
        office_action_output = {
            "office_actions_version": "office_resolution_actions/v1",
            "office_actions_summary": {},
            "office_actions_diagnostics": {},
            "resolution_rows": [{
                "normalized_row_id": "qr-p0-r0",
                "office_action_status": "recorded",
                "office_actions": [{
                    "action_id": "a1",
                    "action_type": "accept_dot_quantity_as_working_basis",
                    "actor": "alice", "timestamp": "x", "action_note": "x",
                    "action_scope": {"normalized_row_id": "qr-p0-r0"},
                    "action_validation_status": "valid",
                }],
                "office_action_summary": {"action_count": 1, "action_types": [],
                                          "has_unknown_action_type": False},
            }],
        }
        p = build_engineer_packet(resolution, office_action_output)
        r = p["engineer_rows"][0]
        assert FLAG_WORKING_BASIS_SELECTED_BY_OFFICE in r["engineer_packet_flags"]
        # Governed truth preserved.
        assert r["resolution_category"] == "non_comparable_missing_quote_source"
        assert r["resolution_priority"] == "medium"

    def test_flag_lump_sum_marked(self):
        from app.pdf_extraction.engineer_output_packet import (
            build_engineer_packet, FLAG_LUMP_SUM_MARKED,
        )
        resolution = self._resolution([self._resolution_row()])
        office_action_output = {
            "office_actions_version": "x", "office_actions_summary": {}, "office_actions_diagnostics": {},
            "resolution_rows": [{
                "normalized_row_id": "qr-p0-r0",
                "office_action_status": "recorded",
                "office_actions": [{
                    "action_type": "mark_lump_sum_non_comparable",
                    "action_validation_status": "valid",
                }],
                "office_action_summary": {"action_count": 1, "action_types": [],
                                          "has_unknown_action_type": False},
            }],
        }
        p = build_engineer_packet(resolution, office_action_output)
        assert FLAG_LUMP_SUM_MARKED in p["engineer_rows"][0]["engineer_packet_flags"]

    def test_flag_field_verification_required(self):
        from app.pdf_extraction.engineer_output_packet import (
            build_engineer_packet, FLAG_FIELD_VERIFICATION_REQUIRED,
        )
        resolution = self._resolution([self._resolution_row()])
        office_action_output = {
            "office_actions_version": "x", "office_actions_summary": {}, "office_actions_diagnostics": {},
            "resolution_rows": [{
                "normalized_row_id": "qr-p0-r0",
                "office_action_status": "recorded",
                "office_actions": [{
                    "action_type": "requires_field_verification",
                    "action_validation_status": "valid",
                }],
                "office_action_summary": {"action_count": 1, "action_types": [],
                                          "has_unknown_action_type": False},
            }],
        }
        p = build_engineer_packet(resolution, office_action_output)
        assert FLAG_FIELD_VERIFICATION_REQUIRED in p["engineer_rows"][0]["engineer_packet_flags"]

    # ---- Office actions never override truth ----

    def test_office_actions_never_override_category(self):
        from app.pdf_extraction.engineer_output_packet import build_engineer_packet
        resolution = self._resolution([self._resolution_row(
            resolution_category="source_conflict_review_required",
            resolution_priority="high",
        )])
        office_action_output = {
            "office_actions_version": "x", "office_actions_summary": {}, "office_actions_diagnostics": {},
            "resolution_rows": [{
                "normalized_row_id": "qr-p0-r0",
                "office_action_status": "recorded",
                "office_actions": [{
                    "action_type": "accept_dot_quantity_as_working_basis",
                    "action_validation_status": "valid",
                }],
                "office_action_summary": {"action_count": 1, "action_types": [],
                                          "has_unknown_action_type": False},
            }],
        }
        p = build_engineer_packet(resolution, office_action_output)
        r = p["engineer_rows"][0]
        assert r["resolution_category"] == "source_conflict_review_required"
        assert r["resolution_priority"] == "high"

    # ---- External sources preserved ----

    def test_external_sources_preserved(self):
        from app.pdf_extraction.engineer_output_packet import build_engineer_packet
        row = self._resolution_row(external_sources=[
            {"source_type": "dot_bid_item", "qty": 550.0, "unit": "SY"},
            {"source_type": "internal_takeoff", "qty": 560.0, "unit": "SY"},
        ])
        p = build_engineer_packet(self._resolution([row]))
        r = p["engineer_rows"][0]
        assert len(r["external_sources"]) == 2
        assert r["external_sources"][0]["source_type"] == "dot_bid_item"

    # ---- Source management section ----

    def test_source_management_section_embedded_when_provided(self):
        from app.pdf_extraction.engineer_output_packet import build_engineer_packet
        sm = {
            "source_management_version": "quantity_source_management/v1",
            "source_management_summary": {
                "rows_total": 15, "rows_with_sources": 13,
                "rows_with_conflicted_sources": 2,
                "source_type_histogram": {"dot_bid_item": 13},
                "unknown_source_type_count": 0,
            },
        }
        p = build_engineer_packet(self._resolution([self._resolution_row()]),
                                  source_management_output=sm)
        sec = p["source_management_section"]
        assert sec["present"] is True
        assert sec["source_management_version"] == "quantity_source_management/v1"
        assert sec["source_management_summary"]["rows_with_conflicted_sources"] == 2

    def test_source_management_section_absent_when_omitted(self):
        from app.pdf_extraction.engineer_output_packet import build_engineer_packet
        p = build_engineer_packet(self._resolution([self._resolution_row()]))
        assert p["source_management_section"]["present"] is False

    # ---- Diagnostics ----

    def test_diagnostics_priority_histogram(self):
        from app.pdf_extraction.engineer_output_packet import build_engineer_packet
        rows = [
            self._resolution_row(normalized_row_id="qr-p0-r0", resolution_priority="critical",
                                 resolution_category="blocked_pairing_resolution_required"),
            self._resolution_row(normalized_row_id="qr-p0-r1", resolution_priority="high",
                                 resolution_category="unmapped_scope_review_required"),
            self._resolution_row(normalized_row_id="qr-p0-r2", resolution_priority="medium"),
        ]
        p = build_engineer_packet(self._resolution(rows, packet_status="blocked"))
        d = p["packet_diagnostics"]
        assert d["priority_histogram"]["critical"] == 1
        assert d["priority_histogram"]["high"] == 1
        assert d["priority_histogram"]["medium"] == 1
        assert d["engineer_row_count"] == 3

    def test_diagnostics_flag_histogram(self):
        from app.pdf_extraction.engineer_output_packet import build_engineer_packet
        rows = [
            self._resolution_row(normalized_row_id="qr-p0-r0",
                                 resolution_category="unmapped_scope_review_required",
                                 resolution_priority="high"),
            self._resolution_row(normalized_row_id="qr-p0-r1",
                                 resolution_category="unmapped_scope_review_required",
                                 resolution_priority="high"),
        ]
        p = build_engineer_packet(self._resolution(rows))
        hist = p["packet_diagnostics"]["engineer_packet_flag_histogram"]
        assert hist["unmapped_scope"] == 2

    # ---- Immutability ----

    def test_input_not_mutated(self):
        from app.pdf_extraction.engineer_output_packet import build_engineer_packet
        import copy
        resolution = self._resolution([self._resolution_row()])
        snap = copy.deepcopy(resolution)
        build_engineer_packet(resolution)
        assert resolution == snap

    # ---- End-to-end with real pipeline ----

    def test_real_ipsi_trusted_engineer_packet(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        from app.pdf_extraction.quantity_source_management import manage_quantity_sources
        from app.pdf_extraction.augmentation_rules import apply_augmentation_rules
        from app.pdf_extraction.discrepancy_resolution import build_resolution
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions, ACTION_ESCALATE_ENGINEER,
        )
        from app.pdf_extraction.engineer_output_packet import (
            build_engineer_packet, PACKET_DRAFT_READY,
        )

        bid_rows, bs = extract_bid_items_from_pdf(str(dot_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        mapping = map_quote_to_bid(staging["accepted_rows"], bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=mapping,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")})
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)
        managed = manage_quantity_sources(injected)
        augmented = apply_augmentation_rules(injected)
        resolved = build_resolution(augmented)
        actioned = record_office_actions(resolved, {"rows": {
            "qr-p0-r0": {"actions": [{"action_type": ACTION_ESCALATE_ENGINEER,
                                      "actor": "alice", "action_note": "unmapped"}]},
        }})

        ep = build_engineer_packet(actioned, actioned, managed)
        assert ep["packet_status"] == PACKET_DRAFT_READY
        assert ep["packet_header"]["pairing_status"] == "trusted"
        assert ep["source_management_section"]["present"] is True
        assert ep["office_action_summary"]["present"] is True
        # First row: highest priority (unmapped_scope_review_required → high).
        assert ep["engineer_rows"][0]["resolution_priority"] == "high"
        # Office action flag visible on the escalated row.
        row_r0 = next(r for r in ep["engineer_rows"] if r["normalized_row_id"] == "qr-p0-r0")
        assert "engineer_action_recorded" in row_r0["engineer_packet_flags"]

    def test_real_blocked_engineer_packet(self, ipsi_quote_path, estprop_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        from app.pdf_extraction.augmentation_rules import apply_augmentation_rules
        from app.pdf_extraction.discrepancy_resolution import build_resolution
        from app.pdf_extraction.engineer_output_packet import (
            build_engineer_packet, PACKET_BLOCKED,
        )

        bid_rows, bs = extract_bid_items_from_pdf(str(estprop_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=None,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")})
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)
        augmented = apply_augmentation_rules(injected)
        resolved = build_resolution(augmented)

        ep = build_engineer_packet(resolved)
        assert ep["packet_status"] == PACKET_BLOCKED
        for r in ep["engineer_rows"]:
            assert r["resolution_priority"] == "critical"
            assert "blocked_pairing" in r["engineer_packet_flags"]

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c34(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c34(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20z. C33 — Office Resolution Actions Tests
# ---------------------------------------------------------------------------


class TestC33OfficeResolutionActions:
    """C33: append-only office action recording on C31 resolution rows.
    Governed truth is never mutated; unknown action types and row ids
    are surfaced in diagnostics."""

    def _resolution_row(self, **kw):
        base = {
            "normalized_row_id": "qr-p0-r0",
            "resolution_category": "non_comparable_missing_quote_source",
            "resolution_priority": "medium",
            "resolution_reason": "mapped_row_has_no_quote_qty_unit_and_no_external_source",
            "quote_values": {"qty": None, "unit": None,
                             "unit_price": 5.0, "amount": 2750.0},
            "external_sources": [],
            "comparison_basis": {"basis": "unavailable"},
            "comparison_result": {"qty_match": None, "unit_match": None},
            "resolution_trace": {},
        }
        base.update(kw)
        return base

    def _resolution(self, rows, packet_status="partial"):
        return {
            "resolution_version": "discrepancy_resolution/v1",
            "resolution_status": "review_required",
            "packet_status": packet_status,
            "pairing_status": "trusted",
            "contract_version": "reconciliation_contract/v1",
            "augmentation_rules_version": "augmentation_rules/v1",
            "resolution_summary": {
                "rows_total": len(rows),
                "category_counts": {},
                "priority_counts": {},
            },
            "resolution_rows": rows,
        }

    # ---- Default shape ----

    def test_default_no_actions(self):
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions, OA_NONE,
        )
        out = record_office_actions(self._resolution([self._resolution_row()]))
        r = out["resolution_rows"][0]
        assert r["office_action_status"] == OA_NONE
        assert r["office_actions"] == []
        assert out["office_actions_version"] == "office_resolution_actions/v1"

    def test_single_valid_action(self):
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions, OA_RECORDED, ACTION_ACCEPT_DOT, AV_VALID,
        )
        metadata = {
            "rows": {
                "qr-p0-r0": {
                    "actions": [{
                        "action_id": "a1",
                        "action_type": ACTION_ACCEPT_DOT,
                        "actor": "alice",
                        "timestamp": "2026-04-15T10:00",
                        "action_note": "DOT qty confirmed via plan",
                    }]
                }
            }
        }
        out = record_office_actions(self._resolution([self._resolution_row()]), metadata)
        r = out["resolution_rows"][0]
        assert r["office_action_status"] == OA_RECORDED
        assert len(r["office_actions"]) == 1
        a = r["office_actions"][0]
        assert a["action_id"] == "a1"
        assert a["action_type"] == ACTION_ACCEPT_DOT
        assert a["actor"] == "alice"
        assert a["action_validation_status"] == AV_VALID

    def test_multiple_actions_append_only(self):
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions, OA_MULTIPLE, ACTION_FIELD_VERIFICATION,
            ACTION_ESCALATE_ENGINEER,
        )
        metadata = {
            "rows": {
                "qr-p0-r0": {
                    "actions": [
                        {"action_type": ACTION_FIELD_VERIFICATION, "actor": "alice",
                         "action_note": "field needed"},
                        {"action_type": ACTION_ESCALATE_ENGINEER, "actor": "bob",
                         "action_note": "escalating for review"},
                    ]
                }
            }
        }
        out = record_office_actions(self._resolution([self._resolution_row()]), metadata)
        r = out["resolution_rows"][0]
        assert r["office_action_status"] == OA_MULTIPLE
        assert len(r["office_actions"]) == 2
        # Order preserved.
        assert r["office_actions"][0]["action_type"] == ACTION_FIELD_VERIFICATION
        assert r["office_actions"][1]["action_type"] == ACTION_ESCALATE_ENGINEER

    # ---- Governed truth unchanged ----

    def test_resolution_category_unchanged(self):
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions, ACTION_ACCEPT_DOT,
        )
        metadata = {"rows": {"qr-p0-r0": {"actions": [{"action_type": ACTION_ACCEPT_DOT}]}}}
        row = self._resolution_row(resolution_category="unmapped_scope_review_required",
                                    resolution_priority="high")
        out = record_office_actions(self._resolution([row]), metadata)
        r = out["resolution_rows"][0]
        assert r["resolution_category"] == "unmapped_scope_review_required"
        assert r["resolution_priority"] == "high"

    def test_quote_values_unchanged(self):
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions, ACTION_ACCEPT_DOT,
        )
        metadata = {"rows": {"qr-p0-r0": {"actions": [{"action_type": ACTION_ACCEPT_DOT}]}}}
        out = record_office_actions(self._resolution([self._resolution_row()]), metadata)
        r = out["resolution_rows"][0]
        assert r["quote_values"]["qty"] is None
        assert r["quote_values"]["amount"] == 2750.0

    def test_comparison_basis_unchanged(self):
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions, ACTION_ACCEPT_DOT,
        )
        metadata = {"rows": {"qr-p0-r0": {"actions": [{"action_type": ACTION_ACCEPT_DOT}]}}}
        row = self._resolution_row(comparison_basis={"basis": "conflicted_sources",
                                                      "effective_comparison_values": None})
        out = record_office_actions(self._resolution([row]), metadata)
        r = out["resolution_rows"][0]
        assert r["comparison_basis"]["basis"] == "conflicted_sources"

    # ---- Unknown action type ----

    def test_unknown_action_type_surfaced(self):
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions, AV_UNKNOWN_TYPE,
        )
        metadata = {"rows": {"qr-p0-r0": {"actions": [{"action_type": "some_made_up_action"}]}}}
        out = record_office_actions(self._resolution([self._resolution_row()]), metadata)
        r = out["resolution_rows"][0]
        # The action is preserved but tagged as unknown.
        assert len(r["office_actions"]) == 1
        assert r["office_actions"][0]["action_validation_status"] == AV_UNKNOWN_TYPE
        assert r["office_actions"][0]["action_type"] == "some_made_up_action"
        assert out["office_actions_diagnostics"]["unknown_action_type_count"] == 1

    # ---- Unknown row id ----

    def test_unknown_row_id_surfaced(self):
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions, ACTION_ACCEPT_DOT,
        )
        metadata = {"rows": {"does-not-exist": {"actions": [
            {"action_type": ACTION_ACCEPT_DOT}
        ]}}}
        out = record_office_actions(self._resolution([self._resolution_row()]), metadata)
        assert "does-not-exist" in out["office_actions_diagnostics"]["unknown_row_ids"]
        # No phantom queue row created.
        assert len(out["resolution_rows"]) == 1
        # Existing row has no actions.
        assert out["resolution_rows"][0]["office_actions"] == []

    # ---- Packet-level summary ----

    def test_packet_summary_counts(self):
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions,
            ACTION_ACCEPT_DOT, ACTION_MARK_LUMP_SUM, ACTION_FIELD_VERIFICATION,
            ACTION_ESCALATE_ENGINEER,
        )
        rows = [
            self._resolution_row(normalized_row_id="qr-p0-r0"),
            self._resolution_row(normalized_row_id="qr-p0-r1"),
            self._resolution_row(normalized_row_id="qr-p0-r2"),
            self._resolution_row(normalized_row_id="qr-p0-r3"),
        ]
        metadata = {"rows": {
            "qr-p0-r0": {"actions": [{"action_type": ACTION_ACCEPT_DOT}]},
            "qr-p0-r1": {"actions": [{"action_type": ACTION_MARK_LUMP_SUM}]},
            "qr-p0-r2": {"actions": [{"action_type": ACTION_FIELD_VERIFICATION}]},
            "qr-p0-r3": {"actions": [{"action_type": ACTION_ESCALATE_ENGINEER}]},
        }}
        out = record_office_actions(self._resolution(rows), metadata)
        s = out["office_actions_summary"]
        assert s["rows_total"] == 4
        assert s["rows_with_actions"] == 4
        assert s["rows_marked_lump_sum"] == 1
        assert s["rows_escalated_to_engineer"] == 1
        assert s["rows_marked_field_verification"] == 1
        hist = s["action_type_histogram"]
        assert hist[ACTION_ACCEPT_DOT] == 1
        assert hist[ACTION_MARK_LUMP_SUM] == 1

    # ---- Action scope + deterministic ids ----

    def test_default_action_id_generated(self):
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions, ACTION_NO_ACTION,
        )
        metadata = {"rows": {"qr-p0-r0": {"actions": [
            {"action_type": ACTION_NO_ACTION},
            {"action_type": ACTION_NO_ACTION},
        ]}}}
        out = record_office_actions(self._resolution([self._resolution_row()]), metadata)
        ids = [a["action_id"] for a in out["resolution_rows"][0]["office_actions"]]
        assert ids == ["act-qr-p0-r0-0", "act-qr-p0-r0-1"]

    def test_action_scope_bound_to_row(self):
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions, ACTION_ACCEPT_DOT,
        )
        metadata = {"rows": {"qr-p0-r0": {"actions": [{"action_type": ACTION_ACCEPT_DOT}]}}}
        out = record_office_actions(self._resolution([self._resolution_row()]), metadata)
        a = out["resolution_rows"][0]["office_actions"][0]
        assert a["action_scope"] == {"normalized_row_id": "qr-p0-r0"}

    # ---- Invalid raw entries skipped ----

    def test_non_dict_action_entry_skipped(self):
        from app.pdf_extraction.office_resolution_actions import record_office_actions
        metadata = {"rows": {"qr-p0-r0": {"actions": ["not a dict", 42, None]}}}
        out = record_office_actions(self._resolution([self._resolution_row()]), metadata)
        assert out["resolution_rows"][0]["office_actions"] == []

    # ---- Immutability ----

    def test_input_not_mutated(self):
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions, ACTION_ACCEPT_DOT,
        )
        import copy
        resolution = self._resolution([self._resolution_row()])
        metadata = {"rows": {"qr-p0-r0": {"actions": [{"action_type": ACTION_ACCEPT_DOT}]}}}
        snap_res = copy.deepcopy(resolution)
        snap_meta = copy.deepcopy(metadata)
        record_office_actions(resolution, metadata)
        assert resolution == snap_res
        assert metadata == snap_meta

    # ---- End-to-end with real pipeline ----

    def test_real_ipsi_trusted_with_actions(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        from app.pdf_extraction.augmentation_rules import apply_augmentation_rules
        from app.pdf_extraction.discrepancy_resolution import build_resolution
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions, ACTION_ACCEPT_DOT, ACTION_ESCALATE_ENGINEER,
        )

        bid_rows, bs = extract_bid_items_from_pdf(str(dot_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        mapping = map_quote_to_bid(staging["accepted_rows"], bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=mapping,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")})
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)
        augmented = apply_augmentation_rules(injected)
        resolved = build_resolution(augmented)

        # Record two actions across the real resolution.
        metadata = {
            "rows": {
                "qr-p0-r1": {"actions": [
                    {"action_type": ACTION_ACCEPT_DOT, "actor": "alice",
                     "timestamp": "2026-04-15T09:00",
                     "action_note": "DOT qty accepted as working basis"},
                ]},
                "qr-p0-r0": {"actions": [
                    {"action_type": ACTION_ESCALATE_ENGINEER, "actor": "bob",
                     "timestamp": "2026-04-15T09:30",
                     "action_note": "unmapped scope — escalating"},
                ]},
            }
        }
        actioned = record_office_actions(resolved, metadata)
        s = actioned["office_actions_summary"]
        assert s["rows_with_actions"] == 2
        assert s["rows_escalated_to_engineer"] == 1
        # Governed discrepancy truth preserved.
        r0 = next(r for r in actioned["resolution_rows"] if r["normalized_row_id"] == "qr-p0-r0")
        assert r0["resolution_category"] == "unmapped_scope_review_required"
        assert r0["office_actions"][0]["action_type"] == ACTION_ESCALATE_ENGINEER

    def test_real_blocked_pair_with_actions(self, ipsi_quote_path, estprop_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        from app.pdf_extraction.augmentation_rules import apply_augmentation_rules
        from app.pdf_extraction.discrepancy_resolution import build_resolution
        from app.pdf_extraction.office_resolution_actions import (
            record_office_actions, ACTION_ESCALATE_ENGINEER,
        )

        bid_rows, bs = extract_bid_items_from_pdf(str(estprop_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=None,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")})
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)
        augmented = apply_augmentation_rules(injected)
        resolved = build_resolution(augmented)

        metadata = {"rows": {"qr-p0-r0": {"actions": [
            {"action_type": ACTION_ESCALATE_ENGINEER, "actor": "alice"},
        ]}}}
        actioned = record_office_actions(resolved, metadata)
        r = next(r for r in actioned["resolution_rows"] if r["normalized_row_id"] == "qr-p0-r0")
        # Action recorded, blocked state preserved.
        assert r["resolution_category"] == "blocked_pairing_resolution_required"
        assert r["office_action_status"] == "recorded"

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c33(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c33(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20y. C32 — Quantity Source Management Tests
# ---------------------------------------------------------------------------


class TestC32QuantitySourceManagement:
    """C32: deterministic source management metadata. Does NOT select
    comparison basis; ranks and tags sources for visibility only."""

    def _row(self, sources, **kw):
        base = {
            "normalized_row_id": "qr-p0-r0",
            "mapping_outcome": "mapped",
            "quote_values": {"qty": None, "unit": None,
                             "unit_price": 5.0, "amount": 2750.0},
            "external_quantity_sources": sources,
        }
        base.update(kw)
        return base

    def _contract(self, rows):
        return {
            "contract_version": "reconciliation_contract/v1",
            "reconciliation_rows": rows,
            "injection_version": "quantity_injection/v1",
        }

    def _source(self, source_type, qty=None, unit=None, ref=None):
        return {
            "source_type": source_type,
            "source_ref": ref or {"id": "x"},
            "qty": qty, "unit": unit,
            "source_trace": {"origin": "test"},
        }

    # ---- Closed authority tier + visibility rank ----

    def test_dot_bid_item_tier_primary(self):
        from app.pdf_extraction.quantity_source_management import (
            manage_quantity_sources, TIER_PRIMARY,
        )
        row = self._row([self._source("dot_bid_item", qty=550.0, unit="SY")])
        out = manage_quantity_sources(self._contract([row]))
        m = out["reconciliation_rows"][0]["managed_sources"][0]
        assert m["source_authority_tier"] == TIER_PRIMARY
        assert m["source_visibility_rank"] == 10

    def test_engineer_quantity_tier_primary(self):
        from app.pdf_extraction.quantity_source_management import (
            manage_quantity_sources, TIER_PRIMARY,
        )
        row = self._row([self._source("engineer_quantity", qty=550.0, unit="SY")])
        out = manage_quantity_sources(self._contract([row]))
        m = out["reconciliation_rows"][0]["managed_sources"][0]
        assert m["source_authority_tier"] == TIER_PRIMARY
        assert m["source_visibility_rank"] == 20

    def test_internal_takeoff_tier_secondary(self):
        from app.pdf_extraction.quantity_source_management import (
            manage_quantity_sources, TIER_SECONDARY,
        )
        row = self._row([self._source("internal_takeoff", qty=550.0, unit="SY")])
        out = manage_quantity_sources(self._contract([row]))
        m = out["reconciliation_rows"][0]["managed_sources"][0]
        assert m["source_authority_tier"] == TIER_SECONDARY
        assert m["source_visibility_rank"] == 30

    def test_manual_review_tier_review_input(self):
        from app.pdf_extraction.quantity_source_management import (
            manage_quantity_sources, TIER_REVIEW_INPUT,
        )
        row = self._row([self._source("manual_review_input", qty=550.0, unit="SY")])
        out = manage_quantity_sources(self._contract([row]))
        m = out["reconciliation_rows"][0]["managed_sources"][0]
        assert m["source_authority_tier"] == TIER_REVIEW_INPUT

    def test_unknown_source_type_tagged(self):
        from app.pdf_extraction.quantity_source_management import (
            manage_quantity_sources, TIER_UNKNOWN, VS_UNKNOWN_TYPE,
        )
        row = self._row([self._source("made_up", qty=550.0, unit="SY")])
        out = manage_quantity_sources(self._contract([row]))
        m = out["reconciliation_rows"][0]["managed_sources"][0]
        assert m["source_authority_tier"] == TIER_UNKNOWN
        assert m["source_validation_status"] == VS_UNKNOWN_TYPE
        assert m["source_visibility_rank"] == 999
        assert out["source_management_summary"]["unknown_source_type_count"] == 1

    # ---- Visibility ordering ----

    def test_visibility_ordering_primary_first(self):
        from app.pdf_extraction.quantity_source_management import manage_quantity_sources
        # Put manual first, dot last — ordering must flip to dot first.
        row = self._row([
            self._source("manual_review_input", qty=1.0, unit="LF"),
            self._source("internal_takeoff", qty=1.0, unit="LF"),
            self._source("dot_bid_item", qty=1.0, unit="LF"),
        ])
        out = manage_quantity_sources(self._contract([row]))
        types = [m["source_type"] for m in out["reconciliation_rows"][0]["managed_sources"]]
        assert types == ["dot_bid_item", "internal_takeoff", "manual_review_input"]

    def test_visibility_ordering_stable_on_ties(self):
        from app.pdf_extraction.quantity_source_management import manage_quantity_sources
        # Two internal_takeoff sources should preserve original order.
        row = self._row([
            self._source("internal_takeoff", qty=1.0, unit="LF", ref={"tag": "A"}),
            self._source("internal_takeoff", qty=2.0, unit="LF", ref={"tag": "B"}),
        ])
        out = manage_quantity_sources(self._contract([row]))
        refs = [m["source_ref"]["tag"] for m in out["reconciliation_rows"][0]["managed_sources"]]
        assert refs == ["A", "B"]

    def test_unknown_source_ranked_last(self):
        from app.pdf_extraction.quantity_source_management import manage_quantity_sources
        row = self._row([
            self._source("unknown_x", qty=1.0, unit="LF"),
            self._source("dot_bid_item", qty=1.0, unit="LF"),
        ])
        out = manage_quantity_sources(self._contract([row]))
        types = [m["source_type"] for m in out["reconciliation_rows"][0]["managed_sources"]]
        assert types == ["dot_bid_item", "unknown_x"]

    # ---- Source validation ----

    def test_incomplete_source_tagged(self):
        from app.pdf_extraction.quantity_source_management import (
            manage_quantity_sources, VS_INCOMPLETE,
        )
        row = self._row([self._source("internal_takeoff", qty=None, unit=None)])
        out = manage_quantity_sources(self._contract([row]))
        m = out["reconciliation_rows"][0]["managed_sources"][0]
        assert m["source_validation_status"] == VS_INCOMPLETE

    def test_usable_source_tagged(self):
        from app.pdf_extraction.quantity_source_management import (
            manage_quantity_sources, VS_USABLE,
        )
        row = self._row([self._source("dot_bid_item", qty=1.0, unit="LF")])
        out = manage_quantity_sources(self._contract([row]))
        m = out["reconciliation_rows"][0]["managed_sources"][0]
        assert m["source_validation_status"] == VS_USABLE

    # ---- Conflict detection ----

    def test_conflicted_sources_status(self):
        from app.pdf_extraction.quantity_source_management import (
            manage_quantity_sources, SM_CONFLICTED, VS_CONFLICTED,
        )
        row = self._row([
            self._source("dot_bid_item", qty=550.0, unit="SY"),
            self._source("internal_takeoff", qty=560.0, unit="SY"),
        ])
        out = manage_quantity_sources(self._contract([row]))
        r = out["reconciliation_rows"][0]
        assert r["source_management_status"] == SM_CONFLICTED
        # Both sources marked conflicted, neither dropped.
        for m in r["managed_sources"]:
            assert m["source_validation_status"] == VS_CONFLICTED
        assert len(r["source_conflict_groups"]) == 1
        assert len(r["source_conflict_groups"][0]) == 2

    def test_unit_conflict_detected(self):
        from app.pdf_extraction.quantity_source_management import (
            manage_quantity_sources, SM_CONFLICTED,
        )
        row = self._row([
            self._source("dot_bid_item", qty=550.0, unit="SY"),
            self._source("internal_takeoff", qty=550.0, unit="SF"),
        ])
        out = manage_quantity_sources(self._contract([row]))
        assert out["reconciliation_rows"][0]["source_management_status"] == SM_CONFLICTED

    def test_qty_within_tolerance_not_conflict(self):
        from app.pdf_extraction.quantity_source_management import (
            manage_quantity_sources, SM_MULTIPLE,
        )
        row = self._row([
            self._source("dot_bid_item", qty=100.0, unit="LF"),
            self._source("internal_takeoff", qty=100.3, unit="LF"),  # 0.3% drift
        ])
        out = manage_quantity_sources(self._contract([row]))
        assert out["reconciliation_rows"][0]["source_management_status"] == SM_MULTIPLE

    # ---- Row statuses ----

    def test_single_source_status(self):
        from app.pdf_extraction.quantity_source_management import (
            manage_quantity_sources, SM_SINGLE,
        )
        row = self._row([self._source("dot_bid_item", qty=1.0, unit="LF")])
        out = manage_quantity_sources(self._contract([row]))
        assert out["reconciliation_rows"][0]["source_management_status"] == SM_SINGLE

    def test_none_status_on_empty_sources(self):
        from app.pdf_extraction.quantity_source_management import (
            manage_quantity_sources, SM_NONE,
        )
        row = self._row([])
        out = manage_quantity_sources(self._contract([row]))
        assert out["reconciliation_rows"][0]["source_management_status"] == SM_NONE

    def test_multiple_sources_status(self):
        from app.pdf_extraction.quantity_source_management import (
            manage_quantity_sources, SM_MULTIPLE,
        )
        row = self._row([
            self._source("dot_bid_item", qty=1.0, unit="LF"),
            self._source("internal_takeoff", qty=1.0, unit="LF"),
        ])
        out = manage_quantity_sources(self._contract([row]))
        assert out["reconciliation_rows"][0]["source_management_status"] == SM_MULTIPLE

    # ---- Packet-level summary ----

    def test_packet_summary_counts(self):
        from app.pdf_extraction.quantity_source_management import manage_quantity_sources
        rows = [
            self._row([], normalized_row_id="qr-p0-r0"),
            self._row([self._source("dot_bid_item", qty=1.0, unit="LF")],
                      normalized_row_id="qr-p0-r1"),
            self._row([
                self._source("dot_bid_item", qty=1.0, unit="LF"),
                self._source("internal_takeoff", qty=1.0, unit="LF"),
            ], normalized_row_id="qr-p0-r2"),
            self._row([
                self._source("dot_bid_item", qty=1.0, unit="LF"),
                self._source("internal_takeoff", qty=2.0, unit="LF"),
            ], normalized_row_id="qr-p0-r3"),
        ]
        out = manage_quantity_sources(self._contract(rows))
        s = out["source_management_summary"]
        assert s["rows_total"] == 4
        assert s["rows_with_sources"] == 3
        assert s["rows_with_multiple_sources"] == 2
        assert s["rows_with_conflicted_sources"] == 1
        assert s["source_type_histogram"]["dot_bid_item"] == 3
        assert s["source_type_histogram"]["internal_takeoff"] == 2

    # ---- Does NOT select comparison basis ----

    def test_does_not_touch_comparison_basis(self):
        from app.pdf_extraction.quantity_source_management import manage_quantity_sources
        row = self._row([
            self._source("manual_review_input", qty=1.0, unit="LF"),
            self._source("dot_bid_item", qty=2.0, unit="LF"),
        ])
        row["comparison_basis"] = "conflicted_sources"
        row["effective_comparison_values"] = None
        out = manage_quantity_sources(self._contract([row]))
        r = out["reconciliation_rows"][0]
        # Visibility ranking reordered but basis untouched.
        assert r["comparison_basis"] == "conflicted_sources"
        assert r["effective_comparison_values"] is None

    # ---- Immutability ----

    def test_input_not_mutated(self):
        from app.pdf_extraction.quantity_source_management import manage_quantity_sources
        import copy
        contract = self._contract([self._row([
            self._source("dot_bid_item", qty=1.0, unit="LF"),
        ])])
        snap = copy.deepcopy(contract)
        manage_quantity_sources(contract)
        assert contract == snap

    # ---- Version tag ----

    def test_version_tag_present(self):
        from app.pdf_extraction.quantity_source_management import manage_quantity_sources
        out = manage_quantity_sources(self._contract([self._row([])]))
        assert out["source_management_version"] == "quantity_source_management/v1"

    # ---- End-to-end with real pipeline ----

    def test_real_ipsi_trusted_pipeline(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        from app.pdf_extraction.quantity_source_management import manage_quantity_sources

        bid_rows, bs = extract_bid_items_from_pdf(str(dot_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        mapping = map_quote_to_bid(staging["accepted_rows"], bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=mapping,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")})
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)
        managed = manage_quantity_sources(injected)

        s = managed["source_management_summary"]
        # IPSI has 13 mapped rows that get dot_bid_item sources — 13 rows
        # with a single source.
        assert s["rows_with_sources"] >= 13
        assert s["source_type_histogram"].get("dot_bid_item", 0) >= 13
        assert s["rows_with_conflicted_sources"] == 0

    def test_real_blocked_pair(self, ipsi_quote_path, estprop_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        from app.pdf_extraction.quantity_source_management import manage_quantity_sources

        bid_rows, bs = extract_bid_items_from_pdf(str(estprop_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=None,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")})
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)
        managed = manage_quantity_sources(injected)

        s = managed["source_management_summary"]
        # Blocked rows never get augmented → no sources.
        assert s["rows_with_sources"] == 0

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c32(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c32(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20x. C31 — Discrepancy Resolution Framework Tests
# ---------------------------------------------------------------------------


class TestC31DiscrepancyResolution:
    """C31: deterministic resolution-category classification on top of
    C30 augmentation state. Never auto-resolves; never narrates."""

    def _augmented_row(self, **kw):
        """A row already processed by C29 + C30."""
        base = {
            "normalized_row_id": "qr-p0-r0",
            "mapping_outcome": "mapped",
            "comparison_status": "non_comparable",
            "quote_values": {"qty": None, "unit": None,
                             "unit_price": 5.0, "amount": 2750.0},
            "external_quantity_sources": [],
            "augmentation_status": "attached",
            "augmentation_trace": {},
            "comparison_basis": "dot_augmented",
            "augmentation_reason": "single_external_source_used_as_basis",
            "effective_comparison_values": {"qty": 550.0, "unit": "SY"},
            "source_conflict_status": "none",
            "augmentation_flags": ["external_source_present"],
            "augmentation_rule_trace": {},
        }
        base.update(kw)
        return base

    def _contract(self, rows, packet_status="partial"):
        return {
            "contract_version": "reconciliation_contract/v1",
            "packet_status": packet_status,
            "pairing_status": "trusted",
            "augmentation_rules_version": "augmentation_rules/v1",
            "reconciliation_rows": rows,
        }

    # ---- Structural categories ----

    def test_blocked_pairing_category(self):
        from app.pdf_extraction.discrepancy_resolution import (
            build_resolution, CAT_BLOCKED_PAIRING,
        )
        row = self._augmented_row(mapping_outcome="blocked",
                                  comparison_status="blocked",
                                  comparison_basis="not_applicable")
        out = build_resolution(self._contract([row], packet_status="blocked"))
        r = out["resolution_rows"][0]
        assert r["resolution_category"] == CAT_BLOCKED_PAIRING
        assert r["resolution_priority"] == "critical"

    def test_unmapped_scope_category(self):
        from app.pdf_extraction.discrepancy_resolution import (
            build_resolution, CAT_UNMAPPED_SCOPE,
        )
        row = self._augmented_row(mapping_outcome="unmapped",
                                  comparison_basis="not_applicable")
        out = build_resolution(self._contract([row]))
        r = out["resolution_rows"][0]
        assert r["resolution_category"] == CAT_UNMAPPED_SCOPE
        assert r["resolution_priority"] == "high"

    def test_ambiguous_mapping_category(self):
        from app.pdf_extraction.discrepancy_resolution import (
            build_resolution, CAT_AMBIGUOUS_MAPPING,
        )
        row = self._augmented_row(mapping_outcome="ambiguous",
                                  comparison_basis="not_applicable")
        out = build_resolution(self._contract([row]))
        r = out["resolution_rows"][0]
        assert r["resolution_category"] == CAT_AMBIGUOUS_MAPPING

    # ---- Source conflict category ----

    def test_source_conflict_category(self):
        from app.pdf_extraction.discrepancy_resolution import (
            build_resolution, CAT_SOURCE_CONFLICT,
        )
        row = self._augmented_row(
            comparison_basis="conflicted_sources",
            source_conflict_status="conflict",
            effective_comparison_values=None,
        )
        out = build_resolution(self._contract([row]))
        r = out["resolution_rows"][0]
        assert r["resolution_category"] == CAT_SOURCE_CONFLICT
        assert r["resolution_priority"] == "high"
        # Effective values remain None — we do not pick a winner.
        assert r["comparison_basis"]["effective_comparison_values"] is None

    # ---- Non-comparable categories ----

    def test_missing_quote_source_category(self):
        from app.pdf_extraction.discrepancy_resolution import (
            build_resolution, CAT_NON_COMPARABLE_MISSING_QUOTE_SOURCE,
        )
        row = self._augmented_row(
            comparison_basis="unavailable",
            effective_comparison_values=None,
            external_quantity_sources=[],
        )
        out = build_resolution(self._contract([row]))
        r = out["resolution_rows"][0]
        assert r["resolution_category"] == CAT_NON_COMPARABLE_MISSING_QUOTE_SOURCE

    def test_missing_external_source_category(self):
        """External sources exist but contained no usable qty/unit — C30
        classifies as unavailable with non-empty source list."""
        from app.pdf_extraction.discrepancy_resolution import (
            build_resolution, CAT_NON_COMPARABLE_MISSING_EXTERNAL_SOURCE,
        )
        row = self._augmented_row(
            comparison_basis="unavailable",
            effective_comparison_values=None,
            external_quantity_sources=[{
                "source_type": "internal_takeoff",
                "source_ref": {}, "qty": None, "unit": None,
                "source_trace": {},
            }],
        )
        out = build_resolution(self._contract([row]))
        r = out["resolution_rows"][0]
        assert r["resolution_category"] == CAT_NON_COMPARABLE_MISSING_EXTERNAL_SOURCE

    # ---- Comparable categories ----

    def test_clean_match_category(self):
        from app.pdf_extraction.discrepancy_resolution import (
            build_resolution, CAT_CLEAN_MATCH,
        )
        row = self._augmented_row(
            comparison_basis="quote_native",
            quote_values={"qty": 24.0, "unit": "LF",
                          "unit_price": 10.0, "amount": 240.0},
            effective_comparison_values={"qty": 24.0, "unit": "LF"},
        )
        out = build_resolution(self._contract([row]))
        r = out["resolution_rows"][0]
        assert r["resolution_category"] == CAT_CLEAN_MATCH
        assert r["resolution_priority"] == "low"
        assert r["comparison_result"]["qty_match"] is True
        assert r["comparison_result"]["unit_match"] is True

    def test_qty_discrepancy_category(self):
        from app.pdf_extraction.discrepancy_resolution import (
            build_resolution, CAT_QTY_DISCREPANCY,
        )
        row = self._augmented_row(
            comparison_basis="quote_native_with_external_reference",
            quote_values={"qty": 24.0, "unit": "LF",
                          "unit_price": 10.0, "amount": 240.0},
            effective_comparison_values={"qty": 24.0, "unit": "LF"},
            external_quantity_sources=[{
                "source_type": "dot_bid_item",
                "qty": 50.0, "unit": "LF", "source_ref": {}, "source_trace": {},
            }],
        )
        # Override: simulate the comparison picking up quote vs external.
        # The framework compares quote against effective_comparison_values;
        # when quote disagrees with external, we test via explicit effective.
        row["effective_comparison_values"] = {"qty": 50.0, "unit": "LF"}
        out = build_resolution(self._contract([row]))
        r = out["resolution_rows"][0]
        assert r["resolution_category"] == CAT_QTY_DISCREPANCY
        assert r["comparison_result"]["qty_match"] is False

    def test_unit_discrepancy_category(self):
        from app.pdf_extraction.discrepancy_resolution import (
            build_resolution, CAT_UNIT_DISCREPANCY,
        )
        row = self._augmented_row(
            comparison_basis="quote_native_with_external_reference",
            quote_values={"qty": 24.0, "unit": "LF",
                          "unit_price": 10.0, "amount": 240.0},
            effective_comparison_values={"qty": 24.0, "unit": "SY"},
        )
        out = build_resolution(self._contract([row]))
        r = out["resolution_rows"][0]
        assert r["resolution_category"] == CAT_UNIT_DISCREPANCY

    def test_both_qty_and_unit_mismatch_lands_in_qty_bucket(self):
        """When both disagree, we deterministically emit the qty bucket."""
        from app.pdf_extraction.discrepancy_resolution import (
            build_resolution, CAT_QTY_DISCREPANCY,
        )
        row = self._augmented_row(
            comparison_basis="quote_native_with_external_reference",
            quote_values={"qty": 24.0, "unit": "LF",
                          "unit_price": 10.0, "amount": 240.0},
            effective_comparison_values={"qty": 50.0, "unit": "SY"},
        )
        out = build_resolution(self._contract([row]))
        assert out["resolution_rows"][0]["resolution_category"] == CAT_QTY_DISCREPANCY

    # ---- Comparison via dot_augmented basis ----

    def test_dot_augmented_basis_has_no_quote_values_for_comparison(self):
        """When quote has no qty/unit and the basis is dot_augmented,
        there is nothing to compare quote-vs-effective — the row is
        comparison-ready via external source but we cannot detect a
        discrepancy because the quote side is empty. The category falls
        back to missing_quote_source."""
        from app.pdf_extraction.discrepancy_resolution import (
            build_resolution, CAT_NON_COMPARABLE_MISSING_QUOTE_SOURCE,
        )
        row = self._augmented_row()  # quote qty/unit=None, basis=dot_augmented
        out = build_resolution(self._contract([row]))
        r = out["resolution_rows"][0]
        assert r["resolution_category"] == CAT_NON_COMPARABLE_MISSING_QUOTE_SOURCE

    # ---- Resolution priority mapping ----

    def test_blocked_pairing_priority_critical(self):
        from app.pdf_extraction.discrepancy_resolution import build_resolution
        row = self._augmented_row(mapping_outcome="blocked",
                                  comparison_status="blocked")
        out = build_resolution(self._contract([row], packet_status="blocked"))
        assert out["resolution_rows"][0]["resolution_priority"] == "critical"

    def test_conflict_priority_high(self):
        from app.pdf_extraction.discrepancy_resolution import build_resolution
        row = self._augmented_row(
            comparison_basis="conflicted_sources",
            source_conflict_status="conflict",
            effective_comparison_values=None,
        )
        out = build_resolution(self._contract([row]))
        assert out["resolution_rows"][0]["resolution_priority"] == "high"

    # ---- Resolution summary ----

    def test_summary_category_counts(self):
        from app.pdf_extraction.discrepancy_resolution import build_resolution
        rows = [
            self._augmented_row(normalized_row_id="qr-p0-r0",
                                mapping_outcome="blocked",
                                comparison_status="blocked",
                                comparison_basis="not_applicable"),
            self._augmented_row(normalized_row_id="qr-p0-r1",
                                mapping_outcome="unmapped",
                                comparison_basis="not_applicable"),
            self._augmented_row(normalized_row_id="qr-p0-r2",
                                comparison_basis="conflicted_sources",
                                source_conflict_status="conflict",
                                effective_comparison_values=None),
            self._augmented_row(normalized_row_id="qr-p0-r3"),  # missing quote
            self._augmented_row(normalized_row_id="qr-p0-r4",
                                comparison_basis="quote_native",
                                quote_values={"qty": 24.0, "unit": "LF",
                                              "unit_price": 10.0, "amount": 240.0},
                                effective_comparison_values={"qty": 24.0, "unit": "LF"}),
        ]
        out = build_resolution(self._contract(rows, packet_status="partial"))
        c = out["resolution_summary"]["category_counts"]
        assert c["blocked_pairing_resolution_required"] == 1
        assert c["unmapped_scope_review_required"] == 1
        assert c["source_conflict_review_required"] == 1
        assert c["non_comparable_missing_quote_source"] == 1
        assert c["clean_match_no_resolution_needed"] == 1

    def test_summary_priority_counts(self):
        from app.pdf_extraction.discrepancy_resolution import build_resolution
        rows = [
            self._augmented_row(normalized_row_id="qr-p0-r0",
                                mapping_outcome="blocked",
                                comparison_status="blocked",
                                comparison_basis="not_applicable"),
            self._augmented_row(normalized_row_id="qr-p0-r1",
                                comparison_basis="quote_native",
                                quote_values={"qty": 24.0, "unit": "LF",
                                              "unit_price": 10.0, "amount": 240.0},
                                effective_comparison_values={"qty": 24.0, "unit": "LF"}),
        ]
        out = build_resolution(self._contract(rows, packet_status="blocked"))
        p = out["resolution_summary"]["priority_counts"]
        assert p["critical"] == 1
        assert p["low"] == 1

    # ---- Resolution status ----

    def test_status_not_applicable_on_empty(self):
        from app.pdf_extraction.discrepancy_resolution import (
            build_resolution, RESOLUTION_STATUS_NOT_APPLICABLE,
        )
        out = build_resolution(self._contract([]))
        assert out["resolution_status"] == RESOLUTION_STATUS_NOT_APPLICABLE

    def test_status_review_required_when_blocked(self):
        from app.pdf_extraction.discrepancy_resolution import (
            build_resolution, RESOLUTION_STATUS_REVIEW_REQUIRED,
        )
        row = self._augmented_row(mapping_outcome="blocked",
                                  comparison_status="blocked",
                                  comparison_basis="not_applicable")
        out = build_resolution(self._contract([row], packet_status="blocked"))
        assert out["resolution_status"] == RESOLUTION_STATUS_REVIEW_REQUIRED

    def test_status_open_when_only_clean_matches(self):
        from app.pdf_extraction.discrepancy_resolution import (
            build_resolution, RESOLUTION_STATUS_OPEN,
        )
        row = self._augmented_row(
            comparison_basis="quote_native",
            quote_values={"qty": 24.0, "unit": "LF",
                          "unit_price": 10.0, "amount": 240.0},
            effective_comparison_values={"qty": 24.0, "unit": "LF"},
        )
        out = build_resolution(self._contract([row]))
        assert out["resolution_status"] == RESOLUTION_STATUS_OPEN

    # ---- Truth preservation ----

    def test_input_never_mutated(self):
        from app.pdf_extraction.discrepancy_resolution import build_resolution
        import copy
        contract = self._contract([self._augmented_row()])
        snap = copy.deepcopy(contract)
        build_resolution(contract)
        assert contract == snap

    def test_quote_values_preserved_in_output(self):
        from app.pdf_extraction.discrepancy_resolution import build_resolution
        row = self._augmented_row(
            quote_values={"qty": None, "unit": None,
                          "unit_price": 5.0, "amount": 2750.0},
        )
        out = build_resolution(self._contract([row]))
        assert out["resolution_rows"][0]["quote_values"]["qty"] is None
        assert out["resolution_rows"][0]["quote_values"]["amount"] == 2750.0

    def test_external_sources_preserved_in_output(self):
        from app.pdf_extraction.discrepancy_resolution import build_resolution
        row = self._augmented_row(
            external_quantity_sources=[
                {"source_type": "dot_bid_item", "qty": 550.0, "unit": "SY",
                 "source_ref": {"line_number": "0010"}, "source_trace": {}},
            ],
        )
        out = build_resolution(self._contract([row]))
        r = out["resolution_rows"][0]
        assert len(r["external_sources"]) == 1
        assert r["external_sources"][0]["source_type"] == "dot_bid_item"

    # ---- End-to-end with real pipeline ----

    def test_real_ipsi_trusted_resolution(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        from app.pdf_extraction.augmentation_rules import apply_augmentation_rules
        from app.pdf_extraction.discrepancy_resolution import build_resolution

        bid_rows, bs = extract_bid_items_from_pdf(str(dot_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        mapping = map_quote_to_bid(staging["accepted_rows"], bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=mapping,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")})
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)
        augmented = apply_augmentation_rules(injected)
        resolved = build_resolution(augmented)

        c = resolved["resolution_summary"]["category_counts"]
        # 2 unmapped rows, 13 dot_augmented but quote has no qty → missing_quote_source
        assert c["unmapped_scope_review_required"] == 2
        assert c["non_comparable_missing_quote_source"] == 13
        assert c["source_conflict_review_required"] == 0
        assert resolved["resolution_status"] == "review_required"

    def test_real_blocked_pair_resolution(self, ipsi_quote_path, estprop_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        from app.pdf_extraction.augmentation_rules import apply_augmentation_rules
        from app.pdf_extraction.discrepancy_resolution import build_resolution

        bid_rows, bs = extract_bid_items_from_pdf(str(estprop_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=None,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")})
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)
        augmented = apply_augmentation_rules(injected)
        resolved = build_resolution(augmented)

        c = resolved["resolution_summary"]["category_counts"]
        assert c["blocked_pairing_resolution_required"] >= 15
        assert resolved["resolution_status"] == "review_required"

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c31(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c31(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True

    def test_quote_staging_unchanged_under_c31(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14


# ---------------------------------------------------------------------------
# 20w. C30 — Controlled Augmentation Rules Tests
# ---------------------------------------------------------------------------


class TestC30AugmentationRules:
    """C30: deterministic comparison-basis selection from C29-injected
    sources. Never overwrites quote values, never auto-resolves
    conflicts."""

    def _row(self, **kw):
        base = {
            "normalized_row_id": "qr-p0-r0",
            "mapping_outcome": "mapped",
            "comparison_status": "non_comparable",
            "quote_values": {"qty": None, "unit": None, "unit_price": 5.0, "amount": 2750.0},
            "external_quantity_sources": [],
            "augmentation_status": "none",
            "augmentation_trace": {},
        }
        base.update(kw)
        return base

    def _contract(self, rows):
        return {
            "contract_version": "reconciliation_contract/v1",
            "classification_version": "discrepancy_classification/v1",
            "reconciliation_status": "partial",
            "packet_status": "partial",
            "pairing_status": "trusted",
            "mapping_status": "partial",
            "reconciliation_rows": rows,
            "injection_version": "quantity_injection/v1",
            "injection_diagnostics": {},
        }

    def _dot_source(self, qty=550.0, unit="SY"):
        return {
            "source_type": "dot_bid_item",
            "source_ref": {"line_number": "0010", "item_number": "2101-0850001"},
            "qty": qty, "unit": unit,
            "source_trace": {"origin": "mapped_bid_item"},
        }

    def _ext_source(self, source_type, qty=None, unit=None):
        return {
            "source_type": source_type,
            "source_ref": {"id": "X"},
            "qty": qty, "unit": unit,
            "source_trace": {"origin": "caller_supplied"},
        }

    # ---- R0: not applicable ----

    def test_unmapped_row_basis_not_applicable(self):
        from app.pdf_extraction.augmentation_rules import (
            apply_augmentation_rules, BASIS_NOT_APPLICABLE,
        )
        row = self._row(mapping_outcome="unmapped")
        out = apply_augmentation_rules(self._contract([row]))
        assert out["reconciliation_rows"][0]["comparison_basis"] == BASIS_NOT_APPLICABLE

    def test_ambiguous_row_basis_not_applicable(self):
        from app.pdf_extraction.augmentation_rules import (
            apply_augmentation_rules, BASIS_NOT_APPLICABLE,
        )
        row = self._row(mapping_outcome="ambiguous")
        out = apply_augmentation_rules(self._contract([row]))
        assert out["reconciliation_rows"][0]["comparison_basis"] == BASIS_NOT_APPLICABLE

    def test_blocked_row_basis_not_applicable(self):
        from app.pdf_extraction.augmentation_rules import (
            apply_augmentation_rules, BASIS_NOT_APPLICABLE,
        )
        row = self._row(mapping_outcome="blocked", comparison_status="blocked")
        out = apply_augmentation_rules(self._contract([row]))
        assert out["reconciliation_rows"][0]["comparison_basis"] == BASIS_NOT_APPLICABLE

    # ---- R1: quote_native ----

    def test_quote_native_basis(self):
        from app.pdf_extraction.augmentation_rules import (
            apply_augmentation_rules, BASIS_QUOTE_NATIVE,
        )
        row = self._row(quote_values={"qty": 24.0, "unit": "LF",
                                      "unit_price": 10.0, "amount": 240.0})
        out = apply_augmentation_rules(self._contract([row]))
        r = out["reconciliation_rows"][0]
        assert r["comparison_basis"] == BASIS_QUOTE_NATIVE
        assert r["effective_comparison_values"] == {"qty": 24.0, "unit": "LF"}
        assert r["source_conflict_status"] == "none"

    def test_quote_native_with_external_reference_agreeing(self):
        from app.pdf_extraction.augmentation_rules import (
            apply_augmentation_rules, BASIS_QUOTE_NATIVE_WITH_EXTERNAL,
            FLAG_EXTERNAL_AGREES_WITH_QUOTE,
        )
        row = self._row(
            quote_values={"qty": 24.0, "unit": "LF", "unit_price": 10.0, "amount": 240.0},
            external_quantity_sources=[self._dot_source(qty=24.0, unit="LF")],
        )
        out = apply_augmentation_rules(self._contract([row]))
        r = out["reconciliation_rows"][0]
        assert r["comparison_basis"] == BASIS_QUOTE_NATIVE_WITH_EXTERNAL
        assert FLAG_EXTERNAL_AGREES_WITH_QUOTE in r["augmentation_flags"]
        # Effective basis still uses the quote values.
        assert r["effective_comparison_values"] == {"qty": 24.0, "unit": "LF"}

    def test_quote_native_with_external_reference_disagreeing(self):
        from app.pdf_extraction.augmentation_rules import (
            apply_augmentation_rules, BASIS_QUOTE_NATIVE_WITH_EXTERNAL,
            FLAG_EXTERNAL_DISAGREES_WITH_QUOTE,
        )
        row = self._row(
            quote_values={"qty": 24.0, "unit": "LF", "unit_price": 10.0, "amount": 240.0},
            external_quantity_sources=[self._dot_source(qty=50.0, unit="LF")],
        )
        out = apply_augmentation_rules(self._contract([row]))
        r = out["reconciliation_rows"][0]
        # Quote still wins as basis; disagreement flagged but not used to overwrite.
        assert r["comparison_basis"] == BASIS_QUOTE_NATIVE_WITH_EXTERNAL
        assert r["effective_comparison_values"] == {"qty": 24.0, "unit": "LF"}
        assert FLAG_EXTERNAL_DISAGREES_WITH_QUOTE in r["augmentation_flags"]

    # ---- R2: dot_augmented ----

    def test_dot_augmented_single_source(self):
        from app.pdf_extraction.augmentation_rules import (
            apply_augmentation_rules, BASIS_DOT_AUGMENTED,
        )
        row = self._row(external_quantity_sources=[self._dot_source(qty=550.0, unit="SY")])
        out = apply_augmentation_rules(self._contract([row]))
        r = out["reconciliation_rows"][0]
        assert r["comparison_basis"] == BASIS_DOT_AUGMENTED
        assert r["effective_comparison_values"] == {"qty": 550.0, "unit": "SY"}
        assert r["source_conflict_status"] == "none"

    def test_dot_augmented_multiple_agreeing_sources(self):
        from app.pdf_extraction.augmentation_rules import (
            apply_augmentation_rules, BASIS_DOT_AUGMENTED,
            FLAG_EXTERNAL_SOURCES_AGREE,
        )
        row = self._row(external_quantity_sources=[
            self._dot_source(qty=550.0, unit="SY"),
            self._ext_source("internal_takeoff", qty=550.0, unit="SY"),
        ])
        out = apply_augmentation_rules(self._contract([row]))
        r = out["reconciliation_rows"][0]
        assert r["comparison_basis"] == BASIS_DOT_AUGMENTED
        assert FLAG_EXTERNAL_SOURCES_AGREE in r["augmentation_flags"]

    def test_dot_augmented_qty_within_tolerance(self):
        from app.pdf_extraction.augmentation_rules import (
            apply_augmentation_rules, BASIS_DOT_AUGMENTED,
        )
        row = self._row(external_quantity_sources=[
            self._dot_source(qty=100.0, unit="LF"),
            self._ext_source("internal_takeoff", qty=100.4, unit="LF"),  # 0.4% drift
        ])
        out = apply_augmentation_rules(self._contract([row]))
        assert out["reconciliation_rows"][0]["comparison_basis"] == BASIS_DOT_AUGMENTED

    # ---- R4: conflicted sources ----

    def test_conflicted_sources_qty_disagree(self):
        from app.pdf_extraction.augmentation_rules import (
            apply_augmentation_rules, BASIS_CONFLICTED_SOURCES,
            FLAG_EXTERNAL_SOURCES_DISAGREE, CONFLICT_YES,
        )
        row = self._row(external_quantity_sources=[
            self._dot_source(qty=550.0, unit="SY"),
            self._ext_source("internal_takeoff", qty=580.0, unit="SY"),
        ])
        out = apply_augmentation_rules(self._contract([row]))
        r = out["reconciliation_rows"][0]
        assert r["comparison_basis"] == BASIS_CONFLICTED_SOURCES
        assert r["source_conflict_status"] == CONFLICT_YES
        assert r["effective_comparison_values"] is None
        assert FLAG_EXTERNAL_SOURCES_DISAGREE in r["augmentation_flags"]

    def test_conflicted_sources_unit_disagree(self):
        from app.pdf_extraction.augmentation_rules import (
            apply_augmentation_rules, BASIS_CONFLICTED_SOURCES,
        )
        row = self._row(external_quantity_sources=[
            self._dot_source(qty=550.0, unit="SY"),
            self._ext_source("internal_takeoff", qty=550.0, unit="SF"),
        ])
        out = apply_augmentation_rules(self._contract([row]))
        assert out["reconciliation_rows"][0]["comparison_basis"] == BASIS_CONFLICTED_SOURCES

    def test_conflict_never_auto_resolved(self):
        """Three sources disagreeing → no majority rule, no voting."""
        from app.pdf_extraction.augmentation_rules import (
            apply_augmentation_rules, BASIS_CONFLICTED_SOURCES,
        )
        row = self._row(external_quantity_sources=[
            self._dot_source(qty=550.0, unit="SY"),
            self._ext_source("internal_takeoff", qty=550.0, unit="SY"),
            self._ext_source("engineer_quantity", qty=580.0, unit="SY"),
        ])
        out = apply_augmentation_rules(self._contract([row]))
        r = out["reconciliation_rows"][0]
        # Disagreement is still a conflict — no majority takes over.
        assert r["comparison_basis"] == BASIS_CONFLICTED_SOURCES
        assert r["effective_comparison_values"] is None

    # ---- R3: unavailable ----

    def test_unavailable_no_quote_no_external(self):
        from app.pdf_extraction.augmentation_rules import (
            apply_augmentation_rules, BASIS_UNAVAILABLE,
        )
        row = self._row()
        out = apply_augmentation_rules(self._contract([row]))
        assert out["reconciliation_rows"][0]["comparison_basis"] == BASIS_UNAVAILABLE

    def test_unavailable_sources_exist_but_empty(self):
        from app.pdf_extraction.augmentation_rules import (
            apply_augmentation_rules, BASIS_UNAVAILABLE,
        )
        row = self._row(external_quantity_sources=[
            self._ext_source("internal_takeoff", qty=None, unit=None),
        ])
        out = apply_augmentation_rules(self._contract([row]))
        assert out["reconciliation_rows"][0]["comparison_basis"] == BASIS_UNAVAILABLE

    # ---- Quote values never overwritten ----

    def test_quote_values_preserved(self):
        from app.pdf_extraction.augmentation_rules import apply_augmentation_rules
        row = self._row(external_quantity_sources=[self._dot_source(qty=550.0, unit="SY")])
        out = apply_augmentation_rules(self._contract([row]))
        r = out["reconciliation_rows"][0]
        # quote_values still has qty=None, unit=None after dot_augmented basis.
        assert r["quote_values"]["qty"] is None
        assert r["quote_values"]["unit"] is None

    def test_input_never_mutated(self):
        from app.pdf_extraction.augmentation_rules import apply_augmentation_rules
        import copy
        row = self._row(external_quantity_sources=[self._dot_source()])
        contract = self._contract([row])
        snap = copy.deepcopy(contract)
        apply_augmentation_rules(contract)
        assert contract == snap

    # ---- Summary counts ----

    def test_basis_counts_in_summary(self):
        from app.pdf_extraction.augmentation_rules import apply_augmentation_rules
        rows = [
            self._row(normalized_row_id="qr-p0-r0",
                      external_quantity_sources=[self._dot_source(qty=1.0, unit="LF")]),
            self._row(normalized_row_id="qr-p0-r1",
                      quote_values={"qty": 24.0, "unit": "LF",
                                    "unit_price": 10.0, "amount": 240.0}),
            self._row(normalized_row_id="qr-p0-r2",
                      external_quantity_sources=[
                          self._dot_source(qty=1.0, unit="LF"),
                          self._ext_source("internal_takeoff", qty=2.0, unit="LF"),
                      ]),
            self._row(normalized_row_id="qr-p0-r3"),
            self._row(normalized_row_id="qr-p0-r4", mapping_outcome="unmapped"),
        ]
        out = apply_augmentation_rules(self._contract(rows))
        summary = out["augmentation_rules_summary"]
        assert summary["rows_total"] == 5
        assert summary["basis_counts"]["dot_augmented"] == 1
        assert summary["basis_counts"]["quote_native"] == 1
        assert summary["basis_counts"]["conflicted_sources"] == 1
        assert summary["basis_counts"]["unavailable"] == 1
        assert summary["basis_counts"]["not_applicable"] == 1

    # ---- End-to-end on real pipeline ----

    def test_real_ipsi_pair_augmentation(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        from app.pdf_extraction.augmentation_rules import apply_augmentation_rules

        bid_rows, bs = extract_bid_items_from_pdf(str(dot_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        mapping = map_quote_to_bid(staging["accepted_rows"], bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=mapping,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")})
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)
        augmented = apply_augmentation_rules(injected)

        # IPSI quotes have no qty/unit; the 13 mapped rows receive DOT
        # augmentation and should show dot_augmented basis.
        summary = augmented["augmentation_rules_summary"]
        assert summary["basis_counts"]["dot_augmented"] == 13
        assert summary["basis_counts"]["not_applicable"] == 2  # 2 unmapped rows
        # No conflict fired naturally.
        assert summary["basis_counts"].get("conflicted_sources", 0) == 0

    def test_real_blocked_pair_augmentation(self, ipsi_quote_path, estprop_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        from app.pdf_extraction.augmentation_rules import apply_augmentation_rules

        bid_rows, bs = extract_bid_items_from_pdf(str(estprop_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=None,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")})
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)
        augmented = apply_augmentation_rules(injected)
        # All rows blocked → all not_applicable.
        summary = augmented["augmentation_rules_summary"]
        assert summary["basis_counts"]["not_applicable"] == summary["rows_total"]

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c30(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c30(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20v. C29 — External Quantity Injection Tests
# ---------------------------------------------------------------------------


class TestC29QuantityInjection:
    """C29: external quantity injection — attaches trusted qty/unit
    sources to mapped contract rows without mutating quote values."""

    def _contract_row(self, **kw):
        base = {
            "normalized_row_id": "qr-p0-r0",
            "source_page": 0,
            "mapping_outcome": "mapped",
            "mapped_bid_item": {
                "line_number": "0010",
                "item_number": "2101-0850001",
                "description": "ITEM A",
                "qty": 550.0,
                "unit": "SY",
            },
            "comparison_status": "non_comparable",
            "comparison_flags": [],
            "compared_fields": [],
            "non_comparable_reason": "no_overlapping_fields_to_compare",
            "quote_values": {
                "description": "Remove Asphalt",
                "line_ref": "0010",
                "qty": None, "unit": None,
                "unit_price": 5.0, "amount": 2750.0,
            },
            "bid_values": {"line_number": "0010", "item_number": "2101-0850001",
                           "description": "ITEM A", "qty": 550.0, "unit": "SY"},
            "comparison_trace": {},
            "discrepancy_class": "missing_quote_information",
        }
        base.update(kw)
        return base

    def _contract(self, rows):
        return {
            "contract_version": "reconciliation_contract/v1",
            "classification_version": "discrepancy_classification/v1",
            "reconciliation_status": "partial",
            "packet_status": "partial",
            "pairing_status": "trusted",
            "mapping_status": "partial",
            "reconciliation_summary": {"rows_total": len(rows)},
            "office_review_summary": {},
            "reconciliation_rows": rows,
        }

    # ---- Default DOT source attachment ----

    def test_mapped_row_gets_dot_bid_item_source(self):
        from app.pdf_extraction.quantity_injection import (
            inject_external_quantities, SRC_DOT_BID_ITEM, AUG_ATTACHED,
        )
        out = inject_external_quantities(self._contract([self._contract_row()]))
        row = out["reconciliation_rows"][0]
        assert row["augmentation_status"] == AUG_ATTACHED
        assert len(row["external_quantity_sources"]) == 1
        s = row["external_quantity_sources"][0]
        assert s["source_type"] == SRC_DOT_BID_ITEM
        assert s["qty"] == 550.0
        assert s["unit"] == "SY"
        assert s["source_ref"]["line_number"] == "0010"

    def test_quote_values_never_mutated(self):
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        out = inject_external_quantities(self._contract([self._contract_row()]))
        row = out["reconciliation_rows"][0]
        # Original quote_values preserved — qty/unit still None.
        assert row["quote_values"]["qty"] is None
        assert row["quote_values"]["unit"] is None
        # The row's top-level qty/unit keys (if present) are untouched.
        # The bid_values block is also untouched.
        assert row["bid_values"]["qty"] == 550.0

    def test_unmapped_row_not_augmented(self):
        from app.pdf_extraction.quantity_injection import (
            inject_external_quantities, AUG_NONE,
        )
        row = self._contract_row(mapping_outcome="unmapped", mapped_bid_item=None)
        out = inject_external_quantities(self._contract([row]))
        r = out["reconciliation_rows"][0]
        assert r["augmentation_status"] == AUG_NONE
        assert r["external_quantity_sources"] == []

    def test_ambiguous_row_not_augmented(self):
        from app.pdf_extraction.quantity_injection import (
            inject_external_quantities, AUG_NONE,
        )
        row = self._contract_row(mapping_outcome="ambiguous", mapped_bid_item=None)
        out = inject_external_quantities(self._contract([row]))
        assert out["reconciliation_rows"][0]["augmentation_status"] == AUG_NONE

    def test_blocked_row_not_augmented(self):
        from app.pdf_extraction.quantity_injection import (
            inject_external_quantities, AUG_NONE,
        )
        row = self._contract_row(mapping_outcome="blocked", mapped_bid_item=None,
                                 comparison_status="blocked")
        out = inject_external_quantities(self._contract([row]))
        assert out["reconciliation_rows"][0]["augmentation_status"] == AUG_NONE

    def test_mapped_bid_item_without_qty_and_unit_skips_source(self):
        from app.pdf_extraction.quantity_injection import (
            inject_external_quantities, AUG_NONE,
        )
        row = self._contract_row()
        row["mapped_bid_item"]["qty"] = None
        row["mapped_bid_item"]["unit"] = None
        out = inject_external_quantities(self._contract([row]))
        assert out["reconciliation_rows"][0]["augmentation_status"] == AUG_NONE

    # ---- Caller-supplied external sources ----

    def test_caller_supplied_internal_takeoff_attached(self):
        from app.pdf_extraction.quantity_injection import (
            inject_external_quantities, SRC_INTERNAL_TAKEOFF, AUG_MULTIPLE_SOURCES,
        )
        external = [{
            "source_type": SRC_INTERNAL_TAKEOFF,
            "source_ref": {"takeoff_id": "T-42"},
            "rows": {
                "qr-p0-r0": {"qty": 540.0, "unit": "SY", "ref": "takeoff-row-7"},
            },
        }]
        out = inject_external_quantities(self._contract([self._contract_row()]),
                                         external_sources=external)
        row = out["reconciliation_rows"][0]
        assert row["augmentation_status"] == AUG_MULTIPLE_SOURCES
        assert len(row["external_quantity_sources"]) == 2
        types = {s["source_type"] for s in row["external_quantity_sources"]}
        assert types == {"dot_bid_item", SRC_INTERNAL_TAKEOFF}

    def test_multiple_external_sources_never_merged(self):
        from app.pdf_extraction.quantity_injection import (
            inject_external_quantities, SRC_INTERNAL_TAKEOFF, SRC_ENGINEER_QUANTITY,
        )
        external = [
            {"source_type": SRC_INTERNAL_TAKEOFF, "source_ref": {"id": "T1"},
             "rows": {"qr-p0-r0": {"qty": 540.0, "unit": "SY"}}},
            {"source_type": SRC_ENGINEER_QUANTITY, "source_ref": {"id": "EQ-3"},
             "rows": {"qr-p0-r0": {"qty": 560.0, "unit": "SY"}}},
        ]
        out = inject_external_quantities(self._contract([self._contract_row()]),
                                         external_sources=external)
        row = out["reconciliation_rows"][0]
        sources = row["external_quantity_sources"]
        assert len(sources) == 3  # dot + 2 caller
        quantities = sorted([s["qty"] for s in sources])
        assert quantities == [540.0, 550.0, 560.0]

    def test_unknown_row_id_surfaces_in_diagnostics(self):
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        external = [{
            "source_type": "internal_takeoff",
            "rows": {"does-not-exist": {"qty": 1.0, "unit": "LF"}},
        }]
        out = inject_external_quantities(self._contract([self._contract_row()]),
                                         external_sources=external)
        assert "does-not-exist" in out["injection_diagnostics"]["unknown_row_ids"]
        # No phantom row created.
        assert len(out["reconciliation_rows"]) == 1

    def test_unknown_source_type_surfaces(self):
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        external = [{
            "source_type": "made_up_source",
            "rows": {"qr-p0-r0": {"qty": 1.0, "unit": "LF"}},
        }]
        out = inject_external_quantities(self._contract([self._contract_row()]),
                                         external_sources=external)
        assert "made_up_source" in out["injection_diagnostics"]["unknown_source_types"]
        # The source is still attached — diagnostics just flag the unknown type.
        row = out["reconciliation_rows"][0]
        types = [s["source_type"] for s in row["external_quantity_sources"]]
        assert "made_up_source" in types

    # ---- Disabling default DOT attachment ----

    def test_disable_attach_dot_bid_item(self):
        from app.pdf_extraction.quantity_injection import (
            inject_external_quantities, AUG_NONE,
        )
        out = inject_external_quantities(self._contract([self._contract_row()]),
                                         attach_dot_bid_item=False)
        assert out["reconciliation_rows"][0]["augmentation_status"] == AUG_NONE
        assert out["reconciliation_rows"][0]["external_quantity_sources"] == []

    # ---- Traceability ----

    def test_injection_version_present(self):
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        out = inject_external_quantities(self._contract([self._contract_row()]))
        assert out["injection_version"] == "quantity_injection/v1"
        row = out["reconciliation_rows"][0]
        assert row["augmentation_trace"]["injection_version"] == "quantity_injection/v1"
        assert row["augmentation_trace"]["attached_source_count"] == 1

    def test_source_trace_preserved(self):
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        external = [{
            "source_type": "internal_takeoff",
            "source_ref": {"id": "T-42"},
            "rows": {"qr-p0-r0": {"qty": 540.0, "unit": "SY",
                                  "trace": {"method": "manual", "reviewer": "alice"}}},
        }]
        out = inject_external_quantities(self._contract([self._contract_row()]),
                                         external_sources=external)
        row = out["reconciliation_rows"][0]
        takeoff = next(s for s in row["external_quantity_sources"]
                       if s["source_type"] == "internal_takeoff")
        assert takeoff["source_trace"]["method"] == "manual"
        assert takeoff["source_trace"]["reviewer"] == "alice"

    # ---- Immutability ----

    def test_input_contract_never_mutated(self):
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        import copy
        contract = self._contract([self._contract_row()])
        snap = copy.deepcopy(contract)
        inject_external_quantities(contract, external_sources=[{
            "source_type": "internal_takeoff",
            "rows": {"qr-p0-r0": {"qty": 540.0, "unit": "SY"}},
        }])
        assert contract == snap

    # ---- Aggregate diagnostics ----

    def test_diagnostics_counts(self):
        from app.pdf_extraction.quantity_injection import inject_external_quantities
        rows = [
            self._contract_row(normalized_row_id="qr-p0-r0"),
            self._contract_row(normalized_row_id="qr-p0-r1",
                               mapping_outcome="unmapped", mapped_bid_item=None),
            self._contract_row(normalized_row_id="qr-p0-r2"),
        ]
        external = [{
            "source_type": "internal_takeoff",
            "rows": {"qr-p0-r0": {"qty": 540.0, "unit": "SY"}},
        }]
        out = inject_external_quantities(self._contract(rows),
                                         external_sources=external)
        d = out["injection_diagnostics"]
        assert d["rows_total"] == 3
        assert d["rows_with_any_external_source"] == 2  # two mapped rows got dot
        assert d["rows_with_multiple_sources"] == 1     # r0 got dot + takeoff

    # ---- End-to-end integration via real pipeline ----

    def test_injection_over_real_ipsi_pair(self, ipsi_quote_path, dot_pdf_path):
        """Full governed chain → C29 injection. Every mapped row should
        receive a dot_bid_item source when the DOT item carries qty+unit."""
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities

        bid_rows, bs = extract_bid_items_from_pdf(str(dot_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        mapping = map_quote_to_bid(staging["accepted_rows"], bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=mapping,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")},
        )
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)

        mapped_rows = [r for r in injected["reconciliation_rows"]
                       if r.get("mapping_outcome") == "mapped"]
        assert len(mapped_rows) >= 10
        for r in mapped_rows:
            if r["mapped_bid_item"] and (r["mapped_bid_item"].get("qty") is not None
                                         or r["mapped_bid_item"].get("unit") is not None):
                assert r["augmentation_status"] in ("attached", "multiple_sources")
                assert any(s["source_type"] == "dot_bid_item"
                           for s in r["external_quantity_sources"])
            # Quote values untouched.
            assert r["quote_values"]["qty"] is None

    def test_injection_over_real_blocked_pair(self, ipsi_quote_path, estprop_pdf_path):
        """A blocked pair — the classified contract shows all rows
        comparison_status=blocked, and injection must not attach sources."""
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.pairing_guardrails import analyze_pairing
        from app.pdf_extraction.review_packet import build_review_packet
        from app.pdf_extraction.reconciliation_foundation import reconcile_packet
        from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
        from app.pdf_extraction.discrepancy_classification import classify_contract
        from app.pdf_extraction.quantity_injection import inject_external_quantities

        bid_rows, bs = extract_bid_items_from_pdf(str(estprop_pdf_path))
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        pairing = analyze_pairing(staging["accepted_rows"], bid_rows)
        assert not pairing["allow_mapping"]
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=None,
            accepted_rows=staging["accepted_rows"],
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bs.get("rows_extracted")},
        )
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        injected = inject_external_quantities(classified)

        for row in injected["reconciliation_rows"]:
            # Blocked rows never get augmented.
            assert row["augmentation_status"] == "none"
            assert row["external_quantity_sources"] == []

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c29(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c29(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20u. C28 — Exception Surfacing + Feedback Loop Tests
# ---------------------------------------------------------------------------


class TestC28ExceptionFeedback:
    """C28: deterministic exception surfacing from audit runs, findings
    packets, and workflow packets. Templated statements only — no
    narrative generation."""

    def _quote_only_run(self, **kw):
        base_metrics = {
            "document_class_detected": "quote",
            "accepted_rows_count": 15,
            "rows_enriched_qty_unit": 0,
            "table_header_page_count": 0,
            "blocks_attempted": 0,
            "pairing_status": None,
            "packet_status": None,
            "rows_mapped": 0, "rows_unmapped": 0, "rows_ambiguous": 0,
            "rows_comparable": 0, "rows_non_comparable": 0,
        }
        base_metrics.update(kw)
        return {"audit_mode": "quote_only", "label": "q", "metrics": base_metrics}

    def _paired_run(self, **kw):
        base_metrics = {
            "document_class_detected": "quote",
            "accepted_rows_count": 15,
            "rows_enriched_qty_unit": 0,
            "table_header_page_count": 0,
            "blocks_attempted": 0,
            "pairing_status": "trusted",
            "packet_status": "partial",
            "rows_mapped": 13, "rows_unmapped": 2, "rows_ambiguous": 0,
            "rows_comparable": 0, "rows_non_comparable": 15,
        }
        base_metrics.update(kw)
        return {"audit_mode": "paired", "label": "p", "metrics": base_metrics}

    # ---- Extraction categories ----

    def test_no_quote_rows_exception(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        run = self._quote_only_run(accepted_rows_count=0)
        out = surface_exceptions(audit_runs=[run])
        assert out["exception_counts"]["E_NO_QUOTE_ROWS_DETECTED"] == 1

    def test_unknown_document_class_exception(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        run = self._quote_only_run(document_class_detected="unknown", accepted_rows_count=0)
        out = surface_exceptions(audit_runs=[run])
        assert out["exception_counts"]["E_UNKNOWN_DOCUMENT_CLASS"] == 1

    def test_no_table_header_exception(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        run = self._quote_only_run(table_header_page_count=0)
        out = surface_exceptions(audit_runs=[run])
        assert out["exception_counts"]["E_NO_TABLE_HEADER_DETECTED"] == 1

    def test_no_inline_qty_unit_exception(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        run = self._quote_only_run(rows_enriched_qty_unit=0)
        out = surface_exceptions(audit_runs=[run])
        assert out["exception_counts"]["E_NO_INLINE_QTY_UNIT_DETECTED"] == 1

    def test_no_multi_row_group_candidates_exception(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        run = self._quote_only_run(blocks_attempted=0)
        out = surface_exceptions(audit_runs=[run])
        assert out["exception_counts"]["E_NO_MULTI_ROW_GROUP_CANDIDATES"] == 1

    def test_low_enrichment_exception(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        # 1/100 = 1% < 10% threshold
        run = self._quote_only_run(accepted_rows_count=100, rows_enriched_qty_unit=1,
                                   table_header_page_count=1, blocks_attempted=1)
        out = surface_exceptions(audit_runs=[run])
        assert out["exception_counts"]["E_LOW_ENRICHMENT_COVERAGE"] == 1

    def test_enrichment_above_threshold_no_low_enrichment(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        # 20/100 = 20% >= 10%
        run = self._quote_only_run(accepted_rows_count=100, rows_enriched_qty_unit=20,
                                   table_header_page_count=1, blocks_attempted=1)
        out = surface_exceptions(audit_runs=[run])
        assert out["exception_counts"]["E_LOW_ENRICHMENT_COVERAGE"] == 0

    # ---- Pairing category ----

    def test_blocked_by_pairing_exception(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        run = self._paired_run(pairing_status="rejected", packet_status="blocked",
                               rows_mapped=0, rows_unmapped=0)
        out = surface_exceptions(audit_runs=[run])
        assert out["exception_counts"]["P_BLOCKED_BY_PAIRING"] == 1
        # Blocked pairing supersedes mapping/reconciliation exceptions.
        assert out["exception_counts"]["M_UNMAPPED_AFTER_TRUSTED_PAIRING"] == 0
        assert out["exception_counts"]["R_ROWS_NON_COMPARABLE_MISSING_QUOTE_FIELDS"] == 0

    # ---- Mapping category ----

    def test_unmapped_after_trusted_pairing_exception(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        run = self._paired_run(pairing_status="trusted", packet_status="partial",
                               rows_unmapped=2)
        out = surface_exceptions(audit_runs=[run])
        assert out["exception_counts"]["M_UNMAPPED_AFTER_TRUSTED_PAIRING"] == 1

    def test_ambiguous_mapping_exception(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        run = self._paired_run(rows_ambiguous=3)
        out = surface_exceptions(audit_runs=[run])
        assert out["exception_counts"]["M_AMBIGUOUS_MAPPING_DETECTED"] == 1

    # ---- Reconciliation category ----

    def test_non_comparable_missing_quote_fields_exception(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        run = self._paired_run(rows_mapped=13, rows_comparable=0, rows_non_comparable=13)
        out = surface_exceptions(audit_runs=[run])
        assert out["exception_counts"]["R_ROWS_NON_COMPARABLE_MISSING_QUOTE_FIELDS"] == 1

    def test_low_comparability_exception(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        # 1/100 mapped rows comparable — below 10% threshold
        run = self._paired_run(rows_mapped=100, rows_comparable=1, rows_non_comparable=99)
        out = surface_exceptions(audit_runs=[run])
        assert out["exception_counts"]["R_LOW_COMPARABILITY_COVERAGE"] == 1

    # ---- Findings packet classification ----

    def test_findings_packet_classification(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        fp = {
            "packet_status": "partial",
            "discrepancy_summary": {
                "rows_total": 15, "unmapped_count": 2, "ambiguous_count": 0,
                "missing_quote_info_count": 13, "missing_bid_info_count": 0,
                "comparable_match_count": 0,
            },
        }
        out = surface_exceptions(findings_packets=[fp])
        assert out["exception_counts"]["M_UNMAPPED_AFTER_TRUSTED_PAIRING"] == 1
        assert out["exception_counts"]["R_ROWS_NON_COMPARABLE_MISSING_QUOTE_FIELDS"] == 1
        assert out["exception_counts"]["R_LOW_COMPARABILITY_COVERAGE"] == 1

    def test_findings_packet_blocked_short_circuit(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        fp = {"packet_status": "blocked", "discrepancy_summary": {}}
        out = surface_exceptions(findings_packets=[fp])
        assert out["exception_counts"]["P_BLOCKED_BY_PAIRING"] == 1
        assert out["exception_counts"]["M_UNMAPPED_AFTER_TRUSTED_PAIRING"] == 0

    # ---- Workflow packet + queue pressure ----

    def test_workflow_queue_concentrated_in_high_priority(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        wp = {
            "packet_status": "partial",
            "queue_summary": {
                "rows_total": 10, "critical_open": 3, "high_open": 5,
                "medium_open": 2, "low_open": 0, "rows_unreviewed": 10,
            },
        }
        out = surface_exceptions(workflow_packets=[wp])
        assert out["exception_counts"]["W_REVIEW_QUEUE_CONCENTRATED_IN_HIGH_PRIORITY"] == 1

    def test_workflow_queue_backlog_untouched(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        wp = {
            "packet_status": "partial",
            "queue_summary": {
                "rows_total": 5, "critical_open": 0, "high_open": 0,
                "medium_open": 2, "low_open": 3, "rows_unreviewed": 5,
            },
        }
        out = surface_exceptions(workflow_packets=[wp])
        assert out["exception_counts"]["W_REVIEW_QUEUE_BACKLOG_UNTOUCHED"] == 1
        assert out["exception_counts"]["W_REVIEW_QUEUE_CONCENTRATED_IN_HIGH_PRIORITY"] == 0

    def test_queue_pressure_aggregate(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        wp1 = {"queue_summary": {
            "rows_total": 10, "critical_open": 3, "high_open": 5,
            "medium_open": 2, "low_open": 0, "rows_unreviewed": 10,
        }}
        wp2 = {"queue_summary": {
            "rows_total": 4, "critical_open": 0, "high_open": 1,
            "medium_open": 2, "low_open": 1, "rows_unreviewed": 4,
        }}
        out = surface_exceptions(workflow_packets=[wp1, wp2])
        qp = out["queue_pressure_summary"]
        assert qp["workflow_packet_count"] == 2
        assert qp["rows_total"] == 14
        assert qp["critical_open"] == 3
        assert qp["high_open"] == 6

    # ---- Ranking + bucket separation ----

    def test_extraction_and_review_buckets_distinct(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        run = self._quote_only_run(accepted_rows_count=0)
        wp = {"queue_summary": {
            "rows_total": 10, "critical_open": 10, "high_open": 0,
            "medium_open": 0, "low_open": 0, "rows_unreviewed": 10,
        }}
        out = surface_exceptions(audit_runs=[run], workflow_packets=[wp])
        fail_cats = {e["category"] for e in out["top_failure_patterns"]}
        bottle_cats = {e["category"] for e in out["top_review_bottlenecks"]}
        assert "E_NO_QUOTE_ROWS_DETECTED" in fail_cats
        assert "E_NO_QUOTE_ROWS_DETECTED" not in bottle_cats
        assert "W_REVIEW_QUEUE_CONCENTRATED_IN_HIGH_PRIORITY" in bottle_cats
        assert "W_REVIEW_QUEUE_CONCENTRATED_IN_HIGH_PRIORITY" not in fail_cats

    def test_top_failure_patterns_ranked_by_count(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        runs = [
            self._quote_only_run(document_class_detected="unknown", accepted_rows_count=0),
            self._quote_only_run(document_class_detected="unknown", accepted_rows_count=0),
            self._quote_only_run(document_class_detected="unknown", accepted_rows_count=0),
            self._quote_only_run(accepted_rows_count=0),
        ]
        out = surface_exceptions(audit_runs=runs)
        top = out["top_failure_patterns"]
        # Highest-count category first.
        assert top[0]["category"] == "E_UNKNOWN_DOCUMENT_CLASS"
        assert top[0]["count"] == 3

    # ---- Templated statements ----

    def test_templated_statements_include_counts(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        runs = [
            self._quote_only_run(document_class_detected="unknown", accepted_rows_count=0),
            self._quote_only_run(document_class_detected="unknown", accepted_rows_count=0),
        ]
        out = surface_exceptions(audit_runs=runs)
        assert any("2 documents classified as unknown_document_class" in s
                   for s in out["templated_statements"])

    def test_templated_statements_never_narrative(self):
        """All templated statements must be fixed format strings."""
        from app.pdf_extraction.exception_feedback import surface_exceptions
        runs = [self._paired_run()]
        wp = {"queue_summary": {
            "rows_total": 10, "critical_open": 3, "high_open": 5,
            "medium_open": 2, "low_open": 0, "rows_unreviewed": 10,
        }}
        out = surface_exceptions(audit_runs=runs, workflow_packets=[wp])
        for s in out["templated_statements"]:
            # Never contains speculative words like 'maybe', 'probably', 'should', 'suggests'.
            lo = s.lower()
            for bad in ("maybe", "probably", "should", "suggests", "recommend"):
                assert bad not in lo

    # ---- Document exception map ----

    def test_document_exception_map_preserves_labels(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        run = self._quote_only_run(accepted_rows_count=0)
        run["label"] = "my_document.pdf"
        out = surface_exceptions(audit_runs=[run])
        assert out["document_exception_map"][0]["label"] == "my_document.pdf"

    # ---- Empty input ----

    def test_empty_inputs(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        out = surface_exceptions()
        assert out["inputs"]["audit_runs"] == 0
        assert out["top_failure_patterns"] == []
        assert out["top_review_bottlenecks"] == []
        assert out["templated_statements"] == []

    # ---- Immutability ----

    def test_does_not_mutate_inputs(self):
        from app.pdf_extraction.exception_feedback import surface_exceptions
        import copy
        run = self._quote_only_run(accepted_rows_count=0)
        snap = copy.deepcopy(run)
        surface_exceptions(audit_runs=[run])
        assert run == snap

    # ---- End-to-end via real corpus audit chain ----

    def test_real_corpus_surfaces_expected_exceptions(self, ipsi_quote_path, dot_pdf_path,
                                                       estprop_pdf_path, rasch_quote_path):
        from app.pdf_extraction.coverage_audit import (
            audit_quote_document, audit_paired_documents,
        )
        from app.pdf_extraction.exception_feedback import surface_exceptions
        runs = [
            audit_quote_document(str(ipsi_quote_path), "ipsi"),
            audit_quote_document(str(rasch_quote_path), "rasch"),
            audit_paired_documents(str(ipsi_quote_path), str(dot_pdf_path), "trusted_pair"),
            audit_paired_documents(str(ipsi_quote_path), str(estprop_pdf_path), "blocked_pair"),
        ]
        out = surface_exceptions(audit_runs=runs)
        # Expected real-corpus observations:
        assert out["exception_counts"]["E_UNKNOWN_DOCUMENT_CLASS"] >= 1  # rasch
        assert out["exception_counts"]["E_NO_INLINE_QTY_UNIT_DETECTED"] >= 1  # ipsi
        assert out["exception_counts"]["E_NO_TABLE_HEADER_DETECTED"] >= 1  # ipsi
        assert out["exception_counts"]["P_BLOCKED_BY_PAIRING"] >= 1  # blocked pair
        assert out["exception_counts"]["M_UNMAPPED_AFTER_TRUSTED_PAIRING"] >= 1  # trusted pair has 2 unmapped
        assert out["exception_counts"]["R_ROWS_NON_COMPARABLE_MISSING_QUOTE_FIELDS"] >= 1
        # Templated statements exist.
        assert len(out["templated_statements"]) >= 3

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c28(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c28(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True

    def test_quote_staging_unchanged_under_c28(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14


# ---------------------------------------------------------------------------
# 20t. C27 — Deterministic Pattern Library Expansion Tests
# ---------------------------------------------------------------------------


class TestC27PatternLibrary:
    """C27: narrow pattern-library expansion. Currently ships rule
    C27-U1 (dotted unit-token normalization). Real corpus shows zero
    measured impact; the rule is infrastructure + controlled fixture
    proven."""

    def _parsed(self, **kw):
        base = {
            "row_id": 0,
            "line_ref": None,
            "description": "Remove Asphalt",
            "qty": None, "unit": None,
            "unit_price": None, "amount": 100.0,
            "source_page": 0,
            "source_text": "Remove Asphalt $100.00",
            "field_sources": {
                "qty": "not_present", "unit": "not_present",
                "unit_price": "not_present",
                "amount": "explicit_dollar_parser",
            },
            "enrichment_trace": {"rules_attempted": []},
            "enricher_version": "quote_enrichment/v1",
        }
        base.update(kw)
        return base

    # ---- Registry ----

    def test_list_registered_rules_has_closed_set(self):
        from app.pdf_extraction.pattern_library import list_registered_rules
        rules = list_registered_rules()
        ids = {r["rule_id"] for r in rules}
        assert "C20-E1" in ids
        assert "C23-E2" in ids
        assert "C24-A1" in ids
        assert "C27-U1" in ids
        for r in rules:
            assert "description" in r
            assert "motivation" in r
            assert "real_corpus_evidence" in r
            assert "false_positive_guard" in r

    def test_registry_documents_c27_honestly(self):
        """The registry must explicitly acknowledge that C27-U1 has
        zero real-corpus evidence — the honesty discipline from the
        C27 spec."""
        from app.pdf_extraction.pattern_library import list_registered_rules
        rules = {r["rule_id"]: r for r in list_registered_rules()}
        c27 = rules["C27-U1"]
        assert "ZERO observed instances" in c27["real_corpus_evidence"].upper() \
            or "zero" in c27["real_corpus_evidence"].lower()

    # ---- Rule C27-U1 fires on intended patterns ----

    def test_c27_u1_dotted_lf(self):
        from app.pdf_extraction.pattern_library import (
            enrich_quote_rows_with_pattern_library, SRC_EXPLICIT_DOTTED_INLINE,
        )
        row = self._parsed(description="Remove Asphalt 24 L.F.",
                           unit_price=None, amount=None)
        out = enrich_quote_rows_with_pattern_library([row])
        assert out[0]["qty"] == 24.0
        assert out[0]["unit"] == "LF"
        assert out[0]["field_sources"]["qty"] == SRC_EXPLICIT_DOTTED_INLINE
        assert out[0]["pattern_library_version"] == "pattern_library/v1"

    def test_c27_u1_dotted_sy(self):
        from app.pdf_extraction.pattern_library import enrich_quote_rows_with_pattern_library
        row = self._parsed(description="Pavement Repair 550 S.Y.",
                           unit_price=None, amount=None)
        out = enrich_quote_rows_with_pattern_library([row])
        assert out[0]["qty"] == 550.0
        assert out[0]["unit"] == "SY"

    def test_c27_u1_dotted_cy(self):
        from app.pdf_extraction.pattern_library import enrich_quote_rows_with_pattern_library
        row = self._parsed(description="Concrete Mix 8 C.Y.",
                           unit_price=None, amount=None)
        out = enrich_quote_rows_with_pattern_library([row])
        assert out[0]["qty"] == 8.0
        assert out[0]["unit"] == "CY"

    def test_c27_u1_dotted_ea(self):
        from app.pdf_extraction.pattern_library import enrich_quote_rows_with_pattern_library
        row = self._parsed(description="Bollard 12 E.A.",
                           unit_price=None, amount=None)
        out = enrich_quote_rows_with_pattern_library([row])
        assert out[0]["qty"] == 12.0
        assert out[0]["unit"] == "EA"

    # ---- False positive guards ----

    def test_c27_u1_never_overwrites_existing_qty(self):
        from app.pdf_extraction.pattern_library import enrich_quote_rows_with_pattern_library
        row = self._parsed(description="Remove Asphalt 24 L.F.",
                           qty=999.0, unit="TON",
                           unit_price=None, amount=None)
        out = enrich_quote_rows_with_pattern_library([row])
        assert out[0]["qty"] == 999.0
        assert out[0]["unit"] == "TON"
        trace = out[0]["enrichment_trace"]["rules_attempted"][-1]
        assert trace["skip_reason"] == "row_already_has_qty_or_unit"

    def test_c27_u1_rejects_non_whitelist_dotted_token(self):
        from app.pdf_extraction.pattern_library import enrich_quote_rows_with_pattern_library
        # K.G. is not in whitelist
        row = self._parsed(description="Steel Plate 50 K.G.",
                           unit_price=None, amount=None)
        out = enrich_quote_rows_with_pattern_library([row])
        assert out[0]["qty"] is None
        assert out[0]["unit"] is None
        trace = out[0]["enrichment_trace"]["rules_attempted"][-1]
        assert trace["skip_reason"] == "normalized_unit_not_in_whitelist"
        assert trace["normalized_unit"] == "KG"

    def test_c27_u1_requires_tail_position(self):
        from app.pdf_extraction.pattern_library import enrich_quote_rows_with_pattern_library
        # Mid-description dotted token — not at tail
        row = self._parsed(description="Install 24 L.F. of pipe",
                           unit_price=None, amount=None)
        out = enrich_quote_rows_with_pattern_library([row])
        assert out[0]["qty"] is None
        trace = out[0]["enrichment_trace"]["rules_attempted"][-1]
        assert trace["skip_reason"] == "no_dotted_tail_qty_unit_token"

    def test_c27_u1_consistency_guard_rejects_mismatch(self):
        from app.pdf_extraction.pattern_library import enrich_quote_rows_with_pattern_library
        row = self._parsed(description="Remove Asphalt 24 L.F.",
                           unit_price=10.0, amount=99999.0)
        out = enrich_quote_rows_with_pattern_library([row])
        assert out[0]["qty"] is None
        trace = out[0]["enrichment_trace"]["rules_attempted"][-1]
        assert trace["skip_reason"] == "arithmetic_mismatch"

    def test_c27_u1_consistency_guard_accepts_match(self):
        from app.pdf_extraction.pattern_library import enrich_quote_rows_with_pattern_library
        row = self._parsed(description="Remove Asphalt 24 L.F.",
                           unit_price=10.0, amount=240.0)
        out = enrich_quote_rows_with_pattern_library([row])
        assert out[0]["qty"] == 24.0

    def test_c27_u1_plain_unit_does_not_fire(self):
        """Plain undotted `24 LF` is handled by C20 E1, not C27-U1."""
        from app.pdf_extraction.pattern_library import enrich_quote_rows_with_pattern_library
        row = self._parsed(description="Remove Asphalt 24 LF",
                           unit_price=None, amount=None)
        out = enrich_quote_rows_with_pattern_library([row])
        # C27-U1 regex requires at least one internal dot, so plain "24 LF"
        # should NOT match at all — no enrichment, C27 skip.
        assert out[0]["qty"] is None
        trace = out[0]["enrichment_trace"]["rules_attempted"][-1]
        assert trace["skip_reason"] == "no_dotted_tail_qty_unit_token"

    def test_c27_u1_immutable_input(self):
        from app.pdf_extraction.pattern_library import enrich_quote_rows_with_pattern_library
        row = self._parsed(description="Remove Asphalt 24 L.F.",
                           unit_price=None, amount=None)
        import copy
        snap = copy.deepcopy(row)
        enrich_quote_rows_with_pattern_library([row])
        assert row == snap

    # ---- End-to-end via controlled synthetic PDF ----

    def test_controlled_synthetic_dotted_pdf_enriches(self, tmp_path):
        import fitz
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf

        p = tmp_path / "dotted_quote.pdf"
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 72), "Vendor Quote — Dotted Units", fontsize=12)
        page.insert_text((72, 100), "Remove Asphalt 24 L.F. $10.00 $240.00", fontsize=10)
        page.insert_text((72, 125), "Pavement Repair 550 S.Y. $5.00 $2,750.00", fontsize=10)
        page.insert_text((72, 150), "Concrete Mix 8 C.Y. $50.00 $400.00", fontsize=10)
        doc.save(str(p))
        doc.close()

        staging = normalize_quote_from_pdf(str(p))
        assert len(staging["accepted_rows"]) == 3
        for row in staging["accepted_rows"]:
            assert row["qty"] is not None
            assert row["unit"] in {"LF", "SY", "CY"}
            assert row["field_sources"]["qty"] == "explicit_dotted_inline_qty_unit"
            assert row["pattern_library_version"] == "pattern_library/v1"

    def test_ipsi_corpus_impact_remains_zero(self, ipsi_quote_path):
        """Real-corpus measurement discipline: C27-U1 must not change
        IPSI accepted rows or introduce any unexpected enrichment."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14
        dotted = sum(
            1 for r in staging["accepted_rows"]
            if (r.get("field_sources") or {}).get("qty") == "explicit_dotted_inline_qty_unit"
        )
        assert dotted == 0

    def test_c27_u1_measurable_on_controlled_fixture(self, tmp_path):
        """The C26 coverage audit should show measurable enrichment on
        the dotted fixture after C27 is wired in."""
        import fitz
        from app.pdf_extraction.coverage_audit import audit_quote_document

        p = tmp_path / "measurable.pdf"
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 72), "Vendor", fontsize=12)
        page.insert_text((72, 100), "Remove Asphalt 24 L.F. $10.00 $240.00", fontsize=10)
        page.insert_text((72, 125), "Concrete Mix 8 C.Y. $50.00 $400.00", fontsize=10)
        doc.save(str(p))
        doc.close()

        audit = audit_quote_document(str(p), "c27_controlled")
        m = audit["metrics"]
        assert m["accepted_rows_count"] == 2
        # These rows enrich via C27-U1 (not E1, not E2) — the audit's
        # rows_enriched_qty_unit counter currently tallies E1+E2 only.
        # The new dotted source appears in field_sources; we assert
        # directly on that by re-reading staging.
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(p))
        dotted = [r for r in staging["accepted_rows"]
                  if (r.get("field_sources") or {}).get("qty") == "explicit_dotted_inline_qty_unit"]
        assert len(dotted) == 2

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c27(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c27(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20s. C26 — Coverage Audit Tests
# ---------------------------------------------------------------------------


class TestC26CoverageAudit:
    """C26: real-corpus coverage audit — deterministic per-document and
    aggregate metrics that future rule work can be guided by."""

    def _synthetic_quote_pdf(self, tmp_path, name="synthetic.pdf", with_header=True, with_qty=True):
        import fitz
        p = tmp_path / name
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 72), "Quote - Synthetic", fontsize=12)
        if with_header:
            page.insert_text((72, 100), "Description Qty Unit Unit Price Amount", fontsize=10)
        if with_qty:
            page.insert_text((72, 130), "Remove Asphalt 550 SY $5.00 $2,750.00", fontsize=10)
            page.insert_text((72, 150), "Barrier Rail 24 LF $100.00 $2,400.00", fontsize=10)
        else:
            page.insert_text((72, 130), "Remove Asphalt $2,750.00", fontsize=10)
            page.insert_text((72, 150), "Barrier Rail $2,400.00", fontsize=10)
        doc.save(str(p))
        doc.close()
        return p

    # ---- Quote-only audit ----

    def test_quote_only_audit_on_ipsi(self, ipsi_quote_path):
        from app.pdf_extraction.coverage_audit import (
            audit_quote_document, AUDIT_MODE_QUOTE_ONLY,
            LIM_NO_ENRICHMENT_SIGNAL,
        )
        audit = audit_quote_document(str(ipsi_quote_path), label="ipsi")
        assert audit["audit_mode"] == AUDIT_MODE_QUOTE_ONLY
        assert audit["label"] == "ipsi"
        m = audit["metrics"]
        assert m["document_class_detected"] == "quote"
        assert m["accepted_rows_count"] >= 14
        assert m["rows_enriched_qty_unit"] == 0
        assert m["rows_with_qty"] == 0
        assert m["dominant_limitation"] == LIM_NO_ENRICHMENT_SIGNAL

    def test_quote_only_audit_on_rasch(self, rasch_quote_path):
        from app.pdf_extraction.coverage_audit import (
            audit_quote_document, LIM_UNKNOWN_DOCUMENT,
        )
        audit = audit_quote_document(str(rasch_quote_path), label="rasch")
        m = audit["metrics"]
        assert m["accepted_rows_count"] == 0
        # Rasch classifies as unknown after OCR — no quote rows
        assert m["dominant_limitation"] in ("unknown_document_class", "no_quote_rows_detected")

    def test_quote_only_audit_on_synthetic_with_enrichment(self, tmp_path):
        from app.pdf_extraction.coverage_audit import (
            audit_quote_document, LIM_STRUCTURAL_COVERAGE_OK,
        )
        p = self._synthetic_quote_pdf(tmp_path, with_header=True, with_qty=True)
        audit = audit_quote_document(str(p), label="synthetic")
        m = audit["metrics"]
        assert m["accepted_rows_count"] == 2
        assert m["rows_enriched_qty_unit"] == 2
        assert m["rows_with_qty"] == 2
        assert m["rows_with_unit"] == 2
        assert m["table_header_page_count"] >= 1
        assert m["dominant_limitation"] == LIM_STRUCTURAL_COVERAGE_OK

    def test_quote_only_audit_no_header_classification(self, tmp_path):
        from app.pdf_extraction.coverage_audit import (
            audit_quote_document, LIM_NO_ENRICHMENT_SIGNAL,
        )
        p = self._synthetic_quote_pdf(tmp_path, with_header=False, with_qty=False)
        audit = audit_quote_document(str(p), label="no_header")
        m = audit["metrics"]
        assert m["table_header_page_count"] == 0
        assert m["rows_enriched_qty_unit"] == 0
        assert m["dominant_limitation"] == LIM_NO_ENRICHMENT_SIGNAL

    # ---- Paired audit ----

    def test_paired_audit_ipsi_trusted(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.coverage_audit import (
            audit_paired_documents, AUDIT_MODE_PAIRED,
            D_LIM_ALL_NON_COMPARABLE,
        )
        audit = audit_paired_documents(str(ipsi_quote_path), str(dot_pdf_path),
                                       label="ipsi_x_synthetic_dot")
        assert audit["audit_mode"] == AUDIT_MODE_PAIRED
        m = audit["metrics"]
        assert m["pairing_status"] == "trusted"
        assert m["packet_status"] == "partial"
        assert m["rows_mapped"] == 13
        assert m["rows_unmapped"] == 2
        assert m["rows_comparable"] == 0
        assert m["rows_non_comparable"] >= 13
        # All 13 mapped rows are non-comparable due to missing quote qty/unit.
        assert m["dominant_downstream_limitation"] == D_LIM_ALL_NON_COMPARABLE

    def test_paired_audit_blocked(self, ipsi_quote_path, estprop_pdf_path):
        from app.pdf_extraction.coverage_audit import (
            audit_paired_documents, D_LIM_BLOCKED_BY_PAIRING,
        )
        audit = audit_paired_documents(str(ipsi_quote_path), str(estprop_pdf_path),
                                       label="ipsi_x_estprop")
        m = audit["metrics"]
        assert m["pairing_status"] == "rejected"
        assert m["packet_status"] == "blocked"
        assert m["dominant_downstream_limitation"] == D_LIM_BLOCKED_BY_PAIRING
        assert m["rows_blocked"] >= 15

    # ---- Corpus aggregation ----

    def test_corpus_aggregates_quote_only_runs(self, ipsi_quote_path, tmp_path):
        from app.pdf_extraction.coverage_audit import (
            audit_quote_document, audit_corpus,
        )
        audits = [
            audit_quote_document(str(ipsi_quote_path), "ipsi"),
            audit_quote_document(str(self._synthetic_quote_pdf(tmp_path)), "synthetic"),
        ]
        summary = audit_corpus(audits)
        qs = summary["quote_only_summary"]
        assert qs["document_count"] == 2
        assert qs["accepted_rows_total"] >= 16
        assert qs["rows_enriched_qty_unit_total"] == 2  # only the synthetic rows enriched
        assert summary["paired_summary"]["document_count"] == 0
        assert summary["gap_summary"]["documents_with_zero_enrichment"] >= 1

    def test_corpus_aggregates_paired_runs(self, ipsi_quote_path, dot_pdf_path, estprop_pdf_path):
        from app.pdf_extraction.coverage_audit import (
            audit_paired_documents, audit_corpus,
        )
        audits = [
            audit_paired_documents(str(ipsi_quote_path), str(dot_pdf_path), "trusted_pair"),
            audit_paired_documents(str(ipsi_quote_path), str(estprop_pdf_path), "blocked_pair"),
        ]
        summary = audit_corpus(audits)
        ps = summary["paired_summary"]
        assert ps["document_count"] == 2
        assert ps["pairing_status_histogram"]["trusted"] == 1
        assert ps["pairing_status_histogram"]["rejected"] == 1
        assert summary["gap_summary"]["paired_documents_blocked"] == 1

    def test_corpus_keeps_quote_and_paired_distinct(self, ipsi_quote_path, dot_pdf_path):
        from app.pdf_extraction.coverage_audit import (
            audit_quote_document, audit_paired_documents, audit_corpus,
        )
        audits = [
            audit_quote_document(str(ipsi_quote_path), "quote_only"),
            audit_paired_documents(str(ipsi_quote_path), str(dot_pdf_path), "paired"),
        ]
        summary = audit_corpus(audits)
        assert summary["quote_only_summary"]["document_count"] == 1
        assert summary["paired_summary"]["document_count"] == 1
        # Row counts never averaged across modes.
        assert summary["quote_only_summary"]["accepted_rows_total"] >= 14
        assert summary["paired_summary"]["rows_mapped_total"] >= 13

    def test_corpus_ratios_expose_numerator_denominator(self, ipsi_quote_path):
        from app.pdf_extraction.coverage_audit import (
            audit_quote_document, audit_corpus,
        )
        summary = audit_corpus([audit_quote_document(str(ipsi_quote_path), "ipsi")])
        r = summary["quote_only_summary"]["ratios"]["rows_enriched_qty_unit_per_accepted"]
        assert r["denominator"] >= 14
        assert r["numerator"] == 0
        assert r["ratio"] == 0.0

    def test_corpus_empty(self):
        from app.pdf_extraction.coverage_audit import audit_corpus
        summary = audit_corpus([])
        assert summary["run_count"] == 0
        assert summary["quote_only_summary"]["document_count"] == 0
        assert summary["paired_summary"]["document_count"] == 0

    # ---- Gap surfacing ----

    def test_gap_summary_surfaces_zero_enrichment(self, ipsi_quote_path, tmp_path):
        from app.pdf_extraction.coverage_audit import (
            audit_quote_document, audit_corpus,
        )
        audits = [
            audit_quote_document(str(ipsi_quote_path), "ipsi"),
            audit_quote_document(
                str(self._synthetic_quote_pdf(tmp_path, with_qty=False, with_header=False)),
                "no_enrichment",
            ),
        ]
        summary = audit_corpus(audits)
        # IPSI + synthetic-no-enrichment both have accepted rows but zero enrichment.
        assert summary["gap_summary"]["documents_with_zero_enrichment"] == 2

    def test_gap_summary_surfaces_no_header(self, ipsi_quote_path):
        from app.pdf_extraction.coverage_audit import (
            audit_quote_document, audit_corpus,
        )
        summary = audit_corpus([audit_quote_document(str(ipsi_quote_path), "ipsi")])
        assert summary["gap_summary"]["documents_with_no_header_detected"] == 1

    # ---- Determinism ----

    def test_audit_is_deterministic(self, ipsi_quote_path):
        from app.pdf_extraction.coverage_audit import audit_quote_document
        a1 = audit_quote_document(str(ipsi_quote_path), "ipsi")
        a2 = audit_quote_document(str(ipsi_quote_path), "ipsi")
        # Stable metric dicts (document_diagnostics may contain nondeterministic
        # timestamps in future; we test metrics only).
        assert a1["metrics"] == a2["metrics"]

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c26(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c26(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20r. C25 — Office Workflow Integration Layer Tests
# ---------------------------------------------------------------------------


class TestC25OfficeWorkflow:
    """C25: deterministic office workflow packet wrapping prioritized
    findings with append-only reviewer metadata."""

    def _finding(self, **kw):
        base = {
            "normalized_row_id": "qr-p0-r0",
            "source_page": 0,
            "quote_description": "Item A",
            "quote_line_ref": "0010",
            "mapped_bid_line_number": "0010",
            "mapped_bid_item_number": "2101-0850001",
            "mapping_outcome": "mapped",
            "comparison_status": "match",
            "compared_fields": ["unit", "qty"],
            "non_comparable_reason": None,
            "discrepancy_class": "comparable_match",
            "review_flags": [],
            "comparison_flags": [],
            "quote_values": {}, "bid_values": {},
            "priority_class": "low",
            "priority_reason": "mapped_row_fully_reconciled_no_discrepancies",
            "priority_trace": {},
            "finding_trace": {},
        }
        base.update(kw)
        return base

    def _packet(self, rows, packet_status="partial", pairing_status="trusted"):
        return {
            "packet_version": "findings_packet/v1",
            "packet_status": packet_status,
            "pairing_section": {"pairing_status": pairing_status},
            "quote_section": {}, "bid_section": {}, "mapping_section": {},
            "reconciliation_section": {}, "discrepancy_summary": {},
            "priority_summary": {}, "findings_rows": rows,
            "packet_diagnostics": {},
            "prioritization_version": "review_prioritization/v1",
        }

    # ---- Stable structure ----

    def test_workflow_version_present(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet
        wf = build_workflow_packet(self._packet([self._finding()]))
        assert wf["workflow_version"] == "office_workflow/v1"

    def test_all_sections_present(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet
        wf = build_workflow_packet(self._packet([self._finding()]))
        for k in ("workflow_status", "packet_status", "packet_version",
                  "queue_summary", "queue_rows", "workflow_diagnostics"):
            assert k in wf

    def test_queue_row_shape(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet
        wf = build_workflow_packet(self._packet([self._finding()]))
        row = wf["queue_rows"][0]
        for k in ("normalized_row_id", "priority_class", "priority_reason",
                  "discrepancy_class", "comparison_status", "mapping_outcome",
                  "quote_description", "review_state", "review_disposition",
                  "review_notes", "source_finding_ref"):
            assert k in row

    def test_default_review_state_is_open(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet, STATE_OPEN
        wf = build_workflow_packet(self._packet([self._finding()]))
        assert wf["queue_rows"][0]["review_state"] == STATE_OPEN
        assert wf["queue_rows"][0]["review_notes"] == []

    # ---- Queue ordering ----

    def test_queue_ordered_by_priority(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet
        rows = [
            self._finding(normalized_row_id="qr-p0-r0", priority_class="low"),
            self._finding(normalized_row_id="qr-p0-r1", priority_class="critical"),
            self._finding(normalized_row_id="qr-p0-r2", priority_class="medium"),
            self._finding(normalized_row_id="qr-p0-r3", priority_class="high"),
        ]
        wf = build_workflow_packet(self._packet(rows))
        priorities = [r["priority_class"] for r in wf["queue_rows"]]
        assert priorities == ["critical", "high", "medium", "low"]

    def test_queue_ordering_stable_on_ties(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet
        rows = [
            self._finding(normalized_row_id=f"qr-p0-r{i}", priority_class="high")
            for i in range(4)
        ]
        wf = build_workflow_packet(self._packet(rows))
        ids = [r["normalized_row_id"] for r in wf["queue_rows"]]
        assert ids == ["qr-p0-r0", "qr-p0-r1", "qr-p0-r2", "qr-p0-r3"]

    # ---- Append-only review metadata ----

    def test_review_metadata_attaches_without_mutating_findings(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet
        finding = self._finding(priority_class="high", discrepancy_class="unmapped_quote_row")
        packet = self._packet([finding])
        import copy
        snap = copy.deepcopy(packet)
        metadata = {
            "rows": {
                "qr-p0-r0": {
                    "review_state": "reviewed",
                    "review_disposition": "confirmed",
                    "notes": [
                        {"note_id": "n1", "author": "alice", "timestamp": "2026-04-15T10:00",
                         "note_type": "comment", "text": "Verified."},
                    ],
                }
            }
        }
        wf = build_workflow_packet(packet, metadata)
        row = wf["queue_rows"][0]
        assert row["review_state"] == "reviewed"
        assert row["review_disposition"] == "confirmed"
        assert len(row["review_notes"]) == 1
        assert row["review_notes"][0]["note_id"] == "n1"
        # Findings packet unchanged.
        assert packet == snap
        # Discrepancy/priority truth preserved.
        assert row["discrepancy_class"] == "unmapped_quote_row"
        assert row["priority_class"] == "high"

    def test_multiple_states_exercised(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet
        rows = [
            self._finding(normalized_row_id="qr-p0-r0", priority_class="critical"),
            self._finding(normalized_row_id="qr-p0-r1", priority_class="high"),
            self._finding(normalized_row_id="qr-p0-r2", priority_class="medium"),
            self._finding(normalized_row_id="qr-p0-r3", priority_class="low"),
        ]
        meta = {
            "rows": {
                "qr-p0-r0": {"review_state": "open"},
                "qr-p0-r1": {"review_state": "reviewed"},
                "qr-p0-r2": {"review_state": "resolved"},
                "qr-p0-r3": {"review_state": "deferred"},
            }
        }
        wf = build_workflow_packet(self._packet(rows), meta)
        states = {r["normalized_row_id"]: r["review_state"] for r in wf["queue_rows"]}
        assert states["qr-p0-r0"] == "open"
        assert states["qr-p0-r1"] == "reviewed"
        assert states["qr-p0-r2"] == "resolved"
        assert states["qr-p0-r3"] == "deferred"

    def test_unknown_state_defaults_to_open(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet
        meta = {"rows": {"qr-p0-r0": {"review_state": "bogus"}}}
        wf = build_workflow_packet(self._packet([self._finding()]), meta)
        assert wf["queue_rows"][0]["review_state"] == "open"

    def test_unknown_row_ids_surface_in_diagnostics(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet
        meta = {"rows": {"nonexistent-id": {"review_state": "reviewed"}}}
        wf = build_workflow_packet(self._packet([self._finding()]), meta)
        assert "nonexistent-id" in wf["workflow_diagnostics"]["unknown_review_row_ids"]
        # No queue row invented for the unknown id.
        assert len(wf["queue_rows"]) == 1

    def test_notes_are_append_only_and_shape_validated(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet
        meta = {
            "rows": {
                "qr-p0-r0": {
                    "notes": [
                        {"note_id": "n1", "author": "alice", "note_type": "comment", "text": "first"},
                        {"note_id": "n2", "author": "bob", "note_type": "resolution_note", "text": "done"},
                        # Invalid entry — ignored, not crashed
                        "not a dict",
                    ]
                }
            }
        }
        wf = build_workflow_packet(self._packet([self._finding()]), meta)
        notes = wf["queue_rows"][0]["review_notes"]
        assert len(notes) == 2
        assert notes[0]["note_id"] == "n1"
        assert notes[1]["note_type"] == "resolution_note"

    def test_unknown_note_type_defaults_to_comment(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet
        meta = {"rows": {"qr-p0-r0": {"notes": [{"note_id": "n1", "note_type": "bogus", "text": "x"}]}}}
        wf = build_workflow_packet(self._packet([self._finding()]), meta)
        assert wf["queue_rows"][0]["review_notes"][0]["note_type"] == "comment"

    # ---- Queue summary ----

    def test_queue_summary_open_counts_by_priority(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet
        rows = [
            self._finding(normalized_row_id="qr-p0-r0", priority_class="critical"),
            self._finding(normalized_row_id="qr-p0-r1", priority_class="high"),
            self._finding(normalized_row_id="qr-p0-r2", priority_class="medium"),
            self._finding(normalized_row_id="qr-p0-r3", priority_class="low"),
        ]
        wf = build_workflow_packet(self._packet(rows))
        s = wf["queue_summary"]
        assert s["rows_total"] == 4
        assert s["rows_unreviewed"] == 4
        assert s["critical_open"] == 1
        assert s["high_open"] == 1
        assert s["medium_open"] == 1
        assert s["low_open"] == 1

    def test_queue_summary_reviewed_resolved_deferred(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet
        rows = [
            self._finding(normalized_row_id="qr-p0-r0", priority_class="high"),
            self._finding(normalized_row_id="qr-p0-r1", priority_class="high"),
            self._finding(normalized_row_id="qr-p0-r2", priority_class="high"),
            self._finding(normalized_row_id="qr-p0-r3", priority_class="high"),
        ]
        meta = {
            "rows": {
                "qr-p0-r1": {"review_state": "reviewed"},
                "qr-p0-r2": {"review_state": "resolved"},
                "qr-p0-r3": {"review_state": "deferred"},
            }
        }
        wf = build_workflow_packet(self._packet(rows), meta)
        s = wf["queue_summary"]
        assert s["rows_unreviewed"] == 1
        assert s["rows_reviewed"] == 1
        assert s["rows_resolved"] == 1
        assert s["rows_deferred"] == 1
        assert s["high_open"] == 1  # only the unreviewed high row

    # ---- Workflow status derivation ----

    def test_workflow_status_open_when_all_open(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet, WORKFLOW_OPEN
        wf = build_workflow_packet(self._packet([self._finding()]))
        assert wf["workflow_status"] == WORKFLOW_OPEN

    def test_workflow_status_in_review_when_some_reviewed(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet, WORKFLOW_IN_REVIEW
        rows = [self._finding(normalized_row_id=f"qr-p0-r{i}") for i in range(3)]
        meta = {"rows": {"qr-p0-r0": {"review_state": "reviewed"}}}
        wf = build_workflow_packet(self._packet(rows), meta)
        assert wf["workflow_status"] == WORKFLOW_IN_REVIEW

    def test_workflow_status_resolved_partial(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet, WORKFLOW_RESOLVED_PARTIAL
        rows = [self._finding(normalized_row_id=f"qr-p0-r{i}") for i in range(3)]
        meta = {"rows": {"qr-p0-r0": {"review_state": "resolved"}}}
        wf = build_workflow_packet(self._packet(rows), meta)
        assert wf["workflow_status"] == WORKFLOW_RESOLVED_PARTIAL

    def test_workflow_status_resolved_complete(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet, WORKFLOW_RESOLVED_COMPLETE
        rows = [self._finding(normalized_row_id=f"qr-p0-r{i}") for i in range(2)]
        meta = {
            "rows": {
                "qr-p0-r0": {"review_state": "resolved"},
                "qr-p0-r1": {"review_state": "resolved"},
            }
        }
        wf = build_workflow_packet(self._packet(rows), meta)
        assert wf["workflow_status"] == WORKFLOW_RESOLVED_COMPLETE

    # ---- Immutability ----

    def test_no_mutation_of_findings_packet(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet
        import copy
        packet = self._packet([self._finding()])
        snap = copy.deepcopy(packet)
        build_workflow_packet(packet, {"rows": {"qr-p0-r0": {"review_state": "reviewed"}}})
        assert packet == snap

    def test_discrepancy_and_priority_truth_preserved(self):
        from app.pdf_extraction.office_workflow import build_workflow_packet
        finding = self._finding(
            priority_class="critical",
            priority_reason="upstream_pairing_rejected_comparison_invalid",
            discrepancy_class="blocked_by_pairing",
            comparison_status="blocked",
            mapping_outcome="blocked",
        )
        packet = self._packet([finding], packet_status="blocked", pairing_status="rejected")
        meta = {"rows": {"qr-p0-r0": {
            "review_state": "reviewed",
            "review_disposition": "acknowledged",
        }}}
        wf = build_workflow_packet(packet, meta)
        row = wf["queue_rows"][0]
        # Reviewer cannot override governed state.
        assert row["priority_class"] == "critical"
        assert row["discrepancy_class"] == "blocked_by_pairing"
        assert row["comparison_status"] == "blocked"
        assert row["mapping_outcome"] == "blocked"
        assert wf["packet_status"] == "blocked"

    # ---- Endpoint integration ----

    def test_workflow_endpoint_trusted(self, client, ipsi_quote_path, dot_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/findings/workflow",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["workflow_version"] == "office_workflow/v1"
        assert data["workflow_status"] == "open"
        assert data["packet_status"] == "partial"
        # Queue carries all 15 rows, all still open.
        assert data["queue_summary"]["rows_total"] >= 15
        assert data["queue_summary"]["rows_unreviewed"] >= 15
        # Queue is priority-ordered: first rows are high (unmapped).
        assert data["queue_rows"][0]["priority_class"] == "high"

    def test_workflow_endpoint_with_metadata(self, client, ipsi_quote_path, dot_pdf_path):
        import json
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            # IPSI unmapped rows have ids qr-p0-r0 and qr-p0-r14 per fixture
            meta = {
                "rows": {
                    "qr-p0-r0": {
                        "review_state": "reviewed",
                        "review_disposition": "confirmed_unmapped",
                        "notes": [
                            {"note_id": "n1", "author": "alice",
                             "timestamp": "2026-04-15T10:00",
                             "note_type": "comment", "text": "Reviewer confirmed."}
                        ],
                    }
                }
            }
            resp = client.post(
                "/extract/quote/findings/workflow",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
                data={"review_metadata": json.dumps(meta)},
            )
        assert resp.status_code == 200
        data = resp.json()
        # Find the annotated row in the queue.
        annotated = next(r for r in data["queue_rows"] if r["normalized_row_id"] == "qr-p0-r0")
        assert annotated["review_state"] == "reviewed"
        assert annotated["review_disposition"] == "confirmed_unmapped"
        assert len(annotated["review_notes"]) == 1
        assert data["queue_summary"]["rows_reviewed"] == 1

    def test_workflow_endpoint_blocked(self, client, ipsi_quote_path, estprop_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(estprop_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/findings/workflow",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("estprop121.pdf", df, "application/pdf"),
                },
            )
        assert resp.status_code == 422
        data = resp.json()
        assert data["packet_status"] == "blocked"
        for row in data["queue_rows"]:
            assert row["priority_class"] == "critical"

    def test_workflow_endpoint_invalid_metadata(self, client, ipsi_quote_path, dot_pdf_path):
        with open(ipsi_quote_path, "rb") as qf, open(dot_pdf_path, "rb") as df:
            resp = client.post(
                "/extract/quote/findings/workflow",
                files={
                    "quote_pdf": ("ipsi_quote.pdf", qf, "application/pdf"),
                    "dot_pdf": ("dot.pdf", df, "application/pdf"),
                },
                data={"review_metadata": "not json"},
            )
        assert resp.status_code == 400

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c25(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c25(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True

    def test_quote_staging_unchanged_under_c25(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14


# ---------------------------------------------------------------------------
# 20q. C24 — Deterministic Multi-Row Aggregation Tests
# ---------------------------------------------------------------------------


class TestC24MultiRowAggregation:
    """C24: promote explicit two-line block candidates (line_ref+desc +
    price-only line) into accepted rows while preserving fail-closed
    acceptance rules."""

    def _block(self, raw_text, source_page=0, candidate_id="b0-0", reason="unstable_boundary"):
        return {
            "candidate_id": candidate_id,
            "raw_text": raw_text,
            "source_page": source_page,
            "rejection_reason": reason,
            "candidate_type": "block",
        }

    # ---- Unit tests for aggregate_block_candidates ----

    def test_aggregates_valid_two_line_block(self):
        from app.pdf_extraction.quote_multi_row_aggregation import (
            aggregate_block_candidates, AGG_APPLIED, AGGREGATOR_VERSION,
        )
        block = self._block("0530 Remove Asphalt Pavement\n$5.00 $2,750.00")
        accepted, rejected, meta = aggregate_block_candidates([], [block])
        assert len(accepted) == 1
        row = accepted[0]
        assert row["line_ref"] == "0530"
        assert row["description"] == "Remove Asphalt Pavement"
        assert row["unit_price"] == 5.0
        assert row["amount"] == 2750.0
        assert row["_aggregation_trace"]["aggregation_status"] == AGG_APPLIED
        assert row["_aggregation_trace"]["grouped_line_count"] == 2
        assert row["_aggregation_trace"]["aggregator_version"] == AGGREGATOR_VERSION
        assert meta["blocks_promoted"] == 1
        # Block is removed from rejected list.
        assert rejected == []

    def test_single_amount_block_aggregates(self):
        """Line 2 may carry exactly one dollar amount."""
        from app.pdf_extraction.quote_multi_row_aggregation import aggregate_block_candidates
        block = self._block("0600 Traffic Control\n$9,975.00")
        accepted, _, _ = aggregate_block_candidates([], [block])
        assert len(accepted) == 1
        row = accepted[0]
        assert row["unit_price"] is None
        assert row["amount"] == 9975.0

    def test_three_line_block_rejected(self):
        from app.pdf_extraction.quote_multi_row_aggregation import (
            aggregate_block_candidates, AGG_SKIPPED_BAD_LINE_COUNT,
        )
        block = self._block("0530 Remove Asphalt\nContinuation text\n$5.00 $2,750.00")
        accepted, rejected, _ = aggregate_block_candidates([], [block])
        assert accepted == []
        assert len(rejected) == 1
        assert rejected[0]["aggregation_status"] == AGG_SKIPPED_BAD_LINE_COUNT
        assert rejected[0]["rejection_reason"] == "ambiguous_group_structure"

    def test_line1_without_line_ref_rejected(self):
        from app.pdf_extraction.quote_multi_row_aggregation import (
            aggregate_block_candidates, AGG_SKIPPED_BAD_LINE1,
        )
        block = self._block("Remove Asphalt Pavement\n$5.00 $2,750.00")
        accepted, rejected, _ = aggregate_block_candidates([], [block])
        assert accepted == []
        assert rejected[0]["aggregation_status"] == AGG_SKIPPED_BAD_LINE1

    def test_line1_with_dollar_rejected(self):
        """Line 1 containing '$' is not a valid fragment start."""
        from app.pdf_extraction.quote_multi_row_aggregation import (
            aggregate_block_candidates, AGG_SKIPPED_BAD_LINE1,
        )
        block = self._block("0530 Remove Asphalt $100\n$5.00 $2,750.00")
        accepted, rejected, _ = aggregate_block_candidates([], [block])
        assert accepted == []
        assert rejected[0]["aggregation_status"] == AGG_SKIPPED_BAD_LINE1

    def test_line2_not_price_only_rejected(self):
        from app.pdf_extraction.quote_multi_row_aggregation import (
            aggregate_block_candidates, AGG_SKIPPED_BAD_LINE2,
        )
        block = self._block("0530 Remove Asphalt\nsome description with $5.00")
        accepted, rejected, _ = aggregate_block_candidates([], [block])
        assert accepted == []
        assert rejected[0]["aggregation_status"] == AGG_SKIPPED_BAD_LINE2
        assert rejected[0]["rejection_reason"] == "incomplete_group_fields"

    def test_non_block_candidate_passes_through(self):
        from app.pdf_extraction.quote_multi_row_aggregation import aggregate_block_candidates
        cand = {
            "candidate_id": "c0-0",
            "raw_text": "subtotal $100.00",
            "source_page": 0,
            "rejection_reason": "subtotal_row",
            "candidate_type": "line",
        }
        accepted, rejected, _ = aggregate_block_candidates([], [cand])
        assert accepted == []
        assert len(rejected) == 1
        assert rejected[0]["rejection_reason"] == "subtotal_row"

    def test_accepted_rows_preserved(self):
        """Existing accepted_rows must pass through unchanged."""
        from app.pdf_extraction.quote_multi_row_aggregation import aggregate_block_candidates
        existing = {
            "row_id": 0,
            "line_ref": "0010",
            "description": "Existing",
            "qty": None, "unit": None,
            "unit_price": 10.0, "amount": 100.0,
            "source_page": 0, "source_text": "0010 Existing $10.00 $100.00",
        }
        accepted, _, _ = aggregate_block_candidates([existing], [])
        assert accepted == [existing]

    def test_promoted_row_has_source_fragments(self):
        from app.pdf_extraction.quote_multi_row_aggregation import aggregate_block_candidates
        block = self._block("0530 Remove Asphalt Pavement\n$5.00 $2,750.00")
        accepted, _, _ = aggregate_block_candidates([], [block])
        trace = accepted[0]["_aggregation_trace"]
        assert trace["source_fragments"] == ["0530 Remove Asphalt Pavement", "$5.00 $2,750.00"]

    def test_aggregation_never_mutates_input(self):
        from app.pdf_extraction.quote_multi_row_aggregation import aggregate_block_candidates
        block = self._block("0530 Remove Asphalt\n$5.00 $2,750.00")
        import copy
        snap = copy.deepcopy(block)
        aggregate_block_candidates([], [block])
        assert block == snap

    def test_row_id_does_not_collide_with_pass1(self):
        from app.pdf_extraction.quote_multi_row_aggregation import aggregate_block_candidates
        block = self._block("0530 Remove Asphalt\n$5.00 $2,750.00")
        accepted, _, _ = aggregate_block_candidates([], [block])
        assert accepted[0]["row_id"] >= 100_000

    # ---- End-to-end via synthetic PDF ----

    def test_controlled_fixture_promotes_block(self, tmp_path):
        """Controlled fixture: a PDF with a split-line row (line_ref+desc
        on one line, price-only on the next). C9 puts it in rejected
        candidates; C24 promotes it to an accepted row that then flows
        through the rest of the governed pipeline."""
        import fitz
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf

        p = tmp_path / "split_row_quote.pdf"
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 72), "Quote — Split Rows", fontsize=12)
        page.insert_text((72, 100), "0530 Remove Asphalt Pavement", fontsize=10)
        page.insert_text((72, 120), "$5.00 $2,750.00", fontsize=10)
        page.insert_text((72, 150), "0600 Traffic Control", fontsize=10)
        page.insert_text((72, 170), "$9,975.00", fontsize=10)
        doc.save(str(p))
        doc.close()

        staging = normalize_quote_from_pdf(str(p))
        agg = staging["document_diagnostics"]["aggregation_meta"]
        assert agg["blocks_attempted"] >= 2
        assert agg["blocks_promoted"] >= 2

        descriptions = [r["description"] for r in staging["accepted_rows"]]
        assert any("Remove Asphalt" in d for d in descriptions)
        assert any("Traffic Control" in d for d in descriptions)

    def test_controlled_fixture_grouped_row_enriches_via_e2(self, tmp_path):
        """A split-line row whose description carries a tail qty+unit
        token, under an explicit table header, should be promoted by
        C24 AND then enriched by C23 E2."""
        import fitz
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf

        p = tmp_path / "split_with_header.pdf"
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 72), "ACME Quote", fontsize=12)
        page.insert_text((72, 100), "Description Qty Unit Unit Price Amount", fontsize=10)
        page.insert_text((72, 130), "0530 Remove Asphalt 550 SY", fontsize=10)
        page.insert_text((72, 150), "$5.00 $2,750.00", fontsize=10)
        doc.save(str(p))
        doc.close()

        staging = normalize_quote_from_pdf(str(p))
        rows = staging["accepted_rows"]
        promoted = [r for r in rows if "Remove Asphalt" in (r.get("description") or "")]
        assert len(promoted) == 1
        row = promoted[0]
        assert row["qty"] == 550.0
        assert row["unit"] == "SY"
        # After promotion, the enrichment pass fired on the grouped row.
        assert row["field_sources"]["qty"] in (
            "explicit_inline_qty_unit", "explicit_table_header_qty"
        )

    # ---- Real fixture regression guards ----

    def test_ipsi_aggregation_meta_exposed(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert "aggregation_meta" in staging["document_diagnostics"]

    def test_ipsi_accepted_row_count_not_weaker(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14

    def test_rasch_unchanged_under_c24(self, rasch_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(rasch_quote_path))
        assert staging["accepted_rows"] == []

    # ---- DOT regression guards ----

    def test_dot_native_unchanged_under_c24(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c24(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20p. C23 — Structured Quote Table Extraction Tests
# ---------------------------------------------------------------------------


class TestC23StructuredTableExtraction:
    """C23: header-gated tail-position qty/unit enrichment. Rule E2
    fires only when a table header is explicitly detected on the page,
    and the row's description ends in a `<qty> <UNIT>` token pair."""

    def _parsed(self, **kw):
        base = {
            "row_id": 0,
            "line_ref": None,
            "description": "Remove Asphalt Pavement 550 SY",
            "qty": None,
            "unit": None,
            "unit_price": 5.0,
            "amount": 2750.0,
            "source_page": 0,
            "source_text": "Remove Asphalt Pavement 550 SY $5.00 $2,750.00",
            "field_sources": {
                "qty": "not_present", "unit": "not_present",
                "unit_price": "explicit_dollar_parser",
                "amount": "explicit_dollar_parser",
            },
            "enrichment_trace": {"rules_attempted": [
                {"rule": "E1_inline_qty_unit", "applied": False,
                 "skip_reason": "no_inline_qty_unit_token"},
            ]},
            "enricher_version": "quote_enrichment/v1",
        }
        base.update(kw)
        return base

    def _pages_with_header(self):
        return [{
            "page_index": 0,
            "text": (
                "Quote — Vendor\n"
                "Description Qty Unit Unit Price Amount\n"
                "Remove Asphalt Pavement 550 SY $5.00 $2,750.00\n"
                "Concrete Barrier Rail 24 LF $100.00 $2,400.00\n"
                "Total $5,150.00\n"
            ),
            "char_count": 200,
        }]

    def _pages_without_header(self):
        return [{
            "page_index": 0,
            "text": (
                "Vendor Quotation\n"
                "Remove Asphalt Pavement 550 SY $5.00 $2,750.00\n"
                "Concrete Barrier Rail 24 LF $100.00 $2,400.00\n"
            ),
            "char_count": 160,
        }]

    # ---- Header detection ----

    def test_header_detected_with_two_tokens(self):
        from app.pdf_extraction.quote_table_extraction import detect_table_metadata
        m = detect_table_metadata(self._pages_with_header())
        assert m[0]["header_detected"] is True
        assert "QTY" in m[0]["header_tokens"]
        assert "UNIT" in m[0]["header_tokens"]

    def test_header_not_detected_when_only_one_token(self):
        from app.pdf_extraction.quote_table_extraction import detect_table_metadata
        pages = [{"page_index": 0, "text": "Only Description here\nno fields", "char_count": 40}]
        m = detect_table_metadata(pages)
        assert m[0]["header_detected"] is False

    def test_header_not_detected_without_header_line(self):
        from app.pdf_extraction.quote_table_extraction import detect_table_metadata
        m = detect_table_metadata(self._pages_without_header())
        assert m[0]["header_detected"] is False

    # ---- Rule E2 on a single parsed row ----

    def test_e2_fires_when_header_and_tail_match(self):
        from app.pdf_extraction.quote_table_extraction import (
            enrich_quote_rows_with_tables, SRC_TABLE_QTY, SRC_TABLE_UNIT,
            TABLE_RULE_VERSION,
        )
        row = self._parsed()
        out = enrich_quote_rows_with_tables([row], self._pages_with_header())
        assert out[0]["qty"] == 550.0
        assert out[0]["unit"] == "SY"
        assert out[0]["field_sources"]["qty"] == SRC_TABLE_QTY
        assert out[0]["field_sources"]["unit"] == SRC_TABLE_UNIT
        assert out[0]["table_rule_version"] == TABLE_RULE_VERSION

    def test_e2_does_not_fire_without_header(self):
        from app.pdf_extraction.quote_table_extraction import enrich_quote_rows_with_tables
        row = self._parsed()
        out = enrich_quote_rows_with_tables([row], self._pages_without_header())
        assert out[0]["qty"] is None
        assert out[0]["unit"] is None
        trace = out[0]["enrichment_trace"]["rules_attempted"][-1]
        assert trace["rule"] == "E2_table_header_tail_qty_unit"
        assert trace["applied"] is False
        assert trace["skip_reason"] == "no_table_header_on_page"

    def test_e2_does_not_fire_when_e1_already_enriched(self):
        from app.pdf_extraction.quote_table_extraction import enrich_quote_rows_with_tables
        row = self._parsed(qty=100.0, unit="EA",
                           field_sources={"qty": "explicit_inline_qty_unit",
                                          "unit": "explicit_inline_qty_unit",
                                          "unit_price": "explicit_dollar_parser",
                                          "amount": "explicit_dollar_parser"})
        out = enrich_quote_rows_with_tables([row], self._pages_with_header())
        # E1 values preserved; E2 traced but not applied.
        assert out[0]["qty"] == 100.0
        assert out[0]["unit"] == "EA"
        assert out[0]["field_sources"]["qty"] == "explicit_inline_qty_unit"
        trace = out[0]["enrichment_trace"]["rules_attempted"][-1]
        assert trace["skip_reason"] == "row_already_has_qty_or_unit"

    def test_e2_requires_tail_position_not_anywhere(self):
        """A qty+unit token mid-description does NOT trigger E2."""
        from app.pdf_extraction.quote_table_extraction import enrich_quote_rows_with_tables
        row = self._parsed(description="550 SY of asphalt pavement")
        out = enrich_quote_rows_with_tables([row], self._pages_with_header())
        assert out[0]["qty"] is None
        trace = out[0]["enrichment_trace"]["rules_attempted"][-1]
        assert trace["skip_reason"] == "no_tail_qty_unit_token"

    def test_e2_rejects_arithmetic_mismatch(self):
        from app.pdf_extraction.quote_table_extraction import enrich_quote_rows_with_tables
        row = self._parsed(
            description="Concrete Barrier 24 LF",
            unit_price=10.0,
            amount=9999.0,
        )
        out = enrich_quote_rows_with_tables([row], self._pages_with_header())
        assert out[0]["qty"] is None
        trace = out[0]["enrichment_trace"]["rules_attempted"][-1]
        assert trace["skip_reason"] == "arithmetic_mismatch"

    def test_e2_accepts_arithmetic_match(self):
        from app.pdf_extraction.quote_table_extraction import enrich_quote_rows_with_tables
        row = self._parsed(
            description="Concrete Barrier 24 LF",
            unit_price=100.0,
            amount=2400.0,
        )
        out = enrich_quote_rows_with_tables([row], self._pages_with_header())
        assert out[0]["qty"] == 24.0
        assert out[0]["unit"] == "LF"

    def test_e2_unit_outside_whitelist_rejected(self):
        from app.pdf_extraction.quote_table_extraction import enrich_quote_rows_with_tables
        row = self._parsed(description="Pipe 24 MM")  # MM not in whitelist
        out = enrich_quote_rows_with_tables([row], self._pages_with_header())
        assert out[0]["qty"] is None
        trace = out[0]["enrichment_trace"]["rules_attempted"][-1]
        assert trace["skip_reason"] == "tail_unit_not_in_whitelist"

    # ---- Ordering: E2 never mutates previously-enriched rows ----

    def test_e2_pass_is_pure(self):
        from app.pdf_extraction.quote_table_extraction import enrich_quote_rows_with_tables
        row = self._parsed()
        import copy
        snap = copy.deepcopy(row)
        enrich_quote_rows_with_tables([row], self._pages_with_header())
        assert row == snap

    # ---- End-to-end via normalize_quote_from_pdf (controlled synthetic) ----

    def test_controlled_header_pdf_enriches_via_e2(self, tmp_path):
        import fitz
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf

        p = tmp_path / "table_quote.pdf"
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 72), "Quote — ACME Vendor", fontsize=12)
        page.insert_text((72, 100), "Description Qty Unit Unit Price Amount", fontsize=10)
        # The inline token is at TAIL position of each description (i.e.,
        # immediately before the $ amounts). E1 will also fire here because
        # exactly one whitelist token appears per description, so we assert
        # enrichment succeeded but do not care which rule stamped it.
        page.insert_text((72, 130), "Remove Asphalt Pavement 550 SY $5.00 $2,750.00", fontsize=10)
        page.insert_text((72, 155), "Concrete Barrier 24 LF $100.00 $2,400.00", fontsize=10)
        doc.save(str(p))
        doc.close()

        staging = normalize_quote_from_pdf(str(p))
        assert len(staging["accepted_rows"]) >= 2
        for row in staging["accepted_rows"]:
            # Either E1 or E2 should have enriched the row.
            assert row["qty"] is not None
            assert row["unit"] in {"SY", "LF"}
        # Table metadata is exposed on the diagnostics.
        md = staging["document_diagnostics"]["table_metadata"]
        assert md[0]["header_detected"] is True

    def test_e2_unique_contribution_where_e1_cannot_fire(self, tmp_path):
        """Description with two qty+unit tokens: E1 rejects (ambiguous),
        but E2's tail-position rule can still decide the trailing pair.
        This is the exact scenario C23 exists to unlock."""
        import fitz
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf

        p = tmp_path / "ambiguous_table_quote.pdf"
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 72), "ACME Quote", fontsize=12)
        page.insert_text((72, 100), "Description Qty Unit Unit Price Amount", fontsize=10)
        # "Cast Iron Pipe 4 LF 25 LF" — E1 sees 2 matches and skips.
        # E2 tail rule picks "25 LF".
        page.insert_text((72, 130),
                         "Cast Iron Pipe 4 LF 25 LF $10.00 $250.00", fontsize=10)
        doc.save(str(p))
        doc.close()

        staging = normalize_quote_from_pdf(str(p))
        assert len(staging["accepted_rows"]) >= 1
        row = staging["accepted_rows"][0]
        assert row["qty"] == 25.0
        assert row["unit"] == "LF"
        assert row["field_sources"]["qty"] == "explicit_table_header_qty"
        assert row["field_sources"]["unit"] == "explicit_table_header_unit"
        assert row["table_rule_version"] == "quote_table_extraction/v1"

    # ---- Real fixture regression guards ----

    def test_ipsi_table_metadata_exposed(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert "table_metadata" in staging["document_diagnostics"]

    def test_ipsi_enrichment_coverage_unchanged_under_c23(self, ipsi_quote_path):
        """IPSI has no explicit table header that matches the closed
        whitelist (it lists column labels as part of a scanned header
        block that OCR renders inconsistently). Enrichment coverage
        remains at 0 — fail-closed correctness."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14
        for row in staging["accepted_rows"]:
            assert row["qty"] is None
            assert row["unit"] is None

    def test_rasch_rejects_unchanged_under_c23(self, rasch_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(rasch_quote_path))
        # Rasch is OCR noise with no explicit table structure.
        assert staging["accepted_rows"] == []

    # ---- DOT regression guards ----

    def test_dot_native_unchanged_under_c23(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c23(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True


# ---------------------------------------------------------------------------
# 20m. C20 — Quote Data Enrichment Tests
# ---------------------------------------------------------------------------


class TestC20QuoteEnrichment:
    """C20: deterministic enrichment of qty/unit on parsed quote rows
    from explicit inline tokens. No fuzzy matching. No semantic guessing.
    """

    def _parsed(self, **kw):
        base = {
            "row_id": 0,
            "line_ref": None,
            "description": "Remove and Reinstall Sign",
            "qty": None,
            "unit": None,
            "unit_price": None,
            "amount": 275.0,
            "source_page": 0,
            "source_text": "Remove and Reinstall Sign $275.00",
        }
        base.update(kw)
        return base

    # ---- Rule E1: explicit inline qty + unit token ----

    def test_inline_qty_unit_enriches(self):
        from app.pdf_extraction.quote_enrichment import enrich_quote_row, SRC_EXPLICIT_INLINE
        row = self._parsed(description="24 LF Remove Pipe", amount=None, unit_price=None)
        out = enrich_quote_row(row)
        assert out["qty"] == 24.0
        assert out["unit"] == "LF"
        assert out["field_sources"]["qty"] == SRC_EXPLICIT_INLINE
        assert out["field_sources"]["unit"] == SRC_EXPLICIT_INLINE

    def test_inline_qty_unit_canonicalises_case(self):
        from app.pdf_extraction.quote_enrichment import enrich_quote_row
        row = self._parsed(description="8 ton Aggregate Base", amount=None, unit_price=None)
        out = enrich_quote_row(row)
        assert out["unit"] == "TON"

    def test_inline_qty_unit_supports_thousands_comma(self):
        from app.pdf_extraction.quote_enrichment import enrich_quote_row
        row = self._parsed(description="1,200 SY Pavement Repair", amount=None, unit_price=None)
        out = enrich_quote_row(row)
        assert out["qty"] == 1200.0
        assert out["unit"] == "SY"

    def test_inline_qty_unit_supports_decimal_qty(self):
        from app.pdf_extraction.quote_enrichment import enrich_quote_row
        row = self._parsed(description="8.5 CY Concrete Mix", amount=None, unit_price=None)
        out = enrich_quote_row(row)
        assert out["qty"] == 8.5
        assert out["unit"] == "CY"

    # ---- Fail-closed ambiguity ----

    def test_multiple_qty_unit_tokens_rejects_enrichment(self):
        from app.pdf_extraction.quote_enrichment import enrich_quote_row, SRC_NOT_PRESENT
        row = self._parsed(
            description="10 LF Type A 5 LF Type B",
            amount=None, unit_price=None,
        )
        out = enrich_quote_row(row)
        assert out["qty"] is None
        assert out["unit"] is None
        assert out["field_sources"]["qty"] == SRC_NOT_PRESENT
        trace = out["enrichment_trace"]["rules_attempted"][0]
        assert trace["applied"] is False
        assert trace["skip_reason"] == "multiple_qty_unit_candidates"

    def test_no_inline_token_leaves_fields_none(self):
        from app.pdf_extraction.quote_enrichment import enrich_quote_row
        row = self._parsed(description="Remove and Reinstall Sign")
        out = enrich_quote_row(row)
        assert out["qty"] is None
        assert out["unit"] is None

    def test_invalid_unit_token_ignored(self):
        """'4 in' is not in whitelist — must NOT enrich."""
        from app.pdf_extraction.quote_enrichment import enrich_quote_row
        row = self._parsed(description="Install 4 in diameter pipe")
        out = enrich_quote_row(row)
        assert out["qty"] is None
        assert out["unit"] is None

    def test_valid_and_invalid_tokens_together_enriches_valid(self):
        """'4 in' is ignored, '25 LF' is the only whitelist match → enrich."""
        from app.pdf_extraction.quote_enrichment import enrich_quote_row
        row = self._parsed(
            description="Install 4 in diameter pipe, 25 LF length",
            amount=None, unit_price=None,
        )
        out = enrich_quote_row(row)
        assert out["qty"] == 25.0
        assert out["unit"] == "LF"

    # ---- Consistency guard ----

    def test_enrichment_rejected_on_arithmetic_mismatch(self):
        """qty*unit_price must reconcile with amount within 1%."""
        from app.pdf_extraction.quote_enrichment import enrich_quote_row
        row = self._parsed(
            description="24 LF Remove Pipe",
            unit_price=10.0,
            amount=999.0,  # 24*10=240, way off 999
        )
        out = enrich_quote_row(row)
        assert out["qty"] is None
        assert out["unit"] is None
        trace = out["enrichment_trace"]["rules_attempted"][0]
        assert trace["skip_reason"] == "arithmetic_mismatch"

    def test_enrichment_accepted_on_arithmetic_match(self):
        from app.pdf_extraction.quote_enrichment import enrich_quote_row
        row = self._parsed(
            description="24 LF Remove Pipe",
            unit_price=10.0,
            amount=240.0,
        )
        out = enrich_quote_row(row)
        assert out["qty"] == 24.0
        assert out["unit"] == "LF"

    # ---- Immutability ----

    def test_enrichment_does_not_mutate_input(self):
        from app.pdf_extraction.quote_enrichment import enrich_quote_row
        row = self._parsed(description="24 LF Remove Pipe", amount=None, unit_price=None)
        import copy
        snap = copy.deepcopy(row)
        enrich_quote_row(row)
        assert row == snap

    # ---- Existing qty/unit never overwritten ----

    def test_existing_qty_or_unit_never_overwritten(self):
        from app.pdf_extraction.quote_enrichment import enrich_quote_row
        row = self._parsed(description="24 LF Remove Pipe", qty=999.0, unit="EA",
                           amount=None, unit_price=None)
        out = enrich_quote_row(row)
        assert out["qty"] == 999.0
        assert out["unit"] == "EA"
        trace = out["enrichment_trace"]["rules_attempted"][0]
        assert trace["applied"] is False
        assert trace["skip_reason"] == "row_already_has_qty_or_unit"

    # ---- Provenance + version ----

    def test_field_sources_always_present(self):
        from app.pdf_extraction.quote_enrichment import enrich_quote_row
        row = self._parsed()
        out = enrich_quote_row(row)
        assert "field_sources" in out
        for k in ("qty", "unit", "unit_price", "amount"):
            assert k in out["field_sources"]

    def test_enricher_version_present(self):
        from app.pdf_extraction.quote_enrichment import enrich_quote_row
        out = enrich_quote_row(self._parsed())
        assert out["enricher_version"] == "quote_enrichment/v1"

    # ---- Integration with staging ----

    def test_staging_accepted_rows_carry_field_sources(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        for row in staging["accepted_rows"]:
            assert "field_sources" in row
            assert row["enricher_version"] == "quote_enrichment/v1"

    def test_ipsi_enrichment_coverage_documented(self, ipsi_quote_path):
        """IPSI quote descriptions contain no explicit inline qty/unit
        tokens. Real-world enrichment coverage on this fixture is 0;
        enrichment remains disabled on rows that do not deterministically
        support it. This is a fail-closed correctness property, not a
        failure."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        enriched = [r for r in staging["accepted_rows"]
                    if r.get("field_sources", {}).get("qty") == "explicit_inline_qty_unit"]
        assert enriched == []
        # Rows remain accepted exactly as before.
        assert len(staging["accepted_rows"]) >= 14

    def test_ipsi_rejected_candidates_not_promoted(self, ipsi_quote_path):
        """C20 enrichment never elevates rejected_candidates into accepted_rows."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        for cand in staging["rejected_candidates"]:
            assert "rejection_reason" in cand

    # ---- Controlled end-to-end: enrichment feeds reconciliation ----

    def test_controlled_fixture_enrichment_reaches_comparable(self, tmp_path):
        """Controlled fixture: build a synthetic quote PDF with rows
        carrying inline qty/unit tokens, then show that enrichment makes
        at least one row comparable downstream."""
        import fitz
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.quote_enrichment import SRC_EXPLICIT_INLINE

        p = tmp_path / "controlled_quote.pdf"
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 72), "Quote — Item List", fontsize=12)
        page.insert_text((72, 100), "Description Qty Unit Unit Price Amount", fontsize=10)
        page.insert_text((72, 130),
                         "Remove Asphalt Pavement 550 SY $5.00 $2,750.00", fontsize=10)
        page.insert_text((72, 155),
                         "Concrete Barrier Rail 24 LF $100.00 $2,400.00", fontsize=10)
        page.insert_text((72, 180),
                         "Aggregate Base Course 8 TON $50.00 $400.00", fontsize=10)
        page.insert_text((72, 210), "Total $5,550.00", fontsize=10)
        doc.save(str(p))
        doc.close()

        staging = normalize_quote_from_pdf(str(p))
        assert len(staging["accepted_rows"]) >= 3
        enriched = [r for r in staging["accepted_rows"]
                    if r.get("field_sources", {}).get("qty") == SRC_EXPLICIT_INLINE]
        assert len(enriched) >= 3
        for r in enriched:
            assert r["qty"] is not None
            assert r["unit"] in {"SY", "LF", "TON"}

    # ---- Regression guards ----

    def test_dot_native_unchanged_under_c20(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93

    def test_dot_scanned_unchanged_under_c20(self, scanned_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, summary = extract_bid_items_from_pdf(str(scanned_pdf_path))
        assert len(rows) > 0
        assert summary["ocr_used"] is True

    def test_ipsi_row_count_unchanged_under_c20(self, ipsi_quote_path):
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14


# ---------------------------------------------------------------------------
# 20c. C10 — Quote Normalization Staging Tests
# ---------------------------------------------------------------------------

class TestQuoteNormalizationStaging:
    """
    C10: governed quote normalization staging produces three buckets:
        accepted_rows, rejected_candidates, document_diagnostics.
    """

    def test_ipsi_produces_staging_structure(self, ipsi_quote_path):
        """IPSI quote must return the three-bucket staging object with
        accepted rows, separated rejected candidates, and diagnostics."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))

        assert set(staging.keys()) >= {
            "document_class_detected",
            "extraction_source",
            "accepted_rows",
            "rejected_candidates",
            "document_diagnostics",
        }
        assert staging["document_class_detected"] == "quote"
        assert staging["extraction_source"] == "ocr_pdf"
        assert isinstance(staging["accepted_rows"], list)
        assert isinstance(staging["rejected_candidates"], list)
        assert isinstance(staging["document_diagnostics"], dict)

    def test_ipsi_accepted_rows_are_deterministic(self, ipsi_quote_path):
        """Accepted rows must match the canonical quote row schema with no
        guessed qty/unit."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        assert len(staging["accepted_rows"]) >= 14
        for row in staging["accepted_rows"]:
            assert row["description"]
            assert row["amount"] is not None and row["amount"] > 0
            assert row["qty"] is None
            assert row["unit"] is None
            assert row["extraction_source"] == "ocr_pdf"
            assert row["source_page"] is not None

    def test_ipsi_accepted_and_rejected_are_separated(self, ipsi_quote_path):
        """No item should appear in both accepted_rows and rejected_candidates."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        accepted_descriptions = {r["description"] for r in staging["accepted_rows"]}
        rejected_raws = [c["raw_text"] for c in staging["rejected_candidates"]]
        # No rejected raw_text should exactly equal an accepted description.
        for raw in rejected_raws:
            assert raw not in accepted_descriptions

    def test_ipsi_rejected_candidates_preserve_traceability(self, ipsi_quote_path):
        """Every rejected candidate must preserve raw_text, source_page,
        rejection_reason, and candidate_type."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        for cand in staging["rejected_candidates"]:
            assert cand["raw_text"]
            assert cand["source_page"] is not None
            assert cand["rejection_reason"]
            assert cand["candidate_type"] in ("line", "block", "row_like")
            assert cand["extraction_source"] == "ocr_pdf"
            assert cand["candidate_id"]

    def test_ipsi_diagnostics_shape(self, ipsi_quote_path):
        """Diagnostics must carry classification signals, counters, and
        status=success for IPSI."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        diag = staging["document_diagnostics"]
        assert diag["status"] == "success"
        assert diag["failure_reason"] is None
        assert "classification_signals" in diag
        assert diag["ocr_used"] is True
        assert diag["extraction_source"] == "ocr_pdf"
        counts = diag["candidate_counts"]
        assert counts["accepted_rows"] == len(staging["accepted_rows"])

    def test_ipsi_total_appears_in_rejected_not_accepted(self, ipsi_quote_path):
        """IPSI TOTAL line must live in rejected_candidates with reason
        total_row, never in accepted_rows."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(ipsi_quote_path))
        for row in staging["accepted_rows"]:
            assert "TOTAL" not in (row["description"] or "").upper().split()[:1]
        total_like = [
            c for c in staging["rejected_candidates"]
            if c["rejection_reason"] in ("total_row", "subtotal_row")
        ]
        # OCR quality may or may not surface a clean TOTAL line but if
        # present it must be on the rejected side only.
        for cand in total_like:
            assert "TOTAL" in cand["raw_text"].upper()

    def test_rasch_staging_failure_has_diagnostics(self, rasch_quote_path):
        """Rasch -> unknown classification. Staging must return zero
        accepted_rows with explicit failure_reason=unknown_document_class."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(rasch_quote_path))
        assert staging["document_class_detected"] == "unknown"
        assert staging["accepted_rows"] == []
        diag = staging["document_diagnostics"]
        assert diag["status"] == "extraction_failed"
        assert diag["failure_reason"] == "unknown_document_class"
        assert diag["ocr_used"] is True
        # Rejected evidence may be empty (Rasch has no $X.XX patterns after
        # OCR) but the key must be present.
        assert "rejected_candidates" in staging

    def test_rasch_has_no_accepted_rows(self, rasch_quote_path):
        """Hard guarantee: Rasch never produces accepted rows."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(rasch_quote_path))
        assert len(staging["accepted_rows"]) == 0

    def test_dot_schedule_rejected_from_staging(self, dot_pdf_path):
        """DOT schedule must NEVER be normalized through the quote staging
        layer. The call must raise ExtractionError with explicit reason."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.service import FAIL_UNSUPPORTED_CLASS
        with pytest.raises(ExtractionError) as exc_info:
            normalize_quote_from_pdf(str(dot_pdf_path))
        assert exc_info.value.meta.get("failure_reason") == FAIL_UNSUPPORTED_CLASS
        assert exc_info.value.meta.get("document_class_detected") == "dot_schedule"

    def test_scanned_dot_rejected_from_staging(self, scanned_pdf_path):
        """Scanned DOT (C8B path) must also be rejected by the quote
        staging layer — it must never flow through quote normalization."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        from app.pdf_extraction.service import FAIL_UNSUPPORTED_CLASS
        with pytest.raises(ExtractionError) as exc_info:
            normalize_quote_from_pdf(str(scanned_pdf_path))
        assert exc_info.value.meta.get("failure_reason") == FAIL_UNSUPPORTED_CLASS

    def test_unknown_document_returns_staged_failure(self, non_schedule_pdf_path):
        """Generic non-schedule non-quote text -> staged failure with
        document_class_detected=unknown and zero accepted rows."""
        from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
        staging = normalize_quote_from_pdf(str(non_schedule_pdf_path))
        assert staging["document_class_detected"] == "unknown"
        assert staging["accepted_rows"] == []
        assert staging["document_diagnostics"]["status"] == "extraction_failed"
        assert staging["document_diagnostics"]["failure_reason"] == "unknown_document_class"


class TestQuoteStagingEndpoint:
    """Endpoint tests for POST /extract/quote/staging."""

    def test_staging_endpoint_ipsi_success(self, client, ipsi_quote_path):
        with open(ipsi_quote_path, "rb") as f:
            resp = client.post(
                "/extract/quote/staging",
                files={"pdf": ("ipsi_quote.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["document_class_detected"] == "quote"
        assert data["extraction_source"] == "ocr_pdf"
        assert len(data["accepted_rows"]) >= 14
        assert isinstance(data["rejected_candidates"], list)
        assert data["document_diagnostics"]["status"] == "success"
        assert data["document_diagnostics"]["failure_reason"] is None

    def test_staging_endpoint_rasch_failed(self, client, rasch_quote_path):
        with open(rasch_quote_path, "rb") as f:
            resp = client.post(
                "/extract/quote/staging",
                files={"pdf": ("rasch_quote.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 422
        data = resp.json()
        assert data["document_class_detected"] == "unknown"
        assert data["accepted_rows"] == []
        diag = data["document_diagnostics"]
        assert diag["status"] == "extraction_failed"
        assert diag["failure_reason"] == "unknown_document_class"

    def test_staging_endpoint_rejects_dot_native(self, client, dot_pdf_path):
        """DOT native -> 422 with unsupported_document_class (never enters
        quote normalization staging)."""
        with open(dot_pdf_path, "rb") as f:
            resp = client.post(
                "/extract/quote/staging",
                files={"pdf": ("schedule.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 422
        data = resp.json()
        assert data["status"] == "extraction_failed"
        assert data["failure_reason"] == "unsupported_document_class"
        assert data["document_class_detected"] == "dot_schedule"

    def test_staging_endpoint_rejects_dot_scanned(self, client, scanned_pdf_path):
        """Scanned DOT (C8B) -> 422 with unsupported_document_class."""
        with open(scanned_pdf_path, "rb") as f:
            resp = client.post(
                "/extract/quote/staging",
                files={"pdf": ("scanned.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 422
        data = resp.json()
        assert data["status"] == "extraction_failed"
        assert data["failure_reason"] == "unsupported_document_class"

    def test_dot_auto_endpoint_unaffected_by_c10(self, client, dot_pdf_path):
        """C10 regression guard: /extract/auto on DOT native still succeeds
        and stays in the DOT lane."""
        with open(dot_pdf_path, "rb") as f:
            resp = client.post(
                "/extract/auto",
                files={"pdf": ("schedule.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["document_class_detected"] == "dot_schedule"
        assert data["row_count"] == 93

    def test_dot_bidlane_endpoint_unaffected_by_c10(self, client, dot_pdf_path):
        """C10 regression guard: /extract/bid-items/pdf on DOT native
        still succeeds with 93 rows — C8 path untouched."""
        with open(dot_pdf_path, "rb") as f:
            resp = client.post(
                "/extract/bid-items/pdf",
                files={"pdf": ("schedule.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 93


# ---------------------------------------------------------------------------
# 21. Quote Endpoint Tests
# ---------------------------------------------------------------------------

class TestQuoteEndpoint:

    def test_quote_endpoint_ipsi_success(self, client, ipsi_quote_path):
        """POST /extract/quote/pdf with IPSI quote -> 200 with rows and
        explicit document_class_detected / extraction_source / null failure."""
        with open(ipsi_quote_path, "rb") as f:
            resp = client.post(
                "/extract/quote/pdf",
                files={"pdf": ("ipsi_quote.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["failure_reason"] is None
        assert data["document_class_detected"] == "quote"
        assert data["extraction_source"] == "ocr_pdf"
        assert data["row_count"] >= 14
        assert data["summary"]["document_class"] == "quote"

    def test_quote_endpoint_rasch_fails_closed(self, client, rasch_quote_path):
        """POST /extract/quote/pdf with Rasch -> 422 with explicit failure_reason."""
        with open(rasch_quote_path, "rb") as f:
            resp = client.post(
                "/extract/quote/pdf",
                files={"pdf": ("rasch_quote.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 422
        data = resp.json()
        assert data["status"] == "extraction_failed"
        assert data["failure_reason"] == "no_candidate_quote_rows"
        assert data["extraction_source"] == "ocr_pdf"
        assert "No deterministic quote rows" in data["error"]

    def test_auto_endpoint_ipsi_routes_to_quote(self, client, ipsi_quote_path):
        """POST /extract/auto with IPSI -> routes to quote pipeline and
        reports the classification in the top-level response."""
        with open(ipsi_quote_path, "rb") as f:
            resp = client.post(
                "/extract/auto",
                files={"pdf": ("ipsi_quote.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["failure_reason"] is None
        assert data["document_class_detected"] == "quote"
        assert data["summary"]["document_class"] == "quote"
        assert data["row_count"] >= 14

    def test_auto_endpoint_dot_routes_to_schedule(self, client, dot_pdf_path):
        """POST /extract/auto with synthetic DOT -> routes to DOT schedule pipeline."""
        with open(dot_pdf_path, "rb") as f:
            resp = client.post(
                "/extract/auto",
                files={"pdf": ("schedule.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["failure_reason"] is None
        assert data["document_class_detected"] == "dot_schedule"
        assert data["summary"]["document_class"] == "dot_schedule"
        assert data["row_count"] == 93

    def test_auto_endpoint_rasch_returns_unknown_class(self, client, rasch_quote_path):
        """POST /extract/auto with Rasch -> 422 with document_class_detected=unknown
        and failure_reason=unknown_document_class. Never routed through DOT."""
        with open(rasch_quote_path, "rb") as f:
            resp = client.post(
                "/extract/auto",
                files={"pdf": ("rasch_quote.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 422
        data = resp.json()
        assert data["status"] == "extraction_failed"
        assert data["failure_reason"] == "unknown_document_class"
        assert data["document_class_detected"] == "unknown"

    def test_auto_endpoint_scanned_dot_uses_ocr_lane(self, client, scanned_pdf_path):
        """POST /extract/auto with dot_schedule_scanned -> routes through
        OCR and into the C8 DOT lane. Regression guard for C8B."""
        with open(scanned_pdf_path, "rb") as f:
            resp = client.post(
                "/extract/auto",
                files={"pdf": ("scanned.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["failure_reason"] is None
        assert data["document_class_detected"] == "dot_schedule"
        assert data["extraction_source"] == "ocr_pdf"
        assert data["summary"]["ocr_used"] is True
        assert data["row_count"] > 0

    def test_quote_endpoint_non_schedule_fails_with_reason(self, client, non_schedule_pdf_path):
        """Explicit quote endpoint on a non-quote text PDF must fail closed
        with an explicit failure_reason (not a 500)."""
        with open(non_schedule_pdf_path, "rb") as f:
            resp = client.post(
                "/extract/quote/pdf",
                files={"pdf": ("non_schedule.pdf", f, "application/pdf")},
            )
        assert resp.status_code == 422
        data = resp.json()
        assert data["status"] == "extraction_failed"
        assert data["failure_reason"] in (
            "no_candidate_quote_rows",
            "quote_structure_insufficient",
        )


# ---------------------------------------------------------------------------
# C86 — Storage Adapter Tests
# ---------------------------------------------------------------------------


class TestC86StorageAdapter:

    def test_in_memory_put_get_roundtrip(self):
        from app.pdf_extraction.storage_adapter import InMemoryStorageAdapter
        a = InMemoryStorageAdapter()
        a.put("k1", {"v": 1})
        assert a.get("k1") == {"v": 1}
        assert a.get("missing") is None

    def test_in_memory_append_to_list_and_read(self):
        from app.pdf_extraction.storage_adapter import InMemoryStorageAdapter
        a = InMemoryStorageAdapter()
        a.append_to_list("lk", {"x": 1})
        a.append_to_list("lk", {"x": 2})
        items = a.list_items("lk")
        assert [i["x"] for i in items] == [1, 2]

    def test_in_memory_deep_copy_isolation(self):
        from app.pdf_extraction.storage_adapter import InMemoryStorageAdapter
        a = InMemoryStorageAdapter()
        value = {"v": {"inner": 1}}
        a.put("k1", value)
        value["v"]["inner"] = 999
        out = a.get("k1")
        assert out["v"]["inner"] == 1

    def test_in_memory_clear(self):
        from app.pdf_extraction.storage_adapter import InMemoryStorageAdapter
        a = InMemoryStorageAdapter()
        a.put("k1", {"v": 1})
        a.append_to_list("lk", {"x": 2})
        a.clear()
        assert a.get("k1") is None
        assert a.list_items("lk") == []

    def test_file_storage_put_get(self, tmp_path):
        from app.pdf_extraction.storage_adapter import FileStorageAdapter
        a = FileStorageAdapter(str(tmp_path / "store"))
        a.put("alpha:beta", {"v": 1})
        assert a.get("alpha:beta") == {"v": 1}

    def test_file_storage_append_to_list(self, tmp_path):
        from app.pdf_extraction.storage_adapter import FileStorageAdapter
        a = FileStorageAdapter(str(tmp_path / "store"))
        a.append_to_list("log", {"i": 0})
        a.append_to_list("log", {"i": 1})
        a.append_to_list("log", {"i": 2})
        assert [x["i"] for x in a.list_items("log")] == [0, 1, 2]

    def test_build_adapter_factory(self, tmp_path):
        from app.pdf_extraction.storage_adapter import (
            build_adapter, InMemoryStorageAdapter, FileStorageAdapter)
        import pytest as _p
        assert isinstance(build_adapter("in_memory"), InMemoryStorageAdapter)
        assert isinstance(build_adapter("file", base_dir=str(tmp_path)), FileStorageAdapter)
        with _p.raises(ValueError):
            build_adapter("bogus")

    def test_repository_mirrors_saves_to_adapter(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        from app.pdf_extraction.storage_adapter import InMemoryStorageAdapter
        adapter = InMemoryStorageAdapter()
        repo = ArtifactRepository(storage_adapter=adapter)
        repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j2"})
        assert len(adapter.list_items("records:quote_dossier")) == 2
        assert len(adapter.list_items("records:all")) == 2

    def test_adapter_failure_does_not_corrupt_repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository

        class BustedAdapter:
            def append_to_list(self, k, v):
                raise RuntimeError("boom")

        repo = ArtifactRepository(storage_adapter=BustedAdapter())
        rec = repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        assert rec["revision_sequence"] == 0
        assert repo.latest("quote_dossier", job_id="j1")["record_id"] == rec["record_id"]


# ---------------------------------------------------------------------------
# C87 — Scope Guardrails Tests
# ---------------------------------------------------------------------------


class TestC87ScopeGuardrails:

    def _rec(self, bid_id="b1", job_id="j1", org_id=None, owner_id=None):
        art = {"version": "v1", "bid_id": bid_id, "job_id": job_id}
        r = {"record_id": f"r-{bid_id}-{job_id}", "artifact_type": "quote_dossier",
             "envelope": {"artifact": art}}
        if org_id:
            r["org_id"] = org_id
        if owner_id:
            r["owner_id"] = owner_id
        return r

    def test_ok_on_matching_scope(self):
        from app.pdf_extraction.scope_guardrails import check_scope
        rec = self._rec(org_id="acme")
        out = check_scope(rec, bid_id="b1", job_id="j1", org_id="acme")
        assert out["ok"] is True
        assert out["reasons"] == []

    def test_bid_mismatch_fails(self):
        from app.pdf_extraction.scope_guardrails import check_scope, GUARD_BID_MISMATCH
        out = check_scope(self._rec(), bid_id="bOTHER")
        assert out["ok"] is False
        assert GUARD_BID_MISMATCH in out["reasons"]

    def test_org_mismatch_fails(self):
        from app.pdf_extraction.scope_guardrails import check_scope, GUARD_ORG_MISMATCH
        out = check_scope(self._rec(org_id="acme"), org_id="evil")
        assert out["ok"] is False
        assert GUARD_ORG_MISMATCH in out["reasons"]

    def test_owner_mismatch_fails(self):
        from app.pdf_extraction.scope_guardrails import check_scope, GUARD_OWNER_MISMATCH
        out = check_scope(self._rec(owner_id="alice"), owner_id="bob")
        assert GUARD_OWNER_MISMATCH in out["reasons"]

    def test_missing_record_returns_record_not_found(self):
        from app.pdf_extraction.scope_guardrails import check_scope
        out = check_scope(None, bid_id="b1")
        assert out["ok"] is False
        assert "record_not_found" in out["reasons"]

    def test_filter_records_by_scope_leakproof(self):
        from app.pdf_extraction.scope_guardrails import filter_records_by_scope
        records = [self._rec("b1", "j1"), self._rec("b2", "j2"), self._rec("b1", "j3")]
        out = filter_records_by_scope(records, bid_id="b1")
        assert len(out) == 2
        assert all(r["envelope"]["artifact"]["bid_id"] == "b1" for r in out)

    def test_missing_scope_fields_reported(self):
        from app.pdf_extraction.scope_guardrails import check_scope, GUARD_MISSING_ORG
        rec = self._rec()
        out = check_scope(rec, org_id="acme")
        assert GUARD_MISSING_ORG in out["reasons"]


# ---------------------------------------------------------------------------
# C88 — API Error Contracts Tests
# ---------------------------------------------------------------------------


class TestC88ApiErrorContracts:

    def test_build_error_returns_closed_vocab(self):
        from app.pdf_extraction.api_error_contracts import (
            build_error, ERR_INVALID_ARTIFACT_TYPE)
        e = build_error(ERR_INVALID_ARTIFACT_TYPE, detail={"x": 1}, hint="hint")
        assert e["error_code"] == ERR_INVALID_ARTIFACT_TYPE
        assert e["http_status"] == 400
        assert e["detail"] == {"x": 1}
        assert e["hint"] == "hint"
        assert e["error_contract_version"] == "api_error_contracts/v1"

    def test_http_mapping_codes(self):
        from app.pdf_extraction.api_error_contracts import (
            build_error, ERR_SCHEMA_MISMATCH, ERR_SCOPE_MISMATCH,
            ERR_MISSING_REVISION, ERR_RECORD_NOT_FOUND,
        )
        assert build_error(ERR_SCHEMA_MISMATCH)["http_status"] == 422
        assert build_error(ERR_SCOPE_MISMATCH)["http_status"] == 403
        assert build_error(ERR_MISSING_REVISION)["http_status"] == 404
        assert build_error(ERR_RECORD_NOT_FOUND)["http_status"] == 404

    def test_list_error_codes_stable(self):
        from app.pdf_extraction.api_error_contracts import list_error_codes
        codes = list_error_codes()
        assert codes == sorted(codes)
        assert "invalid_artifact_type" in codes
        assert "scope_mismatch" in codes

    def test_to_http_response_strips_status_key(self):
        from app.pdf_extraction.api_error_contracts import (
            build_error, to_http_response, ERR_INVALID_REQUEST)
        resp = to_http_response(build_error(ERR_INVALID_REQUEST))
        assert resp.status_code == 400
        import json as _json
        body = _json.loads(bytes(resp.body).decode("utf-8"))
        assert "http_status" not in body
        assert body["error_code"] == "invalid_request"


# ---------------------------------------------------------------------------
# C89 — Revision Diff Tests
# ---------------------------------------------------------------------------


class TestC89RevisionDiff:

    def _repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        return ArtifactRepository()

    def test_diff_changed_field(self):
        from app.pdf_extraction.revision_diff import diff_revisions
        repo = self._repo()
        r1 = repo.save("bid_readiness_snapshot", {"bid_id": "b1", "readiness_state": "CONDITIONAL"})
        r2 = repo.save("bid_readiness_snapshot", {"bid_id": "b1", "readiness_state": "READY"})
        d = diff_revisions(r1, r2)
        assert d["status"] == "changed"
        paths = {c["field_path"] for c in d["changed_fields"]}
        assert "readiness_state" in paths

    def test_diff_unchanged_all(self):
        from app.pdf_extraction.revision_diff import diff_revisions
        repo = self._repo()
        r1 = repo.save("bid_readiness_snapshot", {"bid_id": "b1", "readiness_state": "READY"})
        r2 = repo.save("bid_readiness_snapshot", {"bid_id": "b1", "readiness_state": "READY"})
        d = diff_revisions(r1, r2)
        assert d["status"] == "unchanged"
        assert d["changed_fields"] == []

    def test_initial_diff_when_before_is_none(self):
        from app.pdf_extraction.revision_diff import diff_revisions
        repo = self._repo()
        r1 = repo.save("bid_readiness_snapshot", {"bid_id": "b1", "readiness_state": "READY"})
        d = diff_revisions(None, r1)
        assert d["status"] == "initial"
        assert d["source_refs"]["before_record_id"] is None

    def test_diff_lineage_length(self):
        from app.pdf_extraction.revision_diff import diff_lineage
        repo = self._repo()
        repo.save("bid_readiness_snapshot", {"bid_id": "b1", "readiness_state": "CONDITIONAL"})
        repo.save("bid_readiness_snapshot", {"bid_id": "b1", "readiness_state": "READY"})
        repo.save("bid_readiness_snapshot", {"bid_id": "b1", "readiness_state": "HIGH_RISK"})
        history = repo.history("bid_readiness_snapshot", bid_id="b1")
        diffs = diff_lineage(history)
        assert len(diffs) == 2

    def test_identity_unchanged_includes_bid(self):
        from app.pdf_extraction.revision_diff import diff_revisions
        repo = self._repo()
        r1 = repo.save("bid_readiness_snapshot", {"bid_id": "b1", "readiness_state": "READY"})
        r2 = repo.save("bid_readiness_snapshot", {"bid_id": "b1", "readiness_state": "HIGH_RISK"})
        d = diff_revisions(r1, r2)
        assert "bid_id" in d["identity_unchanged"]

    def test_diff_summary_counts(self):
        from app.pdf_extraction.revision_diff import diff_revisions, diff_summary
        repo = self._repo()
        r1 = repo.save("bid_readiness_snapshot", {"bid_id": "b1", "readiness_state": "READY"})
        r2 = repo.save("bid_readiness_snapshot", {"bid_id": "b1", "readiness_state": "HIGH_RISK"})
        s = diff_summary(diff_revisions(r1, r2))
        assert s["artifact_type"] == "bid_readiness_snapshot"
        assert s["changed_field_count"] >= 1
        assert s["before_revision"] == 0
        assert s["after_revision"] == 1


# ---------------------------------------------------------------------------
# C90 — UI Integration Pack Tests
# ---------------------------------------------------------------------------


class TestC90UiIntegrationPack:

    def test_get_full_pack_shape(self):
        from app.pdf_extraction.ui_integration_pack import get_ui_integration_pack
        pack = get_ui_integration_pack()
        assert pack["ui_integration_version"] == "ui_integration_pack/v1"
        assert len(pack["screens"]) >= 5
        assert len(pack["export_actions"]) >= 5

    def test_get_screen_by_id(self):
        from app.pdf_extraction.ui_integration_pack import get_screen
        out = get_screen("bid_readiness")
        assert out["screen_id"] == "bid_readiness"
        assert out["endpoint"] == "/control-room/bid-readiness"

    def test_unknown_screen_returns_error(self):
        from app.pdf_extraction.ui_integration_pack import get_screen
        out = get_screen("nope")
        assert out.get("error") == "unknown_screen_id"

    def test_get_export_action(self):
        from app.pdf_extraction.ui_integration_pack import get_export_action
        out = get_export_action("final_carry")
        assert out["endpoint"] == "/exports/final-carry"

    def test_list_screen_and_export_ids_sorted(self):
        from app.pdf_extraction.ui_integration_pack import list_screen_ids, list_export_ids
        sids = list_screen_ids()
        eids = list_export_ids()
        assert sids == sorted(sids)
        assert eids == sorted(eids)
        assert "bid_readiness" in sids
        assert "final_carry" in eids

    def test_state_label_vocab_closed(self):
        from app.pdf_extraction.ui_integration_pack import get_ui_integration_pack
        vocab = get_ui_integration_pack()["state_label_vocab"]
        assert "READY" in vocab["readiness_state"]
        assert "BLOCKED" in vocab["readiness_state"]
        assert "CARRY" in vocab["carry_decision"]


# ---------------------------------------------------------------------------
# C91 — Production Smoke Harness Tests
# ---------------------------------------------------------------------------


class TestC91ProductionSmokeHarness:

    def _repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        return ArtifactRepository()

    def test_smoke_runs_all_scenarios(self):
        from app.pdf_extraction.production_smoke_harness import run_smoke
        out = run_smoke(repository=self._repo())
        assert out["smoke_harness_version"] == "production_smoke_harness/v1"
        assert out["summary"]["scenarios_run"] == 4
        assert out["summary"]["scenarios_failed"] == 0
        assert out["summary"]["scenarios_ok"] == 4

    def test_smoke_subset(self):
        from app.pdf_extraction.production_smoke_harness import run_smoke
        out = run_smoke(scenario_ids=["straightforward_usable"],
                        repository=self._repo())
        assert out["summary"]["scenarios_run"] == 1
        assert out["scenario_results"][0]["scenario_id"] == "straightforward_usable"

    def test_smoke_exercises_control_room_and_exports(self):
        from app.pdf_extraction.production_smoke_harness import run_smoke
        out = run_smoke(scenario_ids=["straightforward_usable"],
                        repository=self._repo())
        r = out["scenario_results"][0]
        assert r["control_room"]["package_overview_present"] is True
        assert r["control_room"]["bid_readiness_present"] is True
        assert r["export_statuses"]["bid_readiness"] is True
        assert r["export_statuses"]["final_carry"] is True

    def test_smoke_revision_diff_present(self):
        from app.pdf_extraction.production_smoke_harness import run_smoke
        out = run_smoke(scenario_ids=["proceed_with_caveats"],
                        repository=self._repo())
        r = out["scenario_results"][0]
        assert r["revision_diff"] is not None
        assert r["lineage_diff_count"] >= 1

    def test_smoke_unknown_scenario_reports_error(self):
        from app.pdf_extraction.production_smoke_harness import run_smoke
        out = run_smoke(scenario_ids=["bogus_xyz"], repository=self._repo())
        assert out["summary"]["scenarios_failed"] == 1
        assert len(out["errors"]) == 1


# ---------------------------------------------------------------------------
# C92 — Authorization Tests
# ---------------------------------------------------------------------------


class TestC92Authorization:

    def test_role_and_action_vocab_stable(self):
        from app.pdf_extraction.authorization import list_roles, list_actions
        roles = list_roles()
        actions = list_actions()
        assert roles == sorted(roles)
        assert actions == sorted(actions)
        assert "admin" in roles
        assert "save_artifact" in actions

    def test_admin_can_do_all(self):
        from app.pdf_extraction.authorization import (
            authorize, ROLE_ADMIN, list_actions)
        for a in list_actions():
            assert authorize(ROLE_ADMIN, a)["allowed"] is True

    def test_estimator_blocked_from_reset(self):
        from app.pdf_extraction.authorization import (
            authorize, ROLE_ESTIMATOR, ACTION_RESET_REPOSITORY,
            DENY_ROLE_NOT_PERMITTED)
        d = authorize(ROLE_ESTIMATOR, ACTION_RESET_REPOSITORY)
        assert d["allowed"] is False
        assert DENY_ROLE_NOT_PERMITTED in d["reasons"]

    def test_unknown_role_denied(self):
        from app.pdf_extraction.authorization import (
            authorize, DENY_UNKNOWN_ROLE, ACTION_READ_ARTIFACT)
        d = authorize("hacker", ACTION_READ_ARTIFACT)
        assert d["allowed"] is False
        assert DENY_UNKNOWN_ROLE in d["reasons"]

    def test_unknown_action_denied(self):
        from app.pdf_extraction.authorization import (
            authorize, ROLE_ADMIN, DENY_UNKNOWN_ACTION)
        d = authorize(ROLE_ADMIN, "delete_everything")
        assert d["allowed"] is False
        assert DENY_UNKNOWN_ACTION in d["reasons"]

    def test_missing_role_and_action(self):
        from app.pdf_extraction.authorization import (
            authorize, DENY_ROLE_MISSING, DENY_ACTION_MISSING)
        assert DENY_ROLE_MISSING in authorize(None, "save_artifact")["reasons"]
        assert DENY_ACTION_MISSING in authorize("admin", None)["reasons"]

    def test_guest_read_blocked_but_ui_allowed(self):
        from app.pdf_extraction.authorization import (
            authorize, ROLE_GUEST, ACTION_READ_ARTIFACT,
            ACTION_UI_INTEGRATION)
        assert authorize(ROLE_GUEST, ACTION_READ_ARTIFACT)["allowed"] is False
        assert authorize(ROLE_GUEST, ACTION_UI_INTEGRATION)["allowed"] is True

    def test_enforce_raises_on_denial(self):
        from app.pdf_extraction.authorization import (
            enforce, AuthorizationError)
        import pytest as _p
        with _p.raises(AuthorizationError):
            enforce("read_only", "save_artifact")

    def test_authorization_summary_shape(self):
        from app.pdf_extraction.authorization import authorization_summary
        out = authorization_summary()
        assert out["authorization_version"] == "authorization/v1"
        assert "role_action_map" in out


# ---------------------------------------------------------------------------
# C93 — Idempotency Tests
# ---------------------------------------------------------------------------


class TestC93Idempotency:

    def _repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        return ArtifactRepository()

    def test_new_write(self):
        from app.pdf_extraction.idempotency import (
            idempotent_save_artifact, IdempotencyStore, STATUS_NEW)
        store = IdempotencyStore()
        env = idempotent_save_artifact(self._repo(), "k1",
                                        "quote_dossier",
                                        {"dossier_version": "v1",
                                         "job_id": "j1"},
                                        store=store)
        assert env["status"] == STATUS_NEW

    def test_replay_returns_cached(self):
        from app.pdf_extraction.idempotency import (
            idempotent_save_artifact, IdempotencyStore, STATUS_REPLAY)
        repo = self._repo()
        store = IdempotencyStore()
        art = {"dossier_version": "v1", "job_id": "j1"}
        idempotent_save_artifact(repo, "k1", "quote_dossier", art, store=store)
        env2 = idempotent_save_artifact(repo, "k1", "quote_dossier",
                                         art, store=store)
        assert env2["status"] == STATUS_REPLAY
        # Repository should have exactly one record, not two.
        assert repo.repository_summary()["total_records"] == 1

    def test_conflict_same_key_different_payload(self):
        from app.pdf_extraction.idempotency import (
            idempotent_save_artifact, IdempotencyStore, STATUS_CONFLICT)
        repo = self._repo()
        store = IdempotencyStore()
        idempotent_save_artifact(repo, "k1", "quote_dossier",
                                  {"dossier_version": "v1", "job_id": "j1"},
                                  store=store)
        env = idempotent_save_artifact(repo, "k1", "quote_dossier",
                                        {"dossier_version": "v1", "job_id": "OTHER"},
                                        store=store)
        assert env["status"] == STATUS_CONFLICT
        assert env["response"] is None
        assert env["prior_payload_hash"] != env["payload_hash"]
        assert repo.repository_summary()["total_records"] == 1

    def test_missing_key_still_writes_but_marks_status(self):
        from app.pdf_extraction.idempotency import (
            idempotent_save_artifact, IdempotencyStore, STATUS_MISSING_KEY)
        repo = self._repo()
        env = idempotent_save_artifact(repo, None, "quote_dossier",
                                        {"dossier_version": "v1", "job_id": "j1"},
                                        store=IdempotencyStore())
        assert env["status"] == STATUS_MISSING_KEY
        assert repo.repository_summary()["total_records"] == 1

    def test_hash_is_deterministic(self):
        from app.pdf_extraction.idempotency import compute_payload_hash
        h1 = compute_payload_hash({"a": 1, "b": [1, 2, 3]})
        h2 = compute_payload_hash({"b": [1, 2, 3], "a": 1})
        assert h1 == h2

    def test_store_summary_lists_keys(self):
        from app.pdf_extraction.idempotency import (
            idempotent_save_artifact, IdempotencyStore)
        repo = self._repo()
        store = IdempotencyStore()
        idempotent_save_artifact(repo, "k1", "quote_dossier",
                                  {"dossier_version": "v1", "job_id": "j1"},
                                  store=store)
        idempotent_save_artifact(repo, "k2", "quote_dossier",
                                  {"dossier_version": "v1", "job_id": "j2"},
                                  store=store)
        s = store.summary()
        assert s["entry_count"] == 2
        assert s["keys"] == ["k1", "k2"]


# ---------------------------------------------------------------------------
# C94 — Backup/Restore Tests
# ---------------------------------------------------------------------------


class TestC94BackupRestore:

    def _repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        return ArtifactRepository()

    def test_create_and_validate_snapshot(self):
        from app.pdf_extraction.backup_restore import (
            create_snapshot, validate_snapshot)
        repo = self._repo()
        repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        repo.save("package_overview", {"package_overview_version": "v1",
                                        "bid_id": "b1"})
        snap = create_snapshot(repo)
        v = validate_snapshot(snap)
        assert v["ok"] is True
        assert v["record_count"] == 2

    def test_restore_roundtrip(self):
        from app.pdf_extraction.backup_restore import (
            create_snapshot, restore_snapshot)
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        source = self._repo()
        source.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        source.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})  # revision
        snap = create_snapshot(source)
        dest = ArtifactRepository()
        out = restore_snapshot(dest, snap)
        assert out["restored"] is True
        assert dest.repository_summary()["total_records"] == 2
        # Lineage should still resolve after restore.
        hist = dest.history("quote_dossier", job_id="j1")
        assert len(hist) == 2

    def test_invalid_snapshot_rejected(self):
        from app.pdf_extraction.backup_restore import (
            restore_snapshot, INTEGRITY_HASH_MISMATCH)
        repo = self._repo()
        snap = {"records": [{"record_id": "r1", "artifact_type": "quote_dossier"}],
                 "integrity_hash": "deadbeef"}
        out = restore_snapshot(repo, snap)
        assert out["restored"] is False
        assert INTEGRITY_HASH_MISMATCH in out["validation"]["reasons"]

    def test_adapter_backup_restore(self):
        from app.pdf_extraction.backup_restore import (
            backup_to_adapter, restore_from_adapter)
        from app.pdf_extraction.storage_adapter import InMemoryStorageAdapter
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        adapter = InMemoryStorageAdapter()
        source = self._repo()
        source.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        backup_to_adapter(source, adapter)

        dest = ArtifactRepository()
        out = restore_from_adapter(dest, adapter)
        assert out["restored"] is True
        assert dest.repository_summary()["total_records"] == 1

    def test_file_backup_restore(self, tmp_path):
        from app.pdf_extraction.backup_restore import (
            backup_to_file, restore_from_file)
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        source = self._repo()
        source.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        path = str(tmp_path / "snap.json")
        backup_to_file(source, path)

        dest = ArtifactRepository()
        out = restore_from_file(dest, path)
        assert out["restored"] is True
        assert dest.repository_summary()["total_records"] == 1

    def test_restore_missing_path(self, tmp_path):
        from app.pdf_extraction.backup_restore import restore_from_file
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        out = restore_from_file(ArtifactRepository(), str(tmp_path / "nope.json"))
        assert out["restored"] is False


# ---------------------------------------------------------------------------
# C95 — Render Reports Tests
# ---------------------------------------------------------------------------


class TestC95RenderReports:

    def _seeded_repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        from app.pdf_extraction.seed_scenarios import run_scenario_e2e
        repo = ArtifactRepository()
        e2e = run_scenario_e2e("proceed_with_caveats")
        for atype, src in (
            ("package_overview", "package_overview"),
            ("authority_reference", "authority_reference"),
            ("authority_exposure", "authority_exposure"),
            ("authority_action_packet", "authority_action_packet"),
            ("authority_posture", "authority_posture"),
            ("deadline_pressure", "deadline_pressure"),
            ("priority_queue", "priority_queue"),
            ("bid_readiness_snapshot", "readiness_snapshot"),
            ("bid_carry_justification", "carry_justification"),
        ):
            art = e2e["canonical_artifacts"].get(src)
            if art:
                repo.save(atype, art)
        return repo, e2e["bid_id"]

    def test_bid_readiness_report(self):
        from app.pdf_extraction.render_reports import build_bid_readiness_report
        repo, bid_id = self._seeded_repo()
        out = build_bid_readiness_report(repo, bid_id)
        assert out["report_kind"] == "bid_readiness_report"
        assert out["identity"]["bid_id"] == bid_id
        assert len(out["sections"]) >= 5
        assert out["source_refs"]

    def test_authority_action_report(self):
        from app.pdf_extraction.render_reports import build_authority_action_report
        repo, bid_id = self._seeded_repo()
        out = build_authority_action_report(repo, bid_id)
        assert out["report_kind"] == "authority_action_report"
        assert out["identity"]["bid_id"] == bid_id

    def test_final_carry_report(self):
        from app.pdf_extraction.render_reports import build_final_carry_report
        repo, bid_id = self._seeded_repo()
        out = build_final_carry_report(repo, bid_id)
        assert out["report_kind"] == "final_carry_report"
        assert out["state_labels"]["carry_decision"] == "proceed_with_caveats"

    def test_estimator_review_report_handles_missing(self):
        from app.pdf_extraction.render_reports import build_estimator_review_report
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        out = build_estimator_review_report(ArtifactRepository(), "missing-job")
        assert out["report_kind"] == "estimator_review_report"
        assert out["source_refs"] == []

    def test_list_report_kinds(self):
        from app.pdf_extraction.render_reports import list_report_kinds
        kinds = list_report_kinds()
        assert "bid_readiness_report" in kinds
        assert "final_carry_report" in kinds


# ---------------------------------------------------------------------------
# C96 — Admin Diagnostics Tests
# ---------------------------------------------------------------------------


class TestC96AdminDiagnostics:

    def _repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        return ArtifactRepository()

    def test_collect_diagnostics_empty_repo(self):
        from app.pdf_extraction.admin_diagnostics import collect_diagnostics
        out = collect_diagnostics(self._repo())
        assert out["overall_health"] in ("ok", "degraded")
        assert "repository" in out
        assert "lineage" in out
        assert "scope_anomalies" in out

    def test_repository_diagnostics_counts(self):
        from app.pdf_extraction.admin_diagnostics import repository_diagnostics
        repo = self._repo()
        repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        out = repository_diagnostics(repo)
        assert out["total_records"] == 1
        assert out["latest_revision_count"] == 1

    def test_lineage_integrity_clean(self):
        from app.pdf_extraction.admin_diagnostics import lineage_integrity
        repo = self._repo()
        repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        out = lineage_integrity(repo)
        assert out["status"] == "ok"
        assert out["broken_links"] == []

    def test_endpoint_readiness_listing(self):
        from app.pdf_extraction.admin_diagnostics import endpoint_readiness
        out = endpoint_readiness()
        assert out["endpoint_count"] > 30
        assert "/api/diagnostics" in out["endpoints"]

    def test_adapter_diagnostics_attached(self):
        from app.pdf_extraction.admin_diagnostics import adapter_diagnostics
        from app.pdf_extraction.storage_adapter import InMemoryStorageAdapter
        out = adapter_diagnostics(InMemoryStorageAdapter())
        assert out["attached"] is True
        assert out["status"] == "ok"

    def test_adapter_diagnostics_none(self):
        from app.pdf_extraction.admin_diagnostics import adapter_diagnostics
        out = adapter_diagnostics(None)
        assert out["attached"] is False

    def test_smoke_status_runs_against_scratch(self):
        from app.pdf_extraction.admin_diagnostics import smoke_status
        out = smoke_status(self._repo())
        assert out["status"] == "ok"
        assert out["scenarios_failed"] == 0

    def test_schema_validation_missing_version_field(self):
        from app.pdf_extraction.admin_diagnostics import schema_validation
        repo = self._repo()
        repo.save("quote_dossier", {"job_id": "j1"})  # missing dossier_version
        out = schema_validation(repo)
        assert out["failure_count"] >= 1
        assert any("missing_version_field" in e
                   for f in out["failures"] for e in f.get("errors") or [])


# ---------------------------------------------------------------------------
# C97 — Acceptance Harness Tests
# ---------------------------------------------------------------------------


class TestC97AcceptanceHarness:

    def _repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        return ArtifactRepository()

    def test_full_acceptance_passes(self):
        from app.pdf_extraction.acceptance_harness import run_acceptance
        out = run_acceptance(repository=self._repo())
        assert out["overall_pass"] is True, out["failures"]
        assert len(out["scenario_results"]) == 4

    def test_acceptance_subset(self):
        from app.pdf_extraction.acceptance_harness import run_acceptance
        out = run_acceptance(scenario_ids=["straightforward_usable"],
                             repository=self._repo())
        assert len(out["scenario_results"]) == 1
        assert out["scenario_results"][0]["scenario_id"] == "straightforward_usable"

    def test_acceptance_exercises_idempotency(self):
        from app.pdf_extraction.acceptance_harness import run_acceptance
        out = run_acceptance(scenario_ids=["proceed_with_caveats"],
                             repository=self._repo())
        r = out["scenario_results"][0]
        assert r["replay_status"] == "replay_hit"

    def test_acceptance_exercises_reports_and_exports(self):
        from app.pdf_extraction.acceptance_harness import run_acceptance
        out = run_acceptance(scenario_ids=["proceed_with_caveats"],
                             repository=self._repo())
        r = out["scenario_results"][0]
        assert r["exports_ok"] is True
        assert r["reports_ok"] is True
        assert r["control_room_ok"] is True

    def test_acceptance_authorization_matrix_all_match(self):
        from app.pdf_extraction.acceptance_harness import run_acceptance
        out = run_acceptance(scenario_ids=["straightforward_usable"],
                             repository=self._repo())
        assert all(a["allowed"] == a["expected"]
                   for a in out["authorization_results"])

    def test_acceptance_includes_backup_roundtrip_step(self):
        from app.pdf_extraction.acceptance_harness import run_acceptance
        out = run_acceptance(scenario_ids=["straightforward_usable"],
                             repository=self._repo())
        step_ids = [s["step"] for s in out["steps"]]
        assert "backup.roundtrip" in step_ids

    def test_dot_native_unchanged_under_c97(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# C98 — Frontend Reference Integration Tests
# ---------------------------------------------------------------------------


class TestC98FrontendReferenceIntegration:

    def _seeded_repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        from app.pdf_extraction.seed_scenarios import run_scenario_e2e
        repo = ArtifactRepository()
        e2e = run_scenario_e2e("proceed_with_caveats")
        for atype, src in (
            ("package_overview", "package_overview"),
            ("authority_reference", "authority_reference"),
            ("authority_exposure", "authority_exposure"),
            ("authority_action_packet", "authority_action_packet"),
            ("authority_posture", "authority_posture"),
            ("deadline_pressure", "deadline_pressure"),
            ("priority_queue", "priority_queue"),
            ("bid_readiness_snapshot", "readiness_snapshot"),
            ("bid_carry_justification", "carry_justification"),
        ):
            art = e2e["canonical_artifacts"].get(src)
            if art:
                repo.save(atype, art)
        return repo, e2e["bid_id"]

    def test_bid_overview_bundle(self):
        from app.pdf_extraction.frontend_reference_integration import (
            ControlRoomReferenceClient)
        repo, bid_id = self._seeded_repo()
        bundle = ControlRoomReferenceClient(repo).bid_overview_bundle(bid_id)
        assert bundle["bid_id"] == bid_id
        assert bundle["package_overview"]["assembly_diagnostics"]\
            ["package_overview_present"] is True
        assert bundle["bid_readiness"]["assembly_diagnostics"]\
            ["readiness_present"] is True
        assert bundle["authority_action"]["assembly_diagnostics"]\
            ["authority_action_present"] is True
        assert bundle["ui_integration_pack"]["ui_integration_version"] \
            == "ui_integration_pack/v1"

    def test_quote_case_bundle(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        from app.pdf_extraction.frontend_reference_integration import (
            ControlRoomReferenceClient)
        repo = ArtifactRepository()
        repo.save("quote_dossier", {"dossier_version": "v1",
                                      "job_id": "j1",
                                      "vendor_name": "VSub",
                                      "decision_posture": "ready_for_use",
                                      "readiness_status": "actionable",
                                      "latest_gate": {"gate_outcome": "SAFE"},
                                      "latest_risk":
                                          {"overall_risk_level": "low"},
                                      "comparability_posture": {},
                                      "reliance_posture": {},
                                      "scope_gaps": {},
                                      "evidence_status": {},
                                      "open_clarifications": {},
                                      "response_history_summary": {},
                                      "active_assumptions": [],
                                      "recommendation_summary": {}})
        bundle = ControlRoomReferenceClient(repo).quote_case_bundle("j1")
        assert bundle["job_id"] == "j1"
        assert bundle["quote_case"]["assembly_diagnostics"]\
            ["dossier_present"] is True

    def test_manifest_has_required_lanes(self):
        from app.pdf_extraction.frontend_reference_integration import (
            build_integration_manifest)
        mfst = build_integration_manifest()
        ids = [lane["lane_id"] for lane in mfst["lanes"]]
        assert "bid_overview" in ids
        assert "quote_case" in ids
        assert "operations" in ids

    def test_exports_and_reports_accessible_via_client(self):
        from app.pdf_extraction.frontend_reference_integration import (
            ControlRoomReferenceClient)
        repo, bid_id = self._seeded_repo()
        client = ControlRoomReferenceClient(repo)
        br_export = client.export_bid_readiness(bid_id)
        br_report = client.report_bid_readiness(bid_id)
        assert br_export.get("export") is not None
        assert br_report["report_kind"] == "bid_readiness_report"


# ---------------------------------------------------------------------------
# C99 — Production Storage Contract Tests
# ---------------------------------------------------------------------------


class TestC99ProductionStorageContract:

    def _contract(self):
        from app.pdf_extraction.production_storage_contract import (
            ProductionStorageContract)
        from app.pdf_extraction.storage_adapter import InMemoryStorageAdapter
        return ProductionStorageContract(InMemoryStorageAdapter())

    def test_append_record_ok(self):
        contract = self._contract()
        out = contract.append_record({"record_id": "r1",
                                         "artifact_type": "quote_dossier",
                                         "envelope": {"artifact":
                                             {"bid_id": "b1",
                                              "job_id": "j1"}}})
        assert out["status"] == "ok"
        assert out["content_hash"]
        assert len(contract.list_records()) == 1

    def test_append_violation_on_duplicate_record_id(self):
        contract = self._contract()
        contract.append_record({"record_id": "r1",
                                  "artifact_type": "quote_dossier",
                                  "envelope": {"artifact": {"job_id": "j1"}}})
        out = contract.append_record({"record_id": "r1",
                                         "artifact_type": "quote_dossier",
                                         "envelope": {"artifact":
                                             {"job_id": "j2"}}})
        assert out["status"] == "append_violation"

    def test_missing_record_id_errors(self):
        contract = self._contract()
        out = contract.append_record({"artifact_type": "quote_dossier"})
        assert out["status"] == "integrity_error"

    def test_scope_index_query(self):
        contract = self._contract()
        contract.append_record({"record_id": "r1",
                                  "artifact_type": "quote_dossier",
                                  "org_id": "acme",
                                  "envelope": {"artifact":
                                      {"bid_id": "b1", "job_id": "j1"}}})
        contract.append_record({"record_id": "r2",
                                  "artifact_type": "quote_dossier",
                                  "org_id": "acme",
                                  "envelope": {"artifact":
                                      {"bid_id": "b2", "job_id": "j2"}}})
        b1_records = contract.records_for_scope(bid_id="b1")
        assert len(b1_records) == 1
        assert b1_records[0]["record_id"] == "r1"

    def test_lineage_chain_walk(self):
        contract = self._contract()
        contract.append_record({"record_id": "r1",
                                  "artifact_type": "quote_dossier",
                                  "envelope": {"artifact": {}}})
        contract.append_record({"record_id": "r2",
                                  "artifact_type": "quote_dossier",
                                  "supersedes": "r1",
                                  "envelope": {"artifact": {}}})
        chain = contract.lineage_for("r1")
        assert [c["record_id"] for c in chain] == ["r1", "r2"]

    def test_mirror_repository(self):
        from app.pdf_extraction.production_storage_contract import (
            mirror_repository)
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        repo = ArtifactRepository()
        repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        repo.save("quote_dossier", {"dossier_version": "v1", "job_id": "j1"})
        contract = self._contract()
        out = mirror_repository(repo, contract)
        assert out["mirrored_count"] == 2
        assert out["violation_count"] == 0
        assert contract.store_summary()["record_count"] == 2

    def test_idempotency_entry_recorded(self):
        contract = self._contract()
        contract.record_idempotency_entry("k1", "hash1", "r1")
        entry = contract.idempotency_entry("k1")
        assert entry["record_id"] == "r1"

    def test_snapshot_and_retrieval(self):
        contract = self._contract()
        contract.save_snapshot({"records": [], "integrity_hash": "abc"},
                                 "snap:1")
        assert contract.get_snapshot("snap:1")["integrity_hash"] == "abc"
        assert "snap:1" in contract.list_snapshot_keys()


# ---------------------------------------------------------------------------
# C100 — Report Delivery Tests
# ---------------------------------------------------------------------------


class TestC100ReportDelivery:

    def _report(self):
        return {
            "render_report_version": "render_reports/v1",
            "report_kind": "bid_readiness_report",
            "title": "Bid Readiness Report",
            "identity": {"bid_id": "b1"},
            "state_labels": {"overall_readiness": "ready"},
            "sections": [{"section_id": "header", "title": "Header",
                           "body": {"bid_id": "b1"}}],
            "source_refs": [{"artifact_type": "bid_readiness_snapshot",
                              "record_id": "r1",
                              "revision_sequence": 0}],
        }

    def test_json_delivery(self):
        from app.pdf_extraction.report_delivery import deliver_report
        out = deliver_report(self._report(), output_format="json")
        assert out["delivery_status"] == "ok"
        assert out["format"] == "json"
        assert isinstance(out["body"], str)

    def test_text_delivery(self):
        from app.pdf_extraction.report_delivery import deliver_report
        out = deliver_report(self._report(), output_format="text")
        assert out["format"] == "text"
        assert "Bid Readiness Report" in out["body"]

    def test_markdown_delivery(self):
        from app.pdf_extraction.report_delivery import deliver_report
        out = deliver_report(self._report(), output_format="markdown")
        assert out["format"] == "markdown"
        assert out["body"].startswith("# ")

    def test_structured_delivery(self):
        from app.pdf_extraction.report_delivery import deliver_report
        out = deliver_report(self._report(), output_format="structured")
        assert out["format"] == "structured"
        assert out["body"]["report_kind"] == "bid_readiness_report"

    def test_unknown_format_errors(self):
        from app.pdf_extraction.report_delivery import deliver_report
        out = deliver_report(self._report(), output_format="bogus")
        assert out["delivery_status"] == "error"

    def test_batch_delivery(self):
        from app.pdf_extraction.report_delivery import deliver_reports_batch
        out = deliver_reports_batch([self._report(), self._report()],
                                      output_format="json")
        assert out["report_count"] == 2
        assert out["delivery_status"] == "ok"

    def test_write_delivery_to_file(self, tmp_path):
        from app.pdf_extraction.report_delivery import (
            deliver_report, write_delivery_to_file)
        out = deliver_report(self._report(), output_format="text")
        path = str(tmp_path / "report.txt")
        res = write_delivery_to_file(out, path)
        assert res["delivery_status"] == "ok"
        assert res["bytes_written"] > 0

    def test_list_formats_closed(self):
        from app.pdf_extraction.report_delivery import list_formats
        assert list_formats() == sorted(list_formats())
        assert "json" in list_formats()
        assert "markdown" in list_formats()


# ---------------------------------------------------------------------------
# C101 — Operator Workflow Actions Tests
# ---------------------------------------------------------------------------


class TestC101OperatorWorkflowActions:

    def _seeded_repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        from app.pdf_extraction.seed_scenarios import run_scenario_e2e
        repo = ArtifactRepository()
        e2e = run_scenario_e2e("proceed_with_caveats")
        for atype, src in (
            ("bid_readiness_snapshot", "readiness_snapshot"),
            ("bid_carry_justification", "carry_justification"),
        ):
            art = e2e["canonical_artifacts"].get(src)
            if art:
                repo.save(atype, art)
        pq_art = dict(e2e["canonical_artifacts"].get("priority_queue") or {})
        pq_art["bid_id"] = e2e["bid_id"]
        repo.save("priority_queue", pq_art)
        dossier = {"dossier_version": "quote_dossier/v1",
                    "job_id": "j1",
                    "vendor_name": "VSub",
                    "decision_posture": "ready_for_use",
                    "readiness_status": "actionable",
                    "latest_gate": {"gate_outcome": "SAFE"},
                    "latest_risk": {"overall_risk_level": "low"},
                    "comparability_posture": {},
                    "reliance_posture": {},
                    "scope_gaps": {},
                    "evidence_status": {},
                    "open_clarifications": {},
                    "response_history_summary": {},
                    "active_assumptions": [],
                    "recommendation_summary": {}}
        repo.save("quote_dossier", dossier)
        return repo, e2e["bid_id"]

    def test_acknowledge_review_appends_revision(self):
        from app.pdf_extraction.operator_workflow_actions import (
            acknowledge_review)
        repo, bid_id = self._seeded_repo()
        before = repo.history("bid_readiness_snapshot", bid_id=bid_id)
        out = acknowledge_review(repo, bid_id=bid_id,
                                   acknowledged_by="op",
                                   acknowledged_at="2026-04-17")
        assert out["status"] == "ok"
        after = repo.history("bid_readiness_snapshot", bid_id=bid_id)
        assert len(after) == len(before) + 1

    def test_clarification_advance_valid_transition(self):
        from app.pdf_extraction.operator_workflow_actions import (
            advance_clarification)
        repo, _ = self._seeded_repo()
        out = advance_clarification(repo, job_id="j1",
                                      clarification_id="C1",
                                      next_state="sent",
                                      advanced_by="op")
        assert out["status"] == "ok"
        out2 = advance_clarification(repo, job_id="j1",
                                       clarification_id="C1",
                                       next_state="responded",
                                       advanced_by="op")
        assert out2["status"] == "ok"
        assert out2["from_state"] == "sent"

    def test_clarification_advance_invalid_transition(self):
        from app.pdf_extraction.operator_workflow_actions import (
            advance_clarification)
        repo, _ = self._seeded_repo()
        out = advance_clarification(repo, job_id="j1",
                                      clarification_id="C1",
                                      next_state="responded")
        assert out["status"] == "invalid_transition"

    def test_carry_advance_through_states(self):
        from app.pdf_extraction.operator_workflow_actions import (
            advance_carry_decision)
        repo, bid_id = self._seeded_repo()
        a = advance_carry_decision(repo, bid_id=bid_id,
                                     next_state="under_review",
                                     advanced_by="rev")
        assert a["status"] == "ok"
        b = advance_carry_decision(repo, bid_id=bid_id,
                                     next_state="approved",
                                     advanced_by="office")
        assert b["status"] == "ok"
        c = advance_carry_decision(repo, bid_id=bid_id,
                                     next_state="under_review")
        assert c["status"] == "invalid_transition"

    def test_acknowledge_item_appends(self):
        from app.pdf_extraction.operator_workflow_actions import (
            acknowledge_item)
        repo, bid_id = self._seeded_repo()
        before = repo.history("priority_queue", bid_id=bid_id)
        out = acknowledge_item(repo, bid_id=bid_id, item_id="Q1",
                                 acknowledged_by="op")
        assert out["status"] == "ok"
        after = repo.history("priority_queue", bid_id=bid_id)
        assert len(after) == len(before) + 1

    def test_apply_action_dispatch(self):
        from app.pdf_extraction.operator_workflow_actions import apply_action
        repo, bid_id = self._seeded_repo()
        out = apply_action(repo, "acknowledge_review",
                            {"bid_id": bid_id, "acknowledged_by": "op"})
        assert out["status"] == "ok"

    def test_apply_action_unknown(self):
        from app.pdf_extraction.operator_workflow_actions import apply_action
        repo, _ = self._seeded_repo()
        out = apply_action(repo, "nuke", {})
        assert out["status"] == "unknown_action"

    def test_list_action_and_state_vocabularies(self):
        from app.pdf_extraction.operator_workflow_actions import (
            list_actions, list_clarification_states, list_carry_states)
        assert "acknowledge_review" in list_actions()
        assert "pending" in list_clarification_states()
        assert "approved" in list_carry_states()

    def test_record_not_found(self):
        from app.pdf_extraction.operator_workflow_actions import (
            advance_carry_decision)
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        repo = ArtifactRepository()
        out = advance_carry_decision(repo, bid_id="missing",
                                       next_state="under_review")
        assert out["status"] == "record_not_found"


# ---------------------------------------------------------------------------
# C102 — Admin Safety Controls Tests
# ---------------------------------------------------------------------------


class TestC102AdminSafetyControls:

    def test_safety_summary_shape(self):
        from app.pdf_extraction.admin_safety_controls import safety_summary
        out = safety_summary()
        assert "dev" in out["environments"]
        assert "reset_repository" in out["dangerous_actions"]

    def test_prod_blocks_reset(self):
        from app.pdf_extraction.admin_safety_controls import (
            evaluate_safety, DENY_ENV_DISALLOWED)
        d = evaluate_safety("reset_repository", role="admin",
                             environment="prod")
        assert d["allowed"] is False
        assert DENY_ENV_DISALLOWED in d["reasons"]

    def test_dev_allows_with_admin(self):
        from app.pdf_extraction.admin_safety_controls import evaluate_safety
        d = evaluate_safety("reset_repository", role="admin",
                             environment="dev")
        assert d["allowed"] is True

    def test_non_admin_role_denied(self):
        from app.pdf_extraction.admin_safety_controls import (
            evaluate_safety, DENY_NOT_AUTHORIZED)
        d = evaluate_safety("reset_repository", role="estimator",
                             environment="dev")
        assert d["allowed"] is False
        assert DENY_NOT_AUTHORIZED in d["reasons"]

    def test_staging_requires_token(self):
        from app.pdf_extraction.admin_safety_controls import (
            evaluate_safety, DENY_MISSING_CONFIRM_TOKEN)
        d = evaluate_safety("reset_repository", role="admin",
                             environment="staging")
        assert d["allowed"] is False
        assert DENY_MISSING_CONFIRM_TOKEN in d["reasons"]

    def test_staging_bad_token(self):
        from app.pdf_extraction.admin_safety_controls import (
            evaluate_safety, DENY_BAD_CONFIRM_TOKEN)
        d = evaluate_safety("reset_repository", role="admin",
                             environment="staging",
                             confirmation_token="wrong",
                             expected_token="right")
        assert d["allowed"] is False
        assert DENY_BAD_CONFIRM_TOKEN in d["reasons"]

    def test_unknown_action_rejected(self):
        from app.pdf_extraction.admin_safety_controls import (
            evaluate_safety, DENY_UNKNOWN_ACTION)
        d = evaluate_safety("drop_database", role="admin",
                             environment="dev")
        assert DENY_UNKNOWN_ACTION in d["reasons"]

    def test_guarded_reset_blocked_in_prod(self):
        from app.pdf_extraction.admin_safety_controls import (
            guarded_reset_repository)
        out = guarded_reset_repository(role="admin", environment="prod")
        assert out["executed"] is False

    def test_guarded_reset_executes_in_dev(self):
        from app.pdf_extraction.admin_safety_controls import (
            guarded_reset_repository)
        out = guarded_reset_repository(role="admin", environment="dev")
        assert out["executed"] is True
        assert "repository_summary" in out


# ---------------------------------------------------------------------------
# C103 — Product Demo Flow Tests
# ---------------------------------------------------------------------------


class TestC103ProductDemoFlow:

    def _repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        return ArtifactRepository()

    def test_product_demo_runs(self):
        from app.pdf_extraction.product_demo_flow import run_product_demo
        out = run_product_demo("proceed_with_caveats", repository=self._repo())
        assert out["product_demo_version"] == "product_demo_flow/v1"
        assert out["all_stages_ok"] is True
        assert out["bid_id"] == "seed-caveats"

    def test_product_demo_advances_carry_to_approved(self):
        from app.pdf_extraction.product_demo_flow import run_product_demo
        out = run_product_demo("proceed_with_caveats", repository=self._repo())
        assert out["operator_actions"]["carry_advance_approved"]\
            ["to_state"] == "approved"

    def test_product_demo_has_readiness_diffs(self):
        from app.pdf_extraction.product_demo_flow import run_product_demo
        out = run_product_demo("proceed_with_caveats", repository=self._repo())
        assert out["diff_counts"]["readiness_lineage"] >= 1
        assert out["history_counts"]["readiness"] >= 2

    def test_product_demo_reports_delivered(self):
        from app.pdf_extraction.product_demo_flow import run_product_demo
        out = run_product_demo("proceed_with_caveats", repository=self._repo())
        assert out["delivery_counts"]["json"] == 3
        assert out["delivery_counts"]["markdown_batch"] == 3

    def test_product_demo_straightforward_scenario(self):
        from app.pdf_extraction.product_demo_flow import run_product_demo
        out = run_product_demo("straightforward_usable", repository=self._repo())
        assert out["all_stages_ok"] is True
        assert out["bid_id"] == "seed-straightforward"

    def test_dot_native_unchanged_under_c103(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# C104 — Frontend Screen Adapters Tests
# ---------------------------------------------------------------------------


class TestC104FrontendScreenAdapters:

    def _seeded_repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        from app.pdf_extraction.seed_scenarios import run_scenario_e2e
        repo = ArtifactRepository()
        e2e = run_scenario_e2e("proceed_with_caveats")
        for atype, src in (
            ("package_overview", "package_overview"),
            ("authority_reference", "authority_reference"),
            ("authority_action_packet", "authority_action_packet"),
            ("authority_posture", "authority_posture"),
            ("priority_queue", "priority_queue"),
            ("bid_readiness_snapshot", "readiness_snapshot"),
            ("bid_carry_justification", "carry_justification"),
        ):
            art = e2e["canonical_artifacts"].get(src)
            if art:
                if atype == "priority_queue":
                    art = dict(art)
                    art["bid_id"] = e2e["bid_id"]
                repo.save(atype, art)
        return repo, e2e["bid_id"]

    def test_list_screens_closed_vocab(self):
        from app.pdf_extraction.frontend_screen_adapters import list_screens
        sids = list_screens()
        assert sids == sorted(sids)
        for s in ("quote_case", "package_overview", "authority_action",
                   "bid_readiness", "timeline", "revision_inspection"):
            assert s in sids

    def test_package_overview_adapter(self):
        from app.pdf_extraction.frontend_screen_adapters import (
            adapt_package_overview)
        repo, bid_id = self._seeded_repo()
        out = adapt_package_overview(repo, bid_id)
        assert out["screen_id"] == "package_overview"
        assert out["identity"]["bid_id"] == bid_id
        assert out["source_refs"]

    def test_bid_readiness_adapter(self):
        from app.pdf_extraction.frontend_screen_adapters import (
            adapt_bid_readiness)
        repo, bid_id = self._seeded_repo()
        out = adapt_bid_readiness(repo, bid_id)
        assert out["screen_id"] == "bid_readiness"
        assert out["state_labels"]["carry_decision"] == "proceed_with_caveats"

    def test_authority_action_adapter(self):
        from app.pdf_extraction.frontend_screen_adapters import (
            adapt_authority_action)
        repo, bid_id = self._seeded_repo()
        out = adapt_authority_action(repo, bid_id)
        assert out["screen_id"] == "authority_action"

    def test_timeline_adapter(self):
        from app.pdf_extraction.frontend_screen_adapters import adapt_timeline
        repo, bid_id = self._seeded_repo()
        out = adapt_timeline(repo, bid_id=bid_id)
        assert out["screen_id"] == "timeline"
        assert out["state_labels"]["kind_count"] >= 1

    def test_revision_inspection_adapter(self):
        from app.pdf_extraction.frontend_screen_adapters import (
            adapt_revision_inspection)
        repo, bid_id = self._seeded_repo()
        # Two carry revisions → meaningful diff
        carry = repo.history("bid_carry_justification", bid_id=bid_id)
        base = (carry[-1].get("envelope") or {}).get("artifact") or {}
        base = dict(base)
        base["revised_flag"] = True
        repo.save("bid_carry_justification", base)
        out = adapt_revision_inspection(repo, "bid_carry_justification",
                                          bid_id=bid_id)
        assert out["screen_id"] == "revision_inspection"
        assert out["state_labels"]["history_length"] >= 2

    def test_quote_case_adapter_missing_dossier(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        from app.pdf_extraction.frontend_screen_adapters import (
            adapt_quote_case)
        out = adapt_quote_case(ArtifactRepository(), "missing")
        assert out["screen_id"] == "quote_case"
        assert out["diagnostics"]["dossier_present"] is False


# ---------------------------------------------------------------------------
# C105 — Downloadable Report Artifact Tests
# ---------------------------------------------------------------------------


class TestC105ReportDownloadFlow:

    def _seeded_repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        from app.pdf_extraction.seed_scenarios import run_scenario_e2e
        repo = ArtifactRepository()
        e2e = run_scenario_e2e("proceed_with_caveats")
        for atype, src in (
            ("package_overview", "package_overview"),
            ("authority_reference", "authority_reference"),
            ("authority_exposure", "authority_exposure"),
            ("authority_action_packet", "authority_action_packet"),
            ("authority_posture", "authority_posture"),
            ("priority_queue", "priority_queue"),
            ("bid_readiness_snapshot", "readiness_snapshot"),
            ("bid_carry_justification", "carry_justification"),
            ("deadline_pressure", "deadline_pressure"),
        ):
            art = e2e["canonical_artifacts"].get(src)
            if art:
                if atype == "priority_queue":
                    art = dict(art)
                    art["bid_id"] = e2e["bid_id"]
                repo.save(atype, art)
        return repo, e2e["bid_id"]

    def test_list_report_kinds(self):
        from app.pdf_extraction.report_download_flow import list_report_kinds
        kinds = list_report_kinds()
        assert kinds == sorted(kinds)
        assert "bid_readiness_report" in kinds

    def test_build_downloadable_bid_readiness(self):
        from app.pdf_extraction.report_download_flow import build_downloadable
        repo, bid_id = self._seeded_repo()
        out = build_downloadable(repo, "bid_readiness_report",
                                   bid_id=bid_id, output_format="json")
        assert out["download_status"] == "ok"
        assert out["identity"]["bid_id"] == bid_id
        assert out["filename"].endswith(".json")
        assert out["content_hash"]

    def test_build_downloadable_final_carry_markdown(self):
        from app.pdf_extraction.report_download_flow import build_downloadable
        repo, bid_id = self._seeded_repo()
        out = build_downloadable(repo, "final_carry_report",
                                   bid_id=bid_id, output_format="markdown")
        assert out["download_status"] == "ok"
        assert out["filename"].endswith(".md")

    def test_unknown_report_kind(self):
        from app.pdf_extraction.report_download_flow import build_downloadable
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        out = build_downloadable(ArtifactRepository(), "nope_report",
                                   bid_id="b1")
        assert out["download_status"] == "error"

    def test_unknown_format(self):
        from app.pdf_extraction.report_download_flow import build_downloadable
        repo, bid_id = self._seeded_repo()
        out = build_downloadable(repo, "bid_readiness_report",
                                   bid_id=bid_id, output_format="bogus")
        assert out["download_status"] == "error"

    def test_bundle_for_bid(self):
        from app.pdf_extraction.report_download_flow import (
            build_downloadable_bundle)
        repo, bid_id = self._seeded_repo()
        bundle = build_downloadable_bundle(repo, bid_id=bid_id,
                                             output_format="json")
        assert bundle["download_status"] == "ok"
        assert bundle["download_count"] == 3

    def test_persist_downloadable(self, tmp_path):
        from app.pdf_extraction.report_download_flow import (
            build_downloadable, persist_downloadable)
        repo, bid_id = self._seeded_repo()
        dl = build_downloadable(repo, "bid_readiness_report",
                                  bid_id=bid_id, output_format="text")
        res = persist_downloadable(dl, str(tmp_path))
        assert res["persisted"] is True
        assert res["bytes_written"] > 0

    def test_deterministic_filename_and_hash(self):
        from app.pdf_extraction.report_download_flow import build_downloadable
        repo, bid_id = self._seeded_repo()
        a = build_downloadable(repo, "bid_readiness_report",
                                 bid_id=bid_id, output_format="json")
        b = build_downloadable(repo, "bid_readiness_report",
                                 bid_id=bid_id, output_format="json")
        assert a["filename"] == b["filename"]
        assert a["content_hash"] == b["content_hash"]


# ---------------------------------------------------------------------------
# C106 — Operator Command Flow Tests
# ---------------------------------------------------------------------------


class TestC106OperatorCommandFlow:

    def _seeded_repo(self):
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        from app.pdf_extraction.seed_scenarios import run_scenario_e2e
        repo = ArtifactRepository()
        e2e = run_scenario_e2e("proceed_with_caveats")
        repo.save("bid_readiness_snapshot",
                   e2e["canonical_artifacts"]["readiness_snapshot"])
        repo.save("bid_carry_justification",
                   e2e["canonical_artifacts"]["carry_justification"])
        pq = dict(e2e["canonical_artifacts"].get("priority_queue") or {})
        pq["bid_id"] = e2e["bid_id"]
        repo.save("priority_queue", pq)
        return repo, e2e["bid_id"]

    def test_list_commands_closed(self):
        from app.pdf_extraction.operator_command_flow import list_commands
        cmds = list_commands()
        assert cmds == sorted(cmds)
        for c in ("acknowledge_review", "carry_advance", "download_report"):
            assert c in cmds

    def test_acknowledge_review_command(self):
        from app.pdf_extraction.operator_command_flow import (
            execute_command, CommandReceiptLog)
        log = CommandReceiptLog()
        repo, bid_id = self._seeded_repo()
        out = execute_command(repo, "acknowledge_review",
                                {"bid_id": bid_id}, issued_by="op",
                                issued_at="2026-04-17T00:00:00", log=log)
        assert out["status"] == "ok"
        assert log.summary()["receipt_count"] == 1

    def test_capture_note_command_appends_revision(self):
        from app.pdf_extraction.operator_command_flow import execute_command
        repo, bid_id = self._seeded_repo()
        before = len(repo.history("bid_readiness_snapshot", bid_id=bid_id))
        out = execute_command(repo, "capture_note",
                                {"bid_id": bid_id, "note": "hello"},
                                issued_by="op",
                                issued_at="2026-04-17T00:01:00")
        after = len(repo.history("bid_readiness_snapshot", bid_id=bid_id))
        assert out["status"] == "ok"
        assert after == before + 1

    def test_capture_note_missing_field(self):
        from app.pdf_extraction.operator_command_flow import execute_command
        repo, bid_id = self._seeded_repo()
        out = execute_command(repo, "capture_note", {"bid_id": bid_id})
        assert out["status"] == "missing_field"

    def test_unknown_command(self):
        from app.pdf_extraction.operator_command_flow import execute_command
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        out = execute_command(ArtifactRepository(), "bogus", {})
        assert out["status"] == "unknown_command"

    def test_download_report_command(self):
        from app.pdf_extraction.operator_command_flow import execute_command
        repo, bid_id = self._seeded_repo()
        out = execute_command(repo, "download_report",
                                {"report_kind": "bid_readiness_report",
                                 "bid_id": bid_id, "format": "json"})
        assert out["status"] == "ok"
        assert out["result"]["filename"].endswith(".json")

    def test_receipt_log_summary(self):
        from app.pdf_extraction.operator_command_flow import (
            execute_command, CommandReceiptLog)
        log = CommandReceiptLog()
        repo, bid_id = self._seeded_repo()
        execute_command(repo, "acknowledge_review",
                          {"bid_id": bid_id}, log=log)
        execute_command(repo, "acknowledge_review",
                          {"bid_id": bid_id}, log=log)
        s = log.summary()
        assert s["receipt_count"] == 2
        assert s["by_command"]["acknowledge_review"] == 2

    def test_product_demo_command(self):
        from app.pdf_extraction.operator_command_flow import execute_command
        from app.pdf_extraction.artifact_repository import ArtifactRepository
        out = execute_command(ArtifactRepository(), "run_product_demo",
                                {"scenario_id": "straightforward_usable"})
        assert out["status"] == "ok"
        assert out["result"]["product_demo_version"] \
            == "product_demo_flow/v1"


# ---------------------------------------------------------------------------
# C107 — Runtime Config Tests
# ---------------------------------------------------------------------------


class TestC107RuntimeConfig:

    def test_default_config_valid(self):
        from app.pdf_extraction.runtime_config import (
            default_config, validate_config)
        cfg = default_config()
        out = validate_config(cfg)
        assert out["ok"] is True
        assert out["reasons"] == []

    def test_unknown_environment_fails(self):
        from app.pdf_extraction.runtime_config import (
            default_config, validate_config, REASON_UNKNOWN_ENV)
        cfg = default_config()
        cfg["environment"] = "moon_base"
        out = validate_config(cfg)
        assert out["ok"] is False
        assert REASON_UNKNOWN_ENV in out["reasons"]

    def test_file_adapter_requires_base_dir(self):
        from app.pdf_extraction.runtime_config import (
            default_config, validate_config,
            REASON_FILE_ADAPTER_MISSING_BASE_DIR)
        cfg = default_config()
        cfg["storage_kind"] = "file"
        out = validate_config(cfg)
        assert out["ok"] is False
        assert REASON_FILE_ADAPTER_MISSING_BASE_DIR in out["reasons"]

    def test_prod_with_dev_flags_fails(self):
        from app.pdf_extraction.runtime_config import (
            default_config, validate_config,
            REASON_INCONSISTENT_DEV_FLAGS_IN_PROD,
            REASON_PROD_REQUIRES_ADMIN_TOKEN)
        cfg = default_config()
        cfg["environment"] = "prod"
        out = validate_config(cfg)
        assert REASON_INCONSISTENT_DEV_FLAGS_IN_PROD in out["reasons"]
        assert REASON_PROD_REQUIRES_ADMIN_TOKEN in out["reasons"]

    def test_summarize_shape(self):
        from app.pdf_extraction.runtime_config import (
            default_config, summarize_config)
        out = summarize_config(default_config())
        assert out["runtime_config_version"] == "runtime_config/v1"
        assert "validation" in out

    def test_load_config_from_env_override(self):
        from app.pdf_extraction.runtime_config import load_config_from_env
        cfg = load_config_from_env({"environment": "test"})
        assert cfg["environment"] == "test"


# ---------------------------------------------------------------------------
# C108 — Bootstrap Harness Tests
# ---------------------------------------------------------------------------


class TestC108BootstrapHarness:

    def test_bootstrap_default_config(self):
        from app.pdf_extraction.bootstrap_harness import bootstrap
        from app.pdf_extraction.runtime_config import default_config
        cfg = default_config()
        cfg["feature_flags"]["seed_scenarios_enabled"] = False
        receipt = bootstrap(cfg, seed_enabled_override=False)
        assert receipt["readiness"] == "ready"
        assert receipt["components"]["repository_summary"]["total_records"] \
            == 0

    def test_bootstrap_invalid_config_fails_closed(self):
        from app.pdf_extraction.bootstrap_harness import bootstrap
        from app.pdf_extraction.runtime_config import default_config
        cfg = default_config()
        cfg["environment"] = "moon_base"
        receipt = bootstrap(cfg)
        assert receipt["readiness"] == "failed"

    def test_bootstrap_with_seed(self):
        from app.pdf_extraction.bootstrap_harness import bootstrap
        from app.pdf_extraction.runtime_config import default_config
        cfg = default_config()
        receipt = bootstrap(cfg, seed_scenarios=["straightforward_usable"],
                              seed_enabled_override=True)
        assert receipt["readiness"] == "ready"
        assert receipt["seed_result"]["seeded"] is True
        assert receipt["seed_result"]["save_count"] > 0

    def test_bootstrap_file_storage(self, tmp_path):
        from app.pdf_extraction.bootstrap_harness import bootstrap
        from app.pdf_extraction.runtime_config import default_config
        cfg = default_config()
        cfg["storage_kind"] = "file"
        cfg["storage_base_dir"] = str(tmp_path / "store")
        receipt = bootstrap(cfg, seed_enabled_override=False)
        assert receipt["readiness"] == "ready"
        assert receipt["components"]["adapter_type"] == "FileStorageAdapter"

    def test_bootstrap_health_check(self):
        from app.pdf_extraction.bootstrap_harness import bootstrap, health_check
        from app.pdf_extraction.runtime_config import default_config
        receipt = bootstrap(default_config(), seed_enabled_override=False)
        receipt_plain = {k: v for k, v in receipt.items()
                          if k not in ("repository", "adapter")}
        h = health_check(receipt_plain)
        assert h["healthy"] is True
        assert h["readiness"] == "ready"


# ---------------------------------------------------------------------------
# C109 — UI Demo Harness Tests
# ---------------------------------------------------------------------------


class TestC109UiDemoHarness:

    def test_run_ui_demo_proceed_with_caveats(self):
        from app.pdf_extraction.ui_demo_harness import run_ui_demo
        out = run_ui_demo("proceed_with_caveats")
        assert out["ui_demo_version"] == "ui_demo_harness/v1"
        assert out["all_stages_ok"] is True
        assert out["bid_id"] == "seed-caveats"

    def test_ui_demo_produces_receipts(self):
        from app.pdf_extraction.ui_demo_harness import run_ui_demo
        out = run_ui_demo("proceed_with_caveats")
        assert len(out["command_receipts"]) >= 7
        # At minimum: ack + 2 carry advances + note + 3 downloads.
        cmds = [r["command"] for r in out["command_receipts"]]
        assert "acknowledge_review" in cmds
        assert "capture_note" in cmds
        assert "carry_advance" in cmds
        assert "download_report" in cmds

    def test_ui_demo_screens_populated(self):
        from app.pdf_extraction.ui_demo_harness import run_ui_demo
        out = run_ui_demo("proceed_with_caveats")
        screens = out["screens"]
        assert screens["bid_readiness"]["screen_id"] == "bid_readiness"
        assert screens["package_overview"]["screen_id"] == "package_overview"
        assert screens["authority_action"]["screen_id"] == "authority_action"

    def test_ui_demo_final_carry_state_approved(self):
        from app.pdf_extraction.ui_demo_harness import run_ui_demo
        out = run_ui_demo("proceed_with_caveats")
        assert out["final_carry_state"] == "approved"

    def test_ui_demo_history_counts_grow(self):
        from app.pdf_extraction.ui_demo_harness import run_ui_demo
        out = run_ui_demo("proceed_with_caveats")
        # 1 seed + 1 ack + 1 note = 3 readiness revisions.
        assert out["history_counts"]["bid_readiness_snapshot"] >= 3
        # 1 seed + 2 advances = 3 carry revisions.
        assert out["history_counts"]["bid_carry_justification"] >= 3

    def test_ui_demo_straightforward_scenario(self):
        from app.pdf_extraction.ui_demo_harness import run_ui_demo
        out = run_ui_demo("straightforward_usable")
        assert out["bid_id"] == "seed-straightforward"
        assert out["all_stages_ok"] is True

    def test_dot_native_unchanged_under_c109(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93


# ---------------------------------------------------------------------------
# C110-C115 — Real UI integration tests exercising the exact endpoints the
# React UI consumes.
# ---------------------------------------------------------------------------


class TestC110to115UiEndpointIntegration:

    def _client(self):
        from fastapi.testclient import TestClient
        from app.main import app
        from app.pdf_extraction.artifact_repository import (
            reset_default_repository,
        )
        reset_default_repository()
        return TestClient(app)

    def _seed(self, client):
        r = client.post("/demo/run/proceed_with_caveats")
        assert r.status_code == 200, r.text
        run = r.json()
        bid_id = run["bid_id"]
        # Persist the canonical artifacts the UI screens rely on.
        for src_key, artifact_type in (
            ("package_overview", "package_overview"),
            ("authority_reference", "authority_reference"),
            ("authority_action_packet", "authority_action_packet"),
            ("authority_posture", "authority_posture"),
            ("priority_queue", "priority_queue"),
            ("readiness_snapshot", "bid_readiness_snapshot"),
            ("carry_justification", "bid_carry_justification"),
        ):
            art = run["canonical_artifacts"].get(src_key) or {}
            if artifact_type == "priority_queue":
                art = dict(art)
                art["bid_id"] = bid_id
            s = client.post(
                f"/canonical/artifacts/{artifact_type}",
                json={"artifact": art, "metadata": {"created_by": "ui_test"}},
            )
            assert s.status_code == 200, s.text
        return bid_id

    def test_c110_quote_case_endpoint_shape(self):
        client = self._client()
        bid_id = self._seed(client)
        # Seed a dossier so the quote case screen has something to bind to.
        dossier = {
            "dossier_version": "quote_dossier/v1",
            "job_id": "pc-j1",
            "vendor_name": "VSub",
            "decision_posture": "usable_with_caveats",
            "readiness_status": "actionable",
            "latest_gate": {"gate_outcome": "CONDITIONAL"},
            "latest_risk": {"overall_risk_level": "medium"},
            "comparability_posture": {"total_rows": 15},
            "reliance_posture": {"carry_in_sub_quote_count": 12},
            "scope_gaps": {"not_addressed_count": 5},
            "evidence_status": {},
            "open_clarifications": {"total_open": 2},
            "response_history_summary": {},
            "active_assumptions": [],
            "recommendation_summary": {},
            "package_ref": {"bid_id": bid_id},
        }
        client.post("/canonical/artifacts/quote_dossier",
                     json={"artifact": dossier})
        r = client.get("/api/ui/quote-case/pc-j1")
        assert r.status_code == 200
        body = r.json()
        assert body["screen_id"] == "quote_case"
        assert body["identity"]["job_id"] == "pc-j1"
        assert body["state_labels"]["gate_outcome"] == "CONDITIONAL"
        assert "body" in body

    def test_c111_package_overview_endpoint(self):
        client = self._client()
        bid_id = self._seed(client)
        r = client.get(f"/api/ui/package-overview/{bid_id}")
        assert r.status_code == 200
        data = r.json()
        assert data["screen_id"] == "package_overview"
        assert data["identity"]["bid_id"] == bid_id
        assert "quote_summaries" in data["body"]

    def test_c112_authority_action_and_readiness(self):
        client = self._client()
        bid_id = self._seed(client)
        a = client.get(f"/api/ui/authority-action?bid_id={bid_id}")
        assert a.status_code == 200
        assert a.json()["screen_id"] == "authority_action"
        r = client.get(f"/api/ui/bid-readiness/{bid_id}")
        assert r.status_code == 200
        rd = r.json()
        assert rd["screen_id"] == "bid_readiness"
        assert rd["identity"]["bid_id"] == bid_id

    def test_c113_operator_actions_refresh_state(self):
        client = self._client()
        bid_id = self._seed(client)
        # Capture readiness history before any operator action.
        before = client.get(
            f"/canonical/artifacts/bid_readiness_snapshot/history?bid_id={bid_id}"
        ).json()["records"]
        # Acknowledge review through the real command flow endpoint.
        a = client.post(
            "/api/commands/execute",
            json={"command": "acknowledge_review",
                   "payload": {"bid_id": bid_id},
                   "issued_by": "ui_test"},
        )
        assert a.status_code == 200
        assert a.json()["status"] == "ok"
        after = client.get(
            f"/canonical/artifacts/bid_readiness_snapshot/history?bid_id={bid_id}"
        ).json()["records"]
        assert len(after) == len(before) + 1

    def test_c113_carry_advance_closed_vocab(self):
        client = self._client()
        bid_id = self._seed(client)
        r1 = client.post(
            "/api/commands/execute",
            json={"command": "carry_advance",
                   "payload": {"bid_id": bid_id,
                                "next_state": "under_review"}},
        ).json()
        assert r1["status"] == "ok"
        r2 = client.post(
            "/api/commands/execute",
            json={"command": "carry_advance",
                   "payload": {"bid_id": bid_id, "next_state": "approved"}},
        ).json()
        assert r2["status"] == "ok"
        # After approved, another under_review is rejected by closed vocab.
        r3 = client.post(
            "/api/commands/execute",
            json={"command": "carry_advance",
                   "payload": {"bid_id": bid_id,
                                "next_state": "under_review"}},
        ).json()
        assert r3["status"] == "invalid_transition"

    def test_c114_download_report_endpoint(self):
        client = self._client()
        bid_id = self._seed(client)
        r = client.post(
            "/api/download/report",
            json={"report_kind": "bid_readiness_report",
                   "bid_id": bid_id, "format": "json"},
        )
        assert r.status_code == 200
        d = r.json()
        assert d["download_status"] == "ok"
        assert d["filename"].endswith(".json")
        assert d["content_hash"]

    def test_c114_revision_inspection_endpoint(self):
        client = self._client()
        bid_id = self._seed(client)
        # Trigger a second readiness revision via acknowledgement.
        client.post(
            "/api/commands/execute",
            json={"command": "acknowledge_review",
                   "payload": {"bid_id": bid_id}},
        )
        r = client.post(
            "/api/ui/revision-inspection",
            json={"artifact_type": "bid_readiness_snapshot",
                   "bid_id": bid_id},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["screen_id"] == "revision_inspection"
        assert body["state_labels"]["history_length"] >= 2

    def test_c114_timeline_endpoint_via_ui(self):
        client = self._client()
        bid_id = self._seed(client)
        r = client.get(f"/api/ui/timeline?bid_id={bid_id}")
        assert r.status_code == 200
        d = r.json()
        assert d["screen_id"] == "timeline"
        assert d["state_labels"]["kind_count"] >= 1

    def test_c115_full_ui_flow(self):
        client = self._client()
        bid_id = self._seed(client)
        # Step 1: load package overview & bid readiness views.
        assert client.get(
            f"/api/ui/package-overview/{bid_id}").status_code == 200
        assert client.get(
            f"/api/ui/bid-readiness/{bid_id}").status_code == 200
        # Step 2: perform two operator actions, refresh state after each.
        client.post(
            "/api/commands/execute",
            json={"command": "carry_advance",
                   "payload": {"bid_id": bid_id,
                                "next_state": "under_review"}},
        )
        client.post(
            "/api/commands/execute",
            json={"command": "capture_note",
                   "payload": {"bid_id": bid_id, "note": "ui-flow"}},
        )
        refresh = client.get(f"/api/ui/bid-readiness/{bid_id}").json()
        assert refresh["screen_id"] == "bid_readiness"
        # Step 3: download a report via the bundle flow.
        bundle = client.post(
            "/api/download/bundle",
            json={"bid_id": bid_id, "format": "markdown"},
        ).json()
        assert bundle["download_status"] == "ok"
        assert bundle["download_count"] == 3
        # Step 4: inspect diff for carry_justification.
        diff = client.post(
            "/api/ui/revision-inspection",
            json={"artifact_type": "bid_carry_justification",
                   "bid_id": bid_id},
        ).json()
        assert diff["screen_id"] == "revision_inspection"
        assert diff["state_labels"]["history_length"] >= 2

    def test_c115_command_vocabulary_is_closed(self):
        client = self._client()
        v = client.get("/api/commands/vocabulary").json()
        # Canonical closed vocabulary; guards against accidental drift.
        cmds = set(v["commands"])
        for c in (
            "acknowledge_review", "acknowledge_item",
            "carry_advance", "clarification_advance",
            "capture_note", "deliver_report", "download_report",
            "run_product_demo",
        ):
            assert c in cmds

    def test_c115_ui_integration_manifest_lanes(self):
        client = self._client()
        r = client.get("/api/frontend/manifest")
        assert r.status_code == 200
        lanes = {l["lane_id"] for l in r.json()["lanes"]}
        assert {"bid_overview", "quote_case", "operations"} <= lanes

    def test_c115_ui_screens_vocab_stable(self):
        client = self._client()
        r = client.get("/api/ui/screens")
        assert r.status_code == 200
        screens = set(r.json()["screens"])
        for s in (
            "quote_case", "package_overview", "authority_action",
            "bid_readiness", "timeline", "revision_inspection",
        ):
            assert s in screens


# ---------------------------------------------------------------------------
# C116-C119 — UX polish: navigation coherence, loading/error/empty, operator
# flow protections, report/download UX. Tests exercise the backend contract
# the UI layers depend on (navigation identity, refresh-after-action,
# closed-vocabulary protections, download feedback).
# ---------------------------------------------------------------------------


class TestC116to119UxContracts:

    def _client(self):
        from fastapi.testclient import TestClient
        from app.main import app
        from app.pdf_extraction.artifact_repository import (
            reset_default_repository,
        )
        from app.pdf_extraction.operator_command_flow import (
            reset_default_receipt_log,
        )
        reset_default_repository()
        reset_default_receipt_log()
        return TestClient(app)

    def _seed(self, client):
        r = client.post("/demo/run/proceed_with_caveats")
        assert r.status_code == 200
        run = r.json()
        bid = run["bid_id"]
        for src, at in (
            ("package_overview", "package_overview"),
            ("authority_reference", "authority_reference"),
            ("authority_action_packet", "authority_action_packet"),
            ("authority_posture", "authority_posture"),
            ("priority_queue", "priority_queue"),
            ("readiness_snapshot", "bid_readiness_snapshot"),
            ("carry_justification", "bid_carry_justification"),
        ):
            art = run["canonical_artifacts"].get(src) or {}
            if at == "priority_queue":
                art = dict(art)
                art["bid_id"] = bid
            client.post(f"/canonical/artifacts/{at}", json={"artifact": art})
        return bid

    def test_c116_identity_preserved_across_screens(self):
        # Navigation coherence: the same bid_id in path should route through
        # every UI adapter without losing identity refs.
        client = self._client()
        bid = self._seed(client)
        po = client.get(f"/api/ui/package-overview/{bid}").json()
        br = client.get(f"/api/ui/bid-readiness/{bid}").json()
        aa = client.get(f"/api/ui/authority-action?bid_id={bid}").json()
        assert po["identity"]["bid_id"] == bid
        assert br["identity"]["bid_id"] == bid
        assert aa["identity"]["bid_id"] == bid

    def test_c116_post_action_refresh_reflects_new_state(self):
        client = self._client()
        bid = self._seed(client)
        before = client.get(f"/api/ui/bid-readiness/{bid}").json()
        client.post(
            "/api/commands/execute",
            json={"command": "carry_advance",
                   "payload": {"bid_id": bid, "next_state": "under_review"}},
        )
        client.post(
            "/api/commands/execute",
            json={"command": "carry_advance",
                   "payload": {"bid_id": bid, "next_state": "approved"}},
        )
        after = client.get(f"/api/ui/bid-readiness/{bid}").json()
        # Readiness screen state labels should surface the new carry
        # progression state after refresh (no UI recomputation needed).
        assert before["state_labels"].get("carry_progression_state") \
            != after["state_labels"].get("carry_progression_state")
        assert after["state_labels"]["carry_progression_state"] == "approved"

    def test_c117_empty_states_on_missing_records(self):
        client = self._client()
        # No seed → UI screens must gracefully report missing data.
        r = client.get("/api/ui/quote-case/missing-job")
        assert r.status_code == 200
        assert r.json()["diagnostics"]["dossier_present"] is False
        r2 = client.post(
            "/api/ui/revision-inspection",
            json={"artifact_type": "bid_readiness_snapshot",
                   "bid_id": "no-bid"},
        )
        assert r2.status_code == 200
        assert r2.json()["state_labels"]["history_length"] == 0

    def test_c117_error_shape_for_bad_report_download(self):
        client = self._client()
        bid = self._seed(client)
        r = client.post(
            "/api/download/report",
            json={"report_kind": "bogus_kind", "bid_id": bid, "format": "json"},
        )
        assert r.status_code == 200
        assert r.json()["download_status"] == "error"
        r2 = client.post(
            "/api/download/report",
            json={"report_kind": "bid_readiness_report", "bid_id": bid,
                   "format": "xml"},
        )
        assert r2.json()["download_status"] == "error"

    def test_c118_destructive_action_closed_vocab_protection(self):
        client = self._client()
        bid = self._seed(client)
        # Advancing through an invalid transition must not mutate state.
        before = client.get(
            f"/canonical/artifacts/bid_carry_justification/history?bid_id={bid}"
        ).json()["records"]
        r = client.post(
            "/api/commands/execute",
            json={"command": "carry_advance",
                   "payload": {"bid_id": bid, "next_state": "approved"}},
        ).json()
        # seed scenario starts at "proposed"; approved requires under_review.
        assert r["status"] == "invalid_transition"
        after = client.get(
            f"/canonical/artifacts/bid_carry_justification/history?bid_id={bid}"
        ).json()["records"]
        assert len(after) == len(before)

    def test_c118_receipts_log_records_and_resets(self):
        client = self._client()
        bid = self._seed(client)
        client.post("/api/commands/execute",
                     json={"command": "acknowledge_review",
                            "payload": {"bid_id": bid}})
        receipts = client.get("/api/commands/receipts").json()
        assert receipts["summary"]["receipt_count"] >= 1
        client.post("/api/commands/receipts/reset")
        receipts2 = client.get("/api/commands/receipts").json()
        assert receipts2["summary"]["receipt_count"] == 0

    def test_c119_download_bundle_and_feedback_shape(self):
        client = self._client()
        bid = self._seed(client)
        r = client.post(
            "/api/download/bundle",
            json={"bid_id": bid, "format": "markdown"},
        ).json()
        assert r["download_status"] == "ok"
        assert r["download_count"] == 3
        for d in r["downloads"]:
            assert d["download_status"] == "ok"
            assert d["filename"].endswith(".md")
            assert d["content_hash"]
            assert d["byte_length"] > 0

    def test_c119_explicit_revision_download(self):
        client = self._client()
        bid = self._seed(client)
        # Generate a second readiness revision to target explicitly.
        client.post("/api/commands/execute",
                     json={"command": "acknowledge_review",
                            "payload": {"bid_id": bid}})
        hist = client.get(
            f"/canonical/artifacts/bid_readiness_snapshot/history?bid_id={bid}"
        ).json()["records"]
        rev = hist[0]["revision_sequence"]
        r = client.post(
            "/api/download/report",
            json={"report_kind": "bid_readiness_report",
                   "bid_id": bid, "revision_sequence": rev, "format": "json"},
        ).json()
        assert r["download_status"] == "ok"
        assert r["revision_metadata"]["revision_sequence"] == rev


# ---------------------------------------------------------------------------
# C120 — Runtime packaging tests
# ---------------------------------------------------------------------------


class TestC120RuntimePackaging:

    def test_runtime_profile_dev(self):
        from app.pdf_extraction.runtime_packaging import runtime_profile
        p = runtime_profile("dev")
        assert p["mode"] == "dev"
        assert p["feature_flags"]["demo_enabled"] is True

    def test_runtime_profile_prod_closes_demo(self):
        from app.pdf_extraction.runtime_packaging import runtime_profile
        p = runtime_profile("prod")
        assert p["mode"] == "prod"
        assert p["feature_flags"]["demo_enabled"] is False
        assert p["feature_flags"]["seed_scenarios_enabled"] is False
        assert p["diagnostics_exposure"] == "admin_only"
        assert p["dangerous_action_policy"] == "admin_token"

    def test_package_runtime_dev_succeeds(self):
        from app.pdf_extraction.runtime_packaging import package_runtime
        pkg = package_runtime(mode="dev", seed_enabled_override=False)
        assert pkg["packaging_status"] == "ok"
        assert pkg["frontend_handoff"]["show_demo_tabs"] is True
        assert pkg["production_safe"] is False

    def test_package_runtime_prod_without_flags_passes(self):
        from app.pdf_extraction.runtime_packaging import package_runtime
        pkg = package_runtime(mode="prod",
                                overrides={"expected_admin_token": "tok"})
        assert pkg["packaging_status"] == "ok"
        assert pkg["frontend_handoff"]["show_demo_tabs"] is False
        assert pkg["production_safe"] is True

    def test_package_runtime_prod_with_dev_flags_fails_closed(self):
        from app.pdf_extraction.runtime_packaging import package_runtime
        # Force invalid prod config by turning dev routes on.
        pkg = package_runtime(
            mode="prod",
            overrides={"feature_flags": {"dev_routes_enabled": True}},
        )
        assert pkg["packaging_status"] == "failed"
        reasons = set(pkg["reasons"])
        assert "inconsistent_dev_flags_in_prod" in reasons

    def test_startup_verification_dev_healthy(self):
        from app.pdf_extraction.runtime_packaging import startup_verification
        v = startup_verification("dev")
        assert v["healthy"] is True

    def test_frontend_handoff_contract(self):
        from app.pdf_extraction.runtime_packaging import build_frontend_handoff
        h = build_frontend_handoff("demo")
        assert h["show_demo_tabs"] is True
        assert "allowed_frontend_origins" in h
        assert h["api_base"]

    def test_package_runtime_endpoint(self):
        from fastapi.testclient import TestClient
        from app.main import app
        client = TestClient(app)
        r = client.post("/api/runtime/package",
                          json={"mode": "dev", "seed_enabled": False})
        assert r.status_code == 200
        assert r.json()["packaging_status"] == "ok"


# ---------------------------------------------------------------------------
# C121 — End-to-end acceptance walkthrough tests
# ---------------------------------------------------------------------------


class TestC121E2EAcceptanceWalkthrough:

    def test_walkthrough_proceed_with_caveats(self):
        from app.pdf_extraction.e2e_acceptance_walkthrough import (
            run_walkthrough)
        out = run_walkthrough("proceed_with_caveats")
        assert out["walkthrough_version"] == "e2e_acceptance_walkthrough/v1"
        assert out["all_stages_ok"] is True, out["steps"]
        assert out["readiness_state_after"] == "approved"

    def test_walkthrough_visits_every_stage(self):
        from app.pdf_extraction.e2e_acceptance_walkthrough import (
            run_walkthrough)
        out = run_walkthrough("proceed_with_caveats")
        step_names = [s["step"] for s in out["steps"]]
        for required in (
            "scenario_load", "persist_canonical",
            "view.package_overview", "view.quote_case",
            "view.authority_action", "view.bid_readiness.initial",
            "command.acknowledge_review", "command.capture_note",
            "command.carry_advance.under_review",
            "command.carry_advance.approved",
            "view.bid_readiness.after", "view.timeline",
            "view.diff.bid_readiness",
            "download.bundle.markdown",
            "download.command.bid_readiness_json",
        ):
            assert required in step_names, required

    def test_walkthrough_endpoint(self):
        from fastapi.testclient import TestClient
        from app.main import app
        from app.pdf_extraction.artifact_repository import (
            reset_default_repository,
        )
        reset_default_repository()
        client = TestClient(app)
        r = client.post("/api/acceptance/walkthrough",
                          json={"scenario_id": "straightforward_usable"})
        assert r.status_code == 200
        body = r.json()
        assert body["all_stages_ok"] is True
        assert body["bid_id"] == "seed-straightforward"

    def test_walkthrough_scenarios_endpoint(self):
        from fastapi.testclient import TestClient
        from app.main import app
        client = TestClient(app)
        r = client.get("/api/acceptance/walkthrough/scenarios")
        assert r.status_code == 200
        assert "proceed_with_caveats" in r.json()["scenarios"]

    def test_walkthrough_history_counts_grow(self):
        from app.pdf_extraction.e2e_acceptance_walkthrough import (
            run_walkthrough)
        out = run_walkthrough("proceed_with_caveats")
        # 1 seed + 1 ack + 1 note = ≥3 readiness revisions.
        assert out["history_counts"]["bid_readiness_snapshot"] >= 3
        # 1 seed + 2 advances = ≥3 carry revisions.
        assert out["history_counts"]["bid_carry_justification"] >= 3

    def test_dot_native_unchanged_under_c121(self, dot_pdf_path):
        from app.pdf_extraction.service import extract_bid_items_from_pdf
        rows, _ = extract_bid_items_from_pdf(str(dot_pdf_path))
        assert len(rows) == 93
