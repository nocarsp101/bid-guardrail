"""
Phase B — Quote ingest deterministic tests.
Tests the quote line ingestion pipeline against the IPSI canonical data.
"""
from __future__ import annotations

import pytest

from app.quote_reconciliation.ingest import (
    ingest_quote_lines,
    IngestError,
    build_canonical_mapping,
    norm_header,
)
from app.quote_reconciliation.aliases import ALIASES


class TestQuoteAliasMapping:
    """Verify IPSI headers are recognized after Phase C-1 alias expansion."""

    def test_bid_item_hash_is_recognized(self):
        """
        IPSI header 'Bid Item #' normalizes to 'BID ITEM #'.
        Phase C-1: now mapped to 'item' (line identifier).
        """
        normalized = norm_header("Bid Item #")
        assert normalized == "BID ITEM #"
        assert normalized in ALIASES["item"]

    def test_per_unit_is_recognized(self):
        """
        IPSI header 'Per Unit' normalizes to 'PER UNIT'.
        Phase C-1: now mapped to 'unit_price'.
        """
        normalized = norm_header("Per Unit")
        assert normalized == "PER UNIT"
        assert normalized in ALIASES["unit_price"]

    def test_bid_item_hash_not_in_pay_item(self):
        """'BID ITEM #' should only be in 'item', not 'pay_item' (no ambiguity)."""
        normalized = norm_header("Bid Item #")
        assert normalized not in ALIASES.get("pay_item", [])

    def test_units_is_recognized(self):
        """'Units' normalizes to 'UNITS' which IS in the unit alias list."""
        normalized = norm_header("Units")
        assert normalized == "UNITS"
        assert normalized in ALIASES["unit"]

    def test_quantity_is_recognized(self):
        """'Quantity' normalizes to 'QUANTITY' which IS in the qty alias list."""
        normalized = norm_header("Quantity")
        assert normalized == "QUANTITY"
        assert normalized in ALIASES["qty"]

    def test_description_is_recognized(self):
        """'Description' normalizes to 'DESCRIPTION' which IS in description aliases."""
        normalized = norm_header("Description")
        assert normalized == "DESCRIPTION"
        assert normalized in ALIASES["description"]

    def test_total_is_recognized(self):
        """'Total' normalizes to 'TOTAL' which IS in total aliases."""
        normalized = norm_header("Total")
        assert normalized == "TOTAL"
        assert normalized in ALIASES["total"]


class TestQuoteXlsxIngest:
    """Tests against Unorganized data for IPSI.xlsx (canonical quote truth)."""

    def test_ingest_succeeds(self, quote_xlsx_path):
        """
        Phase C-1: IPSI XLSX should now ingest successfully.
        'Bid Item #' -> item, 'Per Unit' -> unit_price.
        """
        rows, meta = ingest_quote_lines(str(quote_xlsx_path))
        assert len(rows) > 0
        assert "mapping_used" in meta

    def test_ingest_row_count(self, quote_xlsx_path):
        """IPSI has 14 data rows + 1 TOTAL row. TOTAL row is filtered as summary.
        Ingest should return 14 data rows."""
        rows, meta = ingest_quote_lines(str(quote_xlsx_path))
        assert meta["rows_raw_total"] == 15  # 15 raw rows read from file
        assert meta["rows_skipped_summary"] == 1  # TOTAL row filtered
        assert len(rows) == 14  # 14 real data rows returned

    def test_ingest_maps_item_to_line_numbers(self, quote_xlsx_path):
        """Ingested 'item' field should contain proposal line numbers (520, etc.)."""
        rows, meta = ingest_quote_lines(str(quote_xlsx_path))
        # First row: Bid Item # = 520 -> item = "520"
        assert rows[0]["item"] == "520"

    def test_ingest_maps_unit_price(self, quote_xlsx_path):
        """Ingested 'unit_price' should contain the Per Unit values."""
        rows, meta = ingest_quote_lines(str(quote_xlsx_path))
        # First row: Per Unit = 275
        assert float(rows[0]["unit_price"]) == 275.0

    def test_ingest_no_missing_required(self, quote_xlsx_path):
        """No required fields should be missing after alias expansion."""
        rows, meta = ingest_quote_lines(str(quote_xlsx_path))
        missing = meta.get("mapping_missing", [])
        assert missing == [] or len(missing) == 0

    def test_ingest_no_ambiguous(self, quote_xlsx_path):
        """No ambiguous mappings should exist."""
        rows, meta = ingest_quote_lines(str(quote_xlsx_path))
        ambiguous = meta.get("mapping_ambiguous", {})
        assert ambiguous == {} or len(ambiguous) == 0


class TestQuoteSummaryRowFilter:
    """Phase C-5: Verify summary-row filtering in quote ingest."""

    def test_total_row_filtered(self, quote_xlsx_path):
        """The TOTAL row (item=None, description='TOTAL') should not appear in output."""
        rows, _ = ingest_quote_lines(str(quote_xlsx_path))
        descs = [str(r.get("description", "")).strip().upper() for r in rows]
        assert "TOTAL" not in descs

    def test_real_data_rows_preserved(self, quote_xlsx_path):
        """All 14 real IPSI data rows should be preserved."""
        rows, _ = ingest_quote_lines(str(quote_xlsx_path))
        items = [r["item"] for r in rows if r["item"]]
        assert len(items) == 14
        assert "520" in items
        assert "690" in items

    def test_subtotal_would_be_filtered(self):
        """A row with item='' and description='SUBTOTAL' should be detected as summary."""
        from app.quote_reconciliation.ingest import _is_summary_row
        row = {"item": "", "description": "Subtotal"}
        assert _is_summary_row(row) is True

    def test_grand_total_would_be_filtered(self):
        """A row with item='' and description='Grand Total' should be detected as summary."""
        from app.quote_reconciliation.ingest import _is_summary_row
        row = {"item": "", "description": "Grand Total"}
        assert _is_summary_row(row) is True

    def test_item_with_total_in_name_not_filtered(self):
        """A row with a real item number should NOT be filtered even if desc says 'total'."""
        from app.quote_reconciliation.ingest import _is_summary_row
        row = {"item": "580", "description": "Total coverage area"}
        assert _is_summary_row(row) is False

    def test_normal_row_not_filtered(self):
        """A normal quote row should not be filtered."""
        from app.quote_reconciliation.ingest import _is_summary_row
        row = {"item": "520", "description": "Remove and Reinstall Sign"}
        assert _is_summary_row(row) is False

    def test_empty_row_not_treated_as_summary(self):
        """A fully empty row is not summary (no summary token in description)."""
        from app.quote_reconciliation.ingest import _is_summary_row
        row = {"item": "", "description": ""}
        assert _is_summary_row(row) is False


class TestQuoteTruthValues:
    """Verify the derived quote truth fixture matches expectations from mission."""

    def test_quote_has_14_rows(self, quote_truth):
        assert len(quote_truth) == 14

    def test_quote_total_is_78513_75(self, quote_truth):
        """The IPSI quote total is $78,513.75."""
        computed_total = sum(row["qty"] * row["per_unit"] for row in quote_truth)
        assert computed_total == pytest.approx(78513.75, abs=0.01)

    def test_expected_line_numbers(self, quote_truth):
        """Quote references these proposal line numbers."""
        expected = [520, 530, 540, 550, 560, 570, 580, 600, 610, 620, 630, 650, 670, 690]
        actual = [row["bid_item_num"] for row in quote_truth]
        assert actual == expected

    def test_item_580_quantity(self, quote_truth):
        """Item 580 has qty=306.9 in the quote (vs 884.25 in bid XLSX)."""
        row_580 = [r for r in quote_truth if r["bid_item_num"] == 580][0]
        assert row_580["qty"] == pytest.approx(306.9, rel=1e-6)
