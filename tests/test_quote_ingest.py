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
        """IPSI has 14 data rows + 1 TOTAL row. Ingest should return 15 rows
        (TOTAL row is not filtered by quote ingest — it has no summary skip)."""
        rows, meta = ingest_quote_lines(str(quote_xlsx_path))
        # 15 raw rows (14 data + 1 TOTAL), but TOTAL row has item=None
        # which normalizes to "". It still gets included since quote ingest
        # does not skip summary rows.
        assert meta["rows_raw_total"] == 15

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
