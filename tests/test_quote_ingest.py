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


class TestQuoteAliasGaps:
    """Document the alias gaps that prevent IPSI quote ingestion."""

    def test_bid_item_hash_not_recognized(self):
        """
        IPSI header 'Bid Item #' normalizes to 'BID ITEM #'.
        This is NOT in any quote alias list.
        """
        normalized = norm_header("Bid Item #")
        assert normalized == "BID ITEM #"
        # Check it's not in 'item' aliases
        assert normalized not in ALIASES["item"]
        # Check it's not in 'pay_item' aliases
        assert normalized not in ALIASES.get("pay_item", [])

    def test_per_unit_not_recognized(self):
        """
        IPSI header 'Per Unit' normalizes to 'PER UNIT'.
        This is NOT in unit_price aliases.
        """
        normalized = norm_header("Per Unit")
        assert normalized == "PER UNIT"
        assert normalized not in ALIASES["unit_price"]

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

    def test_ingest_fails_missing_required_headers(self, quote_xlsx_path):
        """
        IPSI XLSX has headers: Bid Item #, Description, Units, Quantity, Per Unit, Total
        'Bid Item #' -> 'BID ITEM #' is not in 'item' aliases -> missing required 'item'
        'Per Unit' -> 'PER UNIT' is not in 'unit_price' aliases -> missing required 'unit_price'
        Ingest MUST fail with IngestError.
        """
        with pytest.raises(IngestError) as exc_info:
            ingest_quote_lines(str(quote_xlsx_path))
        error = exc_info.value
        meta = error.meta or {}
        missing = meta.get("mapping_missing", [])
        # Both 'item' and 'unit_price' should be missing
        assert "item" in missing, f"Expected 'item' in missing, got {missing}"
        assert "unit_price" in missing, f"Expected 'unit_price' in missing, got {missing}"

    def test_ingest_error_preserves_meta(self, quote_xlsx_path):
        """IngestError should carry useful diagnostic metadata."""
        with pytest.raises(IngestError) as exc_info:
            ingest_quote_lines(str(quote_xlsx_path))
        meta = exc_info.value.meta
        assert "mapping_missing" in meta
        assert "mapping_ambiguous" in meta


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
