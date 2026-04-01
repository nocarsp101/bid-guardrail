"""
Phase B — Bid ingest deterministic tests.
Tests the bid item ingestion pipeline against the Adel canonical data.
"""
from __future__ import annotations

import pytest

from app.bid_validation.ingest import ingest_bid_items, IngestError, build_canonical_mapping
from app.bid_validation.normalize import norm_header


class TestBidXlsxIngest:
    """Tests against BidItems_FinalTab_values_only.xlsx (canonical bid truth)."""

    def test_ingest_succeeds(self, bid_xlsx_path):
        """The canonical bid XLSX should ingest without errors."""
        rows, meta = ingest_bid_items(str(bid_xlsx_path))
        assert len(rows) > 0
        assert "mapping_used" in meta

    def test_row_count(self, bid_xlsx_path):
        """93 data rows in source; some may be filtered as summary/placeholder."""
        rows, meta = ingest_bid_items(str(bid_xlsx_path))
        # There are 93 data rows. The last row is all-None (skipped by reader).
        # Item 2599-9999010 appears twice (rows 71-72); both should be kept.
        assert len(rows) >= 90, f"Expected ~93 rows, got {len(rows)}"

    def test_header_mapping(self, bid_xlsx_path):
        """Verify expected canonical fields are mapped correctly."""
        rows, meta = ingest_bid_items(str(bid_xlsx_path))
        mapping = meta["mapping_used"]
        assert "item" in mapping
        assert "unit" in mapping
        assert "qty" in mapping
        assert "unit_price" in mapping
        assert "total" in mapping
        assert "description" in mapping

    def test_item_normalization(self, bid_xlsx_path):
        """DOT item numbers should be preserved (not purely numeric, so no leading-zero strip)."""
        rows, meta = ingest_bid_items(str(bid_xlsx_path))
        items = [r["item"] for r in rows]
        # First item should be 2101-0850001
        assert items[0] == "2101-0850001"

    def test_mobilization_present(self, bid_xlsx_path):
        """Mobilization line (2533-4980005) must be present in ingested rows."""
        rows, meta = ingest_bid_items(str(bid_xlsx_path))
        mob_rows = [r for r in rows if "mobilization" in str(r.get("description", "")).lower()]
        assert len(mob_rows) >= 1, "Mobilization row not found"
        mob = mob_rows[0]
        assert mob["item"] == "2533-4980005"

    def test_mobilization_value(self, bid_xlsx_path):
        """Mobilization unit_price should be 175000."""
        rows, meta = ingest_bid_items(str(bid_xlsx_path))
        mob = [r for r in rows if r.get("item") == "2533-4980005"][0]
        assert float(mob.get("unit_price", 0)) == 175000.0

    def test_duplicate_item_numbers_preserved(self, bid_xlsx_path):
        """Item 2599-9999010 appears twice (RRFB NB/SB). Both should be ingested."""
        rows, meta = ingest_bid_items(str(bid_xlsx_path))
        dup_rows = [r for r in rows if r.get("item") == "2599-9999010"]
        assert len(dup_rows) == 2, f"Expected 2 rows for 2599-9999010, got {len(dup_rows)}"

    def test_spot_check_row_values(self, bid_xlsx_path, bid_truth):
        """Spot check: first bid truth row matches ingested data."""
        rows, meta = ingest_bid_items(str(bid_xlsx_path))
        first = rows[0]
        truth_first = bid_truth[0]
        assert first["item"] == truth_first["item_no"]
        assert float(first.get("qty", 0)) == pytest.approx(truth_first["qty"], rel=1e-6)
        assert float(first.get("unit_price", 0)) == pytest.approx(truth_first["unit_price"], rel=1e-6)

    def test_no_mapping_missing(self, bid_xlsx_path):
        """No required fields should be missing from the canonical bid XLSX."""
        rows, meta = ingest_bid_items(str(bid_xlsx_path))
        assert meta.get("mapping_missing") == [] or meta.get("mapping_missing") is None or len(meta.get("mapping_missing", [])) == 0

    def test_no_mapping_ambiguous(self, bid_xlsx_path):
        """No ambiguous mappings should exist for the canonical bid XLSX."""
        rows, meta = ingest_bid_items(str(bid_xlsx_path))
        assert meta.get("mapping_ambiguous") == {} or len(meta.get("mapping_ambiguous", {})) == 0


class TestBidCsvIngest:
    """Tests against ADEL CSV without Numbers filled in.csv (structural control)."""

    def test_csv_ingest_fails_ambiguous_description(self, bid_csv_path):
        """
        The CSV has both 'Item' and 'Description' columns. Both map to
        the 'description' canonical field (ITEM and DESCRIPTION are both
        description aliases). This causes an ambiguity -> IngestError.
        This is EXPECTED and documents the known structural gap.
        """
        with pytest.raises(IngestError) as exc_info:
            ingest_bid_items(str(bid_csv_path))
        assert "ambiguous" in str(exc_info.value).lower()

    def test_csv_headers_detected(self, bid_csv_path):
        """Even though ingest fails, we can verify header detection via build_canonical_mapping."""
        import csv
        with open(str(bid_csv_path), "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
        normalized = [norm_header(h) for h in headers]
        assert "LINE" in normalized
        assert "ITEM" in normalized
        assert "QUANTITY" in normalized
        assert "UNIT" in normalized
        assert "PRICE" in normalized

    def test_csv_line_column_is_not_item_alias(self):
        """
        Verify that 'LINE' is explicitly NOT an alias for bid 'item'.
        This was deliberately removed to prevent ambiguity.
        """
        from app.bid_validation.aliases import ALIASES
        item_aliases = ALIASES["item"]
        assert "LINE" not in item_aliases
        assert "LINE NUMBER" not in item_aliases
        assert "LINE NO." not in item_aliases
