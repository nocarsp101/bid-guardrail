"""
Adel/IPSI reconciliation deterministic tests.

Test classes:
1. End-to-end without mapping -> captures line-number mismatch (preserved)
2. Pre-normalized fixtures with correct DOT-item mapping -> tests reconciliation logic
3. Adapter unit tests -> validates line_mapping adapter in isolation
4. Adapter + real ingest -> end-to-end with adapter in the test path
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.bid_validation.ingest import ingest_bid_items
from app.quote_reconciliation.ingest import ingest_quote_lines
from app.quote_reconciliation.rules import reconcile_quote_lines_against_bid
from app.adapters.line_mapping import apply_line_number_mapping


STRUCTURED_DIR = Path(__file__).parent / "fixtures" / "adel_ipsi" / "structured"


# ---------------------------------------------------------------------------
# Helper: build pre-normalized quote rows using DOT-item mapping
# ---------------------------------------------------------------------------

def build_mapped_quote_rows(quote_truth: list, line_to_item: dict) -> list:
    """
    Construct quote rows in the normalized format expected by
    reconcile_quote_lines_against_bid(), mapping IPSI line numbers
    to DOT item numbers using the CSV-derived mapping.
    """
    ipsi_map = line_to_item["ipsi_subset_mapping"]
    rows = []
    for i, q in enumerate(quote_truth):
        line_str = str(q["bid_item_num"])
        dot_item = ipsi_map.get(line_str, "")
        rows.append({
            "_row_index": i,
            "item": dot_item,              # DOT item number for matching
            "item_raw": line_str,           # original line number
            "pay_item": None,
            "description": q["description"],
            "unit": q["units"],
            "qty": q["qty"],
            "unit_price": q["per_unit"],
            "total": q["total"],
        })
    return rows


# ---------------------------------------------------------------------------
# End-to-end: current system behavior (expected to fail matching)
# ---------------------------------------------------------------------------

class TestReconciliationEndToEndCurrentBehavior:
    """
    Feed the real bid XLSX rows + IPSI quote rows (using line numbers as item)
    into the reconciliation engine. All matches should fail because the quote
    uses line numbers (520, 530, ...) while bid uses DOT items (2524-...).
    This captures the current broken state.
    """

    def test_all_quote_lines_unmatched_with_line_numbers(self, bid_xlsx_path, quote_truth):
        """
        When quote 'item' is set to the proposal line number (520, etc.),
        none will match bid items (which are DOT numbers like 2524-6765010).
        Every quote line should be unmatched -> FAIL findings.
        """
        bid_rows, bid_meta = ingest_bid_items(str(bid_xlsx_path))

        # Simulate what would happen if quote ingest worked but used line numbers
        quote_rows = []
        for i, q in enumerate(quote_truth):
            quote_rows.append({
                "_row_index": i,
                "item": str(q["bid_item_num"]),  # line number, NOT DOT item
                "item_raw": str(q["bid_item_num"]),
                "pay_item": None,
                "description": q["description"],
                "unit": q["units"],
                "qty": q["qty"],
                "unit_price": q["per_unit"],
                "total": q["total"],
            })

        findings, summary = reconcile_quote_lines_against_bid(bid_rows, quote_rows)

        # All 14 should be unmatched
        unmatched_count = summary.get("unmatched_quote_lines_count", 0)
        assert unmatched_count == 14, (
            f"Expected all 14 quote lines unmatched (line-number mismatch), got {unmatched_count}"
        )
        assert summary.get("matched_lines_count", 0) == 0

    def test_all_findings_are_fail(self, bid_xlsx_path, quote_truth):
        """All unmatched lines produce FAIL findings."""
        bid_rows, _ = ingest_bid_items(str(bid_xlsx_path))
        quote_rows = [{
            "_row_index": i,
            "item": str(q["bid_item_num"]),
            "item_raw": str(q["bid_item_num"]),
            "pay_item": None,
            "unit": q["units"],
            "qty": q["qty"],
            "unit_price": q["per_unit"],
            "total": q["total"],
        } for i, q in enumerate(quote_truth)]

        findings, _ = reconcile_quote_lines_against_bid(bid_rows, quote_rows)
        fail_findings = [f for f in findings if f.severity == "FAIL"]
        assert len(fail_findings) == 14


# ---------------------------------------------------------------------------
# Pre-normalized: reconciliation logic with correct DOT-item mapping
# ---------------------------------------------------------------------------

class TestReconciliationWithCorrectMapping:
    """
    Feed bid XLSX rows + quote rows mapped to DOT items.
    This tests the reconciliation logic itself, independent of ingest gaps.
    """

    @pytest.fixture
    def bid_rows(self, bid_xlsx_path):
        rows, _ = ingest_bid_items(str(bid_xlsx_path))
        return rows

    @pytest.fixture
    def mapped_quote_rows(self, quote_truth, line_to_item_mapping):
        return build_mapped_quote_rows(quote_truth, line_to_item_mapping)

    def test_partial_scope_matched_count(self, bid_rows, mapped_quote_rows):
        """
        With correct DOT-item mapping, reconciliation should attempt to match
        14 quote lines. Some will match, some will fail on unit mismatch.
        """
        findings, summary = reconcile_quote_lines_against_bid(bid_rows, mapped_quote_rows)
        matched = summary.get("matched_lines_count", 0)
        unmatched = summary.get("unmatched_quote_lines_count", 0)
        # All 14 should find a candidate in the bid index (DOT items exist)
        # But unit mismatches (EA vs EACH, LS vs LUMP SUM) will cause FAIL
        assert matched + unmatched + summary.get("ambiguous_quote_lines_count", 0) <= 14

    def test_no_unit_mismatches_after_canonicalization(self, bid_rows, mapped_quote_rows):
        """
        Phase C-3: EA/EACH and LS/LUMP SUM now canonicalize to the same form.
        All 14 items should pass unit comparison with zero mismatches.
        """
        findings, summary = reconcile_quote_lines_against_bid(bid_rows, mapped_quote_rows)
        unit_mismatches = [f for f in findings if f.type == "quote_bid_unit_mismatch"]
        assert len(unit_mismatches) == 0, (
            f"Expected 0 unit mismatches after canonicalization, got {len(unit_mismatches)}"
        )

    def test_all_14_items_reach_price_comparison(self, bid_rows, mapped_quote_rows):
        """
        All 14 items should now pass unit comparison and appear in comparisons.
        """
        findings, summary = reconcile_quote_lines_against_bid(bid_rows, mapped_quote_rows)
        comparisons = summary.get("comparisons", [])
        assert len(comparisons) == 14, (
            f"Expected 14 comparisons (all items pass unit check), got {len(comparisons)}"
        )

    def test_all_quote_prices_below_bid(self, bid_rows, mapped_quote_rows):
        """
        For all correctly matched lines, quote unit_price < bid unit_price.
        No quote_unit_price_above_bid FAIL should appear.
        """
        findings, summary = reconcile_quote_lines_against_bid(bid_rows, mapped_quote_rows)
        price_above = [f for f in findings if f.type == "quote_unit_price_above_bid_unit_price"]
        assert len(price_above) == 0, (
            f"No quote prices should exceed bid prices, but found {len(price_above)}"
        )

    def test_totals_mismatch_expected(self, bid_rows, mapped_quote_rows):
        """
        Totals cross-check should show mismatch because:
        - mapped_bid_subtotal sums bid totals for matched items
        - quote_subtotal sums quote qty*unit_price
        - Prices differ, so totals differ.
        """
        findings, summary = reconcile_quote_lines_against_bid(bid_rows, mapped_quote_rows)
        assert summary.get("totals_mismatch") is True

    def test_item_580_quantity_visible_in_comparison(self, bid_rows, mapped_quote_rows):
        """
        Item 580 (DOT 2524-9325001): quote qty=306.9, bid qty=884.25.
        Should appear in comparisons with both quantities visible.
        """
        findings, summary = reconcile_quote_lines_against_bid(bid_rows, mapped_quote_rows)
        comparisons = summary.get("comparisons", [])
        item_580_comp = [
            c for c in comparisons
            if c.get("match_key_used") == "2524-9325001"
        ]
        assert len(item_580_comp) == 1
        comp = item_580_comp[0]
        assert comp["quote_qty"] == pytest.approx(306.9, rel=1e-6)
        assert comp["bid_qty"] == pytest.approx(884.25, rel=1e-6)

    def test_item_580_produces_quantity_mismatch_finding(self, bid_rows, mapped_quote_rows):
        """
        Phase C-4: Item 580 quantity divergence (306.9 vs 884.25) should
        now produce an explicit quote_bid_quantity_mismatch finding.
        """
        findings, summary = reconcile_quote_lines_against_bid(bid_rows, mapped_quote_rows)
        qty_findings = [f for f in findings if f.type == "quote_bid_quantity_mismatch"
                        and f.meta.get("quote_qty") == pytest.approx(306.9, rel=1e-6)]
        assert len(qty_findings) == 1
        f = qty_findings[0]
        assert f.severity == "WARN"
        assert f.meta["bid_qty"] == pytest.approx(884.25, rel=1e-6)
        assert f.meta["delta"] == pytest.approx(306.9 - 884.25, abs=0.01)
        assert f.meta["percent_diff"] is not None

    def test_duplicate_bid_item_2599_causes_ambiguity(self, bid_rows, mapped_quote_rows):
        """
        DOT item 2599-9999010 appears twice in bid. If any quote line
        referenced it, the reconciliation should flag ambiguity.
        (IPSI doesn't quote this item, but this documents the bid-side issue.)
        """
        bid_index_counts = {}
        for r in bid_rows:
            item = r.get("item", "")
            bid_index_counts[item] = bid_index_counts.get(item, 0) + 1
        assert bid_index_counts.get("2599-9999010", 0) == 2


# ---------------------------------------------------------------------------
# Partial scope verification
# ---------------------------------------------------------------------------

class TestPartialScopeRecognition:
    """Verify that the IPSI quote is correctly treated as partial scope."""

    def test_ipsi_covers_14_of_93_items(self, quote_truth, bid_truth):
        assert len(quote_truth) == 14
        assert len(bid_truth) == 93

    def test_quote_total_vs_full_bid_total(self, quote_truth, bid_truth):
        """
        Quote total ($78,513.75) is much less than full bid total.
        This proves it's partial scope.
        """
        quote_total = sum(r["qty"] * r["per_unit"] for r in quote_truth)
        bid_total = sum(r["total_price"] for r in bid_truth if r["total_price"] is not None)
        assert quote_total == pytest.approx(78513.75, abs=0.01)
        assert bid_total > quote_total * 5  # bid total is >> quote total
        # The system should NOT treat the difference as a quote failure

    def test_mapping_covers_expected_lines(self, line_to_item_mapping):
        """Verify the CSV-derived mapping includes all 14 IPSI lines."""
        ipsi_lines = line_to_item_mapping["ipsi_subset_lines"]
        assert len(ipsi_lines) == 14
        expected = ["520", "530", "540", "550", "560", "570", "580",
                    "600", "610", "620", "630", "650", "670", "690"]
        assert ipsi_lines == expected


# ---------------------------------------------------------------------------
# Phase C-2: Adapter unit tests
# ---------------------------------------------------------------------------

class TestLineNumberMappingAdapter:
    """Unit tests for the line_mapping adapter in isolation."""

    def test_mapped_item_replaced(self):
        """A quote row with item='520' should become item='2524-6765010'."""
        rows = [{"_row_index": 0, "item": "520", "unit": "EA", "qty": 1}]
        mapping = {"520": "2524-6765010"}
        result = apply_line_number_mapping(rows, mapping)
        assert result[0]["item"] == "2524-6765010"

    def test_original_preserved_in_item_raw(self):
        """Original line number should be preserved in item_raw."""
        rows = [{"_row_index": 0, "item": "520"}]
        mapping = {"520": "2524-6765010"}
        result = apply_line_number_mapping(rows, mapping)
        assert result[0]["item_raw"] == "520"

    def test_existing_item_raw_not_overwritten(self):
        """If item_raw already set by ingest, adapter should not overwrite it."""
        rows = [{"_row_index": 0, "item": "520", "item_raw": "520"}]
        mapping = {"520": "2524-6765010"}
        result = apply_line_number_mapping(rows, mapping)
        assert result[0]["item_raw"] == "520"
        assert result[0]["item"] == "2524-6765010"

    def test_unmapped_item_unchanged(self):
        """Items not in mapping should be left as-is (fail-closed)."""
        rows = [{"_row_index": 0, "item": "999"}]
        mapping = {"520": "2524-6765010"}
        result = apply_line_number_mapping(rows, mapping)
        assert result[0]["item"] == "999"

    def test_empty_item_unchanged(self):
        """Empty item should be left as-is."""
        rows = [{"_row_index": 0, "item": ""}]
        mapping = {"520": "2524-6765010"}
        result = apply_line_number_mapping(rows, mapping)
        assert result[0]["item"] == ""

    def test_does_not_mutate_input(self):
        """Adapter should return new list, not modify input."""
        rows = [{"_row_index": 0, "item": "520"}]
        mapping = {"520": "2524-6765010"}
        result = apply_line_number_mapping(rows, mapping)
        assert rows[0]["item"] == "520"  # original unchanged
        assert result[0]["item"] == "2524-6765010"

    def test_all_14_ipsi_lines_mapped(self, line_to_item_mapping):
        """All 14 IPSI line numbers should map to DOT items."""
        ipsi_map = line_to_item_mapping["ipsi_subset_mapping"]
        rows = [{"_row_index": i, "item": line}
                for i, line in enumerate(line_to_item_mapping["ipsi_subset_lines"])]
        result = apply_line_number_mapping(rows, ipsi_map)
        for r in result:
            assert r["item"].startswith("2"), f"Expected DOT item, got {r['item']}"
            assert "-" in r["item"]


# ---------------------------------------------------------------------------
# Phase C-2: Adapter + real ingest -> reconciliation end-to-end
# ---------------------------------------------------------------------------

class TestReconciliationWithAdapter:
    """
    End-to-end: real quote ingest (C-1) -> adapter mapping (C-2) -> reconciliation.
    This is the first time the full pipeline runs with real data and produces matches.
    """

    @pytest.fixture
    def bid_rows(self, bid_xlsx_path):
        rows, _ = ingest_bid_items(str(bid_xlsx_path))
        return rows

    @pytest.fixture
    def adapted_quote_rows(self, quote_xlsx_path, line_to_item_mapping):
        """Real ingest + adapter: the full test-path pipeline."""
        quote_rows, _ = ingest_quote_lines(str(quote_xlsx_path))
        mapping = line_to_item_mapping["full_mapping"]
        return apply_line_number_mapping(quote_rows, mapping)

    def test_adapter_converts_line_numbers_to_dot_items(self, adapted_quote_rows):
        """After adapter, item field should contain DOT item numbers, not line numbers."""
        # First real data row (520 -> 2524-6765010)
        assert adapted_quote_rows[0]["item"] == "2524-6765010"
        # Spot check another: row index 7 is line 600 -> 2527-9263217
        assert adapted_quote_rows[7]["item"] == "2527-9263217"

    def test_reconciliation_produces_matches(self, bid_rows, adapted_quote_rows):
        """After adapter, reconciliation should produce >0 matched lines."""
        findings, summary = reconcile_quote_lines_against_bid(bid_rows, adapted_quote_rows)
        matched = summary.get("matched_lines_count", 0)
        assert matched > 0, (
            f"Expected matches after adapter mapping, got {matched}"
        )

    def test_matched_count_is_14(self, bid_rows, adapted_quote_rows):
        """
        14 quote data rows + 1 TOTAL row (item='') from ingest.
        TOTAL row: unit_price=None -> caught by missing_unit_price check.
        14 data rows: all map to DOT items and find a single bid match.
        matched_lines_count includes unit-mismatched lines (by design for reporting).
        So matched_lines_count = 14.
        """
        findings, summary = reconcile_quote_lines_against_bid(bid_rows, adapted_quote_rows)
        assert summary["matched_lines_count"] == 14

    def test_no_unit_mismatches(self, bid_rows, adapted_quote_rows):
        """
        Phase C-3: EA/EACH and LS/LUMP SUM now canonicalize.
        Zero unit mismatches expected.
        """
        findings, summary = reconcile_quote_lines_against_bid(bid_rows, adapted_quote_rows)
        unit_mismatches = [f for f in findings if f.type == "quote_bid_unit_mismatch"]
        assert len(unit_mismatches) == 0

    def test_no_price_violations(self, bid_rows, adapted_quote_rows):
        """All IPSI quote unit prices are below bid unit prices."""
        findings, _ = reconcile_quote_lines_against_bid(bid_rows, adapted_quote_rows)
        price_above = [f for f in findings if f.type == "quote_unit_price_above_bid_unit_price"]
        assert len(price_above) == 0

    def test_item_580_comparison_shows_qty_divergence(self, bid_rows, adapted_quote_rows):
        """
        Item 580 (DOT 2524-9325001, SF=SF): quote qty=306.9, bid qty=884.25.
        Should appear in comparisons with both quantities visible.
        """
        findings, summary = reconcile_quote_lines_against_bid(bid_rows, adapted_quote_rows)
        comparisons = summary.get("comparisons", [])
        item_580 = [c for c in comparisons if c.get("match_key_used") == "2524-9325001"]
        assert len(item_580) == 1, "Item 580 (SF=SF) should match and appear in comparisons"
        assert item_580[0]["quote_qty"] == pytest.approx(306.9, rel=1e-6)
        assert item_580[0]["bid_qty"] == pytest.approx(884.25, rel=1e-6)

    def test_item_580_qty_mismatch_finding_exists(self, bid_rows, adapted_quote_rows):
        """Phase C-4: Item 580 should produce a quantity mismatch finding."""
        findings, _ = reconcile_quote_lines_against_bid(bid_rows, adapted_quote_rows)
        qty_findings = [f for f in findings if f.type == "quote_bid_quantity_mismatch"
                        and f.meta.get("bid_qty") == pytest.approx(884.25, rel=1e-6)]
        assert len(qty_findings) == 1
        assert qty_findings[0].severity == "WARN"

    def test_no_spurious_missing_unit_price(self, bid_rows, adapted_quote_rows):
        """
        Phase C-5: TOTAL summary row is now filtered at ingest.
        No spurious missing_unit_price finding should appear.
        """
        findings, _ = reconcile_quote_lines_against_bid(bid_rows, adapted_quote_rows)
        missing_up = [f for f in findings if f.type == "quote_line_missing_unit_price"]
        assert len(missing_up) == 0


# ---------------------------------------------------------------------------
# Phase C-4: Quantity mismatch detection — isolated unit tests
# ---------------------------------------------------------------------------

class TestQuantityMismatchDetection:
    """Test quantity mismatch finding behavior with controlled inputs."""

    @staticmethod
    def _make_bid_rows(items):
        """Build minimal bid rows for testing."""
        return [{"_row_index": i, "item": it["item"], "unit": it["unit"],
                 "qty": it["qty"], "unit_price": it["up"], "total": it.get("total")}
                for i, it in enumerate(items)]

    @staticmethod
    def _make_quote_rows(items):
        """Build minimal quote rows for testing."""
        return [{"_row_index": i, "item": it["item"], "item_raw": it["item"],
                 "pay_item": None, "unit": it["unit"], "qty": it["qty"],
                 "unit_price": it["up"], "total": it.get("total")}
                for i, it in enumerate(items)]

    def test_matching_quantities_no_finding(self):
        """Identical quantities should produce no qty mismatch finding."""
        bid = self._make_bid_rows([{"item": "X1", "unit": "EA", "qty": 10, "up": 5.0}])
        quote = self._make_quote_rows([{"item": "X1", "unit": "EA", "qty": 10, "up": 4.0}])
        findings, _ = reconcile_quote_lines_against_bid(bid, quote)
        qty_f = [f for f in findings if f.type == "quote_bid_quantity_mismatch"]
        assert len(qty_f) == 0

    def test_tiny_tolerance_no_finding(self):
        """Differences within floating-point tolerance should not trigger finding."""
        bid = self._make_bid_rows([{"item": "X1", "unit": "LF", "qty": 100.0, "up": 5.0}])
        quote = self._make_quote_rows([{"item": "X1", "unit": "LF", "qty": 100.00001, "up": 4.0}])
        findings, _ = reconcile_quote_lines_against_bid(bid, quote)
        qty_f = [f for f in findings if f.type == "quote_bid_quantity_mismatch"]
        assert len(qty_f) == 0

    def test_large_difference_triggers_finding(self):
        """A real quantity difference should produce a finding."""
        bid = self._make_bid_rows([{"item": "X1", "unit": "SF", "qty": 884.25, "up": 31.5}])
        quote = self._make_quote_rows([{"item": "X1", "unit": "SF", "qty": 306.9, "up": 25.0}])
        findings, _ = reconcile_quote_lines_against_bid(bid, quote)
        qty_f = [f for f in findings if f.type == "quote_bid_quantity_mismatch"]
        assert len(qty_f) == 1
        f = qty_f[0]
        assert f.severity == "WARN"
        assert f.meta["quote_qty"] == pytest.approx(306.9)
        assert f.meta["bid_qty"] == pytest.approx(884.25)

    def test_finding_includes_delta(self):
        """Finding meta should include computed delta."""
        bid = self._make_bid_rows([{"item": "A1", "unit": "TON", "qty": 100, "up": 10}])
        quote = self._make_quote_rows([{"item": "A1", "unit": "TON", "qty": 80, "up": 9}])
        findings, _ = reconcile_quote_lines_against_bid(bid, quote)
        qty_f = [f for f in findings if f.type == "quote_bid_quantity_mismatch"]
        assert len(qty_f) == 1
        assert qty_f[0].meta["delta"] == pytest.approx(-20.0)

    def test_finding_includes_percent_diff(self):
        """Finding meta should include percentage difference."""
        bid = self._make_bid_rows([{"item": "A1", "unit": "CY", "qty": 200, "up": 10}])
        quote = self._make_quote_rows([{"item": "A1", "unit": "CY", "qty": 150, "up": 9}])
        findings, _ = reconcile_quote_lines_against_bid(bid, quote)
        qty_f = [f for f in findings if f.type == "quote_bid_quantity_mismatch"]
        assert qty_f[0].meta["percent_diff"] == pytest.approx(-25.0)

    def test_positive_delta_quote_higher(self):
        """Quote qty > bid qty should produce positive delta."""
        bid = self._make_bid_rows([{"item": "A1", "unit": "LF", "qty": 50, "up": 10}])
        quote = self._make_quote_rows([{"item": "A1", "unit": "LF", "qty": 75, "up": 8}])
        findings, _ = reconcile_quote_lines_against_bid(bid, quote)
        qty_f = [f for f in findings if f.type == "quote_bid_quantity_mismatch"]
        assert qty_f[0].meta["delta"] == pytest.approx(25.0)
        assert qty_f[0].meta["percent_diff"] == pytest.approx(50.0)

    def test_zero_bid_qty_no_crash(self):
        """If bid qty is 0, percent_diff should be None (avoid divide-by-zero)."""
        bid = self._make_bid_rows([{"item": "A1", "unit": "EA", "qty": 0, "up": 10}])
        quote = self._make_quote_rows([{"item": "A1", "unit": "EA", "qty": 5, "up": 8}])
        findings, _ = reconcile_quote_lines_against_bid(bid, quote)
        qty_f = [f for f in findings if f.type == "quote_bid_quantity_mismatch"]
        assert len(qty_f) == 1
        assert qty_f[0].meta["percent_diff"] is None

    def test_does_not_affect_price_guardrail(self):
        """Quantity mismatch finding should coexist with price guardrail findings."""
        bid = self._make_bid_rows([{"item": "A1", "unit": "STA", "qty": 100, "up": 10}])
        quote = self._make_quote_rows([{"item": "A1", "unit": "STA", "qty": 200, "up": 15}])
        findings, _ = reconcile_quote_lines_against_bid(bid, quote)
        price_f = [f for f in findings if f.type == "quote_unit_price_above_bid_unit_price"]
        qty_f = [f for f in findings if f.type == "quote_bid_quantity_mismatch"]
        assert len(price_f) == 1  # price guardrail still fires
        assert len(qty_f) == 1    # qty mismatch also fires

    def test_unit_mismatch_blocks_qty_check(self):
        """If units don't match, qty comparison should NOT be reached."""
        bid = self._make_bid_rows([{"item": "A1", "unit": "LF", "qty": 100, "up": 10}])
        quote = self._make_quote_rows([{"item": "A1", "unit": "FURLONGS", "qty": 50, "up": 8}])
        findings, _ = reconcile_quote_lines_against_bid(bid, quote)
        qty_f = [f for f in findings if f.type == "quote_bid_quantity_mismatch"]
        assert len(qty_f) == 0  # not reached — unit mismatch exits early
