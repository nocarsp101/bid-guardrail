"""
Phase B — Normalization unit tests.
Tests the header and item code normalization functions used by both
bid and quote ingest pipelines.
"""
from __future__ import annotations

import pytest

from app.bid_validation.normalize import norm_header, norm_item_code, to_num, to_bool
from app.quote_reconciliation.ingest import (
    norm_header as quote_norm_header,
    norm_item_code as quote_norm_item_code,
)


# ---------------------------------------------------------------------------
# Bid-side header normalization
# ---------------------------------------------------------------------------

class TestBidNormHeader:
    def test_basic_uppercase(self):
        assert norm_header("unit price") == "UNIT PRICE"

    def test_strip_whitespace(self):
        assert norm_header("  Item No.  ") == "ITEM NO."

    def test_collapse_internal_whitespace(self):
        assert norm_header("Bid   Quantity") == "BID QUANTITY"

    def test_newline_collapse(self):
        assert norm_header("Unit\nPrice") == "UNIT PRICE"

    def test_nbsp_handling(self):
        assert norm_header("Unit\u00a0Price") == "UNIT PRICE"

    def test_none_returns_empty(self):
        assert norm_header(None) == ""

    def test_numeric_input(self):
        assert norm_header(123) == "123"


# ---------------------------------------------------------------------------
# Quote-side header normalization
# ---------------------------------------------------------------------------

class TestQuoteNormHeader:
    """Quote norm_header also converts underscores to spaces."""

    def test_underscore_conversion(self):
        assert quote_norm_header("unit_price") == "UNIT PRICE"

    def test_basic_uppercase(self):
        assert quote_norm_header("Bid Item #") == "BID ITEM #"

    def test_per_unit(self):
        assert quote_norm_header("Per Unit") == "PER UNIT"

    def test_none_returns_empty(self):
        assert quote_norm_header(None) == ""


# ---------------------------------------------------------------------------
# Item code normalization
# ---------------------------------------------------------------------------

class TestBidNormItemCode:
    def test_strip_leading_zeros(self):
        assert norm_item_code("0010") == "10"

    def test_pure_zero(self):
        assert norm_item_code("0") == "0"

    def test_dot_item_number_preserved(self):
        assert norm_item_code("2524-6765010") == "2524-6765010"

    def test_whitespace_stripped(self):
        assert norm_item_code("  520  ") == "520"

    def test_none_returns_empty(self):
        assert norm_item_code(None) == ""

    def test_numeric_input(self):
        assert norm_item_code(520) == "520"


class TestQuoteNormItemCode:
    def test_strip_leading_zeros(self):
        assert quote_norm_item_code("0010") == "10"

    def test_numeric_520(self):
        assert quote_norm_item_code(520) == "520"

    def test_string_520(self):
        assert quote_norm_item_code("520") == "520"

    def test_none_returns_empty(self):
        assert quote_norm_item_code(None) == ""


# ---------------------------------------------------------------------------
# Unit equivalence — resolved by canonicalization (Phase C-3)
# ---------------------------------------------------------------------------

class TestUnitEquivalenceResolved:
    """
    Phase C-3: canonicalize_unit() resolves EA/EACH, LS/LUMP SUM, etc.
    Raw strings are still different, but canonicalized forms match.
    """

    def test_ea_canonicalizes_to_each(self):
        """EA and EACH canonicalize to the same value."""
        from app.utils.unit_canonicalization import canonicalize_unit
        assert canonicalize_unit("EA") == canonicalize_unit("EACH")

    def test_ls_canonicalizes_to_lump_sum(self):
        """LS and LUMP SUM canonicalize to the same value."""
        from app.utils.unit_canonicalization import canonicalize_unit
        assert canonicalize_unit("LS") == canonicalize_unit("LUMP SUM")

    def test_sy_equals_sy(self):
        """Same string still matches trivially."""
        from app.utils.unit_canonicalization import canonicalize_unit
        assert canonicalize_unit("SY") == canonicalize_unit("SY")


# ---------------------------------------------------------------------------
# to_num / to_bool
# ---------------------------------------------------------------------------

class TestToNum:
    def test_none(self):
        assert to_num(None) is None

    def test_int(self):
        assert to_num(42) == 42.0

    def test_float(self):
        assert to_num(3.14) == 3.14

    def test_string_with_comma(self):
        assert to_num("1,234.56") == 1234.56

    def test_empty_string(self):
        assert to_num("") is None

    def test_non_numeric(self):
        assert to_num("abc") is None


class TestToBool:
    def test_none(self):
        assert to_bool(None) is False

    def test_true_string(self):
        assert to_bool("yes") is True

    def test_false_string(self):
        assert to_bool("no") is False

    def test_x_is_true(self):
        assert to_bool("x") is True
