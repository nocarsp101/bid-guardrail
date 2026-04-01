"""
Phase C-3 — Unit canonicalization tests.
Tests the centralized unit normalization layer.
"""
from __future__ import annotations

import pytest

from app.utils.unit_canonicalization import (
    canonicalize_unit,
    UNIT_ALIAS_TABLE,
    _REVERSE_LOOKUP,
)


# ---------------------------------------------------------------------------
# Core alias resolution
# ---------------------------------------------------------------------------

class TestCoreAliasResolution:
    """Verify the proven Adel/IPSI cases + core construction units."""

    # EA / EACH — the primary blocker from Phase C-2
    def test_ea_to_each(self):
        assert canonicalize_unit("EA") == "EACH"

    def test_each_to_each(self):
        assert canonicalize_unit("EACH") == "EACH"

    def test_each_lowercase(self):
        assert canonicalize_unit("each") == "EACH"

    def test_ea_lowercase(self):
        assert canonicalize_unit("ea") == "EACH"

    # LS / LUMP SUM — the other Phase C-2 blocker
    def test_ls_to_lump_sum(self):
        assert canonicalize_unit("LS") == "LUMP SUM"

    def test_lump_sum_to_lump_sum(self):
        assert canonicalize_unit("LUMP SUM") == "LUMP SUM"

    def test_lump_sum_mixed_case(self):
        assert canonicalize_unit("Lump Sum") == "LUMP SUM"

    # SF / SQ FT
    def test_sf_canonical(self):
        assert canonicalize_unit("SF") == "SF"

    def test_sq_ft(self):
        assert canonicalize_unit("SQ FT") == "SF"

    def test_square_foot(self):
        assert canonicalize_unit("SQUARE FOOT") == "SF"

    def test_square_feet(self):
        assert canonicalize_unit("Square Feet") == "SF"

    def test_sq_ft_with_periods(self):
        assert canonicalize_unit("SQ. FT.") == "SF"

    # SY / SQ YD
    def test_sy_canonical(self):
        assert canonicalize_unit("SY") == "SY"

    def test_sq_yd(self):
        assert canonicalize_unit("SQ YD") == "SY"

    def test_square_yard(self):
        assert canonicalize_unit("SQUARE YARD") == "SY"

    def test_square_yards(self):
        assert canonicalize_unit("square yards") == "SY"

    # LF / LINEAR FOOT
    def test_lf_canonical(self):
        assert canonicalize_unit("LF") == "LF"

    def test_linear_foot(self):
        assert canonicalize_unit("LINEAR FOOT") == "LF"

    def test_linear_feet(self):
        assert canonicalize_unit("linear feet") == "LF"

    def test_lf_with_periods(self):
        assert canonicalize_unit("L.F.") == "LF"

    # CY / CUBIC YARD
    def test_cy_canonical(self):
        assert canonicalize_unit("CY") == "CY"

    def test_cu_yd(self):
        assert canonicalize_unit("CU YD") == "CY"

    def test_cubic_yard(self):
        assert canonicalize_unit("CUBIC YARD") == "CY"

    def test_cubic_yards(self):
        assert canonicalize_unit("cubic yards") == "CY"

    def test_cu_yd_with_periods(self):
        assert canonicalize_unit("CU. YD.") == "CY"

    # TON
    def test_ton_canonical(self):
        assert canonicalize_unit("TON") == "TON"

    def test_tons_plural(self):
        assert canonicalize_unit("TONS") == "TON"

    # STA / STATION
    def test_sta_canonical(self):
        assert canonicalize_unit("STA") == "STA"

    def test_station(self):
        assert canonicalize_unit("STATION") == "STA"

    def test_stations_plural(self):
        assert canonicalize_unit("stations") == "STA"

    # ACRE
    def test_acre_canonical(self):
        assert canonicalize_unit("ACRE") == "ACRE"

    def test_acres_plural(self):
        assert canonicalize_unit("ACRES") == "ACRE"

    def test_ac_abbreviation(self):
        assert canonicalize_unit("AC") == "ACRE"

    # UNIT
    def test_unit_canonical(self):
        assert canonicalize_unit("UNIT") == "UNIT"

    def test_units_plural(self):
        assert canonicalize_unit("UNITS") == "UNIT"

    # CDAY
    def test_cday_canonical(self):
        assert canonicalize_unit("CDAY") == "CDAY"

    def test_calendar_day(self):
        assert canonicalize_unit("CALENDAR DAY") == "CDAY"

    def test_calendar_days(self):
        assert canonicalize_unit("Calendar Days") == "CDAY"


# ---------------------------------------------------------------------------
# Normalization edge cases
# ---------------------------------------------------------------------------

class TestNormalizationEdgeCases:
    """Test whitespace, punctuation, and casing normalization."""

    def test_none_returns_empty(self):
        assert canonicalize_unit(None) == ""

    def test_empty_string(self):
        assert canonicalize_unit("") == ""

    def test_whitespace_only(self):
        assert canonicalize_unit("   ") == ""

    def test_leading_trailing_whitespace(self):
        assert canonicalize_unit("  EA  ") == "EACH"

    def test_extra_internal_whitespace(self):
        assert canonicalize_unit("LUMP   SUM") == "LUMP SUM"

    def test_tab_whitespace(self):
        assert canonicalize_unit("LUMP\tSUM") == "LUMP SUM"

    def test_periods_removed(self):
        assert canonicalize_unit("L.F.") == "LF"

    def test_mixed_case_with_periods(self):
        assert canonicalize_unit("Sq. Yd.") == "SY"

    def test_numeric_value_passthrough(self):
        """Numeric-like inputs should normalize but not match any alias."""
        result = canonicalize_unit("123")
        assert result == "123"


# ---------------------------------------------------------------------------
# Fail-closed behavior
# ---------------------------------------------------------------------------

class TestFailClosed:
    """Unknown units must remain distinct and NOT silently match anything."""

    def test_unknown_unit_returned_normalized(self):
        """Unknown units are returned uppercase, but not mapped."""
        assert canonicalize_unit("FURLONGS") == "FURLONGS"

    def test_two_unknowns_are_not_equal_unless_same(self):
        """Different unknown strings should remain different."""
        assert canonicalize_unit("FURLONGS") != canonicalize_unit("RODS")

    def test_unknown_does_not_match_known(self):
        """An unknown unit must not accidentally match a known canonical."""
        assert canonicalize_unit("FURLONGS") != canonicalize_unit("LF")

    def test_truly_different_units_fail(self):
        """SF and LF are both known but different — they must NOT match."""
        assert canonicalize_unit("SF") != canonicalize_unit("LF")

    def test_cy_does_not_match_sy(self):
        assert canonicalize_unit("CY") != canonicalize_unit("SY")

    def test_ton_does_not_match_each(self):
        assert canonicalize_unit("TON") != canonicalize_unit("EACH")


# ---------------------------------------------------------------------------
# Alias table integrity
# ---------------------------------------------------------------------------

class TestAliasTableIntegrity:
    """Verify no ambiguity or structural issues in the alias table."""

    def test_no_duplicate_aliases_across_canonicals(self):
        """Each alias string should appear in exactly one canonical family."""
        seen: dict[str, str] = {}
        for canonical, aliases in UNIT_ALIAS_TABLE.items():
            for alias in aliases:
                assert alias not in seen, (
                    f"Alias '{alias}' appears in both '{seen[alias]}' and '{canonical}'"
                )
                seen[alias] = canonical

    def test_canonical_is_in_own_aliases(self):
        """Each canonical form should be a member of its own alias set."""
        for canonical, aliases in UNIT_ALIAS_TABLE.items():
            assert canonical in aliases, (
                f"Canonical '{canonical}' is not in its own alias set"
            )

    def test_reverse_lookup_covers_all_aliases(self):
        """Reverse lookup should have an entry for every alias."""
        total_aliases = sum(len(v) for v in UNIT_ALIAS_TABLE.values())
        assert len(_REVERSE_LOOKUP) == total_aliases
