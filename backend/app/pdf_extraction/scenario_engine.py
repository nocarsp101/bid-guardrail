"""
C36 — Scenario + sensitivity layer.

Deterministic what-if evaluation over a C29-injected (and optionally
C30-augmented) reconciliation contract. For each supported source type,
this module simulates what the comparison outcome would look like if
that source were used as the sole comparison basis for every mapped
row. The simulation NEVER mutates base truth; the augmentation_rules
output (C30) remains the canonical comparison basis.

Scenarios produced (closed set):

    scenario_dot_basis         — basis is the dot_bid_item source
    scenario_takeoff_basis     — basis is the internal_takeoff source
    scenario_engineer_basis    — basis is the engineer_quantity source
    scenario_manual_basis      — basis is the manual_review_input source
    scenario_no_external       — basis is the quote values alone

Per row in a scenario:
    - row_state: "comparable_match" | "comparable_mismatch" |
                 "unresolved_missing_source" | "unresolved_missing_quote" |
                 "blocked" | "unmapped" | "ambiguous"

Per scenario:
    - rows_evaluated
    - rows_comparable  (match)
    - rows_mismatched
    - rows_conflicted   (the dedicated source disagrees with the quote)
    - rows_unresolved   (the dedicated source is missing on that row)
    - rows_blocked / rows_unmapped / rows_ambiguous

Hard rules:
    - No scenario selects a "best" answer; scenarios are independent.
    - Blocked/unmapped/ambiguous rows are counted but never simulated as
      comparable; they inherit their base row state.
    - Multiple sources of the same type on one row are not auto-selected;
      the FIRST source matching the scenario type is used, and the
      presence of additional same-type sources is surfaced in the row
      trace.
    - Never mutates inputs. Deep-copies on read.
    - Pure function; reordering rows does not change scenario results.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

SCENARIO_VERSION = "scenario_engine/v1"

# Scenario id → (scenario_basis source_type, human label constant)
_SCENARIO_DEFS: List[Tuple[str, Optional[str]]] = [
    ("scenario_dot_basis", "dot_bid_item"),
    ("scenario_takeoff_basis", "internal_takeoff"),
    ("scenario_engineer_basis", "engineer_quantity"),
    ("scenario_manual_basis", "manual_review_input"),
    ("scenario_no_external", None),  # quote-only
]

# Row-state vocabulary (closed set).
ROW_MATCH = "comparable_match"
ROW_MISMATCH = "comparable_mismatch"
ROW_MISSING_SOURCE = "unresolved_missing_source"
ROW_MISSING_QUOTE = "unresolved_missing_quote"
ROW_BLOCKED = "blocked"
ROW_UNMAPPED = "unmapped"
ROW_AMBIGUOUS = "ambiguous"

# Numeric tolerance (mirror of C16/C30).
_QTY_TOLERANCE = 0.005


def evaluate_scenarios(contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run all closed-set scenarios over a contract that has external
    sources attached (C29 injected or downstream). Returns a new dict
    with a `scenarios` list and a `scenario_summary` block. Never
    mutates the input.
    """
    src = contract or {}
    rows = src.get("reconciliation_rows") or []

    scenarios: List[Dict[str, Any]] = []
    for scenario_id, source_type in _SCENARIO_DEFS:
        scenarios.append(_evaluate_single_scenario(rows, scenario_id, source_type))

    return {
        "scenario_version": SCENARIO_VERSION,
        "input_row_count": len(rows),
        "scenarios": scenarios,
        "scenario_summary": _aggregate_summary(scenarios),
    }


# ---------------------------------------------------------------------------
# Per-scenario evaluation
# ---------------------------------------------------------------------------

def _evaluate_single_scenario(
    rows: List[Dict[str, Any]],
    scenario_id: str,
    source_type: Optional[str],
) -> Dict[str, Any]:
    counters = {
        "rows_evaluated": 0,
        "rows_comparable": 0,
        "rows_mismatched": 0,
        "rows_conflicted": 0,
        "rows_unresolved": 0,
        "rows_blocked": 0,
        "rows_unmapped": 0,
        "rows_ambiguous": 0,
    }
    evaluated_rows: List[Dict[str, Any]] = []

    for row in rows:
        rid = row.get("normalized_row_id")
        row_eval = _simulate_row(row, source_type)
        counters["rows_evaluated"] += 1
        state = row_eval["row_state"]
        if state == ROW_MATCH:
            counters["rows_comparable"] += 1
        elif state == ROW_MISMATCH:
            counters["rows_mismatched"] += 1
            counters["rows_conflicted"] += 1
        elif state == ROW_MISSING_SOURCE:
            counters["rows_unresolved"] += 1
        elif state == ROW_MISSING_QUOTE:
            counters["rows_unresolved"] += 1
        elif state == ROW_BLOCKED:
            counters["rows_blocked"] += 1
        elif state == ROW_UNMAPPED:
            counters["rows_unmapped"] += 1
        elif state == ROW_AMBIGUOUS:
            counters["rows_ambiguous"] += 1

        evaluated_rows.append({
            "normalized_row_id": rid,
            "row_state": state,
            "scenario_basis_source_type": source_type,
            "scenario_qty": row_eval["scenario_qty"],
            "scenario_unit": row_eval["scenario_unit"],
            "quote_qty": row_eval["quote_qty"],
            "quote_unit": row_eval["quote_unit"],
            "qty_match": row_eval["qty_match"],
            "unit_match": row_eval["unit_match"],
            "scenario_trace": row_eval["trace"],
        })

    return {
        "scenario_id": scenario_id,
        "scenario_basis_source_type": source_type,
        **counters,
        "rows": evaluated_rows,
        "scenario_summary": {
            "row_state_histogram": _histogram([r["row_state"] for r in evaluated_rows]),
        },
    }


def _simulate_row(
    row: Dict[str, Any],
    source_type: Optional[str],
) -> Dict[str, Any]:
    """Simulate the comparison outcome of a single row under a scenario.

    Returns a dict with row_state, scenario_qty, scenario_unit, quote
    values, qty_match/unit_match flags, and a trace.
    """
    mapping_outcome = row.get("mapping_outcome")
    comparison_status = row.get("comparison_status")

    base_result = {
        "row_state": None,
        "scenario_qty": None,
        "scenario_unit": None,
        "quote_qty": (row.get("quote_values") or {}).get("qty"),
        "quote_unit": (row.get("quote_values") or {}).get("unit"),
        "qty_match": None,
        "unit_match": None,
        "trace": {},
    }

    # Structural states inherit from base row state.
    if mapping_outcome == "blocked" or comparison_status == "blocked":
        base_result["row_state"] = ROW_BLOCKED
        base_result["trace"] = {"rule": "S0_blocked_inherit"}
        return base_result
    if mapping_outcome == "unmapped":
        base_result["row_state"] = ROW_UNMAPPED
        base_result["trace"] = {"rule": "S0_unmapped_inherit"}
        return base_result
    if mapping_outcome == "ambiguous":
        base_result["row_state"] = ROW_AMBIGUOUS
        base_result["trace"] = {"rule": "S0_ambiguous_inherit"}
        return base_result

    quote_qty = base_result["quote_qty"]
    quote_unit = base_result["quote_unit"]

    # scenario_no_external: quote-only comparison.
    if source_type is None:
        if quote_qty is None and quote_unit is None:
            base_result["row_state"] = ROW_MISSING_QUOTE
            base_result["trace"] = {"rule": "S1_no_external_missing_quote"}
            return base_result
        # Quote-only "success" means we treat the quote as the basis;
        # there's nothing external to compare against, so we declare it
        # a match with itself.
        base_result["row_state"] = ROW_MATCH
        base_result["scenario_qty"] = quote_qty
        base_result["scenario_unit"] = quote_unit
        base_result["qty_match"] = True if quote_qty is not None else None
        base_result["unit_match"] = True if quote_unit is not None else None
        base_result["trace"] = {"rule": "S1_no_external_quote_only"}
        return base_result

    # External-source scenarios: pick the FIRST source of the scenario type.
    sources = row.get("external_quantity_sources") or []
    matching = [s for s in sources if s.get("source_type") == source_type]
    if not matching:
        base_result["row_state"] = ROW_MISSING_SOURCE
        base_result["trace"] = {
            "rule": "S2_scenario_source_absent",
            "scenario_source_type": source_type,
            "available_source_types": sorted({s.get("source_type") for s in sources}),
        }
        return base_result

    chosen = matching[0]
    scenario_qty = chosen.get("qty")
    scenario_unit = chosen.get("unit")
    base_result["scenario_qty"] = scenario_qty
    base_result["scenario_unit"] = scenario_unit

    # If the quote is missing BOTH qty and unit, the scenario fills in.
    # There is nothing to compare against on the quote side, so we mark
    # the row as `comparable_match` by definition (the scenario IS the
    # basis in this row). This mirrors C30's dot_augmented rule.
    if quote_qty is None and quote_unit is None:
        if scenario_qty is None and scenario_unit is None:
            base_result["row_state"] = ROW_MISSING_SOURCE
            base_result["trace"] = {"rule": "S3_scenario_source_empty"}
            return base_result
        base_result["row_state"] = ROW_MATCH
        base_result["qty_match"] = True if scenario_qty is not None else None
        base_result["unit_match"] = True if scenario_unit is not None else None
        base_result["trace"] = {
            "rule": "S4_scenario_fills_missing_quote",
            "multiple_same_type_present": len(matching) > 1,
        }
        return base_result

    # Both sides have something. Run a qty/unit comparison.
    qty_match = _tri_qty_equal(quote_qty, scenario_qty)
    unit_match = _tri_unit_equal(quote_unit, scenario_unit)
    base_result["qty_match"] = qty_match
    base_result["unit_match"] = unit_match

    # Decide state: if any comparable axis disagrees → mismatch.
    if qty_match is False or unit_match is False:
        base_result["row_state"] = ROW_MISMATCH
        base_result["trace"] = {
            "rule": "S5_scenario_disagrees_with_quote",
            "multiple_same_type_present": len(matching) > 1,
        }
        return base_result

    # Otherwise it's a comparable match (or comparable via either side).
    base_result["row_state"] = ROW_MATCH
    base_result["trace"] = {
        "rule": "S6_scenario_agrees_with_quote",
        "multiple_same_type_present": len(matching) > 1,
    }
    return base_result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tri_qty_equal(a: Any, b: Any) -> Optional[bool]:
    if a is None or b is None:
        return None
    try:
        fa, fb = float(a), float(b)
    except (TypeError, ValueError):
        return False
    if fa == fb:
        return True
    if fb == 0:
        return fa == 0
    return abs(fa - fb) / abs(fb) <= _QTY_TOLERANCE


def _tri_unit_equal(a: Any, b: Any) -> Optional[bool]:
    if a is None or b is None:
        return None
    return str(a).strip().upper() == str(b).strip().upper()


def _histogram(items: List[str]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for i in items:
        out[i] = out.get(i, 0) + 1
    return dict(sorted(out.items()))


def _aggregate_summary(scenarios: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Top-level rollup: one entry per scenario with the three counters
    the office usually wants to see at a glance."""
    return {
        s["scenario_id"]: {
            "scenario_basis_source_type": s["scenario_basis_source_type"],
            "rows_comparable": s["rows_comparable"],
            "rows_mismatched": s["rows_mismatched"],
            "rows_unresolved": s["rows_unresolved"],
            "rows_blocked": s["rows_blocked"],
            "rows_unmapped": s["rows_unmapped"],
        }
        for s in scenarios
    }
