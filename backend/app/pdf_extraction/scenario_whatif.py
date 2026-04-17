"""
C54 — Scenario / what-if layer.

Deterministic scenario evaluation by toggling assumptions on resolution
rows and recomputing decision posture + risk. Base truth is never
modified; each scenario is an independent overlay.

Closed assumption types:
    - treat_as_lump_sum
    - assume_scope_covered
    - exclude_from_scope
    - accept_external_qty_as_basis
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

SCENARIO_WHATIF_VERSION = "scenario_whatif/v1"

ASSUMPTION_LUMP_SUM = "treat_as_lump_sum"
ASSUMPTION_SCOPE_COVERED = "assume_scope_covered"
ASSUMPTION_EXCLUDE_SCOPE = "exclude_from_scope"
ASSUMPTION_ACCEPT_EXTERNAL = "accept_external_qty_as_basis"

_ALL_ASSUMPTIONS = frozenset({
    ASSUMPTION_LUMP_SUM, ASSUMPTION_SCOPE_COVERED,
    ASSUMPTION_EXCLUDE_SCOPE, ASSUMPTION_ACCEPT_EXTERNAL,
})


def evaluate_whatif_scenarios(
    resolution_output: Dict[str, Any],
    scope_interpretation: Optional[Dict[str, Any]] = None,
    risk_output: Optional[Dict[str, Any]] = None,
    scenarios: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Evaluate one or more what-if scenarios.

    Each scenario in `scenarios` is shaped:
        {
            "scenario_id": "...",
            "assumptions": [
                {"row_id": "qr-p0-r0", "assumption": "treat_as_lump_sum"},
                {"row_id": "qr-p0-r1", "assumption": "accept_external_qty_as_basis"},
                ...
            ]
        }

    Returns independently evaluated scenario results without mutating
    the base resolution.
    """
    base_res = deepcopy(resolution_output or {})
    base_si = deepcopy(scope_interpretation or {})
    base_risk = deepcopy(risk_output or {})
    scenarios = scenarios or []

    results: List[Dict[str, Any]] = []
    for scenario in scenarios:
        results.append(_evaluate_one(scenario, base_res, base_si, base_risk))

    return {
        "scenario_whatif_version": SCENARIO_WHATIF_VERSION,
        "base_decision_posture": _base_posture(base_risk),
        "base_risk_level": base_risk.get("overall_risk_level"),
        "scenarios_evaluated": len(results),
        "scenario_results": results,
    }


def _evaluate_one(
    scenario: Dict[str, Any],
    base_res: Dict[str, Any],
    base_si: Dict[str, Any],
    base_risk: Dict[str, Any],
) -> Dict[str, Any]:
    scenario_id = scenario.get("scenario_id") or "unnamed"
    assumptions = scenario.get("assumptions") or []

    overlay_res = deepcopy(base_res)
    overlay_rows = overlay_res.get("resolution_rows") or []
    row_map = {r.get("normalized_row_id"): r for r in overlay_rows}

    applied: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for a in assumptions:
        rid = a.get("row_id")
        assumption = a.get("assumption")
        if assumption not in _ALL_ASSUMPTIONS:
            skipped.append({"row_id": rid, "reason": "unknown_assumption_type", "assumption": assumption})
            continue
        row = row_map.get(rid)
        if row is None:
            skipped.append({"row_id": rid, "reason": "row_not_found", "assumption": assumption})
            continue

        _apply_assumption(row, assumption)
        applied.append({"row_id": rid, "assumption": assumption})

    # Recompute summary counts from the overlaid rows.
    new_cats = _recount_categories(overlay_rows)
    new_risk_level = _recompute_risk_level(new_cats, base_risk)

    from .office_decision_packet import _derive_posture
    new_posture = _derive_posture(new_risk_level, None, [])

    return {
        "scenario_id": scenario_id,
        "assumptions_applied": applied,
        "assumptions_skipped": skipped,
        "scenario_risk_level": new_risk_level,
        "scenario_decision_posture": new_posture,
        "scenario_category_counts": new_cats,
        "base_risk_level": base_risk.get("overall_risk_level"),
        "delta_summary": {
            "assumptions_count": len(applied),
            "risk_changed": new_risk_level != base_risk.get("overall_risk_level"),
            "posture_changed": new_posture != _base_posture(base_risk),
        },
    }


def _apply_assumption(row: Dict[str, Any], assumption: str) -> None:
    """Modify an overlay row in place according to the assumption."""
    if assumption == ASSUMPTION_LUMP_SUM:
        row["resolution_category"] = "clean_match_no_resolution_needed"
        row["resolution_priority"] = "low"
    elif assumption == ASSUMPTION_SCOPE_COVERED:
        row["resolution_category"] = "clean_match_no_resolution_needed"
        row["resolution_priority"] = "low"
    elif assumption == ASSUMPTION_EXCLUDE_SCOPE:
        row["resolution_category"] = "clean_match_no_resolution_needed"
        row["resolution_priority"] = "informational"
    elif assumption == ASSUMPTION_ACCEPT_EXTERNAL:
        row["resolution_category"] = "clean_match_no_resolution_needed"
        row["resolution_priority"] = "low"


def _recount_categories(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for r in rows:
        cat = r.get("resolution_category") or "unknown"
        counts[cat] = counts.get(cat, 0) + 1
    return dict(sorted(counts.items()))


def _recompute_risk_level(cats: Dict[str, int], base_risk: Dict[str, Any]) -> str:
    """Simplified risk recomputation from category counts."""
    blocked = int(cats.get("blocked_pairing_resolution_required") or 0)
    if blocked > 0:
        return "critical"
    high_cats = (
        int(cats.get("source_conflict_review_required") or 0)
        + int(cats.get("quantity_discrepancy_review_required") or 0)
        + int(cats.get("unit_discrepancy_review_required") or 0)
        + int(cats.get("unmapped_scope_review_required") or 0)
    )
    non_comp = (
        int(cats.get("non_comparable_missing_quote_source") or 0)
        + int(cats.get("non_comparable_missing_external_source") or 0)
    )
    total = sum(cats.values())
    if high_cats > 0:
        return "high"
    if non_comp > 0 and total > 0 and (non_comp / total) >= 0.5:
        return "high"
    if non_comp > 0:
        return "medium"
    return "low"


def _base_posture(risk: Dict[str, Any]) -> Optional[str]:
    blocking = risk.get("blocking_risks") or []
    overall = risk.get("overall_risk_level") or "low"
    from .office_decision_packet import _derive_posture
    return _derive_posture(overall, None, blocking)
