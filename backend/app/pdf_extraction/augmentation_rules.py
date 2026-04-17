"""
C30 — Controlled augmentation rules.

Consumes a C29-injected reconciliation contract and applies deterministic
rules to select a `comparison_basis` per row. The original quote values
and all attached external sources are preserved verbatim. The output
adds:

    - comparison_basis: closed vocabulary
    - augmentation_reason: templated, closed rule_id set
    - effective_comparison_values: {qty, unit} (never overrides raw values)
    - source_conflict_status: "none" | "conflict"
    - augmentation_flags: closed list of structural flags
    - augmentation_rule_trace: explicit rule decision path

Closed vocabulary for comparison_basis:

    quote_native
        Quote-extracted qty AND unit are both present. External sources
        are visible but NOT used for the comparison basis.

    dot_augmented
        Quote qty/unit are both None. Exactly one external source exists
        (any source type) with at least one of qty/unit defined. Effective
        comparison values are that source's qty/unit.

    conflicted_sources
        Quote qty/unit are both None. MORE THAN ONE external source
        exists and the sources disagree on qty or unit. Effective values
        are NOT set; the row is deliberately left non-comparable so that
        C31 can surface the conflict for review.

    unavailable
        Quote qty/unit are both None and there are zero external sources.
        No basis can be chosen. Row is deliberately non-comparable.

    not_applicable
        Row is unmapped, ambiguous, or blocked. Augmentation does not
        apply. Existing semantics are preserved.

    quote_native_with_external_reference
        Quote qty AND unit are both present AND at least one external
        source exists. Effective comparison values are the quote values;
        the external sources remain visible as references but are NOT
        used for the basis. Conflicts between external and quote values
        are surfaced in augmentation_flags without overriding.

Hard rules:
    - Never silently overwrite quote-extracted values.
    - Never auto-resolve multiple external sources.
    - Never invent comparison values for unmapped/blocked/ambiguous rows.
    - Never change mapping outcomes, comparison_status, discrepancy_class,
      packet_status.
    - Always produce a deterministic basis decision for every row.
    - Source "agreement" uses canonical equality only (trimmed upper
      for units; direct numeric equality with a 0.5% tolerance for qty).
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional, Tuple

AUGMENTATION_RULES_VERSION = "augmentation_rules/v1"

# Comparison-basis vocabulary (closed).
BASIS_QUOTE_NATIVE = "quote_native"
BASIS_QUOTE_NATIVE_WITH_EXTERNAL = "quote_native_with_external_reference"
BASIS_DOT_AUGMENTED = "dot_augmented"
BASIS_CONFLICTED_SOURCES = "conflicted_sources"
BASIS_UNAVAILABLE = "unavailable"
BASIS_NOT_APPLICABLE = "not_applicable"

# Source-conflict vocabulary.
CONFLICT_NONE = "none"
CONFLICT_YES = "conflict"

# Augmentation flags (closed).
FLAG_QUOTE_QTY_PRESENT = "quote_qty_present"
FLAG_QUOTE_UNIT_PRESENT = "quote_unit_present"
FLAG_EXTERNAL_SOURCE_PRESENT = "external_source_present"
FLAG_EXTERNAL_AGREES_WITH_QUOTE = "external_agrees_with_quote"
FLAG_EXTERNAL_DISAGREES_WITH_QUOTE = "external_disagrees_with_quote"
FLAG_EXTERNAL_SOURCES_DISAGREE = "external_sources_disagree"
FLAG_EXTERNAL_SOURCES_AGREE = "external_sources_agree"

# Qty tolerance for equality (mirror of C16 reconciliation tolerance).
_QTY_TOLERANCE = 0.005


def apply_augmentation_rules(injected_contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply deterministic augmentation rules to a C29-injected contract.

    Returns a new contract with per-row augmentation fields added.
    Never mutates the input.
    """
    out = deepcopy(injected_contract)
    rows = out.get("reconciliation_rows") or []

    rule_counts: Dict[str, int] = {}
    for row in rows:
        basis, reason, effective, conflict, flags, rule_trace = _apply_row_rules(row)

        row["comparison_basis"] = basis
        row["augmentation_reason"] = reason
        row["effective_comparison_values"] = effective
        row["source_conflict_status"] = conflict
        row["augmentation_flags"] = flags
        row["augmentation_rule_trace"] = rule_trace

        rule_counts[basis] = rule_counts.get(basis, 0) + 1

    out["augmentation_rules_version"] = AUGMENTATION_RULES_VERSION
    out["augmentation_rules_summary"] = {
        "rows_total": len(rows),
        "basis_counts": dict(sorted(rule_counts.items())),
    }
    return out


# ---------------------------------------------------------------------------
# Per-row rule evaluation
# ---------------------------------------------------------------------------

def _apply_row_rules(row: Dict[str, Any]):
    mapping_outcome = row.get("mapping_outcome")
    comparison_status = row.get("comparison_status")
    quote_values = row.get("quote_values") or {}
    external_sources = row.get("external_quantity_sources") or []

    trace: Dict[str, Any] = {
        "rules_attempted": [],
        "quote_qty_present": quote_values.get("qty") is not None,
        "quote_unit_present": quote_values.get("unit") is not None,
        "external_source_count": len(external_sources),
    }

    # Rule R0 — non-applicable rows (unmapped/ambiguous/blocked).
    if mapping_outcome in ("unmapped", "ambiguous", "blocked") or comparison_status == "blocked":
        trace["rules_attempted"].append({
            "rule_id": "R0_not_applicable",
            "applied": True,
            "reason": "mapping_outcome_not_mapped_or_row_blocked",
        })
        return (
            BASIS_NOT_APPLICABLE,
            "mapping_outcome_not_mapped_or_row_blocked",
            None,
            CONFLICT_NONE,
            [],
            trace,
        )

    quote_qty = quote_values.get("qty")
    quote_unit = quote_values.get("unit")
    has_quote_qty = quote_qty is not None
    has_quote_unit = quote_unit is not None
    quote_has_both = has_quote_qty and has_quote_unit

    flags: List[str] = []
    if has_quote_qty:
        flags.append(FLAG_QUOTE_QTY_PRESENT)
    if has_quote_unit:
        flags.append(FLAG_QUOTE_UNIT_PRESENT)
    if external_sources:
        flags.append(FLAG_EXTERNAL_SOURCE_PRESENT)

    # Rule R1 — quote has both qty and unit; basis is quote_native.
    if quote_has_both:
        if not external_sources:
            trace["rules_attempted"].append({"rule_id": "R1_quote_native", "applied": True})
            return (
                BASIS_QUOTE_NATIVE,
                "quote_values_complete_no_external_sources",
                {"qty": quote_qty, "unit": quote_unit},
                CONFLICT_NONE,
                flags,
                trace,
            )

        # Quote complete AND external source(s) exist — quote is the basis,
        # externals are references. Surface agreement/disagreement in flags.
        agrees = _all_sources_agree_with(external_sources, quote_qty, quote_unit)
        if agrees is True:
            flags.append(FLAG_EXTERNAL_AGREES_WITH_QUOTE)
        elif agrees is False:
            flags.append(FLAG_EXTERNAL_DISAGREES_WITH_QUOTE)

        trace["rules_attempted"].append({
            "rule_id": "R1b_quote_native_with_external_reference",
            "applied": True,
            "external_agreement": agrees,
        })
        return (
            BASIS_QUOTE_NATIVE_WITH_EXTERNAL,
            "quote_values_complete_with_external_reference",
            {"qty": quote_qty, "unit": quote_unit},
            CONFLICT_NONE,
            flags,
            trace,
        )

    # Quote qty/unit missing. See if external sources can provide a basis.
    if not external_sources:
        trace["rules_attempted"].append({
            "rule_id": "R3_unavailable",
            "applied": True,
            "reason": "no_quote_values_and_no_external_sources",
        })
        return (
            BASIS_UNAVAILABLE,
            "no_quote_values_and_no_external_sources",
            None,
            CONFLICT_NONE,
            flags,
            trace,
        )

    # External sources exist but quote is missing. Filter to sources that
    # have at least one of qty/unit defined — sources with neither are
    # not useful as a basis.
    usable = [s for s in external_sources
              if s.get("qty") is not None or s.get("unit") is not None]

    if len(usable) == 0:
        trace["rules_attempted"].append({
            "rule_id": "R3b_external_sources_empty",
            "applied": True,
            "reason": "external_sources_present_but_have_no_qty_or_unit",
        })
        return (
            BASIS_UNAVAILABLE,
            "external_sources_present_but_have_no_qty_or_unit",
            None,
            CONFLICT_NONE,
            flags,
            trace,
        )

    if len(usable) == 1:
        single = usable[0]
        effective = {"qty": single.get("qty"), "unit": single.get("unit")}
        trace["rules_attempted"].append({
            "rule_id": "R2_dot_augmented_single_source",
            "applied": True,
            "source_type": single.get("source_type"),
        })
        return (
            BASIS_DOT_AUGMENTED,
            "single_external_source_used_as_basis",
            effective,
            CONFLICT_NONE,
            flags,
            trace,
        )

    # Multiple usable sources — do they agree?
    if _sources_all_agree(usable):
        # Deterministic agreement — safe to use the first source's values.
        first = usable[0]
        flags.append(FLAG_EXTERNAL_SOURCES_AGREE)
        effective = {"qty": first.get("qty"), "unit": first.get("unit")}
        trace["rules_attempted"].append({
            "rule_id": "R2b_multiple_external_sources_agree",
            "applied": True,
            "source_count": len(usable),
        })
        return (
            BASIS_DOT_AUGMENTED,
            "multiple_external_sources_agree",
            effective,
            CONFLICT_NONE,
            flags,
            trace,
        )

    flags.append(FLAG_EXTERNAL_SOURCES_DISAGREE)
    trace["rules_attempted"].append({
        "rule_id": "R4_conflicted_sources",
        "applied": True,
        "source_count": len(usable),
        "reason": "usable_external_sources_disagree",
    })
    return (
        BASIS_CONFLICTED_SOURCES,
        "multiple_external_sources_disagree",
        None,
        CONFLICT_YES,
        flags,
        trace,
    )


# ---------------------------------------------------------------------------
# Source-comparison helpers
# ---------------------------------------------------------------------------

def _canonicalize_unit(u: Any) -> Optional[str]:
    if u is None:
        return None
    return str(u).strip().upper()


def _qty_equal(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None
    try:
        fa, fb = float(a), float(b)
    except (TypeError, ValueError):
        return False
    if fa == fb:
        return True
    if fb == 0:
        return fa == 0
    return abs(fa - fb) / abs(fb) <= _QTY_TOLERANCE


def _sources_all_agree(sources: List[Dict[str, Any]]) -> bool:
    """All usable sources must have the same canonical qty AND unit."""
    if not sources:
        return True
    first = sources[0]
    first_qty, first_unit = first.get("qty"), _canonicalize_unit(first.get("unit"))
    for s in sources[1:]:
        if _canonicalize_unit(s.get("unit")) != first_unit:
            return False
        if not _qty_equal(s.get("qty"), first_qty):
            return False
    return True


def _all_sources_agree_with(
    sources: List[Dict[str, Any]],
    quote_qty: Any,
    quote_unit: Any,
) -> Optional[bool]:
    """Do all sources agree with the quote values?

    Returns True if every source matches, False if any disagrees, and
    None if no comparable information exists on any source.
    """
    qu = _canonicalize_unit(quote_unit)
    any_comparable = False
    for s in sources:
        s_qty = s.get("qty")
        s_unit = _canonicalize_unit(s.get("unit"))
        if s_qty is None and s_unit is None:
            continue
        any_comparable = True
        if s_unit is not None and qu is not None and s_unit != qu:
            return False
        if s_qty is not None and quote_qty is not None and not _qty_equal(s_qty, quote_qty):
            return False
    if not any_comparable:
        return None
    return True
