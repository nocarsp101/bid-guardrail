"""
C32 — Quantity source management layer.

Decorates an injected (C29) or augmented (C30) reconciliation contract
with deterministic source-management metadata. Every attached source
receives:

    - source_authority_tier: closed tier vocabulary
    - source_visibility_rank: deterministic integer (lower = shown first)
    - source_validation_status: "usable" | "incomplete" | "conflicted" |
                                "unknown_type"

And every row receives:

    - source_management_status: "none" | "single_source" |
                                "multiple_sources" | "conflicted_sources"
    - managed_sources: list of sources in visibility order (never drops
                       anything; the order is a display hint, not a
                       truth selection)
    - source_conflict_groups: list of grouped source indices whose
                              (qty, unit) pairs do not agree
    - source_management_summary: per-row counters

And the contract receives a top-level:
    - source_management_version
    - source_management_summary: packet-level counters including a
                                 source_type_histogram

Hard rules (all documented and tested):
    - Never selects a comparison basis. Visibility rank is orthogonal to
      truth selection; C30 augmentation_rules remains the only basis
      selector.
    - Never discards a source. All sources remain in managed_sources
      with their original values preserved.
    - Never merges disagreeing sources. Conflict groups identify rows
      where multiple usable sources disagree; the sources themselves
      stay untouched.
    - Never mutates caller inputs.
    - Authority policy is a CLOSED TABLE. Unknown source types are
      tagged "unknown_type" and ranked last.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

SOURCE_MANAGEMENT_VERSION = "quantity_source_management/v1"

# Closed source-management-status vocabulary.
SM_NONE = "none"
SM_SINGLE = "single_source"
SM_MULTIPLE = "multiple_sources"
SM_CONFLICTED = "conflicted_sources"

# Closed source-validation vocabulary.
VS_USABLE = "usable"
VS_INCOMPLETE = "incomplete"
VS_CONFLICTED = "conflicted"
VS_UNKNOWN_TYPE = "unknown_type"

# Closed authority tier vocabulary.
TIER_PRIMARY = "tier_primary"
TIER_SECONDARY = "tier_secondary"
TIER_REVIEW_INPUT = "tier_review_input"
TIER_UNKNOWN = "tier_unknown"

# Closed authority policy. Each entry:
#   source_type -> (tier, visibility_rank_base)
# Visibility rank is a deterministic integer. Lower = shown first.
# Ranks are spaced by 10 so future entries can slot between without
# breaking existing rank comparisons.
_AUTHORITY_POLICY: Dict[str, Tuple[str, int]] = {
    "dot_bid_item":      (TIER_PRIMARY, 10),
    "engineer_quantity": (TIER_PRIMARY, 20),
    "internal_takeoff":  (TIER_SECONDARY, 30),
    "manual_review_input": (TIER_REVIEW_INPUT, 40),
}

# Qty tolerance (mirror of C16/C30/C31).
_QTY_TOLERANCE = 0.005


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def manage_quantity_sources(contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add deterministic source-management metadata to a C29/C30 contract.

    Returns a new contract dict. Never mutates the input.
    """
    out = deepcopy(contract)
    rows = out.get("reconciliation_rows") or []

    packet_rows_with_sources = 0
    packet_rows_with_multiple = 0
    packet_rows_with_conflict = 0
    source_type_histogram: Dict[str, int] = {}
    unknown_source_type_count = 0

    for row in rows:
        managed, row_summary = _manage_row_sources(row)
        row["managed_sources"] = managed
        row["source_management_status"] = row_summary["status"]
        row["source_conflict_groups"] = row_summary["conflict_groups"]
        row["source_management_summary"] = {
            "total_sources": row_summary["total_sources"],
            "usable_sources": row_summary["usable_sources"],
            "incomplete_sources": row_summary["incomplete_sources"],
            "unknown_type_sources": row_summary["unknown_type_sources"],
            "conflict_group_count": row_summary["conflict_group_count"],
            "authority_tiers_present": sorted(row_summary["tiers_present"]),
        }

        if row_summary["total_sources"] > 0:
            packet_rows_with_sources += 1
        if row_summary["total_sources"] >= 2:
            packet_rows_with_multiple += 1
        if row_summary["conflict_group_count"] > 0:
            packet_rows_with_conflict += 1
        for managed_source in managed:
            st = managed_source["source_type"]
            source_type_histogram[st] = source_type_histogram.get(st, 0) + 1
            if managed_source["source_validation_status"] == VS_UNKNOWN_TYPE:
                unknown_source_type_count += 1

    out["source_management_version"] = SOURCE_MANAGEMENT_VERSION
    out["source_management_summary"] = {
        "rows_total": len(rows),
        "rows_with_sources": packet_rows_with_sources,
        "rows_with_multiple_sources": packet_rows_with_multiple,
        "rows_with_conflicted_sources": packet_rows_with_conflict,
        "source_type_histogram": dict(sorted(source_type_histogram.items())),
        "unknown_source_type_count": unknown_source_type_count,
    }
    return out


# ---------------------------------------------------------------------------
# Per-row source management
# ---------------------------------------------------------------------------

def _manage_row_sources(row: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Return (managed_sources, row_summary)."""
    raw = row.get("external_quantity_sources") or []
    managed: List[Dict[str, Any]] = []
    tiers_present: set = set()
    usable_count = 0
    incomplete_count = 0
    unknown_count = 0

    for src in raw:
        meta = _classify_source(src)
        managed.append(meta)
        tiers_present.add(meta["source_authority_tier"])
        status = meta["source_validation_status"]
        if status == VS_USABLE:
            usable_count += 1
        elif status == VS_INCOMPLETE:
            incomplete_count += 1
        elif status == VS_UNKNOWN_TYPE:
            unknown_count += 1

    # Deterministic visibility ordering: primary key = visibility_rank,
    # secondary key = original index (preserves stability on ties).
    managed = [
        dict(m, _orig_idx=i) for i, m in enumerate(managed)
    ]
    managed.sort(key=lambda m: (m["source_visibility_rank"], m["_orig_idx"]))
    for m in managed:
        m.pop("_orig_idx", None)

    # Conflict detection: only over sources marked "usable". Two usable
    # sources are "conflicted" when their canonical unit differs OR their
    # qty is both-present and unequal beyond 0.5%.
    usable_records = [
        (i, m) for i, m in enumerate(managed)
        if m["source_validation_status"] in (VS_USABLE, VS_CONFLICTED)
    ]
    conflict_groups: List[List[int]] = []
    if len(usable_records) >= 2:
        conflict_groups = _group_conflicts(usable_records)
        if conflict_groups:
            # Mark participating sources as conflicted (validation status
            # upgraded but the record is not dropped).
            for group in conflict_groups:
                for idx in group:
                    managed[idx]["source_validation_status"] = VS_CONFLICTED

    total = len(managed)
    if total == 0:
        status = SM_NONE
    elif conflict_groups:
        status = SM_CONFLICTED
    elif total == 1:
        status = SM_SINGLE
    else:
        status = SM_MULTIPLE

    summary = {
        "status": status,
        "total_sources": total,
        "usable_sources": usable_count,
        "incomplete_sources": incomplete_count,
        "unknown_type_sources": unknown_count,
        "conflict_group_count": len(conflict_groups),
        "conflict_groups": conflict_groups,
        "tiers_present": tiers_present,
    }
    return managed, summary


def _classify_source(src: Dict[str, Any]) -> Dict[str, Any]:
    """Decorate one attached source with management metadata."""
    source_type = src.get("source_type")
    policy = _AUTHORITY_POLICY.get(source_type)
    if policy is not None:
        tier, rank = policy
    else:
        tier, rank = TIER_UNKNOWN, 999

    qty = src.get("qty")
    unit = src.get("unit")
    if source_type not in _AUTHORITY_POLICY:
        validation = VS_UNKNOWN_TYPE
    elif qty is None and unit is None:
        validation = VS_INCOMPLETE
    else:
        validation = VS_USABLE

    return {
        "source_type": source_type,
        "source_ref": deepcopy(src.get("source_ref")),
        "qty": qty,
        "unit": unit,
        "source_trace": deepcopy(src.get("source_trace") or {}),
        "source_authority_tier": tier,
        "source_visibility_rank": rank,
        "source_validation_status": validation,
    }


# ---------------------------------------------------------------------------
# Conflict detection
# ---------------------------------------------------------------------------

def _group_conflicts(
    usable: List[Tuple[int, Dict[str, Any]]],
) -> List[List[int]]:
    """Identify indices of usable sources that disagree.

    A "conflict group" is a set of indices whose canonical (qty, unit)
    form does not match. All sources that take part in any disagreement
    are grouped together (single group for the row). The grouping is a
    signal — it is NEVER used to drop sources.
    """
    if not usable:
        return []
    # Reference values from the first usable source.
    ref_idx, ref = usable[0]
    ref_qty = ref.get("qty")
    ref_unit = _canon(ref.get("unit"))
    conflicting: List[int] = []
    for idx, m in usable:
        if _canon(m.get("unit")) != ref_unit:
            conflicting.append(idx)
            continue
        if not _qty_equal(m.get("qty"), ref_qty):
            conflicting.append(idx)
    if conflicting:
        # Include the reference in the conflict group so it's clear that
        # every usable source is part of the disagreement cluster.
        if ref_idx not in conflicting:
            conflicting.insert(0, ref_idx)
        return [sorted(set(conflicting))]
    return []


def _canon(u: Any) -> Optional[str]:
    if u is None:
        return None
    return str(u).strip().upper()


def _qty_equal(a: Any, b: Any) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        fa, fb = float(a), float(b)
    except (TypeError, ValueError):
        return False
    if fa == fb:
        return True
    if fb == 0:
        return fa == 0
    return abs(fa - fb) / abs(fb) <= _QTY_TOLERANCE
