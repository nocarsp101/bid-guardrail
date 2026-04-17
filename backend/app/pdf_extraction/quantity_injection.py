"""
C29 — External quantity injection layer.

Attaches trusted external quantity/unit data to mapped reconciliation
rows WITHOUT mutating the original quote values. Source attribution is
always explicit: every attached source carries its type, reference,
and trace so downstream logic (C30 augmentation rules, C31 discrepancy
resolution) can reason about which source it is using.

The injection layer itself never chooses which source is "correct" —
it only records what is available. C30 is responsible for selecting a
comparison basis from the attached sources.

Hard rules:
    - Quote values (qty, unit, unit_price, amount) are NEVER modified.
    - Unmapped / ambiguous / blocked rows never receive injected data.
      They are passed through unchanged except for an explicit empty
      `external_quantity_sources` list.
    - Multiple sources are attached as a list. They are never merged.
    - The default trusted source is the DOT bid item the row is mapped
      to (source_type = "dot_bid_item"). Callers can supply additional
      trusted sources via the `external_sources` parameter.
    - Every source carries a `source_type` from a closed vocabulary.
    - The module never invents external source identity. When a caller
      supplies a row id that doesn't exist in the contract, the id is
      surfaced in diagnostics but never creates a phantom row.
    - Augmentation state is reversible at the interpretation level:
      consumers can strip the augmentation fields and recover the
      original contract row.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional

INJECTION_VERSION = "quantity_injection/v1"

# Closed source-type vocabulary.
SRC_DOT_BID_ITEM = "dot_bid_item"
SRC_INTERNAL_TAKEOFF = "internal_takeoff"
SRC_ENGINEER_QUANTITY = "engineer_quantity"
SRC_MANUAL_REVIEW_INPUT = "manual_review_input"

_ALL_SOURCE_TYPES = frozenset({
    SRC_DOT_BID_ITEM,
    SRC_INTERNAL_TAKEOFF,
    SRC_ENGINEER_QUANTITY,
    SRC_MANUAL_REVIEW_INPUT,
})

# Closed augmentation-status vocabulary.
AUG_NONE = "none"
AUG_ATTACHED = "attached"
AUG_MULTIPLE_SOURCES = "multiple_sources"


def inject_external_quantities(
    classified_contract: Dict[str, Any],
    external_sources: Optional[List[Dict[str, Any]]] = None,
    attach_dot_bid_item: bool = True,
) -> Dict[str, Any]:
    """
    Build a new contract dict with `external_quantity_sources` attached
    per row. The input contract is never mutated.

    Args:
        classified_contract: the C18 classified reconciliation contract
                             (optionally already decorated by C21).
        external_sources: optional list of external source payloads, each
                          shaped:
                              {
                                  "source_type": "internal_takeoff" | ...,
                                  "source_ref": { arbitrary caller id },
                                  "rows": {
                                      "<normalized_row_id>": {
                                          "qty": float | None,
                                          "unit": str | None,
                                          "ref": str | None (optional),
                                          "trace": dict | None (optional),
                                      },
                                      ...
                                  },
                              }
                          Unknown `source_type` values are preserved verbatim
                          but surfaced in diagnostics.
        attach_dot_bid_item: when True (default), automatically attach the
                             mapped DOT bid item's qty/unit as a source of
                             type `dot_bid_item` for every mapped row.

    Returns a new contract dict with `injection_version`, per-row
    `external_quantity_sources`, per-row `augmentation_status`,
    per-row `augmentation_trace`, and a top-level `injection_diagnostics`
    with counts and unknown-row-id surfacing.
    """
    out = deepcopy(classified_contract)
    rows = out.get("reconciliation_rows") or []
    external_sources = external_sources or []

    # Build an index from normalized_row_id -> list of (source_type, record)
    # for caller-supplied sources so each row can look up its attachments.
    supplied_index: Dict[str, List[Dict[str, Any]]] = {}
    unknown_source_type_hits: List[str] = []
    for payload in external_sources:
        source_type = payload.get("source_type")
        if source_type not in _ALL_SOURCE_TYPES:
            unknown_source_type_hits.append(str(source_type))
        payload_rows = payload.get("rows") or {}
        source_ref = payload.get("source_ref")
        if not isinstance(payload_rows, dict):
            continue
        for rid, rec in payload_rows.items():
            if not isinstance(rec, dict):
                continue
            entry = _build_source_record(
                source_type=source_type,
                source_ref=source_ref,
                record=rec,
            )
            supplied_index.setdefault(str(rid), []).append(entry)

    # Collect unknown ids (supplied but not present in the contract) for
    # diagnostics so callers can debug mismatched inputs.
    known_ids = {r.get("normalized_row_id") for r in rows}
    unknown_row_ids = sorted(
        rid for rid in supplied_index.keys() if rid not in known_ids
    )

    # Walk the contract rows and decorate each one.
    rows_attached = 0
    rows_multiple = 0
    for row in rows:
        rid = row.get("normalized_row_id")
        mapping_outcome = row.get("mapping_outcome")
        mapped_bid_item = row.get("mapped_bid_item") if isinstance(row.get("mapped_bid_item"), dict) else None

        attached: List[Dict[str, Any]] = []

        if mapping_outcome == "mapped":
            if attach_dot_bid_item and mapped_bid_item is not None:
                dot_entry = _dot_source_from_mapped_bid_item(mapped_bid_item)
                if dot_entry is not None:
                    attached.append(dot_entry)
            for entry in supplied_index.get(str(rid), []):
                attached.append(entry)

        row["external_quantity_sources"] = attached
        if len(attached) == 0:
            row["augmentation_status"] = AUG_NONE
        elif len(attached) == 1:
            row["augmentation_status"] = AUG_ATTACHED
            rows_attached += 1
        else:
            row["augmentation_status"] = AUG_MULTIPLE_SOURCES
            rows_multiple += 1

        row["augmentation_trace"] = {
            "injection_version": INJECTION_VERSION,
            "attached_source_count": len(attached),
            "source_types_attached": sorted({s["source_type"] for s in attached}),
            "mapping_outcome_at_injection": mapping_outcome,
        }

    out["injection_version"] = INJECTION_VERSION
    out["injection_diagnostics"] = {
        "rows_total": len(rows),
        "rows_with_any_external_source": rows_attached + rows_multiple,
        "rows_with_multiple_sources": rows_multiple,
        "supplied_sources_count": len(external_sources),
        "unknown_row_ids": unknown_row_ids,
        "unknown_source_types": sorted(set(unknown_source_type_hits)),
    }
    return out


# ---------------------------------------------------------------------------
# Source record construction
# ---------------------------------------------------------------------------

def _dot_source_from_mapped_bid_item(mapped_bid_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Project a mapped DOT bid item into a dot_bid_item source record.

    The DOT bid item carries qty + unit (from C8 extraction). We only
    create a source entry when at least one of qty/unit is present, to
    avoid surfacing a source that has nothing to contribute.
    """
    qty = mapped_bid_item.get("qty")
    unit = mapped_bid_item.get("unit")
    if qty is None and unit is None:
        return None
    return {
        "source_type": SRC_DOT_BID_ITEM,
        "source_ref": {
            "line_number": mapped_bid_item.get("line_number"),
            "item_number": mapped_bid_item.get("item_number"),
        },
        "qty": qty,
        "unit": unit,
        "source_trace": {
            "origin": "mapped_bid_item",
            "injection_version": INJECTION_VERSION,
        },
    }


def _build_source_record(
    source_type: Any,
    source_ref: Any,
    record: Dict[str, Any],
) -> Dict[str, Any]:
    """Shape-validate a caller-supplied source record.

    Unknown fields are passed through untouched. qty/unit are coerced to
    None when absent. The original record dict is never mutated.
    """
    return {
        "source_type": str(source_type) if source_type is not None else None,
        "source_ref": deepcopy(source_ref),
        "qty": record.get("qty"),
        "unit": record.get("unit"),
        "source_trace": deepcopy(record.get("trace") or {
            "origin": "caller_supplied",
            "injection_version": INJECTION_VERSION,
            "record_ref": record.get("ref"),
        }),
    }
