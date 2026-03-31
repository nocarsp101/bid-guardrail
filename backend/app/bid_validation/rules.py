# backend/app/bid_validation/rules.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from app.audit.models import Finding


def validate_bid_items(
    rows: List[Dict[str, Any]],
    ingestion_meta: Dict[str, Any],
) -> Tuple[List[Finding], Dict[str, Any]]:
    """
    Deterministic guardrails per Milestone-1 with DOT-safe behavior:

    - Priced rows are those with unit_price>0 OR total>0
    - Rows that are "unpriced/unused" in DOT-style sheets often appear as:
        unit_price is blank AND total == 0
      These should NOT hard-fail PRIME_BID structural validation.
      We treat them as "unpriced_zero_rows" and skip them deterministically.

    - Each non-skipped item must be priced OR zeroed-with-note OR excluded
    - Detect silent blanks only for rows that are actually priced/expected to be priced
    - Mobilization mandatory (hard FAIL if missing)
    """
    findings: List[Finding] = []
    summary = {
        "rows_total": len(rows),
        "priced": 0,
        "unpriced_zero_rows": 0,   # NEW: DOT-style "present but not bid" rows
        "zeroed_with_note": 0,
        "excluded": 0,
        "invalid": 0,
        "blank_items": 0,
        "mobilization_found": False,
        "mobilization_row_index": None,
    }

    # Ingestion-level warnings (should be empty after alias mapping is correct)
    if ingestion_meta.get("mapping_missing"):
        findings.append(Finding(
            type="bid_ingestion_mapping",
            severity="WARN",
            message=f"Some expected columns were not mapped: {ingestion_meta['mapping_missing']}. Validation will proceed but may be less accurate.",
            meta={"mapping_used": ingestion_meta.get("mapping_used", {})}
        ))

    mobilization_candidates = []

    for r in rows:
        idx = int(r.get("_row_index", -1))
        item = _s(r.get("item"))
        desc = _s(r.get("description"))
        notes = _s(r.get("notes"))
        excluded = _to_bool(r.get("excluded_flag"))

        qty = _to_num(r.get("qty"))
        unit_price = _to_num(r.get("unit_price"))
        total = _to_num(r.get("total"))

        item_ref = item or desc or f"row_{idx}"

        # Identify mobilization line (deterministic contains-check)
        if "mobilization" in (item + " " + desc).lower():
            mobilization_candidates.append((idx, item_ref, unit_price, total, notes, excluded))

        # Skip fully empty rows
        if _is_empty_row(item, desc, qty, unit_price, total, notes, excluded):
            continue

        # Excluded items are valid
        if excluded:
            summary["excluded"] += 1
            continue

        # ✅ DOT-safe: treat "unit_price blank + total == 0" as an unpriced/unused row (skip)
        # This avoids blanket FAIL on valid DOT/ASHTOWare exports while still enforcing
        # strictness on actually-priced rows.
        if _is_unpriced_zero_row(qty=qty, unit_price=unit_price, total=total, notes=notes):
            summary["unpriced_zero_rows"] += 1
            continue

        # Silent blanks (only after unpriced-zero skip)
        missing_unit_price = unit_price is None
        missing_total = total is None

        if missing_unit_price or missing_total:
            summary["blank_items"] += 1
            findings.append(Finding(
                type="bid_item_blank_value",
                severity="FAIL",
                message=f"Silent blank detected: "
                        f"{'unit_price missing' if missing_unit_price else ''}"
                        f"{' and ' if (missing_unit_price and missing_total) else ''}"
                        f"{'total missing' if missing_total else ''}.",
                row_index=idx,
                item_ref=item_ref,
                meta={"unit_price": r.get("unit_price"), "total": r.get("total")}
            ))
            summary["invalid"] += 1
            continue

        # Zero totals without justification
        if (unit_price == 0 or total == 0) and not notes:
            findings.append(Finding(
                type="bid_item_zero_without_note",
                severity="FAIL",
                message="Zero value without explanation. Item must be priced OR zeroed with a note OR excluded.",
                row_index=idx,
                item_ref=item_ref,
                meta={"unit_price": unit_price, "total": total}
            ))
            summary["invalid"] += 1
            continue

        # Priced vs zeroed-with-note
        if (unit_price > 0) or (total > 0):
            summary["priced"] += 1
        else:
            summary["zeroed_with_note"] += 1

    # Mobilization rule (Hard FAIL if missing)
    if not mobilization_candidates:
        findings.append(Finding(
            type="mobilization_missing",
            severity="FAIL",
            message="Mobilization line item is missing (hard FAIL).",
        ))
    else:
        idx, item_ref, unit_price, total, notes, excluded = mobilization_candidates[0]
        summary["mobilization_found"] = True
        summary["mobilization_row_index"] = idx

        if excluded:
            findings.append(Finding(
                type="mobilization_excluded",
                severity="FAIL",
                message="Mobilization is marked as excluded (hard FAIL).",
                row_index=idx,
                item_ref=item_ref
            ))

        # NOTE: If mobilization is blank+0, it will be treated by the same rules above unless excluded.
        # We keep the Milestone-1 semantics as-is here:
        if ((unit_price == 0) or (total == 0)) and not notes:
            findings.append(Finding(
                type="mobilization_zero_without_note",
                severity="FAIL",
                message="Mobilization value is 0 without an override note (hard FAIL).",
                row_index=idx,
                item_ref=item_ref,
                meta={"unit_price": unit_price, "total": total}
            ))

    # If no bid/mobilization findings, add informational success
    if not any(f.type.startswith("bid_") or f.type.startswith("mobilization") for f in findings):
        findings.append(Finding(
            type="bid_item_validation",
            severity="INFO",
            message="Bid item validation passed without issues."
        ))

    return findings, summary


def _is_unpriced_zero_row(
    qty: Optional[float],
    unit_price: Optional[float],
    total: Optional[float],
    notes: str,
) -> bool:
    """
    Deterministic rule for DOT/ASHTOWare style "present but not bid" rows:
      - unit_price is blank (None)
      - total is explicitly 0
      - no notes provided
    We consider these as unpriced and skip them (not a FAIL).
    """
    if notes:
        return False
    if unit_price is not None:
        return False
    if total is None:
        return False
    if float(total) != 0.0:
        return False
    # qty may be 0/None/positive in some exports; we don't rely on it.
    return True


def _s(v: Any) -> str:
    return ("" if v is None else str(v)).strip()


def _to_num(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_bool(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "exclude", "excluded")


def _is_empty_row(
    item: str,
    desc: str,
    qty: Optional[float],
    unit_price: Optional[float],
    total: Optional[float],
    notes: str,
    excluded: bool
) -> bool:
    return (not item and not desc and qty is None and unit_price is None and total is None and not notes and not excluded)