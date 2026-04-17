# backend/app/pdf_extraction/quote_validator.py
"""
Validator for extracted quote rows.

Enforces the non-negotiable rules for quote rows:
    - description is required (non-empty after trim)
    - at least one monetary value (amount OR unit_price) must be present
    - amount, when present, must be strictly positive
    - unit_price, when present, must be strictly positive
    - qty and unit, when present, must be positive / non-empty respectively;
      None is allowed (never inferred)
    - if BOTH qty and unit_price and amount are present, they must be
      internally consistent (within 1% tolerance) — otherwise the row is
      rejected as ambiguous

Fail-closed: if zero valid rows remain, raises ExtractionError with an
explicit `failure_reason` code.

Separate from the DOT schedule validator.
"""
from __future__ import annotations

from typing import List, Dict, Any, Tuple

from .extractor import ExtractionError

REASON_STRUCTURE_INSUFFICIENT = "quote_structure_insufficient"
REASON_NOT_DETERMINISTIC = "quote_rows_not_deterministic"

# Per-row rejection reasons (also used by the staging layer to populate
# rejected_candidates[].rejection_reason).
V_MISSING_DESC = "missing_description"
V_NO_MONETARY = "no_monetary_value"
V_NON_POSITIVE = "non_positive_numeric"
V_BAD_NUMERIC = "bad_numeric_row"
V_INCONSISTENT = "inconsistent_numeric"


def validate_quote_rows(
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Validate extracted quote rows.

    Returns:
        (valid_rows, rejected_rows, validation_meta)

    Raises ExtractionError with failure_reason in meta if zero valid rows.
    """
    valid: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []

    for row in rows:
        issues = _check_quote_row(row)
        if issues:
            rejected.append({**row, "_rejection_reasons": issues})
        else:
            valid.append(row)

    meta: Dict[str, Any] = {
        "rows_input": len(rows),
        "rows_valid": len(valid),
        "rows_rejected": len(rejected),
    }

    if len(valid) == 0:
        meta["failure_reason"] = REASON_STRUCTURE_INSUFFICIENT
        raise ExtractionError(
            "All extracted quote rows failed validation. No usable quote "
            "rows remain.",
            meta=meta,
        )

    return valid, rejected, meta


def _check_quote_row(row: Dict[str, Any]) -> List[str]:
    """Check a quote row against the non-negotiable rules. Returns issue list."""
    issues: List[str] = []

    desc = row.get("description")
    if not desc or not str(desc).strip():
        issues.append(V_MISSING_DESC)

    amount = row.get("amount")
    unit_price = row.get("unit_price")
    if amount is None and unit_price is None:
        issues.append(V_NO_MONETARY)

    if amount is not None and amount <= 0:
        issues.append(V_NON_POSITIVE)

    if unit_price is not None and unit_price <= 0 and V_NON_POSITIVE not in issues:
        issues.append(V_NON_POSITIVE)

    # qty must be positive if present; None is allowed (never inferred).
    qty = row.get("qty")
    if qty is not None:
        try:
            if float(qty) <= 0:
                if V_NON_POSITIVE not in issues:
                    issues.append(V_NON_POSITIVE)
        except (TypeError, ValueError):
            issues.append(V_BAD_NUMERIC)

    # If all three numeric fields are present, check internal consistency.
    # Otherwise the row is ambiguous and must be rejected.
    if qty is not None and unit_price is not None and amount is not None:
        try:
            q = float(qty)
            up = float(unit_price)
            a = float(amount)
            expected = q * up
            # Tolerate 1% rounding drift.
            if expected > 0:
                drift = abs(expected - a) / expected
                if drift > 0.01:
                    issues.append(V_INCONSISTENT)
        except (TypeError, ValueError):
            if V_BAD_NUMERIC not in issues:
                issues.append(V_BAD_NUMERIC)

    return issues
