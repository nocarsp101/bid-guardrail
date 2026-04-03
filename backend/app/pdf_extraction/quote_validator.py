# backend/app/pdf_extraction/quote_validator.py
"""
Validator for extracted quote rows.

Enforces minimal required fields, rejects malformed rows.
Fail-closed: if too many rows are invalid, extraction fails.

This is separate from the DOT schedule validator.
"""
from __future__ import annotations

from typing import List, Dict, Any, Tuple

from .extractor import ExtractionError


def validate_quote_rows(
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Validate extracted quote rows.

    Returns:
        (valid_rows, rejected_rows, validation_meta)

    Raises ExtractionError if zero valid rows.
    """
    valid: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []

    for row in rows:
        issues = _check_quote_row(row)
        if issues:
            rejected.append({**row, "_rejection_reasons": issues})
        else:
            valid.append(row)

    meta = {
        "rows_input": len(rows),
        "rows_valid": len(valid),
        "rows_rejected": len(rejected),
    }

    if len(valid) == 0:
        raise ExtractionError(
            "All extracted quote rows failed validation.",
            meta=meta,
        )

    return valid, rejected, meta


def _check_quote_row(row: Dict[str, Any]) -> List[str]:
    """Check a quote row for minimum required fields."""
    issues: List[str] = []

    # Description is always required
    desc = row.get("description")
    if not desc or not str(desc).strip():
        issues.append("missing description")

    # Must have at least one monetary value (amount or unit_price)
    amount = row.get("amount")
    unit_price = row.get("unit_price")
    if amount is None and unit_price is None:
        issues.append("no monetary value (amount or unit_price)")

    # Amount should be positive if present
    if amount is not None and amount <= 0:
        issues.append(f"non-positive amount: {amount}")

    return issues
