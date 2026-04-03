# backend/app/pdf_extraction/validator.py
"""
Strict validator for extracted schedule rows.

Enforces required fields, rejects malformed rows,
rejects incomplete extraction. Fail-closed.
"""
from __future__ import annotations

from typing import List, Dict, Any, Tuple

from .extractor import ExtractionError


REQUIRED_FIELDS = ("line_number", "item", "description", "qty", "unit")


def validate_extracted_rows(
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Validate extracted rows and separate into valid/rejected.

    Returns:
        (valid_rows, rejected_rows, validation_meta)

    Raises ExtractionError if:
        - Zero valid rows after validation
        - More than 50% of rows are rejected (extraction quality too low)
    """
    valid: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []

    for row in rows:
        issues = _check_row(row)
        if issues:
            rejected.append({**row, "_rejection_reasons": issues})
        else:
            valid.append(row)

    total = len(rows)
    valid_count = len(valid)
    rejected_count = len(rejected)

    validation_meta = {
        "rows_input": total,
        "rows_valid": valid_count,
        "rows_rejected": rejected_count,
    }

    if valid_count == 0:
        raise ExtractionError(
            "All extracted rows failed validation. Extraction invalid.",
            meta=validation_meta,
        )

    if total > 0 and (rejected_count / total) > 0.5:
        raise ExtractionError(
            f"Extraction quality too low: {rejected_count}/{total} rows rejected (>50%).",
            meta=validation_meta,
        )

    return valid, rejected, validation_meta


def _check_row(row: Dict[str, Any]) -> List[str]:
    """Check a single row for required field issues. Returns list of issue descriptions."""
    issues: List[str] = []

    for field in REQUIRED_FIELDS:
        val = row.get(field)
        if val is None:
            issues.append(f"missing field: {field}")
        elif isinstance(val, str) and not val.strip():
            issues.append(f"empty field: {field}")

    # line_number must be 4 digits
    ln = row.get("line_number", "")
    if isinstance(ln, str) and ln and (not ln.isdigit() or len(ln) != 4):
        issues.append(f"line_number not 4 digits: '{ln}'")

    # item must match DOT pattern DDDD-DDDDDDD
    item = row.get("item", "")
    if isinstance(item, str) and item:
        import re
        if not re.fullmatch(r'\d{4}-\d{7}', item):
            issues.append(f"item not DOT format: '{item}'")

    # qty must be positive
    qty = row.get("qty")
    if isinstance(qty, (int, float)) and qty <= 0:
        issues.append(f"qty not positive: {qty}")

    return issues
