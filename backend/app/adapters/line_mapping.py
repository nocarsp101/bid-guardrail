# backend/app/adapters/line_mapping.py
"""
Line-number-to-DOT-item mapping adapter.

Preprocessing step that translates proposal line numbers in quote rows
(e.g., 520, 530) to DOT item numbers (e.g., 2524-6765010) BEFORE
reconciliation runs.

This is an explicit, optional adapter — not embedded in reconciliation logic.
Fail-closed: unmapped items are left unchanged, not guessed.
"""
from __future__ import annotations

from typing import Any, Dict, List


def apply_line_number_mapping(
    quote_rows: List[Dict[str, Any]],
    mapping: Dict[str, str],
) -> List[Dict[str, Any]]:
    """
    For each quote row, if row["item"] exists in the mapping dict,
    replace it with the corresponding DOT item number.

    - Preserves original value in row["item_raw"] (if not already set)
    - Unmapped items are left unchanged (fail-closed)
    - Returns a new list; does not mutate input rows.

    Args:
        quote_rows: Normalized quote rows from ingest.
        mapping: Dict mapping line-number strings to DOT item strings.
                 e.g. {"520": "2524-6765010", ...}
    """
    mapped = []
    for row in quote_rows:
        out = dict(row)
        item = str(out.get("item", "")).strip()
        if item and item in mapping:
            if "item_raw" not in out or not out["item_raw"]:
                out["item_raw"] = item
            out["item"] = mapping[item]
        mapped.append(out)
    return mapped
