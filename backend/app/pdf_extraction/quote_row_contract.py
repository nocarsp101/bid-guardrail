# backend/app/pdf_extraction/quote_row_contract.py
"""
C12 — Normalized quote row contract (pre-mapping schema).

Defines the stable, deterministic contract that downstream mapping
(C13+) will consume. Rules:

    - normalized_row_id is a deterministic, document-scoped string
      formed from source_page + intra-document row ordinal. Stable
      across re-runs of the same document.
    - source_text preserves the exact original line text the row was
      parsed from (extraction-source-faithful).
    - provenance carries extraction_source, source_page, ocr_used,
      parser version. Sufficient for downstream audit.
    - row_issues is a list of governed, deterministic non-fatal notes.
      Issues are emitted ONLY when the parser observed a structural
      gap; they are never speculative. An empty list means the row
      satisfies the strict accepted-row rules with no caveats.

This module contains NO mapping logic. It only defines and applies
the accepted-row schema.
"""
from __future__ import annotations

from typing import Any, Dict, List

PARSER_VERSION = "quote_parser_v1"

# Governed row_issues codes (deterministic, never speculative).
ISSUE_UNIT_PRICE_ABSENT = "unit_price_absent"
ISSUE_LINE_REF_ABSENT = "line_ref_absent"
ISSUE_QTY_ABSENT = "qty_absent"
ISSUE_UNIT_ABSENT = "unit_absent"


def normalized_row_id(source_page: int, row_ordinal: int) -> str:
    """
    Build a stable, deterministic, document-scoped row identifier.

    Format: "qr-p{page}-r{ordinal}"
        qr  = quote row
        p   = source page index
        r   = intra-document row ordinal

    Same input produces the same output across runs — required for
    downstream mapping audit and idempotent reconciliation.
    """
    return f"qr-p{source_page}-r{row_ordinal}"


def build_accepted_row(
    parsed: Dict[str, Any],
    extraction_source: str,
    ocr_used: bool,
) -> Dict[str, Any]:
    """
    Project a parsed+validated row into the C12 accepted_row contract.

    Required input keys: row_id, source_page, description, amount.
    Optional input keys: line_ref, qty, unit, unit_price, source_text.

    The function NEVER invents values. Missing fields stay None.
    """
    source_page = parsed.get("source_page", 0)
    row_id = parsed.get("row_id", 0)

    issues: List[str] = []
    if parsed.get("unit_price") is None:
        issues.append(ISSUE_UNIT_PRICE_ABSENT)
    if parsed.get("line_ref") is None:
        issues.append(ISSUE_LINE_REF_ABSENT)
    if parsed.get("qty") is None:
        issues.append(ISSUE_QTY_ABSENT)
    if parsed.get("unit") is None:
        issues.append(ISSUE_UNIT_ABSENT)

    return {
        # Stable identifier — deterministic, document-scoped.
        "normalized_row_id": normalized_row_id(source_page, row_id),

        # Existing C10 fields preserved verbatim.
        "row_id": row_id,
        "line_ref": parsed.get("line_ref"),
        "description": parsed.get("description"),
        "qty": parsed.get("qty"),
        "unit": parsed.get("unit"),
        "unit_price": parsed.get("unit_price"),
        "amount": parsed.get("amount"),
        "source_page": source_page,
        "extraction_source": extraction_source,

        # C12 additions.
        "source_text": parsed.get("source_text", ""),
        "row_issues": issues,
        "provenance": {
            "extraction_source": extraction_source,
            "source_page": source_page,
            "ocr_used": ocr_used,
            "parser": PARSER_VERSION,
        },

        # C20 additions — enrichment provenance preserved when present.
        "field_sources": parsed.get("field_sources"),
        "enrichment_trace": parsed.get("enrichment_trace"),
        "enricher_version": parsed.get("enricher_version"),

        # C23 addition — table-rule provenance when E2 fired.
        "table_rule_version": parsed.get("table_rule_version"),

        # C27 addition — pattern library provenance when a C27 rule fired.
        "pattern_library_version": parsed.get("pattern_library_version"),
    }


# Schema reference — the canonical key set for accepted_rows.
ACCEPTED_ROW_KEYS = frozenset({
    "normalized_row_id",
    "row_id",
    "line_ref",
    "description",
    "qty",
    "unit",
    "unit_price",
    "amount",
    "source_page",
    "extraction_source",
    "source_text",
    "row_issues",
    "provenance",
    "field_sources",
    "enrichment_trace",
    "enricher_version",
    "table_rule_version",
    "pattern_library_version",
})

PROVENANCE_KEYS = frozenset({
    "extraction_source",
    "source_page",
    "ocr_used",
    "parser",
})
