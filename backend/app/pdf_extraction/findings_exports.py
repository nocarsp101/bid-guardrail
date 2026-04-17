"""
C22 — Findings packet exports.

Deterministic export layer for the C19 findings packet (optionally
decorated by C21 prioritization). Produces three stable formats:

    1. export_findings_json   — stable flat JSON artifact
    2. export_findings_csv    — CSV row export (office-friendly tabular)
    3. export_findings_report — structured deterministic engineer/office
                                report payload (ready for future HTML/PDF
                                rendering)

Hard rules:
    - Exports are a pure function of the input packet.
    - No narrative generation. No legal / commercial conclusions.
    - No suppression of unresolved states (blocked / unmapped / ambiguous
      / non-comparable / mismatch all remain explicit in every export).
    - Row counts in every export equal the findings_rows count in the
      input packet.
    - Status fields (packet_status, pairing_status, etc.) persist.
    - Ordering follows the input ordering — which, when the packet has
      been through C21 prioritization, means highest-priority first.
    - PDF rendering is DEFERRED: the report export returns a structured
      payload that a downstream renderer can format. The export layer
      itself never produces a PDF binary.
"""
from __future__ import annotations

import csv
import io
import json
from copy import deepcopy
from typing import Any, Dict, List, Optional

EXPORT_VERSION = "findings_exports/v1"

# Stable CSV column order — never reordered.
_CSV_COLUMNS = (
    "normalized_row_id",
    "source_page",
    "priority_class",
    "priority_reason",
    "discrepancy_class",
    "mapping_outcome",
    "comparison_status",
    "non_comparable_reason",
    "compared_fields",
    "quote_description",
    "quote_line_ref",
    "quote_qty",
    "quote_unit",
    "quote_unit_price",
    "quote_amount",
    "mapped_bid_line_number",
    "mapped_bid_item_number",
    "bid_qty",
    "bid_unit",
    "comparison_flags",
    "review_flags",
)


# ---------------------------------------------------------------------------
# JSON export
# ---------------------------------------------------------------------------

def export_findings_json(findings_packet: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produce a stable flat JSON artifact from a findings packet.

    Returns a dict (not a serialized string) so the FastAPI layer can
    return it directly without re-serializing. Shape:

        {
            "export_version": "...",
            "export_format": "json",
            "packet_version": "...",
            "packet_status": "...",
            "pairing_status": "...",
            "summary": {
                "reconciliation": {...},
                "discrepancy": {...},
                "priority": {...},
                "mapping": {...},
            },
            "rows": [ ... flattened per-row records ... ],
            "pairing_section": {...},
            "quote_section": {...},
            "bid_section": {...},
        }

    Every key is always present (determinism). Missing sections become
    empty dicts.
    """
    rows = findings_packet.get("findings_rows") or []
    flat_rows = [_flatten_row(r) for r in rows]

    return {
        "export_version": EXPORT_VERSION,
        "export_format": "json",
        "packet_version": findings_packet.get("packet_version"),
        "packet_status": findings_packet.get("packet_status"),
        "pairing_status": (
            (findings_packet.get("pairing_section") or {}).get("pairing_status")
        ),
        "summary": {
            "reconciliation": deepcopy(findings_packet.get("reconciliation_section") or {}),
            "discrepancy": deepcopy(findings_packet.get("discrepancy_summary") or {}),
            "priority": deepcopy(findings_packet.get("priority_summary") or {}),
            "mapping": deepcopy(findings_packet.get("mapping_section") or {}),
        },
        "rows": flat_rows,
        "pairing_section": deepcopy(findings_packet.get("pairing_section") or {}),
        "quote_section": deepcopy(findings_packet.get("quote_section") or {}),
        "bid_section": deepcopy(findings_packet.get("bid_section") or {}),
    }


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def export_findings_csv(findings_packet: Dict[str, Any]) -> str:
    """
    Produce a CSV string with a fixed column set. Row count exactly equals
    findings_rows count in the input packet — nothing is filtered, even
    low-priority or blocked rows.
    """
    rows = findings_packet.get("findings_rows") or []
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(_CSV_COLUMNS)
    for r in rows:
        writer.writerow(_csv_row_values(r))
    return buf.getvalue()


def csv_columns() -> List[str]:
    return list(_CSV_COLUMNS)


# ---------------------------------------------------------------------------
# Engineer-ready report export
# ---------------------------------------------------------------------------

def export_findings_report(findings_packet: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produce a structured deterministic engineer/office report payload.

    The payload is intentionally ready-to-render: it has an ordered list
    of sections, each with a fixed label and structured content. A
    downstream HTML or PDF renderer can walk this payload without making
    decisions. No narrative prose is generated here — labels are
    deterministic constants.

    PDF rendering is deferred to a later layer. This function only
    produces the payload.
    """
    pairing = findings_packet.get("pairing_section") or {}
    reconciliation = findings_packet.get("reconciliation_section") or {}
    mapping_sec = findings_packet.get("mapping_section") or {}
    discrepancy = findings_packet.get("discrepancy_summary") or {}
    priority = findings_packet.get("priority_summary") or {}
    quote_section = findings_packet.get("quote_section") or {}
    bid_section = findings_packet.get("bid_section") or {}
    rows = findings_packet.get("findings_rows") or []

    sections: List[Dict[str, Any]] = [
        {
            "section_id": "packet_status",
            "label": "Packet Status",
            "content": {
                "packet_status": findings_packet.get("packet_status"),
                "packet_version": findings_packet.get("packet_version"),
            },
        },
        {
            "section_id": "pairing",
            "label": "Pairing",
            "content": {
                "pairing_status": pairing.get("pairing_status"),
                "pairing_reason": pairing.get("pairing_reason"),
                "allow_mapping": pairing.get("allow_mapping"),
                "warnings": list(pairing.get("warnings") or []),
            },
        },
        {
            "section_id": "quote",
            "label": "Quote Document",
            "content": deepcopy(quote_section),
        },
        {
            "section_id": "bid",
            "label": "DOT Bid Schedule",
            "content": deepcopy(bid_section),
        },
        {
            "section_id": "mapping",
            "label": "Mapping Summary",
            "content": deepcopy(mapping_sec),
        },
        {
            "section_id": "reconciliation",
            "label": "Reconciliation Summary",
            "content": deepcopy(reconciliation),
        },
        {
            "section_id": "discrepancy",
            "label": "Discrepancy Breakdown",
            "content": deepcopy(discrepancy),
        },
        {
            "section_id": "priority",
            "label": "Priority Triage",
            "content": deepcopy(priority),
        },
        {
            "section_id": "findings_rows",
            "label": "Row-level Findings",
            "content": {
                "row_count": len(rows),
                "rows": [_flatten_row(r) for r in rows],
            },
        },
    ]

    return {
        "export_version": EXPORT_VERSION,
        "export_format": "report",
        "packet_status": findings_packet.get("packet_status"),
        "packet_version": findings_packet.get("packet_version"),
        "pdf_rendering": "deferred",
        "sections": sections,
    }


# ---------------------------------------------------------------------------
# Row flattening
# ---------------------------------------------------------------------------

def _flatten_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a findings row into a stable dict with a fixed key set."""
    quote_values = row.get("quote_values") or {}
    bid_values = row.get("bid_values") or {}
    bid_qty = None
    bid_unit = None
    if isinstance(bid_values, dict):
        bid_qty = bid_values.get("qty")
        bid_unit = bid_values.get("unit")

    return {
        "normalized_row_id": row.get("normalized_row_id"),
        "source_page": row.get("source_page"),
        "priority_class": row.get("priority_class"),
        "priority_reason": row.get("priority_reason"),
        "discrepancy_class": row.get("discrepancy_class"),
        "mapping_outcome": row.get("mapping_outcome"),
        "comparison_status": row.get("comparison_status"),
        "non_comparable_reason": row.get("non_comparable_reason"),
        "compared_fields": list(row.get("compared_fields") or []),
        "quote_description": row.get("quote_description"),
        "quote_line_ref": row.get("quote_line_ref"),
        "quote_qty": quote_values.get("qty"),
        "quote_unit": quote_values.get("unit"),
        "quote_unit_price": quote_values.get("unit_price"),
        "quote_amount": quote_values.get("amount"),
        "mapped_bid_line_number": row.get("mapped_bid_line_number"),
        "mapped_bid_item_number": row.get("mapped_bid_item_number"),
        "bid_qty": bid_qty,
        "bid_unit": bid_unit,
        "comparison_flags": list(row.get("comparison_flags") or []),
        "review_flags": list(row.get("review_flags") or []),
    }


def _csv_row_values(row: Dict[str, Any]) -> List[str]:
    """Turn one flattened row into a CSV value sequence. Lists become
    semicolon-joined strings so Excel shows them as a single cell; None
    becomes an empty string."""
    flat = _flatten_row(row)
    out: List[str] = []
    for col in _CSV_COLUMNS:
        val = flat.get(col)
        if val is None:
            out.append("")
        elif isinstance(val, list):
            out.append(";".join(str(v) for v in val))
        else:
            out.append(str(val))
    return out
