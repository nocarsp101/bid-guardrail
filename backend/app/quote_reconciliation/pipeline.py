# backend/app/quote_reconciliation/pipeline.py
"""
Structured pipeline orchestrator for quote reconciliation.

Pipeline stages:
  1. Ingest quote lines (CSV/XLSX)
  2. Apply line-number-to-DOT-item mapping (optional adapter)
  3. Reconcile against bid items

Fail-closed: ingest/parse errors propagate to caller for handling.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from app.audit.models import Finding
from app.quote_reconciliation.ingest import ingest_quote_lines
from app.quote_reconciliation.rules import reconcile_quote_lines_against_bid
from app.adapters.line_mapping import apply_line_number_mapping


def run_structured_pipeline(
    quote_file_path: str,
    bid_rows: List[Dict[str, Any]],
    line_to_item_mapping: Optional[Dict[str, str]] = None,
) -> Tuple[List[Finding], Dict[str, Any], Dict[str, Any]]:
    """
    Orchestrate the full quote reconciliation pipeline.

    Args:
        quote_file_path: Path to the quote CSV/XLSX file.
        bid_rows: Normalized bid rows from bid ingest.
        line_to_item_mapping: Optional dict mapping line numbers (str) to
            DOT item numbers (str). When provided, the line mapping adapter
            runs after ingest and before reconciliation. When absent, quote
            item values pass through unchanged.

    Returns:
        Tuple of (findings, quote_summary, quote_ingest_meta).

    Raises:
        IngestError or Exception on bad input (fail-closed).
    """
    # Stage 1: Ingest
    quote_rows, quote_ingest_meta = ingest_quote_lines(quote_file_path)

    # Stage 2: Line mapping (toggle — applied only when mapping provided)
    if line_to_item_mapping:
        quote_rows = apply_line_number_mapping(quote_rows, line_to_item_mapping)
        quote_ingest_meta["line_mapping_applied"] = True
        quote_ingest_meta["line_mapping_entries"] = len(line_to_item_mapping)
    else:
        quote_ingest_meta["line_mapping_applied"] = False

    # Stage 3: Reconciliation (deterministic, fail-closed)
    findings, quote_summary = reconcile_quote_lines_against_bid(
        bid_rows=bid_rows,
        quote_rows=quote_rows,
    )

    # Stage 4: Unmatched enrichment (post-reconciliation, informational only)
    # When unmatched findings exist, surface the available bid items so the
    # operator can build or correct a line-to-item mapping.
    unmatched_findings = [f for f in findings if f.type == "quote_line_unmatched"]
    if unmatched_findings:
        bid_item_list = sorted(set(
            str(b.get("item", "")).strip()
            for b in bid_rows
            if str(b.get("item", "")).strip()
        ))
        quote_summary["available_bid_items"] = bid_item_list
        quote_summary["mapping_hint"] = (
            f"{len(unmatched_findings)} quote line(s) could not be matched to bid items. "
            f"Upload or save a line-to-item mapping JSON to resolve. "
            f"{len(bid_item_list)} bid items available."
        )

    return findings, quote_summary, quote_ingest_meta
