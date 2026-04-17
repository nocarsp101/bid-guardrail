"""
C43 — Hybrid evaluation path (machine + human).

Combines machine-extracted accepted_rows (from C10/C41 staging) with
active manual-interpretation rows (from C42) into a unified effective
row list, then feeds that list through the governed downstream pipeline
(pairing → mapping → review packet → reconciliation → augmentation →
resolution) with full provenance preserved on every row.

Hard rules:
    - Machine rows carry `row_origin = native_pdf | ocr_pdf`.
    - Manual rows carry `row_origin = manual_interpretation`.
    - The hybrid assembly never overwrites a machine row with a manual
      one on the same slot. If both exist and the user wants the manual
      row to take precedence, the user must explicitly exclude the
      machine row — the module does NOT auto-replace.
    - All downstream outputs preserve `row_origin` and
      `manual_entry_ref` on every row that has them.
    - Blocked / unmapped / ambiguous / conflicted states remain
      visible exactly as the base pipeline would surface them.
    - Pure-machine documents pass through unchanged — the hybrid layer
      is a no-op when manual_store is None or empty.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

HYBRID_EVAL_VERSION = "hybrid_quote_evaluation/v1"

ROW_ORIGIN_NATIVE = "native_pdf"
ROW_ORIGIN_OCR = "ocr_pdf"
ROW_ORIGIN_MANUAL = "manual_interpretation"
ROW_ORIGIN_MACHINE_PARTIAL = "machine_partial_human_completed"


def build_hybrid_rows(
    intake_output: Dict[str, Any],
    manual_store: Optional[Dict[str, Any]] = None,
    approval_state: Optional[Dict[str, Any]] = None,
    include_unapproved_manual_rows: bool = True,
) -> Dict[str, Any]:
    """
    Merge machine + manual rows with provenance.

    Args:
        include_unapproved_manual_rows: when False (safe default when
            approval_state is provided), only approved manual rows are
            included. When True (legacy default), all active manual rows
            are included regardless of approval state. The chosen mode
            is surfaced in `hybrid_summary.include_unapproved_mode`.
    """
    machine_rows = _tag_machine_rows(intake_output)

    if manual_store and approval_state and not include_unapproved_manual_rows:
        from .manual_interpretation_approval import get_approved_manual_rows
        manual_rows = get_approved_manual_rows(manual_store, approval_state)
        approval_mode = "approved_only"
    elif manual_store:
        manual_rows = _get_manual_rows(manual_store)
        approval_mode = "all_active"
    else:
        manual_rows = []
        approval_mode = "none"

    effective_rows = list(machine_rows) + list(manual_rows)

    return {
        "hybrid_eval_version": HYBRID_EVAL_VERSION,
        "effective_rows": effective_rows,
        "hybrid_summary": {
            "machine_rows_used": len(machine_rows),
            "manual_rows_used": len(manual_rows),
            "total_effective_rows": len(effective_rows),
            "mixed_document": len(machine_rows) > 0 and len(manual_rows) > 0,
            "unresolved_blocks_remaining": _count_unresolved_blocks(intake_output, manual_store),
            "include_unapproved_mode": approval_mode,
        },
    }


def run_hybrid_pipeline(
    intake_output: Dict[str, Any],
    manual_store: Optional[Dict[str, Any]],
    dot_pdf_path: str,
    external_sources: Optional[List[Dict[str, Any]]] = None,
    office_action_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Full hybrid pipeline run: builds effective rows, then runs the
    standard governed chain and returns a result bundle.
    """
    from .service import extract_bid_items_from_pdf
    from .pairing_guardrails import analyze_pairing
    from .quote_to_bid_mapping import map_quote_to_bid
    from .review_packet import build_review_packet
    from .reconciliation_foundation import reconcile_packet
    from .reconciliation_contract import build_reconciliation_contract
    from .discrepancy_classification import classify_contract
    from .quantity_injection import inject_external_quantities
    from .augmentation_rules import apply_augmentation_rules
    from .discrepancy_resolution import build_resolution
    from .office_resolution_actions import record_office_actions
    from .extractor import ExtractionError

    hybrid = build_hybrid_rows(intake_output, manual_store)
    effective_rows = hybrid["effective_rows"]

    try:
        bid_rows, bid_summary = extract_bid_items_from_pdf(dot_pdf_path)
    except ExtractionError as e:
        return {
            "hybrid_eval_version": HYBRID_EVAL_VERSION,
            "hybrid_summary": hybrid["hybrid_summary"],
            "pipeline_status": "dot_extraction_failed",
            "error": str(e),
        }

    pairing = analyze_pairing(effective_rows, bid_rows)
    if pairing.get("allow_mapping"):
        mapping = map_quote_to_bid(effective_rows, bid_rows)
    else:
        mapping = None

    packet = build_review_packet(
        pairing_diagnostics=pairing,
        mapping_result=mapping,
        accepted_rows=effective_rows,
        quote_diagnostics=(intake_output or {}).get("staging_diagnostics") or {},
        bid_summary={
            "rows_extracted": bid_summary.get("rows_extracted"),
            "format_detected": bid_summary.get("format_detected"),
            "document_class": bid_summary.get("document_class"),
            "extraction_source": bid_summary.get("extraction_source"),
        },
    )
    recon = reconcile_packet(packet)
    contract = build_reconciliation_contract(recon, packet)
    classified = classify_contract(contract)
    injected = inject_external_quantities(classified, external_sources=external_sources)
    augmented = apply_augmentation_rules(injected)
    resolved = build_resolution(augmented)
    actioned = record_office_actions(resolved, office_action_metadata)

    return {
        "hybrid_eval_version": HYBRID_EVAL_VERSION,
        "hybrid_summary": hybrid["hybrid_summary"],
        "pipeline_status": "complete",
        "pairing_status": pairing.get("pairing_status"),
        "packet_status": packet.get("packet_status"),
        "reconciliation_status": classified.get("reconciliation_status"),
        "resolution_status": resolved.get("resolution_status"),
        "resolution_summary": deepcopy(resolved.get("resolution_summary") or {}),
        "resolution": deepcopy(actioned),
        "augmented_contract": deepcopy(augmented),
        "effective_rows_used": len(effective_rows),
    }


# ---------------------------------------------------------------------------
# Row tagging
# ---------------------------------------------------------------------------

def _tag_machine_rows(intake_output: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Tag each machine-extracted row with `row_origin` / `row_effective_source`."""
    rows = (intake_output or {}).get("accepted_rows") or []
    extraction_source = (intake_output or {}).get("extraction_source") or "unknown"

    origin = ROW_ORIGIN_OCR if "ocr" in extraction_source else ROW_ORIGIN_NATIVE

    out: List[Dict[str, Any]] = []
    for r in rows:
        tagged = deepcopy(r)
        tagged.setdefault("row_origin", origin)
        tagged.setdefault("row_effective_source", extraction_source)
        out.append(tagged)
    return out


def _get_manual_rows(manual_store: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract active manual rows with `row_origin = manual_interpretation`."""
    from .manual_quote_interpretation import get_current_manual_rows
    rows = get_current_manual_rows(manual_store)
    for r in rows:
        r.setdefault("row_origin", ROW_ORIGIN_MANUAL)
        r.setdefault("row_effective_source", "manual_interpretation")
    return rows


def _count_unresolved_blocks(
    intake_output: Dict[str, Any],
    manual_store: Optional[Dict[str, Any]],
) -> int:
    """Count evidence blocks that have NO matching active manual entry.

    Only count blocks marked `partial` or `unreadable` — readable
    blocks already have machine rows.
    """
    blocks = (intake_output or {}).get("evidence_blocks") or []
    if not manual_store:
        return sum(
            1 for b in blocks
            if b.get("machine_readability") in ("partial", "unreadable")
        )
    manual_block_ids = set()
    for entry in manual_store.get("entries") or []:
        if entry.get("entry_status") == "active":
            bid = (entry.get("source_block_ref") or {}).get("block_id")
            if bid:
                manual_block_ids.add(bid)
    return sum(
        1 for b in blocks
        if b.get("machine_readability") in ("partial", "unreadable")
        and b.get("block_id") not in manual_block_ids
    )
