"""
C35 — Bid Guardrail Control Room (unified job state).

Runs the full governed chain (C8 DOT extraction → C9/C10 quote staging →
C14 pairing → C13 mapping → C15 review packet → C16 reconciliation →
C17 contract → C18 classification → C29 injection → C32 source
management → C30 augmentation → C31 resolution → C33 office actions →
C34 engineer packet) on a quote + DOT pair, then assembles every stage
into a single canonical `control_room` job-state object.

The control room never mutates any sub-output. Every nested section is
a deep copy of the producing layer's return. The object exists to give
the office a single inspection point for the entire pipeline state.

Closed job-status vocabulary:
    - blocked   — pairing rejected; packet is blocked
    - partial   — unresolved review-required rows exist
    - ready     — every unresolved row has an office action recorded
    - complete  — no actionable discrepancies remain (rare)

Determinism rules:
    - Same inputs → same output.
    - No inference, no guessing, no narrative.
    - Status is a pure function of upstream state + office-action state.
    - Sub-objects are deep-copied; callers can freely mutate the
      returned control room without affecting the originals.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

CONTROL_ROOM_VERSION = "control_room/v1"

# Closed job-status vocabulary.
JOB_BLOCKED = "blocked"
JOB_PARTIAL = "partial"
JOB_READY = "ready"
JOB_COMPLETE = "complete"

# Review-required resolution categories (mirror of C31).
_REVIEW_REQUIRED = frozenset({
    "blocked_pairing_resolution_required",
    "unmapped_scope_review_required",
    "ambiguous_mapping_review_required",
    "source_conflict_review_required",
    "quantity_discrepancy_review_required",
    "unit_discrepancy_review_required",
    "non_comparable_missing_quote_source",
    "non_comparable_missing_external_source",
    "review_required_other",
})


def build_control_room(
    quote_pdf_path: str,
    dot_pdf_path: str,
    external_sources: Optional[List[Dict[str, Any]]] = None,
    office_action_metadata: Optional[Dict[str, Any]] = None,
    job_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run the full governed pipeline and assemble a unified control-room
    job-state artifact.

    Args:
        quote_pdf_path: path to the quote PDF.
        dot_pdf_path: path to the DOT proposal schedule PDF.
        external_sources: optional C29-shaped external source payloads.
        office_action_metadata: optional C33-shaped action metadata.
        job_id: optional deterministic job identifier supplied by the
                caller. Defaults to "job-<quote_name>-<dot_name>".

    Returns a deterministic control-room dict. Never mutates its inputs.
    """
    # Lazy imports so the control room module remains importable in
    # contexts that only exercise the status derivation helpers.
    from .service import extract_bid_items_from_pdf
    from .quote_normalization import normalize_quote_from_pdf
    from .pairing_guardrails import analyze_pairing
    from .quote_to_bid_mapping import map_quote_to_bid
    from .review_packet import build_review_packet
    from .reconciliation_foundation import reconcile_packet
    from .reconciliation_contract import build_reconciliation_contract
    from .discrepancy_classification import classify_contract
    from .quantity_injection import inject_external_quantities
    from .quantity_source_management import manage_quantity_sources
    from .augmentation_rules import apply_augmentation_rules
    from .discrepancy_resolution import build_resolution
    from .office_resolution_actions import record_office_actions
    from .engineer_output_packet import build_engineer_packet
    from .extractor import ExtractionError

    effective_job_id = job_id or _default_job_id(quote_pdf_path, dot_pdf_path)

    # ----- Stage 1: DOT extraction -----
    try:
        bid_rows, bid_summary = extract_bid_items_from_pdf(dot_pdf_path)
    except ExtractionError as e:
        return _failure_control_room(
            effective_job_id, "dot_extraction_failed",
            (e.meta or {}).get("failure_reason") or str(e),
            quote_pdf_path, dot_pdf_path,
        )

    # ----- Stage 2: quote staging -----
    try:
        staging = normalize_quote_from_pdf(quote_pdf_path)
    except ExtractionError as e:
        return _failure_control_room(
            effective_job_id, "quote_staging_failed",
            (e.meta or {}).get("failure_reason") or str(e),
            quote_pdf_path, dot_pdf_path,
        )

    doc_class = staging.get("document_class_detected")
    if doc_class != "quote":
        return _failure_control_room(
            effective_job_id, "quote_class_unsupported",
            f"document_class_detected={doc_class}",
            quote_pdf_path, dot_pdf_path,
        )

    accepted_rows = staging.get("accepted_rows", [])

    # ----- Stage 3: pairing guardrails -----
    pairing = analyze_pairing(accepted_rows, bid_rows)

    # ----- Stage 4: mapping (only if pairing allows) -----
    if pairing.get("allow_mapping"):
        mapping = map_quote_to_bid(accepted_rows, bid_rows)
    else:
        mapping = None

    # ----- Stage 5: review packet + reconciliation -----
    review_packet = build_review_packet(
        pairing_diagnostics=pairing,
        mapping_result=mapping,
        accepted_rows=accepted_rows,
        quote_diagnostics=staging.get("document_diagnostics") or {},
        bid_summary={
            "rows_extracted": bid_summary.get("rows_extracted"),
            "format_detected": bid_summary.get("format_detected"),
            "document_class": bid_summary.get("document_class"),
            "extraction_source": bid_summary.get("extraction_source"),
        },
    )
    recon_raw = reconcile_packet(review_packet)
    contract = build_reconciliation_contract(recon_raw, review_packet)
    classified = classify_contract(contract)

    # ----- Stage 6: injection → source management → augmentation → resolution -----
    injected = inject_external_quantities(classified, external_sources=external_sources)
    managed = manage_quantity_sources(injected)
    augmented = apply_augmentation_rules(injected)
    resolved = build_resolution(augmented)

    # ----- Stage 7: office actions + engineer packet -----
    actioned = record_office_actions(resolved, office_action_metadata)
    engineer_packet = build_engineer_packet(actioned, actioned, managed)

    # ----- Assemble control room object -----
    job_status = _derive_job_status(
        pairing_status=pairing.get("pairing_status"),
        packet_status=review_packet.get("packet_status"),
        resolved=actioned,
    )

    return {
        "control_room_version": CONTROL_ROOM_VERSION,
        "job_id": effective_job_id,
        "job_status": job_status,
        "input_summary": {
            "quote_pdf_path": quote_pdf_path,
            "dot_pdf_path": dot_pdf_path,
            "quote_accepted_rows": len(accepted_rows),
            "dot_rows_extracted": len(bid_rows),
            "external_sources_supplied": len(external_sources or []),
            "office_action_metadata_supplied": office_action_metadata is not None,
        },
        "pipeline_status": {
            "classification": staging.get("document_class_detected"),
            "staging_status": (staging.get("document_diagnostics") or {}).get("status"),
            "pairing_status": pairing.get("pairing_status"),
            "mapping_status": (review_packet.get("mapping_summary") or {}).get("mapping_status"),
            "packet_status": review_packet.get("packet_status"),
            "reconciliation_status": classified.get("reconciliation_status"),
            "augmentation_rules_version": augmented.get("augmentation_rules_version"),
            "resolution_status": resolved.get("resolution_status"),
            "engineer_packet_status": engineer_packet.get("packet_status"),
        },
        "pairing_section": deepcopy(pairing),
        "review_packet": deepcopy(review_packet),
        "reconciliation_contract": deepcopy(classified),
        "injected_contract": deepcopy(injected),
        "source_management": deepcopy(managed),
        "augmented_contract": deepcopy(augmented),
        "resolution": deepcopy(resolved),
        "office_actions_output": deepcopy(actioned),
        "engineer_packet": deepcopy(engineer_packet),
        "discrepancy_summary": deepcopy(resolved.get("resolution_summary") or {}),
        "priority_summary": deepcopy(
            (resolved.get("resolution_summary") or {}).get("priority_counts") or {}
        ),
        "source_management_section": deepcopy(
            managed.get("source_management_summary") or {}
        ),
        "office_action_summary": deepcopy(
            actioned.get("office_actions_summary") or {}
        ),
        "engineer_packet_preview": {
            "packet_status": engineer_packet.get("packet_status"),
            "engineer_row_count": (engineer_packet.get("packet_diagnostics") or {}).get(
                "engineer_row_count", 0
            ),
            "priority_histogram": deepcopy(
                (engineer_packet.get("packet_diagnostics") or {}).get(
                    "priority_histogram", {}
                )
            ),
            "flag_histogram": deepcopy(
                (engineer_packet.get("packet_diagnostics") or {}).get(
                    "engineer_packet_flag_histogram", {}
                )
            ),
        },
        "control_room_diagnostics": {
            "pipeline_succeeded": True,
            "failure_reason": None,
            "stages_completed": [
                "dot_extraction", "quote_staging", "pairing", "mapping",
                "review_packet", "reconciliation", "contract", "classification",
                "injection", "source_management", "augmentation", "resolution",
                "office_actions", "engineer_packet",
            ],
        },
    }


# ---------------------------------------------------------------------------
# Status derivation
# ---------------------------------------------------------------------------

def _derive_job_status(
    pairing_status: Optional[str],
    packet_status: Optional[str],
    resolved: Dict[str, Any],
) -> str:
    """Deterministic job_status from upstream state + action coverage."""
    if pairing_status == "rejected" or packet_status == "blocked":
        return JOB_BLOCKED

    rows = resolved.get("resolution_rows") or []
    if not rows:
        return JOB_COMPLETE

    unresolved = 0
    unresolved_with_action = 0
    actionable_total = 0

    for row in rows:
        cat = row.get("resolution_category")
        if cat not in _REVIEW_REQUIRED:
            continue
        actionable_total += 1
        unresolved += 1
        if row.get("office_actions"):
            unresolved_with_action += 1

    if actionable_total == 0:
        return JOB_COMPLETE
    if unresolved_with_action == 0:
        return JOB_PARTIAL
    if unresolved_with_action < unresolved:
        return JOB_PARTIAL
    return JOB_READY


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _default_job_id(quote_pdf_path: str, dot_pdf_path: str) -> str:
    import os
    return f"job-{os.path.basename(quote_pdf_path)}-{os.path.basename(dot_pdf_path)}"


def _failure_control_room(
    job_id: str,
    stage_failed: str,
    failure_reason: str,
    quote_pdf_path: str,
    dot_pdf_path: str,
) -> Dict[str, Any]:
    """Minimal control room for failures that cannot complete the chain."""
    return {
        "control_room_version": CONTROL_ROOM_VERSION,
        "job_id": job_id,
        "job_status": JOB_BLOCKED,
        "input_summary": {
            "quote_pdf_path": quote_pdf_path,
            "dot_pdf_path": dot_pdf_path,
        },
        "pipeline_status": {
            "stage_failed": stage_failed,
        },
        "pairing_section": None,
        "review_packet": None,
        "reconciliation_contract": None,
        "injected_contract": None,
        "source_management": None,
        "augmented_contract": None,
        "resolution": None,
        "office_actions_output": None,
        "engineer_packet": None,
        "discrepancy_summary": {},
        "priority_summary": {},
        "source_management_section": {},
        "office_action_summary": {},
        "engineer_packet_preview": {},
        "control_room_diagnostics": {
            "pipeline_succeeded": False,
            "failure_reason": failure_reason,
            "stages_completed": [],
        },
    }
