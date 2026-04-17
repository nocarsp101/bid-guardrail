# backend/app/main.py
from __future__ import annotations

import json
import os
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response

from app.storage.local_fs import RunStorage
from app.storage.mapping_store import MappingStore
from app.pdf_validation.integrity import validate_pdf_integrity
from app.pdf_validation.context import adjust_pdf_findings_by_doc_type

from app.bid_validation.ingest import ingest_bid_items, IngestError
from app.bid_validation.rules import validate_bid_items

from app.audit.models import AuditEvent, Finding, OverrideInfo
from app.audit.writer import AuditWriter

from app.quote_reconciliation.pipeline import run_structured_pipeline
from app.operator_report import build_operator_report
from app.export_report import render_html, render_csv

from app.pdf_extraction.service import extract_bid_items_from_pdf, extract_quote_from_pdf, extract_pdf_auto
from app.pdf_extraction.quote_normalization import normalize_quote_from_pdf
from app.pdf_extraction.quote_to_bid_mapping import map_quote_to_bid
from app.pdf_extraction.pairing_guardrails import analyze_pairing
from app.pdf_extraction.review_packet import build_review_packet
from app.pdf_extraction.reconciliation_foundation import reconcile_packet
from app.pdf_extraction.reconciliation_contract import build_reconciliation_contract
from app.pdf_extraction.discrepancy_classification import classify_contract
from app.pdf_extraction.findings_packet import build_findings_packet
from app.pdf_extraction.review_prioritization import (
    prioritize_findings_packet,
    prioritize_classified_contract,
)
from app.pdf_extraction.findings_exports import (
    export_findings_json,
    export_findings_csv,
    export_findings_report,
)
from app.pdf_extraction.office_workflow import build_workflow_packet
from app.pdf_extraction.extractor import ExtractionError

# C80+ canonical artifact and orchestration services.
from app.pdf_extraction.artifact_repository import (
    get_default_repository, reset_default_repository,
)
from app.pdf_extraction.control_room_assembly import (
    assemble_quote_case_payload, assemble_package_overview_payload,
    assemble_authority_action_payload, assemble_bid_readiness_payload,
    assemble_timeline_payload,
)
from app.pdf_extraction.export_orchestration import (
    generate_sub_clarification_export, generate_estimator_review_export,
    generate_authority_action_export, generate_bid_readiness_export,
    generate_final_carry_export,
)
from app.pdf_extraction.canonical_api_contracts import (
    list_supported_artifact_types, get_schema_descriptor,
)
from app.pdf_extraction.e2e_demo_harness import run_e2e_demo, build_demo_fixture
from app.pdf_extraction.seed_scenarios import list_scenarios, run_scenario_e2e

# C86-C91 application hardening.
from app.pdf_extraction.scope_guardrails import check_scope, filter_records_by_scope
from app.pdf_extraction.api_error_contracts import (
    build_error, to_http_response, list_error_codes,
    ERR_INVALID_ARTIFACT_TYPE, ERR_RECORD_NOT_FOUND, ERR_UNKNOWN_SCENARIO,
    ERR_SCOPE_MISMATCH, ERR_MISSING_REVISION, ERR_MALFORMED_PAYLOAD,
)
from app.pdf_extraction.revision_diff import (
    diff_revisions, diff_lineage, diff_summary,
)
from app.pdf_extraction.ui_integration_pack import (
    get_ui_integration_pack, get_screen, get_export_action,
    list_screen_ids, list_export_ids,
)
from app.pdf_extraction.production_smoke_harness import run_smoke

# C92-C97 operational-readiness layers.
from app.pdf_extraction.authorization import (
    authorize, authorization_summary, list_roles, list_actions,
)
from app.pdf_extraction.idempotency import (
    get_default_idempotency_store, reset_default_idempotency_store,
    idempotent_save_artifact,
)
from app.pdf_extraction.backup_restore import (
    create_snapshot, validate_snapshot, restore_snapshot,
)
from app.pdf_extraction.render_reports import (
    build_estimator_review_report, build_authority_action_report,
    build_bid_readiness_report, build_final_carry_report,
    list_report_kinds,
)
from app.pdf_extraction.admin_diagnostics import collect_diagnostics
from app.pdf_extraction.acceptance_harness import run_acceptance

# C98-C103 product-facing layers.
from app.pdf_extraction.frontend_reference_integration import (
    ControlRoomReferenceClient, build_integration_manifest,
)
from app.pdf_extraction.production_storage_contract import (
    ProductionStorageContract, mirror_repository,
)
from app.pdf_extraction.report_delivery import (
    deliver_report, deliver_all_for_bid, list_formats,
)
from app.pdf_extraction.operator_workflow_actions import (
    apply_action as apply_operator_action,
    list_actions as list_operator_actions,
    list_clarification_states, list_carry_states,
)
from app.pdf_extraction.admin_safety_controls import (
    evaluate_safety, guarded_reset_repository, guarded_restore_snapshot,
    guarded_wipe_idempotency, safety_summary,
)
from app.pdf_extraction.product_demo_flow import run_product_demo

# C104-C109 UI/deployment-readiness layers.
from app.pdf_extraction.frontend_screen_adapters import (
    adapt_quote_case, adapt_package_overview, adapt_authority_action,
    adapt_bid_readiness, adapt_timeline, adapt_revision_inspection,
    list_screens as list_ui_screens,
)
from app.pdf_extraction.report_download_flow import (
    build_downloadable, build_downloadable_bundle,
    list_report_kinds as list_download_report_kinds,
)
from app.pdf_extraction.operator_command_flow import (
    execute_command as execute_operator_command,
    get_default_receipt_log, reset_default_receipt_log,
    list_commands as list_operator_commands,
)
from app.pdf_extraction.runtime_config import (
    default_config, load_config_from_env, validate_config, summarize_config,
)
from app.pdf_extraction.bootstrap_harness import bootstrap, health_check
from app.pdf_extraction.ui_demo_harness import run_ui_demo

# C120-C121 runtime packaging + acceptance walkthrough
from app.pdf_extraction.runtime_packaging import (
    runtime_profile, build_runtime_config, build_frontend_handoff,
    package_runtime, startup_verification,
)
from app.pdf_extraction.e2e_acceptance_walkthrough import (
    run_walkthrough, list_walkthrough_scenarios,
)


APP_NAME = "Bid Guardrail MVP (Week-2)"
DATA_DIR = os.getenv("BID_GUARDRAIL_DATA_DIR", "/data")

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

storage = RunStorage(DATA_DIR)
audit = AuditWriter(DATA_DIR)
mapping_store = MappingStore(DATA_DIR)


@app.get("/health")
def health():
    return {"status": "ok", "app": APP_NAME}


# ---------------------------------------------------------------------------
# PDF Schedule Extraction (C8A)
# ---------------------------------------------------------------------------

@app.post("/extract/bid-items/pdf")
async def extract_bid_items_pdf(
    pdf: UploadFile = File(..., description="Native-text PDF schedule of items"),
):
    """
    Extract structured bid rows from a native-text DOT schedule PDF.

    Returns normalized bid item rows and extraction diagnostics.
    Fails closed if extraction is ambiguous or incomplete.
    """
    import tempfile

    if not pdf.filename or not pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File must be a PDF.")

    # Save uploaded PDF to temp file for processing
    content = await pdf.read()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        rows, summary = extract_bid_items_from_pdf(tmp_path)
    except ExtractionError as e:
        meta = e.meta or {}
        return JSONResponse(
            status_code=422,
            content={
                "status": "extraction_failed",
                "failure_reason": meta.get("failure_reason"),
                "document_class_detected": meta.get("document_class_detected", "dot_schedule"),
                "extraction_source": meta.get("extraction_source"),
                "error": str(e),
                "meta": meta,
            },
        )
    finally:
        import os
        os.unlink(tmp_path)

    return {
        "status": "success",
        "document_class_detected": summary.get("document_class_detected"),
        "extraction_source": summary.get("extraction_source"),
        "failure_reason": None,
        "rows": rows,
        "row_count": len(rows),
        "summary": summary,
    }


@app.post("/extract/quote/pdf")
async def extract_quote_pdf(
    pdf: UploadFile = File(..., description="Subcontractor/vendor quote PDF"),
):
    """
    Extract structured quote rows from a subcontractor/vendor quote PDF.

    Separate from DOT schedule extraction. Uses quote-specific parser.
    Returns quote rows with provenance, or explicit failure.
    """
    import tempfile

    if not pdf.filename or not pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File must be a PDF.")

    content = await pdf.read()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        rows, summary = extract_quote_from_pdf(tmp_path)
    except ExtractionError as e:
        meta = e.meta or {}
        return JSONResponse(
            status_code=422,
            content={
                "status": "extraction_failed",
                "failure_reason": meta.get("failure_reason"),
                "document_class_detected": meta.get("document_class_detected", "quote"),
                "extraction_source": meta.get("extraction_source"),
                "error": str(e),
                "meta": meta,
            },
        )
    finally:
        import os
        os.unlink(tmp_path)

    return {
        "status": "success",
        "document_class_detected": summary.get("document_class_detected"),
        "extraction_source": summary.get("extraction_source"),
        "failure_reason": None,
        "rows": rows,
        "row_count": len(rows),
        "summary": summary,
    }


@app.post("/extract/quote/staging")
async def extract_quote_staging(
    pdf: UploadFile = File(..., description="Subcontractor/vendor quote PDF"),
):
    """
    C10 governed quote normalization staging.

    Returns the three-bucket staging object:
        - accepted_rows: deterministic rows that passed parse + validate
        - rejected_candidates: row-like evidence rejected with reasons
        - document_diagnostics: classification and extraction metadata

    DOT schedules never flow through this endpoint. Unknown documents
    return a staged failure object with zero accepted_rows and preserved
    diagnostics. Quote documents return accepted_rows when at least one
    row survives; otherwise they return a staged failure with explicit
    failure_reason and preserved rejected_candidates.
    """
    import tempfile

    if not pdf.filename or not pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File must be a PDF.")

    content = await pdf.read()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        try:
            staging = normalize_quote_from_pdf(tmp_path)
        except ExtractionError as e:
            meta = e.meta or {}
            return JSONResponse(
                status_code=422,
                content={
                    "status": "extraction_failed",
                    "failure_reason": meta.get("failure_reason"),
                    "document_class_detected": meta.get("document_class_detected"),
                    "extraction_source": meta.get("extraction_source"),
                    "error": str(e),
                    "meta": meta,
                },
            )
    finally:
        import os
        os.unlink(tmp_path)

    diagnostics = staging.get("document_diagnostics", {})
    staging_status = diagnostics.get("status", "success")

    # success when we have ≥1 accepted row; staged-failure otherwise.
    if staging_status == "success":
        return staging

    return JSONResponse(status_code=422, content=staging)


@app.post("/extract/quote/mapping")
async def extract_quote_mapping(
    quote_pdf: UploadFile = File(..., description="Subcontractor/vendor quote PDF"),
    dot_pdf: UploadFile = File(..., description="DOT proposal schedule PDF"),
):
    """
    C13 controlled mapping foundation.

    Runs the two governed lanes independently:
        - DOT lane (C8) extracts bid items from dot_pdf
        - Quote staging (C12) normalizes accepted quote rows from quote_pdf

    Then runs the deterministic mapping rules from
    quote_to_bid_mapping.map_quote_to_bid. NEVER guesses. Ambiguous and
    unmapped outcomes are explicit and traceable.

    Returns:
        {
            document_class_detected,
            mapping_status,
            accepted_rows,           -- from C12 staging (audit copy)
            mapping_results,         -- per-row outcomes
            mapping_diagnostics,
            quote_diagnostics,       -- C10 diagnostics from staging
            bid_summary,             -- C8 summary
        }
    """
    import tempfile

    if not quote_pdf.filename or not quote_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="quote_pdf must be a PDF.")
    if not dot_pdf.filename or not dot_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="dot_pdf must be a PDF.")

    quote_bytes = await quote_pdf.read()
    dot_bytes = await dot_pdf.read()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as qtmp:
        qtmp.write(quote_bytes)
        quote_path = qtmp.name
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as dtmp:
        dtmp.write(dot_bytes)
        dot_path = dtmp.name

    try:
        # DOT lane (locked C8 path).
        try:
            bid_rows, bid_summary = extract_bid_items_from_pdf(dot_path)
        except ExtractionError as e:
            meta = e.meta or {}
            return JSONResponse(
                status_code=422,
                content={
                    "status": "extraction_failed",
                    "stage": "dot_extraction",
                    "failure_reason": meta.get("failure_reason"),
                    "error": str(e),
                    "meta": meta,
                },
            )

        # Quote staging (governed C12 path).
        try:
            staging = normalize_quote_from_pdf(quote_path)
        except ExtractionError as e:
            meta = e.meta or {}
            return JSONResponse(
                status_code=422,
                content={
                    "status": "extraction_failed",
                    "stage": "quote_normalization",
                    "failure_reason": meta.get("failure_reason"),
                    "document_class_detected": meta.get("document_class_detected"),
                    "error": str(e),
                    "meta": meta,
                },
            )

        # C14: if the quote_pdf wasn't classified as a quote, stop here
        # with the real stage label. Otherwise downstream consumers would
        # see a generic pairing rejection and miss the root cause.
        if staging.get("document_class_detected") != "quote":
            return JSONResponse(
                status_code=422,
                content={
                    "status": "extraction_failed",
                    "stage": "quote_normalization",
                    "failure_reason": (staging.get("document_diagnostics") or {}).get("failure_reason"),
                    "document_class_detected": staging.get("document_class_detected"),
                    "quote_diagnostics": staging.get("document_diagnostics"),
                },
            )

        accepted_rows = staging.get("accepted_rows", [])

        # C14: pairing guardrail runs BEFORE mapping.
        pairing = analyze_pairing(accepted_rows, bid_rows)

        bid_summary_public = {
            "rows_extracted": bid_summary.get("rows_extracted"),
            "format_detected": bid_summary.get("format_detected"),
            "document_class": bid_summary.get("document_class"),
            "extraction_source": bid_summary.get("extraction_source"),
        }

        if not pairing["allow_mapping"]:
            return JSONResponse(
                status_code=422,
                content={
                    "status": "pairing_rejected",
                    "stage": "pairing_guardrail",
                    "document_class_detected": staging.get("document_class_detected"),
                    "pairing_diagnostics": pairing,
                    "mapping_status": "blocked_by_pairing",
                    "mapping_results": [],
                    "mapping_diagnostics": {
                        "mapped_count": 0,
                        "unmapped_count": 0,
                        "ambiguous_count": 0,
                        "rows_input": len(accepted_rows),
                        "bid_items_indexed": len(bid_rows),
                    },
                    "accepted_rows": accepted_rows,
                    "quote_diagnostics": staging.get("document_diagnostics"),
                    "bid_summary": bid_summary_public,
                },
            )

        mapping = map_quote_to_bid(accepted_rows, bid_rows)

        return {
            "document_class_detected": staging.get("document_class_detected"),
            "pairing_diagnostics": pairing,
            "mapping_status": mapping["mapping_status"],
            "accepted_rows": accepted_rows,
            "mapping_results": mapping["mapping_results"],
            "mapping_diagnostics": mapping["mapping_diagnostics"],
            "quote_diagnostics": staging.get("document_diagnostics"),
            "bid_summary": bid_summary_public,
        }
    finally:
        import os
        for p in (quote_path, dot_path):
            try:
                os.unlink(p)
            except OSError:
                pass


@app.post("/extract/quote/review")
async def extract_quote_review(
    quote_pdf: UploadFile = File(..., description="Subcontractor/vendor quote PDF"),
    dot_pdf: UploadFile = File(..., description="DOT proposal schedule PDF"),
):
    """
    C15 governed review packet.

    Runs:
        1. DOT lane (C8) extraction
        2. Quote staging (C12) normalization
        3. Pairing guardrail (C14) analysis
        4. Mapping foundation (C13) IF pairing allows
        5. Review packet (C15) assembly

    Returns the packet regardless of outcome. Blocked pairs receive a
    blocked packet with every accepted quote row stubbed and flagged
    blocked_by_pairing — no mapping outcomes. Trusted/weak pairs receive
    a ready/partial packet with full per-row review entries.

    HTTP status:
        200 when packet is ready or partial
        422 when packet is blocked (pairing rejected) — packet body still
            returned so reviewers can inspect diagnostics and row stubs
    """
    import tempfile

    if not quote_pdf.filename or not quote_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="quote_pdf must be a PDF.")
    if not dot_pdf.filename or not dot_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="dot_pdf must be a PDF.")

    quote_bytes = await quote_pdf.read()
    dot_bytes = await dot_pdf.read()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as qtmp:
        qtmp.write(quote_bytes)
        quote_path = qtmp.name
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as dtmp:
        dtmp.write(dot_bytes)
        dot_path = dtmp.name

    try:
        try:
            bid_rows, bid_summary = extract_bid_items_from_pdf(dot_path)
        except ExtractionError as e:
            return JSONResponse(
                status_code=422,
                content={
                    "status": "extraction_failed",
                    "stage": "dot_extraction",
                    "failure_reason": (e.meta or {}).get("failure_reason"),
                    "error": str(e),
                },
            )

        try:
            staging = normalize_quote_from_pdf(quote_path)
        except ExtractionError as e:
            return JSONResponse(
                status_code=422,
                content={
                    "status": "extraction_failed",
                    "stage": "quote_normalization",
                    "failure_reason": (e.meta or {}).get("failure_reason"),
                    "error": str(e),
                },
            )

        if staging.get("document_class_detected") != "quote":
            return JSONResponse(
                status_code=422,
                content={
                    "status": "extraction_failed",
                    "stage": "quote_normalization",
                    "failure_reason": (staging.get("document_diagnostics") or {}).get("failure_reason"),
                    "document_class_detected": staging.get("document_class_detected"),
                    "quote_diagnostics": staging.get("document_diagnostics"),
                },
            )

        accepted_rows = staging.get("accepted_rows", [])
        pairing = analyze_pairing(accepted_rows, bid_rows)

        bid_summary_public = {
            "rows_extracted": bid_summary.get("rows_extracted"),
            "format_detected": bid_summary.get("format_detected"),
            "document_class": bid_summary.get("document_class"),
            "extraction_source": bid_summary.get("extraction_source"),
        }

        if not pairing["allow_mapping"]:
            packet = build_review_packet(
                pairing_diagnostics=pairing,
                mapping_result=None,
                accepted_rows=accepted_rows,
                quote_diagnostics=staging.get("document_diagnostics") or {},
                bid_summary=bid_summary_public,
            )
            return JSONResponse(status_code=422, content=packet)

        mapping = map_quote_to_bid(accepted_rows, bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing,
            mapping_result=mapping,
            accepted_rows=accepted_rows,
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary=bid_summary_public,
        )
        return packet
    finally:
        import os
        for p in (quote_path, dot_path):
            try:
                os.unlink(p)
            except OSError:
                pass


@app.post("/extract/quote/reconcile")
async def extract_quote_reconcile(
    quote_pdf: UploadFile = File(..., description="Subcontractor/vendor quote PDF"),
    dot_pdf: UploadFile = File(..., description="DOT proposal schedule PDF"),
):
    """
    C16 deterministic reconciliation foundation.

    Chain: DOT → staging → pairing → mapping (if allowed) → review packet
    → reconciliation. Reconciliation compares only mapped rows and only
    fields that explicitly exist on both sides (unit, qty). Never
    infers, never resolves conflicts.

    HTTP status:
        200 when reconciliation is ready or partial
        422 when reconciliation is blocked (pairing rejected) — result
            body is still returned so reviewers see full diagnostics
    """
    import tempfile

    if not quote_pdf.filename or not quote_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="quote_pdf must be a PDF.")
    if not dot_pdf.filename or not dot_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="dot_pdf must be a PDF.")

    quote_bytes = await quote_pdf.read()
    dot_bytes = await dot_pdf.read()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as qtmp:
        qtmp.write(quote_bytes)
        quote_path = qtmp.name
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as dtmp:
        dtmp.write(dot_bytes)
        dot_path = dtmp.name

    try:
        try:
            bid_rows, bid_summary = extract_bid_items_from_pdf(dot_path)
        except ExtractionError as e:
            return JSONResponse(
                status_code=422,
                content={
                    "status": "extraction_failed",
                    "stage": "dot_extraction",
                    "failure_reason": (e.meta or {}).get("failure_reason"),
                    "error": str(e),
                },
            )

        try:
            staging = normalize_quote_from_pdf(quote_path)
        except ExtractionError as e:
            return JSONResponse(
                status_code=422,
                content={
                    "status": "extraction_failed",
                    "stage": "quote_normalization",
                    "failure_reason": (e.meta or {}).get("failure_reason"),
                    "error": str(e),
                },
            )

        if staging.get("document_class_detected") != "quote":
            return JSONResponse(
                status_code=422,
                content={
                    "status": "extraction_failed",
                    "stage": "quote_normalization",
                    "failure_reason": (staging.get("document_diagnostics") or {}).get("failure_reason"),
                    "document_class_detected": staging.get("document_class_detected"),
                    "quote_diagnostics": staging.get("document_diagnostics"),
                },
            )

        accepted_rows = staging.get("accepted_rows", [])
        pairing = analyze_pairing(accepted_rows, bid_rows)
        bid_summary_public = {
            "rows_extracted": bid_summary.get("rows_extracted"),
            "format_detected": bid_summary.get("format_detected"),
            "document_class": bid_summary.get("document_class"),
            "extraction_source": bid_summary.get("extraction_source"),
        }

        if not pairing["allow_mapping"]:
            packet = build_review_packet(
                pairing_diagnostics=pairing,
                mapping_result=None,
                accepted_rows=accepted_rows,
                quote_diagnostics=staging.get("document_diagnostics") or {},
                bid_summary=bid_summary_public,
            )
            recon = reconcile_packet(packet)
            contract = build_reconciliation_contract(recon, packet)
            classified = classify_contract(contract)
            prioritized = prioritize_classified_contract(classified)
            return JSONResponse(status_code=422, content=prioritized)

        mapping = map_quote_to_bid(accepted_rows, bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing,
            mapping_result=mapping,
            accepted_rows=accepted_rows,
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary=bid_summary_public,
        )
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        prioritized = prioritize_classified_contract(classified)
        return prioritized
    finally:
        import os
        for p in (quote_path, dot_path):
            try:
                os.unlink(p)
            except OSError:
                pass


def _build_prioritized_findings_from_pdfs(quote_path: str, dot_path: str):
    """
    Shared helper: run the full governed pipeline
    (DOT extract → quote staging → pairing → mapping → review packet →
    reconciliation contract → classification → findings packet →
    prioritization) for two uploaded PDF paths.

    Returns a tuple (http_status, payload). payload is either the
    prioritized findings packet or an extraction_failed error dict.
    """
    try:
        bid_rows, bid_summary = extract_bid_items_from_pdf(dot_path)
    except ExtractionError as e:
        return 422, {
            "status": "extraction_failed",
            "stage": "dot_extraction",
            "failure_reason": (e.meta or {}).get("failure_reason"),
            "error": str(e),
        }

    try:
        staging = normalize_quote_from_pdf(quote_path)
    except ExtractionError as e:
        return 422, {
            "status": "extraction_failed",
            "stage": "quote_normalization",
            "failure_reason": (e.meta or {}).get("failure_reason"),
            "error": str(e),
        }

    if staging.get("document_class_detected") != "quote":
        return 422, {
            "status": "extraction_failed",
            "stage": "quote_normalization",
            "failure_reason": (staging.get("document_diagnostics") or {}).get("failure_reason"),
            "document_class_detected": staging.get("document_class_detected"),
            "quote_diagnostics": staging.get("document_diagnostics"),
        }

    accepted_rows = staging.get("accepted_rows", [])
    pairing = analyze_pairing(accepted_rows, bid_rows)
    bid_summary_public = {
        "rows_extracted": bid_summary.get("rows_extracted"),
        "format_detected": bid_summary.get("format_detected"),
        "document_class": bid_summary.get("document_class"),
        "extraction_source": bid_summary.get("extraction_source"),
    }

    if not pairing["allow_mapping"]:
        packet = build_review_packet(
            pairing_diagnostics=pairing,
            mapping_result=None,
            accepted_rows=accepted_rows,
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary=bid_summary_public,
        )
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        findings = build_findings_packet(packet, classified)
        findings = prioritize_findings_packet(findings)
        return 422, findings

    mapping = map_quote_to_bid(accepted_rows, bid_rows)
    packet = build_review_packet(
        pairing_diagnostics=pairing,
        mapping_result=mapping,
        accepted_rows=accepted_rows,
        quote_diagnostics=staging.get("document_diagnostics") or {},
        bid_summary=bid_summary_public,
    )
    recon = reconcile_packet(packet)
    contract = build_reconciliation_contract(recon, packet)
    classified = classify_contract(contract)
    findings = build_findings_packet(packet, classified)
    findings = prioritize_findings_packet(findings)
    return 200, findings


def _save_two_pdfs(quote_bytes: bytes, dot_bytes: bytes):
    import tempfile
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as qtmp:
        qtmp.write(quote_bytes)
        quote_path = qtmp.name
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as dtmp:
        dtmp.write(dot_bytes)
        dot_path = dtmp.name
    return quote_path, dot_path


def _unlink_quiet(*paths: str) -> None:
    import os
    for p in paths:
        try:
            os.unlink(p)
        except OSError:
            pass


@app.post("/extract/quote/findings")
async def extract_quote_findings(
    quote_pdf: UploadFile = File(..., description="Subcontractor/vendor quote PDF"),
    dot_pdf: UploadFile = File(..., description="DOT proposal schedule PDF"),
):
    """
    C19 governed findings packet foundation.

    Assembles the full governed chain (DOT → staging → pairing → mapping →
    review packet → reconciliation contract → discrepancy classification)
    into a deterministic findings packet artifact suitable for office
    review. The packet is not narrative — it is a structured, auditable
    artifact.

    HTTP status:
        200 when packet is ready or partial
        422 when packet is blocked (pairing rejected) — the blocked
            packet is still returned in full
    """
    import tempfile

    if not quote_pdf.filename or not quote_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="quote_pdf must be a PDF.")
    if not dot_pdf.filename or not dot_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="dot_pdf must be a PDF.")

    quote_bytes = await quote_pdf.read()
    dot_bytes = await dot_pdf.read()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as qtmp:
        qtmp.write(quote_bytes)
        quote_path = qtmp.name
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as dtmp:
        dtmp.write(dot_bytes)
        dot_path = dtmp.name

    try:
        try:
            bid_rows, bid_summary = extract_bid_items_from_pdf(dot_path)
        except ExtractionError as e:
            return JSONResponse(
                status_code=422,
                content={
                    "status": "extraction_failed",
                    "stage": "dot_extraction",
                    "failure_reason": (e.meta or {}).get("failure_reason"),
                    "error": str(e),
                },
            )

        try:
            staging = normalize_quote_from_pdf(quote_path)
        except ExtractionError as e:
            return JSONResponse(
                status_code=422,
                content={
                    "status": "extraction_failed",
                    "stage": "quote_normalization",
                    "failure_reason": (e.meta or {}).get("failure_reason"),
                    "error": str(e),
                },
            )

        if staging.get("document_class_detected") != "quote":
            return JSONResponse(
                status_code=422,
                content={
                    "status": "extraction_failed",
                    "stage": "quote_normalization",
                    "failure_reason": (staging.get("document_diagnostics") or {}).get("failure_reason"),
                    "document_class_detected": staging.get("document_class_detected"),
                    "quote_diagnostics": staging.get("document_diagnostics"),
                },
            )

        accepted_rows = staging.get("accepted_rows", [])
        pairing = analyze_pairing(accepted_rows, bid_rows)
        bid_summary_public = {
            "rows_extracted": bid_summary.get("rows_extracted"),
            "format_detected": bid_summary.get("format_detected"),
            "document_class": bid_summary.get("document_class"),
            "extraction_source": bid_summary.get("extraction_source"),
        }

        if not pairing["allow_mapping"]:
            packet = build_review_packet(
                pairing_diagnostics=pairing,
                mapping_result=None,
                accepted_rows=accepted_rows,
                quote_diagnostics=staging.get("document_diagnostics") or {},
                bid_summary=bid_summary_public,
            )
            recon = reconcile_packet(packet)
            contract = build_reconciliation_contract(recon, packet)
            classified = classify_contract(contract)
            findings = build_findings_packet(packet, classified)
            findings = prioritize_findings_packet(findings)
            return JSONResponse(status_code=422, content=findings)

        mapping = map_quote_to_bid(accepted_rows, bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing,
            mapping_result=mapping,
            accepted_rows=accepted_rows,
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary=bid_summary_public,
        )
        recon = reconcile_packet(packet)
        contract = build_reconciliation_contract(recon, packet)
        classified = classify_contract(contract)
        findings = build_findings_packet(packet, classified)
        findings = prioritize_findings_packet(findings)
        return findings
    finally:
        import os
        for p in (quote_path, dot_path):
            try:
                os.unlink(p)
            except OSError:
                pass


@app.post("/extract/quote/findings/export/json")
async def extract_quote_findings_export_json(
    quote_pdf: UploadFile = File(...),
    dot_pdf: UploadFile = File(...),
):
    """C22 stable JSON export of the governed findings packet."""
    if not quote_pdf.filename or not quote_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="quote_pdf must be a PDF.")
    if not dot_pdf.filename or not dot_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="dot_pdf must be a PDF.")
    quote_bytes = await quote_pdf.read()
    dot_bytes = await dot_pdf.read()
    quote_path, dot_path = _save_two_pdfs(quote_bytes, dot_bytes)
    try:
        status, payload = _build_prioritized_findings_from_pdfs(quote_path, dot_path)
        if isinstance(payload, dict) and payload.get("status") == "extraction_failed":
            return JSONResponse(status_code=status, content=payload)
        export = export_findings_json(payload)
        return JSONResponse(status_code=status, content=export)
    finally:
        _unlink_quiet(quote_path, dot_path)


@app.post("/extract/quote/findings/export/csv")
async def extract_quote_findings_export_csv(
    quote_pdf: UploadFile = File(...),
    dot_pdf: UploadFile = File(...),
):
    """C22 office-friendly tabular CSV export."""
    if not quote_pdf.filename or not quote_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="quote_pdf must be a PDF.")
    if not dot_pdf.filename or not dot_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="dot_pdf must be a PDF.")
    quote_bytes = await quote_pdf.read()
    dot_bytes = await dot_pdf.read()
    quote_path, dot_path = _save_two_pdfs(quote_bytes, dot_bytes)
    try:
        status, payload = _build_prioritized_findings_from_pdfs(quote_path, dot_path)
        if isinstance(payload, dict) and payload.get("status") == "extraction_failed":
            return JSONResponse(status_code=status, content=payload)
        csv_text = export_findings_csv(payload)
        return Response(
            content=csv_text,
            status_code=status,
            media_type="text/csv",
            headers={
                "Content-Disposition": 'attachment; filename="findings.csv"',
                "X-Packet-Status": payload.get("packet_status") or "unknown",
            },
        )
    finally:
        _unlink_quiet(quote_path, dot_path)


@app.post("/extract/quote/findings/export/report")
async def extract_quote_findings_export_report(
    quote_pdf: UploadFile = File(...),
    dot_pdf: UploadFile = File(...),
):
    """C22 engineer-ready structured report payload. PDF rendering is
    deferred — the payload is a stable section-oriented dict a downstream
    renderer can walk."""
    if not quote_pdf.filename or not quote_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="quote_pdf must be a PDF.")
    if not dot_pdf.filename or not dot_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="dot_pdf must be a PDF.")
    quote_bytes = await quote_pdf.read()
    dot_bytes = await dot_pdf.read()
    quote_path, dot_path = _save_two_pdfs(quote_bytes, dot_bytes)
    try:
        status, payload = _build_prioritized_findings_from_pdfs(quote_path, dot_path)
        if isinstance(payload, dict) and payload.get("status") == "extraction_failed":
            return JSONResponse(status_code=status, content=payload)
        report = export_findings_report(payload)
        return JSONResponse(status_code=status, content=report)
    finally:
        _unlink_quiet(quote_path, dot_path)


@app.post("/extract/quote/findings/workflow")
async def extract_quote_findings_workflow(
    quote_pdf: UploadFile = File(...),
    dot_pdf: UploadFile = File(...),
    review_metadata: Optional[str] = Form(None, description="Optional JSON blob of append-only reviewer metadata"),
):
    """
    C25 office workflow packet endpoint.

    Wraps a governed findings packet in a deterministic review queue
    artifact. Append-only reviewer metadata may be supplied as a JSON
    blob; it never mutates governed findings truth.
    """
    if not quote_pdf.filename or not quote_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="quote_pdf must be a PDF.")
    if not dot_pdf.filename or not dot_pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="dot_pdf must be a PDF.")

    parsed_metadata: Optional[Dict[str, Any]] = None
    if review_metadata:
        try:
            parsed_metadata = json.loads(review_metadata)
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=400,
                detail=f"review_metadata must be valid JSON: {e}",
            )
        if not isinstance(parsed_metadata, dict):
            raise HTTPException(
                status_code=400,
                detail="review_metadata must decode to a JSON object.",
            )

    quote_bytes = await quote_pdf.read()
    dot_bytes = await dot_pdf.read()
    quote_path, dot_path = _save_two_pdfs(quote_bytes, dot_bytes)
    try:
        status, payload = _build_prioritized_findings_from_pdfs(quote_path, dot_path)
        if isinstance(payload, dict) and payload.get("status") == "extraction_failed":
            return JSONResponse(status_code=status, content=payload)
        workflow = build_workflow_packet(payload, parsed_metadata)
        return JSONResponse(status_code=status, content=workflow)
    finally:
        _unlink_quiet(quote_path, dot_path)


@app.post("/extract/auto")
async def extract_auto(
    pdf: UploadFile = File(..., description="PDF document (DOT schedule or quote)"),
):
    """
    Auto-routing extraction: classifies document, routes to correct pipeline.

    Returns structured rows with document_class in summary.
    """
    import tempfile

    if not pdf.filename or not pdf.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File must be a PDF.")

    content = await pdf.read()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        rows, summary = extract_pdf_auto(tmp_path)
    except ExtractionError as e:
        meta = e.meta or {}
        return JSONResponse(
            status_code=422,
            content={
                "status": "extraction_failed",
                "failure_reason": meta.get("failure_reason"),
                "document_class_detected": meta.get("document_class_detected", "unknown"),
                "extraction_source": meta.get("extraction_source"),
                "error": str(e),
                "meta": meta,
            },
        )
    finally:
        import os
        os.unlink(tmp_path)

    return {
        "status": "success",
        "document_class_detected": summary.get("document_class_detected"),
        "extraction_source": summary.get("extraction_source"),
        "failure_reason": None,
        "rows": rows,
        "row_count": len(rows),
        "summary": summary,
    }


# ---------------------------------------------------------------------------
# Mapping CRUD
# ---------------------------------------------------------------------------

def _is_valid_mapping_name(name: str) -> bool:
    """Alphanumeric, hyphens, and underscores only."""
    return bool(name) and all(c.isalnum() or c in ("-", "_") for c in name)


@app.post("/mapping/save")
async def save_mapping(
    name: str = Form(..., description="Mapping name (alphanumeric, hyphens, underscores)"),
    actor: str = Form(..., description="User or system saving the mapping"),
    mapping: UploadFile = File(..., description='JSON file: {"line_number": "dot_item", ...}'),
    project: Optional[str] = Form(None, description="Project identifier for auto-selection in /validate"),
    vendor: Optional[str] = Form(None, description="Vendor identifier for auto-selection in /validate"),
):
    """Save a line-to-item mapping for reuse in /validate via mapping_name or project/vendor auto-selection."""
    name = name.strip()
    if not _is_valid_mapping_name(name):
        raise HTTPException(
            status_code=400,
            detail="name must be alphanumeric with hyphens/underscores only",
        )

    mapping_bytes = await mapping.read()
    try:
        mapping_dict = json.loads(mapping_bytes)
    except (json.JSONDecodeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"mapping must be valid JSON: {e}")
    if not isinstance(mapping_dict, dict):
        raise HTTPException(status_code=400, detail="mapping must be a JSON object")

    _project = project.strip() if project and project.strip() else None
    _vendor = vendor.strip() if vendor and vendor.strip() else None
    record = mapping_store.save(name, mapping_dict, actor.strip(), project=_project, vendor=_vendor)
    return record


@app.get("/mapping/list")
def list_mappings():
    """List all saved mappings with project/vendor metadata (without full mapping data)."""
    return {"mappings": mapping_store.list_records()}


@app.get("/mapping/{name}")
def get_mapping(name: str):
    """Retrieve a saved mapping by name."""
    try:
        return mapping_store.load_record(name)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Mapping not found: {name}")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

@app.post("/validate")
async def validate(
    actor: str = Form(..., description="User or system identity performing the validation"),
    doc_type: str = Form(
        "PRIME_BID",
        description="Validation mode: PRIME_BID (pdf + bid_items) or QUOTE (bid_items + quote_lines)",
    ),
    pdf: Optional[UploadFile] = File(
        None, description="PDF document to validate (required for PRIME_BID mode)",
    ),
    bid_items: Optional[UploadFile] = File(
        None, description="Bid items spreadsheet — CSV or XLSX (always required)",
    ),
    override: Optional[bool] = Form(
        False, description="Set true to override FAIL findings (requires override_reason)",
    ),
    override_reason: Optional[str] = Form(
        None, description="Justification text — required when override=true",
    ),
    override_actor: Optional[str] = Form(
        None, description="Actor authorizing the override (defaults to actor)",
    ),
    quote_lines: Optional[UploadFile] = File(
        None, description="Quote lines spreadsheet — CSV or XLSX (required for QUOTE mode)",
    ),
    line_to_item_mapping: Optional[UploadFile] = File(
        None,
        description=(
            'Optional JSON file mapping proposal line numbers to DOT item numbers, '
            'e.g. {"520": "2524-6765010"}. Only used when quote_lines is provided.'
        ),
    ),
    mapping_name: Optional[str] = Form(
        None,
        description="Name of a saved mapping to auto-load (alternative to uploading line_to_item_mapping file)",
    ),
    project: Optional[str] = Form(
        None,
        description="Project identifier — used for auto-selecting a saved mapping when no explicit mapping is provided",
    ),
    vendor: Optional[str] = Form(
        None,
        description="Vendor identifier — used for auto-selecting a saved mapping when no explicit mapping is provided",
    ),
):
    """
    Validate bid documents and optionally reconcile against quote data.

    **Modes:**
    - **PRIME_BID** — requires `pdf` + `bid_items`. Runs PDF integrity checks and bid item validation.
    - **QUOTE** — requires `bid_items` + `quote_lines`. Runs bid validation and quote reconciliation.
    - **Combined** — provide `pdf` + `bid_items` + `quote_lines` for full validation.

    **Line mapping selection (precedence):**
    1. `line_to_item_mapping` file upload (explicit, highest priority)
    2. `mapping_name` — load a saved mapping by name
    3. `project` / `vendor` — auto-select a saved mapping (must match exactly one)
    4. None — no mapping applied
    """
    actor = (actor or "").strip()
    if not actor:
        raise HTTPException(status_code=400, detail="actor is required")

    doc_type_norm = (doc_type or "PRIME_BID").strip().upper()
    if doc_type_norm not in ("PRIME_BID", "QUOTE"):
        raise HTTPException(status_code=400, detail="doc_type must be PRIME_BID or QUOTE")

    if doc_type_norm == "PRIME_BID" and pdf is None:
        raise HTTPException(status_code=400, detail="pdf is required for PRIME_BID")

    if bid_items is None:
        raise HTTPException(status_code=400, detail="bid_items (CSV/XLSX) is required")

    if doc_type_norm == "QUOTE" and quote_lines is None:
        raise HTTPException(status_code=400, detail="QUOTE mode requires quote_lines upload")

    if line_to_item_mapping is not None and quote_lines is None:
        raise HTTPException(
            status_code=400,
            detail="line_to_item_mapping requires quote_lines (mapping has no target without quote data)",
        )

    if mapping_name and mapping_name.strip() and quote_lines is None:
        raise HTTPException(
            status_code=400,
            detail="mapping_name requires quote_lines (mapping has no target without quote data)",
        )

    run = storage.create_run(actor=actor)

    findings: List[Finding] = []
    checks_executed: List[str] = []
    bid_summary: Optional[Dict[str, Any]] = None
    quote_summary: Optional[Dict[str, Any]] = None

    # Save + run PDF checks only if provided
    if pdf is not None:
        if not (pdf.filename or "").lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail="pdf must be a .pdf file")
        saved_pdf_path = await storage.save_upload(run.run_id, pdf)

        checks_executed += [
            "pdf_open",
            "blank_near_blank_pages",
            "duplicate_pages",
            "missing_last_page_heuristic",
        ]
        pdf_findings = validate_pdf_integrity(saved_pdf_path)
        pdf_findings = adjust_pdf_findings_by_doc_type(pdf_findings, doc_type_norm)
        findings += pdf_findings

    # Save bid_items
    ext = os.path.splitext((bid_items.filename or "").lower())[1]
    if ext not in (".csv", ".xlsx", ".xlsm", ".xltx", ".xltm"):
        raise HTTPException(status_code=400, detail="bid_items must be a CSV or XLSX file")
    saved_bid_path = await storage.save_upload(run.run_id, bid_items)

    # Ingest bid items (catch IngestError -> NO 500)
    checks_executed += ["bid_items_ingest"]
    try:
        bid_rows_norm, ingest_meta = ingest_bid_items(saved_bid_path)
    except IngestError as e:
        payload = {
            "run_id": run.run_id,
            "doc_type": doc_type_norm,
            "overall_status": "FAIL",
            "findings": [{
                "type": "bid_ingest_error",
                "severity": "FAIL",
                "message": str(e),
                "pages": [],
                "row_index": None,
                "item_ref": None,
                "meta": getattr(e, "meta", {}) or {},
            }],
            "bid_summary": {
                "invalid": 0,
                "blank_items": 0,
                "ingestion": {
                    "rows_raw_total": 0,
                    "rows_skipped_summary": 0,
                    "mapping_missing": (getattr(e, "meta", {}) or {}).get("mapping_missing", []),
                    "mapping_ambiguous": (getattr(e, "meta", {}) or {}).get("mapping_ambiguous", {}),
                    "mapping_used": (getattr(e, "meta", {}) or {}).get("mapping_used"),
                    "alias_dictionary": (getattr(e, "meta", {}) or {}).get("mapping_alias_dict"),
                }
            },
            "quote_summary": None,
            "audit_log": storage.audit_log_path(),
        }
        # 400 is fine; frontend should show error message OR you can keep 200 if you prefer.
        return JSONResponse(payload, status_code=400)

    # Bid validation
    checks_executed += ["bid_items_guardrails", "mobilization_rule"]
    bid_findings, bid_summary = validate_bid_items(bid_rows_norm, ingest_meta)
    findings += bid_findings

    bid_summary = bid_summary or {}
    bid_summary["ingestion"] = {
        "rows_raw_total": ingest_meta.get("rows_raw_total"),
        "rows_skipped_summary": ingest_meta.get("rows_skipped_summary"),
        "mapping_missing": ingest_meta.get("mapping_missing"),
        "mapping_ambiguous": ingest_meta.get("mapping_ambiguous"),
        "mapping_used": ingest_meta.get("mapping_used"),
        "alias_dictionary": ingest_meta.get("mapping_alias_dict"),
        "normalization": ingest_meta.get("normalization"),
    }

    # Quote reconciliation (structured pipeline)
    if quote_lines is not None:
        extq = os.path.splitext((quote_lines.filename or "").lower())[1]
        if extq not in (".csv", ".xlsx", ".xlsm", ".xltx", ".xltm"):
            raise HTTPException(status_code=400, detail="quote_lines must be a CSV or XLSX file")

        saved_quote_path = await storage.save_upload(run.run_id, quote_lines)

        checks_executed += [
            "quote_lines_ingest",
            "quote_reconciliation_unit_price_rule",
            "quote_totals_crosscheck",
        ]

        # Mapping selection — strict precedence:
        #   1. file upload  2. mapping_name  3. project/vendor auto-select  4. none
        mapping_dict = None
        mapping_source = None
        mapping_name_used = None

        if line_to_item_mapping is not None:
            mapping_bytes = await line_to_item_mapping.read()
            try:
                mapping_dict = json.loads(mapping_bytes)
            except (json.JSONDecodeError, ValueError) as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"line_to_item_mapping must be valid JSON: {e}",
                )
            if not isinstance(mapping_dict, dict):
                raise HTTPException(
                    status_code=400,
                    detail="line_to_item_mapping must be a JSON object mapping line numbers to item numbers",
                )
            mapping_source = "file_upload"
            checks_executed.append("line_number_mapping")

        elif mapping_name and mapping_name.strip():
            mapping_name_clean = mapping_name.strip()
            if not mapping_store.exists(mapping_name_clean):
                raise HTTPException(
                    status_code=404,
                    detail=f"Saved mapping not found: {mapping_name_clean}",
                )
            mapping_dict = mapping_store.load(mapping_name_clean)
            mapping_source = "named"
            mapping_name_used = mapping_name_clean
            checks_executed.append("line_number_mapping")

        else:
            # Tier 3: auto-select by project/vendor (deterministic, exact match)
            _project = project.strip() if project and project.strip() else None
            _vendor = vendor.strip() if vendor and vendor.strip() else None
            if _project or _vendor:
                candidates = mapping_store.find_by_context(project=_project, vendor=_vendor)
                if len(candidates) == 1:
                    mapping_name_used = candidates[0]
                    mapping_dict = mapping_store.load(mapping_name_used)
                    mapping_source = "auto_selected"
                    checks_executed.append("line_number_mapping")
                elif len(candidates) > 1:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"Ambiguous mapping: {len(candidates)} saved mappings match "
                            f"project={_project!r} vendor={_vendor!r}. "
                            f"Matches: {candidates}. "
                            f"Provide mapping_name to disambiguate."
                        ),
                    )

        # IMPORTANT: catch pipeline errors so we never 500
        try:
            q_findings, quote_summary, quote_ingest_meta = run_structured_pipeline(
                quote_file_path=saved_quote_path,
                bid_rows=bid_rows_norm,
                line_to_item_mapping=mapping_dict,
            )
        except Exception as e:
            # If quote ingest defines a custom IngestError with .meta, we preserve it.
            meta = getattr(e, "meta", {}) or {}
            payload = {
                "run_id": run.run_id,
                "doc_type": doc_type_norm,
                "overall_status": "FAIL",
                "findings": [{
                    "type": "quote_ingest_error",
                    "severity": "FAIL",
                    "message": str(e),
                    "pages": [],
                    "row_index": None,
                    "item_ref": None,
                    "meta": meta,
                }],
                "bid_summary": bid_summary,
                "quote_summary": {
                    "mapped_bid_subtotal": 0.0,
                    "quote_subtotal": 0.0,
                    "unmatched_quote_lines_count": 0,
                    "unmatched_quote_lines": [],
                    "ambiguous_quote_lines_count": 0,
                    "ambiguous_quote_lines": [],
                    "comparisons": [],
                    "totals_mismatch": False,
                    "tolerance": 0.0,
                    "mapping_fields": {"match": "item (normalized exact match)", "unit": "exact match"},
                    "ingestion": {
                        "rows_raw_total": meta.get("rows_raw_total"),
                        "mapping_missing": meta.get("mapping_missing"),
                        "mapping_ambiguous": meta.get("mapping_ambiguous"),
                        "mapping_used": meta.get("mapping_used"),
                        "alias_dictionary": meta.get("mapping_alias_dict"),
                        "normalization": meta.get("normalization"),
                    },
                },
                "audit_log": storage.audit_log_path(),
            }
            return JSONResponse(payload, status_code=400)

        findings += q_findings

        quote_summary = quote_summary or {}
        quote_summary["ingestion"] = {
            "rows_raw_total": quote_ingest_meta.get("rows_raw_total"),
            "mapping_missing": quote_ingest_meta.get("mapping_missing"),
            "mapping_ambiguous": quote_ingest_meta.get("mapping_ambiguous"),
            "mapping_used": quote_ingest_meta.get("mapping_used"),
            "alias_dictionary": quote_ingest_meta.get("mapping_alias_dict"),
            "normalization": quote_ingest_meta.get("normalization"),
            "line_mapping_applied": quote_ingest_meta.get("line_mapping_applied", False),
            "mapping_source": mapping_source,
            "mapping_name_used": mapping_name_used,
        }

    overall_status = compute_overall_status(findings)

    ov: Optional[OverrideInfo] = None
    if override:
        if not (override_reason and override_reason.strip()):
            raise HTTPException(status_code=400, detail="override_reason is required when override=true")
        ov = OverrideInfo(
            override=True,
            override_reason=override_reason.strip(),
            override_actor=(override_actor or actor).strip(),
        )

    event = AuditEvent.build(
        run_id=run.run_id,
        actor=actor,
        input_files=storage.describe_input_files(run.run_id),
        context={"doc_type": doc_type_norm},
        checks_executed=checks_executed,
        findings=findings,
        overall_status=overall_status,
        override=ov,
        event_type="validation_run",
    )
    audit.append_event(event)

    return {
        "run_id": run.run_id,
        "doc_type": doc_type_norm,
        "overall_status": overall_status,
        "findings": [f.model_dump() for f in findings],
        "bid_summary": bid_summary,
        "quote_summary": quote_summary,
        "audit_log": storage.audit_log_path(),
    }


# ---------------------------------------------------------------------------
# Operator report wrapper
# ---------------------------------------------------------------------------

@app.post("/validate/report")
async def validate_report(
    actor: str = Form(..., description="User or system identity performing the validation"),
    doc_type: str = Form(
        "PRIME_BID",
        description="Validation mode: PRIME_BID (pdf + bid_items) or QUOTE (bid_items + quote_lines)",
    ),
    pdf: Optional[UploadFile] = File(None, description="PDF document (required for PRIME_BID)"),
    bid_items: Optional[UploadFile] = File(None, description="Bid items CSV/XLSX (always required)"),
    override: Optional[bool] = Form(False, description="Override FAIL findings"),
    override_reason: Optional[str] = Form(None, description="Justification when override=true"),
    override_actor: Optional[str] = Form(None, description="Actor authorizing override"),
    quote_lines: Optional[UploadFile] = File(None, description="Quote lines CSV/XLSX (required for QUOTE)"),
    line_to_item_mapping: Optional[UploadFile] = File(None, description="Mapping JSON file"),
    mapping_name: Optional[str] = Form(None, description="Saved mapping name"),
    project: Optional[str] = Form(None, description="Project for auto-selecting mapping"),
    vendor: Optional[str] = Form(None, description="Vendor for auto-selecting mapping"),
):
    """
    Run validation and return an **operator-readable report**.

    Same inputs as `/validate`. Response wraps the full validation output with:
    - **run_summary** — status, description, finding counts
    - **mapping_provenance** — what mapping was used and how it was selected
    - **counts** — matched/unmatched/violations at a glance
    - **key_findings** — findings grouped by category with operator-readable summaries
    - **next_action** — single most important action for the operator
    - **detail** — full raw `/validate` response preserved for drill-down
    """
    raw = await validate(
        actor=actor, doc_type=doc_type, pdf=pdf, bid_items=bid_items,
        override=override, override_reason=override_reason,
        override_actor=override_actor, quote_lines=quote_lines,
        line_to_item_mapping=line_to_item_mapping,
        mapping_name=mapping_name, project=project, vendor=vendor,
    )

    # validate() returns dict on success, JSONResponse on ingest errors
    if isinstance(raw, JSONResponse):
        body = json.loads(raw.body)
        return JSONResponse(build_operator_report(body), status_code=raw.status_code)

    return build_operator_report(raw)


# ---------------------------------------------------------------------------
# Export endpoints  (thin wrappers: validate -> report -> render)
# ---------------------------------------------------------------------------

async def _run_report(
    actor, doc_type, pdf, bid_items, override, override_reason,
    override_actor, quote_lines, line_to_item_mapping,
    mapping_name, project, vendor,
) -> Dict[str, Any]:
    """Shared helper: run validation, build operator report, return dict."""
    raw = await validate(
        actor=actor, doc_type=doc_type, pdf=pdf, bid_items=bid_items,
        override=override, override_reason=override_reason,
        override_actor=override_actor, quote_lines=quote_lines,
        line_to_item_mapping=line_to_item_mapping,
        mapping_name=mapping_name, project=project, vendor=vendor,
    )
    if isinstance(raw, JSONResponse):
        return build_operator_report(json.loads(raw.body))
    return build_operator_report(raw)


@app.post("/validate/export/json")
async def export_json(
    actor: str = Form(...), doc_type: str = Form("PRIME_BID"),
    pdf: Optional[UploadFile] = File(None),
    bid_items: Optional[UploadFile] = File(None),
    override: Optional[bool] = Form(False),
    override_reason: Optional[str] = Form(None),
    override_actor: Optional[str] = Form(None),
    quote_lines: Optional[UploadFile] = File(None),
    line_to_item_mapping: Optional[UploadFile] = File(None),
    mapping_name: Optional[str] = Form(None),
    project: Optional[str] = Form(None),
    vendor: Optional[str] = Form(None),
):
    """Download the operator report as a JSON file."""
    report = await _run_report(
        actor, doc_type, pdf, bid_items, override, override_reason,
        override_actor, quote_lines, line_to_item_mapping,
        mapping_name, project, vendor,
    )
    run_id = report.get("run_summary", {}).get("run_id", "report")
    return Response(
        content=json.dumps(report, indent=2, default=str),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{run_id}.json"'},
    )


@app.post("/validate/export/html")
async def export_html(
    actor: str = Form(...), doc_type: str = Form("PRIME_BID"),
    pdf: Optional[UploadFile] = File(None),
    bid_items: Optional[UploadFile] = File(None),
    override: Optional[bool] = Form(False),
    override_reason: Optional[str] = Form(None),
    override_actor: Optional[str] = Form(None),
    quote_lines: Optional[UploadFile] = File(None),
    line_to_item_mapping: Optional[UploadFile] = File(None),
    mapping_name: Optional[str] = Form(None),
    project: Optional[str] = Form(None),
    vendor: Optional[str] = Form(None),
):
    """Download a human-readable HTML validation report."""
    report = await _run_report(
        actor, doc_type, pdf, bid_items, override, override_reason,
        override_actor, quote_lines, line_to_item_mapping,
        mapping_name, project, vendor,
    )
    run_id = report.get("run_summary", {}).get("run_id", "report")
    return Response(
        content=render_html(report),
        media_type="text/html",
        headers={"Content-Disposition": f'attachment; filename="{run_id}.html"'},
    )


@app.post("/validate/export/csv")
async def export_csv(
    actor: str = Form(...), doc_type: str = Form("PRIME_BID"),
    pdf: Optional[UploadFile] = File(None),
    bid_items: Optional[UploadFile] = File(None),
    override: Optional[bool] = Form(False),
    override_reason: Optional[str] = Form(None),
    override_actor: Optional[str] = Form(None),
    quote_lines: Optional[UploadFile] = File(None),
    line_to_item_mapping: Optional[UploadFile] = File(None),
    mapping_name: Optional[str] = Form(None),
    project: Optional[str] = Form(None),
    vendor: Optional[str] = Form(None),
):
    """Download findings as a CSV spreadsheet."""
    report = await _run_report(
        actor, doc_type, pdf, bid_items, override, override_reason,
        override_actor, quote_lines, line_to_item_mapping,
        mapping_name, project, vendor,
    )
    run_id = report.get("run_summary", {}).get("run_id", "report")
    return Response(
        content=render_csv(report),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{run_id}.csv"'},
    )


def compute_overall_status(findings: List[Finding]) -> str:
    if any(f.severity == "FAIL" for f in findings):
        return "FAIL"
    if any(f.severity == "WARN" for f in findings):
        return "WARN"
    return "PASS"


# ---------------------------------------------------------------------------
# C83 — API endpoint surface for canonical artifacts, orchestration, exports,
# timeline, and demo harness.
# ---------------------------------------------------------------------------

def _repo():
    return get_default_repository()


@app.get("/canonical/schema-types")
def canonical_schema_types():
    return {
        "supported_types": list_supported_artifact_types(),
        "descriptors": [get_schema_descriptor(t) for t in list_supported_artifact_types()],
    }


@app.post("/canonical/artifacts/{artifact_type}")
async def save_canonical_artifact(artifact_type: str, request: Dict[str, Any]):
    """Save a canonical artifact. Body: {"artifact": {...}, "metadata": {...}}."""
    artifact = (request or {}).get("artifact") or {}
    metadata = (request or {}).get("metadata") or {}
    rec = _repo().save(artifact_type, artifact, metadata)
    return rec


@app.get("/canonical/artifacts/{artifact_type}/latest")
def get_latest_artifact(artifact_type: str, bid_id: Optional[str] = None,
                        job_id: Optional[str] = None):
    rec = _repo().latest(artifact_type, bid_id=bid_id, job_id=job_id)
    if rec is None:
        return JSONResponse(status_code=404, content={"error": "not_found",
                                                      "artifact_type": artifact_type})
    return rec


@app.get("/canonical/artifacts/{artifact_type}/history")
def get_artifact_history(artifact_type: str, bid_id: Optional[str] = None,
                         job_id: Optional[str] = None):
    return {"records": _repo().history(artifact_type, bid_id=bid_id, job_id=job_id)}


@app.get("/canonical/artifacts/by-bid/{bid_id}")
def get_artifacts_by_bid(bid_id: str):
    return {"bid_id": bid_id, "records": _repo().by_bid_id(bid_id)}


@app.get("/canonical/artifacts/by-record-id/{record_id}")
def get_artifact_by_record_id(record_id: str):
    rec = _repo().by_record_id(record_id)
    if rec is None:
        return JSONResponse(status_code=404, content={"error": "not_found", "record_id": record_id})
    return rec


@app.get("/canonical/artifacts/{artifact_type}/revision/{revision_sequence}")
def get_artifact_by_revision(artifact_type: str, revision_sequence: int,
                              bid_id: Optional[str] = None):
    rec = _repo().by_revision_sequence(artifact_type, revision_sequence, bid_id=bid_id)
    if rec is None:
        return JSONResponse(status_code=404, content={"error": "not_found"})
    return rec


@app.get("/canonical/artifacts/by-record-id/{record_id}/lineage")
def get_artifact_lineage(record_id: str):
    return {"record_id": record_id, "lineage": _repo().lineage(record_id)}


@app.get("/canonical/repository/summary")
def repository_summary():
    return _repo().repository_summary()


@app.post("/canonical/repository/reset")
def repository_reset():
    reset_default_repository()
    return {"reset": True, "repository_summary": _repo().repository_summary()}


# ---------------------------------------------------------------------------
# Control room payloads
# ---------------------------------------------------------------------------

@app.get("/control-room/quote-case/{job_id}")
def get_quote_case_payload(job_id: str):
    return assemble_quote_case_payload(_repo(), job_id)


@app.get("/control-room/package-overview/{bid_id}")
def get_package_overview_payload(bid_id: str):
    return assemble_package_overview_payload(_repo(), bid_id)


@app.get("/control-room/authority-action")
def get_authority_action_payload(bid_id: Optional[str] = None):
    return assemble_authority_action_payload(_repo(), bid_id)


@app.get("/control-room/bid-readiness/{bid_id}")
def get_bid_readiness_payload(bid_id: str):
    return assemble_bid_readiness_payload(_repo(), bid_id)


@app.get("/control-room/timeline")
def get_timeline_payload(bid_id: Optional[str] = None, job_id: Optional[str] = None):
    return assemble_timeline_payload(_repo(), bid_id=bid_id, job_id=job_id)


# ---------------------------------------------------------------------------
# Export generation
# ---------------------------------------------------------------------------

@app.get("/exports/sub-clarification/{job_id}")
def export_sub_clarification(job_id: str, revision_sequence: Optional[int] = None):
    return generate_sub_clarification_export(_repo(), job_id, revision_sequence)


@app.get("/exports/estimator-review/{job_id}")
def export_estimator_review(job_id: str, revision_sequence: Optional[int] = None):
    return generate_estimator_review_export(_repo(), job_id, revision_sequence)


@app.get("/exports/authority-action/{bid_id}")
def export_authority_action(bid_id: str, revision_sequence: Optional[int] = None):
    return generate_authority_action_export(_repo(), bid_id, revision_sequence)


@app.get("/exports/bid-readiness/{bid_id}")
def export_bid_readiness(bid_id: str, revision_sequence: Optional[int] = None):
    return generate_bid_readiness_export(_repo(), bid_id, revision_sequence)


@app.get("/exports/final-carry/{bid_id}")
def export_final_carry(bid_id: str, revision_sequence: Optional[int] = None):
    return generate_final_carry_export(_repo(), bid_id, revision_sequence)


# ---------------------------------------------------------------------------
# Demo harness + seed scenarios
# ---------------------------------------------------------------------------

@app.get("/demo/scenarios")
def list_demo_scenarios():
    return {"scenarios": list_scenarios()}


@app.post("/demo/run/{scenario_id}")
def run_demo_scenario(scenario_id: str, bid_id: Optional[str] = None):
    try:
        out = run_scenario_e2e(scenario_id, bid_id=bid_id)
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    return out


@app.get("/demo/fixture")
def get_demo_fixture(bid_id: str = "demo-bid-1"):
    return build_demo_fixture(bid_id)


@app.post("/demo/run-e2e")
def run_demo_e2e(request: Dict[str, Any] = None):
    req = request or {}
    fixture = req.get("fixture")
    carry_decision = req.get("carry_decision", "proceed_with_caveats")
    decided_by = req.get("decided_by", "api_estimator")
    return run_e2e_demo(fixture=fixture, carry_decision=carry_decision, decided_by=decided_by)


# ---------------------------------------------------------------------------
# C86-C91 hardening endpoints
# ---------------------------------------------------------------------------

@app.get("/api/error-codes")
def api_error_codes():
    return {"error_codes": list_error_codes()}


@app.post("/api/scope-check")
def api_scope_check(request: Dict[str, Any] = None):
    req = request or {}
    record_id = req.get("record_id")
    if not record_id:
        return to_http_response(build_error(ERR_MALFORMED_PAYLOAD,
                                             hint="record_id required"))
    rec = _repo().by_record_id(record_id)
    if rec is None:
        return to_http_response(build_error(ERR_RECORD_NOT_FOUND,
                                             detail={"record_id": record_id}))
    result = check_scope(rec,
                         bid_id=req.get("bid_id"),
                         job_id=req.get("job_id"),
                         org_id=req.get("org_id"),
                         owner_id=req.get("owner_id"))
    if not result["ok"]:
        return to_http_response(build_error(ERR_SCOPE_MISMATCH,
                                             detail=result))
    return result


@app.post("/api/revision-diff")
def api_revision_diff(request: Dict[str, Any] = None):
    req = request or {}
    artifact_type = req.get("artifact_type")
    bid_id = req.get("bid_id")
    job_id = req.get("job_id")
    before_rev = req.get("before_revision_sequence")
    after_rev = req.get("after_revision_sequence")
    if not artifact_type:
        return to_http_response(build_error(ERR_MALFORMED_PAYLOAD,
                                             hint="artifact_type required"))

    repo = _repo()
    if before_rev is not None and after_rev is not None:
        before = repo.by_revision_sequence(artifact_type, before_rev, bid_id=bid_id)
        after = repo.by_revision_sequence(artifact_type, after_rev, bid_id=bid_id)
        if after is None:
            return to_http_response(build_error(ERR_MISSING_REVISION,
                                                 detail={"artifact_type": artifact_type,
                                                         "revision_sequence": after_rev}))
        return diff_revisions(before, after)

    history = repo.history(artifact_type, bid_id=bid_id, job_id=job_id)
    if not history:
        return to_http_response(build_error(ERR_RECORD_NOT_FOUND,
                                             detail={"artifact_type": artifact_type,
                                                     "bid_id": bid_id, "job_id": job_id}))
    return {
        "artifact_type": artifact_type,
        "history_length": len(history),
        "lineage_diffs": diff_lineage(history),
        "latest_diff": (diff_revisions(history[-2], history[-1])
                        if len(history) >= 2 else None),
        "latest_summary": (diff_summary(diff_revisions(history[-2], history[-1]))
                           if len(history) >= 2 else None),
    }


@app.get("/api/ui-integration-pack")
def api_ui_integration_pack():
    return get_ui_integration_pack()


@app.get("/api/ui-integration-pack/screens")
def api_ui_screens():
    return {"screen_ids": list_screen_ids()}


@app.get("/api/ui-integration-pack/screens/{screen_id}")
def api_ui_screen(screen_id: str):
    out = get_screen(screen_id)
    if out.get("error"):
        return to_http_response(build_error(ERR_INVALID_ARTIFACT_TYPE,
                                             detail={"screen_id": screen_id},
                                             hint="unknown_screen_id"))
    return out


@app.get("/api/ui-integration-pack/exports")
def api_ui_exports():
    return {"export_ids": list_export_ids()}


@app.get("/api/ui-integration-pack/exports/{export_id}")
def api_ui_export(export_id: str):
    out = get_export_action(export_id)
    if out.get("error"):
        return to_http_response(build_error(ERR_INVALID_ARTIFACT_TYPE,
                                             detail={"export_id": export_id},
                                             hint="unknown_export_id"))
    return out


@app.post("/api/smoke-harness")
def api_smoke_harness(request: Dict[str, Any] = None):
    req = request or {}
    scenario_ids = req.get("scenario_ids")
    return run_smoke(scenario_ids=scenario_ids, repository=_repo())


@app.get("/canonical/artifacts/{artifact_type}/latest-scoped")
def get_latest_scoped(artifact_type: str,
                      bid_id: Optional[str] = None,
                      job_id: Optional[str] = None,
                      org_id: Optional[str] = None):
    rec = _repo().latest_scoped(artifact_type, bid_id=bid_id,
                                 job_id=job_id, org_id=org_id)
    if rec is None:
        return to_http_response(build_error(ERR_RECORD_NOT_FOUND,
                                             detail={"artifact_type": artifact_type,
                                                     "bid_id": bid_id,
                                                     "job_id": job_id,
                                                     "org_id": org_id}))
    return rec


@app.get("/canonical/artifacts/{artifact_type}/history-scoped")
def get_history_scoped(artifact_type: str,
                       bid_id: Optional[str] = None,
                       job_id: Optional[str] = None,
                       org_id: Optional[str] = None):
    return {"records": _repo().history_scoped(artifact_type, bid_id=bid_id,
                                                job_id=job_id, org_id=org_id)}


# ---------------------------------------------------------------------------
# C92-C97 operational-readiness endpoints
# ---------------------------------------------------------------------------

@app.get("/api/authorization/summary")
def api_authorization_summary():
    return authorization_summary()


@app.post("/api/authorization/check")
def api_authorization_check(request: Dict[str, Any] = None):
    req = request or {}
    return authorize(req.get("role"), req.get("action"),
                      context=req.get("context"))


@app.get("/api/idempotency/summary")
def api_idempotency_summary():
    return get_default_idempotency_store().summary()


@app.post("/api/idempotency/reset")
def api_idempotency_reset():
    reset_default_idempotency_store()
    return {"reset": True, "summary": get_default_idempotency_store().summary()}


@app.post("/api/idempotent-save/{artifact_type}")
def api_idempotent_save(artifact_type: str, request: Dict[str, Any] = None):
    req = request or {}
    key = req.get("idempotency_key")
    artifact = req.get("artifact") or {}
    metadata = req.get("metadata") or {}
    return idempotent_save_artifact(_repo(), key, artifact_type,
                                      artifact, metadata)


@app.post("/api/backup/snapshot")
def api_backup_snapshot():
    return create_snapshot(_repo())


@app.post("/api/backup/validate")
def api_backup_validate(request: Dict[str, Any] = None):
    snap = (request or {}).get("snapshot") or {}
    return validate_snapshot(snap)


@app.post("/api/backup/restore")
def api_backup_restore(request: Dict[str, Any] = None):
    snap = (request or {}).get("snapshot") or {}
    return restore_snapshot(_repo(), snap)


@app.get("/api/reports/kinds")
def api_report_kinds():
    return {"report_kinds": list_report_kinds()}


@app.post("/api/reports/estimator-review")
def api_report_estimator_review(request: Dict[str, Any] = None):
    req = request or {}
    return build_estimator_review_report(
        _repo(), req.get("job_id"),
        revision_sequence=req.get("revision_sequence"))


@app.post("/api/reports/authority-action")
def api_report_authority_action(request: Dict[str, Any] = None):
    req = request or {}
    return build_authority_action_report(
        _repo(), req.get("bid_id"),
        revision_sequence=req.get("revision_sequence"))


@app.post("/api/reports/bid-readiness")
def api_report_bid_readiness(request: Dict[str, Any] = None):
    req = request or {}
    return build_bid_readiness_report(
        _repo(), req.get("bid_id"),
        revision_sequence=req.get("revision_sequence"))


@app.post("/api/reports/final-carry")
def api_report_final_carry(request: Dict[str, Any] = None):
    req = request or {}
    return build_final_carry_report(
        _repo(), req.get("bid_id"),
        revision_sequence=req.get("revision_sequence"))


@app.get("/api/diagnostics")
def api_diagnostics(run_smoke_flag: bool = False):
    return collect_diagnostics(_repo(), run_smoke=run_smoke_flag)


@app.post("/api/acceptance")
def api_acceptance(request: Dict[str, Any] = None):
    req = request or {}
    return run_acceptance(scenario_ids=req.get("scenario_ids"),
                           repository=_repo())


# ---------------------------------------------------------------------------
# C98-C103 product-facing endpoints
# ---------------------------------------------------------------------------

@app.get("/api/frontend/manifest")
def api_frontend_manifest():
    return build_integration_manifest()


@app.get("/api/frontend/bid-overview/{bid_id}")
def api_frontend_bid_overview(bid_id: str):
    return ControlRoomReferenceClient(_repo()).bid_overview_bundle(bid_id)


@app.get("/api/frontend/quote-case/{job_id}")
def api_frontend_quote_case(job_id: str):
    return ControlRoomReferenceClient(_repo()).quote_case_bundle(job_id)


@app.get("/api/storage/summary")
def api_storage_summary():
    from app.pdf_extraction.storage_adapter import InMemoryStorageAdapter
    adapter = InMemoryStorageAdapter()
    contract = ProductionStorageContract(adapter)
    mirror = mirror_repository(_repo(), contract)
    return {
        "mirror_result": mirror,
        "store_summary": contract.store_summary(),
    }


@app.get("/api/delivery/formats")
def api_delivery_formats():
    return {"formats": list_formats()}


@app.post("/api/delivery/report")
def api_delivery_report(request: Dict[str, Any] = None):
    req = request or {}
    report = req.get("report") or {}
    fmt = req.get("format", "json")
    return deliver_report(report, output_format=fmt)


@app.post("/api/delivery/bid/{bid_id}")
def api_delivery_bid(bid_id: str, request: Dict[str, Any] = None):
    req = request or {}
    fmt = req.get("format", "json")
    return deliver_all_for_bid(_repo(), bid_id, output_format=fmt)


@app.get("/api/operator/actions")
def api_operator_actions():
    return {
        "actions": list_operator_actions(),
        "clarification_states": list_clarification_states(),
        "carry_states": list_carry_states(),
    }


@app.post("/api/operator/apply")
def api_operator_apply(request: Dict[str, Any] = None):
    req = request or {}
    action = req.get("action")
    payload = req.get("payload") or {}
    return apply_operator_action(_repo(), action, payload)


@app.get("/api/safety/summary")
def api_safety_summary():
    return safety_summary()


@app.post("/api/safety/evaluate")
def api_safety_evaluate(request: Dict[str, Any] = None):
    req = request or {}
    return evaluate_safety(
        action=req.get("action"),
        role=req.get("role"),
        environment=req.get("environment"),
        confirmation_token=req.get("confirmation_token"),
        expected_token=req.get("expected_token"),
    )


@app.post("/api/safety/reset")
def api_safety_reset(request: Dict[str, Any] = None):
    req = request or {}
    return guarded_reset_repository(
        role=req.get("role"),
        environment=req.get("environment"),
        confirmation_token=req.get("confirmation_token"),
        expected_token=req.get("expected_token"),
    )


@app.post("/api/safety/restore")
def api_safety_restore(request: Dict[str, Any] = None):
    req = request or {}
    return guarded_restore_snapshot(
        role=req.get("role"),
        snapshot=req.get("snapshot") or {},
        environment=req.get("environment"),
        confirmation_token=req.get("confirmation_token"),
        expected_token=req.get("expected_token"),
    )


@app.post("/api/safety/wipe-idempotency")
def api_safety_wipe_idempotency(request: Dict[str, Any] = None):
    req = request or {}
    return guarded_wipe_idempotency(
        role=req.get("role"),
        environment=req.get("environment"),
        confirmation_token=req.get("confirmation_token"),
        expected_token=req.get("expected_token"),
    )


@app.post("/api/demo/product-flow")
def api_product_demo(request: Dict[str, Any] = None):
    req = request or {}
    scenario_id = req.get("scenario_id", "proceed_with_caveats")
    return run_product_demo(scenario_id=scenario_id, repository=_repo())


# ---------------------------------------------------------------------------
# C104-C109 UI/deployment endpoints
# ---------------------------------------------------------------------------

@app.get("/api/ui/screens")
def api_ui_list_screens():
    return {"screens": list_ui_screens()}


@app.get("/api/ui/quote-case/{job_id}")
def api_ui_quote_case(job_id: str):
    return adapt_quote_case(_repo(), job_id)


@app.get("/api/ui/package-overview/{bid_id}")
def api_ui_package_overview(bid_id: str):
    return adapt_package_overview(_repo(), bid_id)


@app.get("/api/ui/authority-action")
def api_ui_authority_action(bid_id: Optional[str] = None):
    return adapt_authority_action(_repo(), bid_id)


@app.get("/api/ui/bid-readiness/{bid_id}")
def api_ui_bid_readiness(bid_id: str):
    return adapt_bid_readiness(_repo(), bid_id)


@app.get("/api/ui/timeline")
def api_ui_timeline(bid_id: Optional[str] = None,
                     job_id: Optional[str] = None):
    return adapt_timeline(_repo(), bid_id=bid_id, job_id=job_id)


@app.post("/api/ui/revision-inspection")
def api_ui_revision_inspection(request: Dict[str, Any] = None):
    req = request or {}
    return adapt_revision_inspection(
        _repo(), req.get("artifact_type"),
        bid_id=req.get("bid_id"), job_id=req.get("job_id"),
        before_revision=req.get("before_revision_sequence"),
        after_revision=req.get("after_revision_sequence"),
    )


@app.get("/api/download/report-kinds")
def api_download_report_kinds():
    return {"report_kinds": list_download_report_kinds()}


@app.post("/api/download/report")
def api_download_report(request: Dict[str, Any] = None):
    req = request or {}
    return build_downloadable(
        _repo(), req.get("report_kind"),
        bid_id=req.get("bid_id"), job_id=req.get("job_id"),
        revision_sequence=req.get("revision_sequence"),
        output_format=req.get("format", "json"),
    )


@app.post("/api/download/bundle")
def api_download_bundle(request: Dict[str, Any] = None):
    req = request or {}
    return build_downloadable_bundle(
        _repo(),
        bid_id=req.get("bid_id"), job_id=req.get("job_id"),
        output_format=req.get("format", "json"),
    )


@app.get("/api/commands/vocabulary")
def api_commands_vocabulary():
    return {"commands": list_operator_commands()}


@app.post("/api/commands/execute")
def api_commands_execute(request: Dict[str, Any] = None):
    req = request or {}
    return execute_operator_command(
        _repo(), req.get("command"),
        req.get("payload") or {},
        issued_by=req.get("issued_by"),
        issued_at=req.get("issued_at"),
    )


@app.get("/api/commands/receipts")
def api_commands_receipts():
    return {
        "summary": get_default_receipt_log().summary(),
        "receipts": get_default_receipt_log().all_receipts(),
    }


@app.post("/api/commands/receipts/reset")
def api_commands_receipts_reset():
    reset_default_receipt_log()
    return {"reset": True,
            "summary": get_default_receipt_log().summary()}


@app.get("/api/config/default")
def api_config_default():
    return default_config()


@app.get("/api/config/summary")
def api_config_summary():
    return summarize_config(load_config_from_env())


@app.post("/api/config/validate")
def api_config_validate(request: Dict[str, Any] = None):
    req = request or {}
    cfg = req.get("config")
    if cfg is None:
        cfg = load_config_from_env()
    return validate_config(cfg)


@app.post("/api/bootstrap/start")
def api_bootstrap_start(request: Dict[str, Any] = None):
    req = request or {}
    cfg = req.get("config") or load_config_from_env()
    receipt = bootstrap(cfg,
                         seed_scenarios=req.get("seed_scenarios"),
                         seed_enabled_override=req.get("seed_enabled"))
    # Do not leak live components over HTTP.
    return {k: v for k, v in receipt.items()
            if k not in ("repository", "adapter")}


@app.post("/api/bootstrap/health")
def api_bootstrap_health(request: Dict[str, Any] = None):
    req = request or {}
    receipt = req.get("receipt")
    if receipt is None:
        cfg = load_config_from_env()
        receipt = bootstrap(cfg, seed_enabled_override=False)
        receipt = {k: v for k, v in receipt.items()
                   if k not in ("repository", "adapter")}
    return health_check(receipt)


@app.post("/api/demo/ui-flow")
def api_demo_ui_flow(request: Dict[str, Any] = None):
    req = request or {}
    scenario_id = req.get("scenario_id", "proceed_with_caveats")
    out = run_ui_demo(scenario_id=scenario_id)
    # The UI demo bootstrap creates its own repository; we return its
    # receipt but do not attempt to graft it into the global repo.
    return out


# ---------------------------------------------------------------------------
# C120 runtime packaging endpoints
# ---------------------------------------------------------------------------

@app.get("/api/runtime/profile")
def api_runtime_profile(mode: Optional[str] = None):
    return runtime_profile(mode)


@app.get("/api/runtime/config")
def api_runtime_config(mode: Optional[str] = None):
    return build_runtime_config(mode=mode)


@app.get("/api/runtime/frontend-handoff")
def api_runtime_frontend_handoff(mode: Optional[str] = None):
    return build_frontend_handoff(mode)


@app.post("/api/runtime/package")
def api_runtime_package(request: Dict[str, Any] = None):
    req = request or {}
    return package_runtime(
        mode=req.get("mode"),
        overrides=req.get("overrides"),
        seed_enabled_override=req.get("seed_enabled"),
    )


@app.get("/api/runtime/startup-verification")
def api_runtime_startup_verification(mode: Optional[str] = None):
    return startup_verification(mode)


# ---------------------------------------------------------------------------
# C121 acceptance walkthrough endpoints
# ---------------------------------------------------------------------------

@app.get("/api/acceptance/walkthrough/scenarios")
def api_walkthrough_scenarios():
    return {"scenarios": list_walkthrough_scenarios()}


@app.post("/api/acceptance/walkthrough")
def api_walkthrough(request: Dict[str, Any] = None):
    req = request or {}
    scenario_id = req.get("scenario_id", "proceed_with_caveats")
    return run_walkthrough(scenario_id=scenario_id)