# backend/app/main.py
from __future__ import annotations

import json
import os
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.storage.local_fs import RunStorage
from app.pdf_validation.integrity import validate_pdf_integrity
from app.pdf_validation.context import adjust_pdf_findings_by_doc_type

from app.bid_validation.ingest import ingest_bid_items, IngestError
from app.bid_validation.rules import validate_bid_items

from app.audit.models import AuditEvent, Finding, OverrideInfo
from app.audit.writer import AuditWriter

from app.quote_reconciliation.pipeline import run_structured_pipeline


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


@app.get("/health")
def health():
    return {"status": "ok", "app": APP_NAME}


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
):
    """
    Validate bid documents and optionally reconcile against quote data.

    **Modes:**
    - **PRIME_BID** — requires `pdf` + `bid_items`. Runs PDF integrity checks and bid item validation.
    - **QUOTE** — requires `bid_items` + `quote_lines`. Runs bid validation and quote reconciliation.
    - **Combined** — provide `pdf` + `bid_items` + `quote_lines` for full validation.

    **Line mapping (optional):** upload a JSON object mapping proposal line numbers to
    DOT item numbers. Applied between quote ingest and reconciliation. Requires `quote_lines`.
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

        # Parse line-to-item mapping toggle (optional JSON file)
        mapping_dict = None
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
            checks_executed.append("line_number_mapping")

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


def compute_overall_status(findings: List[Finding]) -> str:
    if any(f.severity == "FAIL" for f in findings):
        return "FAIL"
    if any(f.severity == "WARN" for f in findings):
        return "WARN"
    return "PASS"