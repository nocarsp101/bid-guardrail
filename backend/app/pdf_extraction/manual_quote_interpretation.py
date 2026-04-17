"""
C42 — Append-only manual interpretation layer.

Lets a human enter row data for unreadable or partially readable
quote evidence blocks WITHOUT mutating any machine-extracted truth or
any prior manual entry. Every new manual entry appends; corrections
create a new entry that supersedes the previous entry on the same key
(the previous entry remains visible with `entry_status = superseded`).

The module never auto-converts manual entries into machine truth and
never pretends a manually-entered row came from OCR. Every entry
carries a `source_provenance = manual_interpretation` tag and a
reference to the source evidence block it was keyed to.

Manual entry row id scheme:
    `qm-p{page}-m{ordinal}`  — visibly distinct from machine
                                `qr-p{page}-r{ordinal}` ids so downstream
                                layers can tell origin at a glance.

Hard rules:
    - Append-only. Stores are rebuilt with new revisions appended; old
      entries are never mutated.
    - Superseding requires a NEW entry; the old entry is tagged
      `superseded` but preserved intact for audit.
    - Unknown source block ids are surfaced in diagnostics; no phantom
      rows are created.
    - Invalid entries (missing required fields) are surfaced in
      diagnostics; they do NOT become active rows but are preserved in
      history as rejected.
    - Effective-current rows can be derived without mutating history.
    - Inputs are deep-copied on read/write.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

MANUAL_INTERPRETATION_VERSION = "manual_quote_interpretation/v1"

# Closed entry-status vocabulary.
ENTRY_ACTIVE = "active"
ENTRY_SUPERSEDED = "superseded"
ENTRY_REJECTED = "rejected"

# Closed entry-validation vocabulary.
VALIDATION_VALID = "valid"
VALIDATION_MISSING_BLOCK_REF = "missing_source_block_ref"
VALIDATION_UNKNOWN_BLOCK_ID = "unknown_source_block_id"
VALIDATION_MISSING_REQUIRED_FIELDS = "missing_required_fields"
VALIDATION_BAD_NUMERIC = "bad_numeric_values"

# Closed source_provenance vocabulary (used by manual rows).
PROV_MANUAL_INTERPRETATION = "manual_interpretation"

# Closed entry-status summary vocabulary for the overall store.
ROW_STATUS_MACHINE_EXTRACTED = "machine_extracted"
ROW_STATUS_MACHINE_PARTIAL_HUMAN_COMPLETED = "machine_partial_human_completed"
ROW_STATUS_FULLY_MANUAL_INTERPRETATION = "fully_manual_interpretation"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_manual_interpretation(
    intake_output: Dict[str, Any],
    entries_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a fresh manual interpretation store.

    Args:
        intake_output: C41 `run_intake()` return value.
        entries_metadata: optional initial entries payload shaped:
            {
                "entries": [
                    {
                        "manual_row_key": "... caller-provided stable key ...",
                        "source_block_id": "blk-7",
                        "entered_by": "alice",
                        "entered_at": "2026-04-15T10:00:00",
                        "entry_reason": "unreadable row interpreted from plan",
                        "entered_values": {
                            "description": "...",
                            "qty": 100.0,
                            "unit": "LF",
                            "unit_price": 5.0,
                            "amount": 500.0,
                        }
                    },
                    ...
                ]
            }
    """
    store = _empty_store(intake_output)
    if entries_metadata:
        store = append_manual_revision(store, entries_metadata)
    return store


def append_manual_revision(
    existing_store: Dict[str, Any],
    entries_metadata: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Append new manual entries to an existing store.

    Returns a NEW store dict. The input is never mutated. New entries
    with the same `manual_row_key` as an existing active entry supersede
    the previous one; the old entry is tagged `superseded` but kept.
    """
    if not existing_store:
        raise ValueError("append_manual_revision requires an existing store")
    out = deepcopy(existing_store)
    entries_list: List[Dict[str, Any]] = list(out.get("entries") or [])
    block_ids = {b["block_id"] for b in out.get("evidence_blocks") or []}
    raw_new = (entries_metadata or {}).get("entries") or []

    unknown_block_ids: List[str] = list(out.get("diagnostics", {}).get("unknown_block_ids") or [])
    rejected_count = int(out.get("diagnostics", {}).get("rejected_count") or 0)

    # Start ordinal where the existing entries left off.
    ordinal = len(entries_list)

    for raw in raw_new:
        if not isinstance(raw, dict):
            continue
        validated = _validate_and_build_entry(raw, block_ids, ordinal)
        ordinal += 1

        if validated["entry_status"] == ENTRY_REJECTED:
            entries_list.append(validated)
            rejected_count += 1
            if validated["entry_validation_status"] == VALIDATION_UNKNOWN_BLOCK_ID:
                src = (raw.get("source_block_ref") or {}).get("block_id") or raw.get("source_block_id")
                if src:
                    unknown_block_ids.append(src)
            continue

        # Mark any prior active entry on the same manual_row_key as superseded.
        row_key = validated["manual_row_key"]
        for prior in entries_list:
            if (prior.get("entry_status") == ENTRY_ACTIVE
                    and prior.get("manual_row_key") == row_key):
                prior["entry_status"] = ENTRY_SUPERSEDED
                prior["superseded_by"] = validated["manual_entry_id"]
        entries_list.append(validated)

    out["entries"] = entries_list
    out["diagnostics"] = {
        "entry_count": len(entries_list),
        "active_count": sum(1 for e in entries_list if e["entry_status"] == ENTRY_ACTIVE),
        "superseded_count": sum(1 for e in entries_list if e["entry_status"] == ENTRY_SUPERSEDED),
        "rejected_count": rejected_count,
        "unknown_block_ids": sorted(set(unknown_block_ids)),
    }
    out["summary"] = _build_store_summary(out)
    return out


def get_current_manual_rows(store: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the active manual rows projected into the quote-row contract
    shape (with manual provenance tags). History is untouched."""
    if not store:
        return []
    out: List[Dict[str, Any]] = []
    for entry in store.get("entries") or []:
        if entry.get("entry_status") == ENTRY_ACTIVE:
            out.append(_project_entry_to_row(entry))
    return out


def get_manual_history(store: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return a deep copy of the full manual entry history in sequence
    order. Includes active, superseded, and rejected entries."""
    if not store:
        return []
    return deepcopy(store.get("entries") or [])


# ---------------------------------------------------------------------------
# Entry validation + construction
# ---------------------------------------------------------------------------

def _validate_and_build_entry(
    raw: Dict[str, Any],
    known_block_ids: set,
    ordinal: int,
) -> Dict[str, Any]:
    entered_values = raw.get("entered_values") or {}
    source_block_ref = raw.get("source_block_ref") or {}
    block_id = raw.get("source_block_id") or source_block_ref.get("block_id")

    manual_row_key = str(raw.get("manual_row_key") or f"manual-{ordinal}")
    manual_entry_id = str(raw.get("manual_entry_id") or f"me-{ordinal}")
    source_page = raw.get("source_page")
    if source_page is None:
        source_page = int(source_block_ref.get("source_page") or 0)

    base = {
        "manual_entry_id": manual_entry_id,
        "manual_row_key": manual_row_key,
        "sequence": ordinal,
        "source_block_ref": {
            "block_id": block_id,
            "source_page": source_page,
        },
        "entered_by": raw.get("entered_by"),
        "entered_at": raw.get("entered_at"),
        "entry_reason": raw.get("entry_reason"),
        "entered_values": deepcopy(entered_values),
        "entry_status": ENTRY_ACTIVE,
        "entry_validation_status": VALIDATION_VALID,
        "entry_trace": deepcopy(raw.get("entry_trace") or {}),
        "superseded_by": None,
    }

    # Validation.
    if not block_id:
        base["entry_status"] = ENTRY_REJECTED
        base["entry_validation_status"] = VALIDATION_MISSING_BLOCK_REF
        return base

    if block_id not in known_block_ids:
        base["entry_status"] = ENTRY_REJECTED
        base["entry_validation_status"] = VALIDATION_UNKNOWN_BLOCK_ID
        return base

    if not entered_values.get("description"):
        base["entry_status"] = ENTRY_REJECTED
        base["entry_validation_status"] = VALIDATION_MISSING_REQUIRED_FIELDS
        return base

    if not _numerics_valid(entered_values):
        base["entry_status"] = ENTRY_REJECTED
        base["entry_validation_status"] = VALIDATION_BAD_NUMERIC
        return base

    return base


def _numerics_valid(entered: Dict[str, Any]) -> bool:
    """Numeric fields must be either None or positive floats. Never
    negative, never NaN."""
    for key in ("qty", "unit_price", "amount"):
        val = entered.get(key)
        if val is None:
            continue
        try:
            f = float(val)
        except (TypeError, ValueError):
            return False
        if f <= 0:
            return False
    return True


# ---------------------------------------------------------------------------
# Row projection
# ---------------------------------------------------------------------------

def _project_entry_to_row(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Project a manual entry into a row shape compatible with the
    C12 accepted-row contract (superset — adds manual provenance)."""
    vals = entry.get("entered_values") or {}
    source_page = int((entry.get("source_block_ref") or {}).get("source_page") or 0)
    ordinal = int(entry.get("sequence") or 0)
    manual_row_id = f"qm-p{source_page}-m{ordinal}"

    return {
        # Canonical quote-row contract fields.
        "normalized_row_id": manual_row_id,
        "row_id": 900_000 + ordinal,  # high ordinal to never collide with C24 aggregation (100_000+) or pass-1 rows
        "line_ref": vals.get("line_ref"),
        "description": vals.get("description"),
        "qty": vals.get("qty"),
        "unit": vals.get("unit"),
        "unit_price": vals.get("unit_price"),
        "amount": vals.get("amount"),
        "source_page": source_page,
        "extraction_source": "manual_interpretation",
        "source_text": f"[manual entry {entry.get('manual_entry_id')}]",
        "row_issues": [],
        "provenance": {
            "extraction_source": "manual_interpretation",
            "source_page": source_page,
            "ocr_used": False,
            "parser": "manual_quote_interpretation/v1",
        },
        "field_sources": {
            "qty": "manual_interpretation" if vals.get("qty") is not None else "not_present",
            "unit": "manual_interpretation" if vals.get("unit") is not None else "not_present",
            "unit_price": "manual_interpretation" if vals.get("unit_price") is not None else "not_present",
            "amount": "manual_interpretation" if vals.get("amount") is not None else "not_present",
        },
        "enrichment_trace": {"rules_attempted": []},
        "enricher_version": None,
        "table_rule_version": None,
        "pattern_library_version": None,

        # C42 additions.
        "source_provenance": PROV_MANUAL_INTERPRETATION,
        "source_block_ref": deepcopy(entry.get("source_block_ref") or {}),
        "manual_entry_ref": {
            "manual_entry_id": entry.get("manual_entry_id"),
            "manual_row_key": entry.get("manual_row_key"),
            "entered_by": entry.get("entered_by"),
            "entered_at": entry.get("entered_at"),
        },
    }


# ---------------------------------------------------------------------------
# Store helpers
# ---------------------------------------------------------------------------

def _empty_store(intake_output: Dict[str, Any]) -> Dict[str, Any]:
    intake_output = intake_output or {}
    return {
        "manual_interpretation_version": MANUAL_INTERPRETATION_VERSION,
        "pdf_path": intake_output.get("pdf_path"),
        "evidence_blocks": deepcopy(intake_output.get("evidence_blocks") or []),
        "machine_intake_status": intake_output.get("machine_intake_status"),
        "machine_accepted_rows_count": len(intake_output.get("accepted_rows") or []),
        "entries": [],
        "diagnostics": {
            "entry_count": 0,
            "active_count": 0,
            "superseded_count": 0,
            "rejected_count": 0,
            "unknown_block_ids": [],
        },
        "summary": {
            "rows_machine_count": len(intake_output.get("accepted_rows") or []),
            "rows_manual_count": 0,
            "rows_partial_human_completed": 0,
            "manual_entry_count": 0,
            "superseded_entry_count": 0,
            "interpretation_status": _overall_status(
                machine_rows=len(intake_output.get("accepted_rows") or []),
                manual_rows=0,
            ),
        },
    }


def _build_store_summary(store: Dict[str, Any]) -> Dict[str, Any]:
    diag = store.get("diagnostics") or {}
    machine_rows = int(store.get("machine_accepted_rows_count") or 0)
    active_manual = int(diag.get("active_count") or 0)
    superseded = int(diag.get("superseded_count") or 0)
    return {
        "rows_machine_count": machine_rows,
        "rows_manual_count": active_manual,
        "rows_partial_human_completed": active_manual if machine_rows > 0 else 0,
        "manual_entry_count": int(diag.get("entry_count") or 0),
        "superseded_entry_count": superseded,
        "interpretation_status": _overall_status(machine_rows, active_manual),
    }


def _overall_status(machine_rows: int, manual_rows: int) -> str:
    if machine_rows > 0 and manual_rows == 0:
        return ROW_STATUS_MACHINE_EXTRACTED
    if machine_rows > 0 and manual_rows > 0:
        return ROW_STATUS_MACHINE_PARTIAL_HUMAN_COMPLETED
    if machine_rows == 0 and manual_rows > 0:
        return ROW_STATUS_FULLY_MANUAL_INTERPRETATION
    return ROW_STATUS_MACHINE_EXTRACTED  # default when nothing present
