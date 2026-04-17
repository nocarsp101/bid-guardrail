"""
C40 — Real job pilot harness.

End-to-end deterministic execution harness for one real bid job. Runs:

    1. C35 control room (which itself runs C8→C34)
    2. C38 interaction model
    3. C36 scenario engine
    4. C37 claim packet
    5. C28 exception surfacing
    6. C26 coverage audit (paired)

and emits a single bundled artifact with a `pilot_summary` of friction
points and counts. The pilot artifact is intentionally non-narrative;
every section is structured data the office can compare across runs.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

PILOT_VERSION = "pilot_harness/v1"


def run_pilot(
    quote_pdf_path: str,
    dot_pdf_path: str,
    external_sources: Optional[List[Dict[str, Any]]] = None,
    office_action_metadata: Optional[Dict[str, Any]] = None,
    job_id: Optional[str] = None,
    pilot_run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run a single real bid job through the full stack and bundle every
    governed output into one pilot artifact.
    """
    from .control_room import build_control_room
    from .control_room_interaction import build_interaction_model
    from .scenario_engine import evaluate_scenarios
    from .claim_packet import build_claim_packet
    from .exception_feedback import surface_exceptions
    from .coverage_audit import audit_paired_documents

    effective_pilot_run_id = pilot_run_id or _default_pilot_run_id(
        quote_pdf_path, dot_pdf_path,
    )

    control_room = build_control_room(
        quote_pdf_path=quote_pdf_path,
        dot_pdf_path=dot_pdf_path,
        external_sources=external_sources,
        office_action_metadata=office_action_metadata,
        job_id=job_id,
    )

    if not control_room.get("control_room_diagnostics", {}).get("pipeline_succeeded"):
        return _pilot_failure_artifact(
            control_room, effective_pilot_run_id, quote_pdf_path, dot_pdf_path,
        )

    interaction_model = build_interaction_model(control_room)

    injected = control_room.get("injected_contract") or {}
    scenario_output = evaluate_scenarios(injected)

    actioned = control_room.get("office_actions_output") or control_room.get("resolution") or {}
    claim_packet = build_claim_packet(
        actioned,
        actioned,
        control_room.get("source_management"),
    )

    coverage_audit = audit_paired_documents(
        quote_pdf_path, dot_pdf_path, label=control_room.get("job_id"),
    )

    exception_summary = surface_exceptions(
        audit_runs=[coverage_audit],
        findings_packets=[control_room.get("engineer_packet")] if control_room.get("engineer_packet") else None,
        workflow_packets=None,
    )

    pilot_summary = _build_pilot_summary(
        control_room=control_room,
        scenario_output=scenario_output,
        claim_packet=claim_packet,
        exception_summary=exception_summary,
        coverage_audit=coverage_audit,
    )

    return {
        "pilot_version": PILOT_VERSION,
        "pilot_run_id": effective_pilot_run_id,
        "job_id": control_room.get("job_id"),
        "inputs": {
            "quote_pdf_path": quote_pdf_path,
            "dot_pdf_path": dot_pdf_path,
            "external_sources_supplied": len(external_sources or []),
            "office_action_metadata_supplied": office_action_metadata is not None,
        },
        "control_room": deepcopy(control_room),
        "interaction_model": deepcopy(interaction_model),
        "scenario_output": deepcopy(scenario_output),
        "claim_packet": deepcopy(claim_packet),
        "exception_summary": deepcopy(exception_summary),
        "coverage_audit": deepcopy(coverage_audit),
        "pilot_summary": pilot_summary,
        "pilot_diagnostics": {
            "pipeline_succeeded": True,
            "stages_run": [
                "control_room", "interaction_model", "scenario_engine",
                "claim_packet", "coverage_audit", "exception_feedback",
            ],
        },
    }


# ---------------------------------------------------------------------------
# Pilot summary
# ---------------------------------------------------------------------------

def _build_pilot_summary(
    control_room: Dict[str, Any],
    scenario_output: Dict[str, Any],
    claim_packet: Dict[str, Any],
    exception_summary: Dict[str, Any],
    coverage_audit: Dict[str, Any],
) -> Dict[str, Any]:
    """Deterministic friction-summary block for the pilot artifact."""
    sections = (claim_packet.get("issue_sections") or {})
    summary_section = (claim_packet.get("summary_section") or {})

    blocked_count = len(sections.get("blocked_pairing", []))
    unmapped_count = len(sections.get("unmapped_scope", []))
    non_comparable_count = (
        len(sections.get("non_comparable_missing_quote", []))
        + len(sections.get("non_comparable_missing_external", []))
    )
    source_conflict_count = len(sections.get("source_conflicts", []))
    qty_discrepancy_count = len(sections.get("quantity_discrepancies", []))
    unit_discrepancy_count = len(sections.get("unit_discrepancies", []))

    rows_with_actions = (
        (control_room.get("office_action_summary") or {}).get("rows_with_actions", 0)
    )

    # Top exception categories from C28 (deterministic ranked list).
    top_failure_patterns = exception_summary.get("top_failure_patterns") or []
    top_exception_categories = [
        {
            "category": e.get("category"),
            "bucket": e.get("bucket"),
            "count": e.get("count"),
        }
        for e in top_failure_patterns[:5]
    ]

    return {
        "job_status": control_room.get("job_status"),
        "packet_status": claim_packet.get("packet_status"),
        "total_rows": summary_section.get("total_rows", 0),
        "critical_issues": summary_section.get("critical_issues", 0),
        "high_priority_issues": summary_section.get("high_priority_issues", 0),
        "medium_priority_issues": summary_section.get("medium_priority_issues", 0),
        "blocked_count": blocked_count,
        "unmapped_count": unmapped_count,
        "non_comparable_count": non_comparable_count,
        "source_conflict_count": source_conflict_count,
        "qty_discrepancy_count": qty_discrepancy_count,
        "unit_discrepancy_count": unit_discrepancy_count,
        "rows_with_actions": rows_with_actions,
        "scenario_basis_counts": _scenario_basis_counts(scenario_output),
        "top_exception_categories": top_exception_categories,
        "coverage_audit_dominant_limitation": (
            (coverage_audit.get("metrics") or {}).get("dominant_downstream_limitation")
        ),
    }


def _scenario_basis_counts(scenario_output: Dict[str, Any]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for s in scenario_output.get("scenarios") or []:
        sid = s.get("scenario_id")
        out[sid] = int(s.get("rows_comparable") or 0)
    return out


# ---------------------------------------------------------------------------
# Failure artifact
# ---------------------------------------------------------------------------

def _pilot_failure_artifact(
    control_room: Dict[str, Any],
    pilot_run_id: str,
    quote_pdf_path: str,
    dot_pdf_path: str,
) -> Dict[str, Any]:
    return {
        "pilot_version": PILOT_VERSION,
        "pilot_run_id": pilot_run_id,
        "job_id": control_room.get("job_id"),
        "inputs": {
            "quote_pdf_path": quote_pdf_path,
            "dot_pdf_path": dot_pdf_path,
        },
        "control_room": deepcopy(control_room),
        "interaction_model": None,
        "scenario_output": None,
        "claim_packet": None,
        "exception_summary": None,
        "coverage_audit": None,
        "pilot_summary": {
            "job_status": control_room.get("job_status"),
            "pipeline_succeeded": False,
            "failure_reason": (
                (control_room.get("control_room_diagnostics") or {}).get("failure_reason")
            ),
        },
        "pilot_diagnostics": {
            "pipeline_succeeded": False,
            "stages_run": ["control_room"],
        },
    }


def _default_pilot_run_id(quote_pdf_path: str, dot_pdf_path: str) -> str:
    import os
    return f"pilot-{os.path.basename(quote_pdf_path)}-{os.path.basename(dot_pdf_path)}"
